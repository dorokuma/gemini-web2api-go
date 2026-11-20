package app

// cdp.go —— Chrome DevTools Protocol 的轻量 JSON-RPC 2.0 多路复用器。
//
// 模型：一条 cdpConn 上并发跑任意数量的 Call；每个 Call 带自增 id，
// 响应按 id 路由回对应的 pending channel；不带 id 的消息是事件
// （Runtime.bindingCalled / Inspector.targetCrashed / Target.* 等），按 method
// 分发给注册的回调。
//
// 关键约束：CDP 的 sessionId 路由——大部分域（Runtime/Page/Network/Inspector）
// 要在某个 target 的 session 里调用，而不是浏览器级顶层连接。所以 Call 的签名
// 第一参就是 sessionID（浏览器级调用传 ""）。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// cdpReply 是一次 Call 的返回值：要么 result，要么 error。
type cdpReply struct {
	result json.RawMessage
	err    error
}

// cdpEvent 是一条不带 id 的事件通知。
type cdpEvent struct {
	SessionID string
	Method    string
	Params    json.RawMessage
}

// cdpClient 挂在一条 cdpConn 上做多路复用。
type cdpClient struct {
	conn *cdpConn

	nextID  atomic.Int64
	pendMu  sync.Mutex
	pending map[int64]chan cdpReply

	// 事件分发：按 method 精确匹配注册。同一个 method 允许多个监听器
	// （崩溃自愈和流式管道都要听 Inspector.targetCrashed 一类的信号时
	// 不必互相覆盖）。
	evtMu      sync.RWMutex
	handlers   map[string][]func(cdpEvent)
	anyHandler func(cdpEvent) // 兜底监听器，nil = 不要

	err     error // 连接死后的终态错误，给后续 Call 立刻返回
	errOnce sync.Once
	done    chan struct{}
}

// newCDPClient 包住一条已握手的 conn，启动读循环。
// wsURL 仅供错误信息用。
func newCDPClient(conn *cdpConn) *cdpClient {
	c := &cdpClient{
		conn:     conn,
		pending:  map[int64]chan cdpReply{},
		handlers: map[string][]func(cdpEvent){},
		done:     make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// On 注册一个按 method 精确匹配的事件回调；fn 在读循环 goroutine 里跑，
// 不许在里面做长阻塞操作（会把所有事件与响应的派发都堵住）。
func (c *cdpClient) On(method string, fn func(cdpEvent)) {
	c.evtMu.Lock()
	c.handlers[method] = append(c.handlers[method], fn)
	c.evtMu.Unlock()
}

// OnAny 设一个兜底监听器（所有事件，注册过 method 的也会先到专用回调再到它）。
func (c *cdpClient) OnAny(fn func(cdpEvent)) {
	c.evtMu.Lock()
	c.anyHandler = fn
	c.evtMu.Unlock()
}

// Call 发一条 method(params)，等带同 id 的响应回来，把 result 解进 out。
// sessionID 为空串 = 浏览器级（Target.* / Browser.* / Network.setCookies 的
// browserContext 级等）；非空 = 在某个 target 会话里调。
//
// ctx 取消时立刻返回，pending 槽位留在表里等响应到了自然回收——
// CDP 的 id 严格递增不复用，槽位不会错配；连接断开时所有 pending 由
// readLoop 的 failAll 统一唤醒，调用方不会挂死。
func (c *cdpClient) Call(ctx context.Context, sessionID, method string, params interface{}, out interface{}) error {
	select {
	case <-c.done:
		return c.err
	default:
	}

	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return err
		}
		raw = b
	}
	id := c.nextID.Add(1)
	msg := map[string]interface{}{
		"id":     id,
		"method": method,
	}
	if raw != nil {
		msg["params"] = raw
	}
	if sessionID != "" {
		msg["sessionId"] = sessionID
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	ch := make(chan cdpReply, 1)
	c.pendMu.Lock()
	c.pending[id] = ch
	c.pendMu.Unlock()

	if err := c.conn.sendText(payload); err != nil {
		c.pendMu.Lock()
		delete(c.pending, id)
		c.pendMu.Unlock()
		return fmt.Errorf("cdp send %s: %w", method, err)
	}

	select {
	case rep := <-ch:
		if rep.err != nil {
			return rep.err
		}
		if out != nil && len(rep.result) > 0 {
			if err := json.Unmarshal(rep.result, out); err != nil {
				return fmt.Errorf("cdp decode %s result: %w", method, err)
			}
		}
		return nil
	case <-ctx.Done():
		// 槽位留到响应来或被 failAll 清掉——CDP id 不复用，留着无害；
		// 删了反而会丢一条「本可解出来」的响应。
		return ctx.Err()
	case <-c.done:
		return c.err
	}
}

// Close 礼貌地发 Close 帧再断连，唤醒所有 pending。
func (c *cdpClient) Close() {
	c.conn.sendClose()
	c.failAll(errors.New("cdp: client closed"))
	c.conn.Close()
}

// Done 返回连接终态信号。
func (c *cdpClient) Done() <-chan struct{} { return c.done }

// readLoop 是唯一的读 goroutine：把帧派给 pending 或事件回调，
// 任何读错误都走 failAll 收尾（保证等待方立刻拿到错而不是挂死）。
func (c *cdpClient) readLoop() {
	for {
		op, payload, err := c.conn.readMessage()
		if err != nil {
			c.failAll(fmt.Errorf("cdp read: %w", err))
			return
		}
		if op != wsOpText {
			continue // DevTools 不用 Binary；保险起见只认 Text
		}
		c.dispatch(payload)
	}
}

// dispatch 把一条 Text 帧按「有 id=响应 / 无 id=事件」路由。
func (c *cdpClient) dispatch(payload []byte) {
	var head struct {
		ID        int64           `json:"id"`
		Method    string          `json:"method"`
		SessionID string          `json:"sessionId"`
		Result    json.RawMessage `json:"result"`
		Error     *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(payload, &head); err != nil {
		logf("[cdp] drop undecodable frame (%d bytes)", len(payload))
		return
	}
	if head.ID != 0 {
		c.pendMu.Lock()
		ch, ok := c.pending[head.ID]
		if ok {
			delete(c.pending, head.ID)
		}
		c.pendMu.Unlock()
		if !ok {
			return // ctx 取消后迟到的响应，槽位已被调用方放弃回收
		}
		if head.Error != nil {
			ch <- cdpReply{err: fmt.Errorf("cdp %d: %s", head.Error.Code, head.Error.Message)}
		} else {
			ch <- cdpReply{result: head.Result}
		}
		return
	}
	// 事件
	evt := cdpEvent{SessionID: head.SessionID, Method: head.Method, Params: head.Params}
	c.evtMu.RLock()
	fns := append([]func(cdpEvent){}, c.handlers[head.Method]...)
	any := c.anyHandler
	c.evtMu.RUnlock()
	for _, fn := range fns {
		fn(evt)
	}
	if any != nil {
		any(evt)
	}
}

// failAll 终态化：记错、关 done、唤醒全部 pending。
func (c *cdpClient) failAll(err error) {
	c.errOnce.Do(func() {
		c.err = err
		close(c.done)
	})
	c.pendMu.Lock()
	for id, ch := range c.pending {
		delete(c.pending, id)
		ch <- cdpReply{err: err}
	}
	c.pendMu.Unlock()
}

// callCDPWithTimeout 是 Call 的便捷包：给 ctx 再套一个超时。
func (c *cdpClient) callCDPWithTimeout(ctx context.Context, timeout time.Duration, sessionID, method string, params interface{}, out interface{}) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.Call(cctx, sessionID, method, params, out)
}
