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
	// 不必互相覆盖）。onceHandlers 是触发一次就摘除的监听器（waitPageLoad
	// 这类「等一次事件」的场景，反复注册会累积）。每条 onceEntry 带单调
	// 递增 id（cancel 按 id 精确摘除——同一函数字面量生成的多个闭包共享
	// 相同代码指针，靠指针识别会跨协程误删）与可选的 sessionID 过滤
	// （别的 session 的同名事件不该摘除本 session 的监听器）。
	evtMu        sync.RWMutex
	handlers     map[string][]func(cdpEvent)
	onceHandlers map[string][]onceEntry
	nextOnceID   atomic.Uint64
	anyHandler   func(cdpEvent) // 兜底监听器，nil = 不要

	err     error // 连接死后的终态错误，给后续 Call 立刻返回
	errOnce sync.Once
	done    chan struct{}

	// onClose 是断连终态回调（failAll 首次触发时调用一次）：常驻浏览器层
	// 靠它把 CDP 断连传播到所有在途流——拿到 headers 后上游沉默的流
	// 不该再干等 stallLimit。异步派发（failAll 可能正持读循环路径，
	// 回调里再抢锁会把断连通知本身卡死）。
	onCloseMu sync.Mutex
	onClose   func(error)
}

// newCDPClient 包住一条已握手的 conn，启动读循环。
// wsURL 仅供错误信息用。
func newCDPClient(conn *cdpConn) *cdpClient {
	c := &cdpClient{
		conn:         conn,
		pending:      map[int64]chan cdpReply{},
		handlers:     map[string][]func(cdpEvent){},
		onceHandlers: map[string][]onceEntry{},
		done:         make(chan struct{}),
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

// onceEntry 是一条 Once 监听器：id 是注册序号（单调递增，cancel 按它
// 精确摘除）；sessionID 非空时只响应同 session 的事件（"" = 不限 session）。
type onceEntry struct {
	id        uint64
	sessionID string
	fn        func(cdpEvent)
}

// Once 注册一个只触发一次的 method 监听器：第一次匹配事件到达时调用 fn 后
// 自动摘除。用于「等一次事件」的场合（Page.loadEventFired），避免反复注册累积。
// 返回的 cancel 可在等待方放弃时提前摘除监听器（事件永不到达时防泄漏）。
func (c *cdpClient) Once(method string, fn func(cdpEvent)) (cancel func()) {
	return c.OnceSession("", method, fn)
}

// OnceSession 同 Once，但只响应 sessionID 匹配的事件（sessionID="" 时不限，
// 仍要求监听器本身不绑定特定 session）。别的 session 发出的同名事件不会
// 摘除本监听器——dispatch 按 entry.sessionID 精确过滤，只摘除命中者。
func (c *cdpClient) OnceSession(sessionID, method string, fn func(cdpEvent)) (cancel func()) {
	id := c.nextOnceID.Add(1)
	c.evtMu.Lock()
	c.onceHandlers[method] = append(c.onceHandlers[method],
		onceEntry{id: id, sessionID: sessionID, fn: fn})
	c.evtMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.evtMu.Lock()
			list := c.onceHandlers[method]
			for i, e := range list {
				if e.id == id {
					c.onceHandlers[method] = append(list[:i:i], list[i+1:]...)
					break
				}
			}
			c.evtMu.Unlock()
		})
	}
}

// setOnClose 注册断连回调（只允许设一次，重复设置会覆盖旧值）。
// 回调在独立 goroutine 里异步执行，拿到的 err 与 failAll 的终态错误一致。
func (c *cdpClient) setOnClose(fn func(error)) {
	c.onCloseMu.Lock()
	c.onClose = fn
	c.onCloseMu.Unlock()
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

	if err := c.conn.sendTextCtx(ctx, payload); err != nil {
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

// Close 直接断底层连接再唤醒所有 pending，**不发礼貌 Close 帧**：
// sendClose 要排队抢 writeMu——若有一个阻塞中的 sendText 正持锁
// （对端不收、5s 写超时未到），Close 自己就排在它后面被拖住，
// 停机预算被白白吃掉。cdpConn.Close 立即 close(closed)+conn.Close，
// 瞬间打断任何阻塞中的 sendText 与 writeMu 持有者，再 failAll 唤醒
// pending 调用方——两端都不被拖。
func (c *cdpClient) Close() {
	c.conn.Close()
	c.failAll(errors.New("cdp: client closed"))
}

// Done 返回连接终态信号。
func (c *cdpClient) Done() <-chan struct{} { return c.done }

// Closed 报告连接是否已终态（断/关）：ensureRunning 的存活校验用它，
// 避免每次拿一个僵尸 client 空转一轮才被发现。
func (c *cdpClient) Closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

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
	c.evtMu.Lock()
	fns := append([]func(cdpEvent){}, c.handlers[head.Method]...)
	// onceHandlers 按 entry.sessionID 精确过滤：只摘除并触发匹配
	// （不限 session 或与事件 session 一致）的条目；别的 session 的同名
	// 监听器原样留在表里等自己的事件，绝不能被无关事件整键清空。
	var onces []func(cdpEvent)
	if list := c.onceHandlers[head.Method]; len(list) > 0 {
		kept := list[:0]
		for _, e := range list {
			if e.sessionID == "" || e.sessionID == evt.SessionID {
				onces = append(onces, e.fn)
			} else {
				kept = append(kept, e)
			}
		}
		if len(kept) == 0 {
			delete(c.onceHandlers, head.Method)
		} else {
			c.onceHandlers[head.Method] = kept
		}
	}
	any := c.anyHandler
	c.evtMu.Unlock()
	for _, fn := range fns {
		fn(evt)
	}
	for _, fn := range onces {
		fn(evt)
	}
	if any != nil {
		any(evt)
	}
}

// failAll 终态化：记错、关 done、唤醒全部 pending、异步通知 onClose。
func (c *cdpClient) failAll(err error) {
	first := false
	c.errOnce.Do(func() {
		c.err = err
		close(c.done)
		first = true
	})
	if first {
		c.onCloseMu.Lock()
		cb := c.onClose
		c.onCloseMu.Unlock()
		if cb != nil {
			go cb(err)
		}
	}
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
