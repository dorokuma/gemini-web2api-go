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

// cdpPending 是一个在飞请求的路由槽（R7-5）：除答复 channel 外还携带
// 可选的迟到回调与「调用方已放弃」标记。调用方 ctx 先于回复取消时
// 绝不删除槽位——槽位归属保留到真实回复到达（readLoop 删除并一次性
// 调 onLate）或连接断开（failAll 统一清理），零常驻等待协程。
type cdpPending struct {
	ch     chan cdpReply
	onLate func(json.RawMessage, error) // 迟到回复处理器，nil = 无
	late   bool                         // 调用方已按 ctx 取消放弃等待
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
	pending map[int64]*cdpPending

	// latePendingCap / latePendingCount 是按连接的未决迟到回收背压
	//（R8-4，Reviewer MF-4）：仅对注册时携带 onLate 的请求（浏览器
	// BrowserContext 创建）计数。共享 WS 保持连通但从不回创建回复时，
	// 每次构建超时都释放 m.ctxs 名额却会留下一个 pending+channel+回调
	// ——不设上限则在外请求（以及上游已创建但未交回 ID 的远端 BC）
	// 无限积累。达到 cap 的新创建直接拒发；回复到达（dispatch 消费槽位）、
	// 发送失败或断连清空时回收名额（绝不靠定时删除 pending 项）。
	latePendingCap   int
	latePendingCount int

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
		conn:           conn,
		pending:        map[int64]*cdpPending{},
		handlers:       map[string][]func(cdpEvent){},
		onceHandlers:   map[string][]onceEntry{},
		done:           make(chan struct{}),
		latePendingCap: 16, // R8-4 默认按连接最多 16 个未决迟到回收
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
	return c.callCommon(ctx, sessionID, method, params, out, nil)
}

// CallWithLateHandler 是 Call 的迟到回收版（R7-5，Reviewer MF-5/MF-6）：
// 签名与语义与 Call 一致，额外承担「调用方已按 ctx 取消放弃后，真实回复
// 迟早到达」的零协程回收——
//
//  1. 写发送准入（拿 writeSem 与写入 WS）严格受调用方 ctx 约束：ctx 已
//     取消绝不向共享连接发送（排队等写权期间被取消同样不外发）；
//  2. 请求写入 WS 后，若 <-ctx.Done() 先于回复到达：不删除 pending[id]，
//     只把槽位标记 late（onLate 在注册时随请求一并装入）——readLoop 在
//     收到真实回复时删除槽位并启一次性短协程调用 onLate（绝不在 readLoop
//     内同步阻塞）；连接断开时 failAll 清理 pending 表自动释放。调用方
//     无固定收割窗口、无常驻等待 goroutine——迟到多久的回复都被确定
//     回收，回复永远不到时零协程开销。
func (c *cdpClient) CallWithLateHandler(ctx context.Context, sessionID, method string, params interface{}, out interface{}, onLate func(raw json.RawMessage, err error)) error {
	return c.callCommon(ctx, sessionID, method, params, out, onLate)
}

// callCommon 是 Call / CallWithLateHandler 的共用实现。
func (c *cdpClient) callCommon(ctx context.Context, sessionID, method string, params interface{}, out interface{}, onLate func(json.RawMessage, error)) error {
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
	if err := c.registerPending(id, ch, onLate); err != nil {
		return err
	}

	// R10-6（Reviewer S1/Oracle S1）：登记后排队等写权期间读循环可能
	// 已不可恢复退出——failAll 清扫了 pending 并终态化底层写端，但已
	// 登记的调用方拿到写权时绝不能把创建帧发往无读循环的连接（远端
	// 泄漏 BrowserContext）。发送前再次核对终态：立即撤登记返回错误。
	select {
	case <-c.done:
		c.pendMu.Lock()
		if _, ok := c.pending[id]; ok {
			delete(c.pending, id)
			if onLate != nil && c.latePendingCount > 0 {
				c.latePendingCount--
			}
		}
		c.pendMu.Unlock()
		return c.err
	default:
	}

	// 测试钩子（生产恒 nil）：在越过 done 预检之后、进入 sendTextCtx
	// 写权等待之前执行一次（R12-6-S3，Oracle S3/Reviewer #8）——回归测试
	// 借此确证真实写者已越过发送前终态预检再触发读失败：pending 非空
	// 只证明登记完成，不能证明写者越过预检（反转顺序的实现可先关闭
	// done，写者恢复便在预检返回，错误实现也能看到零创建）。生产路径
	// 绝不经由它做任何判断，绝不改变生产行为。
	if hook := loadTestHookBeforeCDPWriteWait(); hook != nil {
		hook()
	}

	// R7-5：发送准入严格受调用方 ctx 约束——sendTextCtx 拿 writeSem 与
	// 持权写入全程 select ctx.Done()，排队等写权期间被取消绝不再向共享
	// 连接发送（半活连接也不累积外发命令）。
	if err := c.conn.sendTextCtx(ctx, payload); err != nil {
		c.pendMu.Lock()
		if _, ok := c.pending[id]; ok {
			delete(c.pending, id)
			if onLate != nil && c.latePendingCount > 0 {
				c.latePendingCount--
			}
		}
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
		c.handleCancelOwnership(id, ch, onLate)
		return ctx.Err()
	case <-c.done:
		return c.err
	}
}

// handleCancelOwnership 是取消分支的生产善后处理函数（R10-7，
// Reviewer S3）：先经 resolveCancelOwnership 裁决回复所有权；归取消方
// 时从缓冲 ch 实际接收 dispatch 已送达的唯一回复并调 onLate 善后
// （ch 容量为 1 且 dispatch 锁内删除后必然送达，协程绝不阻塞 readLoop、
// 绝不关闭共享连接）。测试观察本函数的回调与回收效果即可验证生产
// 接管链，无需自行删槽/送 ch/读 ch/调 onLate。
func (c *cdpClient) handleCancelOwnership(id int64, ch chan cdpReply,
	onLate func(json.RawMessage, error)) {
	if !c.resolveCancelOwnership(id, onLate) {
		return
	}
	if onLate != nil {
		go func() {
			r := <-ch
			onLate(r.result, r.err)
		}()
	}
}

// resolveCancelOwnership 是取消分支的唯一所有者裁决（R8-1，Reviewer
// MF-1；R9-6 抽取为可确定性构造的生产函数）：在 pendMu 下查
// pending[id]——
//   - 槽位仍在（回复尚未被派发）：onLate != nil 时仅标记 late，
//     readLoop 收到真实回复时删除槽位并一次性调 onLate 回收；
//     onLate == nil 时照常删除槽位，迟到的回复在 dispatch 端
//     自然被丢弃（无副作用的普通调用无需迟到处理）。返回 false
//     （回复所有权归 dispatch，取消方不得再碰 ch）。
//   - 槽位已消失（dispatch 刚刚删了槽并将唯一回复送进了缓冲 ch，
//     而本次 select 恰选中了取消分支）：回复绝不可被双方同时
//     放弃——返回 true，取消方负责从 ch 接走该唯一回复并调 onLate
//     善后（ch 缓冲容量为 1 且 dispatch 在锁内删除后必然送达，
//     协程绝不阻塞 readLoop、绝不关闭共享连接）。
//
// 由此每条已发请求的回复有且仅有一个所有者：要么 dispatch 直接
// 调 onLate，要么取消方从 ch 接走调 onLate——绝不零次也不二次。
func (c *cdpClient) resolveCancelOwnership(id int64, onLate func(json.RawMessage, error)) bool {
	c.pendMu.Lock()
	defer c.pendMu.Unlock()
	if ent, ok := c.pending[id]; ok {
		if onLate != nil {
			ent.late = true
		} else {
			delete(c.pending, id)
		}
		return false
	}
	return true
}

// registerPending 在 pendMu 内原子完成「终态复核 → 名额背压 → 登记
// → 计数」（R9-5，Reviewer MF-5）。登记前的 <-c.done 预检在 pendMu
// 之外，调用方通过预检后读协程可能已先 failAll 关闭 done 并扫空
// pending 表——若仅凭锁外预检放行登记，槽位与 latePendingCap 名额无人
// 回收（泄漏），甚至向已无读循环的连接发出创建。锁内再次拒绝终态后，
// 登记与终态清扫有唯一先后：要么 failAll 先扫完（此处必然看到 done
// 已关而拒绝），要么本次先登记（failAll 随后必然把本槽一并清出唤醒）。
// 返回终态错误时调用方绝不发送。
func (c *cdpClient) registerPending(id int64, ch chan cdpReply,
	onLate func(json.RawMessage, error)) error {
	c.pendMu.Lock()
	defer c.pendMu.Unlock()
	select {
	case <-c.done:
		return c.err
	default:
	}
	// R8-4（Reviewer MF-4）：携带 onLate 的请求受按连接未决上限背压——
	// 未决创建已达上限时直接拒绝发送（槽位绝不登记，名额不占），让半活
	// 连接上的在外创建与远端 BC 数量严格有界。
	if onLate != nil && c.latePendingCap > 0 && c.latePendingCount >= c.latePendingCap {
		return browserErr("too many pending browser context creations")
	}
	c.pending[id] = &cdpPending{ch: ch, onLate: onLate}
	if onLate != nil {
		c.latePendingCount++
	}
	return nil
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
		// late/onLate 必须在 pendMu 内拷贝：调用方取消路径在同一锁下
		// 写 ent.late，锁外裸读构成数据竞争。
		c.pendMu.Lock()
		ent, ok := c.pending[head.ID]
		if ok {
			delete(c.pending, head.ID)
			// R8-4：携带 onLate 的槽位被真实回复消费——回收未决名额。
			if ent.onLate != nil && c.latePendingCount > 0 {
				c.latePendingCount--
			}
		}
		var late bool
		var onLate func(json.RawMessage, error)
		var ch chan cdpReply
		if ok {
			late, onLate, ch = ent.late, ent.onLate, ent.ch
		}
		c.pendMu.Unlock()
		if !ok {
			return // ctx 取消后迟到的响应，槽位已被调用方放弃回收
		}
		var rep cdpReply
		if head.Error != nil {
			rep = cdpReply{err: fmt.Errorf("cdp %d: %s", head.Error.Code, head.Error.Message)}
		} else {
			rep = cdpReply{result: head.Result}
		}
		// R7-5：调用方已按 ctx 取消放弃（late）——回复绝不再发给无人
		// 接收的 channel，改为一次性短协程调 onLate 回收（绝不在 readLoop
		// 内同步阻塞，共享连接的读派发绝不被迟到处理拖住）。
		if late && onLate != nil {
			go onLate(rep.result, rep.err)
			return
		}
		ch <- rep
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

// testHookBeforeCDPWriteWait 仅供测试注入（生产恒为 nil）：在
// callCommon 越过 done 预检之后、进入 sendTextCtx 写权等待之前执行
// 一次。存取经原子容器——钩子读取发生在调用方协程，与测试的安装/
// 复位并发交错是测试结构的固有行为；生产恒 nil 时零开销、零行为影响。
var testHookBeforeCDPWriteWait atomic.Value // func()

func loadTestHookBeforeCDPWriteWait() func() {
	if v := testHookBeforeCDPWriteWait.Load(); v != nil {
		return v.(func())
	}
	return nil
}

// testHookAfterConnClosedBeforeDone 仅供测试注入（生产恒为 nil）：在
// failAll 封闭底层写端之后、公布 client.done 之前执行一次。生产路径
// 绝不经由它做任何判断，绝不改变生产行为。存取经原子容器——钩子读取
// 发生在 failAll（连接读循环）协程，与测试的安装/复位并发交错是
// 测试结构的固有行为；生产恒 nil 时零开销、零行为影响。
var testHookAfterConnClosedBeforeDone atomic.Value // func()

func loadTestHookAfterConnClosedBeforeDone() func() {
	if v := testHookAfterConnClosedBeforeDone.Load(); v != nil {
		return v.(func())
	}
	return nil
}

// failAll 终态化：记错、终态化底层连接/写端、关 done、唤醒全部
// pending、异步通知 onClose。R10-6（Reviewer S1/Oracle S1）：读循环
// 不可恢复退出（本地解析超限/读错误）时底层写端必须同步终态化——
// c.conn.Close 幂等关闭底层连接，close(c.closed) 唤醒一切正在
// writeSem 排队或阻塞写入的调用方：已登记等写权的创建调用绝不向
// 无读循环的连接发出创建帧（远端泄漏 BrowserContext）。普通请求
// ctx 取消路径不受影响（sendTextCtx 的取消守卫语义不动）。
//
// R11-7（Oracle B1/Reviewer #4）：顺序严格——**先封闭底层写端，再公布
// done 终态**。此前顺序相反（先 close(c.done) 后 c.conn.Close()）：在两步
// 之间取得写权的排队写者看到 conn.closed 尚未关闭，仍会向无读循环的连接
// 发出整帧——观察到 client.done 并不等于写端已封闭。conn.Close 先
// close(closed) 置终态标记再断底层连接，此后 writeFrame 与排队准入一律
// 被拒；done 在其后公布，保证任何观察到 done 的写者绝无可能再发出帧。
func (c *cdpClient) failAll(err error) {
	first := false
	c.errOnce.Do(func() {
		c.err = err
		first = true
	})
	if first {
		// 先封闭底层写端（幂等）：close(closed) + conn.Close——此后
		// writeSem 排队准入与 writeFrame 全部拒绝，阻塞写者被底层断连唤醒。
		_ = c.conn.Close()
		// 测试钩子（生产恒 nil）：在写端已封闭、done 尚未公布的精确交错
		// 点上让出执行权——回归测试借此确定性构造「排队写者在两个终态
		// 信号之间取得写权」的 Oracle B1 交错（顺序反转时写端未封闭，
		// 帧可出门；顺序正确时准入必被拒）。生产路径绝不经由它做任何
		// 判断，绝不改变生产行为。
		if hook := loadTestHookAfterConnClosedBeforeDone(); hook != nil {
			hook()
		}
		// 写端已彻底封闭后才公布终态：观察到 done ⇒ 绝无可能再发出帧。
		close(c.done)
		c.onCloseMu.Lock()
		cb := c.onClose
		c.onCloseMu.Unlock()
		if cb != nil {
			go cb(err)
		}
	} else {
		// 非首个调用方：done 已由首个调用方在封闭写端后关闭，此处仅为
		// 幂等（conn.Close 幂等，重复调用无副作用）。
		_ = c.conn.Close()
	}
	c.pendMu.Lock()
	for id, ent := range c.pending {
		delete(c.pending, id)
		// R8-4：断连清空 pending——每个携带 onLate 的槽位回收未决名额。
		if ent.onLate != nil && c.latePendingCount > 0 {
			c.latePendingCount--
		}
		ent.ch <- cdpReply{err: err}
	}
	c.pendMu.Unlock()
}

// callCDPWithTimeout 是 Call 的便捷包：给 ctx 再套一个超时。
func (c *cdpClient) callCDPWithTimeout(ctx context.Context, timeout time.Duration, sessionID, method string, params interface{}, out interface{}) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.Call(cctx, sessionID, method, params, out)
}
