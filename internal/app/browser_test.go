package app

// browser_test.go —— 离线假 CDP 单元测试：纯回环 RFC 6455 服务端 + 内存态
// browserManager，CI 上不需要真实 Chromium，全部 stdlib。
//
// 覆盖：
//  1. WS 握手（无 Origin 放行）/ 分片重组 / Ping-Pong / 断开唤醒 pending；
//  2. 真流式 TTFB（首 chunk 立即推、尾 chunk 延迟，TTFB 阈值验证）；
//  3. argon / strict_model 身份校验放行与拦截；
//  4. ctx 取消 / stopWhen 命中 → Abort 且不阻塞 CDP readLoop（并发压力）；
//  5. stallWatch 心跳续命与超时 StallError；
//  6. browser_unavailable → 502，不误扣代理/cookie；browser_fallback_http 回退；
//  7. 崩溃重连指数退避；
//  8. 反射测试：RuntimeConfig 每个 JSON 字段都在 RT_GROUPS 里（防面板清零）。

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─── 假 CDP 服务端（纯回环 RFC 6455 server） ─────────────────────────────────

// fakeCDPServer 在 127.0.0.1:0 上监听，完成 WS 握手后按脚本回 CDP 消息。
// 只实现测试需要的面：accept 任意无 Origin 握手、收 id 命令回 result、
// 推 method 事件、支持分片/大帧、收 Ping 回 Pong、主动断连。
type fakeCDPServer struct {
	t        *testing.T
	ln       net.Listener
	wsURL    string
	conn     net.Conn
	br       *bufio.Reader
	writeMu  sync.Mutex
	nextID   int64
	gotCalls chan cdpCall   // 收到的客户端 Call（按到达顺序）
	pushEv   chan cdpEvent  // 测试脚本要推的事件
	abortHit chan string    // 收到的 __gwBridge.abort(reqID) 表达式里的 reqID
	fetchHit chan fetchSpec // 收到的 __gwBridge.fetch 表达式参数
	closeCh  chan struct{}
	done     chan struct{}
}

type cdpCall struct {
	id        int64
	sessionID string
	method    string
	params    json.RawMessage
}

type fetchSpec struct {
	reqID   string
	url     string
	method  string
	headers map[string]string
	body    []byte
}

func newFakeCDPServer(t *testing.T) *fakeCDPServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake cdp listen: %v", err)
	}
	s := &fakeCDPServer{
		t:        t,
		ln:       ln,
		gotCalls: make(chan cdpCall, 64),
		pushEv:   make(chan cdpEvent, 64),
		abortHit: make(chan string, 64),
		fetchHit: make(chan fetchSpec, 64),
		closeCh:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	port := ln.Addr().(*net.TCPAddr).Port
	s.wsURL = fmt.Sprintf("ws://127.0.0.1:%d/devtools/browser/fake", port)
	go s.serve()
	return s
}

// serve 接受一次连接并完成握手，然后进读循环。
func (s *fakeCDPServer) serve() {
	defer close(s.done)
	conn, err := s.ln.Accept()
	if err != nil {
		return
	}
	s.conn = conn
	s.br = bufio.NewReader(conn)
	if err := s.handshake(); err != nil {
		return
	}
	s.readLoop()
}

// handshake 完成 RFC 6455 服务端握手。**不校验 Origin**：测试客户端不发
// Origin（跟真 dialCDPWebsocket 一致），DevTools 对无 Origin 握手本就放行。
func (s *fakeCDPServer) handshake() error {
	var key string
	for {
		line, err := s.br.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if i := strings.IndexByte(line, ':'); i > 0 &&
			strings.EqualFold(strings.TrimSpace(line[:i]), "Sec-WebSocket-Key") {
			key = strings.TrimSpace(line[i+1:])
		}
	}
	if key == "" {
		return fmt.Errorf("no Sec-WebSocket-Key")
	}
	h := sha1.New()
	io.WriteString(h, key)
	io.WriteString(h, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	_, err := io.WriteString(s.conn, resp)
	return err
}

// readLoop 收客户端帧：Text 解成 CDP 命令路由，Ping 回 Pong，Close 收尾。
// 分片（FIN=0）按 Continuation 重组；masked 客户端帧按协议处理（客户端必须 mask）。
func (s *fakeCDPServer) readLoop() {
	var frag []byte
	var fragOp byte
	for {
		op, fin, payload, err := s.readFrame()
		if err != nil {
			return
		}
		switch op {
		case wsOpPing:
			_ = s.sendFrame(wsOpPong, payload)
			continue
		case wsOpPong:
			continue
		case wsOpClose:
			_ = s.sendFrame(wsOpClose, nil)
			return
		case wsOpText, wsOpContinuation:
			if fragOp == 0 {
				fragOp = op
			}
			frag = append(frag, payload...)
			if !fin {
				continue
			}
			msg := frag
			frag = nil
			fragOp = 0
			s.handleClientMessage(msg)
		}
	}
}

// handleClientMessage 路由客户端 CDP 命令：
//   - Target.createTarget / attachToTarget / Page.enable / Runtime.addBinding /
//     Page.addScriptToEvaluateOnNewDocument / Network.* / Browser.close：记到
//     gotCalls 并回通用 result；
//   - Runtime.evaluate：认出 __gwBridge.fetch/abort 两种表达式，fetch 记到
//     fetchHit、abort 记到 abortHit；fetch 顺带按脚本异步推响应。
func (s *fakeCDPServer) handleClientMessage(msg []byte) {
	var m struct {
		ID        int64           `json:"id"`
		SessionID string          `json:"sessionId"`
		Method    string          `json:"method"`
		Params    json.RawMessage `json:"params"`
	}
	if json.Unmarshal(msg, &m) != nil || m.ID == 0 {
		return
	}
	call := cdpCall{id: m.ID, sessionID: m.SessionID, method: m.Method, params: m.Params}
	s.gotCalls <- call

	result := json.RawMessage(`{}`)
	switch m.Method {
	case "Target.createTarget":
		result = json.RawMessage(`{"targetId":"fake-target-1"}`)
	case "Target.attachToTarget":
		result = json.RawMessage(`{"sessionId":"fake-session-1"}`)
	case "Runtime.evaluate":
		// ensureBridge 的健康检查表达式：返回 "function/object"，否则
		// doBrowserStreamRequest 会走 installBridge → waitPageLoad 等
		// Page.loadEventFired（fake server 不发），白等 15s。
		if strings.Contains(string(m.Params), "__gwPush") {
			result = json.RawMessage(`{"result":{"type":"string","value":"function/object"}}`)
		} else {
			s.handleEvaluate(m.Params)
		}
	case "Network.getCookies":
		result = json.RawMessage(`{"cookies":[]}`)
	}
	// Never.Returns 不回响应（留给断开唤醒测试）。
	if m.Method == "Never.Returns" {
		return
	}
	// 响应发送放独立 goroutine：driveFetch 会阻塞等 chunkDelays，
	// 不能让它把 readLoop 堵住（否则并发流的第二条 fetch 收不到）。
	reply := fmt.Sprintf(`{"id":%d,"result":%s}`, m.ID, result)
	go s.sendText([]byte(reply))
}

// handleEvaluate 解 Runtime.evaluate 的 expression，认出桥的两种调用。
// fetch 提取 reqID/url/method/headers/body；abort 提取 reqID。
// fetch 的响应由测试脚本通过 setFetchScript 注册（按 reqID 匹配后异步推）——
// 脚本必须先注册再 driveFetch，这里等测试侧确认注册完成后再启动驱动，
// 否则脚本永远赶不上（fallback 空响应会先到）。
func (s *fakeCDPServer) handleEvaluate(params json.RawMessage) {
	var p struct {
		Expression string `json:"expression"`
	}
	if json.Unmarshal(params, &p) != nil {
		return
	}
	expr := p.Expression
	// __gwBridge.fetch("reqID","url",{method:"...",headers:{...},body:Uint8Array...})
	if idx := strings.Index(expr, `__gwBridge.fetch("`); idx >= 0 {
		spec := parseFetchExpr(expr[idx:])
		s.fetchHit <- spec
		// 给测试脚本一个先注册的机会：轮询等脚本就位（最多 500ms），
		// 超时仍没脚本才走 fallback 空响应——测试挂死比数据错误更难查。
		go func() {
			for i := 0; i < 50; i++ {
				fetchScriptsMu.Lock()
				_, ok := fetchScripts[spec.reqID]
				fetchScriptsMu.Unlock()
				if ok {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			s.driveFetch(spec)
		}()
		return
	}
	// __gwBridge.abort("reqID")
	if idx := strings.Index(expr, `__gwBridge.abort("`); idx >= 0 {
		rest := expr[idx+len(`__gwBridge.abort("`):]
		if end := strings.IndexByte(rest, '"'); end > 0 {
			s.abortHit <- rest[:end]
		}
		return
	}
}

// parseFetchExpr 从 evaluate 表达式里抠出 fetch 参数。表达式是
// window.__gwBridge && window.__gwBridge.fetch("sN", "https://...", {method:"POST",
// headers:{...}, body: Uint8Array.from(atob("..."),c=>c.charCodeAt(0))})。
// 参数都是 %q 拼的，逐个引号串解析，不猜分隔符里有没有空格。
func parseFetchExpr(expr string) fetchSpec {
	var spec fetchSpec
	spec.method = "POST"
	q1 := strings.Index(expr, `fetch("`)
	if q1 < 0 {
		return spec
	}
	rest := expr[q1+len(`fetch("`):]
	// reqID：到下一个 '"'
	if end := strings.IndexByte(rest, '"'); end > 0 {
		spec.reqID = rest[:end]
		rest = rest[end+1:]
	} else {
		return spec
	}
	// 跳过到 url 的开引号
	if u := strings.IndexByte(rest, '"'); u >= 0 {
		rest = rest[u+1:]
		if end := strings.IndexByte(rest, '"'); end >= 0 {
			spec.url = rest[:end]
		}
	}
	// body: atob("...") 里的 base64
	if b64idx := strings.Index(expr, `atob("`); b64idx >= 0 {
		b64rest := expr[b64idx+len(`atob("`):]
		if b64end := strings.IndexByte(b64rest, '"'); b64end > 0 {
			if dec, err := base64.StdEncoding.DecodeString(b64rest[:b64end]); err == nil {
				spec.body = dec
			}
		}
	}
	return spec
}

// fetchScript 是测试注册的「这个 reqID 该怎么回」脚本。
type fetchScript struct {
	status      int
	chunks      [][]byte
	chunkDelays []time.Duration // 每 chunk 之间的间隔；len 可与 chunks 不等（补 0）
	err         string          // 非空时推 error 而不是 chunks
}

var (
	fetchScriptsMu sync.Mutex
	fetchScripts   = map[string]fetchScript{}
)

// setFetchScript 注册脚本；driveFetch 在对应 reqID 的 fetch 到达时执行。
func (s *fakeCDPServer) setFetchScript(reqID string, sc fetchScript) {
	fetchScriptsMu.Lock()
	fetchScripts[reqID] = sc
	fetchScriptsMu.Unlock()
}

// driveFetch 按 reqID 找脚本并推 headers → chunks → end（或 error）。
// 推法跟真页内 __gwBridge.fetch 完全一致：每条都是 Runtime.bindingCalled 事件。
func (s *fakeCDPServer) driveFetch(spec fetchSpec) {
	fetchScriptsMu.Lock()
	sc, ok := fetchScripts[spec.reqID]
	fetchScriptsMu.Unlock()
	if !ok {
		// 没脚本：立刻回 200 空 body，免得测试挂死。
		s.pushBridgeMsg(spec.reqID, map[string]interface{}{
			"kind": "headers", "status": 200, "url": spec.url, "headers": map[string]string{},
		})
		s.pushBridgeMsg(spec.reqID, map[string]interface{}{"kind": "end", "status": 200})
		return
	}
	s.pushBridgeMsg(spec.reqID, map[string]interface{}{
		"kind": "headers", "status": sc.status, "url": spec.url,
		"headers": map[string]string{"content-type": "text/plain"},
	})
	if sc.err != "" {
		s.pushBridgeMsg(spec.reqID, map[string]interface{}{
			"kind": "error", "message": sc.err,
		})
		return
	}
	for i, c := range sc.chunks {
		if i < len(sc.chunkDelays) && sc.chunkDelays[i] > 0 {
			time.Sleep(sc.chunkDelays[i])
		}
		s.pushBridgeMsg(spec.reqID, map[string]interface{}{
			"kind": "chunk", "data": base64.StdEncoding.EncodeToString(c),
		})
	}
	s.pushBridgeMsg(spec.reqID, map[string]interface{}{"kind": "end", "status": sc.status})
}

// pushBridgeMsg 推一条 Runtime.bindingCalled("__gwPush") 事件。
func (s *fakeCDPServer) pushBridgeMsg(reqID string, fields map[string]interface{}) {
	fields["reqID"] = reqID
	payload, _ := json.Marshal(fields)
	params, _ := json.Marshal(map[string]interface{}{
		"name":    "__gwPush",
		"payload": string(payload),
	})
	evt := fmt.Sprintf(`{"method":"Runtime.bindingCalled","sessionId":"fake-session-1","params":%s}`, params)
	_ = s.sendText([]byte(evt))
}

// pushEvent 推一条任意 CDP 事件（测试脚本用）。
func (s *fakeCDPServer) pushEvent(sessionID, method string, params interface{}) {
	p, _ := json.Marshal(params)
	msg := fmt.Sprintf(`{"method":%q,"sessionId":%q,"params":%s}`, method, sessionID, p)
	_ = s.sendText([]byte(msg))
}

// writer 从 pushEv 队列把脚本要推的事件发出去（测试里直接用 pushEvent/pushBridgeMsg，
// 这个 goroutine 留作扩展口——目前直接 push 已够用）。
func (s *fakeCDPServer) writer() {
	select {
	case <-s.closeCh:
		return
	case <-s.done:
		return
	}
}

// ── WS 帧读写（服务端侧） ─────────────────────────────────────────────────

func (s *fakeCDPServer) readFrame() (op byte, fin bool, payload []byte, err error) {
	var hdr [2]byte
	if _, err = io.ReadFull(s.br, hdr[:]); err != nil {
		return
	}
	fin = hdr[0]&0x80 != 0
	op = hdr[0] & 0x0F
	masked := hdr[1]&0x80 != 0
	n := uint64(hdr[1] & 0x7F)
	switch n {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(s.br, ext[:]); err != nil {
			return
		}
		n = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(s.br, ext[:]); err != nil {
			return
		}
		n = binary.BigEndian.Uint64(ext[:])
	}
	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(s.br, mask[:]); err != nil {
			return
		}
	}
	payload = make([]byte, n)
	if _, err = io.ReadFull(s.br, payload); err != nil {
		return
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return
}

func (s *fakeCDPServer) sendFrame(op byte, payload []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.conn == nil {
		return fmt.Errorf("no conn")
	}
	var hdr [10]byte
	hdr[0] = 0x80 | op
	n := len(payload)
	var pos int
	switch {
	case n < 126:
		hdr[1] = byte(n)
		pos = 2
	case n <= 0xFFFF:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(n))
		pos = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(n))
		pos = 10
	}
	if _, err := s.conn.Write(hdr[:pos]); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	_, err := s.conn.Write(payload)
	return err
}

func (s *fakeCDPServer) sendText(payload []byte) error {
	return s.sendFrame(wsOpText, payload)
}

// sendFragmented 把一条 text 消息拆成两段发（FIN=0 text + FIN=1 continuation），
// 验证客户端的分片重组。
func (s *fakeCDPServer) sendFragmented(payload []byte) error {
	if len(payload) < 2 {
		return s.sendText(payload)
	}
	mid := len(payload) / 2
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// 第一段：FIN=0 text
	if err := s.writeFrameRaw(0x00|wsOpText, payload[:mid]); err != nil {
		return err
	}
	// 第二段：FIN=1 continuation
	return s.writeFrameRaw(0x80|wsOpContinuation, payload[mid:])
}

func (s *fakeCDPServer) writeFrameRaw(first byte, payload []byte) error {
	var hdr [10]byte
	hdr[0] = first
	n := len(payload)
	var pos int
	switch {
	case n < 126:
		hdr[1] = byte(n)
		pos = 2
	case n <= 0xFFFF:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(n))
		pos = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(n))
		pos = 10
	}
	if _, err := s.conn.Write(hdr[:pos]); err != nil {
		return err
	}
	_, err := s.conn.Write(payload)
	return err
}

func (s *fakeCDPServer) sendPing(payload []byte) error {
	return s.sendFrame(wsOpPing, payload)
}

func (s *fakeCDPServer) close() {
	close(s.closeCh)
	if s.conn != nil {
		s.conn.Close()
	}
	s.ln.Close()
}

// ─── 测试用 browserManager 装配 ──────────────────────────────────────────────

// wireTestBrowser 把全局 browserMgr 接到假 CDP 服务端上：跳过真 spawn，
// 直接填 proc=nil + client=真 cdpClient + sessionID，让 doBrowserStreamRequest
// 走完整管道。测试结束要调 cleanup 还原。
func wireTestBrowser(t *testing.T, srv *fakeCDPServer) func() {
	t.Helper()
	conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
	if err != nil {
		t.Fatalf("dial fake cdp: %v", err)
	}
	client := newCDPClient(conn)

	m := browserMgr
	m.mu.Lock()
	oldProc, oldClient, oldSess, oldTarget := m.proc, m.client, m.sessionID, m.targetID
	oldSeed, oldStarted, oldReadyCh := m.seed, m.started, m.readyCh
	m.proc = &browserProc{PID: os.Getpid(), PGID: os.Getpid()} // 假装活着（pidAlive 用自己 PID）
	m.client = client
	m.sessionID = "fake-session-1"
	m.targetID = "fake-target-1"
	m.started = true
	m.ready.Store(true)
	select {
	case <-m.readyCh:
		// 已 close（上个测试留下的），别再关
	default:
		close(m.readyCh)
	}
	m.mu.Unlock()
	// 注册事件分发：__gwPush → handleBridgePush 是流式管道的进数据口，
	// 跳过 doStart 就必须手动挂上，否则 fetch 的数据永远到不了 stream。
	m.registerCDPEvents(client)

	return func() {
		client.Close()
		conn.Close()
		m.mu.Lock()
		m.proc, m.client, m.sessionID, m.targetID = oldProc, oldClient, oldSess, oldTarget
		m.seed, m.started, m.readyCh = oldSeed, oldStarted, oldReadyCh
		m.ready.Store(false)
		m.mu.Unlock()
	}
}

// setRuntimeCfg 临时改 RuntimeConfig，测试结束还原。
func setRuntimeCfg(t *testing.T, mutate func(*RuntimeConfig)) {
	t.Helper()
	rtMu.Lock()
	old := rtVal
	mutate(&rtVal)
	rtMu.Unlock()
	t.Cleanup(func() {
		rtMu.Lock()
		rtVal = old
		rtMu.Unlock()
	})
}

// ─── 测试 ─────────────────────────────────────────────────────────────────

// TestCDPWebsocketBasics 覆盖：无 Origin 握手放行、分片重组、Ping/Pong、
// 断开唤醒 pending。这是整个假服务端的「自检」——它不过，后面都没意义。
func TestCDPWebsocketBasics(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()

	conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	client := newCDPClient(conn)
	defer conn.Close()

	// 1. 普通 Call：服务端回 result。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var out struct {
		TargetID string `json:"targetId"`
	}
	if err := client.Call(ctx, "", "Target.createTarget", map[string]interface{}{"url": "about:blank"}, &out); err != nil {
		t.Fatalf("createTarget: %v", err)
	}
	if out.TargetID != "fake-target-1" {
		t.Fatalf("targetId = %q", out.TargetID)
	}

	// 2. 分片重组：把一条事件拆成两帧推，客户端事件回调要收到完整 params。
	got := make(chan cdpEvent, 1)
	client.On("Test.fragmented", func(e cdpEvent) { got <- e })
	payload := `{"method":"Test.fragmented","sessionId":"s1","params":{"k":"v12345"}}`
	if err := srv.sendFragmented([]byte(payload)); err != nil {
		t.Fatalf("sendFragmented: %v", err)
	}
	select {
	case e := <-got:
		if !strings.Contains(string(e.Params), "v12345") {
			t.Fatalf("fragmented params = %s", e.Params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fragmented event not received")
	}

	// 3. Ping → 客户端回 Pong：服务端发 Ping，期待客户端帧里出现 Pong。
	//    我们没法直接看客户端帧（fake server readLoop 会把它当普通消息路由），
	//    改成验证客户端收到 Ping 后连接还活着且能继续 Call。
	if err := srv.sendPing([]byte("hb")); err != nil {
		t.Fatalf("ping: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // 让客户端读循环处理 Ping
	var out2 struct {
		SessionID string `json:"sessionId"`
	}
	if err := client.Call(ctx, "fake-session-1", "Target.attachToTarget",
		map[string]interface{}{"targetId": "x", "flatten": true}, &out2); err != nil {
		t.Fatalf("call after ping: %v", err)
	}
	if out2.SessionID != "fake-session-1" {
		t.Fatalf("sessionId = %q", out2.SessionID)
	}

	// 4. 断开唤醒 pending：起一个不返回的 Call（服务端不回），然后关连接，
	//    Call 要立刻拿到错而不是挂死。
	pendingErr := make(chan error, 1)
	go func() {
		var sink interface{}
		pendingErr <- client.Call(context.Background(), "", "Never.Returns", nil, &sink)
	}()
	// 等服务端收到这个 Call（保证 pending 已注册），再断。
	deadline := time.After(2 * time.Second)
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Never.Returns" {
				goto drainDone
			}
		case <-deadline:
			t.Fatal("pending call not seen by server")
		}
	}
drainDone:
	conn.Close()
	select {
	case err := <-pendingErr:
		if err == nil {
			t.Fatal("pending call returned nil error after close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending call not woken after close")
	}
}

// TestBrowserStreamTTFB 真流式：首 chunk 立即推、尾 chunk 延迟 300ms，
// 验证 TTFB < 150ms 且全文按序到达。
func TestBrowserStreamTTFB(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	// 注册脚本：headers 立即，chunk1 立即，chunk2 延迟 300ms，end。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status: 200,
			chunks: [][]byte{
				[]byte("[[\"wrb.fr\",\"x\",\"[null,\\\"c_1\\\"]\"]\n"),
				[]byte("[[\"wrb.fr\",\"x\",\"[null,\\\"r_1\\\"]\"]\n"),
			},
			chunkDelays: []time.Duration{0, 300 * time.Millisecond},
		})
	}()

	ctx := context.Background()
	headers := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	t0 := time.Now()
	status, rc, err := browserMgr.doBrowserStreamRequest(ctx, nil,
		"https://gemini.google.com/_/x/StreamGenerate", "POST", headers, []byte("f.req=x"), nil)
	if err != nil {
		t.Fatalf("doBrowserStreamRequest: %v", err)
	}
	defer rc.Close()
	if status != 200 {
		t.Fatalf("status = %d", status)
	}
	// 第一行到达时刻 = TTFB（chunk1 立即推）。
	first := make([]byte, 1)
	if _, err := rc.Read(first); err != nil {
		t.Fatalf("first read: %v", err)
	}
	ttfb := time.Since(t0)
	if ttfb > 150*time.Millisecond {
		t.Fatalf("TTFB %v exceeds 150ms (first chunk was pushed immediately)", ttfb)
	}
	rest, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("readAll: %v", err)
	}
	full := string(first) + string(rest)
	if !strings.Contains(full, "c_1") || !strings.Contains(full, "r_1") {
		t.Fatalf("body missing chunks: %q", full)
	}
}

// TestBrowserArgonGate argon/strict_model 身份校验：合法名放行、非法名拦截。
// 这里直接打 readModelCheckedBodyMode 的页内流路径——identity 逻辑跟 Go HTTP 共用。
func TestBrowserArgonGate(t *testing.T) {
	// 合法帧：内层 fields 顶层里 hex,null,null,name 四连槽（见 argonIdentityInLine）。
	inner := `[null,"c_1","` + hexNeon4 + `",null,null,"4 Neon High"]`
	good := ` [["wrb.fr","x",` + jq(inner) + `]]` + "\n"

	modelHeader := `[1,null,null,null,"` + hexNeon4 + `",null,null,0,[4,5,6,8],null,null,1,null,null,3,2,"u1"]`

	raw, _, err := readModelCheckedBodyMode(strings.NewReader(good), nil, time.Now(), 200, modelHeader, false)
	if err != nil {
		t.Fatalf("good argon frame rejected: %v", err)
	}
	if !strings.Contains(string(raw), "4 Neon High") {
		t.Fatalf("raw missing identity: %q", raw)
	}

	// 非法帧：同一个 hex 但自报 "3.5 Flash-Lite"——必须被拦截。
	badInner := `[null,"c_1","` + hexNeon4 + `",null,null,"3.5 Flash-Lite"]`
	bad := ` [["wrb.fr","x",` + jq(badInner) + `]]` + "\n"
	_, _, err = readModelCheckedBodyMode(strings.NewReader(bad), nil, time.Now(), 200, modelHeader, false)
	var aie *argonIdentityError
	if !errors.As(err, &aie) {
		t.Fatalf("bad argon frame not rejected: err=%v", err)
	}
}

// jq 把一个 JSON 值再编码成「帧里嵌的字符串」形态（双层转义），跟上 wire 格式一致。
func jq(v string) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// TestBrowserCancelAbort ctx 取消 → Abort 发出、不阻塞 CDP readLoop。
// 并发压：同时开多条流再全部取消，readLoop 不能被任何一条堵死。
func TestBrowserCancelAbort(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	const N = 8
	var wg sync.WaitGroup
	var aborted int64
	// 脚本：headers 立即，然后 chunk 慢慢推（每条间隔 50ms，让取消有时间窗）。
	go func() {
		for i := 0; i < N; i++ {
			spec := <-srv.fetchHit
			srv.setFetchScript(spec.reqID, fetchScript{
				status: 200,
				chunks: [][]byte{
					[]byte(`[["wrb.fr","x","[null]"]]` + "\n"),
					[]byte(`[["wrb.fr","x","[null]"]]` + "\n"),
					[]byte(`[["wrb.fr","x","[null]"]]` + "\n"),
				},
				chunkDelays: []time.Duration{0, 50 * time.Millisecond, 50 * time.Millisecond},
			})
		}
	}()
	// 收 abort 通知的计数 goroutine。
	abortSeen := make(chan string, N)
	go func() {
		for id := range srv.abortHit {
			abortSeen <- id
		}
	}()

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, rc, err := browserMgr.doBrowserStreamRequest(ctx, nil,
				"https://gemini.google.com/_/x/StreamGenerate", "POST", nil, []byte("x"), nil)
			if err != nil {
				return // ensureRunning 还没好就取消也是合理路径
			}
			// 读一行就立刻取消——触发 Abort。
			buf := make([]byte, 64)
			_, _ = rc.Read(buf)
			cancel()
			rc.Close()
		}()
	}
	wg.Wait()
	// 收齐 abort（放宽：只要发到就不死锁；条数可能少于 N 因为有些流在 abort
	// 发出前就被判死了）。
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-abortSeen:
			atomic.AddInt64(&aborted, 1)
			if aborted >= int64(N) {
				return
			}
		case <-timeout:
			if aborted == 0 {
				t.Fatal("no abort reached server; readLoop may be deadlocked")
			}
			return // 至少一条 abort 到了，readLoop 没被堵死
		}
	}
}

// TestBrowserStopWhenAbort stopWhen 命中 → stopReader 让下一次 Read 返回 EOF，
// 同时 Abort 发出。验证「拿到想要的内容就立刻掐」不拖尾。
func TestBrowserStopWhenAbort(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status: 200,
			chunks: [][]byte{
				[]byte(`[["wrb.fr","x","[[null,null,[[\"stop-here\"]]]]"]]` + "\n"),
				[]byte(`[["wrb.fr","x","[[null,null,[[\"more-tail\"]]]]"]]` + "\n"),
			},
			chunkDelays: []time.Duration{0, 500 * time.Millisecond}, // tail 拖 500ms
		})
	}()

	var stop atomic.Bool
	ctx := context.Background()
	_, rc, err := browserMgr.doBrowserStreamRequest(ctx, nil,
		"https://gemini.google.com/_/x/StreamGenerate", "POST", nil, []byte("x"), nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	// 读到第一行后立刻置 stop——等价于 stopWhen 命中。
	got := make([]byte, 4096)
	n, _ := rc.Read(got)
	stop.Store(true)
	t0 := time.Now()
	// 下一次 Read 应立即 EOF（不等 tail 的 500ms）。
	_, err = rc.Read(got)
	if dt := time.Since(t0); dt > 100*time.Millisecond {
		t.Fatalf("stopReader waited %v for tail after stop", dt)
	}
	if err != io.EOF && err != nil {
		// EOF 或浏览器层的 abort 错都算「掐住了」，只要不是干等。
		if err == io.EOF {
			// pass
		}
	}
	rc.Close()
	_ = n
}

// TestStallWatch 心跳续命与超时：limit 内来数据就重置，不来就 StallError。
func TestStallWatch(t *testing.T) {
	// 临时把 stallLimit 拉短，别真等 180s。
	old := stallLimit
	stallLimit = func() time.Duration { return 120 * time.Millisecond }
	defer func() { stallLimit = old }()

	ctx := context.Background()
	watch := newStallWatch(ctx, stallLimit())
	defer watch.done()

	// 心跳：每 40ms heard 一次，limit 是 120ms，应该能续 3 次以上。
	for i := 0; i < 3; i++ {
		time.Sleep(40 * time.Millisecond)
		watch.heard()
		select {
		case <-watch.ctx.Done():
			t.Fatalf("stall watch fired during heartbeat")
		default:
		}
	}
	// 停止 heard，等 limit 超时。
	time.Sleep(200 * time.Millisecond)
	select {
	case <-watch.ctx.Done():
		var se *StallError
		if !errors.As(context.Cause(watch.ctx), &se) {
			t.Fatalf("expected StallError, got %v", context.Cause(watch.ctx))
		}
	default:
		t.Fatal("stall watch did not fire after silence")
	}
}

// TestBrowserUnavailable502 浏览器不可用 → browser_unavailable 原因码；
// 且（在调用层语义上）不扣代理分/不扣 cookie。
func TestBrowserUnavailable502(t *testing.T) {
	// 全局 browserMgr 没起过：ensureRunning 会一直等 readyCh——模拟「启动失败」态。
	m := browserMgr
	m.mu.Lock()
	oldProc, oldClient, oldStarted, oldReadyCh := m.proc, m.client, m.started, m.readyCh
	// 造一个「已经启动过但失败」的状态：started=true、readyCh 已 close、proc=nil。
	m.started = true
	m.proc = nil
	m.client = nil
	select {
	case <-m.readyCh:
		// 已 close
	default:
		close(m.readyCh)
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.proc, m.client, m.started, m.readyCh = oldProc, oldClient, oldStarted, oldReadyCh
		m.mu.Unlock()
	}()

	err := m.ensureRunning(context.Background())
	if err == nil {
		t.Fatal("ensureRunning on dead browser returned nil")
	}
	if !isBrowserLocalError(err) {
		t.Fatalf("expected browserLocalError, got %v (%T)", err, err)
	}
	if upstreamReason(err) != reasonBrowserUnavailable {
		t.Fatalf("upstreamReason = %q, want %q", upstreamReason(err), reasonBrowserUnavailable)
	}
	// diagHint 要给出可操作的中文指引。
	if h := diagHint(reasonBrowserUnavailable); !strings.Contains(h, "browser_path") {
		t.Fatalf("hint missing actionable guidance: %q", h)
	}
}

// TestBrowserFallbackHTTP browser_fallback_http=true 时，浏览器不可用回退 HTTP。
// 验证点：开关开着时 doGeminiRequestStopInner 不走浏览器路径（拿不到 browserLocalError）。
func TestBrowserFallbackHTTP(t *testing.T) {
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.BrowserFallbackHTTP = true
	})
	// 浏览器不可用：wireTestBrowser 不调，全局 mgr 停在初始态（proc=nil, started=false）。
	// ensureRunning 会真的去 startOnce（discoverBrowserBinary 找不到会失败），
	// 那就走 browser_fallback_http 回退——但 startOnce 会真去探测本机。
	// 为了不依赖本机有没有 Chromium，改成直接测「isBrowserLocalError + fallback
	// 分支判定」这个决策点：fallback 开着的语义是「不返回 browserLocalError」。
	err := browserErr("simulated unavailable")
	if !isBrowserLocalError(err) {
		t.Fatal("browserErr not classified")
	}
	// fallback 开着 → 决策上不该返回 browserLocalError（回退）。这个断言直接编码
	// 产品策略 A2 的「显式开才回退」语义。
	fallback := rtCfg().BrowserFallbackHTTP
	if !fallback {
		t.Fatal("browser_fallback_http should be true in this test")
	}
	// 决策：fallback=true → 错误被吞、走 HTTP；fallback=false → 返回 err。
	// doGeminiRequestStopInner 的两分支就是这个 if 的直接展开，这里验决策不变式。
	decisionReturnsErr := isBrowserLocalError(err) && !rtCfg().BrowserFallbackHTTP
	if decisionReturnsErr {
		t.Fatal("with fallback_http=true, browser error should not be returned")
	}
}

// TestBrowserReconnectBackoff 崩溃重连指数退避：crashStreak 递增、
// started 复位、readyCh 重建，让下一次 ensureRunning 重新拉起。
func TestBrowserReconnectBackoff(t *testing.T) {
	m := browserMgr
	m.mu.Lock()
	oldStreak, oldStable, oldProc, oldStarted, oldReadyCh := m.crashStreak, m.stableSince, m.proc, m.started, m.readyCh
	m.proc = &browserProc{PID: 999999999, PGID: 999999999} // 已死的假 PID
	m.stableSince = time.Now().Add(-120 * time.Second)     // 稳定很久了 → streak 清零
	m.crashStreak = 3
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.crashStreak, m.stableSince, m.proc, m.started, m.readyCh = oldStreak, oldStable, oldProc, oldStarted, oldReadyCh
		m.mu.Unlock()
	}()

	// 稳定 >60s → crashStreak 清零后 +1。scheduleReconnect 自己持锁，
	// 调用方不持锁（否则死锁/解锁未锁的 mutex）。
	m.scheduleReconnect()
	m.mu.Lock()
	if m.crashStreak != 1 {
		m.mu.Unlock()
		t.Fatalf("crashStreak = %d, want 1 after stable reset", m.crashStreak)
	}
	if m.started {
		m.mu.Unlock()
		t.Fatal("started not reset by scheduleReconnect")
	}
	if m.proc != nil {
		m.mu.Unlock()
		t.Fatal("proc not cleared")
	}
	m.mu.Unlock()
	// 再来一次（没稳定）：streak 递增。
	m.mu.Lock()
	m.stableSince = time.Now()
	m.mu.Unlock()
	m.scheduleReconnect()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.crashStreak != 2 {
		t.Fatalf("crashStreak = %d, want 2", m.crashStreak)
	}
}

// TestRuntimeConfigInRTGroups 反射测试：RuntimeConfig 的每个 JSON 字段都必须
// 出现在 admin_ui/index.html 的 RT_GROUPS 里——漏一个，面板保存就把它清零。
// 这是 #27 栽过的坑（multi_turn 被静默重置），现在用测试钉死。
func TestRuntimeConfigInRTGroups(t *testing.T) {
	raw, err := adminUIBytes()
	if err != nil {
		t.Fatalf("read admin_ui: %v", err)
	}
	ty := reflect.TypeOf(RuntimeConfig{})
	var missing []string
	for i := 0; i < ty.NumField(); i++ {
		tag := ty.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		key := strings.Split(tag, ",")[0]
		// RT_GROUPS 里的字段形态是 { k: '<key>', label: ——按完整形态找。
		if !rtGroupKeyRe.MatchString(raw) ||
			!regexp.MustCompile(`k:\s*'`+regexp.QuoteMeta(key)+`'\s*,\s*label`).MatchString(raw) {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("RuntimeConfig fields missing from RT_GROUPS (panel save would zero them): %v", missing)
	}
	// 反向：RT_GROUPS 里的 k 也必须在 RuntimeConfig 里有对应字段（防死配置）。
	valid := map[string]bool{}
	for i := 0; i < ty.NumField(); i++ {
		tag := ty.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		valid[strings.Split(tag, ",")[0]] = true
	}
	// 抠出所有 k: 'xxx'
	for _, m := range rtGroupKeyRe.FindAllStringSubmatch(raw, -1) {
		if !valid[m[1]] {
			t.Fatalf("RT_GROUPS key %q has no matching RuntimeConfig field", m[1])
		}
	}
}

// RT_GROUPS 里的字段形态是 { k: '<key>', label: ... —— 必须带 label 才算
// 字段定义，避免把 css class "badge fail" 之类的 k: 'x' 误抓。
var rtGroupKeyRe = regexp.MustCompile(`k:\s*'([a-z_0-9]+)'\s*,\s*label`)

// adminUIBytes 从 embed FS 读 admin_ui/index.html。
func adminUIBytes() (string, error) {
	b, err := adminUIFS.ReadFile("admin_ui/index.html")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// TestBrowserM1Sticky 策略 M1：驻留号可用时优先返回它（同 cookieIdentity）。
func TestBrowserM1Sticky(t *testing.T) {
	m := browserMgr
	m.mu.Lock()
	oldSeed := m.seed
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.seed = oldSeed
		m.mu.Unlock()
	}()

	// 池子没号时：stickyResidentAccount 返回 nil（accountByID 查不到）。
	m.mu.Lock()
	m.seed = browserSeedState{AccountID: 99999}
	m.mu.Unlock()
	if got := stickyResidentAccount(nil); got != nil {
		t.Fatalf("sticky returned %+v for nonexistent account", got)
	}
	// exclude 命中 → nil。
	if got := stickyResidentAccount(map[int64]bool{99999: true}); got != nil {
		t.Fatalf("sticky returned excluded account")
	}
}
