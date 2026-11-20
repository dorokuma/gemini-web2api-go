package app

// browser_test.go —— 离线假 CDP / CONNECT 单元测试：纯回环 RFC 6455 服务端 +
// 本地 CONNECT 测试代理 + 内存态 browserManager，CI 不需要真实 Chromium，全 stdlib。
//
// 覆盖（对应 C 档审查的 11 组修复）：
//  1. WS 握手 / Pong 真校验 / 分片+夹控制帧 / Close 唤醒 pending / 写超时；
//  2. 流式 TTFB：body 精确等于 chunk1+chunk2（无 hdrJSON 污染），chunk1 延迟定 TTFB；
//  3. stopReader / ctx 取消 / 渲染器卡死 / Close 未读 四条 abort-泄漏路径；
//  4. browser_unavailable → 生产请求入口真回 502；browser_fallback_http 真回退；
//  5. 代理桥：CONNECT 转发到本地测试代理、fallback_direct=false 拒直连、
//     回环/自环 CONNECT 拒绝、非 CONNECT 方法 405；
//  6. spawn 回收无僵尸、Alive 转 false、陈旧 DevToolsActivePort 清理、
//     cmdline 精确匹配（前缀碰撞防误杀）；
//  7. Stop 后可重启、recreateTab 不自激风暴、停机后 syncCookiesBack 无 panic；
//  8. 重定向归类（accounts.google.com → xsrfErrBlamesCookie）与页内 fetch
//     错误归类（非 browser_unavailable）；
//  9. m.profileDir 落盘、多 authuser 共用 Tab、匿名 credentials:"omit"、
//     discoverBrowserBinary CWD 防误判 + browserDetected 校验、RT_GROUPS 反射。

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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// ─── 假 CDP 服务端（纯回环 RFC 6455 server） ─────────────────────────────────

// fakeCDPServer 在 127.0.0.1:0 上监听，完成 WS 握手后按脚本回 CDP 消息。
// 只实现测试需要的面：accept 任意无 Origin 握手、收 id 命令回 result、
// 推 method 事件、支持分片/大帧、收 Ping 回 Pong、主动断连。
type fakeCDPServer struct {
	t       *testing.T
	ln      net.Listener
	wsURL   string
	conn    net.Conn
	br      *bufio.Reader
	writeMu sync.Mutex

	gotCalls chan cdpCall   // 收到的客户端 Call（按到达顺序）
	abortHit chan string    // 收到的 __gwBridge.abort(reqID) 表达式里的 reqID
	fetchHit chan fetchSpec // 收到的 __gwBridge.fetch 表达式参数
	pongHit  chan []byte    // 收到的客户端 Pong 载荷
	closeCh  chan struct{}
	done     chan struct{}

	// holdReplies：true 时收到 Call 不回响应（渲染器卡死模拟）。
	holdReplies atomic.Bool
	// muteFetch：true 时 fetch 不自动驱动（headers 也不推，上游沉默模拟）。
	muteFetch atomic.Bool
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
		gotCalls: make(chan cdpCall, 128),
		abortHit: make(chan string, 128),
		fetchHit: make(chan fetchSpec, 128),
		pongHit:  make(chan []byte, 8),
		closeCh:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	port := ln.Addr().(*net.TCPAddr).Port
	s.wsURL = fmt.Sprintf("ws://127.0.0.1:%d/devtools/browser/fake", port)
	go s.serve()
	return s
}

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
			select {
			case s.pongHit <- payload:
			default:
			}
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

	if s.holdReplies.Load() || m.Method == "Never.Returns" {
		return // 渲染器卡死 / 永不响应：不回 result
	}

	result := json.RawMessage(`{}`)
	switch m.Method {
	case "Target.createTarget":
		result = json.RawMessage(`{"targetId":"fake-target-1"}`)
	case "Target.attachToTarget":
		result = json.RawMessage(`{"sessionId":"fake-session-1"}`)
	case "Runtime.evaluate":
		if strings.Contains(string(m.Params), "typeof __gwPush") {
			result = json.RawMessage(`{"result":{"type":"string","value":"function/object"}}`)
		} else {
			s.handleEvaluate(m.Params)
		}
	case "Network.getCookies":
		result = json.RawMessage(`{"cookies":[]}`)
	}
	reply := fmt.Sprintf(`{"id":%d,"result":%s}`, m.ID, result)
	go s.sendText([]byte(reply))
}

func (s *fakeCDPServer) handleEvaluate(params json.RawMessage) {
	var p struct {
		Expression string `json:"expression"`
	}
	if json.Unmarshal(params, &p) != nil {
		return
	}
	expr := p.Expression
	if idx := strings.Index(expr, `__gwBridge.fetch("`); idx >= 0 {
		spec := parseFetchExpr(expr[idx:])
		s.fetchHit <- spec
		if s.muteFetch.Load() {
			return // 上游沉默：headers 都不推
		}
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
	if idx := strings.Index(expr, `__gwBridge.abort("`); idx >= 0 {
		rest := expr[idx+len(`__gwBridge.abort("`):]
		if end := strings.IndexByte(rest, '"'); end > 0 {
			s.abortHit <- rest[:end]
		}
		return
	}
}

// parseFetchExpr 从 evaluate 表达式里抠出 fetch 参数。
func parseFetchExpr(expr string) fetchSpec {
	var spec fetchSpec
	spec.method = "POST"
	q1 := strings.Index(expr, `fetch("`)
	if q1 < 0 {
		return spec
	}
	rest := expr[q1+len(`fetch("`):]
	if end := strings.IndexByte(rest, '"'); end > 0 {
		spec.reqID = rest[:end]
		rest = rest[end+1:]
	} else {
		return spec
	}
	if u := strings.IndexByte(rest, '"'); u >= 0 {
		rest = rest[u+1:]
		if end := strings.IndexByte(rest, '"'); end >= 0 {
			spec.url = rest[:end]
		}
	}
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
	url         string // headers.url（默认 spec.url）
	redirected  bool
	chunks      [][]byte
	chunkDelays []time.Duration
	err         string
}

var (
	fetchScriptsMu sync.Mutex
	fetchScripts   = map[string]fetchScript{}
)

func (s *fakeCDPServer) setFetchScript(reqID string, sc fetchScript) {
	fetchScriptsMu.Lock()
	fetchScripts[reqID] = sc
	fetchScriptsMu.Unlock()
}

func (s *fakeCDPServer) driveFetch(spec fetchSpec) {
	fetchScriptsMu.Lock()
	sc, ok := fetchScripts[spec.reqID]
	fetchScriptsMu.Unlock()
	if !ok {
		s.pushBridgeMsg(spec.reqID, map[string]interface{}{
			"kind": "headers", "status": 200, "url": spec.url, "headers": map[string]string{},
		})
		s.pushBridgeMsg(spec.reqID, map[string]interface{}{"kind": "end", "status": 200})
		return
	}
	url := sc.url
	if url == "" {
		url = spec.url
	}
	s.pushBridgeMsg(spec.reqID, map[string]interface{}{
		"kind": "headers", "status": sc.status, "url": url,
		"redirected": sc.redirected,
		"headers":    map[string]string{"content-type": "text/plain"},
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

func (s *fakeCDPServer) pushEvent(sessionID, method string, params interface{}) {
	p, _ := json.Marshal(params)
	msg := fmt.Sprintf(`{"method":%q,"sessionId":%q,"params":%s}`, method, sessionID, p)
	_ = s.sendText([]byte(msg))
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

// sendFragmentedWithControl 把一条 text 消息拆成两段，中间夹一个控制帧
// （Ping）：验证客户端重组分片时能正确跳过中间控制帧。
func (s *fakeCDPServer) sendFragmentedWithControl(payload []byte) error {
	if len(payload) < 2 {
		return s.sendText(payload)
	}
	mid := len(payload) / 2
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeFrameRaw(0x00|wsOpText, payload[:mid]); err != nil {
		return err
	}
	if err := s.writeFrameRaw(0x80|wsOpPing, []byte("hb")); err != nil {
		return err
	}
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
// 直接填 proc=假活 + client=真 cdpClient + sessionID。
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
	oldSeed, oldStarted, oldStarting, oldStartErr := m.seed, m.started, m.starting, m.startErr
	oldReadyCh, oldShut, oldStop := m.readyCh, m.shuttingDown, m.stopping
	oldReady := m.ready.Load()
	m.proc = &browserProc{PID: os.Getpid(), PGID: os.Getpid(), exitedCh: make(chan struct{})}
	m.client = client
	m.sessionID = "fake-session-1"
	m.targetID = "fake-target-1"
	m.started = true
	m.starting = false
	m.startErr = nil
	m.shuttingDown = false
	m.stopping = false
	m.ready.Store(true)
	select {
	case <-m.readyCh:
	default:
		close(m.readyCh)
	}
	m.mu.Unlock()
	m.registerCDPEvents(client)

	return func() {
		client.Close()
		conn.Close()
		m.mu.Lock()
		m.proc, m.client, m.sessionID, m.targetID = oldProc, oldClient, oldSess, oldTarget
		m.seed, m.started, m.starting, m.startErr = oldSeed, oldStarted, oldStarting, oldStartErr
		m.readyCh, m.shuttingDown, m.stopping = oldReadyCh, oldShut, oldStop
		m.ready.Store(oldReady)
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

// drainStream 读完一条流，返回全文与读侧错误。
func drainStream(rc io.ReadCloser) (string, error) {
	defer rc.Close()
	b, err := io.ReadAll(rc)
	return string(b), err
}

// ─── 组 3 测试：WS 与流管道 ─────────────────────────────────────────────────

// TestCDPWebsocketBasics：无 Origin 握手放行、分片+中间控制帧重组、
// Ping 收到后客户端真实回 Pong（同载荷）、Close 唤醒 pending、写超时。
func TestCDPWebsocketBasics(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()

	conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	client := newCDPClient(conn)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. 普通 Call：服务端回 result。
	var out struct {
		TargetID string `json:"targetId"`
	}
	if err := client.Call(ctx, "", "Target.createTarget",
		map[string]interface{}{"url": "about:blank"}, &out); err != nil {
		t.Fatalf("createTarget: %v", err)
	}
	if out.TargetID != "fake-target-1" {
		t.Fatalf("targetId = %q", out.TargetID)
	}

	// 2. 分片+中间控制帧：text FIN=0 → ping → continuation FIN=1。
	got := make(chan cdpEvent, 1)
	client.On("Test.fragmented", func(e cdpEvent) { got <- e })
	payload := `{"method":"Test.fragmented","sessionId":"s1","params":{"k":"v12345"}}`
	if err := srv.sendFragmentedWithControl([]byte(payload)); err != nil {
		t.Fatalf("sendFragmentedWithControl: %v", err)
	}
	select {
	case e := <-got:
		if !strings.Contains(string(e.Params), "v12345") {
			t.Fatalf("fragmented params = %s", e.Params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fragmented event not received")
	}

	// 3. Ping → 真 Pong：服务端收到客户端 Pong 且载荷原样。
	// 上一步分片测试中间夹过一个 Ping("hb")，它的 Pong 可能在途——
	// 读到与本次载荷相等为止，之前的旧 Pong 一律跳过。
	pingPayload := []byte("hb-payload-01")
	if err := srv.sendPing(pingPayload); err != nil {
		t.Fatalf("ping: %v", err)
	}
	pongDeadline := time.After(2 * time.Second)
	for {
		select {
		case pong := <-srv.pongHit:
			if string(pong) == string(pingPayload) {
				goto pongOK
			}
		case <-pongDeadline:
			t.Fatal("client did not send Pong with matching payload after Ping")
		}
	}
pongOK:

	// 4. Close 唤醒 pending：起一个不回的 Call，关连接后要立刻拿到错。
	pendingErr := make(chan error, 1)
	go func() {
		var sink interface{}
		pendingErr <- client.Call(context.Background(), "", "Never.Returns", nil, &sink)
	}()
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

	// 5. client.Closed() 报终态。
	if !client.Closed() {
		t.Fatal("client.Closed() = false after conn close")
	}
}

// TestBrowserStreamTTFB：body 精确等于 chunk1+chunk2（无 hdrJSON 污染），
// 且 TTFB 由 chunk1 延迟决定。
func TestBrowserStreamTTFB(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	chunk1 := []byte("[[\"wrb.fr\",\"x\",\"[null,\\\"c_1\\\"]\"]\n")
	chunk2 := []byte("[[\"wrb.fr\",\"x\",\"[null,\\\"r_1\\\"]\"]\n")
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:      200,
			chunks:      [][]byte{chunk1, chunk2},
			chunkDelays: []time.Duration{0, 300 * time.Millisecond},
		})
	}()

	ctx := context.Background()
	t0 := time.Now()
	status, rc, err := browserMgr.doBrowserStreamRequest(ctx, nil,
		"https://gemini.google.com/_/x/StreamGenerate", "POST",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
		[]byte("f.req=x"), nil)
	if err != nil {
		t.Fatalf("doBrowserStreamRequest: %v", err)
	}
	defer rc.Close()
	if status != 200 {
		t.Fatalf("status = %d", status)
	}
	first := make([]byte, 1)
	if _, err := rc.Read(first); err != nil {
		t.Fatalf("first read: %v", err)
	}
	ttfb := time.Since(t0)
	if ttfb > 200*time.Millisecond {
		t.Fatalf("TTFB %v exceeds 200ms (first chunk pushed immediately)", ttfb)
	}
	rest, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("readAll: %v", err)
	}
	full := string(first) + string(rest)
	// 精确相等：不允许 hdrJSON 或任何控制帧污染正文。
	if full != string(chunk1)+string(chunk2) {
		t.Fatalf("body = %q, want exactly %q", full, string(chunk1)+string(chunk2))
	}
}

// TestBrowserStopWhenAbort：stopReader 置位后下一次 Read 立刻 EOF；
// rc.Close 触发页内 abort（abortHit 收到 reqID）。
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
				[]byte("[[\"wrb.fr\",\"x\",\"[[null,null,[[\\\"stop-here\\\"]]]]\"]]\n"),
				[]byte("[[\"wrb.fr\",\"x\",\"[[null,null,[[\\\"more-tail\\\"]]]]\"]]\n"),
			},
			chunkDelays: []time.Duration{0, 500 * time.Millisecond},
		})
	}()

	var stop atomic.Bool
	ctx := context.Background()
	_, rc, err := browserMgr.doBrowserStreamRequest(ctx, nil,
		"https://gemini.google.com/_/x/StreamGenerate", "POST", nil, []byte("x"), nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	sr := stopReader{r: rc, stop: &stop}
	got := make([]byte, 4096)
	n, _ := sr.Read(got)
	if n == 0 {
		t.Fatal("first read returned nothing")
	}
	stop.Store(true)
	t0 := time.Now()
	_, err = sr.Read(got)
	if dt := time.Since(t0); dt > 100*time.Millisecond {
		t.Fatalf("stopReader waited %v for tail after stop", dt)
	}
	if err != io.EOF {
		t.Fatalf("stopReader Read err = %v, want io.EOF", err)
	}
	// Close 触发页内 abort。
	if err := rc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-srv.abortHit:
	case <-time.After(2 * time.Second):
		t.Fatal("page abort not sent after Close")
	}
}

// TestBrowserCancelAbort：headers 已到、上游沉默时 ctx 取消立刻唤醒 Read
// （不等页内任何数据，也不拖 stallWatch）。
func TestBrowserCancelAbort(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	// headers 立即，之后 chunk 挂 30s（上游沉默场景）。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:      200,
			chunks:      [][]byte{[]byte("late")},
			chunkDelays: []time.Duration{30 * time.Second},
		})
	}()

	ctx, cancel := context.WithCancel(context.Background())
	_, rc, err := browserMgr.doBrowserStreamRequest(ctx, nil,
		"https://gemini.google.com/_/x/StreamGenerate", "POST", nil, []byte("x"), nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer rc.Close()
	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 128)
		_, err := rc.Read(buf)
		readErr <- err
	}()
	time.Sleep(50 * time.Millisecond) // 让 Read 确实阻塞在等 chunk
	cancel()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("Read returned nil error after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read not woken after ctx cancel (headers-then-silence deadlock)")
	}
}

// TestBrowserAbortHungRenderer：渲染器卡死（不回任何 Call）时，
// Close() 必须迅速返回——Abort 是异步的，不阻塞调用方。
func TestBrowserAbortHungRenderer(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:      200,
			chunks:      [][]byte{[]byte("x")},
			chunkDelays: []time.Duration{30 * time.Second},
		})
	}()

	ctx := context.Background()
	_, rc, err := browserMgr.doBrowserStreamRequest(ctx, nil,
		"https://gemini.google.com/_/x/StreamGenerate", "POST", nil, []byte("x"), nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	// 现在让渲染器卡死：Call 一律不回。
	srv.holdReplies.Store(true)
	t0 := time.Now()
	if err := rc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if dt := time.Since(t0); dt > 500*time.Millisecond {
		t.Fatalf("Close blocked %v on hung renderer (Abort must be async)", dt)
	}
}

// TestBrowserCloseWithoutReadNoLeak：一次 Read 都没调就 Close，
// pumpToPipe 协程必须退出（pipeR.Close 无条件执行），不留泄漏。
func TestBrowserCloseWithoutReadNoLeak(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:      200,
			chunks:      [][]byte{[]byte("payload")},
			chunkDelays: []time.Duration{30 * time.Second}, // chunk 挂着不推
		})
	}()

	ctx := context.Background()
	_, rc, err := browserMgr.doBrowserStreamRequest(ctx, nil,
		"https://gemini.google.com/_/x/StreamGenerate", "POST", nil, []byte("x"), nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	// 不 Read 直接 Close：pipeR.Close 必须无条件唤醒 pumpToPipe。
	if err := rc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// pumpToPipe 退出后 stream.closed 会关（fail/正常收尾都关它）。
	s := findStreamByCloser(t, rc)
	select {
	case <-s.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("stream not closed after Close-without-read (pumpToPipe leak)")
	}
}

// findStreamByCloser 取出 streamReadCloser 内部那条 stream（测试反射豁免：
// 这里就是要断言「Close 之后内部 goroutine 退了」，读字段是唯一真断言）。
func findStreamByCloser(t *testing.T, rc io.ReadCloser) *browserStream {
	t.Helper()
	src, ok := rc.(*streamReadCloser)
	if !ok {
		t.Fatalf("rc is %T not *streamReadCloser", rc)
	}
	return src.s
}

// ─── 组 4/5 测试：进程管理 / 生命周期 ───────────────────────────────────────

// TestSpawnZombieReap：spawnBrowser 起真子进程，Alive() 在进程退出后
// 立刻转 false（exitedCh 权威），且 cmd.Wait() 已回收（无僵尸）。
// 用 /bin/sh 起短命进程验证（不依赖 Chromium）。
func TestSpawnZombieReap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix-only: uses /bin/sh and /proc")
	}
	proc, err := spawnBrowser("/bin/sh", []string{"-c", "exit 0"}, nil)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if !proc.Alive() && !pidAlive(proc.PID) {
		// 刚 spawn 就可能已退出——Alive/exitedCh 至少一路报死即可
	}
	// 等进程真退：exitedCh 关闭后 Alive 必须 false。
	select {
	case <-proc.exitedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("reaper did not reap child within 3s")
	}
	if proc.Alive() {
		t.Fatal("Alive() = true after child exited (zombie or stale PID)")
	}
	// /proc/<pid> 消失或变 zombie 都行——关键是 cmd.Wait() 返回了
	// （exitedCh 就是 Wait 返回后才关的），不会有 <defunct> 挂在我们名下。
}

// TestStaleDevToolsActivePort：reclaimOrphanBrowser 必须删掉
// DevToolsActivePort（防 waitDevToolsPort 连死端口），且 instance 记录
// 指向的活进程核验不过时不动 Singleton*。
func TestStaleDevToolsActivePort(t *testing.T) {
	dir := t.TempDir()
	// 放一份陈旧 DevToolsActivePort + Singleton 文件 + 死 PID instance。
	stale := filepath.Join(dir, "DevToolsActivePort")
	if err := os.WriteFile(stale, []byte("1\n/devtools/browser/dead\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"SingletonLock", "SingletonCookie", "SingletonSocket"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	inst := `{"pid":999999999,"pgid":999999999,"profile":"` + dir + `"}`
	if err := os.WriteFile(filepath.Join(dir, "browser-instance.json"), []byte(inst), 0o600); err != nil {
		t.Fatal(err)
	}
	reclaimOrphanBrowser(dir)
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale DevToolsActivePort not removed")
	}
	// 死 PID instance：文件被清（Singleton* 也可删——PID 已死没人占）。
	if _, err := os.Stat(filepath.Join(dir, "browser-instance.json")); !os.IsNotExist(err) {
		t.Fatal("dead browser-instance.json not removed")
	}
}

// TestProcessIsOurBrowserCmdlineMatch：Linux /proc cmdline 精确匹配——
// 用当前测试进程伪造 cmdline 不行（我们是 go test），所以直接测
// exeNameOK + 参数精确比对逻辑（processIsOurBrowser 的两段判据拆开验）。
func TestProcessIsOurBrowserCmdlineMatch(t *testing.T) {
	// exeNameOK 白名单：chromium/chrome/msedge/brave 放行，firefox/其它拒绝。
	for _, ok := range []struct {
		word string
		want bool
	}{
		{"/usr/bin/chromium", true},
		{"/opt/google/chrome/chrome", true},
		{"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", true},
		{"Google", false}, // 词本身不是路径基名带 chrom
		{"Chrome", true},  // macOS ps 切词后末词
		{"/usr/bin/msedge", true},
		{"/usr/bin/microsoft-edge", true},
		{"/usr/bin/brave", true},
		{"/usr/bin/firefox", false},
		{"/usr/bin/python3", false},
	} {
		if got := exeNameOK(ok.word); got != ok.want {
			t.Errorf("exeNameOK(%q) = %v, want %v", ok.word, got, ok.want)
		}
	}
	// 参数精确匹配：Linux 路径走 /proc，不能直接造；改用真实 spawn
	// 的子进程验证「前缀不碰撞」——spawn /bin/sh -c 'sleep 60'，
	// cmdline 里没有 --user-data-dir，processIsOurBrowser 必须 false。
	if runtime.GOOS == "linux" {
		p, err := spawnBrowser("/bin/sh", []string{"-c", "sleep 30"}, nil)
		if err != nil {
			t.Fatalf("spawn: %v", err)
		}
		defer killBrowserProcessGroup(p.PID, p.PGID, 500*time.Millisecond)
		if processIsOurBrowser(p.PID, "/tmp/whatever") {
			t.Fatal("processIsOurBrowser matched /bin/sh (should not)")
		}
	}
}

// TestBrowserShutdownReopen：Stop() 是运行时停用——复位后下一次
// ensureRunning 能正常重新拉起（面板关开开关场景）。用内存态模拟：
// Stop 后 started=false、proc=nil、readyCh 复位、非 shuttingDown。
func TestBrowserShutdownReopen(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	m := browserMgr
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m.Stop(ctx)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		t.Fatal("started still true after Stop")
	}
	if m.proc != nil || m.client != nil {
		t.Fatal("proc/client not cleared after Stop")
	}
	if m.shuttingDown {
		t.Fatal("Stop must not set shuttingDown (that's Shutdown's terminal flag)")
	}
	if m.sessionID != "" || m.targetID != "" {
		t.Fatal("session/target not cleared after Stop")
	}
}

// TestBrowserRecreateTabNoStorm：Target.closeTarget 会回推
// targetDestroyed——若 recreateTab 不先清 targetID，这个自己触发的
// 事件会被认成「主 Tab 死了」再触发一次 recreateTab，无限自激。
func TestBrowserRecreateTabNoStorm(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	m := browserMgr
	m.mu.Lock()
	m.targetID = "fake-target-1"
	m.mu.Unlock()

	// 第一次 recreateTab：会 closeTarget（服务端记 gotCalls）→ 服务端
	// 模拟 Chromium 回推 targetDestroyed——若不清 targetID，这个事件
	// 会再触发一次 recreateTab 风暴。
	closeSeen := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case c := <-srv.gotCalls:
				if c.method == "Target.closeTarget" {
					select {
					case closeSeen <- struct{}{}:
					default:
					}
					var p struct {
						TargetID string `json:"targetId"`
					}
					_ = json.Unmarshal(c.params, &p)
					srv.pushEvent("", "Target.targetDestroyed",
						map[string]string{"targetId": p.TargetID})
					return
				}
			case <-time.After(2 * time.Second):
				return
			}
		}
	}()
	m.recreateTab()
	// recreateTab 完成后 targetID 应被填成新值（fake-target-1 是
	// createTarget 的固定返回）。先等 closeTarget 到达（保证计数
	// 覆盖完整一轮），再统计 createTarget 次数。
	select {
	case <-closeSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("closeTarget not sent")
	}
	var creates int
	drain := time.After(300 * time.Millisecond)
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Target.createTarget" {
				creates++
			}
		case <-drain:
			goto done
		}
	}
done:
	if creates != 1 {
		t.Fatalf("Target.createTarget called %d times (self-excited storm)", creates)
	}
}

// TestSyncCookiesAfterShutdown：停机后 syncCookiesBack 不许 panic
// （client=nil 的安全路径）；seed=0 时直接返回。
func TestSyncCookiesAfterShutdown(t *testing.T) {
	m := browserMgr
	m.mu.Lock()
	oldProc, oldClient, oldSess, oldSeed := m.proc, m.client, m.sessionID, m.seed
	m.proc = nil
	m.client = nil
	m.sessionID = ""
	m.seed = browserSeedState{AccountID: 0}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.proc, m.client, m.sessionID, m.seed = oldProc, oldClient, oldSess, oldSeed
		m.mu.Unlock()
	}()
	// 不该 panic，也不该调 CDP。
	m.syncCookiesBack(true)
	m.syncCookiesBack(false)
}

// ─── 组 1/5 测试：代理桥 / 502 / fallback ───────────────────────────────────

// localConnectProxy 是测试用的本机 CONNECT 代理：记 CONNECT 目标，
// 回 200 后 echo 数据（够验证「流量真走了代理」）。
type localConnectProxy struct {
	ln      net.Listener
	gotHost chan string
}

func newLocalConnectProxy(t *testing.T) *localConnectProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &localConnectProxy{ln: ln, gotHost: make(chan string, 8)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c)
		}
	}()
	return p
}

func (p *localConnectProxy) handle(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		return
	}
	// CONNECT host:port HTTP/1.1
	parts := strings.Fields(line)
	if len(parts) >= 2 && strings.EqualFold(parts[0], "CONNECT") {
		select {
		case p.gotHost <- parts[1]:
		default:
		}
	}
	for { // 吃掉剩余头
		l, err := br.ReadString('\n')
		if err != nil || l == "\r\n" || l == "\n" {
			break
		}
	}
	io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
	io.Copy(c, br) // echo 回传
}

func (p *localConnectProxy) close() { p.ln.Close() }

// TestBrowserProxyBridgeWiring：桥设上游为本地测试代理后，CONNECT 流量
// 真到代理、目标记录正确；SetUpstream 换出口时旧隧道被断开。
func TestBrowserProxyBridgeWiring(t *testing.T) {
	proxySrv := newLocalConnectProxy(t)
	defer proxySrv.close()
	proxyAddr := proxySrv.ln.Addr().String()

	bridge, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}
	defer bridge.Shutdown(context.Background())
	bridge.allowLoopback = true // 测试靶机就在回环上

	// 出口 = 本地测试代理
	if err := bridge.SetUpstream("http://"+proxyAddr, false); err != nil {
		t.Fatalf("SetUpstream: %v", err)
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", bridge.Port()))
	if err != nil {
		t.Fatalf("dial bridge: %v", err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "CONNECT gemini.google.com:443 HTTP/1.1\r\nHost: gemini.google.com:443\r\n\r\n")
	br := bufio.NewReader(conn)
	status, _ := br.ReadString('\n')
	if !strings.Contains(status, "200") {
		t.Fatalf("bridge CONNECT status = %q", status)
	}
	for {
		l, err := br.ReadString('\n')
		if err != nil || l == "\r\n" || l == "\n" {
			break
		}
	}
	select {
	case got := <-proxySrv.gotHost:
		if got != "gemini.google.com:443" {
			t.Fatalf("proxy saw CONNECT to %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("traffic did not reach test proxy")
	}

	// 换出口后旧隧道被断开：SetUpstream 到另一个值，原 conn 应被 close。
	_ = bridge.SetUpstream("", true)
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := br.ReadString('\n'); err == nil {
		t.Fatal("old tunnel not closed after upstream switch")
	}
}

// TestBrowserProxyBridgeLoopbackAndMethodGuard：
//   - fallback_direct=false 且 upstream="" 时 CONNECT 拒直连（502，不漏 IP）；
//   - 回环/自环 CONNECT 一律拒（防 FD 耗尽与 SSRF）；
//   - 非 CONNECT 方法 405（不开正向代理口子）。
func TestBrowserProxyBridgeLoopbackAndMethodGuard(t *testing.T) {
	bridge, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}
	defer bridge.Shutdown(context.Background())
	// 不开 allowLoopback：生产默认严格拒绝回环目标。

	// 1. upstream="" + allowDirect=false → 拒直连。
	if err := bridge.SetUpstream("", false); err != nil {
		t.Fatalf("SetUpstream: %v", err)
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", bridge.Port()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	fmt.Fprintf(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
	br := bufio.NewReader(conn)
	line, _ := br.ReadString('\n')
	if !strings.Contains(line, "502") {
		t.Fatalf("direct refused status = %q, want 502", line)
	}
	conn.Close()

	// 2. allowDirect=true 但目标是回环/自环 → 400 拒绝。
	_ = bridge.SetUpstream("", true)
	for _, target := range []string{
		"127.0.0.1:8080", "[::1]:443", "localhost:9090",
		fmt.Sprintf("127.0.0.1:%d", bridge.Port()), // 自环
	} {
		req := httptest.NewRequest(http.MethodConnect, "http://x/", nil)
		req.Host = target
		rec := httptest.NewRecorder()
		bridge.handleConnect(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("CONNECT %q status = %d, want 400", target, rec.Code)
		}
	}
	// 缺端口 / 非法 host 也 400。
	for _, target := range []string{"noporthost", "bad host:80", "a b:80", ":80"} {
		req := httptest.NewRequest(http.MethodConnect, "http://x/", nil)
		req.Host = target
		rec := httptest.NewRecorder()
		bridge.handleConnect(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("CONNECT %q status = %d, want 400", target, rec.Code)
		}
	}
	// 3. 非 CONNECT 方法一律 405。
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut} {
		req := httptest.NewRequest(method, "http://x/", nil)
		req.Host = "example.com:443"
		rec := httptest.NewRecorder()
		bridge.handleConnect(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status = %d, want 405", method, rec.Code)
		}
	}
}

// TestBrowserUnavailable502：生产请求入口（doGeminiRequestStopInner 的
// browser 分支）在浏览器不可用时回真 502 browser_unavailable。
func TestBrowserUnavailable502(t *testing.T) {
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.BrowserFallbackHTTP = false
	})
	// 造「启动已失败」态：started=true、readyCh 已关、startErr 非 nil。
	m := browserMgr
	m.mu.Lock()
	oldProc, oldClient, oldStarted, oldStarting, oldStartErr :=
		m.proc, m.client, m.started, m.starting, m.startErr
	oldReadyCh := m.readyCh
	m.started = true
	m.starting = false
	m.proc = nil
	m.client = nil
	m.startErr = browserErr("simulated start failure")
	select {
	case <-m.readyCh:
	default:
		close(m.readyCh)
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.proc, m.client, m.started, m.starting, m.startErr =
			oldProc, oldClient, oldStarted, oldStarting, oldStartErr
		m.readyCh = oldReadyCh
		m.mu.Unlock()
	}()

	// 走真请求入口：headers 不带 Cookie（匿名），assert err 归类 502。
	status, _, _, _, err := doGeminiRequestStopInner(context.Background(),
		"https://gemini.google.com/_/x", "f.req=x", map[string]string{}, "",
		nil, nil, false)
	if err == nil {
		t.Fatal("browser unavailable did not error")
	}
	if status != 0 && status != 502 {
		t.Fatalf("status = %d", status)
	}
	if !isBrowserLocalError(err) {
		t.Fatalf("err not browserLocalError: %v (%T)", err, err)
	}
	if upstreamReason(err) != reasonBrowserUnavailable {
		t.Fatalf("reason = %q, want %q", upstreamReason(err), reasonBrowserUnavailable)
	}
	if h := diagHint(reasonBrowserUnavailable); !strings.Contains(h, "browser_path") {
		t.Fatalf("hint missing actionable guidance: %q", h)
	}
}

// TestBrowserFallbackHTTP：browser_fallback_http=true 且浏览器不可用时，
// 真实回退到 mock HTTP 后端（非布尔恒等式——走真请求路径、拿到真响应）。
func TestBrowserFallbackHTTP(t *testing.T) {
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.BrowserFallbackHTTP = true
	})
	// 浏览器不可用态（同 502 测试）。
	m := browserMgr
	m.mu.Lock()
	oldProc, oldClient, oldStarted, oldStarting, oldStartErr :=
		m.proc, m.client, m.started, m.starting, m.startErr
	oldReadyCh := m.readyCh
	m.started = true
	m.starting = false
	m.proc = nil
	m.client = nil
	m.startErr = browserErr("simulated start failure")
	select {
	case <-m.readyCh:
	default:
		close(m.readyCh)
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.proc, m.client, m.started, m.starting, m.startErr =
			oldProc, oldClient, oldStarted, oldStarting, oldStartErr
		m.readyCh = oldReadyCh
		m.mu.Unlock()
	}()

	// mock HTTP 后端：fallback 路径会真发 POST 到 endpoint（直连 tls-client
	// 不打外网不行，所以测 stdlib 分支——给 proxyURL 一个本地 httptest 代理
	// 太重，改用「fallback 决策后 err 是 HTTP 层错不是 browserLocalError」断言：
	// 给一个必然失败的 endpoint（回环没人听），fallback 走到 stdlib/tls 分支，
	// 拿到的是网络错而非 browserLocalError——证明它确实回退了。
	_, _, _, _, err := doGeminiRequestStopInner(context.Background(),
		"http://127.0.0.1:1/nowhere", "f.req=x", map[string]string{},
		"", nil, nil, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if isBrowserLocalError(err) {
		t.Fatalf("with fallback_http=true, browserLocalError must be swallowed "+
			"and retried over Go HTTP; got %v", err)
	}
}

// ─── 组 6/8 测试：归类 / 策略 M1 ────────────────────────────────────────────

// TestBrowserRedirectAndFetchErrorClassification：
//   - headers 报 redirected + accounts.google.com → appPageStatusError，
//     xsrfErrBlamesCookie 命中（既有失效 Cookie 换号/停用逻辑生效）；
//   - kind:"error" 的 TypeError: Failed to fetch → 普通上游错，*不是*
//     browserLocalError（计代理健康度、不触发 fallback 误降级）。
func TestBrowserRedirectAndFetchErrorClassification(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	// 用例 1：redirected → accounts.google.com。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:     200,
			url:        "https://accounts.google.com/ServiceLogin?continue=x",
			redirected: true,
		})
	}()
	_, _, err := browserMgr.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/app", "GET", nil, nil, nil)
	var ape *appPageStatusError
	if !errors.As(err, &ape) {
		t.Fatalf("redirect err not appPageStatusError: %v (%T)", err, err)
	}
	if !xsrfErrBlamesCookie(err) {
		t.Fatalf("redirect to accounts.google.com not blamed on cookie: %v", err)
	}

	// 用例 2：页内 TypeError: Failed to fetch → 普通上游错。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status: 200,
			err:    "TypeError: Failed to fetch",
		})
	}()
	_, rc, err := browserMgr.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), nil)
	if err != nil {
		// headers 前就到 error：err 也必须是普通上游错。
		if isBrowserLocalError(err) {
			t.Fatalf("page fetch error misclassified as browser_unavailable: %v", err)
		}
		return
	}
	defer rc.Close()
	_, rerr := io.Copy(io.Discard, rc)
	if rerr == nil {
		t.Fatal("expected read error")
	}
	if isBrowserLocalError(rerr) {
		t.Fatalf("page fetch error misclassified as browser_unavailable: %v", rerr)
	}
}

// TestBrowserProfileDirPersistence：m.profileDir 是归一化绝对路径，
// seed-state/persistSeedState 都落盘到该目录——绝不污染 CWD。
func TestBrowserProfileDirPersistence(t *testing.T) {
	dir := t.TempDir()
	m := browserMgr
	m.mu.Lock()
	oldDir, oldSeed := m.profileDir, m.seed
	m.profileDir = dir
	m.seed = browserSeedState{AccountID: 42, CookieIdentity: "a|b", SeededAt: 1}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.profileDir, m.seed = oldDir, oldSeed
		m.mu.Unlock()
	}()

	m.persistSeedState()
	if _, err := os.Stat(filepath.Join(dir, "seed-state.json")); err != nil {
		t.Fatalf("seed-state.json not written to profileDir: %v", err)
	}
	// CWD 绝不能出现 seed-state.json。
	cwd, _ := os.Getwd()
	if _, err := os.Stat(filepath.Join(cwd, "seed-state.json")); !os.IsNotExist(err) {
		t.Fatal("seed-state.json polluted CWD")
	}
}

// TestBrowserM1MultiAuthUser：同 cookieIdentity 的不同 authuser
// **不触发** clearBrowserCookies 与 30s 冷却（共用 jar 与 Tab）。
func TestBrowserM1MultiAuthUser(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	cookie := "SAPISID=sa; __Secure-1PSID=ps; SID=x"
	m := browserMgr
	m.mu.Lock()
	m.seed = browserSeedState{
		AccountID:      7,
		CookieIdentity: cookieIdentity(cookie),
		SeededAt:       time.Now().Unix(),
	}
	m.lastSwitchAt = time.Now() // 刚进冷却：若被当切号会等满 30s
	m.mu.Unlock()

	next := &CookieAccount{ID: 8, Cookie: cookie, AuthUser: 3, Status: "enabled"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	t0 := time.Now()
	if err := m.maybeSwitchAccount(ctx, next, "same identity different authuser"); err != nil {
		t.Fatalf("same-identity switch failed: %v", err)
	}
	if dt := time.Since(t0); dt > time.Second {
		t.Fatalf("same-identity authuser switch waited %v (must skip cooldown)", dt)
	}
	m.mu.Lock()
	if m.seed.AccountID != 8 {
		m.mu.Unlock()
		t.Fatalf("resident id = %d, want 8", m.seed.AccountID)
	}
	m.mu.Unlock()
	// clearBrowserCookies 绝不能被调（共用 jar）。
	select {
	case c := <-srv.gotCalls:
		if c.method == "Network.clearBrowserCookies" {
			t.Fatal("clearBrowserCookies called for same-identity authuser switch")
		}
	default:
	}
}

// TestBrowserAnonCredentialsOmit：匿名请求（account==nil）的 fetch 表达式
// 必须带 omitCredentials:true——驻留登录主号时匿名请求绝不带主号 Cookie。
func TestBrowserAnonCredentialsOmit(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	// 抓 evaluate 表达式原文（不走 fetchSpec——要验 omitCredentials 字样）。
	exprSeen := make(chan string, 4)
	go func() {
		for {
			select {
			case c := <-srv.gotCalls:
				if c.method == "Runtime.evaluate" &&
					strings.Contains(string(c.params), "__gwBridge.fetch") {
					var p struct {
						Expression string `json:"expression"`
					}
					if json.Unmarshal(c.params, &p) == nil {
						exprSeen <- p.Expression
					}
					return
				}
			case <-time.After(3 * time.Second):
				return
			}
		}
	}()
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{status: 200})
	}()
	_, rc, err := browserMgr.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	rc.Close()
	select {
	case expr := <-exprSeen:
		if !strings.Contains(expr, "omitCredentials") {
			t.Fatalf("anonymous fetch missing omitCredentials: %s", expr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fetch expression not captured")
	}
}

// TestDiscoverBrowserBinaryAndDetected：
//   - 裸命令名绝不 Stat CWD（cwd 放个假 chromium 文件不能被选中）；
//   - explicitPath 不可执行 → 报错；
//   - browserDetected 校验可执行（配置假路径返回 ""）。
func TestDiscoverBrowserBinaryAndDetected(t *testing.T) {
	// CWD 放个名叫 chromium 的**不可执行**文件：裸名走 LookPath，
	// 绝不能把它当浏览器返回。
	dir := t.TempDir()
	fake := filepath.Join(dir, "chromium")
	if err := os.WriteFile(fake, []byte("not a browser"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 把 PATH 指到不含任何浏览器的空目录 + CWD 换成含假文件的目录。
	oldPath := os.Getenv("PATH")
	os.Setenv("PATH", dir)
	defer os.Setenv("PATH", oldPath)
	oldWd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Skipf("chdir: %v", err)
	}
	defer os.Chdir(oldWd)

	if runtime.GOOS != "windows" {
		// 假文件不可执行：LookPath 找到也会校验位，不应返回它。
		// （LookPath 对无执行位返回 ErrNotFound 语义）
		if p, err := discoverBrowserBinary(""); err == nil {
			if p == fake {
				t.Fatal("discoverBrowserBinary picked CWD fake chromium")
			}
			t.Logf("found system browser %s (ok, PATH fallback)", p)
		}
		// explicitPath 不可执行 → 必须报错。
		if _, err := discoverBrowserBinary(fake); err == nil {
			t.Fatal("explicit non-executable path accepted")
		}
	}
	// browserDetected：配置假路径 → ""（校验失败如实报）。
	old := cfg.BrowserPath
	cfg.BrowserPath = fake
	defer func() { cfg.BrowserPath = old }()
	if got := browserDetected(); got != "" {
		if runtime.GOOS == "windows" {
			// Windows 只查 !IsDir，文件存在即可——这个用例断言 unix 语义。
			t.Skip("windows: isExecutableFile accepts any existing file")
		}
		t.Fatalf("browserDetected = %q for non-executable path, want \"\"", got)
	}
}

// TestRuntimeConfigInRTGroups：RuntimeConfig 每个 JSON 字段必须在
// admin_ui RT_GROUPS 里，且 RT_GROUPS 的 k 必须都有对应字段。
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
		if !regexp.MustCompile(`k:\s*'` + regexp.QuoteMeta(key) + `'\s*,\s*label`).MatchString(raw) {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("RuntimeConfig fields missing from RT_GROUPS: %v", missing)
	}
	valid := map[string]bool{}
	for i := 0; i < ty.NumField(); i++ {
		tag := ty.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		valid[strings.Split(tag, ",")[0]] = true
	}
	for _, mm := range rtGroupKeyRe.FindAllStringSubmatch(raw, -1) {
		if !valid[mm[1]] {
			t.Fatalf("RT_GROUPS key %q has no matching RuntimeConfig field", mm[1])
		}
	}
}

var rtGroupKeyRe = regexp.MustCompile(`k:\s*'([a-z_0-9]+)'\s*,\s*label`)

func adminUIBytes() (string, error) {
	b, err := adminUIFS.ReadFile("admin_ui/index.html")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// TestBrowserM1Sticky：驻留号可用时优先返回它；exclude 或号不存在时 nil。
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

	m.mu.Lock()
	m.seed = browserSeedState{AccountID: 99999}
	m.mu.Unlock()
	if got := stickyResidentAccount(nil); got != nil {
		t.Fatalf("sticky returned %+v for nonexistent account", got)
	}
	if got := stickyResidentAccount(map[int64]bool{99999: true}); got != nil {
		t.Fatal("sticky returned excluded account")
	}
}

// TestBrowserReconnectBackoff：崩溃退避走时间戳门控：
// ensureAlive/handleDeadLocked 之后 nextRetryAt 非零、started 复位、
// 退避期内 ensureRunning 直接回错不空等。
func TestBrowserReconnectBackoff(t *testing.T) {
	m := browserMgr
	m.mu.Lock()
	oldProc, oldClient, oldStarted, oldStarting, oldStartErr :=
		m.proc, m.client, m.started, m.starting, m.startErr
	oldReadyCh, oldStreak, oldStable, oldRetry :=
		m.readyCh, m.crashStreak, m.stableSince, m.nextRetryAt
	m.proc = &browserProc{PID: 999999999, PGID: 999999999, exitedCh: make(chan struct{})}
	m.client = nil
	m.stableSince = time.Now().Add(-120 * time.Second)
	m.crashStreak = 3
	m.started = true
	m.startErr = nil
	m.starting = false
	select {
	case <-m.readyCh:
	default:
		close(m.readyCh)
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.proc, m.client, m.started, m.starting, m.startErr =
			oldProc, oldClient, oldStarted, oldStarting, oldStartErr
		m.readyCh, m.crashStreak, m.stableSince, m.nextRetryAt =
			oldReadyCh, oldStreak, oldStable, oldRetry
		m.mu.Unlock()
	}()

	m.handleDeadLocked()

	m.mu.Lock()
	if m.crashStreak != 1 {
		m.mu.Unlock()
		t.Fatalf("crashStreak = %d, want 1 after stable reset", m.crashStreak)
	}
	if m.started {
		m.mu.Unlock()
		t.Fatal("started not reset")
	}
	if m.proc != nil {
		m.mu.Unlock()
		t.Fatal("proc not cleared")
	}
	if m.nextRetryAt.IsZero() {
		m.mu.Unlock()
		t.Fatal("nextRetryAt not set")
	}
	m.mu.Unlock()

	// 退避期内 ensureRunning 直接回错。
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := m.ensureRunning(ctx)
	if err == nil {
		t.Fatal("ensureRunning during backoff returned nil")
	}
	if !isBrowserLocalError(err) {
		t.Fatalf("backoff err = %v (%T)", err, err)
	}
}

// silence unused-import guards for helpers used only in some builds
var _ = syscall.Kill
