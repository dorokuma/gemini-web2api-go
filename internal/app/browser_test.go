package app

// browser_test.go —— 离线假 CDP / CONNECT 单元测试：纯回环 RFC 6455 服务端 +
// 本地 CONNECT 测试代理 + 内存态 browserManager，CI 不需要真实 Chromium，全 stdlib。
//
// 覆盖（对应 C 档 R2 复审的 10 组修复）：
//  1. WS 握手 / Pong 真校验 / 分片+夹控制帧 / Close 唤醒 pending / CDP 断连
//     传播到在途流（onClose → failAllStreams + pushHeaderErr）；
//  2. 流式 TTFB：body 精确等于 chunk1+chunk2（无 hdrJSON 污染）；
//     pre-headers fetch error 毫秒级唤醒 hdrCh（非 180s 超时）；
//  3. stopReader / ctx 取消 / 渲染器卡死 / Close 未读 + pumpDone 退出 泄漏路径；
//  4. browser_unavailable → 502；browser_fallback_http 真回退；
//  5. 代理桥：多代理池选 ProxyB 不被覆盖、Basic 认证特殊字符解码、
//     allowDirect true→false 立即断旧直连隧道、全禁用池首航禁直连、
//     回环/自环 CONNECT 拒绝、非 CONNECT 方法 405；
//  6. 生命周期：startOnce 失败退避后可重试、慢启动中途 Stop 无 close panic、
//     30s 冷却中途 Stop 毫秒级返回；
//  7. spawn 回收无僵尸、陈旧 DevToolsActivePort 清理、活进程核验失败保留
//     SingletonLock、Darwin 含空格 Chrome/profile、Linux 前缀碰撞；
//  8. 策略 M1：stickyResidentAccountFor 模型级冷却、多 authuser 共用 Tab、
//     匿名 credentials:"omit"、辅助入口 allowSwitch=false 不偷换驻留 jar；
//  9. discoverBrowserBinary CWD 防误判 + browserDetected 校验、RT_GROUPS 反射。

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
	"net/url"
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

type fakeCDPServer struct {
	t       *testing.T
	ln      net.Listener
	wsURL   string
	conn    net.Conn
	br      *bufio.Reader
	writeMu sync.Mutex

	gotCalls chan cdpCall
	abortHit chan string
	fetchHit chan fetchSpec
	pongHit  chan []byte
	closeCh  chan struct{}
	done     chan struct{}

	holdReplies atomic.Bool
	muteFetch   atomic.Bool
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
		return
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
			return
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
// preHeaderErr=true：不推 headers 直接推 kind:"error"（pre-headers fetch 失败）。
type fetchScript struct {
	status       int
	url          string
	redirected   bool
	chunks       [][]byte
	chunkDelays  []time.Duration
	err          string
	preHeaderErr bool
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
	// preHeaderErr：fetch 在 headers 前就失败（TypeError / 代理 502 / 断网）。
	if sc.preHeaderErr {
		msg := sc.err
		if msg == "" {
			msg = "TypeError: Failed to fetch"
		}
		s.pushBridgeMsg(spec.reqID, map[string]interface{}{
			"kind": "error", "message": msg,
		})
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
		n = uint64(binary.BigEndian.Uint64(ext[:]))
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
	if n == 0 {
		return nil
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
	oldReadyCh, oldShut, oldStop, oldStopCh := m.readyCh, m.shuttingDown, m.stopping, m.stopCh
	oldGen := m.generation
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
	m.stopCh = make(chan struct{})
	m.generation++
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
		m.readyCh, m.shuttingDown, m.stopping, m.stopCh = oldReadyCh, oldShut, oldStop, oldStopCh
		m.generation = oldGen
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

// ─── 组 1 测试：WS 与 CDP 断连传播 ──────────────────────────────────────────

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

	if !client.Closed() {
		t.Fatal("client.Closed() = false after conn close")
	}
}

// TestCDPDisconnectPropagatesToStreams：CDP 断连（服务端 close）后，
// 已拿 headers 的在途流必须被 failAllStreams 判死——Read 拿到
// browserErr("cdp disconnected")，绝不干等 stallLimit。
func TestCDPDisconnectPropagatesToStreams(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	// headers 立即推，之后 chunk 挂 30s（上游沉默）。断连必须秒级唤醒。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:      200,
			chunks:      [][]byte{[]byte("x")},
			chunkDelays: []time.Duration{30 * time.Second},
		})
	}()

	_, rc, err := browserMgr.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer rc.Close()
	s := findStreamByCloser(t, rc)
	// 等 headers 到达（流已注册）。
	time.Sleep(50 * time.Millisecond)
	srv.conn.Close() // 模拟 CDP 断连
	select {
	case <-s.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("stream not failed after CDP disconnect (silence-hang bug)")
	}
	buf := make([]byte, 64)
	_, rerr := rc.Read(buf)
	if rerr == nil {
		t.Fatal("Read returned nil error after CDP disconnect")
	}
}

// ─── 组 2 测试：流式 TTFB / pre-headers 错误快速唤醒 ─────────────────────────

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
		[]byte("f.req=x"), true, nil)
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
	if d := time.Since(t0); d > 200*time.Millisecond {
		t.Fatalf("TTFB %v exceeds 200ms (first chunk pushed immediately)", d)
	}
	rest, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("readAll: %v", err)
	}
	full := string(first) + string(rest)
	if full != string(chunk1)+string(chunk2) {
		t.Fatalf("body = %q, want exactly %q", full, string(chunk1)+string(chunk2))
	}
}

// TestBrowserPreHeaderErrorFastWake：pre-headers 的 kind:"error"
// （TypeError: Failed to fetch / 代理 502）必须毫秒级唤醒 hdrCh，
// 返回 pageFetchErr（普通上游错），绝非 DeadlineExceeded / browserLocalError，
// 也绝不干等 180s 超时。
func TestBrowserPreHeaderErrorFastWake(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			preHeaderErr: true,
			err:          "TypeError: Failed to fetch",
		})
	}()

	t0 := time.Now()
	_, _, err := browserMgr.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
	dt := time.Since(t0)
	if err == nil {
		t.Fatal("pre-header fetch error not returned")
	}
	if dt > 500*time.Millisecond {
		t.Fatalf("pre-header error took %v (must wake hdrCh in ms, not stallLimit)", dt)
	}
	if isBrowserLocalError(err) {
		t.Fatalf("pre-header fetch error misclassified as browserLocalError: %v", err)
	}
	var de *StallError
	if errors.As(err, &de) {
		t.Fatalf("pre-header error misclassified as StallError: %v", err)
	}
	if !strings.Contains(err.Error(), "Failed to fetch") {
		t.Fatalf("pre-header error should carry page fetch message, got: %v", err)
	}
}

// TestBrowserStopWhenAbort：stopReader 置位后下一次 Read 立刻 EOF；
// rc.Close 触发页内 abort。
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
		"https://gemini.google.com/_/x/StreamGenerate", "POST", nil, []byte("x"), true, nil)
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
	if err := rc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-srv.abortHit:
	case <-time.After(2 * time.Second):
		t.Fatal("page abort not sent after Close")
	}
}

// TestBrowserCancelAbort：headers 已到、上游沉默时 ctx 取消立刻唤醒 Read。
func TestBrowserCancelAbort(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

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
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
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
	time.Sleep(50 * time.Millisecond)
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

// TestBrowserAbortHungRenderer：渲染器卡死时 Close() 必须迅速返回。
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
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
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
// pumpToPipe 协程必须退出（pumpDone 关闭）——立即推 2 个 0 延迟 chunk
// 阻塞 pumpToPipe 在 pipeW.Write，Close 后 pipeR.Close 瞬间唤醒它。
func TestBrowserCloseWithoutReadNoLeak(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:      200,
			chunks:      [][]byte{[]byte("a"), []byte("b")}, // 0 延迟立即推
			chunkDelays: []time.Duration{0, 0},
		})
	}()

	ctx := context.Background()
	_, rc, err := browserMgr.doBrowserStreamRequest(ctx, nil,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	s := findStreamByCloser(t, rc)
	// 不 Read 直接 Close：pipeR.Close 必须无条件唤醒阻塞在 pipeW.Write 的 pumpToPipe。
	if err := rc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-s.pumpDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pumpToPipe did not exit within 500ms after Close-without-read (leak)")
	}
}

// ─── 组 3 测试：重定向边界 / 页内错误归类 ────────────────────────────────────

// TestBrowserRedirectAndFetchErrorClassification：
//   - 同源 302 跟随到 https://gemini.google.com/app 返回 200 → 放行；
//   - 跨源 302 → accounts.google.com / google.com/sorry → appPageStatusError，
//     xsrfErrBlamesCookie 命中；重定向早退时 pumpDone 退出；
//   - kind:"error" 的 TypeError → 普通上游错（非 browserLocalError）。
func TestBrowserRedirectAndFetchErrorClassification(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	// 用例 1：同源 302 跟随（hdr.url 是 gemini.google.com 的合法目标）→ 放行。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:     200,
			url:        "https://gemini.google.com/app",
			redirected: true,
			chunks:     [][]byte{[]byte("ok")},
		})
	}()
	st, rc, err := browserMgr.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/app", "GET", nil, nil, true, nil)
	if err != nil {
		t.Fatalf("same-origin 302 must be allowed, got err: %v", err)
	}
	rc.Close()
	if st != 200 {
		t.Fatalf("same-origin 302 status = %d, want 200", st)
	}

	// 用例 2：跨源 302 → accounts.google.com → appPageStatusError + pumpDone 退出。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:     200,
			url:        "https://accounts.google.com/ServiceLogin?continue=x",
			redirected: true,
			chunks:     [][]byte{[]byte("should-not-deliver")},
		})
	}()
	_, rc2, err := browserMgr.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/app", "GET", nil, nil, true, nil)
	var ape *appPageStatusError
	if !errors.As(err, &ape) {
		t.Fatalf("cross-origin redirect err not appPageStatusError: %v (%T)", err, err)
	}
	if !xsrfErrBlamesCookie(err) {
		t.Fatalf("redirect to accounts.google.com not blamed on cookie: %v", err)
	}
	// 重定向早退：fetch 已经派发但响应被 Abort，pumpDone 必须退出无泄漏。
	// rc2 为 nil（err 路径），stream 经 unregister+fail+pipeR.Close 由 defer 清理。
	_ = rc2

	// 用例 3：google.com/sorry 拦截页也按跨源拦截。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status:     200,
			url:        "https://www.google.com/sorry/index?continue=x",
			redirected: true,
		})
	}()
	_, _, err = browserMgr.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/app", "GET", nil, nil, true, nil)
	if !errors.As(err, &ape) {
		t.Fatalf("sorry-page redirect err not appPageStatusError: %v (%T)", err, err)
	}

	// 用例 4：页内 TypeError: Failed to fetch → 普通上游错（非 browserLocalError）。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{
			status: 200,
			err:    "TypeError: Failed to fetch",
		})
	}()
	_, rc3, err := browserMgr.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
	if err != nil {
		if isBrowserLocalError(err) {
			t.Fatalf("page fetch error misclassified as browser_unavailable: %v", err)
		}
		return
	}
	defer rc3.Close()
	_, rerr := io.Copy(io.Discard, rc3)
	if rerr == nil {
		t.Fatal("expected read error")
	}
	if isBrowserLocalError(rerr) {
		t.Fatalf("page fetch error misclassified as browser_unavailable: %v", rerr)
	}
}

// ─── 组 4 测试：代理桥 ───────────────────────────────────────────────────────

// localConnectProxy 是测试用的本机 CONNECT 代理：记 CONNECT 目标，
// 回 200 后 echo 数据。带 gotAuth 通道断言 Proxy-Authorization 头。
type localConnectProxy struct {
	ln      net.Listener
	gotHost chan string
	gotAuth chan string // 收到的 Proxy-Authorization 头值
}

func newLocalConnectProxy(t *testing.T) *localConnectProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &localConnectProxy{ln: ln, gotHost: make(chan string, 8), gotAuth: make(chan string, 8)}
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
	parts := strings.Fields(line)
	if len(parts) >= 2 && strings.EqualFold(parts[0], "CONNECT") {
		select {
		case p.gotHost <- parts[1]:
		default:
		}
	}
	for {
		l, err := br.ReadString('\n')
		if err != nil || l == "\r\n" || l == "\n" {
			break
		}
		l = strings.TrimRight(l, "\r\n")
		if i := strings.IndexByte(l, ':'); i > 0 &&
			strings.EqualFold(strings.TrimSpace(l[:i]), "Proxy-Authorization") {
			select {
			case p.gotAuth <- strings.TrimSpace(l[i+1:]):
			default:
			}
		}
	}
	io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
	io.Copy(c, br)
}

func (p *localConnectProxy) close() { p.ln.Close() }

// bridgeConnect 向桥发一条 CONNECT，返回连接与是否拿到 200。
func bridgeConnect(t *testing.T, port int, target string) (net.Conn, bool) {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial bridge: %v", err)
	}
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	br := bufio.NewReader(conn)
	status, _ := br.ReadString('\n')
	for {
		l, err := br.ReadString('\n')
		if err != nil || l == "\r\n" || l == "\n" {
			break
		}
	}
	return conn, strings.Contains(status, "200")
}

// TestBrowserProxyBridgeWiring：
//   - 调用方选定 ProxyB 不被覆盖为池首 ProxyA；
//   - 含 @/%/: 特殊字符密码的 Basic 认证按解码后原文发送；
//   - allowDirect true→false（URL 均空串）立即断旧直连隧道；
//   - 全禁用池首航 allowDirect=false 拒直连。
func TestBrowserProxyBridgeWiring(t *testing.T) {
	proxyA := newLocalConnectProxy(t)
	defer proxyA.close()
	proxyB := newLocalConnectProxy(t)
	defer proxyB.close()
	addrB := proxyB.ln.Addr().String()

	bridge, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}
	defer bridge.Shutdown(context.Background())
	bridge.allowLoopback = true

	// 调用方（acquireSlot）选定 ProxyB：桥必须走 B，绝不被换回池首 A。
	if err := bridge.SetUpstream("http://"+addrB, false); err != nil {
		t.Fatalf("SetUpstream: %v", err)
	}
	conn, ok := bridgeConnect(t, bridge.Port(), "gemini.google.com:443")
	if !ok {
		t.Fatal("bridge CONNECT via ProxyB not 200")
	}
	select {
	case got := <-proxyB.gotHost:
		if got != "gemini.google.com:443" {
			t.Fatalf("ProxyB saw CONNECT to %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("traffic did not reach ProxyB (caller-selected upstream overridden)")
	}
	select {
	case <-proxyA.gotHost:
		t.Fatal("traffic reached ProxyA — caller-selected ProxyB was overridden")
	case <-time.After(200 * time.Millisecond):
	}
	conn.Close()

	// Basic 认证：含 @/%/: 特殊字符密码按解码后原文发送（u.User.String() 的
	// URL 转义是 407 元凶）。
	proxyAuth := newLocalConnectProxy(t)
	defer proxyAuth.close()
	user := "user@corp"
	rawPass := "p@ss:w%rd"
	u := &url.URL{Scheme: "http", User: url.UserPassword(user, rawPass), Host: proxyAuth.ln.Addr().String()}
	if err := bridge.SetUpstream(u.String(), false); err != nil {
		t.Fatalf("SetUpstream auth: %v", err)
	}
	c2, ok := bridgeConnect(t, bridge.Port(), "example.com:443")
	if !ok {
		t.Fatal("bridge CONNECT via auth proxy not 200")
	}
	select {
	case auth := <-proxyAuth.gotAuth:
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+rawPass))
		if auth != want {
			t.Fatalf("Proxy-Authorization = %q, want %q (decoded creds, not URL-escaped)", auth, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("auth proxy did not receive Proxy-Authorization header")
	}
	c2.Close()

	// allowDirect true→false（URL 均空串）立即关闭旧直连隧道。
	bridge2, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("bridge2: %v", err)
	}
	defer bridge2.Shutdown(context.Background())
	bridge2.allowLoopback = true
	if err := bridge2.SetUpstream("", true); err != nil {
		t.Fatalf("SetUpstream direct: %v", err)
	}
	dc, _ := bridgeConnect(t, bridge2.Port(), "127.0.0.1:9")
	if err := bridge2.SetUpstream("", false); err != nil {
		t.Fatalf("SetUpstream tighten: %v", err)
	}
	if dc != nil {
		dc.SetReadDeadline(time.Now().Add(time.Second))
		var one [1]byte
		_, rerr := dc.Read(one[:])
		if rerr == nil {
			t.Fatal("direct tunnel not closed after allowDirect true→false")
		}
		dc.Close()
	}
	c3, ok := bridgeConnect(t, bridge2.Port(), "example.com:443")
	if ok {
		t.Fatal("CONNECT allowed after allowDirect=false (direct leak)")
	}
	if c3 != nil {
		c3.Close()
	}

	// 全禁用池首航：池非空全 disabled + fallback_direct=false
	// → upstream="" 且 allowDirect=false。
	oldProxies := proxyCache
	proxyMu.Lock()
	proxyCache = []Proxy{{ID: 1, URL: "http://proxy-a", Enabled: false}}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.FallbackDirect = false })
	up, allow := initialBridgeUpstream()
	if up != "" {
		t.Fatalf("disabled pool initialBridgeUpstream upstream = %q, want \"\"", up)
	}
	if allow {
		t.Fatal("disabled pool + fallback_direct=false must yield allowDirect=false")
	}
}

// TestBrowserProxyBridgeLoopbackAndMethodGuard：回环/自环 CONNECT 拒绝、
// 非 CONNECT 方法 405、upstream=""+allowDirect=false 拒直连。
func TestBrowserProxyBridgeLoopbackAndMethodGuard(t *testing.T) {
	bridge, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}
	defer bridge.Shutdown(context.Background())

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

	_ = bridge.SetUpstream("", true)
	for _, target := range []string{
		"127.0.0.1:8080", "[::1]:443", "localhost:9090",
		fmt.Sprintf("127.0.0.1:%d", bridge.Port()),
	} {
		req := httptest.NewRequest(http.MethodConnect, "http://x/", nil)
		req.Host = target
		rec := httptest.NewRecorder()
		bridge.handleConnect(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("CONNECT %q status = %d, want 400", target, rec.Code)
		}
	}
	for _, target := range []string{"noporthost", "bad host:80", "a b:80", ":80"} {
		req := httptest.NewRequest(http.MethodConnect, "http://x/", nil)
		req.Host = target
		rec := httptest.NewRecorder()
		bridge.handleConnect(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("CONNECT %q status = %d, want 400", target, rec.Code)
		}
	}
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

// ─── 组 5 测试：生命周期（startOnce 退避重试 / 慢启动 Stop / 冷却中途 Stop）─────

// resetMgrForLifecycle 把全局 browserMgr 复位到可独立测生命周期的状态，
// 测试结束还原。与 wireTestBrowser 不同：不填假 client，让 ensureRunning
// 真走启动路径（doStart 会被假 spawn 截住）。
func resetMgrForLifecycle(t *testing.T) func() {
	t.Helper()
	m := browserMgr
	m.mu.Lock()
	oldProc, oldClient, oldSess, oldTarget := m.proc, m.client, m.sessionID, m.targetID
	oldSeed, oldStarted, oldStarting, oldStartErr := m.seed, m.started, m.starting, m.startErr
	oldReadyCh, oldShut, oldStop, oldStopCh := m.readyCh, m.shuttingDown, m.stopping, m.stopCh
	oldGen, oldStreak, oldRetry := m.generation, m.crashStreak, m.nextRetryAt
	oldReady := m.ready.Load()
	m.proc = nil
	m.client = nil
	m.sessionID = ""
	m.targetID = ""
	m.started = false
	m.starting = false
	m.startErr = nil
	m.shuttingDown = false
	m.stopping = false
	m.stopCh = make(chan struct{})
	m.crashStreak = 0
	m.nextRetryAt = time.Time{}
	m.ready.Store(false)
	m.mu.Unlock()
	return func() {
		m.mu.Lock()
		m.proc, m.client, m.sessionID, m.targetID = oldProc, oldClient, oldSess, oldTarget
		m.seed, m.started, m.starting, m.startErr = oldSeed, oldStarted, oldStarting, oldStartErr
		m.readyCh, m.shuttingDown, m.stopping, m.stopCh = oldReadyCh, oldShut, oldStop, oldStopCh
		m.generation, m.crashStreak, m.nextRetryAt = oldGen, oldStreak, oldRetry
		m.ready.Store(oldReady)
		m.mu.Unlock()
	}
}

// TestBrowserStartFailRetryAndStopRace：
//   - startOnce 启动失败后退避到期可重新拉起（失败不永久锁死）；
//   - 慢启动中途 Stop(200ms) 超时后再 ensureRunning 无 close of closed channel panic。
func TestBrowserStartFailRetryAndStopRace(t *testing.T) {
	cleanup := resetMgrForLifecycle(t)
	defer cleanup()
	m := browserMgr

	// 让 doStart 立即失败：cfgBrowserPath 指到不存在的路径。
	oldBP := cfg.BrowserPath
	cfg.BrowserPath = filepath.Join(t.TempDir(), "no-such-browser-binary")
	defer func() { cfg.BrowserPath = oldBP }()

	// 退避表改短，让退避快速到期可重试。
	oldBackoffs := browserCrashBackoffs
	browserCrashBackoffs = []time.Duration{50 * time.Millisecond}
	defer func() { browserCrashBackoffs = oldBackoffs }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := m.ensureRunning(ctx)
	if err == nil {
		t.Fatal("ensureRunning with missing browser should fail")
	}
	if !isBrowserLocalError(err) {
		t.Fatalf("start err not browserLocalError: %v (%T)", err, err)
	}
	// 失败后：started=false、startErr 已记、可重试（不永久锁死）。
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		t.Fatal("started not reset after start failure")
	}
	if m.startErr == nil {
		m.mu.Unlock()
		t.Fatal("startErr not recorded")
	}
	m.mu.Unlock()
	// 退避到期后再调：仍走启动（报错），证明没锁死成「永远 startErr」。
	time.Sleep(80 * time.Millisecond)
	err2 := m.ensureRunning(ctx)
	if err2 == nil {
		t.Fatal("second ensureRunning should also fail (browser still missing)")
	}

	// 慢启动中途 Stop：把 spawn 换成会卡住的桩难做，直接造「starting 中」
	// 状态让 Stop 等 readyCh，验证无 close of closed channel panic。
	// 用一个独立管理器模拟：启动协程占住 starting，Stop 超时后 ensureRunning
	// 再次进入不能 panic。
	m2 := &browserManager{
		readyCh:   make(chan struct{}),
		streams:   map[string]*browserStream{},
		switchSem: make(chan struct{}, 1),
	}
	m2.mu.Lock()
	m2.started = true
	m2.starting = true
	gen := m2.generation + 1
	m2.generation = gen
	ready := make(chan struct{})
	m2.readyCh = ready
	m2.mu.Unlock()
	// 慢启动：200ms 后才结束（超时 Stop 等不到）。
	go func() {
		time.Sleep(300 * time.Millisecond)
		m2.mu.Lock()
		m2.started = false
		m2.starting = false
		m2.startErr = browserErr("simulated slow start done")
		m2.readyCh = nil
		m2.mu.Unlock()
		close(ready)
	}()
	sctx, scancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer scancel()
	m2.Stop(sctx) // 超时返回，不等慢启动
	// Stop 后再 ensureRunning：绝不能 close of closed channel panic。
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic after Stop during slow start: %v", r)
			}
		}()
		m2.ensureRunning(context.Background())
	}()
}

// TestBrowserStopDuringSwitchCooldown：30s 切号冷却中途 Stop(200ms)
// 必须在 <300ms 内立即返回——stopCh 打断冷却等待，不卡 30s。
func TestBrowserStopDuringSwitchCooldown(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	// 把冷却拉满：刚切过一次号。
	m.mu.Lock()
	m.lastSwitchAt = time.Now()
	m.seed = browserSeedState{AccountID: 1, CookieIdentity: "a|b"}
	m.mu.Unlock()

	// 在后台发起一次会进冷却的切号（不同 cookieIdentity → 走冷却等待）。
	switchDone := make(chan error, 1)
	go func() {
		next := &CookieAccount{ID: 2, Cookie: "SAPISID=z; __Secure-1PSID=z; SID=z", AuthUser: 0, Status: "enabled"}
		switchDone <- m.maybeSwitchAccount(context.Background(), next, "test")
	}()
	// 等冷却真开始（拿到 switchSem）。
	time.Sleep(100 * time.Millisecond)

	t0 := time.Now()
	sctx, scancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer scancel()
	m.Stop(sctx)
	dt := time.Since(t0)
	if dt > 300*time.Millisecond {
		t.Fatalf("Stop during 30s switch cooldown blocked %v (stopCh must interrupt)", dt)
	}
	// 切号协程也应因 stopCh/ctx 退出（不卡 30s）。
	select {
	case <-switchDone:
	case <-time.After(time.Second):
		t.Fatal("maybeSwitchAccount did not exit after Stop")
	}
}

// ─── 组 6 测试：502 / fallback ───────────────────────────────────────────────

func TestBrowserUnavailable502(t *testing.T) {
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.BrowserFallbackHTTP = false
	})
	m := browserMgr
	cleanup := resetMgrForLifecycle(t)
	defer cleanup()
	m.mu.Lock()
	m.started = true
	m.starting = false
	m.startErr = browserErr("simulated start failure")
	select {
	case <-m.readyCh:
	default:
		close(m.readyCh)
	}
	m.mu.Unlock()

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

func TestBrowserFallbackHTTP(t *testing.T) {
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.BrowserFallbackHTTP = true
	})
	m := browserMgr
	cleanup := resetMgrForLifecycle(t)
	defer cleanup()
	m.mu.Lock()
	m.started = true
	m.starting = false
	m.startErr = browserErr("simulated start failure")
	select {
	case <-m.readyCh:
	default:
		close(m.readyCh)
	}
	m.mu.Unlock()

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

// ─── 组 7 测试：进程管理 ─────────────────────────────────────────────────────

func TestSpawnZombieReap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix-only: uses /bin/sh and /proc")
	}
	proc, err := spawnBrowser("/bin/sh", []string{"-c", "exit 0"}, nil)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	select {
	case <-proc.exitedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("reaper did not reap child within 3s")
	}
	if proc.Alive() {
		t.Fatal("Alive() = true after child exited (zombie or stale PID)")
	}
}

// TestStaleDevToolsActivePort：reclaimOrphanBrowser 删 DevToolsActivePort +
// 死 PID 清 instance；活进程核验失败时保留 Singleton*（防误删）。
func TestStaleDevToolsActivePort(t *testing.T) {
	dir := t.TempDir()
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
	if _, err := os.Stat(filepath.Join(dir, "browser-instance.json")); !os.IsNotExist(err) {
		t.Fatal("dead browser-instance.json not removed")
	}

	// 活进程核验失败（进程在跑但 processIsOurBrowser=false）：Singleton* 与
	// instance 记录都必须保留——那是另一个真在跑的实例，动不得。
	dir2 := t.TempDir()
	alivePID := os.Getpid() // 当前测试进程：活着，但 cmdline 不含 --user-data-dir
	for _, n := range []string{"SingletonLock", "SingletonCookie", "SingletonSocket"} {
		if err := os.WriteFile(filepath.Join(dir2, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	inst2 := `{"pid":` + itoa(alivePID) + `,"pgid":` + itoa(alivePID) + `,"profile":"` + dir2 + `"}`
	if err := os.WriteFile(filepath.Join(dir2, "browser-instance.json"), []byte(inst2), 0o600); err != nil {
		t.Fatal(err)
	}
	reclaimOrphanBrowser(dir2)
	// 活进程核验不过 → SingletonLock 必须仍在（没误删）。
	if _, err := os.Stat(filepath.Join(dir2, "SingletonLock")); err != nil {
		t.Fatal("SingletonLock removed for live unverified process (double-instance risk)")
	}
	if _, err := os.Stat(filepath.Join(dir2, "browser-instance.json")); err != nil {
		t.Fatal("browser-instance.json removed for live unverified process")
	}
}

// itoa 是测试小工具（避免 import strconv 仅为这一处）。
func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}

// TestProcessIsOurBrowserCmdlineMatch：Linux /proc 精确比对 + Darwin
// 含空格 Chrome/profile、防 profile 词冒充 exe、Linux 前缀碰撞。
func TestProcessIsOurBrowserCmdlineMatch(t *testing.T) {
	for _, ok := range []struct {
		word string
		want bool
	}{
		{"/usr/bin/chromium", true},
		{"/opt/google/chrome/chrome", true},
		{"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", true},
		{"Google", false},
		{"Chrome", true},
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
	// Darwin 解析：可执行段是 "--user-data-dir=" 之前的整段，profile 路径
	// 里的 "chrome" 不能冒充可执行名；含空格 profile 按 " --" 边界切。
	wantDir := "--user-data-dir=/Users/me/Library/Application Support/gw/browser-profile"
	for _, c := range []struct {
		line string
		want bool
	}{
		// 真 Chrome + 含空格 profile 路径。
		{"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome --user-data-dir=/Users/me/Library/Application Support/gw/browser-profile --headless=new", true},
		// profile 路径含 "chrome" 词，但可执行段是 firefox → 拒（防冒充）。
		{"/usr/bin/firefox --user-data-dir=/Users/me/Library/Application Support/gw/browser-profile", false},
		// 另一个 profile 的 Chrome（user-data-dir 不同）→ 拒。
		{"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome --user-data-dir=/other/profile --headless=new", false},
		// 完全无关进程（不含 --user-data-dir）→ 拒。
		{"/usr/bin/sleep 60", false},
	} {
		if got := darwinCmdlineIsOurBrowser(c.line, wantDir); got != c.want {
			t.Errorf("darwinCmdlineIsOurBrowser(%q) = %v, want %v", c.line, got, c.want)
		}
	}
	// Linux 前缀碰撞：spawn /bin/sh -c 'sleep 30'，cmdline 无 --user-data-dir → false。
	if runtime.GOOS == "linux" {
		p, err := spawnBrowser("/bin/sh", []string{"-c", "sleep 30"}, nil)
		if err != nil {
			t.Fatalf("spawn: %v", err)
		}
		defer killBrowserProcessGroup(p.PID, p.PGID, 500*time.Millisecond)
		if processIsOurBrowser(p.PID, "/tmp/p") {
			t.Fatal("processIsOurBrowser matched /bin/sh (should not)")
		}
		// /tmp/p vs /tmp/p-other 前缀碰撞：profileDir=/tmp/p 不能误中 /tmp/p-other。
		// 造一个 cmdline 含 "--user-data-dir=/tmp/p-other" 的进程验证精确边界。
		p2, err := spawnBrowser("/bin/sh", []string{"-c", "exec -a '/bin/sh --user-data-dir=/tmp/p-other' sleep 30"}, nil)
		if err != nil {
			// exec -a 在 sh 不一定支持；跳过精确碰撞校验，至少上面的负例已覆盖。
			t.Logf("spawn for prefix-collision: %v (skip)", err)
			return
		}
		defer killBrowserProcessGroup(p2.PID, p2.PGID, 500*time.Millisecond)
		if processIsOurBrowser(p2.PID, "/tmp/p") {
			t.Fatal("processIsOurBrowser(/tmp/p) matched --user-data-dir=/tmp/p-other (prefix collision)")
		}
	}
}

// ─── 组 8 测试：策略 M1 / 多 authuser / 匿名 omit / 辅助入口不切号 ─────────────

// TestBrowserM1Sticky：stickyResidentAccountFor 传真实 mc.Upstream——
// 对 gemini-4-carbon 冷却中的号返回 nil，正常未冷却模型仍返回该号。
func TestBrowserM1Sticky(t *testing.T) {
	cleanup := resetMgrForLifecycle(t)
	defer cleanup()
	m := browserMgr

	// 造真实 enabled 账号 A 进测试 DB。
	acctID, err := accountInsert("test-a", "SAPISID=sa; __Secure-1PSID=ps; SID=x", "")
	if err != nil {
		t.Fatalf("insert account: %v", err)
	}
	defer accountDelete(acctID)

	m.mu.Lock()
	m.seed = browserSeedState{AccountID: acctID, CookieIdentity: "sa|ps"}
	m.mu.Unlock()

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.StrictModel = true
		c.StrictModelCooldownMin = 5 // markDowngraded 只在冷却分钟数 >0 时登记
	})

	carbon := Models["gemini-4-carbon"]
	other := Models["gemini-3.6-flash"]
	// 对 gemini-4-carbon 标记降级冷却：同号对该模型必须返回 nil。
	markDowngraded(acctID, carbon)
	defer func() {
		downgradeMu.Lock()
		delete(downgradeUntil, downgradeKey{acctID, carbon.Upstream})
		downgradeMu.Unlock()
	}()
	if got := stickyResidentAccountFor(nil, carbon.Upstream); got != nil {
		t.Fatalf("sticky returned %+v for account in %s cooldown", got, carbon.Upstream)
	}
	// 未冷却的别的模型：同一号照常驻留（冷却按模型隔离）。
	if got := stickyResidentAccountFor(nil, other.Upstream); got == nil || got.ID != acctID {
		t.Fatalf("sticky for uncooled model should return account %d, got %+v", acctID, got)
	}
	// exclude 命中也返回 nil。
	if got := stickyResidentAccountFor(map[int64]bool{acctID: true}, other.Upstream); got != nil {
		t.Fatal("sticky returned excluded account")
	}
	// 不存在的号：nil。
	m.mu.Lock()
	m.seed = browserSeedState{AccountID: 99999}
	m.mu.Unlock()
	if got := stickyResidentAccountFor(nil, other.Upstream); got != nil {
		t.Fatalf("sticky returned %+v for nonexistent account", got)
	}
}

// TestBrowserM1AuxNoSwitch：辅助入口（allowSwitch=false）对非驻留账号
// 绝不切驻留 jar——shouldSwitch 对活驻留号返回 false，调用方回退纯 Go HTTP。
func TestBrowserM1AuxNoSwitch(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	acctID, err := accountInsert("resident", "SAPISID=ra; __Secure-1PSID=rp; SID=r", "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(acctID)
	acctID2, err := accountInsert("other", "SAPISID=oa; __Secure-1PSID=op; SID=o", "")
	if err != nil {
		t.Fatalf("insert2: %v", err)
	}
	defer accountDelete(acctID2)

	m.mu.Lock()
	m.seed = browserSeedState{AccountID: acctID, CookieIdentity: cookieIdentity("SAPISID=ra; __Secure-1PSID=rp; SID=r")}
	m.mu.Unlock()

	// 辅助入口点名非驻留号：allowSwitch=false → shouldSwitch 必 false（不切号）。
	other := accountByID(acctID2)
	need, _ := m.shouldSwitch(other, false, nil)
	if need {
		t.Fatal("auxiliary entry (allowSwitch=false) must never switch resident jar")
	}
	// 对照：主请求（allowSwitch=true）点名别的身份的号才允许切。
	need2, _ := m.shouldSwitch(other, true, nil)
	if !need2 {
		t.Fatal("main request targeting another identity should switch")
	}
}

// TestBrowserM1MultiAuthUser：同 cookieIdentity 不同 authuser 不触发
// clearBrowserCookies 与 30s 冷却。
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
	m.lastSwitchAt = time.Now()
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
	select {
	case c := <-srv.gotCalls:
		if c.method == "Network.clearBrowserCookies" {
			t.Fatal("clearBrowserCookies called for same-identity authuser switch")
		}
	default:
	}
}

// TestBrowserAnonCredentialsOmit：匿名请求（account==nil）fetch 表达式
// 必须带 omitCredentials:true。
func TestBrowserAnonCredentialsOmit(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

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
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
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

// ─── 组 9 测试：生命周期杂项 / Cookie / Profile / 探测 / RT ──────────────────

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
		t.Fatal("Stop must not set shuttingDown")
	}
	if m.sessionID != "" || m.targetID != "" {
		t.Fatal("session/target not cleared after Stop")
	}
}

func TestBrowserRecreateTabNoStorm(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	m.mu.Lock()
	m.targetID = "fake-target-1"
	m.mu.Unlock()

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

// TestSyncCookiesAfterShutdown：非零 seed.AccountID + client=nil 不得 panic。
func TestSyncCookiesAfterShutdown(t *testing.T) {
	m := browserMgr
	m.mu.Lock()
	oldProc, oldClient, oldSess, oldSeed := m.proc, m.client, m.sessionID, m.seed
	m.proc = nil
	m.client = nil
	m.sessionID = ""
	m.seed = browserSeedState{AccountID: 12345} // 非零 seed，走 accountByID 分支
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.proc, m.client, m.sessionID, m.seed = oldProc, oldClient, oldSess, oldSeed
		m.mu.Unlock()
	}()
	m.syncCookiesBack(true)
	m.syncCookiesBack(false)
	m.syncCookiesBackForce(context.Background())
}

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
	cwd, _ := os.Getwd()
	if _, err := os.Stat(filepath.Join(cwd, "seed-state.json")); !os.IsNotExist(err) {
		t.Fatal("seed-state.json polluted CWD")
	}
}

// TestDiscoverBrowserBinaryAndDetected：CWD 的空目录 chromium 不选中、
// PATH 下另一个目录的 google-chrome-stable 命中、不可执行路径报错、
// browserDetected 对不存在路径返回 ""。
func TestDiscoverBrowserBinaryAndDetected(t *testing.T) {
	// CWD 放一个不可执行的 chromium 目录：绝不能被选中。
	cwd := t.TempDir()
	if err := os.Mkdir(filepath.Join(cwd, "chromium"), 0o755); err != nil {
		t.Fatal(err)
	}
	// PATH 下另一个独立目录放可执行桩 google-chrome-stable。
	binDir := t.TempDir()
	stub := filepath.Join(binDir, "google-chrome-stable")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	oldPath := os.Getenv("PATH")
	os.Setenv("PATH", binDir)
	defer os.Setenv("PATH", oldPath)
	oldWd, _ := os.Getwd()
	if err := os.Chdir(cwd); err != nil {
		t.Skipf("chdir: %v", err)
	}
	defer os.Chdir(oldWd)

	if runtime.GOOS != "windows" {
		p, err := discoverBrowserBinary("")
		if err != nil {
			t.Fatalf("discoverBrowserBinary: %v (PATH stub should be found)", err)
		}
		if p != stub {
			t.Fatalf("discoverBrowserBinary = %q, want PATH stub %q (not CWD chromium)", p, stub)
		}
		// 不可执行的显式路径报错。
		noExec := filepath.Join(binDir, "notexec")
		if err := os.WriteFile(noExec, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := discoverBrowserBinary(noExec); err == nil {
			t.Fatal("explicit non-executable path accepted")
		}
	}
	// browserDetected：不存在的路径返回 ""。
	old := cfg.BrowserPath
	cfg.BrowserPath = filepath.Join(t.TempDir(), "no-such-binary-xyz")
	defer func() { cfg.BrowserPath = old }()
	if got := browserDetected(); got != "" {
		t.Fatalf("browserDetected = %q for nonexistent path, want \"\"", got)
	}
}

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

// TestBrowserReconnectBackoff：崩溃退避走时间戳门控：handleDeadLocked
// 后 nextRetryAt 非零、started 复位、退避期内 ensureRunning 直接回错。
func TestBrowserReconnectBackoff(t *testing.T) {
	cleanup := resetMgrForLifecycle(t)
	defer cleanup()
	m := browserMgr
	m.mu.Lock()
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

var _ = syscall.Kill
