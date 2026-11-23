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
	// cookiesJSON 非 nil 时 Network.getCookies 返回它（测试注入持久 Jar）。
	cookiesJSON atomic.Value // json.RawMessage

	// pageOrigin / pageReady 模拟当前活动文档的 location.origin 与
	// document.readyState（about:blank 初始为 "null"/"complete"）；
	// pageNavDelay>0 时 Page.navigate 后延迟这么久才切 origin 并推
	// Page.loadEventFired（模拟真实导航的网络往返）。
	// navErrorText 非空时 Page.navigate 返回 RPC 成功 + errorText（模拟
	// 代理/网络错误下 Chromium 停在 chrome-error:// 的导航失败）。
	pageOrigin   atomic.Value // string
	pageReady    atomic.Value // string
	pageNavDelay atomic.Int64 // 纳秒
	navErrorText atomic.Value // string

	// failMethods 列出的 CDP method 一律回 JSON-RPC 错误（fault 注入）。
	failMethods sync.Map // map[string]bool
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
	s.pageOrigin.Store("null")     // about:blank
	s.pageReady.Store("complete")  // about:blank 的 readyState 恰为 complete
	s.pageNavDelay.Store(int64(0)) // 默认立即切 origin 推 load
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

	// Network.* 域必须走 page session：sessionID="" 的浏览器顶层连接没有
	// Network 域，真实 Chromium 会报 -32601——测试必须拦住这个回归。
	if strings.HasPrefix(m.Method, "Network.") && m.SessionID == "" {
		go s.sendText([]byte(fmt.Sprintf(
			`{"id":%d,"error":{"code":-32601,"message":%q}}`,
			m.ID, "'"+m.Method+"' wasn't found (empty sessionId)")))
		return
	}

	if s.holdReplies.Load() || m.Method == "Never.Returns" {
		return
	}
	if _, bad := s.failMethods.Load(m.Method); bad {
		go s.sendText([]byte(fmt.Sprintf(
			`{"id":%d,"error":{"code":-32000,"message":"injected %s failure"}}`,
			m.ID, m.Method)))
		return
	}

	result := json.RawMessage(`{}`)
	switch m.Method {
	case "Target.createTarget":
		result = json.RawMessage(`{"targetId":"fake-target-1"}`)
	case "Target.attachToTarget":
		result = json.RawMessage(`{"sessionId":"fake-session-1"}`)
	case "Page.navigate":
		if et := s.navErrorText.Load(); et != nil && et.(string) != "" {
			// 代理/网络错误：RPC 成功但 errorText 非空，页面停在
			// chrome-error://——不切 origin、不推 loadEventFired。
			result = json.RawMessage(fmt.Sprintf(
				`{"frameId":"frame-1","errorText":%q}`, et.(string)))
			break
		}
		// 模拟真实导航：返回响应时活动文档仍是旧文档；延迟 navDelay 后
		// 切 origin 并推 Page.loadEventFired（sessionId 与请求一致）。
		delay := time.Duration(s.pageNavDelay.Load())
		go func(sessionID string) {
			if delay > 0 {
				time.Sleep(delay)
			}
			s.pageOrigin.Store("https://gemini.google.com")
			s.pageReady.Store("complete")
			s.pushEvent(sessionID, "Page.loadEventFired",
				map[string]interface{}{"timestamp": 1})
		}(m.SessionID)
		result = json.RawMessage(`{"frameId":"frame-1","loaderId":"loader-1"}`)
	case "Page.enable":
		// 模拟真实浏览器：新 attach 的 page 起于 about:blank。
		s.pageOrigin.Store("null")
		s.pageReady.Store("complete")
		result = json.RawMessage(`{}`)
	case "Runtime.evaluate":
		switch {
		case strings.Contains(string(m.Params), "typeof __gwPush"):
			result = json.RawMessage(`{"result":{"type":"string","value":"function/object"}}`)
		case strings.Contains(string(m.Params), "location.origin"):
			// navigateAndWaitApp 的终核：按当前模拟 origin 回
			// "origin/readyState"——about:blank 的 origin 是 "null"，
			// 导航 /app 后应切到 https://gemini.google.com。
			val := s.pageOrigin.Load().(string) + "/" + s.pageReady.Load().(string)
			result = json.RawMessage(fmt.Sprintf(
				`{"result":{"type":"string","value":%q}}`, val))
		case strings.Contains(string(m.Params), "document.readyState"):
			// waitPageLoad 的轮询：按模拟 readyState 回。
			result = json.RawMessage(fmt.Sprintf(
				`{"result":{"type":"string","value":%q}}`,
				s.pageReady.Load().(string)))
		default:
			s.handleEvaluate(m.Params)
		}
	case "Network.getCookies":
		if v := s.cookiesJSON.Load(); v != nil {
			result = v.(json.RawMessage)
		} else {
			result = json.RawMessage(`{"cookies":[]}`)
		}
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
	return wireTestBrowserBridge(t, srv, nil)
}

// wireTestBrowserBridge 同 wireTestBrowser，允许测试挂一个真 bridge
// （代理桥接线断言用），nil 则不挂。
func wireTestBrowserBridge(t *testing.T, srv *fakeCDPServer, bridge *browserProxyBridge) func() {
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
	oldBridge, oldCleaning, oldStartCancel := m.bridge, m.cleaning, m.startCancel
	oldGen := m.generation
	oldReady := m.ready.Load()
	m.proc = &browserProc{PID: os.Getpid(), PGID: os.Getpid(), exitedCh: make(chan struct{})}
	m.client = client
	m.sessionID = "fake-session-1"
	m.targetID = "fake-target-1"
	m.bridge = bridge
	m.started = true
	m.starting = false
	m.startErr = nil
	m.shuttingDown = false
	m.stopping = false
	m.cleaning = false
	m.startCancel = nil
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
		m.bridge, m.cleaning, m.startCancel = oldBridge, oldCleaning, oldStartCancel
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

// lastStreamReqID 返回 browserMgr.streamReg 里最新注册的 reqID（重定向早退等
// rc==nil 的错误路径里，调用方拿不到 stream——泵协程退出断言只能按 reqID 找）。
func lastStreamReqID(m *browserManager) string {
	m.streamRegMu.Lock()
	defer m.streamRegMu.Unlock()
	latest := ""
	for id := range m.streams {
		if id > latest {
			latest = id
		}
	}
	return latest
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
	// headers 后立即推一条 0 延迟真实 chunk 把 pumpToPipe 顶进 pipeW.Write
	// 背压阻塞（读侧还没消费），再断 CDP——failAllStreams 的 pipeR.Close
	// 必须唤醒它；若删掉那行这个断言必 FAIL。
	s.push("chunk", []byte("backpressure-chunk"), browserStreamHeader{}, "")
	// 等 pumpToPipe 取走 chunk 阻塞在 pipeW.Write（queue 已空但 done 未置）。
	for i := 0; i < 200; i++ {
		s.mu.Lock()
		empty := len(s.queue) == 0
		s.mu.Unlock()
		if empty {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 断开后：closed（failAll 判死）+ pumpDone（failAllStreams 的 pipeR.Close
	// 唤醒背压 pumpToPipe）都必须在预算内退出。
	srv.conn.Close() // 模拟 CDP 断连
	select {
	case <-s.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("stream not failed after CDP disconnect (silence-hang bug)")
	}
	select {
	case <-s.pumpDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pumpToPipe not woken after CDP disconnect (failAllStreams must close pipeR)")
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

	// 用例 2：跨源 302 → accounts.google.com → appPageStatusError +
	// 重定向早退时 pumpToPipe 必须在 500ms 内退出（chunk 已到、阻塞在
	// pipeW.Write 的背压泵被 pipeR.Close 唤醒）。
	fetchHit2 := make(chan fetchSpec, 1)
	go func() {
		spec := <-srv.fetchHit
		fetchHit2 <- spec
		srv.setFetchScript(spec.reqID, fetchScript{
			status:     200,
			url:        "https://accounts.google.com/ServiceLogin?continue=x",
			redirected: true,
			chunks:     [][]byte{[]byte("should-not-deliver")},
		})
	}()
	type streamRes struct {
		rc  io.ReadCloser
		err error
	}
	resCh := make(chan streamRes, 1)
	go func() {
		_, rc, err := browserMgr.doBrowserStreamRequest(context.Background(), nil,
			"https://gemini.google.com/app", "GET", nil, nil, true, nil)
		resCh <- streamRes{rc, err}
	}()
	// 先拿在飞 reqID 定位 stream（请求返回时它已注销，只能靠此刻找）。
	spec2 := <-fetchHit2
	var s2 *browserStream
	for i := 0; i < 100 && s2 == nil; i++ {
		browserMgr.streamRegMu.Lock()
		s2 = browserMgr.streams[spec2.reqID]
		browserMgr.streamRegMu.Unlock()
		if s2 == nil {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if s2 == nil {
		t.Fatal("stream not found in registry while in-flight")
	}
	res := <-resCh
	var ape *appPageStatusError
	if !errors.As(res.err, &ape) {
		t.Fatalf("cross-origin redirect err not appPageStatusError: %v (%T)", res.err, res.err)
	}
	if !xsrfErrBlamesCookie(res.err) {
		t.Fatalf("redirect to accounts.google.com not blamed on cookie: %v", res.err)
	}
	if res.rc != nil {
		res.rc.Close()
	}
	// 错误路径 rc==nil：defer 清理（unregister+fail+pipeR.Close）必须让
	// 阻塞在 chunk 背压的 pumpToPipe 立刻退出。
	select {
	case <-s2.pumpDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pumpToPipe did not exit within 500ms after redirect abort (pipeR.Close missing)")
	}

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
//   - 真实 echo server 上验证旧直连隧道活通、SetUpstream("") 后 EOF/reset；
//   - 生产明确空串直连 SetUpstream("", true) 把桥从代理 B 切回直连；
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
	// 真实 echo server：先证明直连隧道活通，再断言 SetUpstream 后 EOF/reset。
	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoLn.Close()
	go func() {
		for {
			c, err := echoLn.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c); c.Close() }()
		}
	}()
	echoAddr := echoLn.Addr().String()

	bridge2, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("bridge2: %v", err)
	}
	defer bridge2.Shutdown(context.Background())
	bridge2.allowLoopback = true
	if err := bridge2.SetUpstream("", true); err != nil {
		t.Fatalf("SetUpstream direct: %v", err)
	}
	dc, _ := bridgeConnect(t, bridge2.Port(), echoAddr)
	if dc == nil {
		t.Fatal("bridge CONNECT to echo server not 200")
	}
	// 隧道活通断言：写 PING 必须原样读回。
	if _, err := dc.Write([]byte("PING")); err != nil {
		t.Fatalf("echo write: %v", err)
	}
	dc.SetReadDeadline(time.Now().Add(2 * time.Second))
	pong := make([]byte, 4)
	if _, err := io.ReadFull(dc, pong); err != nil {
		t.Fatalf("echo read: %v", err)
	}
	if string(pong) != "PING" {
		t.Fatalf("echo reply = %q, want PING (tunnel not live)", pong)
	}
	// SetUpstream("", false) 断旧直连隧道：500ms 内必须收到 EOF/reset，
	// 明确拒绝超时（隧道没被关才算失败）。
	if err := bridge2.SetUpstream("", false); err != nil {
		t.Fatalf("SetUpstream tighten: %v", err)
	}
	dc.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var one [1]byte
	_, rerr := dc.Read(one[:])
	if rerr == nil {
		t.Fatal("direct tunnel not closed after allowDirect true→false")
	}
	if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
		t.Fatalf("direct tunnel read timed out after SetUpstream close: %v (must be EOF/reset)", rerr)
	}
	if errors.Is(rerr, os.ErrDeadlineExceeded) {
		t.Fatalf("direct tunnel read deadline exceeded after SetUpstream close: %v", rerr)
	}
	dc.Close()
	c3, ok := bridgeConnect(t, bridge2.Port(), "example.com:443")
	if ok {
		t.Fatal("CONNECT allowed after allowDirect=false (direct leak)")
	}
	if c3 != nil {
		c3.Close()
	}

	// 生产明确空串直连：先 SetUpstream(proxyB, false) 让桥走代理 B，
	// 再 SetUpstream("", true) 明确切回直连——upstream 必须真变成 ""，
	// 绝不因空串跳过同步沿用代理 B（修「proxyURL=="" 不更新桥」的旧 bug）。
	bridge4, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("bridge4: %v", err)
	}
	defer bridge4.Shutdown(context.Background())
	if err := bridge4.SetUpstream("http://"+addrB, false); err != nil {
		t.Fatalf("SetUpstream proxyB: %v", err)
	}
	if err := bridge4.SetUpstream("", true); err != nil {
		t.Fatalf("SetUpstream explicit direct: %v", err)
	}
	bridge4.mu.Lock()
	if bridge4.upstream != "" {
		bridge4.mu.Unlock()
		t.Fatalf("explicit-direct SetUpstream(\"\") did not clear upstream: %q", bridge4.upstream)
	}
	if !bridge4.allowDirect {
		bridge4.mu.Unlock()
		t.Fatal("explicit-direct SetUpstream(\"\",true) must set allowDirect")
	}
	bridge4.mu.Unlock()

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

	// 冷启动/请求路径的桥接线（wire 级）：m.bridge 挂真 bridge 后，走浏览器的
	// 请求把 proxyURL 经 doBrowserStreamRequestCtx 同步进桥（SetUpstream 仅
	// 在确实变更时换代际）；skipBrowser 的非池会话载体走纯 Go HTTP，严禁
	// 碰桥（gen 不变 = 没有 SetUpstream 扰动）。
	srv := newFakeCDPServer(t)
	defer srv.close()
	bridge3, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("bridge3: %v", err)
	}
	defer bridge3.Shutdown(context.Background())
	cleanup := wireTestBrowserBridge(t, srv, bridge3)
	defer cleanup()
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.BrowserTransport = true })
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{status: 200})
	}()
	proxyBURL := "http://" + addrB
	_, rcB, err := browserMgr.doBrowserStreamRequestCtx(context.Background(), proxyBURL, nil,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
	if err != nil {
		t.Fatalf("stream via bridge3: %v", err)
	}
	rcB.Close()
	bridge3.mu.Lock()
	if bridge3.upstream != proxyBURL {
		bridge3.mu.Unlock()
		t.Fatalf("request proxyURL not applied to bridge: upstream=%q, want %q", bridge3.upstream, proxyBURL)
	}
	genBefore := bridge3.gen
	bridge3.mu.Unlock()
	// 非池会话载体（acct==nil 但带 Cookie）走纯 Go HTTP：桥的 upstream/gen
	// 必须原封不动——不允许 syncBridgeUpstream 被调用。
	_, _, _, _, herr := doGeminiRequestStopInner(context.Background(),
		"http://127.0.0.1:1/nowhere", "f.req=x",
		map[string]string{"Cookie": "SAPISID=carrier; __Secure-1PSID=c; SID=x"},
		"", nil, nil, false)
	_ = herr // 走纯 Go HTTP 直连 127.0.0.1:1 必然失败；只关心桥没被动
	bridge3.mu.Lock()
	if bridge3.gen != genBefore || bridge3.upstream != proxyBURL {
		bridge3.mu.Unlock()
		t.Fatalf("non-pool HTTP bypass disturbed bridge: gen %d->%d upstream %q",
			genBefore, bridge3.gen, bridge3.upstream)
	}
	genDirectBefore := bridge3.gen
	bridge3.mu.Unlock()
	// proxyURL="" 显式直连：经生产 doBrowserStreamRequestCtx 路径把已走
	// ProxyB 的桥真切回 ""（空串不是「没给出口信息」，绝不能跳过同步
	// 沿用代理 B——那是把真 IP 走旧出口的泄漏方向修反）。本函数前段
	// 已把 fallback_direct 改成 false，故这里 allowDirect 必须为 false
	// （池非空禁直连），upstream 仍须真切回 "" 不再留 ProxyB。
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{status: 200})
	}()
	_, rcD, err := browserMgr.doBrowserStreamRequestCtx(context.Background(), "", nil,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
	if err != nil {
		t.Fatalf("stream explicit-direct via bridge3: %v", err)
	}
	rcD.Close()
	bridge3.mu.Lock()
	if bridge3.upstream != "" {
		bridge3.mu.Unlock()
		t.Fatalf("explicit-direct request did not clear bridge upstream: %q", bridge3.upstream)
	}
	if bridge3.allowDirect {
		bridge3.mu.Unlock()
		t.Fatal("explicit-direct request with fallback_direct=false must leave allowDirect=false")
	}
	if bridge3.gen == genDirectBefore {
		bridge3.mu.Unlock()
		t.Fatal("explicit-direct request did not SetUpstream on bridge (gen unchanged)")
	}
	bridge3.mu.Unlock()
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
	oldBridge2, oldCleaning2, oldStartCancel2 := m.bridge, m.cleaning, m.startCancel
	oldGen, oldStreak, oldRetry := m.generation, m.crashStreak, m.nextRetryAt
	oldReady := m.ready.Load()
	m.proc = nil
	m.client = nil
	m.sessionID = ""
	m.targetID = ""
	m.bridge = nil
	m.cleaning = false
	m.startCancel = nil
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
		m.bridge, m.cleaning, m.startCancel = oldBridge2, oldCleaning2, oldStartCancel2
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
		stopCh:    make(chan struct{}),
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

	// ── handleDeadLocked 单飞清场：两条并发真实调用同一快照，
	// 第二条不放弃就会踩坏新代际。用可控 stub 卡死第一条的锁外清理，
	// 保证第二条真在它清理期间到达 ──
	m3 := &browserManager{
		readyCh:   make(chan struct{}),
		streams:   map[string]*browserStream{},
		switchSem: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
	// 假 CDP conn：cdpClient.Close 需要真实 net.Conn，用 TCP 回环对。
	// （net.Pipe 无真实 fd，kill -pgid 探活与 conn 生命周期要真实 socket。）
	pl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pipe listen: %v", err)
	}
	defer pl.Close()
	go func() {
		c, _ := pl.Accept()
		if c != nil {
			defer c.Close()
			io.Copy(io.Discard, c)
		}
	}()
	cliC, err := net.Dial("tcp", pl.Addr().String())
	if err != nil {
		t.Fatalf("pipe dial: %v", err)
	}
	deadClient := newCDPClient(&cdpConn{conn: cliC, br: bufio.NewReader(cliC),
		closed: make(chan struct{}), writeSem: make(chan struct{}, 1)})
	m3.mu.Lock()
	// PID 用一个永远不存在的极大值：tasklist/kill 查活立刻返回死，
	// killBrowserProcessGroup 的优雅段秒退，第一条清场不被 5s 探活拖死。
	m3.proc = &browserProc{PID: 0x7fffffff, PGID: 0x7fffffff, exitedCh: make(chan struct{})}
	m3.client = deadClient
	m3.started = true
	m3.generation = 7
	m3.mu.Unlock()

	// cleanupBlock 卡住第一条 handleDeadLocked 的锁外 killProcGroup 段：
	// 桩住杀进程钩子让第一条进清理后真实阻塞在可控点，第二条同快照并发
	// 到达时第一条仍在「清理中」（cleaning 已置、gen 已+1）必须放弃。
	// 这是「可控测试钩子 + 真实并发调用」——不是手改状态冒充执行。
	cleanupBlock := make(chan struct{})
	enteredKill := make(chan struct{}, 1)
	oldKill := killProcGroupFn
	defer func() { killProcGroupFn = oldKill }()
	killProcGroupFn = func(pid, pgid int, d time.Duration) {
		select {
		case enteredKill <- struct{}{}:
		default:
		}
		<-cleanupBlock // 第一条清理真卡在杀进程段，直到测试放行
	}
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		m3.handleDeadLocked(deadClient, 7)
	}()
	// 等第一条认领并置 cleaning + 真进入锁外清理的杀进程段。
	select {
	case <-enteredKill:
	case <-time.After(2 * time.Second):
		t.Fatal("first handleDeadLocked did not reach killProcGroup")
	}
	m3.mu.Lock()
	if !m3.cleaning {
		m3.mu.Unlock()
		t.Fatal("first handleDeadLocked did not claim cleaning")
	}
	m3.mu.Unlock()
	// 期间 ensureRunning 必须被 cleaning 拒绝。
	if err := m3.ensureRunning(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "cleaning") {
		t.Fatalf("ensureRunning during cleanup = %v, want cleaning rejection", err)
	}
	// 第二条并发清理拿旧快照来：client/gen 不匹配 → 放弃，不得再复位一次。
	m3.handleDeadLocked(deadClient, 7)
	m3.mu.Lock()
	if m3.generation != 8 {
		m3.mu.Unlock()
		t.Fatalf("generation advanced twice: got %d, want 8", m3.generation)
	}
	m3.mu.Unlock()
	close(cleanupBlock) // 放行第一条清理完成
	<-firstDone

	// ── startCancel 两轮生命周期：第 1 轮慢启动被 Stop() 取消后 doStart
	// 仍迟到返回（模拟不可瞬间打断的启动段），期间第 2 轮启动必须拿到
	// 自己完好的 startCancel，且能被第二次 Stop() 真取消——旧轮迟到
	// 退出绝不能清掉新轮的取消句柄（那是「把新轮 Stop 的中断能力抹掉」）。
	m4 := &browserManager{
		readyCh:   make(chan struct{}),
		streams:   map[string]*browserStream{},
		switchSem: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
	ctx1Cancelled := make(chan struct{})
	ctx1LateReturn := make(chan struct{})
	allowRound1Return := make(chan struct{})
	ctx2Cancelled := make(chan struct{})
	var roundIdx atomic.Int64
	oldDoStart := doStartFn
	defer func() { doStartFn = oldDoStart }()
	doStartFn = func(mgr *browserManager, sctx context.Context) startResult {
		round := roundIdx.Add(1)
		if round == 1 {
			// 第 1 轮：startCtx 取消后**不立刻返回**，由测试放行才交回
			// ——这段时间第 2 轮已在登记自己的 startCancel，旧轮迟到
			// 退出绝不能清掉新轮的取消句柄。
			<-sctx.Done()
			close(ctx1Cancelled)
			<-allowRound1Return
			close(ctx1LateReturn)
			return startResult{err: browserErr("round1 cancelled-late")}
		}
		// 第 2 轮：等 startCtx 真被取消才醒（不靠测试放行——第二次
		// Stop 若不生效这里就醒不了，断言自然超时）。
		<-sctx.Done()
		close(ctx2Cancelled)
		return startResult{err: browserErr("round2 finished")}
	}
	// 任一断言失败也必须放行两轮 doStart 的阻塞点，避免 goroutine 泄漏。
	defer func() {
		select {
		case <-allowRound1Return:
		default:
			close(allowRound1Return)
		}
		select {
		case <-ctx2Cancelled:
		default:
			close(ctx2Cancelled)
		}
	}()
	go func() { _ = m4.ensureRunning(context.Background()) }()
	for i := 0; i < 400; i++ {
		m4.mu.Lock()
		has := m4.startCancel != nil
		m4.mu.Unlock()
		if has {
			break
		}
		time.Sleep(time.Millisecond)
	}
	m4.mu.Lock()
	if m4.startCancel == nil {
		m4.mu.Unlock()
		t.Fatal("round1 startCancel not registered at ensureRunning claim")
	}
	m4.mu.Unlock()
	// 第一次 Stop：用足够大的预算（2s）让它走到「等 readyCh」段——
	// 在那里 doStart 的 ctx 已被取消但 doStart 仍卡在放行门前，Stop
	// 会拿着锁等 startOnce 结束。我们在 Stop 还持锁等 readyCh 时，从
	// 锁外直接发起第 2 轮启动：第 2 轮在旧轮 startOnce 存活期间就
	// 登记好自己的 startCancel（旧轮 startCtx 已取消，ensureRunning
	// 见 started/starting 还在但 startErr==nil 会等 readyCh——而
	// readyCh 属于第 1 轮，不能复用；必须靠 readyCh 关闭后的下一轮
	// 认领）。为把第 2 轮 startCancel 钉在旧轮存活期内，先放行 doStart
	// 让 startOnce 退出、再用下面的 readyCh 观察窗发起第 2 轮。
	sctxStop1, stopCancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel1()
	m4.Stop(sctxStop1)
	select {
	case <-ctx1Cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("first Stop did not cancel round1 startCtx")
	}
	select {
	case <-ctx1LateReturn:
		t.Fatal("round1 doStart returned before release (test gate broken)")
	default:
	}
	// 放行第 1 轮 doStart 交回，并等它的 startOnce 完全退出（starting
	// 复位）。注意：旧轮按代际校验跳过时不会清 startCancel——字段里
	// 可能仍留第 1 轮的过期句柄，直到下一轮认领覆盖。
	close(allowRound1Return)
	select {
	case <-ctx1LateReturn:
	case <-time.After(2 * time.Second):
		t.Fatal("round1 doStart did not late-return after release")
	}
	for i := 0; i < 400; i++ {
		m4.mu.Lock()
		idle := !m4.starting
		m4.mu.Unlock()
		if idle {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// 第 2 轮：第 1 轮已完全退出后正常发起。认领信号看 generation 前进
	// （gen++ 与 startCancel 登记同锁原子）——不能只看 startCancel 非
	// nil，第 1 轮的过期句柄没被清掉时会误认。
	m4.mu.Lock()
	genBefore2 := m4.generation
	m4.mu.Unlock()
	go func() { _ = m4.ensureRunning(context.Background()) }()
	var cancel2 context.CancelFunc
	for i := 0; i < 400; i++ {
		m4.mu.Lock()
		if m4.generation > genBefore2 {
			cancel2 = m4.startCancel
		}
		m4.mu.Unlock()
		if cancel2 != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if cancel2 == nil {
		t.Fatal("round2 startCancel not registered after round1 exited")
	}
	// 第二次 Stop：取消的是第 2 轮的句柄——若第 1 轮 startOnce 退出把
	// m.startCancel 清 nil（即未按代际区分），这次 Stop 就抓不到句柄，
	// ctx2Cancelled 必超时。
	sctxStop2, stopCancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer stopCancel2()
	m4.Stop(sctxStop2)
	select {
	case <-ctx2Cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("second Stop did not cancel round2 startCtx (stale startCancel clobbered new round)")
	}
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
	if h := diagHint(reasonBrowserUnavailable, nil); !strings.Contains(h, "browser_path") {
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

// TestProcessIsOurBrowserCmdlineMatch：三平台命令行纯函数精确判据——
// exeNameOK 白名单、Linux NUL 分隔精确比对、Darwin 含空格 Chrome/profile、
// Windows 引号/空格拆词与前缀碰撞（/tmp/p vs /tmp/p-other、
// C:\data\p vs C:\data\p-other、带引号含空格路径）。不再用 /bin/sh 真进程。
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
		{"chrome.exe", true},
		{"msedge.exe", true},
		{"/usr/bin/msedge", true},
		{"/usr/bin/microsoft-edge", true},
		{"/usr/bin/brave", true},
		{"/usr/bin/firefox", false},
		{"/bin/sh", false},
		{"/usr/bin/python3", false},
	} {
		if got := exeNameOK(ok.word); got != ok.want {
			t.Errorf("exeNameOK(%q) = %v, want %v", ok.word, got, ok.want)
		}
	}

	// ── Linux：NUL 分隔 cmdline 逐字段精确比对 ──
	linuxWant := "/tmp/gw/browser-profile"
	for _, c := range []struct {
		name string
		raw  string
		want bool
	}{
		{"exact", "/usr/bin/chromium\x00--headless=new\x00--user-data-dir=/tmp/gw/browser-profile\x00", true},
		{"prefix-collision", "/usr/bin/chromium\x00--user-data-dir=/tmp/gw/browser-profile-other\x00", false},
		{"prefix-collision-reverse", "/usr/bin/chromium\x00--user-data-dir=/tmp/gw/browser-profil\x00", false},
		{"wrong-exe", "/usr/bin/firefox\x00--user-data-dir=/tmp/gw/browser-profile\x00", false},
		{"sh-interpreter", "/bin/sh\x00-c\x00--user-data-dir=/tmp/gw/browser-profile\x00", false},
		{"empty", "", false},
		{"trailing-slash", "/usr/bin/chromium\x00--user-data-dir=/tmp/gw/browser-profile/\x00", true},
		{"separate-arg", "/usr/bin/chromium\x00--user-data-dir\x00/tmp/gw/browser-profile\x00", true},
	} {
		if got := linuxCmdlineIsOurBrowser([]byte(c.raw), linuxWant); got != c.want {
			t.Errorf("linuxCmdlineIsOurBrowser[%s] = %v, want %v", c.name, got, c.want)
		}
	}

	// ── Darwin：ps -ww command= 按 " --" 边界切，含空格 exe 段 ──
	darwinWant := "/Users/me/Library/Application Support/gw/browser-profile"
	for _, c := range []struct {
		name string
		line string
		want bool
	}{
		{"chrome-with-spaces",
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome --user-data-dir=/Users/me/Library/Application Support/gw/browser-profile --headless=new", true},
		{"firefox-imposter",
			"/usr/bin/firefox --user-data-dir=/Users/me/Library/Application Support/gw/browser-profile", false},
		{"other-profile",
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome --user-data-dir=/other/profile --headless=new", false},
		{"profile-prefix-collision",
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome --user-data-dir=/Users/me/Library/Application Support/gw/browser-profile-other --headless=new", false},
		{"no-flag", "/usr/bin/sleep 60", false},
	} {
		if got := darwinCmdlineIsOurBrowser(c.line, darwinWant); got != c.want {
			t.Errorf("darwinCmdlineIsOurBrowser[%s] = %v, want %v", c.name, got, c.want)
		}
	}

	// ── Windows：引号/空格拆词 + EqualFold + 前缀碰撞 ──
	winWant := `C:\data\browser-profile`
	for _, c := range []struct {
		name    string
		cmdline string
		want    bool
	}{
		{"plain", `C:\Google\Chrome\Application\chrome.exe --user-data-dir=C:\data\browser-profile --headless=new`, true},
		{"quoted-exe", `"C:\Program Files\Google\Chrome\Application\chrome.exe" --user-data-dir=C:\data\browser-profile --headless=new`, true},
		{"quoted-value", `chrome.exe --user-data-dir="C:\data\browser-profile" --headless=new`, true},
		{"quoted-value-spaces", `chrome.exe "--user-data-dir=C:\data\browser-profile" --headless=new`, true},
		{"separate-arg-quoted", `chrome.exe --user-data-dir "C:\data\browser-profile" --headless=new`, true},
		{"case-insensitive-flag-name", `CHROME.EXE --USER-DATA-DIR=c:\data\BROWSER-PROFILE --headless=new`, false}, // flag 名大小写敏感（生成方只发小写）
		{"value-case-fold", `chrome.exe --user-data-dir=c:\DATA\BROWSER-PROFILE --headless=new`, true},             // 值 EqualFold
		{"prefix-collision", `chrome.exe --user-data-dir=C:\data\browser-profile-other --headless=new`, false},
		{"prefix-collision-subdir-of-other", `chrome.exe --user-data-dir=C:\data\browser-profile\sub --headless=new`, false},
		{"wrong-exe", `C:\Windows\System32\notepad.exe --user-data-dir=C:\data\browser-profile`, false},
		{"firefox", `C:\Program Files\Mozilla Firefox\firefox.exe --user-data-dir=C:\data\browser-profile`, false},
		{"spaces-in-path", `chrome.exe --user-data-dir="C:\My Data\browser-profile"`, false}, // want 不匹配含空格 profile
		{"no-flag", `chrome.exe --headless=new`, false},
		{"empty", ``, false},
	} {
		if got := windowsCmdlineIsOurBrowser(c.cmdline, winWant); got != c.want {
			t.Errorf("windowsCmdlineIsOurBrowser[%s] = %v, want %v", c.name, got, c.want)
		}
	}
	// 含空格 profile 正值用例（引号包裹后仍须精确命中）。
	winWantSp := `C:\My Data\browser-profile`
	if !windowsCmdlineIsOurBrowser(`chrome.exe --user-data-dir="C:\My Data\browser-profile"`, winWantSp) {
		t.Error("windowsCmdlineIsOurBrowser quoted-space path = false, want true")
	}
	if windowsCmdlineIsOurBrowser(`chrome.exe --user-data-dir="C:\My Data\browser-profile-other"`, winWantSp) {
		t.Error("windowsCmdlineIsOurBrowser quoted-space prefix collision = true, want false")
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

// TestBrowserM1AuxNoSwitch：辅助入口（allowSwitch=false）端到端验证：
//   - 驻留 acct1 时对 acct2 调 getXSRFSwitch(allowSwitch=false) 绝不返回
//     acct1 的 token 也不发进 jar，而是 errBrowserSkipAux 无条件回退纯
//     Go HTTP，且真到达 Go HTTP 传输层（经正向代理见证）；
//   - disabled 账号（cookie 非空但 browserAccountFromHeaders==nil）调
//     getXSRFSwitch 走纯 Go HTTP 真检测，不当匿名假成功；
//   - uploadPostCtxAux(false) 无驻留 + BrowserFallbackHTTP=false 时零启动、
//     零 browserLocalError 直接回退纯 Go HTTP 并真发出；
//   - 身份守卫拒绝的错误文案绝不含原始 SAPISID/__Secure-1PSID 值。
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

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.BrowserFallbackHTTP = false // 默认配置也必退：哨兵不走 fallback 开关
	})

	// shouldSwitch 语义：点名非驻留身份的号 = 需要切（事实），不分 allowSwitch。
	other := accountByID(acctID2)
	need, _ := m.shouldSwitch(other, nil)
	if !need {
		t.Fatal("shouldSwitch(non-resident identity) = false, want true (fact, not policy)")
	}
	resident := accountByID(acctID)
	need2, _ := m.shouldSwitch(resident, nil)
	if need2 {
		t.Fatal("shouldSwitch(resident account) = true, want false")
	}

	// 正向 HTTP 代理（非 CONNECT）：stdlib 客户端把目标当绝对 URI 发过来，
	// 捕获它证明「回退后的请求真到达了 Go HTTP 传输层」而不是只拿到了
	// 某个非 nil error 就放行。
	sawGo := make(chan string, 8)
	fwd := newForwardHTTPProxy(t, sawGo)
	defer fwd.close()
	fwdURL := "http://" + fwd.ln.Addr().String()

	// 端到端：驻留 acct1 时对 acct2 调辅助入口——绝不返回 acct1 token、
	// 绝不把 fetch 发进驻留 jar，而是回退纯 Go HTTP 并真发出。
	tok, err := getXSRFSwitch(other.Cookie, other.AuthUser, fwdURL, false)
	if err == nil {
		t.Fatalf("aux xsrf for non-resident account succeeded via browser jar (token=%q)", tok)
	}
	if isBrowserLocalError(err) {
		t.Fatalf("aux skip misclassified as browserLocalError: %v (%T)", err, err)
	}
	// 假服务端绝不能收到任何 fetch（jar 没借给 acct2）。
	select {
	case spec := <-srv.fetchHit:
		t.Fatalf("browser fetch dispatched to resident jar for non-resident aux request: %s", spec.url)
	case <-time.After(300 * time.Millisecond):
	}
	// 回退的请求真到达了 Go HTTP 传输层（正向代理收到 absolute-URI 请求）。
	select {
	case u := <-sawGo:
		if !strings.Contains(u, "gemini.google.com") {
			t.Fatalf("forward proxy saw non-gemini request: %q", u)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("aux xsrf fallback did not reach Go HTTP transport (no request at forward proxy)")
	}

	// disabled 账号：cookie 非空但 browserAccountFromHeaders==nil（status=
	// disabled 反查不到 enabled 记录）——绝不能把 acct=nil 传进浏览器当
	// 匿名假成功，必须走纯 Go HTTP 用原 cookie 真检测（经正向代理见证）。
	disID, err := accountInsert("disabled", "SAPISID=da; __Secure-1PSID=dp; SID=d", "")
	if err != nil {
		t.Fatalf("insert disabled: %v", err)
	}
	defer accountDelete(disID)
	if err := accountSetStatus(disID, "disabled"); err != nil {
		t.Fatalf("setStatus: %v", err)
	}
	dis := accountByID(disID)
	if dis == nil || dis.Status != "disabled" {
		t.Fatal("disabled account not persisted")
	}
	// browserAccountFromHeaders 对该 cookie 必须返回 nil（disabled 反查不到）。
	if got := browserAccountFromHeaders(map[string]string{"Cookie": dis.Cookie}); got != nil {
		t.Fatalf("disabled cookie unexpectedly resolved to %+v", got)
	}
	// 走纯 Go HTTP 真检测：不当匿名假成功（代理见证请求真发出）。
	_, err = getXSRFSwitch(dis.Cookie, dis.AuthUser, fwdURL, false)
	if err == nil {
		t.Fatal("disabled-account xsrf succeeded anonymously (acct=nil passed to browser as anon)")
	}
	select {
	case u := <-sawGo:
		if !strings.Contains(u, "gemini.google.com") {
			t.Fatalf("forward proxy saw non-gemini request for disabled account: %q", u)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("disabled-account xsrf did not reach Go HTTP transport")
	}
	// 假服务端仍绝不能收到 fetch（disabled 的 acct 没进 jar）。
	select {
	case spec := <-srv.fetchHit:
		t.Fatalf("browser fetch dispatched for disabled account: %s", spec.url)
	case <-time.After(200 * time.Millisecond):
	}

	// 辅助 POST（uploadPostCtxAux allowSwitch=false）无驻留 + BrowserFallbackHTTP=
	// false：入口前置 auxJarBlocked 直接 errBrowserSkipAux 回退纯 Go HTTP——
	// 零启动（不冷启 Chromium）、零 browserLocalError（不误报 502），并真
	// 把请求发到了 Go HTTP 传输层。
	m.mu.Lock()
	m.seed = browserSeedState{}
	m.mu.Unlock()
	st, _, _, perr := uploadPostCtxAux(context.Background(),
		"https://gemini.google.com/_/x", map[string]string{"Cookie": other.Cookie},
		[]byte("x=1"), fwdURL, false)
	if perr == nil && st == 0 {
		t.Fatal("aux POST returned no error and no status")
	}
	if isBrowserLocalError(perr) {
		t.Fatalf("aux POST misclassified as browserLocalError (would be 502): %v", perr)
	}
	select {
	case u := <-sawGo:
		if !strings.Contains(u, "gemini.google.com") {
			t.Fatalf("forward proxy saw non-gemini POST: %q", u)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("aux POST did not reach Go HTTP transport")
	}
	// 浏览器绝不能被冷启动（proc 仍是无启动状态）。
	m.mu.Lock()
	cold := m.started && m.starting
	m.mu.Unlock()
	if cold {
		t.Fatal("aux POST cold-started the browser (must skip ensureRunning)")
	}

	// 身份守卫拒绝的错误文案：绝不包含任何原始 SAPISID / __Secure-1PSID 值。
	// 异身份 + allowSwitch=false：守卫返回 errBrowserSkipAux；异身份 +
	// allowSwitch=true 时守卫的 browserErr 文案也只许带账号 ID（整数），
	// 绝不带原始 cookieIdentity。先验 skip 分支文案，再验真守卫拒斥路径
	// 不带凭据（同身份不同 authuser 才过守卫，异身份必在守卫前被拒，
	// 两条路径的错误文案都必须零凭据）。
	alienCookie := "SAPISID=xa; __Secure-1PSID=xp; SID=x"
	alienID, err := accountInsert("alien", alienCookie, "")
	if err != nil {
		t.Fatalf("insert alien: %v", err)
	}
	defer accountDelete(alienID)
	alien := accountByID(alienID)
	m.mu.Lock()
	m.seed = browserSeedState{AccountID: acctID,
		CookieIdentity: cookieIdentity("SAPISID=ra; __Secure-1PSID=rp; SID=r")}
	m.mu.Unlock()
	// aux 路径（allowSwitch=false）：errBrowserSkipAux，且文案零凭据。
	_, _, gerr := m.doBrowserStreamRequest(context.Background(), alien,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), false, nil)
	if gerr == nil {
		t.Fatal("identity guard did not reject mismatched cookieIdentity (aux)")
	}
	for _, leak := range []string{"ra", "rp", "xa", "xp", "oa", "op", "SAPISID", "__Secure-1PSID"} {
		if strings.Contains(gerr.Error(), leak) {
			t.Fatalf("aux-guard error leaks credential fragment %q: %v", leak, gerr)
		}
	}
	// 真实触发生产守卫的 mismatch 文案（allowSwitch=true）：把驻留种子
	// 置为「并发切换窗口里被换成另一身份」的状态（种子记的是 acct1 的
	// AccountID、CookieIdentity 却已是 alien 的——等价于请求自己的切号
	// 完成后另一轮切换又把 jar 换成了异身份），点名 resident 的请求走
	// 完整生产路径 doBrowserStreamRequestCtx（shouldSwitch→maybeSwitchAccount
	// 同 ID 快路径→ensureBridge→jarRW 读锁→browser.go 最终身份守卫），
	// 拿生产返回的真实 err 断言：含 "jar identity mismatch" 且绝不含
	// 任何原始 SAPISID/__Secure-1PSID 值（守卫文案只带账号 ID）。
	m.mu.Lock()
	m.seed = browserSeedState{AccountID: acctID,
		CookieIdentity: cookieIdentity(alien.Cookie)}
	m.mu.Unlock()
	_, _, gerr2 := m.doBrowserStreamRequestCtx(context.Background(), "", resident,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
	if gerr2 == nil {
		t.Fatal("identity guard did not reject mismatched resident identity (main path)")
	}
	if !strings.Contains(gerr2.Error(), "jar identity mismatch") {
		t.Fatalf("guard error = %v, want jar identity mismatch", gerr2)
	}
	for _, leak := range []string{"ra", "rp", "xa", "xp", "oa", "op", "SAPISID", "__Secure-1PSID"} {
		if strings.Contains(gerr2.Error(), leak) {
			t.Fatalf("guard error leaks credential fragment %q: %v", leak, gerr2)
		}
	}
	// 守卫在 fetch 派发前拒：假服务端绝不能收到该请求的 fetch。
	select {
	case spec := <-srv.fetchHit:
		t.Fatalf("fetch dispatched after identity-guard rejection: %s", spec.url)
	case <-time.After(200 * time.Millisecond):
	}
}

// forwardHTTPProxy 是测试用的正向 HTTP 代理（非 CONNECT）：stdlib 客户端把
// absolute-URI 请求发过来，我们记下目标 URI 后回 502——只为见证「回退后
// 的请求真到达了 Go HTTP 传输层」。
type forwardHTTPProxy struct {
	ln  net.Listener
	saw chan<- string
}

func newForwardHTTPProxy(t *testing.T, saw chan<- string) *forwardHTTPProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fwd proxy listen: %v", err)
	}
	p := &forwardHTTPProxy{ln: ln, saw: saw}
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

func (p *forwardHTTPProxy) handle(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	uri := req.URL.String()
	if uri == "" {
		uri = req.Host + req.URL.Path
	}
	select {
	case p.saw <- uri:
	default:
	}
	io.WriteString(c, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
}

func (p *forwardHTTPProxy) close() { p.ln.Close() }

// TestBrowserM1MultiAuthUser：同 cookieIdentity 不同 authuser 不触发
// clearBrowserCookies 与 30s 冷却；且端到端 doBrowserStreamRequest 验证
// 不同 AccountID（DB 记录不同）但 Cookie 同串可合法共用驻留 jar。
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

	// 端到端：驻留 seed 是 ID=7，请求点名 ID=8 但 cookieIdentity 相同——
	// 守卫必须放行（按 Cookie 身份核验，不按 DB 记录 ID），fetch 发出并
	// 返回 200 正文。
	m.mu.Lock()
	m.seed.AccountID = 7 // 恢复一个「DB 记录不同」的驻留 ID
	m.mu.Unlock()
	go func() {
		spec := <-srv.fetchHit
		srv.setFetchScript(spec.reqID, fetchScript{status: 200, chunks: [][]byte{[]byte("shared-jar-body")}})
	}()
	acct8 := &CookieAccount{ID: 8, Cookie: cookie, AuthUser: 1, Status: "enabled"}
	status, rc, err := m.doBrowserStreamRequest(context.Background(), acct8,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), true, nil)
	if err != nil {
		t.Fatalf("same-identity different AccountID stream rejected: %v", err)
	}
	defer rc.Close()
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	body, _ := io.ReadAll(rc)
	if string(body) != "shared-jar-body" {
		t.Fatalf("body = %q, want shared-jar-body", body)
	}
	// 对照：异 cookieIdentity 的号无论 allowSwitch 都绝不进 jar（守卫拒绝）。
	m.mu.Lock()
	m.seed.AccountID = 7
	m.mu.Unlock()
	alien := &CookieAccount{ID: 9, Cookie: "SAPISID=xa; __Secure-1PSID=xp; SID=x", Status: "enabled"}
	_, _, err = m.doBrowserStreamRequest(context.Background(), alien,
		"https://gemini.google.com/_/x", "POST", nil, []byte("x"), false, nil)
	if !isBrowserAuxSkip(err) {
		t.Fatalf("alien-identity aux request err = %v, want errBrowserSkipAux", err)
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

	m.handleDeadLocked(m.client, m.generation)

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

// TestBrowserBridgeSwitchCancelRestoresSameExit：跨出口等待者被 ctx
// 取消后，最后一名取消必须撤销切换阻塞状态，同出口健康请求立刻恢复
// 自由并发（不被已离开的等待者挡死）。
func TestBrowserBridgeSwitchCancelRestoresSameExit(t *testing.T) {
	m := &browserManager{
		readyCh:   make(chan struct{}),
		streams:   map[string]*browserStream{},
		switchSem: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
	// 请求 A 持有出口 B。
	if err := m.acquireBridgeExit(context.Background(), "http://proxy-b:8080", true); err != nil {
		t.Fatalf("acquire exit B: %v", err)
	}
	defer m.releaseBridgeExit()
	// 请求 D 以短 ctx 排队切到出口 C，必然超时退出。
	dCtx, dCancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer dCancel()
	dDone := make(chan error, 1)
	go func() { dDone <- m.acquireBridgeExit(dCtx, "http://proxy-c:8080", true) }()
	if err := <-dDone; err == nil {
		t.Fatal("cross-exit waiter D acquired without waiting for in-flight drain")
	}
	// D 离开后，同出口请求 E 必须立刻并发成功（<100ms），不被 A 挡死，
	// 也不被 D 残留的切换阻塞状态暂缓。
	t0 := time.Now()
	eDone := make(chan error, 1)
	go func() { eDone <- m.acquireBridgeExit(context.Background(), "http://proxy-b:8080", true) }()
	select {
	case err := <-eDone:
		if err != nil {
			t.Fatalf("same-exit acquire E failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("same-exit acquire E blocked after cancelled cross-exit waiter left (bridgeSwitchWaiters leak)")
	}
	if dt := time.Since(t0); dt >= 100*time.Millisecond {
		t.Fatalf("same-exit acquire E took %v, want <100ms", dt)
	}
	m.releaseBridgeExit()
}

// TestBrowserBridgeAcquireCancelledCtxNoDeadlock：acquireBridgeExit 以已
// 取消 ctx 调用必须立刻返回 context.Canceled（<100ms）——绝不因持锁
// return 后 defer 重入 bridgeMu 而同协程自死锁。空闲态与占用态各验
// 一次：占用态用后台协程包裹断言「不阻塞」，否则死锁会挂住测试进程。
func TestBrowserBridgeAcquireCancelledCtxNoDeadlock(t *testing.T) {
	newMgr := func() *browserManager {
		return &browserManager{
			readyCh:   make(chan struct{}),
			streams:   map[string]*browserStream{},
			switchSem: make(chan struct{}, 1),
			stopCh:    make(chan struct{}),
		}
	}

	// 空闲态：无在飞流，已取消 ctx 仍必须立即返回 Canceled。
	m := newMgr()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t0 := time.Now()
	if err := m.acquireBridgeExit(ctx, "http://proxy-b:8080", true); !errors.Is(err, context.Canceled) {
		t.Fatalf("idle cancelled acquire err = %v, want context.Canceled", err)
	}
	if dt := time.Since(t0); dt >= 100*time.Millisecond {
		t.Fatalf("idle cancelled acquire took %v, want <100ms", dt)
	}

	// 占用态：请求 A 持有出口 B，已取消 ctx 的跨出口请求 D 必须立刻
	// 返回而不是重入 bridgeMu 死锁。
	m = newMgr()
	if err := m.acquireBridgeExit(context.Background(), "http://proxy-b:8080", true); err != nil {
		t.Fatalf("acquire exit B: %v", err)
	}
	defer m.releaseBridgeExit()
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	dDone := make(chan error, 1)
	t0 = time.Now()
	go func() { dDone <- m.acquireBridgeExit(ctx2, "http://proxy-c:8080", true) }()
	select {
	case err := <-dDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("busy cancelled acquire err = %v, want context.Canceled", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("busy cancelled acquire blocked >100ms (bridgeMu re-entry deadlock)")
	}
	if dt := time.Since(t0); dt >= 100*time.Millisecond {
		t.Fatalf("busy cancelled acquire took %v, want <100ms", dt)
	}
}

// TestBrowserBridgeSameExitStalledDuringSwitchUnblocked：跨出口等待者 D
// 排队期间，同出口请求 E 被暂缓但绝不登记 bridgeSwitchWaiters；D 被
// ctx 取消后计数必须归零并广播，E 立刻并发获取出口 B（无需等 A 释
// 放）——不被已离开的等待者挡死。
func TestBrowserBridgeSameExitStalledDuringSwitchUnblocked(t *testing.T) {
	m := &browserManager{
		readyCh:   make(chan struct{}),
		streams:   map[string]*browserStream{},
		switchSem: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
	// 请求 A 持有出口 B。
	if err := m.acquireBridgeExit(context.Background(), "http://proxy-b:8080", true); err != nil {
		t.Fatalf("acquire exit B: %v", err)
	}
	defer m.releaseBridgeExit()

	// 请求 D 排队切到出口 C（ctx 可手动取消），确认其进入等待。
	dCtx, dCancel := context.WithCancel(context.Background())
	defer dCancel()
	dDone := make(chan error, 1)
	go func() { dDone <- m.acquireBridgeExit(dCtx, "http://proxy-c:8080", true) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		m.bridgeMu.Lock()
		waiters := m.bridgeSwitchWaiters
		m.bridgeMu.Unlock()
		if waiters == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cross-exit waiter D not registered, bridgeSwitchWaiters = %d", waiters)
		}
		time.Sleep(time.Millisecond)
	}

	// 同出口请求 E 申请出口 B：D 在等时 E 必须被暂缓（不立刻返回），
	// 且不占 bridgeSwitchWaiters 名额。
	eDone := make(chan error, 1)
	go func() { eDone <- m.acquireBridgeExit(context.Background(), "http://proxy-b:8080", true) }()
	select {
	case err := <-eDone:
		t.Fatalf("same-exit E returned early with %v while cross-exit waiter pending (should be stalled)", err)
	case <-time.After(30 * time.Millisecond):
	}
	m.bridgeMu.Lock()
	waiters := m.bridgeSwitchWaiters
	m.bridgeMu.Unlock()
	if waiters != 1 {
		t.Fatalf("stalled same-exit E registered as switch waiter, bridgeSwitchWaiters = %d, want 1", waiters)
	}

	// 取消 D 的 ctx：D 撤销最后一份登记，广播唤醒 E；E 必须在 <100ms
	// 内拿到出口 B（在 A 未释放时即可并发），D 返回 Canceled。
	t0 := time.Now()
	dCancel()
	select {
	case err := <-eDone:
		if err != nil {
			t.Fatalf("same-exit acquire E failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("same-exit acquire E blocked after cancelled cross-exit waiter left (bridgeSwitchWaiters leak)")
	}
	if dt := time.Since(t0); dt >= 100*time.Millisecond {
		t.Fatalf("same-exit acquire E took %v, want <100ms", dt)
	}
	select {
	case err := <-dDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cross-exit waiter D err = %v, want context.Canceled", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cancelled cross-exit waiter D did not return within 100ms")
	}
	m.releaseBridgeExit()
}

// TestBrowserBridgeSwitchCompletionClearsWaitersAndAllowsSameExit：两个
// 同目标跨出口等待者 D1/D2 被在飞释放广播唤醒后，D1 认领切换出口 C
// 必须撤销自己登记名额，D2 在退化路径（activeURL 已是 C）撤销最后
// 一份登记后必须当轮走同出口自由并发——绝不误入新建 bridgeWait 永久
// 挂起；全部登记归零后新同出口请求 E 立刻自由并发。
func TestBrowserBridgeSwitchCompletionClearsWaitersAndAllowsSameExit(t *testing.T) {
	m := &browserManager{
		readyCh:   make(chan struct{}),
		streams:   map[string]*browserStream{},
		switchSem: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
	// 请求 A 持有出口 B。
	if err := m.acquireBridgeExit(context.Background(), "http://proxy-b:8080", true); err != nil {
		t.Fatalf("acquire exit B: %v", err)
	}
	defer m.releaseBridgeExit()
	// 跨出口等待者 D1、D2 排队切到出口 C，确认两份登记。
	d1Done := make(chan error, 1)
	d2Done := make(chan error, 1)
	go func() { d1Done <- m.acquireBridgeExit(context.Background(), "http://proxy-c:8080", true) }()
	go func() { d2Done <- m.acquireBridgeExit(context.Background(), "http://proxy-c:8080", true) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		m.bridgeMu.Lock()
		waiters := m.bridgeSwitchWaiters
		m.bridgeMu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cross-exit waiters not registered, bridgeSwitchWaiters = %d, want 2", waiters)
		}
		time.Sleep(time.Millisecond)
	}
	// A 释放在飞唤醒 D1/D2：两者都必须在 <100ms 内拿到出口 C
	//（D1 认领路径撤销登记；D2 退化路径撤销登记后当轮放行，不挂起），
	// 且登记计数必须归零（无泄漏）。
	t0 := time.Now()
	m.releaseBridgeExit()
	for i, ch := range []chan error{d1Done, d2Done} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("cross-exit waiter D%d failed: %v", i+1, err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("cross-exit waiter D%d blocked >100ms after in-flight drain (hang on degenerate path)", i+1)
		}
	}
	if dt := time.Since(t0); dt >= 100*time.Millisecond {
		t.Fatalf("cross-exit waiters took %v, want <100ms", dt)
	}
	m.bridgeMu.Lock()
	waiters := m.bridgeSwitchWaiters
	m.bridgeMu.Unlock()
	if waiters != 0 {
		t.Fatalf("bridgeSwitchWaiters = %d after switch completed, want 0 (count leak)", waiters)
	}
	// D1/D2 均未释放时新同出口请求 E 申请出口 C：登记已归零，必须
	// 立刻自由并发。
	t0 = time.Now()
	eDone := make(chan error, 1)
	go func() { eDone <- m.acquireBridgeExit(context.Background(), "http://proxy-c:8080", true) }()
	select {
	case err := <-eDone:
		if err != nil {
			t.Fatalf("same-exit acquire E failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("same-exit acquire E blocked >100ms (residual switch-waiter state)")
	}
	if dt := time.Since(t0); dt >= 100*time.Millisecond {
		t.Fatalf("same-exit acquire E took %v, want <100ms", dt)
	}
	m.releaseBridgeExit()
	m.releaseBridgeExit()
	m.releaseBridgeExit()
}

// TestCDPSendTextCtxCancelDuringBlockedWrite：持写权阻塞写（对端不读的
// net.Pipe）期间取消无 deadline 的 ctx，必须毫秒级返回 ctx.Err() 且
// c.closed 终态关闭——绝不干等 5s 写超时，残帧连接也绝不复用。
func TestCDPSendTextCtxCancelDuringBlockedWrite(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	// 对端永不读：conn.Write 必然阻塞在管道上。
	c := &cdpConn{
		conn:     client,
		br:       bufio.NewReader(client),
		writeSem: make(chan struct{}, 1),
		closed:   make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	payload := make([]byte, 1<<20) // 足够大，单次 Write 一定卡住
	done := make(chan error, 1)
	go func() { done <- c.sendTextCtx(ctx, payload) }()
	// 等写方真持权进 write（poll writeSem 满）。
	for i := 0; i < 400; i++ {
		if len(c.writeSem) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	t0 := time.Now()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("sendTextCtx returned nil after ctx cancel during blocked write")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("sendTextCtx err = %v, want context.Canceled", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("sendTextCtx blocked %v after cancel (must not wait 5s write timeout)", time.Since(t0))
	}
	select {
	case <-c.closed:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("cdpConn not terminally closed after ctx cancel during write (residual frame conn would be reused)")
	}
}

// failAfterFirstWriteConn 最小桩 net.Conn：第 1 次 Write（帧头）放行，
// 第 2 次 Write（payload）返回 0 + ErrDeadlineExceeded，模拟「帧已写一半
// 超时」的残帧场景；之后计数，便于断言连接关闭后不再进底层 Write。
type failAfterFirstWriteConn struct {
	net.Conn
	writes atomic.Int32
}

func (c *failAfterFirstWriteConn) Write(p []byte) (int, error) {
	if c.writes.Add(1) >= 2 {
		return 0, os.ErrDeadlineExceeded
	}
	return len(p), nil
}

func (c *failAfterFirstWriteConn) Close() error                     { return nil }
func (c *failAfterFirstWriteConn) SetDeadline(time.Time) error      { return nil }
func (c *failAfterFirstWriteConn) SetReadDeadline(time.Time) error  { return nil }
func (c *failAfterFirstWriteConn) SetWriteDeadline(time.Time) error { return nil }

// TestCDPSendWriteErrClosesConn：R6 回归——sendTextCtx / sendPong 在非 ctx
// 取消路径写帧失败（5s 写超时先于 AfterFunc 触发）时必须终态断连，
// 绝不让残帧连接被下一条消息复用。
func TestCDPSendWriteErrClosesConn(t *testing.T) {
	newConn := func() (*cdpConn, *failAfterFirstWriteConn) {
		stub := &failAfterFirstWriteConn{}
		return &cdpConn{
			conn:     stub,
			br:       bufio.NewReader(stub),
			writeSem: make(chan struct{}, 1),
			closed:   make(chan struct{}),
		}, stub
	}

	// sendTextCtx：写帧失败后返回错误 + closed 终态置位；再调直接报
	// connection closed，不再进底层 Write（writes 停在 2）。
	c, stub := newConn()
	if err := c.sendTextCtx(context.Background(), []byte("payload")); err == nil {
		t.Fatal("sendTextCtx returned nil on write failure, want error")
	}
	select {
	case <-c.closed:
	default:
		t.Fatal("cdpConn not terminally closed after sendTextCtx write failure (residual frame conn would be reused)")
	}
	if err := c.sendTextCtx(context.Background(), []byte("x")); err == nil {
		t.Fatal("sendTextCtx on closed conn returned nil, want connection closed error")
	}
	if got := stub.writes.Load(); got != 2 {
		t.Fatalf("conn.Write called %d times after close, want 2 (closed conn must not reach Write)", got)
	}

	// sendPong：同样语义——写帧失败终态断连。
	c2, _ := newConn()
	if err := c2.sendPong([]byte("pong")); err == nil {
		t.Fatal("sendPong returned nil on write failure, want error")
	}
	select {
	case <-c2.closed:
	default:
		t.Fatal("cdpConn not terminally closed after sendPong write failure")
	}
}

// TestBrowserProxyCheckTargetRejectsUnspecified：R6/R7 回归——CONNECT 目标
// 为未指定地址（0.0.0.0 / [::] / [::ffff:0.0.0.0]）时 checkTarget 必须
// 拒绝：这些地址实际拨到本机端口，和回环一样是 SSRF。R7 追加两类绕过：
// 尾点回环域名（localhost. 会被 DNS 规范化回 localhost，ParseIP/
// HasSuffix 字面比较漏放）与 IPv6 zone id（[::1%lo] 的 % 使 ParseIP
// 返回 nil 而逃逸判定，拨号时 zone 仍指向本机接口）。
func TestBrowserProxyCheckTargetRejectsUnspecified(t *testing.T) {
	b := &browserProxyBridge{port: 19000} // allowLoopback=false（生产默认）
	for _, target := range []string{
		// R6：未指定地址字面形态。
		"0.0.0.0:8000", "[::]:8000", "[::ffff:0.0.0.0]:8000",
		// R7：尾点回环域名——"localhost." 是合法 FQDN 且解析回环，
		// 必须先剥离尾点再做 localhost/HasSuffix 判定。
		"localhost.:8000", "LOCALHOST.:8000", "sub.localhost.:8000",
		// R7：IPv6 zone id——host 含 '%' 时 net.ParseIP 返回 nil，
		// 不拦截则绕过 loopback/unspecified 判定拨到本机接口。
		"[::1%lo]:8000", "[::%lo]:8000", "[::ffff:127.0.0.1%lo]:8000", "[::ffff:0.0.0.0%lo]:8000",
	} {
		if err := b.checkTarget(target); err == nil {
			t.Fatalf("checkTarget(%q) returned nil, want refused (unspecified/loopback bypass reaches local ports = SSRF)", target)
		}
	}
}

// ─── alignColdStartJar / alignResidentAndNavigate 回归（R2 补齐）───────────
//
// M2：冷启动/重建标签页的 Jar 对齐必须 fail-closed——读 Jar 出错、Jar 里有
// 匹配不到启用账号的 Cookie、清 Jar 失败，都绝不带着残留身份去导航；
// 且全部 Network.* 走 page session（fake 断言空 session 的 Network.* 报
// -32601，所以走到这段代码本身就验证了 session 接线）。

func newTestClientAndBridge(t *testing.T, srv *fakeCDPServer) (*cdpClient, *browserProxyBridge) {
	t.Helper()
	conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
	if err != nil {
		t.Fatalf("dial fake cdp: %v", err)
	}
	client := newCDPClient(conn)
	t.Cleanup(func() { client.Close(); conn.Close() })
	bridge, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("newBrowserProxyBridge: %v", err)
	}
	t.Cleanup(func() { bridge.Shutdown(context.Background()) })
	return client, bridge
}

// 陌生 Cookie（匹配不到启用账号）→ 清 Jar + 按匿名首航出口导航。
func TestAlignColdStartJarUnknownCookieCleared(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	client, bridge := newTestClientAndBridge(t, srv)
	// Jar 里有一份不属于任何启用账号的 Cookie。
	srv.cookiesJSON.Store(json.RawMessage(
		`{"cookies":[{"name":"SAPISID","value":"alien"},{"name":"__Secure-1PSID","value":"alienp"}]}`))
	m := &browserManager{}
	res := &startResult{seed: browserSeedState{AccountID: 42}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.alignColdStartJar(ctx, client, "fake-session-1", bridge, "", res); err != nil {
		t.Fatalf("alignColdStartJar: %v", err)
	}
	// 必须见过 clearBrowserCookies（fail-closed）。
	sawClear, sawNav := false, false
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Network.clearBrowserCookies" {
				sawClear = true
				if c.sessionID != "fake-session-1" {
					t.Fatalf("clearBrowserCookies 没带 page session：%q", c.sessionID)
				}
			}
			if c.method == "Page.navigate" {
				sawNav = true
			}
		default:
			goto done
		}
	}
done:
	if !sawClear {
		t.Fatal("陌生 Cookie 未触发 clearBrowserCookies")
	}
	if !sawNav {
		t.Fatal("清 Jar 后未导航 /app")
	}
	if res.seed.AccountID != 0 {
		t.Fatalf("清 Jar 后 seed 应复位，得到 %+v", res.seed)
	}
}

// Jar Cookie 匹配到启用账号 → 不清 Jar、seed 同步为匹配到的账号。
func TestAlignColdStartJarMatchedKeeps(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	client, bridge := newTestClientAndBridge(t, srv)
	cookie := "SAPISID=sa; __Secure-1PSID=ps; SID=x"
	id := addTestAccount(t, "coldMatch")
	// accountAdd 存的 cookie 与 Jar 一致 → cookieIdentity 相同。
	_, _ = getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`, cookie, id)
	srv.cookiesJSON.Store(json.RawMessage(
		`{"cookies":[{"name":"SAPISID","value":"sa"},{"name":"__Secure-1PSID","value":"ps"}]}`))
	m := &browserManager{}
	res := &startResult{seed: browserSeedState{AccountID: 0}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.alignColdStartJar(ctx, client, "fake-session-1", bridge, "", res); err != nil {
		t.Fatalf("alignColdStartJar: %v", err)
	}
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Network.clearBrowserCookies" {
				t.Fatal("匹配账号的 Jar 不该清")
			}
		default:
			goto done2
		}
	}
done2:
	if res.seed.AccountID != id {
		t.Fatalf("seed 应同步为匹配账号 #%d，得到 %+v", id, res.seed)
	}
}

// 匹配到账号但绑定出口已失效 → 清 Jar fail-closed。
func TestAlignColdStartJarBoundDeadClears(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	client, bridge := newTestClientAndBridge(t, srv)
	cookie := "SAPISID=sa2; __Secure-1PSID=ps2; SID=x"
	id := addTestAccount(t, "coldDeadBound")
	_, _ = getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`, cookie, id)
	bindAccountProxy(id, 424242) // 绑定不存在的代理 → coldStartUpstreamForAccount 报错
	srv.cookiesJSON.Store(json.RawMessage(
		`{"cookies":[{"name":"SAPISID","value":"sa2"},{"name":"__Secure-1PSID","value":"ps2"}]}`))
	m := &browserManager{}
	res := &startResult{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.alignColdStartJar(ctx, client, "fake-session-1", bridge, "", res); err != nil {
		t.Fatalf("alignColdStartJar: %v", err)
	}
	sawClear := false
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Network.clearBrowserCookies" {
				sawClear = true
			}
		default:
			goto done3
		}
	}
done3:
	if !sawClear {
		t.Fatal("绑定失效未触发 clearBrowserCookies")
	}
}

// alignResidentAndNavigate：驻留号已删除 → 清 Jar 后才导航；bridgeMu 下的
// bridgeActiveURL/bridgeActiveDirect 必须与 SetUpstream 的实际出口一致。
func TestAlignResidentAndNavigateDeadSeed(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	bridge, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("newBrowserProxyBridge: %v", err)
	}
	defer bridge.Shutdown(context.Background())
	cleanup := wireTestBrowserBridge(t, srv, bridge)
	defer cleanup()
	m := browserMgr
	m.mu.Lock()
	client := m.client
	m.seed = browserSeedState{AccountID: 987654} // 库里不存在
	m.bridgeActiveURL, m.bridgeActiveDirect = "http://stale:1", false
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.alignResidentAndNavigate(ctx, client, "fake-session-1"); err != nil {
		t.Fatalf("alignResidentAndNavigate: %v", err)
	}
	sawClear, sawNav := false, false
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Network.clearBrowserCookies" {
				sawClear = true
				if c.sessionID != "fake-session-1" {
					t.Fatalf("clearBrowserCookies 没带 page session：%q", c.sessionID)
				}
			}
			if c.method == "Page.navigate" {
				sawNav = true
				if !sawClear {
					t.Fatal("导航发生在清 Jar 之前")
				}
			}
		default:
			goto done4
		}
	}
done4:
	if !sawClear {
		t.Fatal("驻留号已删未触发 clearBrowserCookies")
	}
	if !sawNav {
		t.Fatal("未导航 /app")
	}
	m.bridgeMu.Lock()
	url, direct := m.bridgeActiveURL, m.bridgeActiveDirect
	m.bridgeMu.Unlock()
	wantURL, wantDirect := initialBridgeUpstream()
	if url != wantURL || direct != wantDirect {
		t.Fatalf("bridgeActive* 与实际出口不一致：got(%q,%v) want(%q,%v)",
			url, direct, wantURL, wantDirect)
	}
}

// R2-S2 ⑦ alignColdStartJar fail-closed：读 Jar 出错 → 清 Jar 后按匿名出口导航；
// 清 Jar 失败 → 直接中止返回错误，绝不带残留身份导航。
func TestAlignColdStartJarFailClosed(t *testing.T) {
	t.Run("read error clears jar then navigates", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		client, bridge := newTestClientAndBridge(t, srv)
		srv.failMethods.Store("Network.getCookies", true) // 读 Jar 出错
		m := &browserManager{}
		res := &startResult{}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.alignColdStartJar(ctx, client, "fake-session-1", bridge, "", res); err != nil {
			t.Fatalf("读 Jar 出错应清 Jar 后继续：%v", err)
		}
		sawClear, sawNav := false, false
		for {
			select {
			case c := <-srv.gotCalls:
				if c.method == "Network.clearBrowserCookies" {
					sawClear = true
				}
				if c.method == "Page.navigate" {
					sawNav = true
					if !sawClear {
						t.Fatal("导航发生在清 Jar 之前")
					}
				}
			default:
				goto done
			}
		}
	done:
		if !sawClear {
			t.Fatal("读 Jar 出错必须 fail-closed 清 Jar")
		}
		if !sawNav {
			t.Fatal("清 Jar 后应按匿名出口导航 /app")
		}
	})

	t.Run("clear failure aborts", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		client, bridge := newTestClientAndBridge(t, srv)
		srv.cookiesJSON.Store(json.RawMessage(
			`{"cookies":[{"name":"SAPISID","value":"alien"}]}`)) // 陌生 Cookie
		srv.failMethods.Store("Network.clearBrowserCookies", true) // 清 Jar 失败
		m := &browserManager{}
		res := &startResult{seed: browserSeedState{AccountID: 1}}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.alignColdStartJar(ctx, client, "fake-session-1", bridge, "", res); err == nil {
			t.Fatal("清 Jar 失败必须中止冷启动，绝不能带残留身份导航")
		}
		// 绝不能发出 Page.navigate。
		for {
			select {
			case c := <-srv.gotCalls:
				if c.method == "Page.navigate" {
					t.Fatal("清 Jar 失败后绝不该导航")
				}
			default:
				return
			}
		}
	})
}

// navigateAndWaitApp 不得在 about:blank（readyState 恰为 "complete"）上提前返回：
// fake 把 origin 切换延迟到导航之后，若实现只看 readyState 就会误以为已就绪。
// 断言 fake 侧确实收到了导航前注册的 Once 监听所等的那次 loadEventFired——
// 即函数返回时 origin 已切到 gemini.google.com。
func TestNavigateAndWaitAppWaitsForOrigin(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	client, _ := newTestClientAndBridge(t, srv)
	// 导航有真实耗时：origin 在 300ms 后才切到 gemini.google.com。
	srv.pageNavDelay.Store(int64(300 * time.Millisecond))
	m := &browserManager{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.navigateAndWaitApp(ctx, client, "fake-session-1"); err != nil {
		t.Fatalf("navigateAndWaitApp: %v", err)
	}
	if got := srv.pageOrigin.Load().(string); got != "https://gemini.google.com" {
		t.Fatalf("返回时 origin 仍为 %q（在 about:blank 提前返回）", got)
	}
	// 必须真发出了 Page.navigate。
	sawNav := false
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Page.navigate" {
				sawNav = true
			}
		default:
			goto done
		}
	}
done:
	if !sawNav {
		t.Fatal("未发出 Page.navigate")
	}
}

// R3-M4 回归：Once 的 cancel 必须按注册 id 精确摘除——同一函数字面量求值
// 出的两个闭包共享相同代码指针，旧实现按 reflect 指针比较会在 cancel2()
// 时误删先注册的 ch1 监听器（ch1 永久丢唤醒、ch2 反而收到事件）。
// OnceSession 另须按 session 过滤：别的 session 发出同名事件不得摘除本
// session 的监听器。
func TestOnceCancelSameLiteralClosureDoesNotDropPeer(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
	if err != nil {
		t.Fatalf("dial fake cdp: %v", err)
	}
	client := newCDPClient(conn)
	defer func() { client.Close(); conn.Close() }()

	// reg 是唯一闭包构造点：ch1/ch2 的监听器由同一函数字面量生成，
	// reflect.ValueOf(fn).Pointer() 恒相等——这正是 R3-M4 的触发形态。
	reg := func(ch chan struct{}) func() {
		return client.OnceSession("sess-1", "Test.Evt", func(evt cdpEvent) {
			select {
			case ch <- struct{}{}:
			default:
			}
		})
	}
	ch1, ch2 := make(chan struct{}, 1), make(chan struct{}, 1)
	cancel1 := reg(ch1)
	defer cancel1()
	cancel2 := reg(ch2)
	cancel2() // 只摘 ch2，绝不能误删 ch1

	// 本 session 事件：ch1 收到，ch2（已取消）收不到。
	srv.pushEvent("sess-1", "Test.Evt", map[string]interface{}{})
	select {
	case <-ch1:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel2() 误删了 ch1 监听器（同字面量闭包指针碰撞）")
	}
	select {
	case <-ch2:
		t.Fatal("cancel2() 未生效：已取消的监听器仍被触发")
	case <-time.After(50 * time.Millisecond):
	}

	// 跨 session 事件不得摘除本 session 的 OnceSession 监听器。
	ch3 := make(chan struct{}, 1)
	cancel3 := client.OnceSession("sess-1", "Test.Evt2", func(evt cdpEvent) {
		select {
		case ch3 <- struct{}{}:
		default:
		}
	})
	defer cancel3()
	srv.pushEvent("other-session", "Test.Evt2", map[string]interface{}{})
	select {
	case <-ch3:
		t.Fatal("别的 session 的事件误触发了本 session 监听器")
	case <-time.After(50 * time.Millisecond):
	}
	srv.pushEvent("sess-1", "Test.Evt2", map[string]interface{}{})
	select {
	case <-ch3:
	case <-time.After(2 * time.Second):
		t.Fatal("别的 session 的同名事件误删了本 session 的 OnceSession 监听器")
	}
}

// R3-M2/R3-M3 回归：
//  1. recreateTab 中途失败（installBridge 段的 RPC 错）必须走 handleDeadLocked
//     整机回收——绝不留下 m.sessionID=="" 的永久僵尸实例（ensureRunning 只看
//     进程/CDP 存活，空 session 会让 ensureBridge 永远报 no page session）。
//     同时刚创建的孤儿 Tab 必须 best-effort 关闭（Target.closeTarget）。
//  2. navigateAndWaitApp 遇到 Page.navigate 的 errorText（代理不可用时的
//     chrome-error:// 导航失败）或导航后仍停在 "null" origin（about:blank），
//     必须 fail-closed 返回 error——绝不回退 waitPageLoad 秒回假就绪。
func TestRecreateTabFailureMarksDeadNotZombie(t *testing.T) {
	t.Run("recreate failure reclaims instance", func(t *testing.T) {
		cleanup := resetMgrForLifecycle(t)
		defer cleanup()
		srv := newFakeCDPServer(t)
		defer srv.close()
		conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
		if err != nil {
			t.Fatalf("dial fake cdp: %v", err)
		}
		defer conn.Close()
		client := newCDPClient(conn)
		defer client.Close()

		m := browserMgr
		m.mu.Lock()
		m.proc = &browserProc{PID: 0x7fffffff, PGID: 0x7fffffff,
			exitedCh: make(chan struct{})}
		m.client = client
		m.sessionID = "old-session"
		m.targetID = "old-target"
		m.started = true
		m.starting = false
		m.ready.Store(true)
		m.mu.Unlock()

		// installBridgeFor 的第一步 Runtime.addBinding 注入失败 → 中途失败。
		srv.failMethods.Store("Runtime.addBinding", true)
		m.recreateTab()

		m.mu.Lock()
		started := m.started
		sess := m.sessionID
		cleaned := m.client == nil && m.proc == nil
		m.mu.Unlock()
		if started && sess == "" {
			t.Fatal("recreateTab 失败留下 sessionID==\"\" 的永久僵尸实例")
		}
		if started {
			t.Fatal("recreateTab 失败必须经 handleDeadLocked 复位实例（started 仍为 true）")
		}
		if !cleaned {
			t.Fatal("handleDeadLocked 未清场：client/proc 未复位")
		}
		// 孤儿 Tab 必须被 best-effort 关闭。
		sawClose := false
		deadline := time.After(2 * time.Second)
		for !sawClose {
			select {
			case c := <-srv.gotCalls:
				if c.method == "Target.closeTarget" &&
					strings.Contains(string(c.params), "fake-target-1") {
					sawClose = true
				}
			case <-deadline:
				t.Fatal("recreateTab 失败未关闭刚创建的孤儿 Tab")
			}
		}
	})

	// R4-B 回归：
	//  1. alignResidentAndNavigate 按旧快照判定需清 Jar、进 jarRW 复核时
	//     驻留号已被并发切成另一个启用且绑定有效的账号，必须按新驻留
	//     号的绑定出口对齐、绝不发 Network.clearBrowserCookies——旧实现
	//     在复核分支里先 Unlock 一次、落到结尾又 Unlock 第二次，必然
	//     sync: unlock of unlocked RWMutex panic（该 panic 会炸掉整个
	//     测试进程，跑过即证明无双重解锁）。
	//  2. navigateAndWaitApp 被迟到事件唤醒后立即核 origin 仍不过、随即
	//     navCtx 超时退出：重挂的 OnceSession 必须被函数出口的 defer
	//     精确摘除，onceHandlers 零残留——旧实现 defer 按值捕获首个
	//     cancelOnce，循环里重挂的监听器永远摘不掉。
	t.Run("resident race clears nothing and late-event timeout leaks no once", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()

		// 驻留号 seedID 绑定一个不存在的代理 → resolveResidentExit 判
		// 绑定失效需清 Jar；进 jarRW 复核时驻留已并发切到 acct2（启用、
		// 绑定可用代理）→ 必须按 acct2 的绑定出口对齐、绝不发
		// Network.clearBrowserCookies 抹掉刚切好的驻留 Jar。
		seedID, err := accountInsert("stale-resident",
			"SAPISID=sa; __Secure-1PSID=sp; SID=s", "")
		if err != nil {
			t.Fatalf("insert stale resident: %v", err)
		}
		defer accountDelete(seedID)
		if _, err := getDB().Exec(`UPDATE accounts SET proxy_id=? WHERE id=?`,
			int64(99999), seedID); err != nil {
			t.Fatalf("bind stale resident to dead proxy: %v", err)
		}
		acct2, err := accountInsert("swapped-resident",
			"SAPISID=oa; __Secure-1PSID=op; SID=o", "")
		if err != nil {
			t.Fatalf("insert swapped resident: %v", err)
		}
		defer accountDelete(acct2)
		pxID, err := proxyCreate("race-exit", "http://127.0.0.1:18090", 1)
		if err != nil {
			t.Fatalf("create race proxy: %v", err)
		}
		defer proxyDelete(pxID)
		if _, err := getDB().Exec(`UPDATE accounts SET proxy_id=? WHERE id=?`,
			pxID, acct2); err != nil {
			t.Fatalf("bind swapped resident to race proxy: %v", err)
		}

		// 用全局 browserMgr + 真 bridge：syncBridgeUpstream 经
		// currentBridge() 只认 browserMgr.bridge，局部 m.bridge 测不到
		// 真实 SetUpstream。fakeCDPServer 只 Accept 一次——wire 用它
		// 自带的 dial，不能再复用 newTestClientAndBridge 的连接。
		bridge, err := newBrowserProxyBridge()
		if err != nil {
			t.Fatalf("newBrowserProxyBridge: %v", err)
		}
		defer bridge.Shutdown(context.Background())
		unwire := wireTestBrowserBridge(t, srv, bridge)
		defer unwire()
		m := browserMgr
		m.mu.Lock()
		m.seed = browserSeedState{AccountID: seedID}
		m.mu.Unlock()

		// 主协程先持 jarRW.RLock：alignResidentAndNavigate 按旧快照
		// seedID 判得 clearJar==true 后进 clearResidentJarIfNeeded 时
		// 必然阻塞在 jarRW.Lock() 上——这段窗口就是「拿到锁前并发切号」
		// 的注入口（旧实现复核段的双重 Unlock 会在此后 panic）。
		m.jarRW.RLock()
		entered := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			defer close(entered)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			done <- m.alignResidentAndNavigate(ctx, m.client, "fake-session-1")
		}()
		time.Sleep(150 * time.Millisecond) // 让子协程确实阻塞在 jarRW.Lock
		// 模拟并发同身份快路径在清 Jar 拿到锁之前完成切号：锁内复核
		// 必须读实时 m.seed=acct2，直接按它的绑定出口对齐、绝不清 Jar。
		m.mu.Lock()
		m.seed = browserSeedState{AccountID: acct2}
		m.mu.Unlock()
		m.jarRW.RUnlock()
		<-entered
		if err := <-done; err != nil {
			t.Fatalf("alignResidentAndNavigate: %v", err)
		}
		for {
			select {
			case c := <-srv.gotCalls:
				if c.method == "Network.clearBrowserCookies" {
					t.Fatal("驻留已切到绑定有效账号仍发 clearBrowserCookies（误清新驻留 Jar）")
				}
			default:
				goto drained
			}
		}
	drained:
		m.mu.Lock()
		gotSeed := m.seed.AccountID
		m.mu.Unlock()
		if gotSeed != acct2 {
			t.Fatalf("并发切号后 seed 被覆写：got %d want %d", gotSeed, acct2)
		}
		// 桥真实切到了 acct2 绑定的代理出口。
		m.bridgeMu.Lock()
		activeURL := m.bridgeActiveURL
		m.bridgeMu.Unlock()
		wantURL := proxyURLByID(pxID)
		if activeURL != wantURL {
			t.Fatalf("bridgeActiveURL = %q，acct2 绑定出口应得 %q", activeURL, wantURL)
		}
		bridge.mu.Lock()
		up := bridge.upstream
		bridge.mu.Unlock()
		if up != wantURL {
			t.Fatalf("桥 upstream = %q，acct2 绑定出口应得 %q", up, wantURL)
		}

		// navigateAndWaitApp：navDelay 远大于 navCtx 使 origin 恒停 null；
		// 50ms 时推迟到事件唤醒循环 → 重挂 OnceSession → origin 核不过
		// → 回 select 等 navCtx 到期走超时返回。重挂的那条监听器必须
		// 被函数出口的 defer 摘干净（旧实现 defer 按值捕获首个
		// cancelOnce，重挂的监听器永远摘不掉）。
		srv2 := newFakeCDPServer(t)
		defer srv2.close()
		client2, _ := newTestClientAndBridge(t, srv2)
		srv2.pageNavDelay.Store(int64(2 * time.Second))
		m2 := &browserManager{}
		navCtx, navCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer navCancel()
		go func() {
			time.Sleep(50 * time.Millisecond)
			srv2.pushEvent("fake-session-1", "Page.loadEventFired",
				map[string]interface{}{"timestamp": 0})
		}()
		if err := m2.navigateAndWaitApp(navCtx, client2, "fake-session-1"); err == nil {
			t.Fatal("navCtx 超时且 origin 不到目标页必须 fail-closed")
		}
		client2.evtMu.Lock()
		onceLeft := 0
		for _, v := range client2.onceHandlers {
			onceLeft += len(v)
		}
		client2.evtMu.Unlock()
		if onceLeft != 0 {
			t.Fatalf("navigateAndWaitApp 超时退出残留 %d 条 OnceSession 监听器（defer 值捕获泄漏回归）", onceLeft)
		}
	})

	// R4-M1 回归：Target.createTarget 失败是 recreateTab 的第 5 个失败
	// 出口，必须走 recreateFailDead 整机回收——绝不留下 m.sessionID 仍指
	// 旧死会话的僵尸实例（旧实现该分支只 logf 后 return）。
	t.Run("createTarget failure reclaims instance", func(t *testing.T) {
		cleanup := resetMgrForLifecycle(t)
		defer cleanup()
		srv := newFakeCDPServer(t)
		defer srv.close()
		conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
		if err != nil {
			t.Fatalf("dial fake cdp: %v", err)
		}
		defer conn.Close()
		client := newCDPClient(conn)
		defer client.Close()

		m := browserMgr
		m.mu.Lock()
		m.proc = &browserProc{PID: 0x7fffffff, PGID: 0x7fffffff,
			exitedCh: make(chan struct{})}
		m.client = client
		m.sessionID = "old-session"
		m.targetID = "old-target"
		m.started = true
		m.starting = false
		m.ready.Store(true)
		m.mu.Unlock()

		oldKill := killProcGroupFn
		killProcGroupFn = func(pid, pgid int, d time.Duration) {}
		defer func() { killProcGroupFn = oldKill }()

		srv.failMethods.Store("Target.createTarget", true)
		m.recreateTab()

		m.mu.Lock()
		started := m.started
		sess := m.sessionID
		cleaned := m.client == nil && m.proc == nil
		m.mu.Unlock()
		if started || !cleaned || sess != "" {
			t.Fatalf("createTarget 失败未整机回收：started=%v cleaned=%v sessionID=%q（僵尸实例）",
				started, cleaned, sess)
		}
	})

	// R4-M2 回归：alignResidentAndNavigate 阻塞在 navigateAndWaitApp 的
	// select 时 CDP 断连（client.Close）必须毫秒级退出并释放桥出口租约
	// ——旧实现不监听 client.Done()，旧代际会死占 m.bridgeInFlight 挂满
	// 25s navCtx，新实例同出口请求跳过 syncBridgeUpstream 泄漏 Cookie、
	// 跨出口请求被堵到超时。
	t.Run("align navigate exits fast on client close", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		client, bridge := newTestClientAndBridge(t, srv)
		// 导航后 20s 才切 origin/推 load：select 纯阻塞，只能靠
		// client.Done() 唤醒。
		srv.pageNavDelay.Store(int64(20 * time.Second))
		m := &browserManager{}
		m.mu.Lock()
		m.bridge = bridge
		m.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		done := make(chan error, 1)
		start := time.Now()
		go func() {
			done <- m.alignResidentAndNavigate(ctx, client, "fake-session-1")
		}()
		// 等 Page.navigate 发出后模拟断连（Stop/崩溃同路径）。
		deadline := time.After(5 * time.Second)
	waitNav:
		for {
			select {
			case c := <-srv.gotCalls:
				if c.method == "Page.navigate" {
					break waitNav
				}
			case <-deadline:
				t.Fatal("5s 内未见 Page.navigate")
			}
		}
		client.Close()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("断连后 alignResidentAndNavigate 必须返回 error")
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("断连后 500ms 内未返回（旧实现会死等 25s navCtx 并死占桥出口租约）")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("断连退出耗时 %v，应毫秒级", elapsed)
		}
		m.bridgeMu.Lock()
		inFlight := m.bridgeInFlight
		m.bridgeMu.Unlock()
		if inFlight != 0 {
			t.Fatalf("退出后 bridgeInFlight = %d，桥出口租约未释放（跨代际残留）", inFlight)
		}
	})

	t.Run("navigate errorText fails closed", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		client, _ := newTestClientAndBridge(t, srv)
		// RPC 成功但 errorText 非空（代理不可用时 Chromium 停在
		// chrome-error://）：旧实现完全忽略 navRes，新实现必须 fail-closed。
		srv.navErrorText.Store("net::ERR_TUNNEL_CONNECTION_FAILED")
		srv.pageNavDelay.Store(int64(10 * time.Second)) // 绝不切换 origin
		m := &browserManager{}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		start := time.Now()
		err := m.navigateAndWaitApp(ctx, client, "fake-session-1")
		if err == nil {
			t.Fatal("Page.navigate errorText 必须 fail-closed 返回 error")
		}
		if !strings.Contains(err.Error(), "ERR_TUNNEL") {
			t.Fatalf("error 应包含 errorText 内容，得到 %v", err)
		}
		if time.Since(start) > 2*time.Second {
			t.Fatalf("errorText 应在 Page.navigate 返回后立即失败，实际 %.0fms",
				float64(time.Since(start))/float64(time.Millisecond))
		}
	})

	t.Run("late about:blank event then real load succeeds", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		client, _ := newTestClientAndBridge(t, srv)
		// 导航有真实耗时：目标页 load 在 400ms 后才到；但 60ms 时先推一条
		// 迟到的旧文档（about:blank）loadEventFired——旧实现会拿它核 origin
		// （"null/complete" 不过）再退回 waitPageLoad 在 readyState=complete
		// 的 about:blank 上秒回。新实现必须重挂 OnceSession 等真 load。
		srv.pageNavDelay.Store(int64(400 * time.Millisecond))
		go func() {
			time.Sleep(60 * time.Millisecond)
			srv.pushEvent("fake-session-1", "Page.loadEventFired",
				map[string]interface{}{"timestamp": 0})
		}()
		m := &browserManager{}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		start := time.Now()
		if err := m.navigateAndWaitApp(ctx, client, "fake-session-1"); err != nil {
			t.Fatalf("目标页 load 终会到达，不应 fail：%v", err)
		}
		if got := srv.pageOrigin.Load().(string); got != "https://gemini.google.com" {
			t.Fatalf("返回时 origin 仍为 %q（迟到事件导致 about:blank 秒回回归）", got)
		}
		if time.Since(start) < 300*time.Millisecond {
			t.Fatalf("%.0fms 即在 null origin 返回（waitPageLoad 秒回回归）",
				float64(time.Since(start))/float64(time.Millisecond))
		}
	})

	t.Run("navigate stuck on about:blank fails closed", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		client, _ := newTestClientAndBridge(t, srv)
		// Page.navigate 的 goroutine 把 origin 切换推迟到 10s 后（比 navCtx
		// 的 1.2s 预算长得多）——导航期间 origin 恒 "null"（about:blank）。
		srv.pageNavDelay.Store(int64(10 * time.Second))
		m := &browserManager{}
		ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := m.navigateAndWaitApp(ctx, client, "fake-session-1")
		if err == nil {
			t.Fatal("导航后仍停在 null origin 必须 fail-closed，绝不能 waitPageLoad 秒回假就绪")
		}
		// 秒回防护：waitPageLoad 兜底路径会在 <50ms 返回；真等待路径至少
		// 会等到 navCtx（继承 ctx 的 1.2s）到期。
		if time.Since(start) < 500*time.Millisecond {
			t.Fatalf("在 null origin 上 %.0fms 提前返回（waitPageLoad 秒回回归）",
				float64(time.Since(start))/float64(time.Millisecond))
		}
	})
}

var _ = syscall.Kill
