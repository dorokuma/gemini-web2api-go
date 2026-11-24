package app

// browser_proxy.go —— 按需启动的 127.0.0.1 本地 HTTP CONNECT 透明上游代理转发器。
//
// 存在理由：Chromium 的 --proxy-server 只接受一个静态值，且不支持每条请求换
// 上游；我们的代理池是动态的（绑定账号、轮换、熔断恢复）。常驻浏览器想跟着
// 池走，就必须在中间垫一层自己控制的转发器：
//
//	Chromium --proxy-server=http://127.0.0.1:<bridgePort>
//	    │ CONNECT gemini.google.com:443
//	    ▼
//	browserProxyBridge（本文件）
//	    │ 按当前 SetUpstream 选的出口：直连 / HTTP CONNECT / SOCKS5
//	    ▼
//	真上游代理 / 直连
//
// 安全边界（审查后收紧的三条）：
//  1. 只认 CONNECT 方法：对 GET 回 "200 Connection Established" 等于把本机
//     回环上开了一个开放式正向代理，任何能摸到这个端口的进程都能借它打内网
//     （/admin、metadata 端点都在射程里）。非 CONNECT 一律 405。
//  2. 目标必须是合法的 host:port，且不是回环地址、不是本桥自己的监听端口：
//     CONNECT 127.0.0.1:<bridgePort> 会让桥自己给自己开隧道，几条就 FD 耗尽；
//     CONNECT 127.0.0.1:<其它本机服务> 就是 SSRF。测试可通过 allowLoopback
//     字段只放行测试靶机，生产默认严格拒绝。
//  3. upstream 为空且不允许直连时拒绝建隧道（502）：fallback_direct=false
//     配了代理池时，本机真实 IP 绝不能经这里漏出去。
//
// 命令行零凭据暴露：Chromium 只拿到 --proxy-server=http://127.0.0.1:<port>，
// 用户名密码从不进 args —— ps 看不到，崩溃转储也不会带。

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"
)

// browserProxyBridge 是 127.0.0.1 上按需监听的 HTTP CONNECT 转发器。
// upstream 存当前要走的代理 URL（"" = 直连），SetUpstream 原子切换。
type browserProxyBridge struct {
	ln   net.Listener
	port int

	// 单一把 mu 保护全部可变状态 + 活跃隧道表：upstream 与 allowDirect 必须
	// 作为同一个原子快照被读到——拆成两个独立 atomic 时，读侧可能拿到
	// 「新 upstream="" 配旧 allowDirect=true」的撕裂组合，在
	// fallback_direct=false 收紧的瞬间把真实 IP 漏成直连。
	mu          sync.Mutex
	upstream    string // "" / http:// / https:// / socks5:// / socks5h://
	allowDirect bool   // false = upstream="" 一律拒（绝不直连漏 IP）
	gen         uint64 // 出口代际：upstream 变更或 allowDirect 收紧时 +1
	closed      bool
	// tunnels 记全部在建与活跃隧道（upstreamConn → 建隧道的 clientConn）。
	// 拨号前就登记在建项（conn=nil），拨号完成回填——SetUpstream/Close 只对
	// 已建成的连接 close，对在建项的收口靠「建成后在 mu 下比对 gen/closed」：
	// 代际已换或桥已关，立刻关掉刚建成的连接，绝不漏关。
	tunnels map[net.Conn]net.Conn

	server *http.Server

	// allowLoopback 仅测试用：允许 CONNECT 回环目标。生产恒 false，
	// 回环 + 自身端口一律拒绝（防自环 FD 耗尽与 /admin SSRF）。
	allowLoopback bool

	// onConnectTarget 仅测试用：每条进入 handleConnect 的 CONNECT 目标
	// host:port 上报一次（含被拒项），生产恒 nil。让冒烟测试可断言
	// rootBridge（根 Profile）零业务流量。
	onConnectTarget func(target string)
}

// newBrowserProxyBridge 起监听但**不**接请求——Chromium 真正连过来之前，
// 它只是个端口占位，开销极小。
func newBrowserProxyBridge() (*browserProxyBridge, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("browser proxy bridge listen: %w", err)
	}
	b := &browserProxyBridge{
		ln:          ln,
		port:        ln.Addr().(*net.TCPAddr).Port,
		tunnels:     map[net.Conn]net.Conn{},
		allowDirect: true, // 未调用 SetUpstream 前默认允许直连，由首个请求方校正
	}
	b.server = &http.Server{Handler: http.HandlerFunc(b.handleConnect)}
	go b.server.Serve(ln)
	return b, nil
}

// Port 是 Chromium 该连的本地端口。
func (b *browserProxyBridge) Port() int { return b.port }

// SetUpstream 原子换上游与直连许可。空 proxyURL 且 allowDirect=false 时
// 全部 CONNECT 被拒（代理池锁定场景绝不直连）；allowDirect=true 时空串 = 直连。
//
// 代际语义：upstream 实际变更 **或** allowDirect 从 true 收紧为 false
// （即使 URL 都是空串——收紧后旧直连隧道就是违规隧道）时，递增 gen 并关闭
// 全部旧代际隧道；Chromium 对同一 origin 的 CONNECT 隧道复用很粘，不关它
// 新请求会继续走旧出口/旧许可，换代理等于没换。allowDirect false→true 放宽
// 不换代际：旧的非直连隧道仍然合法。
func (b *browserProxyBridge) SetUpstream(proxyURL string, allowDirect bool) error {
	if proxyURL != "" {
		if err := validateProxyURL(proxyURL); err != nil {
			return err
		}
	}
	b.mu.Lock()
	genBumped := false
	if b.upstream != proxyURL || (!allowDirect && b.allowDirect) {
		b.gen++
		genBumped = true
	}
	b.upstream = proxyURL
	b.allowDirect = allowDirect
	var stale []net.Conn
	if genBumped {
		for upConn := range b.tunnels {
			stale = append(stale, upConn)
		}
	}
	b.mu.Unlock()
	for _, c := range stale {
		_ = c.Close() // nil 在建项跳过：建成后由 gen 校验关闭
	}
	return nil
}

// handleConnect 只认 CONNECT 方法。
// 非 CONNECT 一律 405：本机回环端口对 GET 回 "200 Connection Established"
// 等于开了一个零鉴权正向代理，任何本机进程都能借它打内网 /admin。
func (b *browserProxyBridge) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		w.Header().Set("Allow", http.MethodConnect)
		http.Error(w, "CONNECT only", http.StatusMethodNotAllowed)
		return
	}
	if cb := b.onConnectTarget; cb != nil {
		cb(r.Host)
	}
	if err := b.checkTarget(r.Host); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b.tunnel(w, r, r.Host)
}

// checkTarget 严格校验 CONNECT 目标：
//   - net.SplitHostPort 必须解析出非空 host 与合法端口（0-65535）；
//   - host 不得含空白/控制字符/CR-LF（注入防御）；
//   - host 含 '%'（IPv6 zone id，如 [::1%lo]）一律拒绝——zone id 使
//     net.ParseIP 返回 nil 逃逸回环判定，但拨号时仍指向本机接口；
//   - host 剥离尾点（"localhost." 是合法 FQDN 且解析回环，字面比较
//     会漏放）后再判定：回环地址（127.0.0.0/8、::1、localhost）、
//     未指定地址（0.0.0.0、::、::ffff:0.0.0.0）与本桥监听端口一律
//     拒绝，除非测试显式打开 allowLoopback——CONNECT 0.0.0.0:<port>
//     会拨到本机该端口，和回环一样是 SSRF（IsUnspecified/IsLoopback
//     对 IPv4-mapped 形式经 To4 等价比较，一并覆盖）。
func (b *browserProxyBridge) checkTarget(target string) error {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("bad CONNECT target %q: %w", target, err)
	}
	if host == "" {
		return fmt.Errorf("bad CONNECT target %q: empty host", target)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("bad CONNECT target %q: invalid port", target)
	}
	for i := 0; i < len(target); i++ {
		c := target[i]
		if c <= 0x20 || c == 0x7f {
			return fmt.Errorf("bad CONNECT target %q: control character", target)
		}
	}
	if b.allowLoopback {
		return nil
	}
	if port == b.port {
		return fmt.Errorf("CONNECT target %q is the bridge itself (self-loop refused)", target)
	}
	h := strings.ToLower(strings.Trim(host, "[]"))
	if strings.Contains(h, "%") {
		return fmt.Errorf("CONNECT target %q carries IPv6 zone id (refused)", target)
	}
	h = strings.TrimRight(h, ".")
	if h == "" {
		return fmt.Errorf("bad CONNECT target %q: empty host", target)
	}
	if h == "localhost" || strings.HasSuffix(h, ".localhost") {
		return fmt.Errorf("CONNECT target %q is loopback (refused)", target)
	}
	if ip := net.ParseIP(h); ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
		return fmt.Errorf("CONNECT target %q is loopback/unspecified (refused)", target)
	}
	return nil
}

// tunnel 完成一条 CONNECT：先 hijack 出客户端 conn，在 mu 下取
// (upstream, allowDirect, gen, closed) 原子快照并登记在建隧道，再按快照
// 拨号到目标。拨号完成后回 mu 校验 closed/gen——桥已关或代际已换，立即
// 关闭刚建成的连接，绝不把旧代际隧道放上线。
func (b *browserProxyBridge) tunnel(w http.ResponseWriter, r *http.Request, target string) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	clientConn, clientRW, err := hj.Hijack()
	if err != nil {
		return
	}

	b.mu.Lock()
	upstreamURL := b.upstream
	allowDirect := b.allowDirect
	snapGen := b.gen
	closed := b.closed
	if !closed {
		b.tunnels[clientConn] = nil // 在建登记：upstreamConn 建成前为 nil
	}
	b.mu.Unlock()

	drop := func() {
		b.mu.Lock()
		delete(b.tunnels, clientConn)
		b.mu.Unlock()
		clientConn.Close()
	}
	if closed || (upstreamURL == "" && !allowDirect) {
		fmt.Fprintf(clientRW, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
		clientRW.Flush()
		drop()
		return
	}
	upstreamConn, err := b.dialUpstream(r.Context(), upstreamURL, target)
	if err != nil {
		// 按 CONNECT 语义回 502 再关，Chromium 会把这次导航标成代理失败。
		fmt.Fprintf(clientRW, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
		clientRW.Flush()
		drop()
		return
	}

	// 拨号完成：在 mu 下终检——桥关了或代际换了，这条新隧道不许上线。
	b.mu.Lock()
	if b.closed || b.gen != snapGen {
		delete(b.tunnels, clientConn)
		b.mu.Unlock()
		upstreamConn.Close()
		fmt.Fprintf(clientRW, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
		clientRW.Flush()
		clientConn.Close()
		return
	}
	b.tunnels[clientConn] = upstreamConn
	b.mu.Unlock()

	// 200 之后再交隧道：回完这一行，clientConn 的后续字节就是目标协议的流量。
	if _, err := clientRW.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		b.mu.Lock()
		delete(b.tunnels, clientConn)
		b.mu.Unlock()
		clientConn.Close()
		upstreamConn.Close()
		return
	}
	if err := clientRW.Flush(); err != nil {
		b.mu.Lock()
		delete(b.tunnels, clientConn)
		b.mu.Unlock()
		clientConn.Close()
		upstreamConn.Close()
		return
	}

	untrack := func() {
		b.mu.Lock()
		delete(b.tunnels, clientConn)
		b.mu.Unlock()
	}
	go func() {
		io.Copy(upstreamConn, clientConn)
		upstreamConn.Close()
		clientConn.Close()
		untrack()
	}()
	go func() {
		io.Copy(clientConn, upstreamConn)
		clientConn.Close()
		upstreamConn.Close()
		untrack()
	}()
}

// dialUpstream 按 upstreamURL 建一条到 target（host:port）的 TCP/TLS 隧道。
//   - ""          → 直连 target
//   - http://     → CONNECT target 到代理（可带 Basic 认证头）
//   - https://    → 先跟代理握 TLS，再发 CONNECT
//   - socks5(h):// → x/net/proxy.SOCKS5（复用仓库已有依赖；h 让代理解 DNS）
func (b *browserProxyBridge) dialUpstream(ctx context.Context, upstreamURL, target string) (net.Conn, error) {
	d := net.Dialer{Timeout: 15 * time.Second}
	if upstreamURL == "" {
		return d.DialContext(ctx, "tcp", target)
	}
	u, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, fmt.Errorf("upstream proxy parse: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)

	switch scheme {
	case "http", "https":
		proxyAddr := u.Host
		conn, err := d.DialContext(ctx, "tcp", proxyAddr)
		if err != nil {
			return nil, err
		}
		if scheme == "https" {
			host := proxyAddr
			if h, _, err := net.SplitHostPort(proxyAddr); err == nil {
				host = h
			}
			tconn := tls.Client(conn, &tls.Config{ServerName: host})
			if err := tconn.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, fmt.Errorf("upstream https handshake: %w", err)
			}
			conn = tconn
		}
		// CONNECT 请求带 Basic 认证（若 URL 有 userinfo）；密码不出现在任何地方
		// 的命令行——Chromium 不知道上游有认证，只看见 127.0.0.1。
		// 必须取解码后的原始凭据（Username()/Password()）：u.User.String()
		// 是按 URL 规则转义的串，密码含 @ / : / % 时会发出去一份错误凭据，
		// 上游回 407 整条隧道死掉。
		var authHdr string
		if u.User != nil {
			pass, _ := u.User.Password()
			cred := u.User.Username() + ":" + pass
			authHdr = "Proxy-Authorization: Basic " +
				base64.StdEncoding.EncodeToString([]byte(cred)) + "\r\n"
		}
		req := "CONNECT " + target + " HTTP/1.1\r\n" +
			"Host: " + target + "\r\n" + authHdr + "\r\n"
		if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
			conn.Close()
			return nil, err
		}
		if _, err := io.WriteString(conn, req); err != nil {
			conn.Close()
			return nil, err
		}
		br := bufio.NewReader(conn)
		status, err := br.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("upstream CONNECT response: %w", err)
		}
		if !strings.Contains(status, "200") {
			conn.Close()
			return nil, fmt.Errorf("upstream CONNECT refused: %s", strings.TrimSpace(status))
		}
		// 消费完响应头到空行
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				conn.Close()
				return nil, err
			}
			if line == "\r\n" || line == "\n" {
				break
			}
		}
		conn.SetDeadline(time.Time{})
		// br 里可能带目标已推的第一批字节；bufio.Reader 包进 conn 一并返回。
		return &bufferedConn{Conn: conn, r: br}, nil

	case "socks5", "socks5h":
		var auth *proxy.Auth
		if u.User != nil {
			pw, _ := u.User.Password()
			auth = &proxy.Auth{User: u.User.Username(), Password: pw}
		}
		dialer, err := proxy.SOCKS5("tcp", u.Host, auth, &d)
		if err != nil {
			return nil, fmt.Errorf("upstream socks5: %w", err)
		}
		if cd, ok := dialer.(proxy.ContextDialer); ok {
			return cd.DialContext(ctx, "tcp", target)
		}
		return dialer.Dial("tcp", target)

	default:
		return nil, fmt.Errorf("unsupported upstream scheme %q", scheme)
	}
}

// bufferedConn 在 CONNECT 响应头读完后，把可能预读进 bufio 的目标字节
// 一并透传——http 代理可能紧接着回包就推数据。
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if c.r.Buffered() > 0 {
		return c.r.Read(p)
	}
	return c.Conn.Read(p)
}

// Shutdown 停掉监听并关掉在飞连接（http.Server.Close 只停新连接，hijack
// 出去的隧道不归它管——这里显式把全部在建与活跃隧道一并关掉；在建项
// 由 tunnel 建成后的 closed/gen 终检负责立刻自关）。
func (b *browserProxyBridge) Shutdown(ctx context.Context) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	b.gen++ // 作废所有在建快照
	list := make([]net.Conn, 0, len(b.tunnels))
	for c := range b.tunnels {
		list = append(list, c)
	}
	b.mu.Unlock()
	for _, c := range list {
		_ = c.Close()
	}
	_ = b.server.Close()
	_ = b.ln.Close()
}

// errProxyBridgeUnavailable 是浏览器代理层自身的硬错（不是上游错）。
var errProxyBridgeUnavailable = errors.New("browser proxy bridge unavailable")
