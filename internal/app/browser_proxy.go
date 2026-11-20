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
// 切换上游只改 atomic 里存的 URL，已经在飞的 CONNECT 隧道不受影响（它们
// 各自持有自己握手时定下的目标），新隧道立刻用新值；空闲连接由我们显式
// CloseIdleConnections 清掉，Chromium 侧看到的是无感的代理端点。
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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/proxy"
)

// browserProxyBridge 是 127.0.0.1 上按需监听的 HTTP CONNECT 转发器。
// upstream 存当前要走的代理 URL（"" = 直连），SetUpstream 原子切换。
type browserProxyBridge struct {
	ln       net.Listener
	port     int
	upstream atomic.Value // string："" / http:// / https:// / socks5:// / socks5h://
	server   *http.Server

	mu     sync.Mutex
	closed bool
}

// newBrowserProxyBridge 起监听但**不**接请求——Chromium 真正连过来之前，
// 它只是个端口占位，开销极小。
func newBrowserProxyBridge() (*browserProxyBridge, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("browser proxy bridge listen: %w", err)
	}
	b := &browserProxyBridge{ln: ln, port: ln.Addr().(*net.TCPAddr).Port}
	b.upstream.Store("")
	b.server = &http.Server{Handler: http.HandlerFunc(b.handleConnect)}
	go b.server.Serve(ln)
	return b, nil
}

// Port 是 Chromium 该连的本地端口。
func (b *browserProxyBridge) Port() int { return b.port }

// SetUpstream 原子换上游。空串 = 直连；其余必须是 validateProxyURL 接受的
// scheme。换完不重启任何东西：新 CONNECT 立刻走新上游，旧空闲连接由调用方
// 视情况 CloseIdle。
func (b *browserProxyBridge) SetUpstream(proxyURL string) error {
	if proxyURL != "" {
		if err := validateProxyURL(proxyURL); err != nil {
			return err
		}
	}
	b.upstream.Store(proxyURL)
	return nil
}

// CloseIdle 关闭连接池里已建立的隧道空闲端。
// 现在每条 CONNECT 都是独立 TCP 直连目标，没有 http.Transport 的连接池可清；
// 留这个方法占位，供将来若换成 http.Transport 转发时挂接。
func (b *browserProxyBridge) CloseIdle() {}

// handleConnect 只认 CONNECT 方法；普通 HTTP 请求（Chromium 对 ws/http 站点
// 会发普通 GET）也转成 CONNECT 到目标的 80 端口——Gemini 全站 HTTPS，这条
// 路径平时用不上，但 about:blank / devtools 页面不会触发它。
func (b *browserProxyBridge) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		// 非 CONNECT 的明文 http:// 请求也走隧道模式：CONNECT 到 host:80。
		// DevTools 的 ws 回环不在此列（它压根不走代理）。
		target := r.Host
		if !strings.Contains(target, ":") {
			target += ":80"
		}
		b.tunnel(w, r, target)
		return
	}
	b.tunnel(w, r, r.Host)
}

// tunnel 完成一条 CONNECT：先 hijack 出客户端 conn，再按当前上游建到目标的
// 连接，两边 io.Copy 收尾。
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

	upstreamURL, _ := b.upstream.Load().(string)
	upstreamConn, err := b.dialUpstream(r.Context(), upstreamURL, target)
	if err != nil {
		// 按 CONNECT 语义回 502 再关，Chromium 会把这次导航标成代理失败。
		fmt.Fprintf(clientRW, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
		clientRW.Flush()
		clientConn.Close()
		return
	}

	// 200 之后再交隧道：回完这一行，clientConn 的后续字节就是目标协议的流量。
	if _, err := clientRW.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		clientConn.Close()
		upstreamConn.Close()
		return
	}
	if err := clientRW.Flush(); err != nil {
		clientConn.Close()
		upstreamConn.Close()
		return
	}

	go func() {
		io.Copy(upstreamConn, clientConn)
		upstreamConn.Close()
		clientConn.Close()
	}()
	go func() {
		io.Copy(clientConn, upstreamConn)
		clientConn.Close()
		upstreamConn.Close()
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
		var authHdr string
		if u.User != nil {
			cred := u.User.String()
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

// Shutdown 停掉监听并关掉在飞连接（http.Server.Close 会同时收 hijack 掉的，
// 因为 hijack 后 server 不管它——所以这里直接 ln.Close + 记 closed，已经
// hijack 的隧道由各自的 io.Copy 自然收尾或被 conn 关闭打断）。
func (b *browserProxyBridge) Shutdown(ctx context.Context) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	b.mu.Unlock()
	// hijack 出去的 conn 不归 http.Server 管，Close 只停新连接；在飞的靠
	// 对端断或 Chromium 退出收尾。浏览器进程本身马上会被杀掉，隧道随之断。
	_ = b.server.Close()
	_ = b.ln.Close()
}

// errProxyBridgeUnavailable 是浏览器代理层自身的硬错（不是上游错）。
var errProxyBridgeUnavailable = errors.New("browser proxy bridge unavailable")
