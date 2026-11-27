package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// 面板改 request_timeout_sec 要立刻作用到新请求：缓存的客户端不能沿用旧超时。
func TestClientCacheFollowsTimeout(t *testing.T) {
	rtMu.Lock()
	saved := rtVal
	rtVal.RequestTimeout = 20
	rtMu.Unlock()
	defer func() { rtMu.Lock(); rtVal = saved; rtMu.Unlock() }()

	const proxy = "http://127.0.0.1:1"
	c1, err1 := getStdlibClient(proxy)
	if err1 != nil {
		t.Fatalf("getStdlibClient: %v", err1)
	}
	if got := c1.Timeout; got != 20*time.Second {
		t.Fatalf("timeout = %v, want 20s", got)
	}
	rtMu.Lock()
	rtVal.RequestTimeout = 180
	rtMu.Unlock()
	c2, err2 := getStdlibClient(proxy)
	if err2 != nil {
		t.Fatalf("getStdlibClient: %v", err2)
	}
	if got := c2.Timeout; got != 180*time.Second {
		t.Fatalf("after change timeout = %v, want 180s", got)
	}
	if getTLSClient() == nil {
		t.Fatal("tls client nil")
	}
}

// tls-client 的 HTTP/2 连接断了要重拨时，这一步不带请求的 ctx（见 tlsDialer）：对方连上了
// 却不回话，以前请求就一直卡着，stallWatch 和客户端断开都掐不掉。
func TestTLSRedialGivesUpOnASilentServer(t *testing.T) {
	prev := handshakeTimeout
	handshakeTimeout = 200 * time.Millisecond
	t.Cleanup(func() { handshakeTimeout = prev })
	useInsecureTLSClients(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	quiet := &quietAfterFirst{Listener: ln}
	t.Cleanup(quiet.closeHeld)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.Listener.Close()
	srv.Listener = quiet
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	send := func() error {
		_, _, _, _, err := doGeminiRequestStop(context.Background(), srv.URL+"/StreamGenerate", "f.req=x",
			map[string]string{}, "", nil, nil)
		return err
	}
	if err := send(); err != nil {
		t.Fatalf("头一个请求就失败了：%v", err)
	}
	srv.CloseClientConnections()      // 断掉这条 HTTP/2 连接，下一个请求只能重拨
	time.Sleep(50 * time.Millisecond) // 等客户端看到连接断了

	done := make(chan error, 1)
	go func() { done <- send() }()
	select {
	case err := <-done:
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("对方不回话应报超时，实际 %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("重拨时对方不回话，请求一直卡着")
	}
	if n := quiet.accepted(); n != 2 {
		t.Fatalf("应该重拨一次，实际连上来 %d 次", n)
	}
}

// quietAfterFirst 只把第一条连接交给服务器，之后连上来的都留着不理：TCP 连得上，
// TLS 握手没有回音。
type quietAfterFirst struct {
	net.Listener
	mu   sync.Mutex
	n    int
	held []net.Conn
}

func (l *quietAfterFirst) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		l.mu.Lock()
		l.n++
		first := l.n == 1
		if !first {
			l.held = append(l.held, c)
		}
		l.mu.Unlock()
		if first {
			return c, nil
		}
	}
}

func (l *quietAfterFirst) accepted() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.n
}

func (l *quietAfterFirst) closeHeld() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.held {
		c.Close()
	}
}
