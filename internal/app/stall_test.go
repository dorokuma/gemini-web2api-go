package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func withStallLimit(t *testing.T, d time.Duration) {
	t.Helper()
	prev := stallLimit
	stallLimit = func() time.Duration { return d }
	t.Cleanup(func() { stallLimit = prev })
}

// 每个用例都在两条路上各跑一遍，假上游的行为一样：代理那条（同 cancel_test：http:// 目标
// 以绝对 URI 发给代理，httptest 服务器本身就能当上游），直连那条（HTTPS + HTTP/2 的测试
// 服务器，tls-client 换成不校验证书的同款，见 useInsecureTLSClients）。
var upstreamPaths = []struct {
	name  string
	start func(t *testing.T, h http.Handler) (endpoint, proxyURL string)
}{
	{"proxy", func(t *testing.T, h http.Handler) (string, string) {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		return "http://upstream.invalid/StreamGenerate", srv.URL
	}},
	{"direct", func(t *testing.T, h http.Handler) (string, string) {
		useInsecureTLSClients(t)
		srv := httptest.NewUnstartedServer(h)
		srv.EnableHTTP2 = true
		srv.StartTLS()
		t.Cleanup(srv.Close)
		return srv.URL + "/StreamGenerate", ""
	}},
}

// 一直在出字的回复比 request_timeout_sec 长也不能被截断：以前整次请求限时，
// 180 秒时还在写的回复被拦腰掐掉。
func TestStallWatchKeepsAReplyThatKeepsComing(t *testing.T) {
	withRequestTimeout(t, 1) // 整次限时的客户端 1 秒就掐，回复要写 1.2 秒
	withStallLimit(t, 150*time.Millisecond)
	for _, p := range upstreamPaths {
		t.Run(p.name, func(t *testing.T) {
			endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				for i := 0; i < 20; i++ {
					fmt.Fprintf(w, "frame %d\n", i)
					w.(http.Flusher).Flush()
					time.Sleep(60 * time.Millisecond)
				}
			}))

			lines := 0
			start := time.Now()
			status, _, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
				map[string]string{}, proxyURL, func(line string) error {
					lines++
					return nil
				}, nil)
			if err != nil || status != 200 {
				t.Fatalf("持续出字的回复被掐了：status=%d err=%v", status, err)
			}
			if lines != 20 {
				t.Fatalf("只读到 %d 行", lines)
			}
			if took := time.Since(start); took < time.Second {
				t.Fatalf("测试前提不成立：回复只用了 %v，没比整次限时长", took)
			}
		})
	}
}

// 上游让请求排队时只发心跳帧（实测每 60 秒一个），排得比沉默上限还久也要等到出字：
// 以前整次请求限时，排队超过 180 秒的请求一律被掐掉重发。
func TestStallWatchWaitsThroughHeartbeats(t *testing.T) {
	withStallLimit(t, 150*time.Millisecond)
	for _, p := range upstreamPaths {
		t.Run(p.name, func(t *testing.T) {
			endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				w.Write([]byte(")]}'\nfirst frame\n"))
				w.(http.Flusher).Flush()
				for i := 0; i < 6; i++ {
					time.Sleep(100 * time.Millisecond)
					w.Write([]byte("heartbeat\n"))
					w.(http.Flusher).Flush()
				}
				w.Write([]byte("answer\n"))
			}))

			var got []string
			start := time.Now()
			_, _, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
				map[string]string{}, proxyURL, func(line string) error {
					got = append(got, line)
					return nil
				}, nil)
			if err != nil {
				t.Fatalf("还在发心跳的上游被当成卡住了：%v", err)
			}
			if len(got) == 0 || got[len(got)-1] != "answer" {
				t.Fatalf("没等到排队之后的回复：%q", got)
			}
			if took := time.Since(start); took < 4*150*time.Millisecond {
				t.Fatalf("测试前提不成立：排队只用了 %v，没比沉默上限长", took)
			}
		})
	}
}

// 首帧之后就不再出字的上游，沉默满上限就放弃，报 StallError，并真的断开上游连接。
func TestStallWatchEndsAQuietUpstream(t *testing.T) {
	withStallLimit(t, 150*time.Millisecond)
	for _, p := range upstreamPaths {
		t.Run(p.name, func(t *testing.T) {
			upstreamGone := make(chan struct{})
			endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				w.Write([]byte(")]}'\nfirst frame\n"))
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					close(upstreamGone)
				case <-time.After(10 * time.Second):
				}
			}))

			start := time.Now()
			_, _, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
				map[string]string{}, proxyURL, func(string) error { return nil }, nil)
			var stall *StallError
			if !errors.As(err, &stall) {
				t.Fatalf("沉默的上游应报 StallError，实际 %v", err)
			}
			if took := time.Since(start); took > 3*time.Second {
				t.Fatalf("沉默 150ms 就该放弃，实际等了 %v", took)
			}
			select {
			case <-upstreamGone:
			case <-time.After(3 * time.Second):
				t.Fatal("上游连接没有被关闭")
			}
		})
	}
}

// 连上了却一直不回响应头，同样按沉默计时。
func TestStallWatchEndsAnUpstreamThatNeverAnswers(t *testing.T) {
	withStallLimit(t, 150*time.Millisecond)
	for _, p := range upstreamPaths {
		t.Run(p.name, func(t *testing.T) {
			endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body) // 读完请求体，服务端才盯得到连接断开
				select {
				case <-r.Context().Done():
				case <-time.After(10 * time.Second):
				}
			}))

			_, _, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
				map[string]string{}, proxyURL, nil, nil)
			var stall *StallError
			if !errors.As(err, &stall) {
				t.Fatalf("不回响应头的上游应报 StallError，实际 %v", err)
			}
		})
	}
}

// 客户端断开不是上游沉默：照旧报 context.Canceled，调用方据此不重试、不记出口的错。
func TestStallWatchLeavesClientCancelAlone(t *testing.T) {
	withStallLimit(t, 10*time.Second)
	for _, p := range upstreamPaths {
		t.Run(p.name, func(t *testing.T) {
			endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body) // 读完请求体，服务端才盯得到连接断开
				select {
				case <-r.Context().Done():
				case <-time.After(10 * time.Second):
				}
			}))

			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(50*time.Millisecond, cancel)
			_, _, _, _, err := doGeminiRequestStop(ctx, endpoint, "f.req=x",
				map[string]string{}, proxyURL, nil, nil)
			var stall *StallError
			if errors.As(err, &stall) || !errors.Is(err, context.Canceled) {
				t.Fatalf("客户端断开应报 context.Canceled，实际 %v", err)
			}
		})
	}
}
