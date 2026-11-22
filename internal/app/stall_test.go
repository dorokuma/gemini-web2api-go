package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func withStallLimit(t *testing.T, d time.Duration) {
	t.Helper()
	prev := stallLimit
	stallLimit = func() time.Duration { return d }
	t.Cleanup(func() { stallLimit = prev })
}

func withStreamIdleLimit(t *testing.T, d time.Duration) {
	t.Helper()
	prev := streamIdleLimit
	streamIdleLimit = func() time.Duration { return d }
	t.Cleanup(func() { streamIdleLimit = prev })
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
	// 沉默窗口放宽到 350ms：-race 下 TLS+HTTP/2 握手偶发 >150ms，窗口太短会在
	// 握手阶段误掐（同 TestStallWatchWaitsThroughHeartbeats）。
	withStallLimit(t, 350*time.Millisecond)
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
	// 沉默窗口放宽到 350ms：linux/arm64 + -race 下 tls-client 首次 TLS+HTTP/2 握手
	// 偶发 ~170ms，150ms 的窗口会在握手阶段就把请求掐掉（假红）。心跳间隔提到
	// 180ms 让判据不变：排队心跳总时长（>1s）仍显著大于单次沉默上限。
	withStallLimit(t, 350*time.Millisecond)
	for _, p := range upstreamPaths {
		t.Run(p.name, func(t *testing.T) {
			endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				w.Write([]byte(")]}'\nfirst frame\n"))
				w.(http.Flusher).Flush()
				for i := 0; i < 6; i++ {
					time.Sleep(180 * time.Millisecond)
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
			if took := time.Since(start); took < 3*350*time.Millisecond {
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

// 开流吐字后块间沉默超过 stream_idle_timeout_sec 才掐断；没到 idle 上限的缓慢
// 吐字不被误杀。首个内容帧到达前 idle 不生效（排队心跳仍由 request_timeout_sec
// 的沉默窗口续着）。
func TestStallWatchSwitchesToIdleAfterFirstContent(t *testing.T) {
	withStallLimit(t, 10*time.Second) // 排队窗口够长，肯定不是它掐的
	withStreamIdleLimit(t, 250*time.Millisecond)
	for _, p := range upstreamPaths {
		t.Run(p.name, func(t *testing.T) {
			content := mkContentLine("c_idle", "r_1", "第一段", "3.6 Flash", nil, "")
			endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				fl := w.(http.Flusher)
				fmt.Fprint(w, ")]}'\n"+content+"\n")
				fl.Flush()
				// 开流吐字后沉默超过 idle：应被 idle 掐断，而不是等满总窗口。
				time.Sleep(900 * time.Millisecond)
				fmt.Fprint(w, content+"\n")
			}))

			start := time.Now()
			_, raw, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
				map[string]string{}, proxyURL, func(string) error { return nil }, nil)
			if !errors.Is(err, errStreamIdle) {
				t.Fatalf("开流后块间沉默应报 errStreamIdle，实际 %v", err)
			}
			if took := time.Since(start); took > 3*time.Second {
				t.Fatalf("idle 250ms 就该掐断，实际等了 %v", took)
			}
			if !strings.Contains(string(raw), "第一段") {
				t.Fatalf("首帧内容丢了：%q", raw)
			}
		})
	}
}

// 开流前只发心跳（没有内容帧）时 idle 不生效：沉默窗口仍是 request_timeout_sec，
// 短 idle 不会在排队期误杀。
func TestStallWatchIdleDoesNotFireDuringHeartbeatQueue(t *testing.T) {
	withStallLimit(t, 2*time.Second)
	withStreamIdleLimit(t, 150*time.Millisecond) // 比心跳间隔短：若误生效会在排队期掐断
	for _, p := range upstreamPaths {
		t.Run(p.name, func(t *testing.T) {
			content := mkContentLine("c_q", "r_1", "answer", "3.6 Flash", nil, "")
			endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				fl := w.(http.Flusher)
				w.Write([]byte(")]}'\n"))
				fl.Flush()
				for i := 0; i < 3; i++ {
					time.Sleep(300 * time.Millisecond) // > idle 的心跳间隔
					w.Write([]byte("heartbeat\n"))
					fl.Flush()
				}
				fmt.Fprint(w, content+"\n")
			}))

			var got []string
			_, raw, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
				map[string]string{}, proxyURL, func(line string) error {
					got = append(got, line)
					return nil
				}, nil)
			if err != nil {
				t.Fatalf("排队心跳期被 idle 误杀：%v", err)
			}
			if !strings.Contains(string(raw), "answer") {
				t.Fatalf("没等到排队之后的回复：%q", raw)
			}
		})
	}
}

// idle<=0 或未超总窗口时行为与未配 idle 完全一致：掐断仍报 StallError。
func TestStallWatchIdleDisabledOrGeLimit(t *testing.T) {
	withStallLimit(t, 150*time.Millisecond)
	// idle=0 与 idle>=limit 两种都不收缩窗口：停顿 400ms 触发的是 StallError。
	for _, idle := range []time.Duration{0, 10 * time.Second} {
		for _, p := range upstreamPaths {
			t.Run(fmt.Sprintf("%s/idle=%v", p.name, idle), func(t *testing.T) {
				withStreamIdleLimit(t, idle)
				endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(200)
					w.Write([]byte(")]}'\nfirst frame\n"))
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
					case <-time.After(5 * time.Second):
					}
				}))
				_, _, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
					map[string]string{}, proxyURL, nil, nil)
				var stall *StallError
				if !errors.As(err, &stall) {
					t.Fatalf("idle=%v 时不应出现 errStreamIdle，实际 %v", idle, err)
				}
			})
		}
	}
}

// errStreamIdle 的重试语义：不能被当成「上游总超时」，否则 retry_on_timeout=关
// 时会连它一起挡掉。它是瞬时抖动，必须无条件重试（同上游 #81）。
func TestStreamIdleRetrySemantics(t *testing.T) {
	if isUpstreamTimeout(errStreamIdle) {
		t.Error("errStreamIdle 不该算 isUpstreamTimeout（会被 retry_on_timeout=关 一起挡掉）")
	}
	if !retryAfterTimeout(errStreamIdle) {
		t.Error("errStreamIdle 应当总是重试")
	}
	// retry_on_timeout 关掉时，真正的总超时（StallError）仍被挡、idle 仍放行。
	rtMu.Lock()
	saved := rtVal.RetryOnTimeout
	rtVal.RetryOnTimeout = false
	rtMu.Unlock()
	t.Cleanup(func() { rtMu.Lock(); rtVal.RetryOnTimeout = saved; rtMu.Unlock() })
	if retryAfterTimeout(&StallError{Limit: time.Second}) {
		t.Error("retry_on_timeout=关 时 StallError 不该重试")
	}
	if !retryAfterTimeout(errStreamIdle) {
		t.Error("retry_on_timeout=关 时 errStreamIdle 仍应重试")
	}
}

// 并发 markStreaming/heard/pause/resume/done 不产生 data race（go test -race 专查）。
func TestStallWatchConcurrentMarkHeardNoRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newStallWatchIdle(ctx, 50*time.Millisecond, 20*time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				w.heard()
				w.markStreaming()
				w.pause()
				w.resume()
			}
		}()
	}
	wg.Wait()
	w.done()
	if err := w.explain(context.Canceled); err != context.Canceled {
		t.Fatalf("done 后 explain 应原样返回，实际 %v", err)
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
