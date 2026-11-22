package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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
	// 沉默窗口放宽到 700ms：-race 下 tls-client 冷 TLS+HTTP/2 握手实测偶发
	// ~360ms，350ms 窗口仍会在握手阶段误掐（同 TestStallWatchWaitsThroughHeartbeats）。
	withStallLimit(t, 700*time.Millisecond)
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
	// 沉默窗口放宽到 700ms：linux/arm64 + -race 下 tls-client 冷 TLS+HTTP/2 握手
	// 实测偶发 ~360ms，350ms 窗口仍会在握手阶段就把请求掐掉（假红）。心跳间隔
	// 180ms 让判据不变：排队心跳总时长（>1s）仍显著大于单次沉默上限。
	withStallLimit(t, 700*time.Millisecond)
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
			if took := time.Since(start); took < 700*time.Millisecond {
				t.Fatalf("测试前提不成立：排队只用了 %v，没比沉默上限长", took)
			}
		})
	}
}

// 首帧之后就不再出字的上游，沉默满上限就放弃，报 StallError，并真的断开上游连接。
func TestStallWatchEndsAQuietUpstream(t *testing.T) {
	// 沉默窗口 700ms：linux/arm64 + -race 下 tls-client 冷 TLS+HTTP/2 握手实测
	// 偶发 ~360ms，350ms 窗口会在握手阶段就掐断请求——服务端 handler 压根没跑到，
	// upstreamGone 永不关闭，等满 5s 上限报「上游连接没有被关闭」（5.18s 假红，
	// 与 TestStallWatchWaitsThroughHeartbeats 同类的窗口过短问题）。
	withStallLimit(t, 700*time.Millisecond)
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
				t.Fatalf("沉默 350ms 就该放弃，实际等了 %v", took)
			}
			select {
			case <-upstreamGone:
				// -race 下 tls-client HTTP/2 连接复用的取消传播偶发延迟（基线即存），
				// 放宽到 5s 仍能抓住「根本没取消」的回归。
			case <-time.After(5 * time.Second):
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

// idle<=0 或未超总窗口时行为与未配 idle 完全一致：即使首个**有效内容帧**已到、
// 调用方带了逐行回调，也不切换 idle 窗口——沉默满上限仍报 StallError。
// （用真实内容帧 + 非 nil 回调验证：光传 nil 回调测不出「切换被正确跳过」。）
func TestStallWatchIdleDisabledOrGeLimit(t *testing.T) {
	// 沉默窗口 700ms：同 TestStallWatchEndsAQuietUpstream，-race 下冷握手实测
	// 偶发 ~360ms，会在服务端分发前掐断，内容帧根本到不了回调（假红）。
	withStallLimit(t, 700*time.Millisecond)
	// idle=0 与 idle>=limit 两种都不收缩窗口：停顿 400ms 触发的是 StallError。
	for _, idle := range []time.Duration{0, 10 * time.Second} {
		for _, p := range upstreamPaths {
			t.Run(fmt.Sprintf("%s/idle=%v", p.name, idle), func(t *testing.T) {
				withStreamIdleLimit(t, idle)
				content := mkContentLine("c_idle", "r_1", "first", "3.6 Flash", nil, "")
				endpoint, proxyURL := p.start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(200)
					w.Write([]byte(")]}'\n" + content + "\n"))
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
					case <-time.After(5 * time.Second):
					}
				}))
				var sawContent bool
				_, _, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
					map[string]string{}, proxyURL, func(line string) error {
						if strings.Contains(line, "first") {
							sawContent = true
						}
						return nil
					}, nil)
				var stall *StallError
				if !errors.As(err, &stall) {
					t.Fatalf("idle=%v 时不应出现 errStreamIdle，实际 %v", idle, err)
				}
				if !sawContent {
					t.Fatalf("idle=%v 时内容帧没到回调，前提不成立", idle)
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

// 并发 markStreaming/heard/pause/resume/done 不产生 data race（go test -race 专查），
// 且所有 goroutine 各自跑完循环、真实交错（不是恒真断言）。
func TestStallWatchConcurrentMarkHeardNoRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newStallWatchIdle(ctx, 5*time.Second, 200*time.Millisecond) // 窗口足够长，不会在循环中到期
	// Add 必须先于 goroutine 启动全部完成，主 goroutine 再 Wait——
	// Add 与 Wait 并发是 WaitGroup 自身的 data race（-race 必现）。
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
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("并发循环没在限定时间内跑完（疑似死锁）")
	}
	if !w.streaming.Load() {
		t.Fatal("markStreaming 已被调用，streaming 应为 true")
	}
	w.done()
	if err := w.explain(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("done 后 explain 对无关错误应原样返回 context.Canceled，实际 %v", err)
	}
}

// M1：onLine 为 nil 的非流式请求，首个有效内容帧之后卡住也要按
// stream_idle_timeout_sec 掐断（此前 onLine==nil 时根本不会标开流）。
func TestStallWatchIdleAppliesWhenOnLineNil(t *testing.T) {
	withStallLimit(t, 10*time.Second) // 排队窗口够长，肯定不是它掐的
	withStreamIdleLimit(t, 200*time.Millisecond)
	content := mkContentLine("c_nil", "r_1", "第一段", "3.6 Flash", nil, "")
	endpoint, proxyURL := upstreamPaths[0].start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, ")]}'\n"+content+"\n")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	_, _, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
		map[string]string{}, proxyURL, nil, nil)
	if !errors.Is(err, errStreamIdle) {
		t.Fatalf("onLine=nil 的开流后沉默应报 errStreamIdle，实际 %v", err)
	}
}

// M2：应用层业务哨兵错（*ModelDowngradedError、*argonIdentityError 等）哪怕正好
// 赶在 stallWatch 掐断的同一刻返回，也不能被 explain 覆写成超时原因。
func TestStallWatchExplainKeepsSentinelErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newStallWatchIdle(ctx, 50*time.Millisecond, 10*time.Millisecond)
	defer w.done()
	// 排队期先掐断，制造 context.Cause = *StallError 的现场。
	time.Sleep(120 * time.Millisecond)
	mde := &ModelDowngradedError{Requested: "gemini-4-carbon", Reported: "3.6 Flash"}
	if got := w.explain(mde); got != mde {
		t.Fatalf("*ModelDowngradedError 不能被覆写，实际 %v", got)
	}
	aie := &argonIdentityError{name: "4 Carbon", id: "x"}
	if got := w.explain(aie); got != aie {
		t.Fatalf("*argonIdentityError 不能被覆写，实际 %v", got)
	}
	if got := w.explain(errArgonIdentityMissing); !errors.Is(got, errArgonIdentityMissing) {
		t.Fatalf("errArgonIdentityMissing 不能被覆写，实际 %v", got)
	}
	if got := w.explain(errUpstreamFailureReply); !errors.Is(got, errUpstreamFailureReply) {
		t.Fatalf("errUpstreamFailureReply 不能被覆写，实际 %v", got)
	}
	if got := w.explain(bufio.ErrTooLong); !errors.Is(got, bufio.ErrTooLong) {
		t.Fatalf("bufio.ErrTooLong 不能被覆写，实际 %v", got)
	}
	// 读路径被掐出来的 context.Canceled 才换掐断原因。
	var stall *StallError
	if got := w.explain(context.Canceled); !errors.As(got, &stall) {
		t.Fatalf("ctx 取消类错误应换回 StallError，实际 %v", got)
	}
	// 与掐断无关的普通错误原样返回。
	plain := errors.New("some application error")
	if got := w.explain(plain); got != plain {
		t.Fatalf("无关错误不能被覆写成超时，实际 %v", got)
	}
}

// S3：排队期 timer 已到期、回调还没执行时并发 markStreaming，掐断原因必须仍是
// 排队超时的 *StallError，不能被错挂成 errStreamIdle（阶段由 timer 专属回调决定）。
// 用 0 < idle < limit：idle 真实生效（旧写法 idle>limit 时 idle 被禁用，测不到
// 两条窗口并存的路径）；并覆盖 markStreaming 的 CAS 已完成、持锁换 timer 之前的
// 半发布窗口——期间 heard 绝不能按 streaming 把排队计时器误缩成 idle。
func TestStallWatchQueuedExpiryStaysStallError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const idle, limit = 40 * time.Millisecond, 400 * time.Millisecond
	w := newStallWatchIdle(ctx, limit, idle)
	defer w.done()
	if w.idle <= 0 {
		t.Fatalf("idle 应生效（0 < idle < limit），实际 %v", w.idle)
	}
	// 半发布窗口：streaming 已置位，当前 timer 仍是排队计时器（timerIdle=false）。
	// limitForLocked 只看锁内 timerIdle，所以这里的 heard 重置的仍是 limit 窗口，
	// 不会在 idle 到期时提前掐断。
	w.streaming.Store(true)
	w.heard()
	select {
	case <-w.ctx.Done():
		t.Fatalf("heard 把排队计时器误缩成 idle：%v 内被掐断，实际 %v", 4*idle, context.Cause(w.ctx))
	case <-time.After(4 * idle): // > idle，< limit：误缩成 idle 此刻必然已触发
	}
	// 另起一只 0<idle<limit 的新 watch，等它的排队计时器真实到期、回调已落
	// 盘后调 markStreaming：CAS 成功并真走 armIdleLocked——后建的 idle 计时器
	// 改判不了先到期的 *StallError（WithCancelCause 首个 cancel 写死原因）。
	//（旧写法直接复用 w，streaming 已置位导致 markStreaming 整体 no-op，
	// 根本测不到这段。）
	w2 := newStallWatchIdle(context.Background(), 40*time.Millisecond, 10*time.Millisecond)
	defer w2.done()
	select {
	case <-w2.ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("w2 的排队计时器没有按 40ms 上限掐断")
	}
	w2.markStreaming()
	if w2.idle <= 0 {
		t.Fatalf("w2 的 idle 应生效（0 < idle < limit），实际 %v", w2.idle)
	}
	if !w2.streaming.Load() {
		t.Fatal("markStreaming 后 streaming 应为 true")
	}
	cause := context.Cause(w2.ctx)
	var stall *StallError
	if !errors.As(cause, &stall) {
		t.Fatalf("排队到期应报 *StallError，实际 %v", cause)
	}
	if errors.Is(cause, errStreamIdle) {
		t.Fatalf("排队到期不能报 errStreamIdle，实际 %v", cause)
	}
}

// S4b：request_timeout_sec=0（w.timer==nil）时 pause 也要记阶段：暂停期的
// markStreaming 只记 timerIdle、不建 idle 计时器（暂停期间沉默不计时），
// resume 才把 idle 计时器补回来；此后沉默满 idle 报 errStreamIdle。
func TestStallWatchPauseResumeWithNoQueueLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const idle = 80 * time.Millisecond
	w := newStallWatchIdle(ctx, 0, idle)
	defer w.done()
	if w.idle <= 0 {
		t.Fatalf("idle 应生效，实际 %v", w.idle)
	}
	w.pause()
	// 暂停期间开流：只记阶段（timerIdle=true），不建 idle 计时器。
	w.markStreaming()
	// 暂停中沉默不计时：等 >idle 也不能掐断。
	select {
	case <-w.ctx.Done():
		t.Fatalf("暂停期间不应掐断，实际 %v", context.Cause(w.ctx))
	case <-time.After(2 * idle):
	}
	// resume 补上 idle 计时器：此后沉默满 idle 报 errStreamIdle。
	w.resume()
	select {
	case <-w.ctx.Done():
		if !errors.Is(context.Cause(w.ctx), errStreamIdle) {
			t.Fatalf("resume 后沉默满 idle 应报 errStreamIdle，实际 %v", context.Cause(w.ctx))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("resume 后 idle 计时器没有生效")
	}
}

// S4：request_timeout_sec=0（不限总窗口）但配了 idle 时，首个有效内容帧到达后
// 沉默超过 idle 仍按 errStreamIdle 掐断（此前 w.timer==nil 让 idle 静默失效）。
func TestStallWatchIdleWithNoQueueLimit(t *testing.T) {
	withStallLimit(t, 0) // 排队不限时：stallWatch 不建排队计时器
	withStreamIdleLimit(t, 200*time.Millisecond)
	content := mkContentLine("c_nolimit", "r_1", "第一段", "3.6 Flash", nil, "")
	endpoint, proxyURL := upstreamPaths[0].start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		fmt.Fprint(w, ")]}'\n"+content+"\n")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	_, _, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
		map[string]string{}, proxyURL, func(string) error { return nil }, nil)
	if !errors.Is(err, errStreamIdle) {
		t.Fatalf("limit=0 且开流后沉默应报 errStreamIdle，实际 %v", err)
	}
}

// S5：生图占位链（image_generation_content/…）和纯代码执行产物占位帧不算
// 开流——它们清洗后是空串，不能把沉默窗口缩成 stream_idle_timeout_sec。
func TestStallWatchPlaceholderFramesDoNotMarkStreaming(t *testing.T) {
	withStallLimit(t, 500*time.Millisecond) // 排队窗口 < 占位帧后的沉默，得是它掐的
	withStreamIdleLimit(t, 100*time.Millisecond)
	imgPlaceholder := mkContentLine("c_img", "r_1",
		"http://googleusercontent.com/image_generation_content/0_480\n", "3.6 Flash", nil, "")
	codeOnly := mkContentLine("c_code", "r_1",
		"```text?code_stdout&code_event_index=1\nstdout stuff\n```\n", "3.6 Flash", nil, "")
	for name, frame := range map[string]string{"image_placeholder": imgPlaceholder, "code_artifact": codeOnly} {
		t.Run(name, func(t *testing.T) {
			endpoint, proxyURL := upstreamPaths[0].start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				fmt.Fprint(w, ")]}'\n"+frame+"\n")
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			}))
			_, _, _, _, err := doGeminiRequestStop(context.Background(), endpoint, "f.req=x",
				map[string]string{}, proxyURL, func(string) error { return nil }, nil)
			var stall *StallError
			if !errors.As(err, &stall) {
				t.Fatalf("%s 占位帧后沉默应报 *StallError（未开流），实际 %v", name, err)
			}
			if errors.Is(err, errStreamIdle) {
				t.Fatalf("%s 占位帧不该触发 idle 掐断，实际 %v", name, err)
			}
		})
	}
}

// S1：lineHandler（stop/watch/deltaTracker）每轮重试都要全新构造——Attempt 0
// 已经用闭合 tool_call 触发过 stopWhen、或把失败帧增量计进 watch 之后，
// Attempt 1 若复用脏 lh 会首帧提前 EOF 或直接漏掉 stopWhen 判定。
//
// 端到端走真实路径：假 RoundTripper 第 1 次请求回「半截 tool_call 帧 + 一条坏帧」
// 掐出 EOF 触发重试，第 2 次回完整 tool_call 帧——只有 stop 生效才会让请求在
// 「tail」帧到达前结束。
func TestStreamGenerateRetryRebuildsLineHandler(t *testing.T) {
	seedXSRFAndRetry(t, 3)
	fence := "```tool_call\n{\"name\":\"bash\",\"arguments\":{\"command\":\"ls\"}}\n```"
	complete := mkContentLine("c_ok", "r_1", fence, "3.6 Flash", nil, "")
	// inner[31]=true 是上游出错话术标记（见 failureReplyInLine）：非流式路径
	// 由 hasFailureReply 补判、走重试，能稳定制造 Attempt 0 失败。
	failFrame := mkContentLine("c_f", "r_1", "I seem to be encountering an error", "3.6 Flash", true, "")
	var calls int32
	secondReads := make(chan int, 8) // Attempt 1 响应体每次 Read 报数
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/app":
			return respJSON(req, 200, fakeAppTokens), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			if atomic.AddInt32(&calls, 1) == 1 {
				// Attempt 0：完整 tool_call 帧（非流式下 watch 记住这段并
				// stopWhen→stop 置位）+ 带 inner[31]=true 的出错话术帧，
				// 上层补判走重试。lh 若跨轮复用，Attempt 1 会带着已置位的
				// stop 进 readBody——首帧前就被 stopReader 掐成 EOF。
				return respJSON(req, 200, ")]}'\n"+complete+"\n"+failFrame+"\n"), nil
			}
			// Attempt 1：交付完整 tool_call 帧后挂住，直到请求 ctx 被取消
			//（std lib 关 body）才 EOF。干净的 lh：帧边界 stopWhen→stop
			// 置位 → stopReader 下一次 Read 返回 EOF → resp.Body.Close →
			// 挂住的 Read 解除，本轮立刻收尾。脏 lh：stop 残留 → 首帧
			// 提前 EOF（本轮没内容帧、失败）；或 watch 带上轮 fence →
			// stopWhen 不触发 → Read 一直挂到 stall 窗口掐断。
			return &http.Response{
				StatusCode: 200, Status: "200",
				Header:  http.Header{},
				Body:    &stallGateBody{prefix: ")]}'\n" + complete + "\n", reads: secondReads, req: req},
				Request: req,
			}, nil
		}
		return respJSON(req, 404, ""), nil
	})
	fakeUpstreamServer(t, rt)
	withStallLimit(t, 5*time.Second) // 真卡住时兜底掐断，不拖死测试

	start := time.Now()
	_, err := streamGenerateOnce(context.Background(), "run ls", promptCtx{Latest: "run ls"},
		ModelConfig{HexID: hexFlash36, Mode: 1}, nil, nil, nil,
		toolCallComplete(toolsFixture("bash")), map[int64]bool{})
	if err != nil {
		t.Fatalf("重试后应按 stopWhen 正常收尾，实际 err=%v", err)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Fatalf("应发了 2 次上游请求，实际 %d", n)
	}
	// 干净的 lh：stopWhen 在帧边界就置位，挂住的 Read 立刻被关、本轮远早于
	// stall 窗口收尾；脏 lh 要么失败要么被 5s stall 窗口掐断。
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("stopWhen 未生效：挂着的上游没有被提前掐断，等了 %v", took)
	}
	// 至少见过一次 Read 交付 prefix（否则根本没读到帧，前提不成立）。
	select {
	case <-secondReads:
	default:
		t.Fatal("Attempt 1 的响应体一次都没被读到")
	}
}

// stallGateBody 先吐一段 prefix，之后每次 Read 挂到请求 ctx 取消才 EOF，
// 并把每次 Read 报给 reads：测试用它确认「帧已交付、挂起被请求取消解开」。
type stallGateBody struct {
	prefix string
	sent   bool
	reads  chan int
	req    *http.Request
}

func (b *stallGateBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		n := copy(p, b.prefix)
		select {
		case b.reads <- n:
		default:
		}
		return n, nil
	}
	<-b.req.Context().Done()
	return 0, io.EOF
}

func (b *stallGateBody) Close() error { return nil }

// S6：errStreamIdle 要归到 upstream_timeout（超时类诊断），不落泛化 upstream_error。
func TestUpstreamReasonClassifiesStreamIdle(t *testing.T) {
	if got := upstreamReason(errStreamIdle); got != reasonUpstreamTimeout {
		t.Fatalf("errStreamIdle 应归 %q，实际 %q", reasonUpstreamTimeout, got)
	}
	if got := upstreamReason(&StallError{Limit: time.Second}); got != reasonUpstreamTimeout {
		t.Fatalf("StallError 应归 %q，实际 %q", reasonUpstreamTimeout, got)
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
