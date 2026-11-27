package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func withWCTimings(t *testing.T, ackWait, promptGap time.Duration) {
	t.Helper()
	prevAck, prevGap := wcAckWait, wcPromptGap
	wcAckWait, wcPromptGap = ackWait, promptGap
	t.Cleanup(func() {
		wcAckWait, wcPromptGap = prevAck, prevGap
	})
}

func withWCCachedKey(t *testing.T, key string) {
	t.Helper()
	wcKeyMu.Lock()
	oldKey, oldAt := wcKey, wcKeyAt
	wcKey = key
	if key == "" {
		wcKeyAt = time.Time{}
	} else {
		wcKeyAt = time.Now()
	}
	wcKeyMu.Unlock()
	t.Cleanup(func() {
		wcKeyMu.Lock()
		wcKey, wcKeyAt = oldKey, oldAt
		wcKeyMu.Unlock()
	})
}

func registerWCMockProxy(t *testing.T, fakeProxy string, rt http.RoundTripper, reqTimeoutSec int) {
	t.Helper()
	stdlibClientCache.Store(fakeProxy+"|0", &http.Client{Transport: rt})
	stdlibClientCache.Store(fakeProxy+"|"+fmt.Sprint(reqTimeoutSec), &http.Client{
		Transport: rt,
		Timeout:   time.Duration(reqTimeoutSec) * time.Second,
	})
	t.Cleanup(func() {
		stdlibClientCache.Delete(fakeProxy + "|0")
		stdlibClientCache.Delete(fakeProxy + "|" + fmt.Sprint(reqTimeoutSec))
	})
}

func wcFrame(id int64, payload string) string {
	body := fmt.Sprintf("[[%d,%s]]", id, payload)
	return fmt.Sprintf("%d\n%s", len(body), body)
}

func wcTextPayload(cid, text, flag string) string {
	tb, _ := json.Marshal(text)
	cb, _ := json.Marshal(cid)
	x := `[null,[` + string(cb) + `,"r_2"],null,null,[["rc_3",[` + string(tb) + `],null,null,null,null,null,null,[1],"zh"]],` +
		`null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,` +
		`"fbb127bbb056c959",null,null,"3.6 Flash",true]`
	return `[[null,["5",null,null,null,null,null,[[` + flag + `,["r_2"],null,[null,null,null,` + x + `]]]]]]`
}

func wcMinimalCIDPayload(cid string) string {
	cb, _ := json.Marshal(cid)
	x := `[null,[` + string(cb) + `,"r_1"],null,4]`
	return `[[null,["5",null,null,null,null,null,[[0,["r_1"],null,[null,null,null,` + x + `]]]]]]`
}

// ctxPipeBody 包装 io.PipeReader：当请求 ctx 取消时自动用 ctx.Err() 关闭读端，
// 模拟真实 HTTP Transport 在 ctx 取消时立刻打断 Response.Body.Read。
type ctxPipeBody struct {
	pr   *io.PipeReader
	stop func() bool
}

func newCtxPipeBody(ctx context.Context) (*io.PipeWriter, io.ReadCloser) {
	pr, pw := io.Pipe()
	stop := context.AfterFunc(ctx, func() {
		_ = pr.CloseWithError(ctx.Err())
	})
	return pw, &ctxPipeBody{pr: pr, stop: stop}
}

func (b *ctxPipeBody) Read(p []byte) (int, error) { return b.pr.Read(p) }
func (b *ctxPipeBody) Close() error {
	b.stop()
	return b.pr.Close()
}

func wcHTTPResp(req *http.Request, status int, h http.Header, body string) *http.Response {
	if h == nil {
		h = http.Header{}
	}
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d", status),
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func wcHandshakeOKResp(req *http.Request, h http.Header) *http.Response {
	if h == nil {
		h = http.Header{}
	}
	h.Set("X-HTTP-Session-Id", "GSID_TEST")
	return wcHTTPResp(req, 200, h, wcFrame(0, `["c","SID_TEST"]`))
}

func isWCHandshakeReq(req *http.Request) bool {
	return req.Method == "POST" && strings.Contains(req.URL.RawQuery, "X-HTTP-Session-Id=gsessionid")
}

// 前置项 1：调用方 ctx 已取消时直接返回，不发任何上游 HTTP 请求。
func TestWCCtxPreCancelled(t *testing.T) {
	withWCCachedKey(t, "")
	var calls atomic.Int32
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return wcHTTPResp(req, 500, nil, ""), nil
	})
	const fakeProxy = "http://wcprecancel.invalid:3128"
	registerWCMockProxy(t, fakeProxy, rt, rtCfg().RequestTimeout)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, _, err := doWebChannelRequest(ctx, wcRequest{HexID: hexFlash36, Mode: 1, Prompt: "hi"}, fakeProxy, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("预取消应返回 context.Canceled，实际 %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("预取消不该发任何请求，实际发了 %d 次", calls.Load())
	}
}

// 前置项 1：调用方 ctx 取消必须传播至 API key 抓取、握手、上行、wcPromptGap 等待与下行流读取，
// 及时中止上游连接并返回 context.Canceled（而不是误报 StallError / errUpstreamTimeout）。
func TestWCCtxCancelPropagatesToAllPhases(t *testing.T) {
	cases := []struct {
		name string
		mode string // apikey | handshake | uplink0 | prompt-gap | downlink
	}{
		{"apikey", "apikey"},
		{"handshake", "handshake"},
		{"uplink-ofs0", "uplink0"},
		{"prompt-gap", "prompt-gap"},
		{"downlink-stream", "downlink"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withStallLimit(t, 10*time.Second)
			if tc.mode == "prompt-gap" {
				withWCTimings(t, 5*time.Second, 5*time.Second)
			} else {
				withWCTimings(t, 3*time.Second, 5*time.Millisecond)
			}
			if tc.mode == "apikey" {
				withWCCachedKey(t, "")
			} else {
				withWCCachedKey(t, wcTestAPIKey)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			upstreamCancelled := make(chan struct{})
			var cancelOnce sync.Once
			markCancelled := func() { cancelOnce.Do(func() { close(upstreamCancelled) }) }

			var ofs1Called atomic.Int32
			rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/app", "/u/0/app":
					if tc.mode == "apikey" {
						time.AfterFunc(30*time.Millisecond, cancel)
						<-req.Context().Done()
						markCancelled()
						return nil, req.Context().Err()
					}
					return wcHTTPResp(req, 200, nil, `"d2zJAe":"`+wcTestAPIKey+`"`), nil
				case "/v1/processSession":
					if isWCHandshakeReq(req) {
						if tc.mode == "handshake" {
							time.AfterFunc(30*time.Millisecond, cancel)
							<-req.Context().Done()
							markCancelled()
							return nil, req.Context().Err()
						}
						return wcHandshakeOKResp(req, nil), nil
					}
					if req.Method == "GET" {
						pw, body := newCtxPipeBody(req.Context())
						go func() {
							defer pw.Close()
							switch tc.mode {
							case "prompt-gap":
								_, _ = io.WriteString(pw, wcFrame(1, `[[[]]]`))
								time.AfterFunc(40*time.Millisecond, cancel)
								<-req.Context().Done()
								markCancelled()
							case "downlink":
								_, _ = io.WriteString(pw, wcFrame(1, `[[[]]]`))
								_, _ = io.WriteString(pw, wcFrame(2, wcTextPayload("c_cancel_1", wcTestText, "0")))
								<-req.Context().Done()
								markCancelled()
							default:
								<-req.Context().Done()
								markCancelled()
							}
						}()
						return &http.Response{
							StatusCode: 200, Status: "200", Header: http.Header{},
							Body: body, Request: req,
						}, nil
					}
					if req.Method == "POST" {
						b, _ := io.ReadAll(req.Body)
						s := string(b)
						if strings.Contains(s, "ofs=0") && tc.mode == "uplink0" {
							time.AfterFunc(30*time.Millisecond, cancel)
							<-req.Context().Done()
							markCancelled()
							return nil, req.Context().Err()
						}
						if strings.Contains(s, "ofs=1") {
							ofs1Called.Add(1)
						}
						return wcHTTPResp(req, 200, nil, ""), nil
					}
				}
				return wcHTTPResp(req, 404, nil, ""), nil
			})

			fakeProxy := "http://wccancel-" + tc.mode + ".invalid:3128"
			registerWCMockProxy(t, fakeProxy, rt, rtCfg().RequestTimeout)

			var lineCount atomic.Int32
			onLine := func(string) error {
				n := lineCount.Add(1)
				if tc.mode == "downlink" && n == 1 {
					cancel()
				}
				return nil
			}

			start := time.Now()
			_, _, _, _, err := doWebChannelRequest(ctx, wcRequest{
				Inner:  []interface{}{[]interface{}{"", 0}},
				Prompt: "hi", HexID: hexFlash36, Mode: 1, Cookie: "SAPISID=x", SAPISID: "x",
			}, fakeProxy, onLine, nil)
			if took := time.Since(start); took > 2*time.Second {
				t.Fatalf("%s: 取消后等待过久 %v", tc.name, took)
			}
			var se *StallError
			if errors.As(err, &se) || errors.Is(err, errUpstreamTimeout) {
				t.Fatalf("%s: 调用方取消不应误报为超时: %v", tc.name, err)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s: 期望 context.Canceled，实际 %v", tc.name, err)
			}
			select {
			case <-upstreamCancelled:
			case <-time.After(2 * time.Second):
				t.Fatalf("%s: 上游请求 ctx 没有收到取消信号", tc.name)
			}
			if tc.mode == "prompt-gap" && ofs1Called.Load() != 0 {
				t.Fatalf("prompt-gap 取消后不应继续发 ofs=1")
			}
		})
	}
}

// 前置项 2：持续出字的回复超过 request_timeout_sec 不能被截断（下行走 stream client，由 stallWatch 接管）。
func TestWCStallWatchKeepsReplyLongerThanRequestTimeout(t *testing.T) {
	withRequestTimeout(t, 1) // 短 RPC 客户端 1s 掐断；流式回复要写 ~1.28s
	withStallLimit(t, 350*time.Millisecond)
	withWCTimings(t, 3*time.Second, 5*time.Millisecond)
	withWCCachedKey(t, wcTestAPIKey)

	ofs1Ready := make(chan struct{})
	var ofs1Once sync.Once
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/v1/processSession" {
			if isWCHandshakeReq(req) {
				return wcHandshakeOKResp(req, nil), nil
			}
			if req.Method == "GET" {
				pw, body := newCtxPipeBody(req.Context())
				go func() {
					defer pw.Close()
					_, _ = io.WriteString(pw, wcFrame(1, `[[[]]]`))
					select {
					case <-ofs1Ready:
					case <-req.Context().Done():
						return
					}
					for i := 1; i <= 16; i++ {
						flag := "0"
						if i == 16 {
							flag = "1"
						}
						txt := fmt.Sprintf("%s 第 %d 段", wcTestText, i)
						_, _ = io.WriteString(pw, wcFrame(int64(i+1), wcTextPayload("c_long_1", txt, flag)))
						time.Sleep(80 * time.Millisecond)
					}
					_, _ = io.WriteString(pw, wcFrame(18, `[[null,null,null,[]]]`))
				}()
				return &http.Response{StatusCode: 200, Status: "200", Header: http.Header{}, Body: body, Request: req}, nil
			}
			if req.Method == "POST" {
				b, _ := io.ReadAll(req.Body)
				if strings.Contains(string(b), "ofs=1") {
					ofs1Once.Do(func() { close(ofs1Ready) })
				}
				return wcHTTPResp(req, 200, nil, ""), nil
			}
		}
		return wcHTTPResp(req, 404, nil, ""), nil
	})

	const fakeProxy = "http://wcstall-long.invalid:3128"
	registerWCMockProxy(t, fakeProxy, rt, 1)

	lines := 0
	start := time.Now()
	status, raw, _, _, err := doWebChannelRequest(context.Background(), wcRequest{
		Inner: []interface{}{[]interface{}{"", 0}}, Prompt: "hi", HexID: hexFlash36, Mode: 1,
	}, fakeProxy, func(string) error {
		lines++
		return nil
	}, nil)
	if err != nil || status != 200 {
		t.Fatalf("持续出字的 WebChannel 回复被掐了: status=%d err=%v", status, err)
	}
	if lines != 16 {
		t.Fatalf("期望 16 行，实际 %d", lines)
	}
	if took := time.Since(start); took < time.Second {
		t.Fatalf("测试前提不成立：回复只用了 %v，没超过 request_timeout_sec=1s", took)
	}
	if !strings.Contains(extractResponseText(string(raw)), "第 16 段") {
		t.Fatalf("最终文本缺失: %q", extractResponseText(string(raw)))
	}
}

// 前置项 2：排队期间上游只发 ["noop"] 心跳帧，总时长超过 stallLimit 也要等到正文。
func TestWCStallWatchWaitsThroughNoopHeartbeats(t *testing.T) {
	withStallLimit(t, 200*time.Millisecond)
	withWCTimings(t, 3*time.Second, 5*time.Millisecond)
	withWCCachedKey(t, wcTestAPIKey)

	ofs1Ready := make(chan struct{})
	var ofs1Once sync.Once
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/v1/processSession" {
			if isWCHandshakeReq(req) {
				return wcHandshakeOKResp(req, nil), nil
			}
			if req.Method == "GET" {
				pw, body := newCtxPipeBody(req.Context())
				go func() {
					defer pw.Close()
					_, _ = io.WriteString(pw, wcFrame(1, `[[[]]]`))
					select {
					case <-ofs1Ready:
					case <-req.Context().Done():
						return
					}
					for i := 0; i < 6; i++ {
						time.Sleep(100 * time.Millisecond)
						_, _ = io.WriteString(pw, wcFrame(int64(i+2), `["noop"]`))
					}
					_, _ = io.WriteString(pw, wcFrame(8, wcTextPayload("c_hb_1", wcTestText, "1")))
					_, _ = io.WriteString(pw, wcFrame(9, `[[null,null,null,[]]]`))
				}()
				return &http.Response{StatusCode: 200, Status: "200", Header: http.Header{}, Body: body, Request: req}, nil
			}
			if req.Method == "POST" {
				b, _ := io.ReadAll(req.Body)
				if strings.Contains(string(b), "ofs=1") {
					ofs1Once.Do(func() { close(ofs1Ready) })
				}
				return wcHTTPResp(req, 200, nil, ""), nil
			}
		}
		return wcHTTPResp(req, 404, nil, ""), nil
	})

	const fakeProxy = "http://wcstall-hb.invalid:3128"
	registerWCMockProxy(t, fakeProxy, rt, rtCfg().RequestTimeout)

	start := time.Now()
	status, raw, _, _, err := doWebChannelRequest(context.Background(), wcRequest{
		Inner: []interface{}{[]interface{}{"", 0}}, Prompt: "hi", HexID: hexFlash36, Mode: 1,
	}, fakeProxy, nil, nil)
	if err != nil || status != 200 {
		t.Fatalf("noop 心跳续命失败: status=%d err=%v", status, err)
	}
	if took := time.Since(start); took < 500*time.Millisecond {
		t.Fatalf("测试前提不成立：只用了 %v，未超过 2.5×stallLimit", took)
	}
	if got := extractResponseText(string(raw)); got != wcTestText {
		t.Fatalf("正文不对: %q", got)
	}
}

// 前置项 2：下行沉默满 stallLimit 必须返回 *StallError 并断开上游连接；
// 同时覆盖「客户端 wcPromptGap(500ms) > stallLimit(150ms) 时不误掐、等 ofs=1 发出后再掐沉默」的边界。
func TestWCStallWatchTerminatesQuietDownlink(t *testing.T) {
	cases := []struct {
		name           string
		mode           string // quiet-after-ofs1 | quiet-after-frame | handshake-hang
		keepDefaultGap bool
	}{
		{"沉默-发完ofs1后无响应(含默认500ms间隔)", "quiet-after-ofs1", true},
		{"沉默-首帧后断流", "quiet-after-frame", false},
		{"沉默-握手挂死", "handshake-hang", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withStallLimit(t, 150*time.Millisecond)
			if !tc.keepDefaultGap {
				withWCTimings(t, 3*time.Second, 5*time.Millisecond)
			}
			withWCCachedKey(t, wcTestAPIKey)

			upstreamGone := make(chan struct{})
			var goneOnce sync.Once
			markGone := func() { goneOnce.Do(func() { close(upstreamGone) }) }
			var ofs1Seen atomic.Int32

			rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/v1/processSession" {
					if isWCHandshakeReq(req) {
						if tc.mode == "handshake-hang" {
							<-req.Context().Done()
							markGone()
							return nil, req.Context().Err()
						}
						return wcHandshakeOKResp(req, nil), nil
					}
					if req.Method == "GET" {
						pw, body := newCtxPipeBody(req.Context())
						go func() {
							defer pw.Close()
							_, _ = io.WriteString(pw, wcFrame(1, `[[[]]]`))
							if tc.mode == "quiet-after-frame" {
								_, _ = io.WriteString(pw, wcFrame(2, wcTextPayload("c_stall_frame", wcTestText, "0")))
							}
							<-req.Context().Done()
							markGone()
						}()
						return &http.Response{StatusCode: 200, Status: "200", Header: http.Header{}, Body: body, Request: req}, nil
					}
					if req.Method == "POST" {
						b, _ := io.ReadAll(req.Body)
						if strings.Contains(string(b), "ofs=1") {
							ofs1Seen.Add(1)
						}
						return wcHTTPResp(req, 200, nil, ""), nil
					}
				}
				return wcHTTPResp(req, 404, nil, ""), nil
			})

			fakeProxy := "http://wcstall-quiet-" + tc.mode + ".invalid:3128"
			registerWCMockProxy(t, fakeProxy, rt, rtCfg().RequestTimeout)

			start := time.Now()
			_, raw, _, _, err := doWebChannelRequest(context.Background(), wcRequest{
				Inner: []interface{}{[]interface{}{"", 0}}, Prompt: "hi", HexID: hexFlash36, Mode: 1,
			}, fakeProxy, nil, nil)
			var se *StallError
			if !errors.As(err, &se) {
				t.Fatalf("%s: 沉默超时应报 *StallError，实际 %v", tc.name, err)
			}
			if !isUpstreamTimeout(err) {
				t.Fatalf("%s: *StallError 必须命中 isUpstreamTimeout", tc.name)
			}
			if took := time.Since(start); took > 3*time.Second {
				t.Fatalf("%s: 超时等待过久 %v", tc.name, took)
			}
			select {
			case <-upstreamGone:
			case <-time.After(2 * time.Second):
				t.Fatalf("%s: 上游连接没有被关闭", tc.name)
			}
			if tc.mode == "quiet-after-ofs1" && ofs1Seen.Load() == 0 {
				t.Fatalf("默认 wcPromptGap 期间不应误触 stallWatch，期望 ofs=1 已发出")
			}
			if tc.mode == "quiet-after-frame" {
				if cid := extractConversationID(string(raw)); cid != "c_stall_frame" {
					t.Fatalf("沉默超时后丢了首帧 cid=%q raw=%q", cid, truncate(string(raw), 120))
				}
			}
		})
	}
}

// 前置项 2：工具调用围栏闭合时通过 &lh.stop 立刻停止读上游，关闭下行连接、返回 200，
// 客户端不泄露围栏原文，最终 raw 能完整解析出 tool_calls（流式与非流式双路径）。
func TestWCStopWhenStopsUpstreamRead(t *testing.T) {
	withStallLimit(t, 10*time.Second)
	withWCTimings(t, 3*time.Second, 5*time.Millisecond)
	withWCCachedKey(t, wcTestAPIKey)

	call := "```tool_call\n{\"name\": \"bash\", \"arguments\": {\"command\": \"ls\"}}\n```"
	// 补足长度确保超过 textsInLine 的 200 字节门槛。
	prefix := "好的，我来执行命令查看当前目录内容列表，请稍候查看结果。"
	frames := []string{
		prefix,
		prefix + "\n```tool_call\n{\"name\": \"ba",
		prefix + "\n" + call,
	}

	for _, streaming := range []bool{true, false} {
		name := "non-stream"
		if streaming {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			upstreamGone := make(chan struct{})
			var goneOnce sync.Once
			markGone := func() { goneOnce.Do(func() { close(upstreamGone) }) }
			ofs1Ready := make(chan struct{})
			var ofs1Once sync.Once

			rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/v1/processSession" {
					if isWCHandshakeReq(req) {
						return wcHandshakeOKResp(req, nil), nil
					}
					if req.Method == "GET" {
						pw, body := newCtxPipeBody(req.Context())
						go func() {
							defer pw.Close()
							_, _ = io.WriteString(pw, wcFrame(1, `[[[]]]`))
							select {
							case <-ofs1Ready:
							case <-req.Context().Done():
								markGone()
								return
							}
							for i, txt := range frames {
								_, _ = io.WriteString(pw, wcFrame(int64(i+2), wcTextPayload("c_tool_1", txt, "0")))
								time.Sleep(15 * time.Millisecond)
							}
							// 模拟上游在工具调用闭合后还拖几秒才发收尾帧：stopWhen 必须提前掐断，不干等。
							select {
							case <-req.Context().Done():
								markGone()
							case <-time.After(10 * time.Second):
							}
						}()
						return &http.Response{StatusCode: 200, Status: "200", Header: http.Header{}, Body: body, Request: req}, nil
					}
					if req.Method == "POST" {
						b, _ := io.ReadAll(req.Body)
						if strings.Contains(string(b), "ofs=1") {
							ofs1Once.Do(func() { close(ofs1Ready) })
						}
						return wcHTTPResp(req, 200, nil, ""), nil
					}
				}
				return wcHTTPResp(req, 404, nil, ""), nil
			})

			fakeProxy := "http://wcstop-" + name + ".invalid:3128"
			registerWCMockProxy(t, fakeProxy, rt, rtCfg().RequestTimeout)

			var sent strings.Builder
			var onDelta func(string)
			if streaming {
				gate := newToolFenceGate(func(s string) { sent.WriteString(s) })
				onDelta = gate.Push
			}
			lh := newLineHandler(onDelta, nil, toolCallComplete(toolsFixture("bash")), nil)

			start := time.Now()
			status, raw, _, _, err := doWebChannelRequest(context.Background(), wcRequest{
				Inner: []interface{}{[]interface{}{"", 0}}, Prompt: "run ls", HexID: hexFlash36, Mode: 1,
			}, fakeProxy, lh.cb, &lh.stop)
			if err != nil || status != 200 {
				t.Fatalf("%s: stopWhen 提前停止应返回 200/nil，实际 status=%d err=%v", name, status, err)
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Fatalf("%s: 未提前停止，等了 %v", name, took)
			}
			if !lh.stop.Load() {
				t.Fatalf("%s: lh.stop 应为 true", name)
			}
			select {
			case <-upstreamGone:
			case <-time.After(2 * time.Second):
				t.Fatalf("%s: 提前停止后未关闭上游下行连接", name)
			}
			if streaming {
				if sent.String() != prefix+"\n" {
					t.Fatalf("流式围栏原文泄露或缺失: got=%q want=%q", sent.String(), prefix+"\n")
				}
			} else if lh.tracker.emitted != "" {
				t.Fatalf("非流式不应污染 tracker.emitted: %q", lh.tracker.emitted)
			}
			_, calls := parseToolCalls(extractResponseText(string(raw)))
			if len(calls) != 1 || calls[0].Function.Name != "bash" || calls[0].Function.Arguments != `{"command":"ls"}` {
				t.Fatalf("%s: 提前收尾后解析不出工具调用: %+v (raw=%q)", name, calls, truncate(string(raw), 200))
			}
		})
	}
}

// 前置项 3：Set-Cookie 按来源主机与目标主机过滤，禁止将 geminiweb-pa.clients6.google.com
// 下发的 host-only / clients6 作用域 / 非根路径 / __Host- / 越权域 cookie 混入账号池。
func TestMergeSetCookieForHostFiltering(t *testing.T) {
	base := "SAPISID=keep_sap; __Secure-1PSID=keep_psid; SIDCC=orig_sidcc"

	t.Run("WebChannel主机非共享cookie全部拦截", func(t *testing.T) {
		blocked := []string{
			"SIDCC=wc_host_only; path=/; secure",
			"WC_ONLY=1; Domain=geminiweb-pa.clients6.google.com; Path=/",
			"CLIENTS6=1; Domain=.clients6.google.com; Path=/",
			"SPOOF=1; Domain=gemini.google.com; Path=/",
			"__Host-GAPS=bad; Path=/; Secure",
			"SIDCC=default_v1_path; Domain=.google.com; Secure",
			"SIDCC=scoped_path; Domain=.google.com; Path=/v1/processSession",
			"SIDCC=part; Domain=.google.com; Path=/; Partitioned",
			"TLD=1; Domain=.com; Path=/",
		}
		got := mergeSetCookieForHost(base, blocked, wcEndpoint, geminiCookieTargetHost)
		if got != base {
			t.Fatalf("clients6 非共享 Set-Cookie 不应混入主账号池:\ngot =%s\nwant=%s", got, base)
		}
	})

	t.Run("WebChannel主机仅允许显式根路径的.google.com共享cookie", func(t *testing.T) {
		got := mergeSetCookieForHost(base, []string{
			"WC_LOCAL=dropme; Path=/",
			"SIDCC=shared_fresh; Domain=.google.com; Path=/; Secure; HttpOnly",
		}, wcEndpoint, geminiCookieTargetHost)
		if strings.Contains(got, "WC_LOCAL") {
			t.Fatalf("host-only cookie 被误混入: %s", got)
		}
		if !strings.Contains(got, "SIDCC=shared_fresh") {
			t.Fatalf("合法 .google.com; Path=/ 共享刷新应保留: %s", got)
		}
	})

	t.Run("StreamGenerate同源主机允许host-only与.google.com但拒绝跨站Domain", func(t *testing.T) {
		sgEndpoint := "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate"
		got := mergeSetCookieForHost(base, []string{
			"SIDCC=sg_new; Path=/; Secure",
			"__Host-OK=1; Path=/; Secure",
			"__Secure-1PSIDCC=psidcc_new; Domain=.google.com; Path=/; Secure",
			"BAD1=x; Domain=.clients6.google.com; Path=/",
			"BAD2=x; Domain=.evil.com; Path=/",
		}, sgEndpoint, geminiCookieTargetHost)
		for _, want := range []string{"SIDCC=sg_new", "__Host-OK=1", "__Secure-1PSIDCC=psidcc_new"} {
			if !strings.Contains(got, want) {
				t.Errorf("合法同源/共享 cookie 缺失 %q: %s", want, got)
			}
		}
		for _, bad := range []string{"BAD1=", "BAD2="} {
			if strings.Contains(got, bad) {
				t.Errorf("非法 Domain cookie %q 不应混入: %s", bad, got)
			}
		}
	})

	t.Run("空或非法host时fail-closed且不污染账号池DB", func(t *testing.T) {
		if got := mergeSetCookieForHost(base, []string{"SIDCC=x; Path=/"}, "", geminiCookieTargetHost); got != base {
			t.Fatalf("空 sourceHost 应 fail-closed: %s", got)
		}
		if got := mergeSetCookieForHost(base, []string{"SIDCC=x; Path=/"}, wcEndpoint, ""); got != base {
			t.Fatalf("空 targetHost 应 fail-closed: %s", got)
		}

		id, err := accountAdd("wc-host-filter", base, "")
		if err != nil {
			t.Fatalf("accountAdd: %v", err)
		}
		t.Cleanup(func() { _ = accountDelete(id) })

		// 模拟 gemini.go:931-936：WC 响应只返回 clients6 作用域/host-only cookie 时，绝不写回 DB。
		wcCookies := []string{
			"SIDCC=poisoned_from_wc; Path=/",
			"CLIENTS6_SID=abc; Domain=.clients6.google.com; Path=/",
		}
		if merged := mergeSetCookieForHost(base, wcCookies, wcEndpoint, geminiCookieTargetHost); merged != base {
			updateAccountCookie(id, merged)
		}
		if a := accountByID(id); a == nil || a.Cookie != base {
			t.Fatalf("DB 账号 cookie 被 clients6 Set-Cookie 污染: got=%+v", a)
		}
	})
}

// 前置项 4：wcParseStatus 对无法识别的 200 状态帧绝不回传原始 payload 前 200 字节，
// 只给出脱敏诊断（字节数 + JSON 形态），避免泄露到 requests.error / last_error / 502 响应体。
func TestWCParseStatusRedactsUnrecognizedFrame(t *testing.T) {
	const probe = "UNRECOG_SECRET_PROBE_999"
	cases := []struct {
		name     string
		payload  string
		wantKind string
	}{
		{"未知object", `{"__sm__":{"status":[[{"token":"` + probe + `","detail":"internal"}]]}}`, "object"},
		{"纯字符串", `{"__sm__":{"status":[["` + probe + `"]]}}`, "string"},
		{"非数字tuple", `{"__sm__":{"status":[[["notnum","` + probe + `"]]]}}`, "array"},
		{"零码tuple", `{"__sm__":{"status":[[[0,"` + probe + `"]]]}}`, "array"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := wcParseStatus([]byte(tc.payload))
			if e == nil {
				t.Fatalf("应返回 *wcStatusError")
			}
			if e.Code != 2 || e.httpStatus() != 502 {
				t.Fatalf("期望 code=2/http=502，实际 code=%d http=%d", e.Code, e.httpStatus())
			}
			if strings.Contains(e.Error(), probe) || strings.Contains(e.Msg, probe) {
				t.Fatalf("未识别状态帧泄露原始探针串: %q", e.Error())
			}
			if !strings.Contains(e.Msg, "unrecognized status frame") || !strings.Contains(e.Msg, "kind="+tc.wantKind) {
				t.Fatalf("脱敏诊断格式不符合预期: %q", e.Msg)
			}
		})
	}

	// 端到端：下行 200 流里收到未识别 __sm__ 帧时，doWebChannelRequest 返回 502 且 raw 不含探针串。
	withStallLimit(t, 5*time.Second)
	withWCTimings(t, 3*time.Second, 5*time.Millisecond)
	withWCCachedKey(t, wcTestAPIKey)

	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/v1/processSession" {
			if isWCHandshakeReq(req) {
				return wcHandshakeOKResp(req, nil), nil
			}
			if req.Method == "GET" {
				body := wcFrame(1, `{"__sm__":{"status":[[{"secret":"`+probe+`"}]]}}`)
				return wcHTTPResp(req, 200, nil, body), nil
			}
			return wcHTTPResp(req, 200, nil, ""), nil
		}
		return wcHTTPResp(req, 404, nil, ""), nil
	})
	const fakeProxy = "http://wcstatus-unrecog.invalid:3128"
	registerWCMockProxy(t, fakeProxy, rt, rtCfg().RequestTimeout)

	status, raw, _, _, err := doWebChannelRequest(context.Background(), wcRequest{
		Inner: []interface{}{[]interface{}{"", 0}}, Prompt: "hi", HexID: hexFlash36, Mode: 1,
	}, fakeProxy, nil, nil)
	if err != nil {
		t.Fatalf("下行状态帧应映射为 HTTP 状态码而非 err: %v", err)
	}
	if status != 502 {
		t.Fatalf("status=%d want 502 (raw=%q)", status, string(raw))
	}
	if strings.Contains(string(raw), probe) {
		t.Fatalf("端到端 502 响应体泄露未识别帧探针串: %s", string(raw))
	}
	if !strings.Contains(string(raw), "unrecognized status frame") {
		t.Fatalf("期望结构化脱敏诊断，实际 %q", string(raw))
	}
}

// 前置项 5：握手 400 含 "xsrf" 透出脱敏结构化标记 wcHandshakeXSRFMarker 供 isXSRFError 识别，
// 不泄露上游正文；非 400 不误标；命中后 healXSRFRefresh 作废 XSRF 与 API key 缓存，重试带新凭证成功。
func TestWCHandshake400XSRFSelfHeal(t *testing.T) {
	const (
		probe   = "XSRF_BODY_SECRET_PROBE"
		oldKey  = "AIzaOLDKEYAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		newKey  = "AIzaNEWKEYBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
		cookie  = "SAPISID=xsrf_sap; __Secure-1PSID=xsrf_psid"
		oldXSRF = "OLD_SNLM0E_TOKEN_1234567890"
		newXSRF = "NEW_SNLM0E_TOKEN_9876543210"
	)

	// 1) 信号透出与脱敏边界（400+"xsrf" / 400-无xsrf / 500+"xsrf"）
	t.Run("握手状态码与xsrf信号过滤", func(t *testing.T) {
		cases := []struct {
			name     string
			hsStatus int
			hsBody   string
			wantXSRF bool
		}{
			{"400含xsrf", 400, `[["er",null,400,[{"48448350":["xsrf","` + probe + `"]}]]]`, true},
			{"400无xsrf", 400, `[["er","` + probe + `",400]]`, false},
			{"500含xsrf不标", 500, `{"error":"xsrf","secret":"` + probe + `","tag":"xsrf"}`, false},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				withWCCachedKey(t, wcTestAPIKey)
				rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
					if req.URL.Path == "/v1/processSession" {
						return wcHTTPResp(req, tc.hsStatus, nil, tc.hsBody), nil
					}
					return wcHTTPResp(req, 404, nil, ""), nil
				})
				fakeProxy := "http://wcxsrf-" + tc.name + ".invalid:3128"
				registerWCMockProxy(t, fakeProxy, rt, rtCfg().RequestTimeout)

				status, raw, _, _, err := doWebChannelRequest(context.Background(), wcRequest{
					HexID: hexFlash36, Mode: 1, Cookie: cookie, SAPISID: "xsrf_sap", Prompt: "hi",
				}, fakeProxy, nil, nil)
				if err != nil {
					t.Fatalf("非 200 握手应返回状态码而非 err: %v", err)
				}
				if status != tc.hsStatus {
					t.Fatalf("status=%d want %d", status, tc.hsStatus)
				}
				if strings.Contains(string(raw), probe) {
					t.Fatalf("诊断泄露上游正文探针串: %q", string(raw))
				}
				if got := isXSRFError(string(raw)); got != tc.wantXSRF {
					t.Fatalf("isXSRFError(%q) = %v, want %v", string(raw), got, tc.wantXSRF)
				}
			})
		}
	})

	// 2) 端到端自愈：400+"xsrf" -> isXSRFError -> healXSRFRefresh 作废 XSRF + wcKey -> 重试换新 API key 成功。
	t.Run("自愈刷新XSRF与APIKey后重试成功", func(t *testing.T) {
		withStallLimit(t, 5*time.Second)
		withWCTimings(t, 3*time.Second, 5*time.Millisecond)
		withWCCachedKey(t, oldKey)

		// 预置旧 XSRF 缓存。
		invalidateXSRF(cookie)
		xsrfMu.Lock()
		xsrfCache[xsrfKey(cookie, 0)] = xsrfEntry{token: oldXSRF, fetched: time.Now()}
		xsrfMu.Unlock()
		t.Cleanup(func() { invalidateXSRF(cookie) })

		var appFetches atomic.Int32
		var hsAttempt atomic.Int32
		var secondHSQuery string
		var qMu sync.Mutex
		ofs1Ready := make(chan struct{})
		var ofs1Once sync.Once

		rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/app", "/u/0/app":
				appFetches.Add(1)
				return wcHTTPResp(req, 200, nil, `"SNlM0e":"`+newXSRF+`","d2zJAe":"`+newKey+`"`), nil
			case "/v1/processSession":
				if isWCHandshakeReq(req) {
					n := hsAttempt.Add(1)
					if n == 1 {
						return wcHTTPResp(req, 400, nil, `[["er",null,400,[{"48448350":["xsrf","`+probe+`"]}]]]`), nil
					}
					qMu.Lock()
					secondHSQuery = req.URL.RawQuery
					qMu.Unlock()
					return wcHandshakeOKResp(req, nil), nil
				}
				if req.Method == "GET" {
					pw, body := newCtxPipeBody(req.Context())
					go func() {
						defer pw.Close()
						_, _ = io.WriteString(pw, wcFrame(1, `[[[]]]`))
						select {
						case <-ofs1Ready:
						case <-req.Context().Done():
							return
						}
						_, _ = io.WriteString(pw, wcFrame(2, wcTextPayload("c_xsrf_ok", wcTestText, "1")))
						_, _ = io.WriteString(pw, wcFrame(3, `[[null,null,null,[]]]`))
					}()
					return &http.Response{StatusCode: 200, Status: "200", Header: http.Header{}, Body: body, Request: req}, nil
				}
				if req.Method == "POST" {
					b, _ := io.ReadAll(req.Body)
					if strings.Contains(string(b), "ofs=1") {
						ofs1Once.Do(func() { close(ofs1Ready) })
					}
					return wcHTTPResp(req, 200, nil, ""), nil
				}
			}
			return wcHTTPResp(req, 404, nil, ""), nil
		})

		const fakeProxy = "http://wcxsrf-heal.invalid:3128"
		registerWCMockProxy(t, fakeProxy, rt, rtCfg().RequestTimeout)

		req := wcRequest{
			Inner: []interface{}{[]interface{}{"", 0}}, Prompt: "hi",
			HexID: hexFlash36, Mode: 1, Cookie: cookie, SAPISID: "xsrf_sap",
		}
		// 第 1 次：握手回 400+"xsrf"
		st1, raw1, _, _, err := doWebChannelRequest(context.Background(), req, fakeProxy, nil, nil)
		if err != nil || st1 != 400 || !isXSRFError(string(raw1)) {
			t.Fatalf("第 1 次期望 400 + isXSRFError=true，实际 st=%d raw=%q err=%v", st1, string(raw1), err)
		}
		if strings.Contains(string(raw1), probe) {
			t.Fatalf("握手 400 诊断泄露原文探针: %q", string(raw1))
		}
		// 走 gemini.go 同款自愈刷新：作废 XSRF + wcKey 缓存并重抓 /app
		tok, herr := healXSRFRefresh(cookie, 0, fakeProxy, true, 0)
		if herr != nil || tok != newXSRF {
			t.Fatalf("healXSRFRefresh 失败: tok=%q err=%v", tok, herr)
		}
		// 第 2 次：重试拿到新 API key 并成功返回 200
		st2, raw2, _, _, err := doWebChannelRequest(context.Background(), req, fakeProxy, nil, nil)
		if err != nil || st2 != 200 {
			t.Fatalf("自愈重试失败: st=%d raw=%q err=%v", st2, string(raw2), err)
		}
		if got := extractResponseText(string(raw2)); got != wcTestText {
			t.Fatalf("自愈重试正文不对: %q", got)
		}
		qMu.Lock()
		q := secondHSQuery
		qMu.Unlock()
		if !strings.Contains(q, newKey) || strings.Contains(q, oldKey) {
			t.Fatalf("自愈后第 2 次握手未使用刷新后的 API key: query=%s", wcRedactURL("https://h/?"+q))
		}
		if appFetches.Load() < 2 {
			t.Fatalf("期望自愈触发重抓 /app（XSRF 与 wcKey），实际抓了 %d 次", appFetches.Load())
		}
	})
}

// 前置项 6：下行帧在读取时线程安全累计进 raw，任何提前返回路径（ev.Ack 上行报错、ackTimer 上行报错、
// onLine 报错、ctx 取消、stallWatch 超时、ev.Err 下行错误）都不丢队列里已到达的 c_xxx 帧，
// 确保 phantomCleanup 能可靠提取并清理幽灵会话。
func TestWCQueueDropPreservesConversationIDOnEarlyReturn(t *testing.T) {
	const ghostCID = "c_deadbeefcafe1234"
	cases := []struct {
		name string
		mode string // ev-ack-err | ack-timer-err | online-err | ctx-cancel | ev-err
	}{
		{"ev.Ack触发post(1)报错不丢残帧", "ev-ack-err"},
		{"ackTimer触发post(1)报错不丢残帧", "ack-timer-err"},
		{"onLine报错不丢后续已读帧", "online-err"},
		{"ctx取消不丢已入队残帧", "ctx-cancel"},
		{"ev.Err不丢前置帧并喂通phantomCleanup", "ev-err"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withStallLimit(t, 5*time.Second)
			if tc.mode == "ack-timer-err" {
				withWCTimings(t, 40*time.Millisecond, 5*time.Millisecond)
			} else {
				withWCTimings(t, 3*time.Second, 20*time.Millisecond)
			}
			withWCCachedKey(t, wcTestAPIKey)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			frameWritten := make(chan struct{})
			var writtenOnce sync.Once
			markWritten := func() { writtenOnce.Do(func() { close(frameWritten) }) }

			rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/v1/processSession" {
					if isWCHandshakeReq(req) {
						return wcHandshakeOKResp(req, nil), nil
					}
					if req.Method == "GET" {
						pw, body := newCtxPipeBody(req.Context())
						go func() {
							defer pw.Close()
							switch tc.mode {
							case "ev-ack-err":
								// 同一批先发 Ack 再紧跟幽灵帧：主循环拿到 Ack 就去 post(1) 并立即失败，
								// 若不在读帧时累计 raw，紧跟在 events 缓冲里的幽灵帧必然被丢弃。
								_, _ = io.WriteString(pw, wcFrame(1, `[[[]]]`)+wcFrame(2, wcMinimalCIDPayload(ghostCID)))
								markWritten()
								<-req.Context().Done()
							case "ack-timer-err":
								// 不发 Ack，只先发幽灵帧，让 ackTimer 到点发 post(1) 并失败。
								_, _ = io.WriteString(pw, wcFrame(1, wcMinimalCIDPayload(ghostCID)))
								markWritten()
								<-req.Context().Done()
							case "online-err":
								// 首帧无 cid（会触发 onLine 报错），次帧带 ghostCID 紧随其后被下行 goroutine 读入。
								_, _ = io.WriteString(pw, wcFrame(1, wcTextPayload("", wcTestText, "0"))+wcFrame(2, wcMinimalCIDPayload(ghostCID)))
								markWritten()
								<-req.Context().Done()
							case "ctx-cancel":
								// 先发 Ack 再发幽灵帧；在 post(1) 之前/同时取消 ctx。
								_, _ = io.WriteString(pw, wcFrame(1, `[[[]]]`)+wcFrame(2, wcMinimalCIDPayload(ghostCID)))
								markWritten()
								time.Sleep(10 * time.Millisecond)
								cancel()
								<-req.Context().Done()
							case "ev-err":
								_, _ = io.WriteString(pw, wcFrame(1, wcMinimalCIDPayload(ghostCID))+
									wcFrame(2, `{"__sm__":{"status":[[[16,"unauth"]]]}}`))
								markWritten()
							}
						}()
						return &http.Response{StatusCode: 200, Status: "200", Header: http.Header{}, Body: body, Request: req}, nil
					}
					if req.Method == "POST" {
						b, _ := io.ReadAll(req.Body)
						if strings.Contains(string(b), "ofs=1") {
							// 等下行把幽灵帧写完再让 ofs=1 报错，消除调度抖动。
							<-frameWritten
							time.Sleep(15 * time.Millisecond)
							if tc.mode == "ev-ack-err" || tc.mode == "ack-timer-err" {
								return wcHTTPResp(req, 500, nil, "uplink failed"), nil
							}
						}
						return wcHTTPResp(req, 200, nil, ""), nil
					}
				}
				return wcHTTPResp(req, 404, nil, ""), nil
			})

			fakeProxy := "http://wcqdrop-" + tc.mode + ".invalid:3128"
			registerWCMockProxy(t, fakeProxy, rt, rtCfg().RequestTimeout)

			sentinelErr := errors.New("stop-from-online")
			var onLine func(string) error
			if tc.mode == "online-err" {
				onLine = func(string) error {
					<-frameWritten
					time.Sleep(15 * time.Millisecond)
					return sentinelErr
				}
			}

			status, raw, _, _, err := doWebChannelRequest(ctx, wcRequest{
				Inner: []interface{}{[]interface{}{"", 0}}, Prompt: "hi", HexID: hexFlash36, Mode: 1,
			}, fakeProxy, onLine, nil)

			switch tc.mode {
			case "ev-ack-err", "ack-timer-err":
				if err == nil {
					t.Fatalf("%s: 期望上行报错", tc.name)
				}
			case "online-err":
				if !errors.Is(err, sentinelErr) {
					t.Fatalf("%s: 期望 sentinelErr，实际 %v", tc.name, err)
				}
			case "ctx-cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("%s: 期望 context.Canceled，实际 %v", tc.name, err)
				}
			case "ev-err":
				if err != nil || status != 401 {
					t.Fatalf("%s: 期望 status=401/err=nil，实际 status=%d err=%v", tc.name, status, err)
				}
			}

			if got := extractConversationID(string(raw)); got != ghostCID {
				t.Fatalf("%s: 提前返回路径丢了幽灵会话 cid: got=%q want=%q raw=%q",
					tc.name, got, ghostCID, truncate(string(raw), 200))
			}

			if tc.mode == "ev-err" {
				saveRt := rtVal
				t.Cleanup(func() { rtMu.Lock(); rtVal = saveRt; rtMu.Unlock() })
				rtMu.Lock()
				rtVal.TemporaryChat, rtVal.AutoDeleteConversation, rtVal.MultiTurn = true, false, false
				rtMu.Unlock()

				deleted := make(chan string, 1)
				prevDel := deleteConversationFn
				deleteConversationFn = func(cid, _, _, _, _ string, _ int) { deleted <- cid }
				t.Cleanup(func() { deleteConversationFn = prevDel })

				pc := phantomCleanupFunc(func() (string, string, string, int) {
					return "SAPISID=x", "x", "tok", 0
				}, "")
				pc.feed(string(raw))
				pc.flush()
				select {
				case cid := <-deleted:
					if cid != ghostCID {
						t.Fatalf("phantomCleanup 删错 cid=%q want %q", cid, ghostCID)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("phantomCleanup 未触发 GzXR5e 删除")
				}
			}
		})
	}
}
