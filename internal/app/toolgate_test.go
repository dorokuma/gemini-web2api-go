package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// feed 按给定的切法把整段文本喂进闸门，返回实际发给客户端的内容。
// 切法要能任意变化：上游帧边界落在哪儿完全不由我们决定，闸门的结果不能因此不同。
func feed(t *testing.T, chunks []string) string {
	t.Helper()
	var got strings.Builder
	g := newToolFenceGate(func(s string) { got.WriteString(s) })
	for _, c := range chunks {
		g.Push(c)
	}
	if got.String() != g.Sent() {
		t.Fatalf("Sent() 和实际发出的对不上: %q vs %q", g.Sent(), got.String())
	}
	return got.String()
}

// 逐字节喂是最狠的切法：每个字符都是一个新帧，半截围栏必然出现在帧边界上。
func chars(s string) []string {
	var out []string
	for _, r := range s {
		out = append(out, string(r))
	}
	return out
}

func TestToolFenceGate(t *testing.T) {
	cases := []struct {
		name string
		full string
		want string
	}{
		{"没有围栏就原样发", "你好，今天天气不错。", "你好，今天天气不错。"},
		{
			"围栏前后的正文都要发，围栏本身扣住",
			"让我查一下。\n```tool_call\n{\"name\": \"get_weather\", \"arguments\": {}}\n```\n查完了。",
			"让我查一下。\n\n查完了。",
		},
		{
			"整段都是围栏则一个字都不发",
			"```tool_call\n{\"name\": \"f\", \"arguments\": {}}\n```",
			"",
		},
		{
			"连着两个围栏",
			"a\n```tool_call\n{\"name\":\"f\"}\n```\nb\n```tool_call\n{\"name\":\"g\"}\n```\nc",
			"a\n\nb\n\nc",
		},
		{
			// 普通代码块不是 tool_call，不能扣住
			"普通代码块照常发",
			"看这段：\n```python\nprint(1)\n```\n就这样。",
			"看这段：\n```python\nprint(1)\n```\n就这样。",
		},
		{
			// 参数的字符串里带 ```：闭合围栏是 JSON 结束之后那个，不是第一个
			"参数里带代码块",
			"写文件。\n```tool_call\n" + fencedArgsCall + "\n```\n写好了。",
			"写文件。\n\n写好了。",
		},
		{
			// JSON 写坏了：退回第一个 ``` 当闭合
			"围栏里 JSON 写坏了",
			"a\n```tool_call\n{\"name\": f}\n```\nb",
			"a\n\nb",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 三种切法结果必须一致
			if got := feed(t, []string{c.full}); got != c.want {
				t.Errorf("整段喂: 得到 %q，期望 %q", got, c.want)
			}
			if got := feed(t, chars(c.full)); got != c.want {
				t.Errorf("逐字符喂: 得到 %q，期望 %q", got, c.want)
			}
			mid := len(c.full) / 2
			for mid < len(c.full) && !isRuneStart(c.full[mid]) {
				mid++
			}
			if got := feed(t, []string{c.full[:mid], c.full[mid:]}); got != c.want {
				t.Errorf("对半喂: 得到 %q，期望 %q", got, c.want)
			}
		})
	}
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }

// 闸门发出去的内容 + parseToolCalls 的最终文本，必须能拼回同一份正文 ——
// 这是流式和非流式结果一致的判据。
func TestToolFenceGateMatchesParse(t *testing.T) {
	full := "让我查一下。\n```tool_call\n{\"name\": \"get_weather\", \"arguments\": {\"city\": \"London\"}}\n```\n查完了。"

	streamed := feed(t, chars(full))
	final, calls := parseToolCalls(full)

	if len(calls) != 1 || calls[0].Function.Name != "get_weather" {
		t.Fatalf("工具调用没解析出来: %+v", calls)
	}
	// 流式发出的可能比最终文本多首尾空白（parseToolCalls 收尾会 TrimSpace）。
	if strings.TrimSpace(streamed) != final {
		t.Errorf("流式发出 %q，最终文本 %q", strings.TrimSpace(streamed), final)
	}
}

// fencedArgsCall 是一次写文件调用，content 里有两个 markdown 代码块（实测模型就这么吐：
// ``` 原样出现在 JSON 字符串里，换行和引号正常转义）。
const fencedArgsCall = `{"name": "write_file", "arguments": {"path": "README.md", "content": "# Foo\n\n` +
	"```" + `bash\npip install foo\n` + "```" + `\n\n` + "```" + `python\nprint(\"hi\")\n` + "```" + `\n"}}`

// 参数里的 ``` 不能把调用截断：要解析出完整参数，正文里不能漏出半截 JSON，
// 流式发出的和最终文本要一致。
func TestToolCallWithFencedArgs(t *testing.T) {
	full := "写文件。\n```tool_call\n" + fencedArgsCall + "\n```\n写好了。"
	final, calls := parseToolCalls(full)
	if len(calls) != 1 || calls[0].Function.Name != "write_file" {
		t.Fatalf("工具调用没解析出来: %+v，正文 %q", calls, final)
	}
	var args map[string]string
	if err := json.Unmarshal([]byte(calls[0].Function.Arguments), &args); err != nil {
		t.Fatalf("参数不是合法 JSON: %v", err)
	}
	if want := "# Foo\n\n```bash\npip install foo\n```\n\n```python\nprint(\"hi\")\n```\n"; args["content"] != want {
		t.Errorf("content 得到 %q，期望 %q", args["content"], want)
	}
	if final != "写文件。\n\n写好了。" {
		t.Errorf("最终正文 %q", final)
	}
	if streamed := feed(t, chars(full)); strings.TrimSpace(streamed) != final {
		t.Errorf("流式发出 %q，最终文本 %q", streamed, final)
	}
}

// 未闭合的围栏：闸门扣住不发，收尾时由 remainingOf 补回去，客户端不会丢内容。
func TestToolFenceGateUnclosed(t *testing.T) {
	full := "开始了。\n```tool_call\n{\"name\": \"f\""
	streamed := feed(t, chars(full))
	if streamed != "开始了。\n" {
		t.Fatalf("扣住的部分不对: %q", streamed)
	}
	// parseToolCalls 对未闭合围栏不做处理，原样留在文本里
	final, calls := parseToolCalls(full)
	if len(calls) != 0 {
		t.Fatalf("未闭合围栏不该解析出工具调用: %+v", calls)
	}
	if rest := remainingOf(final, streamed); rest == "" {
		t.Error("收尾补发算出来是空的，扣住的内容会丢")
	}
}

func TestPartialPrefixLen(t *testing.T) {
	m := toolFenceOpen // "```tool_call"
	for _, c := range []struct {
		s    string
		want int
	}{
		{"abc", 0},
		{"abc`", 1},
		{"abc``", 2},
		{"abc```", 3},
		{"abc```tool_c", 9},
		{"", 0},
		{"中文`", 1}, // 切点必须落在 ASCII 上，不能劈开多字节字符
		{"中文", 0},
	} {
		if got := partialPrefixLen(c.s, m); got != c.want {
			t.Errorf("partialPrefixLen(%q) = %d，期望 %d", c.s, got, c.want)
		}
	}
}

// 围栏里 arguments 缺失、为 null、换了键名、平铺、二次编码时，参数都要整理成 JSON 对象，
// 绝不能是 "null"（OpenAI 客户端会拒收）；没有工具名的围栏丢弃，不发半个调用（#69）。
func TestParseToolCallsArgsShapes(t *testing.T) {
	cases := []struct {
		fence    string
		wantName string // 空 = 期望丢弃
		wantArgs string
	}{
		{`{"name":"pwsh","arguments":{"command":"Get-Date"}}`, "pwsh", `{"command":"Get-Date"}`},
		{`{"name":"pwsh"}`, "pwsh", `{}`},
		{`{"name":"pwsh","arguments":null}`, "pwsh", `{}`},
		{`{"name":"pwsh","parameters":{"command":"ls"}}`, "pwsh", `{"command":"ls"}`},
		{`{"name":"pwsh","command":"pwd"}`, "pwsh", `{"command":"pwd"}`},
		{`{"name":"pwsh","arguments":"{\"command\":\"echo hi\"}"}`, "pwsh", `{"command":"echo hi"}`},
		{`{"tool":"read_file","input":{"path":"/a"}}`, "read_file", `{"path":"/a"}`},
		{`{"type":"function","function":{"name":"f","arguments":"{\"a\":1}"}}`, "f", `{"a":1}`},
		{`{"name":"pwsh","arguments":{"command":"ls"},"input":{"x":1}}`, "pwsh", `{"command":"ls"}`},
		{`{"arguments":{"command":"x"}}`, "", ""},
		{`null`, "", ""},
	}
	for _, c := range cases {
		text, calls := parseToolCalls("前文\n```tool_call\n" + c.fence + "\n```")
		if text != "前文" {
			t.Errorf("%s: 正文 = %q", c.fence, text)
		}
		if c.wantName == "" {
			if len(calls) != 0 {
				t.Errorf("%s: 应丢弃，得到 %+v", c.fence, calls)
			}
			continue
		}
		if len(calls) != 1 {
			t.Fatalf("%s: 得到 %d 个调用", c.fence, len(calls))
		}
		f := calls[0].Function
		if f.Name != c.wantName || f.Arguments != c.wantArgs {
			t.Errorf("%s: 得到 name=%q args=%s，期望 name=%q args=%s", c.fence, f.Name, f.Arguments, c.wantName, c.wantArgs)
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(f.Arguments), &m); err != nil || m == nil {
			t.Errorf("%s: args 不是 JSON 对象：%s", c.fence, f.Arguments)
		}
	}
}

// 回复只有一个解析不了的围栏时，parseToolCalls 返回空正文、无调用（PR #72 自带用例）。
// callGemini / callGeminiConv 都要把这种结果当失败，不能回 200 + content:null。
func TestParseToolCallsOnlyBadFenceIsEmpty(t *testing.T) {
	for _, body := range []string{`{"arguments":{"command":"x"}}`, `{"name":`} {
		text, calls := parseToolCalls("```tool_call\n" + body + "\n```")
		if text != "" || len(calls) != 0 {
			t.Errorf("body %q: got text=%q calls=%d, want empty", body, text, len(calls))
		}
	}
}

// 端到端回归（上游 PR #72）：回复只有一个解析不了的 ```tool_call``` 围栏时，
// callGemini 必须报错而不是返回 text==""、err==nil（那会回客户端 200 + content:null
// 且不带 tool_calls）。
//
// 假上游按 URL 分派：/app 出 XSRF，StreamGenerate 回一帧只装坏围栏的内容。
// 出口侧不打真网络：把该 proxyURL 的 stdlibClientCache 换成走假 RoundTripper 的
// 客户端（同 argon_test.go 的做法），走代理分支（stdlib）发请求。
func TestCallGeminiRejectsEmptyReplyAfterToolFence(t *testing.T) {
	cid := "c_badfencereply1"
	// 内容帧形状同 argon_test.go：inner[1]=[cid,rid]，inner[4]=[["rc_x",[文本]]]。
	// 正文就是一个缺工具名的闭合围栏：剥掉后 text=""。行要够长——
	// textsInLine 对太短的行直接跳过，坏围栏体的 JSON 里塞字段撑长度。
	badFence := "```tool_call\n{\"arguments\":{\"command\":\"" + strings.Repeat("x", 200) + "\"}}\n```"
	inner := make([]any, 43)
	inner[1] = []any{cid, "r_1"}
	inner[4] = []any{[]any{"rc_x", []any{badFence}}}
	ib, _ := json.Marshal(inner)
	line, _ := json.Marshal([]any{[]any{"wrb.fr", "rpc1", string(ib), nil}})

	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp := func(status int, body string) *http.Response {
			return &http.Response{
				StatusCode: status, Status: fmt.Sprintf("%d", status),
				Header:  http.Header{},
				Body:    io.NopCloser(strings.NewReader(body)),
				Request: req,
			}
		}
		switch req.URL.Path {
		case "/app":
			return resp(200, `"SNlM0e":"tok-xyz:1700000000000","cfb2h":"boq_x_20260101.01_p1"`), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			return resp(200, ")]}'\n"+string(line)+"\n"), nil
		}
		return resp(404, ""), nil
	})
	const fakeProxy = "http://fake-exit.invalid:3128"
	stdlibClientCache.Store(fakeProxy+"|0", &http.Client{Transport: rt})
	stdlibClientCache.Store(fakeProxy+"|"+fmt.Sprint(rtCfg().RequestTimeout), &http.Client{Transport: rt})
	defer func() {
		stdlibClientCache.Delete(fakeProxy + "|0")
		stdlibClientCache.Delete(fakeProxy + "|" + fmt.Sprint(rtCfg().RequestTimeout))
	}()

	withPoolCookie(t)
	acct, ok := pickCookieAccount()
	if !ok {
		t.Fatal("池里没有号")
	}
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{{ID: 998, Name: "test", URL: fakeProxy, Enabled: true}}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()

	cookie := acct.Cookie
	invalidateXSRF(cookie)
	xsrfMu.Lock()
	xsrfCache[xsrfKey(cookie, acct.AuthUser)] = xsrfEntry{token: "tok", fetched: time.Now()}
	xsrfMu.Unlock()

	// rtVal 在测试进程里是零值：RetryAttempts=0 会让重试循环一次都不跑。
	saveRt := rtVal
	defer func() { rtMu.Lock(); rtVal = saveRt; rtMu.Unlock() }()
	rtMu.Lock()
	rtVal.RetryAttempts, rtVal.RetryDelaySec = 1, 0
	rtMu.Unlock()

	text, calls, _, err := callGemini(context.Background(), "run ls", promptCtx{Latest: "run ls"},
		ModelConfig{HexID: hexFlash36, Mode: 1}, toolsFixture("bash"), nil, nil, nil)
	if err == nil {
		t.Fatalf("回复只有一个解析不了的 tool_call 围栏，应报错而不是 text=%q、err==nil", text)
	}
	if len(calls) != 0 {
		t.Fatalf("不应解析出工具调用: %+v", calls)
	}
	if !strings.Contains(err.Error(), "raw ") {
		t.Fatalf("错误信息应带原始字节数便于排查: %v", err)
	}
}

// ─── 502 结构化诊断回归（TASK-502） ──────────────────────────────────────────

// fakeUpstreamServer 把整条出口链钉在假 RoundTripper 上：代理池指向假出口，
// stdlibClientCache 两个 key（限时/不限时客户端）都换成走 rt 的客户端。
// 返回要用假出口的 proxy URL。
func fakeUpstreamServer(t *testing.T, rt http.RoundTripper) string {
	t.Helper()
	const fakeProxy = "http://fake-exit.invalid:3128"
	stdlibClientCache.Store(fakeProxy+"|0", &http.Client{Transport: rt})
	stdlibClientCache.Store(fakeProxy+"|"+fmt.Sprint(rtCfg().RequestTimeout), &http.Client{Transport: rt})
	t.Cleanup(func() {
		stdlibClientCache.Delete(fakeProxy + "|0")
		stdlibClientCache.Delete(fakeProxy + "|" + fmt.Sprint(rtCfg().RequestTimeout))
	})
	proxyMu.Lock()
	old := proxyCache
	proxyCache = []Proxy{{ID: 997, Name: "diag-test", URL: fakeProxy, Enabled: true}}
	proxyMu.Unlock()
	t.Cleanup(func() { proxyMu.Lock(); proxyCache = old; proxyMu.Unlock() })
	return fakeProxy
}

// seedXSRFAndRetry 跟 toolgate 主用例同一套前提：池里一个号 + 预置 XSRF +
// 重试预算（rtVal 在测试进程是零值，RetryAttempts=0 会让上游循环一次都不跑）。
func seedXSRFAndRetry(t *testing.T, attempts int) {
	t.Helper()
	withPoolCookie(t)
	acct, ok := pickCookieAccount()
	if !ok {
		t.Fatal("池里没有号")
	}
	cookie := acct.Cookie
	invalidateXSRF(cookie)
	xsrfMu.Lock()
	xsrfCache[xsrfKey(cookie, acct.AuthUser)] = xsrfEntry{token: "tok", fetched: time.Now()}
	xsrfMu.Unlock()

	saveRt := rtVal
	t.Cleanup(func() { rtMu.Lock(); rtVal = saveRt; rtMu.Unlock() })
	rtMu.Lock()
	rtVal.RetryAttempts, rtVal.RetryDelaySec = attempts, 0
	rtMu.Unlock()
}

// respJSON 是假传输层便捷构造器。
func respJSON(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status, Status: fmt.Sprintf("%d", status),
		Header:  http.Header{},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: req,
	}
}

// fakeAppTokens 应答 /app 页面：SNlM0e + cfb2h，足够 getXSRF / currentBL 用。
const fakeAppTokens = `"SNlM0e":"tok-xyz:1700000000000","cfb2h":"boq_x_20260101.01_p1"`

// mkContentLine 拼一条 wrb.fr 内容帧：inner[1]=[cid,rid]、inner[4]=[[rc,[text…]]]、
// inner[39]=hex、inner[42]=显示名（extractUpstreamModel 靠这槽位取上游自报名）。
// extra 是非 nil 时写进 inner[31]（true=上游出错话术）。thinking 非空时写进
// inner[4][0][37][0][0]（思考链槽位，reasoningInLine 路径）。
func mkContentLine(cid, rid, text, modelName string, v31 interface{}, thinking string) string {
	inner := make([]any, 43)
	inner[1] = []any{cid, rid}
	part := []any{"rc_x", []any{text}}
	if thinking != "" {
		for len(part) < 38 {
			part = append(part, nil)
		}
		part[37] = []any{[]any{thinking}}
	}
	inner[4] = []any{part}
	inner[39] = "fbb127bbb056c959"
	inner[42] = modelName
	if v31 != nil {
		inner[31] = v31
	}
	ib, _ := json.Marshal(inner)
	line, _ := json.Marshal([]any{[]any{"wrb.fr", "rpc1", string(ib), nil}})
	return string(line)
}

// postChat 让 handleChatCompletions 跑一遍，返回记录器。
func postChat(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handleChatCompletions(rec, req)
	return rec
}

// requestWatermark 在发请求前记下 requests 表的 MAX(id)：之后轮询只认 id>水位
// 的行。不先记水位而按「同长度最新一条」取，前一轮异步写若拖到本轮才落库，
// 或 -count>1 时同 probe 多轮撞同长度，都可能把旧行当本轮结果 —— 假绿。
func requestWatermark(t *testing.T) int64 {
	t.Helper()
	var maxID int64
	if err := getDB().QueryRow(`SELECT COALESCE(MAX(id),0) FROM requests`).Scan(&maxID); err != nil {
		t.Fatalf("取 requests 水位失败: %v", err)
	}
	return maxID
}

// waitRequestRow 轮询到本条请求落库为止（recordRequest 是 go 出去的异步写）。
// 只认 id>since、endpoint=chat.completions、prompt_chars 匹配的行：三条叠一起，
// 本轮 probe 行是唯一的；水位后异步落库的老行和别的 endpoint 都进不来。
func waitRequestRow(t *testing.T, since int64, promptChars int) (status int, errStr, upstreamModel string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		row := getDB().QueryRow(`SELECT status, COALESCE(error,''), COALESCE(upstream_model,'')
			FROM requests WHERE id>? AND endpoint='chat.completions' AND prompt_chars=? ORDER BY id DESC LIMIT 1`, since, promptChars)
		if row.Scan(&status, &errStr, &upstreamModel) == nil {
			return status, errStr, upstreamModel
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("没有记录这次请求")
	return 0, "", ""
}

// 回归 1（TASK-502 #1/#2）：上游只回「空正文的内容帧 + 结束帧」——帧确实有
// inner[4] 结构（不算被上游完全拒绝）但 text 结构为空、没有 reasoning，按
// hasContentFrame 判据如实归 no_content_frame（跟 reasoning-only 的 empty_content 区分）。
// 502 body 必带 reason/frames/hint（字段存在且取值确定），requests.error 落
// [no_content_frame] 前缀；log_requests 关掉也要落库（开关只管 stderr 日志）。
func TestChatCompletions502StructuredEmptyFrame(t *testing.T) {
	fakeUpstreamServer(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/app":
			return respJSON(req, 200, fakeAppTokens), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			// 真正的「空正文内容帧」：inner[4] 有 [["rc_x", []]]（结构在、文本为空）、
			// 内层串 430+ 字节跨 200 门槛，能解析到自报名 3.6 Flash；再接结束帧。
			pad := strings.Repeat("p", 430)
			inner := fmt.Sprintf(`[null,["c_ef","r_1"],null,null,[["rc_x",[]]],"%s"%s`,
				pad, `,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"fbb127bbb056c959",null,null,"3.6 Flash",true]`)
			line, _ := json.Marshal([]any{[]any{"wrb.fr", "rpc1", inner, nil}})
			raw := ")]}'\n" + string(line) + "\n" +
				`[["wrb.fr",null,null,null,null,[13,null,[["type.googleapis.com/assistant.boq.bard.application.BardErrorInfo",[1096]]]]]]` + "\n"
			return respJSON(req, 200, raw), nil
		}
		return respJSON(req, 404, ""), nil
	}))
	seedXSRFAndRetry(t, 1)
	rtMu.Lock()
	rtVal.LogRequests = false // 核对字段实名：日志开关只管 stderr，落库不受影响
	rtMu.Unlock()

	const probe = "diag-empty-frame-probe"
	since := requestWatermark(t)
	rec := postChat(t, `{"model":"gemini-3.6-flash","messages":[{"role":"user","content":"`+probe+`"}]}`)
	if rec.Code != 502 {
		t.Fatalf("应回 502，得到 %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Error struct {
			Message       string `json:"message"`
			Reason        string `json:"reason"`
			UpstreamModel string `json:"upstream_model"`
			Frames        int    `json:"frames"`
			ContentFrame  bool   `json:"content_frame"`
			Hint          string `json:"hint"`
		} `json:"error"`
	}
	var rawBody map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &rawBody); err != nil {
		t.Fatalf("body 不是 JSON: %v body=%s", err, rec.Body.String())
	}
	var errObj map[string]json.RawMessage
	if err := json.Unmarshal(rawBody["error"], &errObj); err != nil {
		t.Fatalf("error 不是对象: %v", err)
	}
	for _, f := range []string{"reason", "upstream_model", "frames", "content_frame", "hint"} {
		if _, ok := errObj[f]; !ok {
			t.Errorf("error 缺字段 %q: %s", f, rec.Body.String())
		}
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("error 字段解不开: %v body=%s", err, rec.Body.String())
	}
	if out.Error.Reason != "no_content_frame" {
		t.Errorf("reason=%q 应为 no_content_frame（空 text/无 reasoning 的帧不算内容帧）", out.Error.Reason)
	}
	if out.Error.UpstreamModel != "3.6 Flash" {
		t.Errorf("upstream_model=%q 应取到自报 '3.6 Flash'（跟 okRaw 同 hex 同显示名槽位）", out.Error.UpstreamModel)
	}
	if out.Error.Frames != 2 {
		t.Errorf("frames=%d 应为确切 2（一个 wrb.fr 内容帧 + 一个 wrb.fr 结束帧）", out.Error.Frames)
	}
	if out.Error.ContentFrame {
		t.Error("text 结构为空且无 reasoning 的帧不算内容帧")
	}
	if out.Error.Hint == "" {
		t.Error("hint 应是一句中文提示")
	}
	if !strings.Contains(out.Error.Message, "upstream error: ") {
		t.Errorf("message 丢了原前缀: %q", out.Error.Message)
	}

	status, errStr, upModel := waitRequestRow(t, since, len(probe))
	if status != 502 {
		t.Fatalf("requests.status=%d 应为 502（log_requests=false 也要落库）", status)
	}
	if !strings.HasPrefix(errStr, "[no_content_frame]") {
		t.Fatalf("requests.error 应带 [no_content_frame] 前缀，得到 %q", errStr)
	}
	if upModel != "3.6 Flash" {
		t.Errorf("requests.upstream_model=%q 应记自报 '3.6 Flash'", upModel)
	}
}

// 回归 2（TASK-502 #1/#2）：上游回「出错话术」（inner[31]=true）→ reason=failure_reply，
// body 带自报模型名，requests.error 落 [failure_reply] 前缀。
// 旧实现只回 "upstream error: upstream returned a failure reply…" 无任何字段 —— 全挂。
// 话术正文刻意撑过 4096 字节：诊断必须对完整 raw 即时解析，帧统计不能因截断虚报。
func TestChatCompletions502FailureReply(t *testing.T) {
	fakeUpstreamServer(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/app":
			return respJSON(req, 200, fakeAppTokens), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			line := mkContentLine("c_fail", "r_1",
				"I seem to be encountering an error. Can I try something else for you? "+
					strings.Repeat("padding ", 600), // ~4.7KB，盖过旧 4KB 截断位
				"4 Argon", true, "")
			return respJSON(req, 200, ")]}'\n"+line+"\n"), nil
		}
		return respJSON(req, 404, ""), nil
	}))
	seedXSRFAndRetry(t, 1)

	const probe = "diag-failure-reply-probe"
	since := requestWatermark(t)
	rec := postChat(t, `{"model":"gemini-3.6-flash","messages":[{"role":"user","content":"`+probe+`"}]}`)
	if rec.Code != 502 {
		t.Fatalf("应回 502，得到 %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Error struct {
			Reason        string `json:"reason"`
			UpstreamModel string `json:"upstream_model"`
			Frames        int    `json:"frames"`
			Hint          string `json:"hint"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body 不是 JSON: %v", err)
	}
	if out.Error.Reason != "failure_reply" {
		t.Errorf("reason=%q 应为 failure_reply", out.Error.Reason)
	}
	if out.Error.UpstreamModel != "4 Argon" {
		t.Errorf("upstream_model=%q 应取到自报 '4 Argon'", out.Error.UpstreamModel)
	}
	if out.Error.Frames != 1 {
		t.Errorf("frames=%d 应为确切 1（>4KB 话术帧也只是一个 wrb.fr 帧，截断不能虚报）", out.Error.Frames)
	}
	if out.Error.Hint == "" {
		t.Error("hint 应是一句中文提示")
	}

	status, errStr, upModel := waitRequestRow(t, since, len(probe))
	if status != 502 {
		t.Fatalf("requests.status=%d 应为 502", status)
	}
	if !strings.HasPrefix(errStr, "[failure_reply]") {
		t.Fatalf("requests.error 应带 [failure_reply] 前缀，得到 %q", errStr)
	}
	if upModel != "4 Argon" {
		t.Errorf("requests.upstream_model=%q 应记 '4 Argon'", upModel)
	}
}

// 回归 3（TASK-502 #3）：有内容帧但正文抽取为空（reasoning-only）→
// reason=empty_content，不是 no_content_frame；也绝不回 200+空正文。
// 决策：思考链不当正文返回 —— reasoning_content 是辅助字段，拿它顶替 content
// 等于回一个「成功但没答案」的假 200，跟空正文一样误导。报 empty_content 如实。
func TestChatCompletions502ReasoningOnlyIsEmptyContent(t *testing.T) {
	fakeUpstreamServer(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/app":
			return respJSON(req, 200, fakeAppTokens), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			// 帧里有思考链、没有正文文本：hasContentFrame=true（reasoningInLine 命中），
			// extractResponseText="" → 归 empty_content。
			line := mkContentLine("c_think", "r_1", "", "3.1 Pro", nil,
				"thinking about the problem thinking about the problem thinking about the problem")
			return respJSON(req, 200, ")]}'\n"+line+"\n"), nil
		}
		return respJSON(req, 404, ""), nil
	}))
	seedXSRFAndRetry(t, 1)

	const probe = "diag-reasoning-only-probe"
	rec := postChat(t, `{"model":"gemini-3.6-flash","messages":[{"role":"user","content":"`+probe+`"}]}`)
	if rec.Code != 502 {
		t.Fatalf("reasoning-only 应回 502，得到 %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Error struct {
			Reason       string `json:"reason"`
			ContentFrame bool   `json:"content_frame"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body 不是 JSON: %v", err)
	}
	if out.Error.Reason != "empty_content" {
		t.Errorf("reason=%q 应为 empty_content（有思考帧但正文为空）", out.Error.Reason)
	}
	if !out.Error.ContentFrame {
		t.Error("思考链帧算内容帧，content_frame 应为 true")
	}
}

// 回归 4（TASK-502 多轮）：multiTurn 续接路径上游 200 但无内容帧时，502 仍归
// no_content_frame。conversation.go 里这个分支报的是中文「续接无内容帧（会话可能
// 已失效，raw N 字节）」，错误串里没有 "no content frame"，upstreamReason 按串
// 匹配会漏归成兜底 upstream_error —— 必须由 diagErr 显式带 reasonNoContentFrame。
// 会话按历史指纹续接：首轮成功落 convStore，第二轮 parentKey 命中即续接；首轮的
// conv.cid 由 parseConvIDs 从成功帧回填。续接路径上游只回结束帧 → 中文报错。
func TestChatCompletionsMultiTurnNoContentFrame(t *testing.T) {
	fakeUpstreamServer(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/app":
			return respJSON(req, 200, fakeAppTokens), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			var body []byte
			if req.Body != nil {
				body, _ = io.ReadAll(req.Body)
				_ = req.Body.Close()
			}
			if !strings.Contains(string(body), `c_mt1`) {
				// 首轮成功：带 [cid,rid] + 自报名 + 正文帧。
				line := mkContentLine("c_mt1", "r_1",
					"first reply padded to clear the 200-byte textsInLine minimum line length gate, enough padding here.",
					"3.6 Flash", nil, "")
				return respJSON(req, 200, ")]}'\n"+line+"\n"), nil
			}
			// 续接轮（body 里带 cid=c_mt1）：上游只回结束帧（无 inner[4] 文本），
			// 触发中文「续接无内容帧」。
			raw := ")]}'\n" +
				`[["wrb.fr",null,null,null,null,[13,null,[["type.googleapis.com/assistant.boq.bard.application.BardErrorInfo",[1096]]]]]]` + "\n"
			return respJSON(req, 200, raw), nil
		}
		return respJSON(req, 404, ""), nil
	}))
	seedXSRFAndRetry(t, 1)
	rtMu.Lock()
	saveMT := rtVal.MultiTurn
	rtVal.MultiTurn = true
	rtMu.Unlock()
	t.Cleanup(func() { rtMu.Lock(); rtVal.MultiTurn = saveMT; rtMu.Unlock() })

	const probe = "diag-mt-noframe-probe"
	t.Cleanup(func() {
		// 清理 convStore 里本测试建的两路指纹，不留给下一个用例。
		convDelete(convChildKey([]map[string]interface{}{
			{"role": "user", "content": probe},
		}, "first reply padded to clear the 200-byte textsInLine minimum line length gate, enough padding here."))
	})
	since := requestWatermark(t)
	// 首轮建会话（parentKey 为空 → fresh，服务端历史落 convStore）。
	rec := postChat(t, `{"model":"gemini-3.6-flash","messages":[{"role":"user","content":"`+probe+`"}]}`)
	if rec.Code != 200 {
		t.Fatalf("首轮应回 200（会话建档），得到 %d body=%s", rec.Code, rec.Body.String())
	}
	// 第二轮 parentKey 命中（首轮落库的是 [user(probe), assistant(首轮回复)] 的指纹）→
	// 续接。assistant content 必须是首轮返回的正文原文，否则指纹对不上。
	// 末条用 probe 自身同长（多了会被水位条件挡住时不乱撞别的行）。
	rec = postChat(t, `{"model":"gemini-3.6-flash","messages":[`+
		`{"role":"user","content":"`+probe+`"},`+
		`{"role":"assistant","content":"first reply padded to clear the 200-byte textsInLine minimum line length gate, enough padding here."},`+
		`{"role":"user","content":"`+probe+`"}]}`)
	if rec.Code != 502 {
		t.Fatalf("续接无内容帧应回 502，得到 %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Error struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body 不是 JSON: %v", err)
	}
	if out.Error.Reason != "no_content_frame" {
		t.Errorf("reason=%q 应为 no_content_frame（中文报错串也得归对类，不能漏成 upstream_error）", out.Error.Reason)
	}
	if !strings.Contains(out.Error.Message, "续接无内容帧") {
		t.Errorf("message 应保留原中文现场: %q", out.Error.Message)
	}

	// 续接轮的落库行：recordUpstreamFailure 记的是 handler 里 messagesToPrompt 拼出的
	// 整段 prompt（probe + [Assistant]: 首轮正文 + probe），不是 formatNewTurn 的「more」。
	// 直接按 id>水位 + endpoint + 本条 prompt 长度取：首轮 200 行同长但 status 不同，
	// 续接轮只有一条 502 行。
	fullPromptLen := len(probe) + len("first reply padded to clear the 200-byte textsInLine minimum line length gate, enough padding here.") + len("[Assistant]: ") + len(probe) + len("\n\n")*2
	status, errStr, _ := waitRequestRow(t, since, fullPromptLen)
	if status != 502 {
		t.Fatalf("续接轮 requests.status=%d 应为 502", status)
	}
	if !strings.HasPrefix(errStr, "[no_content_frame]") {
		t.Fatalf("requests.error 应带 [no_content_frame] 前缀，得到 %q", errStr)
	}
}

// 回归 5（TASK-502 #3）：合法路径不被新守卫误伤 —— 代码产物 / 纯 tool_call / canvas。
// 旧守卫下这些早已 200；新守卫把「有帧但空正文」单独归 empty_content 后，它们仍须 200。
func TestChatCompletionsLegitPathsNotFalsePositive(t *testing.T) {
	cases := []struct {
		name     string
		tools    string // 请求里 tools 字段；空 = 不传
		inner4   func() interface{}
		wantCode int
		check    func(t *testing.T, body []byte)
	}{
		{
			name: "纯代码产物（codeMarkerRe 回退后非空）",
			inner4: func() interface{} {
				code := "```python?code_reference&code_event_index=1\nprint(1+1)  # deliberately padded to pass the 200-byte minimum line length gate for textsInLine\n```"
				return []any{[]any{"rc_x", []any{code}}}
			},
			wantCode: 200,
			check: func(t *testing.T, body []byte) {
				if !strings.Contains(string(body), "```python") {
					t.Errorf("代码产物应降级成普通代码块回 200: %s", body)
				}
			},
		},
		{
			name:  "纯 tool_call（合法围栏解析出调用）",
			tools: `,"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}]`,
			inner4: func() interface{} {
				fenced := "```tool_call\n{\"name\": \"get_weather\", \"arguments\": {\"city\": \"London\"}}\n```"
				fenced += strings.Repeat(" ", 220) // 凑够 200 字节 textsInLine 门槛
				return []any{[]any{"rc_x", []any{fenced}}}
			},
			wantCode: 200,
			check: func(t *testing.T, body []byte) {
				if !strings.Contains(string(body), "tool_calls") {
					t.Errorf("合法 tool_call 应带出 tool_calls 字段: %s", body)
				}
			},
		},
		{
			name: "canvas 文档（immersive 里抠出 DOCTYPE）",
			inner4: func() interface{} {
				// extractCanvasDoc 在整个 inner 树里找含 DOCTYPE 的最长字符串。
				doc := "```html\n<!DOCTYPE html><html><body>canvas doc padding padding padding padding padding padding padding</body></html>\n```"
				inner := []interface{}{}
				inner = append(inner, []interface{}{"rc_x", []interface{}{"Generating your document…"}})
				inner = append(inner, doc) // immersive 结构直接塞 inner[4] 子位
				return inner
			},
			wantCode: 200,
			check: func(t *testing.T, body []byte) {
				if !strings.Contains(string(body), "DOCTYPE") {
					t.Errorf("canvas 应返回 HTML 文档: %s", body)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fakeUpstreamServer(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/app":
					return respJSON(req, 200, fakeAppTokens), nil
				case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
					inner := make([]any, 43)
					inner[1] = []any{"c_legit", "r_1"}
					inner[4] = c.inner4()
					inner[42] = "3.6 Flash"
					ib, _ := json.Marshal(inner)
					l, _ := json.Marshal([]any{[]any{"wrb.fr", "rpc1", string(ib), nil}})
					return respJSON(req, 200, ")]}'\n"+string(l)+"\n"), nil
				}
				return respJSON(req, 404, ""), nil
			}))
			seedXSRFAndRetry(t, 1)

			model := "gemini-3.6-flash"
			if c.name == "canvas 文档（immersive 里抠出 DOCTYPE）" {
				model = "gemini-canvas"
			}
			body := `{"model":"` + model + `","messages":[{"role":"user","content":"diag-legit"}]` + c.tools + `}`
			rec := postChat(t, body)
			if rec.Code != c.wantCode {
				t.Fatalf("应回 %d，得到 %d body=%s", c.wantCode, rec.Code, rec.Body.String())
			}
			if c.check != nil {
				c.check(t, rec.Body.Bytes())
			}
		})
	}
}

// 媒体路径不误伤：gemini-image 有产物就走 appendArtifactMarkdown 返回，
// 不进「正文为空」守卫（产物 markdown 本身就是正文）。旧实现已这样，回归防退化。
// fixture 刻意用 image 占位正文（清洗后为空）验证「正文为空的合法媒体边界」。
func TestChatCompletionsMediaArtifactNotFalsePositive(t *testing.T) {
	var rawCapture struct{ body string } // 记录假上游回出的 raw，给下方边界断言用
	fakeUpstreamServer(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/app":
			return respJSON(req, 200, fakeAppTokens), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			inner := make([]any, 43)
			inner[1] = []any{"c_img_" + strings.Repeat("x", 200), "r_1"}
			// 图片 CDN 链单独一个字符串（collectImageURLs 抠整个串，
			// 跟别文本混在一个串里会把整串当 URL 解析）。
			// 正文只放 image 占位串：textsInLine 认得出它（content_frame=true），
			// stripImagePlaceholders 整段剥掉 → extractResponseText=""，覆盖
			// 「空正文媒体合法」边界。textsInLine 的 200 字节行门槛由长 cid 垫过。
			inner[4] = []any{
				[]any{"rc_x", []any{"http://googleusercontent.com/image_generation_content/0_480"}},
				"https://lh3.googleusercontent.com/gg-dl/imgtoken",
			}
			inner[42] = "Nano Banana 2"
			ib, _ := json.Marshal(inner)
			l, _ := json.Marshal([]any{[]any{"wrb.fr", "rpc1", string(ib), nil}})
			rawCapture.body = ")]}'\n" + string(l) + "\n"
			return respJSON(req, 200, rawCapture.body), nil
		case "/gg-dl/imgtoken=s0":
			r := respJSON(req, 200, "FAKEPNGBYTES")
			r.Header.Set("Content-Type", "image/png")
			return r, nil
		}
		return respJSON(req, 404, ""), nil
	}))
	seedXSRFAndRetry(t, 1)

	rec := postChat(t, `{"model":"gemini-image","messages":[{"role":"user","content":"diag-img"}]}`)
	if rec.Code != 200 {
		t.Fatalf("媒体模型有产物应回 200，得到 %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "data:image/png;base64,") {
		t.Errorf("正文应带 base64 data URL 产物: %s", rec.Body.String())
	}

	// 空正文媒体边界：同一份 fixture 的正文被 image 占位链占满（清洗剥空），
	// 显式钉 extractResponseText=='' 且 hasContentFrame==true —— 证明这次走的是
	// 「正文为空的合法媒体」分支而不是侥幸有正文文本。
	if rawCapture.body == "" {
		t.Fatal("假上游没被调到")
	}
	var line string
	for _, l := range strings.Split(rawCapture.body, "\n") {
		if strings.Contains(l, `"wrb.fr"`) && strings.Contains(l, "image_generation_content") {
			line = l
		}
	}
	if line == "" {
		t.Fatal("假上游应返回含 image 占位链的 wrb.fr 帧")
	}
	raw := ")]}'\n" + line + "\n"
	if got := extractResponseText(raw); got != "" {
		t.Fatalf("占位链正文应被清洗剥空，extractResponseText 得到 %q", got)
	}
	if !hasContentFrame(raw) {
		t.Fatal("占位链是真实内容帧，hasContentFrame 应为 true")
	}
}

// 末尾锚点必须明说「单轮一块只是格式约定、全程没有次数上限」，否则多轮重放
// 历史工具调用时 neon/hydrogen 会泛化出「工具调用次数有上限/工具被停用」拒调。
// 保留的约束：每轮仍只能出一个 tool_call 块、不许拿 bash/python 代码块冒充执行。
func TestToolPromptTailDeclaresNoCallLimit(t *testing.T) {
	msgs := []map[string]interface{}{
		{"role": "user", "content": "list files then read README"},
	}
	_, pc := messagesToPrompt(msgs, toolsFixture("bash"), nil)
	tail := pc.Tail
	if tail == "" {
		t.Fatal("带工具时末尾锚点不能为空")
	}
	for _, want := range []string{
		// 无上限的显式声明
		"NO limit on the total number of tool calls",
		"Never invent or claim a limit on tool calls",
		"tools are disabled",
		// 每轮一块只是格式约定
		"per-reply format convention",
		"ONE ```tool_call```",
		// 别重复已成功步骤，但不许泛化成停调所有工具
		"Do not repeat a finished step",
		"do not let it cut short other steps that are still unfinished",
	} {
		if !strings.Contains(tail, want) {
			t.Errorf("末尾锚点缺少 %q\n---\n%s", want, tail)
		}
	}
	if strings.Contains(tail, "Know when to STOP") || strings.Contains(tail, "stop calling tools") {
		t.Error("末尾锚点不应再出现容易泛化成次数上限的停手措辞")
	}
}
