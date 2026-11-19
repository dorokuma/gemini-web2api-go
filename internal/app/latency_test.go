package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// SIDCC 每个响应都刷新，XSRF 缓存不能因此失效，否则每个登录态请求都多抓一次 /app。
func TestXSRFCacheSurvivesSIDCCRefresh(t *testing.T) {
	cookie := "SAPISID=sap; __Secure-1PSID=psid; SIDCC=old; __Secure-1PSIDCC=old"
	refreshed := mergeSetCookie(cookie, []string{"SIDCC=new; Path=/", "__Secure-1PSIDCC=new; Path=/"})
	if refreshed == cookie {
		t.Fatal("测试前提不成立：合并后 cookie 没变")
	}
	if cookieKey(cookie) != cookieKey(refreshed) {
		t.Fatal("同一账号刷新 SIDCC 后缓存键变了")
	}
	if cookieKey(cookie) == cookieKey("SAPISID=other; __Secure-1PSID=psid2") {
		t.Fatal("不同账号撞了缓存键")
	}
	if cookieKey("a=1") == cookieKey("a=2") {
		t.Fatal("没有身份项时应退回按整串区分")
	}

	key := xsrfKey(cookie, 0)
	xsrfMu.Lock()
	xsrfCache[key] = xsrfEntry{token: "tok", fetched: time.Now()}
	xsrfMu.Unlock()
	t.Cleanup(func() { invalidateXSRF(cookie) })

	// 命中缓存时不发网络请求；这里 proxyURL 指向不可达地址，真去抓会报错。
	tok, err := getXSRF(refreshed, 0, "http://127.0.0.1:1")
	if err != nil || tok != "tok" {
		t.Fatalf("刷新后的 cookie 没命中缓存: tok=%q err=%v", tok, err)
	}
}

func toolsFixture(names ...string) []map[string]interface{} {
	var out []map[string]interface{}
	for _, n := range names {
		out = append(out, map[string]interface{}{
			"type": "function",
			"function": map[string]interface{}{
				"name":        n,
				"description": "Run <cmd> & report",
				"parameters": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"command": map[string]interface{}{"type": "string"},
					},
					"required": []interface{}{"command"},
				},
			},
		})
	}
	return out
}

func TestToolSchemaIsCompact(t *testing.T) {
	msgs := []map[string]interface{}{{"role": "user", "content": "hi"}}
	p, pc := messagesToPrompt(msgs, toolsFixture("bash"), nil, false)
	if strings.Contains(pc.Head, "\n  \"") {
		t.Error("工具定义仍是缩进格式")
	}
	if strings.Contains(pc.Head, `\u00`) || !strings.Contains(pc.Head, "Run <cmd> & report") {
		t.Error("工具描述里的 HTML 字符被转义了")
	}
	if pc.Tail == "" || !strings.HasPrefix(p, pc.Head) || !strings.HasSuffix(p, pc.Tail) {
		t.Fatal("prompt 首尾应分别是工具说明和末尾锚点")
	}
	if p != buildPrompt(msgs, toolsFixture("bash"), nil, false) {
		t.Fatal("messagesToPrompt 和 buildPrompt 结果不一致")
	}
}

// 超长转附件时，工具说明和末尾锚点留在正文，附件里只有历史。
func TestContextFileKeepsToolProtocolInline(t *testing.T) {
	msgs := longConversation(300)
	prompt, pc := messagesToPrompt(msgs, toolsFixture("bash", "read"), nil, false)
	budget := len(prompt) / 2

	history, inline := planContextFile(prompt, pc, budget)
	if strings.Contains(history, pc.Head) || strings.Contains(history, pc.Tail) {
		t.Fatal("工具协议跟着历史进了附件")
	}
	if !strings.HasPrefix(inline, pc.Head) || !strings.HasSuffix(inline, pc.Tail) {
		t.Fatal("正文里没保留工具协议")
	}
	if !strings.Contains(inline, contextFileName) || !strings.Contains(inline, "读取一下 task.md") {
		t.Fatal("正文缺少附件指引或最新提问")
	}
	if !strings.Contains(history, "历史填充内容") || len(inline) > budget {
		t.Fatalf("附件缺历史或正文超预算（%d > %d）", len(inline), budget)
	}

	// 工具说明本身就超预算：退回整段进附件，至少不撞墙。
	history, inline = planContextFile(prompt, pc, len(pc.Head)/2)
	if history != prompt || strings.Contains(inline, pc.Head) {
		t.Fatal("工具说明超预算时应整段进附件")
	}

	// 没有工具：跟原来一样整段进附件。
	plain, ppc := messagesToPrompt(msgs, nil, nil, false)
	if history, _ := planContextFile(plain, ppc, len(plain)/2); history != plain {
		t.Fatal("没有工具时附件内容变了")
	}
}

func TestToolCallComplete(t *testing.T) {
	done := toolCallComplete(toolsFixture("bash"))
	cases := []struct {
		text string
		want bool
	}{
		{"先说两句", false},
		{"```tool_call\n{\"name\": \"bash\", \"arguments\": {\"command\": \"ls\"}}", false},     // 未闭合
		{"```tool_call\n{\"name\": \"bash\", \"arguments\": {\"command\": \"ls\"}}\n``", false}, // 闭合到一半
		{"```tool_call\n{\"name\": \"bash\", \"arguments\": {\"command\": \"ls\"}}\n```", true},
		{"好的\n```tool_call {\"name\":\"bash\",\"arguments\":{}}```", true},
		{"```tool_call\n{\"name\": \"rm_rf\", \"arguments\": {}}\n```", false},                        // 不认识的函数
		{"```tool_call\n{\"name\": \"bash\", \"arguments\": {\"command\": \"echo ```\"}}\n```", true}, // 参数里有围栏，外层已闭合
		{"```tool_call\n{\"name\": \"bash\", \"arguments\": {\"command\": \"echo ```\"}}", false},     // 参数里有围栏，外层未闭合
	}
	for _, c := range cases {
		if got := done(c.text); got != c.want {
			t.Errorf("toolCallComplete(%q) = %v, want %v", c.text, got, c.want)
		}
	}
	if toolCallComplete(nil) != nil {
		t.Error("没有工具时不应做判定")
	}
}

// countingReader 按块吐数据并记录被读了几次。
type countingReader struct {
	chunks [][]byte
	reads  int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.reads >= len(r.chunks) {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[r.reads])
	r.reads++
	return n, nil
}

func TestStopReaderEndsReadEarly(t *testing.T) {
	src := &countingReader{chunks: [][]byte{[]byte("a\n"), []byte("b\n"), []byte("c\n"), []byte("d\n")}}
	var stop atomic.Bool
	var got []string
	_, _, err := readBody(stopReader{src, &stop}, func(line string) error {
		got = append(got, line)
		if line == "b" {
			stop.Store(true)
		}
		return nil
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "a,b" || src.reads != 2 {
		t.Fatalf("置位后仍在读上游: lines=%v reads=%d", got, src.reads)
	}
}

func argonLine(hexID, name, answer string) []byte {
	return makeSyntheticArgonFrame(hexID, name, answer)
}

func TestArgonIncrementalForwarding(t *testing.T) {
	header := fmt.Sprintf(`[1,null,null,null,"%s"]`, hexArgon4)
	noID := argonLine("", "", "x")

	// 身份晚到：之前的帧先扣住，确认后按原顺序放行。
	var got []string
	body := bytes.Join([][]byte{[]byte(")]}'\n"), noID, argonLine(hexArgon4, "4 Argon", "ok")}, nil)
	_, _, err := readModelCheckedBody(bytes.NewReader(body), func(l string) error {
		got = append(got, l)
		return nil
	}, time.Now(), 200, header)
	if err != nil || len(got) != 3 || got[0] != ")]}'" {
		t.Fatalf("晚到身份: err=%v lines=%d", err, len(got))
	}

	// 确认之后增量转发：每读一块就要回调，不能攒到结尾。
	calls := 0
	inc := &incrementalReader{
		chunks:          [][]byte{argonLine(hexArgon4, "4 Argon", "a"), argonLine(hexArgon4, "4 Argon", "ab"), noID},
		callbacksCalled: &calls,
	}
	if _, _, err := readModelCheckedBody(inc, func(string) error {
		calls++
		return nil
	}, time.Now(), 200, header); err != nil || calls != 3 {
		t.Fatalf("确认后未增量转发: err=%v calls=%d", err, calls)
	}

	// 先对后错：已放行的收不回，但必须报错并停读。
	got = nil
	src := &countingReader{chunks: [][]byte{
		argonLine(hexArgon4, "4 Argon", "a"),
		argonLine(hexFlash36, "3.6 Flash", "b"),
		argonLine(hexArgon4, "4 Argon", "c"),
	}}
	_, _, err = readModelCheckedBody(src, func(l string) error {
		got = append(got, l)
		return nil
	}, time.Now(), 200, header)
	if err == nil || !strings.Contains(err.Error(), "mismatch") || len(got) != 1 || src.reads != 2 {
		t.Fatalf("先对后错: err=%v lines=%d reads=%d", err, len(got), src.reads)
	}

	// 始终没有身份：一行都不放行。
	got = nil
	_, _, err = readModelCheckedBody(bytes.NewReader(bytes.Repeat(noID, 3)), func(l string) error {
		got = append(got, l)
		return nil
	}, time.Now(), 200, header)
	if err == nil || !strings.Contains(err.Error(), "identity missing") || len(got) != 0 {
		t.Fatalf("无身份: err=%v lines=%d", err, len(got))
	}

	// 非流式（onLine 为 nil）仍整段校验。
	if _, _, err := readModelCheckedBody(bytes.NewReader(noID), nil, time.Now(), 200, header); err == nil {
		t.Fatal("非流式无身份应报错")
	}
}

// 后台重取失败时必须作废缓存，否则失效的号会一直拿着旧 token 被当成可用。
func TestRefreshAppTokensFailureDropsCache(t *testing.T) {
	cookie := "SAPISID=sap2; __Secure-1PSID=psid2"
	key := xsrfKey(cookie, 0)
	xsrfMu.Lock()
	xsrfCache[key] = xsrfEntry{token: "stale", fetched: time.Now()}
	xsrfMu.Unlock()
	t.Cleanup(func() { invalidateXSRF(cookie) })

	refreshAppTokens(key, cookie, 0, "http://127.0.0.1:1")
	xsrfMu.Lock()
	_, ok := xsrfCache[key]
	xsrfMu.Unlock()
	if ok {
		t.Fatal("重取失败后缓存仍在")
	}
}

// genFrame 造一行 StreamGenerate 帧：inner[4] 带累积正文，argon=true 时带上模型身份槽。
func genFrame(text string, argon bool) []byte {
	inner := make([]any, 43)
	inner[0] = "padding-to-pass-the-minimum-frame-length-checks-in-textsInLine"
	inner[4] = []any{[]any{"rc_test", []any{text}}}
	if argon {
		inner[39], inner[40], inner[41], inner[42] = hexArgon4, nil, nil, "4 Argon"
	}
	ib, _ := json.Marshal(inner)
	line, _ := json.Marshal([]any{[]any{"wrb.fr", nil, string(ib)}})
	return append(line, '\n')
}

// 端到端：工具块闭合那一帧之后不再读上游，客户端只收到围栏外的正文，最终能解析出调用。
func TestToolCallStopsUpstreamRead(t *testing.T) {
	call := "```tool_call\n{\"name\": \"bash\", \"arguments\": {\"command\": \"ls\"}}\n```"
	texts := []string{"好的", "好的\n```tool_call\n{\"name\": \"ba", "好的\n" + call, "好的\n" + call, "好的\n" + call}
	for _, argon := range []bool{false, true} {
		hexID := hexFlash36
		if argon {
			hexID = hexArgon4
		}
		var frames [][]byte
		for _, tx := range texts {
			frames = append(frames, genFrame(tx, argon))
		}
		src := &countingReader{chunks: frames}
		var sent strings.Builder
		gate := newToolFenceGate(func(s string) { sent.WriteString(s) })
		lh := newLineHandler(gate.Push, nil, toolCallComplete(toolsFixture("bash")), nil)

		raw, _, err := readModelCheckedBody(stopReader{src, &lh.stop}, lh.cb, time.Now(), 200,
			fmt.Sprintf(`[1,null,null,null,"%s"]`, hexID))
		if err != nil {
			t.Fatalf("argon=%v: %v", argon, err)
		}
		if !lh.stop.Load() || src.reads != 3 {
			t.Fatalf("argon=%v: 工具块闭合后仍在读上游（reads=%d，应为 3）", argon, src.reads)
		}
		if sent.String() != "好的\n" {
			t.Fatalf("argon=%v: 围栏原文漏给了客户端: %q", argon, sent.String())
		}
		_, calls := parseToolCalls(extractResponseText(string(raw)))
		if len(calls) != 1 || calls[0].Function.Name != "bash" || calls[0].Function.Arguments != `{"command":"ls"}` {
			t.Fatalf("argon=%v: 提前收尾后解析不出调用: %+v", argon, calls)
		}
	}

	// 非流式（onDelta 为 nil）同样提前收尾，但不能把 tracker 记成「已发给客户端」，否则失败时不能重试。
	src := &countingReader{chunks: [][]byte{genFrame("好的\n"+call, false), genFrame("好的\n"+call, false)}}
	lh := newLineHandler(nil, nil, toolCallComplete(toolsFixture("bash")), nil)
	if _, _, err := readModelCheckedBody(stopReader{src, &lh.stop}, lh.cb, time.Now(), 200, `[1,null,null,null,"x"]`); err != nil {
		t.Fatal(err)
	}
	if !lh.stop.Load() || src.reads != 1 || lh.tracker.emitted != "" {
		t.Fatalf("非流式: stop=%v reads=%d tracker=%q", lh.stop.Load(), src.reads, lh.tracker.emitted)
	}
}

// web_channel 被忽略的提示：开关每打开一次只提示一次，不能每个请求都刷一行日志。
func TestWebChannelIgnoredLoggedOncePerEnable(t *testing.T) {
	webChannelIgnoredLogged.Store(false)
	t.Cleanup(func() { webChannelIgnoredLogged.Store(false) })

	got := []bool{
		noteWebChannelIgnored(true), // 第一次看到开着：提示
		noteWebChannelIgnored(true), // 后续请求：不再提示
		noteWebChannelIgnored(true),
		noteWebChannelIgnored(false), // 关掉：清零
		noteWebChannelIgnored(true),  // 再打开：再提示一次
		noteWebChannelIgnored(true),
	}
	want := []bool{true, false, false, false, true, false}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 次: got %v, want %v（全部 %v）", i, got[i], want[i], got)
		}
	}
}
