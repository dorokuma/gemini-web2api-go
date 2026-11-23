package app

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// compact 模式（context_overflow_mode="compact"）的结构化裁剪回归：
// - 原子组（assistant+tool_calls 与其 tool 结果）整组移除，绝不拆开；
// - 超长 tool 输出先头尾截断（UTF-8 安全）；
// - 必保集合（system/首条 user/最后一组）仍超 → fail-closed *PromptTooLongError。

func withContextCfg(t *testing.T, maxFile int, mode string) {
	t.Helper()
	withContextCfgBudget(t, maxFile, mode, 0)
}

func withContextCfgBudget(t *testing.T, maxFile int, mode string, maxPrompt int) {
	t.Helper()
	old := rtCfg()
	next := old
	next.MaxContextFileBytes = maxFile
	next.ContextOverflowMode = mode
	if maxPrompt > 0 {
		next.MaxPromptBytes = maxPrompt
	}
	rtMu.Lock()
	rtVal = next
	rtMu.Unlock()
	t.Cleanup(func() {
		rtMu.Lock()
		rtVal = old
		rtMu.Unlock()
	})
}

func toolCallMsg(name, args string) map[string]interface{} {
	return map[string]interface{}{
		"role":    "assistant",
		"content": "calling tool",
		"tool_calls": []interface{}{map[string]interface{}{
			"function": map[string]interface{}{"name": name, "arguments": args},
		}},
	}
}

// 原子组完整性：带 tool_calls 的 assistant 与它后面的 tool 结果必须同生共死。
func TestBuildAtomicGroupsKeepsToolPairs(t *testing.T) {
	msgs := []map[string]interface{}{
		{"role": "system", "content": "sys"},
		{"role": "user", "content": "task"},
		toolCallMsg("run", `{"c":"ls"}`),
		{"role": "tool", "name": "run", "content": "out1"},
		{"role": "tool", "name": "run", "content": "out2"},
		{"role": "assistant", "content": "done"},
	}
	gs := buildAtomicGroups(msgs)
	if len(gs) != 4 {
		t.Fatalf("groups=%d want 4", len(gs))
	}
	if len(gs[2].msgs) != 3 {
		t.Fatalf("assistant+2 tool 应并成一组，实际 %d 条", len(gs[2].msgs))
	}
}

// 超长 tool 输出按 rune 安全边界截头尾，绝不切在多字节字符中间。
func TestTruncateToolContentUTF8Safe(t *testing.T) {
	// 中文字符 3 字节/个，构造头部恰好落在多字节边界上的内容。
	head := strings.Repeat("汉", 1366) // 4098 字节，头 4096 会落到 rune 中间
	body := head + strings.Repeat("x", 10000) + strings.Repeat("尾", 1366)
	out, ok := truncateToolContent(body, compactToolHead, compactToolTail)
	if !ok {
		t.Fatal("应触发截断")
	}
	if !strings.Contains(out, "UTF-8 bytes omitted") {
		t.Error("缺省略提示")
	}
	if !strings.Contains(out, "尾") {
		// 尾巴应保留（原断言 `Contains("x") && !Contains("尾")` 恒假，等于没断言）。
		t.Error("尾部内容丢失")
	}
	if !strings.Contains(out, "汉") {
		t.Error("头部内容丢失")
	}
	// 截断结果必须是合法 UTF-8。
	for i, r := range out {
		if r == '�' {
			t.Fatalf("截断产生 U+FFFD @%d", i)
		}
	}
}

// 中间组超长先截 tool 输出；仍超再从最旧中间组整组丢，并插省略说明。
func TestCompactMessagesDropsOldestGroups(t *testing.T) {
	withContextCfg(t, 3000, "compact")
	msgs := []map[string]interface{}{
		{"role": "system", "content": "sys"},
		{"role": "user", "content": "最初任务"},
	}
	// 5 个中间原子组，每组约 700 字节
	for i := 0; i < 5; i++ {
		msgs = append(msgs,
			map[string]interface{}{"role": "user", "content": strings.Repeat("问", 200)},
			toolCallMsg("run", `{"c":"x"}`),
			map[string]interface{}{"role": "tool", "name": "run", "content": strings.Repeat("答", 200)},
		)
	}
	msgs = append(msgs, map[string]interface{}{"role": "user", "content": "最后的问题"})
	out, err := compactMessages(msgs, 3000, 0)
	if err != nil {
		t.Fatalf("compactMessages: %v", err)
	}
	// system、首条 user、省略说明、最后一组必保
	if getStr(out[0], "role") != "system" {
		t.Error("system 丢了")
	}
	if contentToString(out[1]["content"]) != "最初任务" {
		t.Error("首条 user 任务消息丢了")
	}
	hasNote := false
	for _, m := range out {
		if strings.Contains(contentToString(m["content"]), "omitted") {
			hasNote = true
		}
	}
	if !hasNote {
		t.Error("缺 [System note: omitted …] 说明")
	}
	if contentToString(out[len(out)-1]["content"]) != "最后的问题" {
		t.Error("最后一组交互丢了")
	}
	// 被保留的 tool 结果绝不能是孤儿（其 assistant+tool_calls 必须在）。
	for i, m := range out {
		if getStr(m, "role") == "tool" {
			prev := out[i-1]
			orphan := getStr(prev, "role") != "tool"
			if orphan {
				found := false
				for j := i - 1; j >= 0; j-- {
					if getStr(out[j], "role") == "tool" {
						continue
					}
					found = getStr(out[j], "role") == "assistant" &&
						out[j]["tool_calls"] != nil
					break
				}
				if !found {
					t.Fatalf("孤立 tool 结果 @%d", i)
				}
			}
		}
	}
}

// 必保集合本身超预算（system 巨长）→ fail-closed *PromptTooLongError。
func TestCompactMessagesFailClosed(t *testing.T) {
	withContextCfg(t, 1000, "compact")
	msgs := []map[string]interface{}{
		{"role": "system", "content": strings.Repeat("长系统提示。", 500)},
		{"role": "user", "content": "任务"},
	}
	_, err := compactMessages(msgs, 1000, 0)
	var ptl *PromptTooLongError
	if !errors.As(err, &ptl) {
		t.Fatalf("应 fail-closed PromptTooLongError，得到 %v", err)
	}
	if !ptl.HasCookie {
		t.Error("HasCookie 应为 true")
	}
}

// error 模式下超长 history 在 prepareContextFile 里 fail-closed。
func TestPrepareContextFileHistoryCap(t *testing.T) {
	withContextCfg(t, 5000, "error")
	// 有 cookie 走附件路径时 history=整段 prompt。
	longPrompt := strings.Repeat("很", 10000)
	_, _, _, err := prepareContextFile(nil, longPrompt, promptCtx{}, 100, "SAPISID=x; __Secure-1PSID=y", 0, "")
	var ptl *PromptTooLongError
	if !errors.As(err, &ptl) {
		t.Fatalf("history 超 maxFile 应 PromptTooLongError，得到 %v", err)
	}
	if ptl.Budget != 5000 || !ptl.HasCookie {
		t.Errorf("Budget=%d HasCookie=%v", ptl.Budget, ptl.HasCookie)
	}
}

// retryBackoffDelay：瞬时错误（no content frame / failure reply）走指数+抖动，
// 其余错误固定 baseSec；baseSec<=0 → 0。
func TestRetryBackoffDelay(t *testing.T) {
	if d := retryBackoffDelay(0, 3, errors.New("x")); d != 0 {
		t.Fatalf("baseSec=0 应 0，得到 %v", d)
	}
	// 非瞬时错误：固定 baseSec 秒
	fixed := retryBackoffDelay(2, 5, errors.New("perm"))
	if fixed != 2*time.Second {
		t.Fatalf("非瞬时错误应固定 %v，得到 %v", 2*time.Second, fixed)
	}
	// 瞬时错误：attempt=0 → 约 baseSec*[0.8,1.2]
	diag := &diagErr{err: errors.New("no frame"), reason: reasonNoContentFrame}
	for i := 0; i < 50; i++ {
		d := retryBackoffDelay(1, 0, diag)
		if d < 800*time.Millisecond || d > 1200*time.Millisecond {
			t.Fatalf("attempt=0 抖动应落在 [0.8,1.2]s，得到 %v", d)
		}
	}
	// attempt=3 → 8s * [0.8,1.2]，封顶 15s
	for i := 0; i < 50; i++ {
		d := retryBackoffDelay(1, 3, diag)
		if d < 6400*time.Millisecond || d > 15*time.Second {
			t.Fatalf("attempt=3 应落在 [6.4,15]s，得到 %v", d)
		}
	}
	// errUpstreamFailureReply 同样走指数
	for i := 0; i < 50; i++ {
		d := retryBackoffDelay(1, 2, errUpstreamFailureReply)
		if d < 3200*time.Millisecond || d > 15*time.Second {
			t.Fatalf("failure reply attempt=2 应落在 [3.2,15]s，得到 %v", d)
		}
	}
	// 大 attempt 封顶 15s
	for i := 0; i < 20; i++ {
		d := retryBackoffDelay(10, 10, diag)
		if d > 15*time.Second {
			t.Fatalf("应封顶 15s，得到 %v", d)
		}
	}
}

// ── applyCompactIfNeeded 接线回归（R2 补齐）──
//
// 真触发条件 = "compact" 模式 + prompt 超 MaxPromptBytes + maxFile>0 + 有 cookie
// + planContextFile 规划出的 history 真的超过 maxFile。缺一就 100% 原样返回，
// 绝不误改写、不误扣 Head+Tail。

func TestApplyCompactIfNeededNoTriggerPassthrough(t *testing.T) {
	withContextCfg(t, 5000, "compact")
	msgs := []map[string]interface{}{
		{"role": "system", "content": "sys"},
		{"role": "user", "content": "hi"},
	}
	// prompt 不超 budget：不触发
	out, done, err := applyCompactIfNeeded(msgs, "short prompt", promptCtx{})
	if err != nil || done || len(out) != 2 {
		t.Fatalf("未超限应原样返回：done=%v err=%v out=%d", done, err, len(out))
	}
	// 非 compact 模式：不触发
	withContextCfg(t, 5000, "error")
	out, done, err = applyCompactIfNeeded(msgs, strings.Repeat("x", 99999), promptCtx{})
	if err != nil || done || len(out) != 2 {
		t.Fatalf("非 compact 模式应原样返回：done=%v err=%v", done, err)
	}
	// 无 cookie 时也不触发（hasCookie=false → 走 PromptTooLongError 而非 compact）
	rtMu.Lock()
	oldAccts := rtVal
	rtMu.Unlock()
	_ = oldAccts // hasCookie 由 DB 决定，测试库无账号→false
	out, done, err = applyCompactIfNeeded(msgs, strings.Repeat("x", 99999), promptCtx{})
	if err != nil || done {
		t.Fatalf("无 cookie 不应触发 compact：done=%v err=%v", done, err)
	}
}

// history 真超 maxFile 时裁剪后必须 ≤maxFile（用真实 formatToolResult 渲染字节
// 记账，不是拿原始 content 长度估算）。
func TestApplyCompactIfNeededRealBytesWithinBudget(t *testing.T) {
	withContextCfgBudget(t, 4000, "compact", 5000)
	// 需要一个有 cookie 的账号让 hasCookie()=true 走附件路径。
	ck := addTestAccount(t, "compactCookie")
	defer func() { _ = accountDelete(ck) }()
	msgs := []map[string]interface{}{
		{"role": "system", "content": "sys"},
		{"role": "user", "content": "最初任务"},
	}
	for i := 0; i < 6; i++ {
		msgs = append(msgs,
			toolCallMsg("run", `{"c":"x"}`),
			map[string]interface{}{"role": "tool", "name": "run",
				"content": strings.Repeat("答", 300)},
		)
	}
	msgs = append(msgs, map[string]interface{}{"role": "user", "content": "最后"})
	longPrompt := strings.Repeat("很", 20000)
	out, done, err := applyCompactIfNeeded(msgs, longPrompt, promptCtx{})
	if err != nil || !done {
		t.Fatalf("应触发裁剪：done=%v err=%v", done, err)
	}
	total := 0
	for _, m := range out {
		total += len(contentToString(m["content"]))
	}
	if total > 4000 {
		t.Fatalf("裁剪后 %d 字节仍超 maxFile=4000", total)
	}
}

// 必保集合不挪位：首条 user 超 25% 也要留在原位（不把它挪到 system 后面当
// 「最后 user」），中间 system 的顺序也不乱。
func TestCompactMessagesPinnedNoReorder(t *testing.T) {
	withContextCfg(t, 3000, "compact")
	msgs := []map[string]interface{}{
		{"role": "system", "content": "S1"},
		{"role": "user", "content": strings.Repeat("首", 900)}, // >25% of 3000
		{"role": "system", "content": "S2-mid"},
		{"role": "user", "content": "mid-q"},
		toolCallMsg("run", `{"c":"x"}`),
		{"role": "tool", "name": "run", "content": "r"},
		{"role": "user", "content": "tail"},
	}
	out, err := compactMessages(msgs, 3000, 0)
	if err != nil {
		t.Fatalf("compactMessages: %v", err)
	}
	// S1 必须在最前
	if getStr(out[0], "role") != "system" || contentToString(out[0]["content"]) != "S1" {
		t.Fatalf("首条 system 被挪位：%v", out[0])
	}
	// 长首条 user 仍紧跟 S1（原位 pinned，不挪到尾部）
	if getStr(out[1], "role") != "user" || !strings.Contains(contentToString(out[1]["content"]), "首") {
		t.Fatalf("超长首条 user 应原位保留：%v", out[1])
	}
	// 最后一条仍是 tail
	if contentToString(out[len(out)-1]["content"]) != "tail" {
		t.Fatalf("最后一组被挪位：%v", out[len(out)-1])
	}
}

// noteMsg（[System note: omitted …]）的字节必须计入预算：插 note 后再超限要
// 继续丢组，不能插完就不管。
func TestCompactMessagesNoteCountedInBudget(t *testing.T) {
	withContextCfg(t, 1500, "compact")
	msgs := []map[string]interface{}{
		{"role": "system", "content": "sys"},
		{"role": "user", "content": "task"},
	}
	// 多个中间组逼出 note，且 note 自身占空间
	for i := 0; i < 8; i++ {
		msgs = append(msgs,
			map[string]interface{}{"role": "user", "content": strings.Repeat("q", 150)},
			map[string]interface{}{"role": "assistant", "content": strings.Repeat("a", 150)},
		)
	}
	msgs = append(msgs, map[string]interface{}{"role": "user", "content": "tail"})
	out, err := compactMessages(msgs, 1500, 0)
	if err != nil {
		t.Fatalf("compactMessages: %v", err)
	}
	total := 0
	for _, m := range out {
		total += len(contentToString(m["content"]))
	}
	if total > 1500 {
		t.Fatalf("含 note 的总字节 %d 仍超预算 1500", total)
	}
}

// responsesContextOverflowCheck：multiTurn 短路；无 cookie/匿名 eligible 时
// prompt 超 budget → PromptTooLongError；history 超 maxFile → PromptTooLongError。
func TestResponsesContextOverflowCheck(t *testing.T) {
	withContextCfgBudget(t, 5000, "error", 10000)
	longPrompt := strings.Repeat("很", 99999)
	// multiTurn=true：无条件放行
	if err := responsesContextOverflowCheck(longPrompt, promptCtx{}, true); err != nil {
		t.Fatalf("multiTurn 应放行：%v", err)
	}
	// 无 cookie：超 budget → PromptTooLongError（无 HasCookie）
	_, _ = getDB().Exec(`DELETE FROM accounts`)
	var ptl *PromptTooLongError
	err := responsesContextOverflowCheck(longPrompt, promptCtx{}, false)
	if !errors.As(err, &ptl) || ptl.HasCookie {
		t.Fatalf("无 cookie 应报无 HasCookie 的 PTL：%v", err)
	}
	// 有 cookie + history 超 maxFile → HasCookie PTL
	ck := addTestAccount(t, "ovflCookie")
	defer func() { _ = accountDelete(ck) }()
	err = responsesContextOverflowCheck(longPrompt, promptCtx{}, false)
	if !errors.As(err, &ptl) || !ptl.HasCookie || ptl.Budget != 5000 {
		t.Fatalf("history 超 maxFile 应报 HasCookie PTL(budget=5000)：%v", err)
	}
	// anonEligible=true：同无 cookie 路径（不查库）
	err = responsesContextOverflowCheck(longPrompt, promptCtx{}, false, true)
	if !errors.As(err, &ptl) {
		t.Fatalf("anonEligible 超 budget 应报 PTL：%v", err)
	}
	// 未超 budget：放行
	if err := responsesContextOverflowCheck("short", promptCtx{}, false); err != nil {
		t.Fatalf("未超限应放行：%v", err)
	}
}

// argonMissingReclassify 三分支回归：出错话术→errUpstreamFailureReply；
// 无内容帧无思考链非视频→nil；有正文/思考链但无合法身份→原样保留。
func TestArgonMissingReclassify(t *testing.T) {
	mkLine := func(payload string) string {
		b, _ := json.Marshal([]interface{}{[]interface{}{
			"wrb.fr", "x", payload, nil, "generic"}})
		return string(b)
	}
	// 1) 有出错话术帧（inner[31] 真值）→ errUpstreamFailureReply
	pad := make([]interface{}, 32)
	pad[31] = true
	fb, _ := json.Marshal(pad)
	raw := mkLine(string(fb))
	if got := argonMissingReclassify(errArgonIdentityMissing, raw, 0); !errors.Is(got, errUpstreamFailureReply) {
		t.Fatalf("出错话术应归 errUpstreamFailureReply，得到 %v", got)
	}
	// 2) 无内容帧无思考链非视频任务 → nil（落入调用方空帧重试）
	if got := argonMissingReclassify(errArgonIdentityMissing, "[]", 0); got != nil {
		t.Fatalf("空响应应归 nil，得到 %v", got)
	}
	// 3) 有正文帧但无合法身份 → 原样保留 errArgonIdentityMissing
	//    正文帧结构：inner[4] 是 parts，parts[i][1] 是文本列表。
	contentPayload := `[null,null,null,null,[["a",["hello world"]]]]`
	raw2 := mkLine(contentPayload)
	if got := argonMissingReclassify(errArgonIdentityMissing, raw2, 0); !errors.Is(got, errArgonIdentityMissing) {
		t.Fatalf("有正文无身份应保留 errArgonIdentityMissing，得到 %v", got)
	}
	// 非 errArgonIdentityMissing 原样透传
	other := errors.New("other")
	if got := argonMissingReclassify(other, raw2, 0); got != other {
		t.Fatalf("其它错误应原样透传，得到 %v", got)
	}
}
