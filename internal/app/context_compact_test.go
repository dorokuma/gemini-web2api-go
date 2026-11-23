package app

import (
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
	old := rtCfg()
	next := old
	next.MaxContextFileBytes = maxFile
	next.ContextOverflowMode = mode
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
	if strings.Contains(out, "x") && !strings.Contains(out, "尾") {
		// 尾巴应保留
		t.Error("尾部内容丢失")
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
