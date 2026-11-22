package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func conversationTestCall(id, name, arguments string) map[string]interface{} {
	return map[string]interface{}{"id": id, "type": "function", "function": map[string]interface{}{"name": name, "arguments": arguments}}
}

func TestCanonicalMessagesTracksToolIdentity(t *testing.T) {
	base := []map[string]interface{}{{"role": "assistant", "content": "", "tool_calls": []interface{}{conversationTestCall("call_1", "read_file", `{"path":"one"}`)}}}
	for _, tc := range []struct{ label, id, name, args string }{
		{"id", "call_2", "read_file", `{"path":"one"}`},
		{"name", "call_1", "write_file", `{"path":"one"}`},
		{"arguments", "call_1", "read_file", `{"path":"two"}`},
	} {
		t.Run(tc.label, func(t *testing.T) {
			changed := []map[string]interface{}{{"role": "assistant", "content": "", "tool_calls": []interface{}{conversationTestCall(tc.id, tc.name, tc.args)}}}
			if canonicalMessages(base) == canonicalMessages(changed) {
				t.Fatal("different tool call histories must not share a conversation")
			}
		})
	}
}

func TestCanonicalMessagesTracksToolResultAssociation(t *testing.T) {
	base := []map[string]interface{}{{"role": "tool", "content": "same result", "name": "read_file", "tool_call_id": "call_1"}}
	for _, tc := range []struct{ name, id string }{{"write_file", "call_1"}, {"read_file", "call_2"}} {
		changed := []map[string]interface{}{{"role": "tool", "content": "same result", "name": tc.name, "tool_call_id": tc.id}}
		if canonicalMessages(base) == canonicalMessages(changed) {
			t.Fatal("different tool result associations must not share a conversation")
		}
	}
}

func TestCanonicalMessagesDoesNotConfuseMessageBoundaries(t *testing.T) {
	one := []map[string]interface{}{{"role": "user", "content": "first\x1eassistant\x1fsecond"}}
	two := []map[string]interface{}{{"role": "user", "content": "first"}, {"role": "assistant", "content": "second"}}
	if canonicalMessages(one) == canonicalMessages(two) {
		t.Fatal("message text containing separators must not collide with another history")
	}
}

func TestCanonicalMessagesNormalizesOnlyToolArguments(t *testing.T) {
	first := []map[string]interface{}{{"role": "assistant", "content": "keep whitespace ", "tool_calls": []interface{}{conversationTestCall("call_1", "read_file", `{"path":"one","options":{"a":1,"b":2}}`)}}}
	equivalent := []map[string]interface{}{{"role": "assistant", "content": "keep whitespace ", "tool_calls": []interface{}{conversationTestCall("call_1", "read_file", ` { "options": {"b":2, "a":1}, "path": "one" } `)}}}
	if canonicalMessages(first) != canonicalMessages(equivalent) {
		t.Fatal("JSON argument formatting must not cause a false history miss")
	}
	equivalent[0]["content"] = "keep whitespace"
	if canonicalMessages(first) == canonicalMessages(equivalent) {
		t.Fatal("ordinary text whitespace must remain part of the exact history")
	}
	big := []map[string]interface{}{{"role": "assistant", "tool_calls": []interface{}{conversationTestCall("c", "n", `{"n":9007199254740992}`)}}}
	bigger := []map[string]interface{}{{"role": "assistant", "tool_calls": []interface{}{conversationTestCall("c", "n", `{"n":9007199254740993}`)}}}
	if canonicalMessages(big) == canonicalMessages(bigger) {
		t.Fatal("argument canonicalization must preserve integer precision")
	}
}

// 规范层不再合并相邻 assistant（任务书修复 3）：同轮拆分回放的合并由
// responsesToMessages 适配层承担，规范层保留 Chat 消息边界。split 与
// combined 现在指纹不同、是合法的两段历史。Responses 拆分回放等价性在
// TestResponsesInputAssistantReplayGrouping 核验。
func TestCanonicalMessagesDoesNotMergeAssistantReplay(t *testing.T) {
	call := conversationTestCall("call_1", "read_file", `{"path":"one"}`)
	combined := []map[string]interface{}{{"role": "assistant", "content": "Checking", "tool_calls": []interface{}{call}}}
	split := []map[string]interface{}{
		{"role": "assistant", "tool_calls": []interface{}{call}},
		{"role": "assistant", "content": "Checking"},
	}
	if canonicalMessages(combined) == canonicalMessages(split) {
		t.Fatal("normalization must not merge adjacent assistant messages into one turn")
	}
	// Chat 里客户端分两条发的合法边界要原样保留。
	if got := normalizeConversationMessages(split); len(got) != 2 {
		t.Fatalf("adjacent assistant messages must stay separate: %#v", got)
	}
}

func resetConversationTestState(t *testing.T) {
	t.Helper()
	convMu.Lock()
	oldStore := convStore
	convStore = map[string]*convState{}
	convMu.Unlock()
	oldRT := rtCfg()
	rtMu.Lock()
	rtVal.AnonFirst = true
	rtVal.StrictModel = false
	rtMu.Unlock()
	t.Cleanup(func() {
		convMu.Lock()
		convStore = oldStore
		convMu.Unlock()
		rtMu.Lock()
		rtVal = oldRT
		rtMu.Unlock()
	})
}

func conversationTestRaw(t *testing.T, text string) string {
	t.Helper()
	inner := make([]interface{}, 44)
	// inner[1] 故意不给 cid/rid：测试不依赖 parseConvIDs 回填，各续接桩显式
	// 设置 st.cid（生产路径只有拿到 cid 才入库，见 callGeminiConv 尾部）。
	inner[1] = []interface{}{"", ""}
	inner[4] = []interface{}{[]interface{}{"rc_test", []interface{}{text}, nil, nil, nil, nil, nil, nil, []interface{}{2}, "en"}}
	inner[39], inner[42], inner[43] = "cf41b0e0dd7d53e5", "3.5 Flash-Lite", true
	body, err := json.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	line, err := json.Marshal([]interface{}{[]interface{}{"wrb.fr", nil, string(body)}})
	if err != nil {
		t.Fatal(err)
	}
	return string(line)
}

// stubConversationTestTurn 替换上游那一轮（本仓带 ctx 透传）。f 收到的 mc 是
// callGeminiConv 传入的 ModelConfig —— 用于断言 TaskMode 透传。
func stubConversationTestTurn(t *testing.T, f func(context.Context, string, ModelConfig, *convState) (*StreamResult, error)) {
	t.Helper()
	old := runConversationTurn
	runConversationTurn = func(ctx context.Context, prompt string, mc ModelConfig, conv *convState, _, _ func(string)) (*StreamResult, error) {
		return f(ctx, prompt, mc, conv)
	}
	t.Cleanup(func() { runConversationTurn = old })
}

func conversationTestTools() []map[string]interface{} {
	return []map[string]interface{}{{"type": "function", "function": map[string]interface{}{"name": "read_file", "parameters": map[string]interface{}{"type": "object"}}}}
}

func TestConversationScopeSeparatesModelToolsAndRequests(t *testing.T) {
	mc := Models["gemini-3.5-flash-lite"]
	tools := conversationTestTools()
	base := convScopeKey(mc, tools, "auto", "thread-one")
	otherModel, otherThinking, otherTask := mc, mc, mc
	otherModel.Mode++
	otherThinking.ThinkWire = thinkingExtended
	otherTask.TaskMode = true // 本仓高危冲突 2：TaskMode 必须隔离会话作用域
	otherTools := conversationTestTools()
	otherTools[0]["function"].(map[string]interface{})["description"] = "changed schema"
	for label, key := range map[string]string{
		"model":    convScopeKey(otherModel, tools, "auto", "thread-one"),
		"thinking": convScopeKey(otherThinking, tools, "auto", "thread-one"),
		"taskmode": convScopeKey(otherTask, tools, "auto", "thread-one"),
		"tools":    convScopeKey(mc, otherTools, "auto", "thread-one"),
		"choice":   convScopeKey(mc, tools, "required", "thread-one"),
		"request":  convScopeKey(mc, tools, "auto", "thread-two"),
	} {
		if key == base {
			t.Errorf("%s must start a different conversation scope", label)
		}
	}
}

func TestConversationTakeSelectsLongestProperPrefixAndCopiesState(t *testing.T) {
	resetConversationTestState(t)
	keys := []string{"short", "long", "whole"}
	st := &convState{cid: "long-conversation", turn: 2, accountID: 9, proxyID: 4}
	convPut("short", &convState{cid: "short-conversation", turn: 1})
	convPut("long", st)
	convPut("whole", &convState{cid: "must-not-use-whole-request"})
	st.cid = "mutated-after-put"
	got, prefix := convTake(keys)
	if got == nil || prefix != 2 || got.cid != "long-conversation" || got.accountID != 9 || got.proxyID != 4 {
		t.Fatalf("longest prefix and original state must be used, got %+v prefix=%d", got, prefix)
	}
	convMu.Lock()
	_, stillPresent := convStore["long"]
	_, wholePresent := convStore["whole"]
	convMu.Unlock()
	if stillPresent || !wholePresent {
		t.Fatal("take must consume only the selected proper prefix")
	}
}

func TestConversationTakeRejectsExpiredState(t *testing.T) {
	resetConversationTestState(t)
	convMu.Lock()
	convStore["expired"] = &convState{cid: "old", updated: time.Now().Add(-convTTL - time.Second)}
	convMu.Unlock()
	if got, prefix := convTake([]string{"expired", "suffix"}); got != nil || prefix != 0 {
		t.Fatalf("expired state must cold-start, got %+v prefix=%d", got, prefix)
	}
	convMu.Lock()
	_, kept := convStore["expired"]
	convMu.Unlock()
	if kept {
		t.Fatal("expired entry should be removed")
	}
}

func TestConversationTakeHasOneConcurrentOwner(t *testing.T) {
	resetConversationTestState(t)
	convPut("head", &convState{cid: "one-conversation"})
	var wg sync.WaitGroup
	owners := make(chan bool, 20)
	for i := 0; i < cap(owners); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, _ := convTake([]string{"head", "suffix"})
			owners <- got != nil
		}()
	}
	wg.Wait()
	close(owners)
	n := 0
	for owner := range owners {
		if owner {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("one upstream conversation must have exactly one owner, got %d", n)
	}
}

func TestCallGeminiConvSendsAllNewToolAndUserMessages(t *testing.T) {
	resetConversationTestState(t)
	mc := Models["gemini-3.5-flash-lite"]
	tools := conversationTestTools()
	first := []map[string]interface{}{{"role": "system", "content": "stable-system"}, {"role": "user", "content": "original-question"}}
	var prompts []string
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, prompt string, _ ModelConfig, st *convState) (*StreamResult, error) {
		prompts = append(prompts, prompt)
		turns = append(turns, st.turn)
		if len(prompts) == 1 {
			st.accountID, st.proxyID = 17, 4
		} else if st.accountID != 17 || st.proxyID != 4 || st.cid != "same-conversation" {
			t.Fatalf("continuation must retain account, proxy and upstream id: %+v", st)
		}
		st.cid = "same-conversation"
		st.turn++
		text := "finished"
		if len(prompts) == 1 {
			text = "Checking\n```tool_call\n{\"name\":\"read_file\",\"arguments\":{\"path\":\"one\"}}\n```\n```tool_call\n{\"name\":\"read_file\",\"arguments\":{\"path\":\"two\"}}\n```"
		}
		return &StreamResult{Raw: conversationTestRaw(t, text)}, nil
	})
	text, calls, _, err := callGeminiConv(context.Background(), first, mc, tools, "auto", nil, nil, conversationOptions{requestScope: "thread"})
	if err != nil || len(calls) != 2 {
		t.Fatalf("initial tool turn: %q %+v %v", text, calls, err)
	}
	callItems := make([]interface{}, len(calls))
	for i, c := range calls {
		callItems[i] = conversationTestCall(c.ID, c.Function.Name, c.Function.Arguments)
	}
	replay := append(append([]map[string]interface{}{}, first...),
		map[string]interface{}{"role": "assistant", "content": text, "tool_calls": callItems},
		map[string]interface{}{"role": "tool", "tool_call_id": calls[0].ID, "content": "result-one"},
		map[string]interface{}{"role": "tool", "tool_call_id": calls[1].ID, "content": "result-two"},
		map[string]interface{}{"role": "user", "content": "time-context-message"},
		map[string]interface{}{"role": "user", "content": "continue-now"})
	_, _, _, err = callGeminiConv(context.Background(), replay, mc, tools, "auto", nil, nil, conversationOptions{requestScope: "thread"})
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 2 || turns[0] != 0 || turns[1] != 1 {
		t.Fatalf("expected same upstream conversation, turns=%v", turns)
	}
	for _, want := range []string{"result-one", "result-two", "time-context-message", "continue-now", "[Tool result for read_file]"} {
		if !strings.Contains(prompts[1], want) {
			t.Errorf("continuation dropped %q: %s", want, prompts[1])
		}
	}
	for _, old := range []string{"stable-system", "original-question", "Available tools"} {
		if strings.Contains(prompts[1], old) {
			t.Errorf("continuation resent old context %q", old)
		}
	}
}

func TestCallGeminiConvPlainChatAndConsumedHeads(t *testing.T) {
	resetConversationTestState(t)
	mc := Models["gemini-3.5-flash-lite"]
	first := []map[string]interface{}{{"role": "user", "content": "first-question"}}
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		turns = append(turns, st.turn)
		st.cid = "plain-conversation"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "first-answer")}, nil
	})
	if _, _, _, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	next := append(append([]map[string]interface{}{}, first...), map[string]interface{}{"role": "assistant", "content": "first-answer"}, map[string]interface{}{"role": "user", "content": "follow-up"})
	if _, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(turns) != 3 || turns[0] != 0 || turns[1] != 1 || turns[2] != 0 {
		t.Fatalf("ordinary follow-up should hit once, stale fork must cold-start: %v", turns)
	}
}

func TestCallGeminiConvChangedHistoryOrScopeStartsFresh(t *testing.T) {
	for _, change := range []string{"system", "model", "tools", "choice", "scope", "taskmode"} {
		t.Run(change, func(t *testing.T) {
			resetConversationTestState(t)
			mc := Models["gemini-3.5-flash-lite"]
			tools, choice, scope := conversationTestTools(), interface{}("auto"), "one"
			first := []map[string]interface{}{{"role": "system", "content": "stable"}, {"role": "user", "content": "question"}}
			var turns []int
			stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
				turns = append(turns, st.turn)
				st.cid = "scope-conversation"
				st.turn++
				return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
			})
			if _, _, _, err := callGeminiConv(context.Background(), first, mc, tools, choice, nil, nil, conversationOptions{requestScope: scope}); err != nil {
				t.Fatal(err)
			}
			next := []map[string]interface{}{{"role": "system", "content": "stable"}, {"role": "user", "content": "question"}, {"role": "assistant", "content": "answer"}, {"role": "user", "content": "next"}}
			switch change {
			case "system":
				next[0]["content"] = "changed"
			case "model":
				mc.Mode++
			case "tools":
				tools[0]["function"].(map[string]interface{})["description"] = "changed"
			case "choice":
				choice = "required"
			case "scope":
				scope = "two"
			case "taskmode":
				mc.TaskMode = true
			}
			if _, _, _, err := callGeminiConv(context.Background(), next, mc, tools, choice, nil, nil, conversationOptions{requestScope: scope}); err != nil {
				t.Fatal(err)
			}
			if len(turns) != 2 || turns[1] != 0 {
				t.Fatalf("changed %s must cold-start: %v", change, turns)
			}
		})
	}
}

func TestCallGeminiConvFailedContinuationIsNotReused(t *testing.T) {
	resetConversationTestState(t)
	mc := Models["gemini-3.5-flash-lite"]
	first := []map[string]interface{}{{"role": "user", "content": "original-question"}}
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		turns = append(turns, st.turn)
		st.cid = "failed-continuation"
		st.turn++ // Even a failed request may already have advanced upstream.
		if len(turns) == 2 {
			return &StreamResult{}, errors.New("lost upstream response")
		}
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	if _, _, _, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	next := append(append([]map[string]interface{}{}, first...), map[string]interface{}{"role": "assistant", "content": "answer"}, map[string]interface{}{"role": "user", "content": "next"})
	if _, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil); err == nil {
		t.Fatal("expected upstream failure")
	}
	if _, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(turns) != 3 || turns[1] != 1 || turns[2] != 0 {
		t.Fatalf("failure must invalidate consumed head: %v", turns)
	}
}

func TestCallGeminiConvEmptyContinuationIsNotReused(t *testing.T) {
	resetConversationTestState(t)
	mc := Models["gemini-3.5-flash-lite"]
	first := []map[string]interface{}{{"role": "user", "content": "original-question"}}
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		turns = append(turns, st.turn)
		st.cid = "empty-continuation"
		st.turn++
		if len(turns) == 2 {
			return &StreamResult{Raw: conversationTestRaw(t, "")}, nil
		}
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	if _, _, _, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	next := append(append([]map[string]interface{}{}, first...), map[string]interface{}{"role": "assistant", "content": "answer"}, map[string]interface{}{"role": "user", "content": "next"})
	if _, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil); err == nil {
		t.Fatal("empty upstream result must fail")
	}
	if _, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(turns) != 3 || turns[1] != 1 || turns[2] != 0 {
		t.Fatalf("empty response must invalidate consumed head: %v", turns)
	}
}

func TestGeminiConversationFingerprintNormalizesOptionalWireIDs(t *testing.T) {
	original := []map[string]interface{}{
		{"role": "user", "content": "read one"},
		{"role": "assistant", "tool_calls": []interface{}{conversationTestCall("server_call", "read_file", `{"path":"one"}`)}},
		{"role": "tool", "tool_call_id": "server_call", "name": "read_file", "content": "result"},
	}
	replayed := []map[string]interface{}{
		{"role": "user", "content": "read one"},
		{"role": "assistant", "tool_calls": []interface{}{conversationTestCall("generated_on_replay", "read_file", `{"path":"one"}`)}},
		{"role": "tool", "name": "read_file", "content": "result"},
	}
	beforeOriginal, beforeReplay := canonicalMessages(original), canonicalMessages(replayed)
	first := convPrefixKeys(original, "gemini-thread", true)
	second := convPrefixKeys(replayed, "gemini-thread", true)
	if len(first) != 3 || len(second) != 3 || first[2] != second[2] {
		t.Fatalf("Gemini optional IDs must match the same logical call/result: %v vs %v", first, second)
	}
	strictFirst := convPrefixKeys(original, "openai-thread")
	strictSecond := convPrefixKeys(replayed, "openai-thread")
	if strictFirst[2] == strictSecond[2] {
		t.Fatal("OpenAI histories must retain explicit call IDs for matching")
	}
	if beforeOriginal != canonicalMessages(original) || beforeReplay != canonicalMessages(replayed) {
		t.Fatal("fingerprinting must not rewrite public call IDs or input messages")
	}
}

func TestGeminiConversationFingerprintKeepsExplicitResultAssociation(t *testing.T) {
	makeHistory := func(firstID, secondID string) []map[string]interface{} {
		return []map[string]interface{}{
			{"role": "assistant", "tool_calls": []interface{}{
				conversationTestCall("call_one", "read_file", `{"path":"one"}`),
				conversationTestCall("call_two", "read_file", `{"path":"two"}`),
			}},
			{"role": "tool", "tool_call_id": firstID, "name": "read_file", "content": "result-A"},
			{"role": "tool", "tool_call_id": secondID, "name": "read_file", "content": "result-B"},
		}
	}
	ordered := convPrefixKeys(makeHistory("call_one", "call_two"), "gemini-thread", true)
	swapped := convPrefixKeys(makeHistory("call_two", "call_one"), "gemini-thread", true)
	if ordered[len(ordered)-1] == swapped[len(swapped)-1] {
		t.Fatal("result text associated with a different explicit call must not hit")
	}
}

// 本仓高危冲突 1：callGeminiConv 第一参数是 ctx context.Context，必须透传到
// 上游那一轮，客户端取消要能打断（上游 PR #93 没有 ctx 参数，移植时容易丢）。
func TestCallGeminiConvPassesContextThrough(t *testing.T) {
	resetConversationTestState(t)
	mc := Models["gemini-3.5-flash-lite"]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got context.Context
	stubConversationTestTurn(t, func(c context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		got = c
		st.cid = "ctx-conversation"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	first := []map[string]interface{}{{"role": "user", "content": "question"}}
	if _, _, _, err := callGeminiConv(ctx, first, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if got != ctx {
		t.Fatal("callGeminiConv must forward the caller's context to the upstream turn")
	}
}

// 本仓高危冲突 2：TaskMode 进入 convScopeKey（上面 TestConversationScope* 已覆盖
// 隔离），这里断言续接链路上 mc.TaskMode 原样透传到上游轮——Task 模式的 inner 槽位
// 和末尾工具锚点分支都靠它。
func TestCallGeminiConvTaskModeContinuationKeepsScopeAndMode(t *testing.T) {
	resetConversationTestState(t)
	base := Models["gemini-3.5-flash-lite"]
	base.TaskMode = true
	// 同名同上游同 hex/mode、仅 TaskMode 不同的另一路配置不能命中同一会话。
	chat := base
	chat.TaskMode = false
	if convScopeKey(base, nil, nil, "thread") == convScopeKey(chat, nil, nil, "thread") {
		t.Fatal("TaskMode must be part of the conversation scope")
	}
	var taskModes []bool
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, _ string, mc ModelConfig, st *convState) (*StreamResult, error) {
		taskModes = append(taskModes, mc.TaskMode)
		turns = append(turns, st.turn)
		st.cid = "task-conversation"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	first := []map[string]interface{}{{"role": "user", "content": "task-question"}}
	if _, _, _, err := callGeminiConv(context.Background(), first, base, nil, nil, nil, nil, conversationOptions{requestScope: "task-thread"}); err != nil {
		t.Fatal(err)
	}
	next := append(append([]map[string]interface{}{}, first...),
		map[string]interface{}{"role": "assistant", "content": "answer"},
		map[string]interface{}{"role": "user", "content": "task-followup"})
	if _, _, _, err := callGeminiConv(context.Background(), next, base, nil, nil, nil, nil, conversationOptions{requestScope: "task-thread"}); err != nil {
		t.Fatal(err)
	}
	if len(turns) != 2 || turns[1] != 1 {
		t.Fatalf("task-mode continuation must reuse the same upstream conversation: %v", turns)
	}
	for i, tm := range taskModes {
		if !tm {
			t.Fatalf("turn %d did not receive TaskMode=true upstream", i)
		}
	}
	// 同历史换成 Chat 模式（TaskMode=false）必须冷启动，不能撞上面的 Task 会话。
	if _, _, _, err := callGeminiConv(context.Background(), next, chat, nil, nil, nil, nil, conversationOptions{requestScope: "task-thread"}); err != nil {
		t.Fatal(err)
	}
	if turns[len(turns)-1] != 0 {
		t.Fatalf("chat-mode request must cold-start, not reuse task conversation: %v", turns)
	}
}

// 相邻两条 assistant 是合法的 Chat 历史边界（比如客户端拼接的续写）。归一化
// 无条件合并会把 ["first","second"] 和 ["firstsecond"] 变成同一份指纹，
// 让两段真实不同的对话互相续接 —— 指纹必须保留消息边界。
func TestNormalizeConversationKeepsAssistantBoundaries(t *testing.T) {
	split := []map[string]interface{}{
		{"role": "user", "content": "question"},
		{"role": "assistant", "content": "first"},
		{"role": "assistant", "content": "second"},
		{"role": "user", "content": "next"},
	}
	merged := []map[string]interface{}{
		{"role": "user", "content": "question"},
		{"role": "assistant", "content": "firstsecond"},
		{"role": "user", "content": "next"},
	}
	if canonicalMessages(split) == canonicalMessages(merged) {
		t.Fatal("adjacent assistant messages must keep their boundary, not collapse into one")
	}
	if got := normalizeConversationMessages(split); len(got) != 4 {
		t.Fatalf("split assistant messages must not be merged: %#v", got)
	}
	// 冷启动 prompt 也要带两条独立的 [Assistant] 行，不是拼接后的一条。
	prompt := buildPrompt(split, nil, nil, false)
	if !strings.Contains(prompt, "[Assistant]: first\n\n[Assistant]: second") {
		t.Fatalf("prompt must keep two separate assistant turns: %q", prompt)
	}
}

// 缺省 call_id 的 tool_calls 不能污染 names[""]：后面一条既没 tool_call_id 也
// 没 name 的 tool 消息不能蹭到别的调用的函数名，也不能和显式 name 撞车。
func TestNormalizeConversationEmptyCallIDDoesNotLeakName(t *testing.T) {
	messages := []map[string]interface{}{
		{"role": "assistant", "tool_calls": []interface{}{
			map[string]interface{}{"type": "function", "function": map[string]interface{}{"name": "alpha", "arguments": "{}"}},
			map[string]interface{}{"type": "function", "function": map[string]interface{}{"name": "beta", "arguments": "{}"}},
		}},
		{"role": "tool", "content": "anonymous result"},
		{"role": "tool", "name": "beta", "content": "named result"},
	}
	got := normalizeConversationMessages(messages)
	if getStr(got[1], "name") != "" {
		t.Fatalf("id-less tool result must not inherit a call name: %#v", got[1])
	}
	if getStr(got[2], "name") != "beta" {
		t.Fatalf("explicit tool result name must be kept: %#v", got[2])
	}
	// 显式 name 回填仍要工作：tool_call_id 能对上时从 calls 找回函数名。
	resolved := normalizeConversationMessages([]map[string]interface{}{
		{"role": "assistant", "tool_calls": []interface{}{
			conversationTestCall("call_x", "gamma", "{}"),
		}},
		{"role": "tool", "tool_call_id": "call_x", "content": "ok"},
	})
	if getStr(resolved[1], "name") != "gamma" {
		t.Fatalf("tool result name resolution broke: %#v", resolved[1])
	}
}

// 归一化位置 ID（#N）与客户端原样的未匹配 wireID 不能撞名："call_0"/"#0"
// 这类原始 ID 必须重写成独立命名空间，否则两条语义不同的结果撞同一份指纹。
func TestConversationWithoutWireIDsNamespacesUnmatchedIDs(t *testing.T) {
	calls := []interface{}{
		conversationTestCall("fc_1", "read_file", `{"path":"one"}`),
		conversationTestCall("fc_2", "read_file", `{"path":"two"}`),
	}
	histA := []map[string]interface{}{
		{"role": "assistant", "tool_calls": calls},
		{"role": "tool", "tool_call_id": "fc_1", "name": "read_file", "content": "result-A"},
	}
	histB := []map[string]interface{}{
		{"role": "assistant", "tool_calls": calls},
		{"role": "tool", "tool_call_id": "call_0", "name": "read_file", "content": "result-A"},
	}
	keysA := convPrefixKeys(histA, "scope", true)
	keysB := convPrefixKeys(histB, "scope", true)
	if len(keysA) != 2 || len(keysB) != 2 {
		t.Fatalf("unexpected key counts: %v vs %v", keysA, keysB)
	}
	if keysA[1] == keysB[1] {
		t.Fatal("unmatched raw wire ID must not collide with the normalized position ID")
	}
	// 显式 wireID 是 "#0"（位置 ID 的同名字面量）也不能撞位置 0。
	histC := []map[string]interface{}{
		{"role": "assistant", "tool_calls": calls},
		{"role": "tool", "tool_call_id": "#0", "name": "read_file", "content": "result-A"},
	}
	if convPrefixKeys(histC, "scope", true)[1] == keysA[1] {
		t.Fatal("literal \"#0\" wire ID must not collide with normalized position 0")
	}
}

// mc.Upstream 必须进会话 scope：同 hex/mode、仅登记上游名不同的模型（例如
// gemini-3.8-flash 与 gemini-4-hydrogen）在 strict_model 下语义不同，不能共享
// 一路服务端会话（混用会把 3.8 的续接发到 4 Hydrogen 上报的身份下）。
func TestConversationScopeSeparatesUpstreamName(t *testing.T) {
	if convScopeKey(Models["gemini-3.8-flash"], nil, nil, "thread") ==
		convScopeKey(Models["gemini-4-hydrogen"], nil, nil, "thread") {
		t.Fatal("models with different registered Upstream names must not share a scope")
	}
}

// 已取消的 ctx 必须在动会话存储和账号池之前返回：以前续接请求先 convTake
// 销毁了健康 head，等 runConversationTurn 才发现 ctx 已死，好好的会话作废。
func TestCallGeminiConvRejectsCancelledContextBeforeTake(t *testing.T) {
	resetConversationTestState(t)
	mc := Models["gemini-3.5-flash-lite"]
	first := []map[string]interface{}{{"role": "user", "content": "question"}}
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		turns = append(turns, st.turn)
		st.cid = "cancel-conversation"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	if _, _, _, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	next := append(append([]map[string]interface{}{}, first...),
		map[string]interface{}{"role": "assistant", "content": "answer"},
		map[string]interface{}{"role": "user", "content": "next"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := callGeminiConv(ctx, next, mc, nil, nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context must return context.Canceled, got %v", err)
	}
	// 被取消的请求不能碰会话 head：正常请求还得在原会话上续接（turn 1）。
	if _, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(turns) != 2 || turns[1] != 1 {
		t.Fatalf("cancelled request must not consume the conversation head: %v", turns)
	}
}

// 续接绑定的登录号被禁用/删除后，不能再拿原号发——禁用号在 BrowserTransport
// 下绕过浏览器会漏 Go HTTP 指纹，也可能直接失败。必须回退冷启动重新挑号。
func TestCallGeminiConvDisabledAccountFallsBackToFresh(t *testing.T) {
	resetConversationTestState(t)
	// 会话要求登录态：放一个池内账号供首轮挑号，再手动把 convState 记成它。
	id, err := accountAdd("test", "SAPISID=dummy; SID=x", "")
	if err != nil {
		t.Fatalf("insert test account: %v", err)
	}
	t.Cleanup(func() { _ = accountDelete(id) })
	mc := Models["gemini-3.8-flash"] // modelNeedsLogin，走登录号路径
	var turns []int
	var accountSeen []int64
	stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		turns = append(turns, st.turn)
		accountSeen = append(accountSeen, st.accountID)
		st.cid = "account-conversation"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	first := []map[string]interface{}{{"role": "user", "content": "account-question"}}
	if _, _, _, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil, conversationOptions{requestScope: "acct-thread"}); err != nil {
		t.Fatal(err)
	}
	if accountSeen[0] != id {
		t.Fatalf("first turn must pick the pool account #%d, got %d", id, accountSeen[0])
	}
	// 禁用该账号：下一轮同历史续接必须冷启动（fresh），不能再把请求发到 #id。
	if err := accountSetStatus(id, "disabled"); err != nil {
		t.Fatal(err)
	}
	next := append(append([]map[string]interface{}{}, first...),
		map[string]interface{}{"role": "assistant", "content": "answer"},
		map[string]interface{}{"role": "user", "content": "account-followup"})
	// 唯一账号被禁后没有健康账号可挑：调用必须报错或冷启动（turn==0），
	// 绝不能拿被禁的号去发续接（turn==1 是失败信号）。turns=[0,0] 正确：
	// 禁用后第二轮回退冷启动（upstream 桩仍被调用，但 turn=0 不是续接）。
	_, _, _, callErr := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil, conversationOptions{requestScope: "acct-thread"})
	if len(turns) != 2 || turns[1] != 0 {
		t.Fatalf("disabled bound account must not continue turn 1 on it: turns=%v err=%v", turns, callErr)
	}
	_ = callErr // 报错（无号可挑）或冷启动都接受，关键是没在原号上续接 turn 1
}

// 上游一轮成功但没回 cid（坏帧）：这种状态续接不上，绝不能落库——否则下一
// 路同前缀请求撞进一条永远失败的发空 cid 的死会话。
func TestCallGeminiConvDoesNotStoreHeadWithoutCID(t *testing.T) {
	resetConversationTestState(t)
	mc := Models["gemini-3.5-flash-lite"]
	first := []map[string]interface{}{{"role": "user", "content": "no-cid-question"}}
	stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		// 成功返回但刻意不给 st.cid，模拟上游帧缺续接 ID。
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "no-cid-answer")}, nil
	})
	if _, _, _, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil, conversationOptions{requestScope: "nocid-thread"}); err != nil {
		t.Fatal(err)
	}
	convMu.Lock()
	defer convMu.Unlock()
	if len(convStore) != 0 {
		t.Fatalf("a turn without cid must not be stored as a continuation head: %d entries", len(convStore))
	}
}
