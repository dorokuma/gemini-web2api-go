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
	"testing"
	"time"
)

// blockingBody 让 StreamGenerate 的响应体读取阻塞到 ctx 结束：模拟真实
// 流式响应中途客户端取消——readBody 经 stopReader/heardReader 读到 ctx.Err() 中断。
// reachedRead 非 nil 时首个 Read 关闭它一次：调用方据此确定取消发生在
// StreamGenerate 已进入响应体读取之后（R3-M3 要求确定性，不靠 sleep）。
type blockingBody struct {
	ctx         context.Context
	reachedRead chan struct{}
	once        sync.Once
}

func (b *blockingBody) Read(p []byte) (int, error) {
	if b.reachedRead != nil {
		b.once.Do(func() { close(b.reachedRead) })
	}
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b *blockingBody) Close() error { return nil }

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
	// 相邻两条 assistant 是合法的 Chat 历史边界（比如客户端拼接的续写）。
	// 归一化无条件合并会把 ["first","second"] 和 ["firstsecond"] 变成同一份指纹，
	// 让两段真实不同的对话互相续接 —— 指纹必须保留消息边界。
	splitText := []map[string]interface{}{
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
	if canonicalMessages(splitText) == canonicalMessages(merged) {
		t.Fatal("adjacent assistant messages must keep their boundary, not collapse into one")
	}
	if got := normalizeConversationMessages(splitText); len(got) != 4 {
		t.Fatalf("split assistant messages must not be merged: %#v", got)
	}
	// 冷启动 prompt 也要带两条独立的 [Assistant] 行，不是拼接后的一条。
	prompt := buildPrompt(splitText, nil, nil, false)
	if !strings.Contains(prompt, "[Assistant]: first\n\n[Assistant]: second") {
		t.Fatalf("prompt must keep two separate assistant turns: %q", prompt)
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
			// CanRestoreConvHead 零值（fail-closed）：失败发生在已发往上游之后
			//（中途断流），head 已被服务端推进，绝不能归还复用（区别于本地
			// Bound/取消那种上游没碰过的确定性失败）。
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

// ── R1 修复 1/2/4 回归：strict_proxy_binding 下的多轮 Bound 行为 ──

// 登录态多轮会话架：禁用匿名优先，让首轮必须从 cookie 池挑号。
func resetConvBoundState(t *testing.T) {
	t.Helper()
	resetConversationTestState(t)
	rtMu.Lock()
	rtVal.AnonFirst = false
	rtMu.Unlock()
}

// stubConvTurnRealSlot 把上游轮桩在「拿到 slot 之后」：先真实执行
// acquireClaimedSlot（strict_proxy_binding 的 Bound 判定全靠它），
// 拿到 slot 才进桩（桩等于把上游那一发换成了成功帧）。Bound 时
// acquireClaimedSlot 返回 *RateLimitError{Bound:true} 且 CanRestoreConvHead
// 显式置 true——跟真实 streamGenerateConv 的本地失败路径完全同形。
func stubConvTurnRealSlot(t *testing.T, f func(st *convState) (*StreamResult, error)) {
	t.Helper()
	old := runConversationTurn
	runConversationTurn = func(ctx context.Context, _ string, _ ModelConfig, conv *convState, _, _ func(string)) (*StreamResult, error) {
		if err := ctx.Err(); err != nil {
			return &StreamResult{}, err
		}
		pp, release, err := acquireClaimedSlot(conv.accountID, conv.proxyID)
		if pp == nil {
			// CanRestoreConvHead=true：跟真实 streamGenerateConv 的本地失败路径
			// 同形——上游没碰过，被消费的 head 可以归还。
			return &StreamResult{AccountID: conv.accountID, CanRestoreConvHead: true}, err
		}
		release()
		// conv.proxyID 按真实 streamGenerateConv 的语义就地同步成拿到的出口。
		conv.proxyID = pp.ID
		res, err := f(conv)
		if res != nil {
			res.AccountID = conv.accountID
			res.ProxyID = pp.ID
			// CanRestoreConvHead 保持零值：桩走到这里 = 上游那一发已经发生。
		}
		return res, err
	}
	t.Cleanup(func() { runConversationTurn = old })
}

// 真实入口回归：新会话全池 Bound 受阻时，callGeminiConv 必须返回
// *RateLimitError{Bound:true}（真实的 429），绝不能被 pickAccountForModel 的
// 「排除集非空 → ModelDowngradedError」谎报成模型降级。
func TestCallGeminiConvBoundExhaustedReturnsRateLimitNotDowngrade(t *testing.T) {
	resetConvBoundState(t)
	a, _ := withTwoProxies(t, true)
	acct := addTestAccount(t, "boundonly")
	bindAccountProxy(acct, a) // 唯一账号绑在满出口 A 上
	mc := Models["gemini-3.6-flash"]

	var calls int
	stubConvTurnRealSlot(t, func(st *convState) (*StreamResult, error) {
		calls++
		st.cid = "bound-conv"
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	first := []map[string]interface{}{{"role": "user", "content": "q"}}
	_, _, res, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil, conversationOptions{requestScope: "bound-thread"})
	var rle *RateLimitError
	if !errors.As(err, &rle) || !rle.Bound || rle.ProxyID != a {
		t.Fatalf("全池 Bound 受阻应报 Bound 429(出口 %d)，得到 err=%v res=%+v", a, err, res)
	}
	var mde *ModelDowngradedError
	if errors.As(err, &mde) {
		t.Fatalf("Bound 耗尽不能谎报 ModelDowngradedError：%v", err)
	}
	if calls != 0 {
		t.Fatalf("上游一轮都不该被调用（Bound 是本地限流），实际 %d 次", calls)
	}
}

// 真实入口回归：4 账号池（前 3 绑满 A、第 4 绑空闲 B）必须试到第 4 号——
// Bound 换号不受 maxCookieTries=3 截断。
func TestCallGeminiConvBoundSwitchReachesFourthAccount(t *testing.T) {
	resetConvBoundState(t)
	a, b := withTwoProxies(t, true)
	var ids []int64
	for i := 0; i < 3; i++ {
		id := addTestAccount(t, fmt.Sprintf("stuck%d", i))
		bindAccountProxy(id, a) // 前 3 号全绑在满出口 A
		ids = append(ids, id)
	}
	fourth := addTestAccount(t, "free4")
	bindAccountProxy(fourth, b) // 第 4 号绑空闲出口 B
	mc := Models["gemini-3.6-flash"]

	var tried []int64
	stubConvTurnRealSlot(t, func(st *convState) (*StreamResult, error) {
		tried = append(tried, st.accountID)
		st.cid = "fourth-conv"
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	first := []map[string]interface{}{{"role": "user", "content": "q"}}
	_, _, res, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil, conversationOptions{requestScope: "fourth-thread"})
	if err != nil {
		t.Fatalf("第 4 号绑在空闲出口上必须能被试到，得到 err=%v tried=%v", err, tried)
	}
	if res == nil || res.AccountID != fourth {
		t.Fatalf("应落在第 4 号 #%d，得到 res=%+v", fourth, res)
	}
	if len(tried) != 1 || tried[0] != fourth {
		t.Fatalf("前 3 号 Bound 受阻不该调到上游轮，只有第 4 号能进：tried=%v", tried)
	}
}

// 真实入口回归：续接（!fresh）遇本地 Bound 429——上游一个字节都没发——
// 返回 Bound 429 且被 convTake 消费的健康 head 必须归还：释放 A 后重试
// 在同一路会话上继续 turn 1，而不是作废 head 逼回全量重发。
func TestCallGeminiConvBoundContinuationRestoresHead(t *testing.T) {
	resetConvBoundState(t)
	a, _ := withTwoProxies(t, true)
	// 首轮要 A 空闲：withTwoProxies 占了它，先释放。
	s := getSlot(a)
	s.mu.Lock()
	s.inflight = 0
	s.mu.Unlock()
	acct := addTestAccount(t, "boundcont")
	bindAccountProxy(acct, a) // 会话要绑的号，出口 A 此刻空闲
	mc := Models["gemini-3.6-flash"]

	var turns []int
	stubConvTurnRealSlot(t, func(st *convState) (*StreamResult, error) {
		turns = append(turns, st.turn)
		st.cid = "cont-conv"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	first := []map[string]interface{}{{"role": "user", "content": "hello"}}
	if _, _, _, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil, conversationOptions{requestScope: "cont-thread"}); err != nil {
		t.Fatalf("首轮应在 A 空闲时成功: %v", err)
	}
	next := append(append([]map[string]interface{}{}, first...),
		map[string]interface{}{"role": "assistant", "content": "answer"},
		map[string]interface{}{"role": "user", "content": "follow"})

	// 占满 A：续接轮 acquireSlot(conv.proxyID=A) 本地 Bound 429，零字节发上游。
	s.mu.Lock()
	s.inflight = 1
	s.mu.Unlock()
	_, _, res, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil, conversationOptions{requestScope: "cont-thread"})
	var rle *RateLimitError
	if !errors.As(err, &rle) || !rle.Bound || rle.ProxyID != a {
		t.Fatalf("续接遇绑定出口满应报 Bound 429，得到 err=%v res=%+v", err, res)
	}
	if len(turns) != 1 {
		t.Fatalf("本地 Bound 不该调到上游轮：turns=%v", turns)
	}

	// 释放 A：重试同一路请求必须命中归还的 head 继续 turn 1，不是冷启动 turn 0。
	s.mu.Lock()
	s.inflight = 0
	s.mu.Unlock()
	if _, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil, conversationOptions{requestScope: "cont-thread"}); err != nil {
		t.Fatalf("head 归还后重试应成功续接: %v", err)
	}
	if len(turns) != 2 || turns[1] != 1 {
		t.Fatalf("归还的 head 应让重试继续 turn=1（不是冷启动 0）：turns=%v", turns)
	}
}

// 真实入口回归：管理员把账号绑定的出口从 A 改成 B 后，携带旧出口记忆的续接
// 必须强制冷启动——严格绑定的承诺是绝不从旧 IP 外发。
func TestCallGeminiConvRebindForcesColdStart(t *testing.T) {
	resetConvBoundState(t)
	a, b := withTwoProxies(t, true)
	// A 不占满：续接若不走冷启动会在旧出口上真发（桩可见 turn=1）。
	s := getSlot(a)
	s.mu.Lock()
	s.inflight = 0
	s.mu.Unlock()
	acct := addTestAccount(t, "rebind")
	bindAccountProxy(acct, a)
	mc := Models["gemini-3.6-flash"]

	var turns []int
	var proxies []int64
	stubConvTurnRealSlot(t, func(st *convState) (*StreamResult, error) {
		turns = append(turns, st.turn)
		proxies = append(proxies, st.proxyID)
		st.cid = "rebind-conv"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	first := []map[string]interface{}{{"role": "user", "content": "hello"}}
	if _, _, _, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil, conversationOptions{requestScope: "rebind-thread"}); err != nil {
		t.Fatal(err)
	}
	next := append(append([]map[string]interface{}{}, first...),
		map[string]interface{}{"role": "assistant", "content": "answer"},
		map[string]interface{}{"role": "user", "content": "follow"})

	bindAccountProxy(acct, b) // 管理员改绑到 B
	if _, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil, conversationOptions{requestScope: "rebind-thread"}); err != nil {
		t.Fatalf("改绑后应冷启动成功: %v", err)
	}
	if len(turns) != 2 || turns[1] != 0 {
		t.Fatalf("管理员改绑后必须冷启动（turn=0），不能在旧出口上续接 turn=1：turns=%v", turns)
	}
	if proxies[1] != b {
		t.Fatalf("冷启动应带新绑定出口 %d 进会话，得到 %d", b, proxies[1])
	}
}

// convFakeUpstream 把指定 proxyURL 的 stdlibClientCache（限时/不限时两个 key）
// 换成走 rt 的假客户端——streamGenerateConv 的 /app（XSRF）与 StreamGenerate
// 都走 stdlib 代理分支时被它接管。返回该 proxyURL。
func convFakeUpstream(t *testing.T, proxyURL string, rt http.RoundTripper) string {
	t.Helper()
	timeout := rtCfg().RequestTimeout
	for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
		stdlibClientCache.Store(proxyURL+suf, &http.Client{Transport: rt})
	}
	t.Cleanup(func() {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Delete(proxyURL + suf)
		}
	})
	return proxyURL
}

// R2 修复 2 回归：真实 streamGenerateConv 的 slot 记账——
//   - 胜者改占（ok2=true）：胜者 slot 不泄漏、败方不双释放；
//   - 败方重取胜者失败（ok2=false）：败方 slot 不双释放、胜者的占用保持。
//
// 用 claimMu 同步门把仲裁卡在临界区，主线程在期间改状态，跟真实并发首绑同构。
// R3-M3：两个子例共用同一套「并发首绑」场景与哨兵计位——
//   - 账号在库中保持未绑定（ProxyID=0），goroutine 拿 B 只是轮询落空；
//   - PerIPConcurrent=2，slots[a]=2 满载迫使 acquireSlot(0) 必选 B，
//     slots[b]=1 哨兵（借出变 2、单释放回 1、双释放跌 0），
//     slots[0]=1 哨兵（误释放直连槽会跌 0）；
//   - 主线程持 claimMu → goroutine 拿到 B 后阻塞在 claimAccountProxy 的锁上
//     （轮询 slotUsage(b)==2 且 done 未关 = 确定性同步点，不用 sleep），
//     主线程以 bindAccountProxyLocked 模拟并发胜者落库后再放锁。
func TestStreamGenerateConvClaimLoserSlotAccounting(t *testing.T) {
	// 轮询等待 goroutine 拿下 B 的 slot（inflight 1→2）并阻塞在 claimMu 上：
	// 拿到 B 后唯一的锁点就是 claimAccountProxy，done 未关证明它还在临界区前。
	waitBLoaned := func(t *testing.T, b int64, done chan struct{}) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for slotUsage(b).Inflight != 2 {
			select {
			case <-done:
				t.Fatal("goroutine 在拿到 B 的 slot 之前就退出了")
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("goroutine 未在 5s 内拿到 B 的 slot：%+v", slotUsage(b))
			}
			time.Sleep(time.Millisecond)
		}
	}

	t.Run("ok2 winner takes over without leak", func(t *testing.T) {
		a, b := withTwoProxies(t, true)
		rtMu.Lock()
		rtVal.PerIPConcurrent = 2
		rtMu.Unlock()
		setSlotInflight := func(id int64, n int) {
			s := getSlot(id)
			s.mu.Lock()
			s.inflight = n
			s.mu.Unlock()
		}
		setSlotInflight(a, 2) // 满载迫使 acquireSlot(0) 必选 B
		setSlotInflight(b, 1) // 哨兵：借出→2、单释放回 1、双释放跌 0
		setSlotInflight(0, 1) // 哨兵：误释放直连槽会跌 0
		acct := addTestAccount(t, "convwin")
		// 库中保持未绑定：goroutine 先拿到 B，主线程稍后落库胜者 A。

		claimMu.Lock()
		done := make(chan struct{})
		var res *StreamResult
		var err error
		conv := &convState{isLogin: true, accountID: acct,
			cookie: "SAPISID=convwin; SID=x; __Secure-1PSID=convwin", turn: 0, proxyID: 0}
		go func() {
			defer close(done)
			res, err = streamGenerateConv(context.Background(), "hi",
				Models["gemini-3.6-flash"], conv, nil, nil)
		}()
		waitBLoaned(t, b, done)
		// 模拟并发胜者落库，再给胜者 A 腾出 1 个并发位（2→1）：
		// 放锁后败方释放 B、重取 A 必成功（ok2=true）。
		bindAccountProxyLocked(acct, a)
		setSlotInflight(a, 1)
		claimMu.Unlock()
		<-done
		_ = res
		// getXSRF 会真发请求（127.0.0.1:1 不可达）→ 失败，但 slot 必须先正确记账。
		_ = err
		// goroutine 拿走 A 又随 defer release() 归还：哨兵终态必须回到主线程占的 1——
		// 泄漏留 2（旧实现 defer releaseSlot(p.ID) 记的是败方 B）。
		if u := slotUsage(a); u.Inflight != 1 {
			t.Fatalf("胜者 A 不能泄漏 slot（哨兵应回 1）：%+v", u)
		}
		if u := slotUsage(b); u.Inflight != 1 {
			t.Fatalf("败方 B 必须单释放回哨兵 1（双释放会跌 0）：%+v", u)
		}
		if u := slotUsage(0); u.Inflight != 1 {
			t.Fatalf("直连 slot 绝不能被误释放（哨兵应保持 1）：%+v", u)
		}
		// 会话跟库都必须绑到胜者 A。
		if conv.proxyID != a {
			t.Fatalf("会话应就地在胜者 A：%d", conv.proxyID)
		}
		if acc := accountByID(acct); acc == nil || acc.ProxyID != a {
			t.Fatalf("胜者绑定必须落库：%+v", acc)
		}
	})

	t.Run("ok2 false loser never double releases", func(t *testing.T) {
		a, b := withTwoProxies(t, true)
		rtMu.Lock()
		rtVal.PerIPConcurrent = 2
		rtMu.Unlock()
		setSlotInflight := func(id int64, n int) {
			s := getSlot(id)
			s.mu.Lock()
			s.inflight = n
			s.mu.Unlock()
		}
		setSlotInflight(a, 2) // 保持满载：败方重取 A 必然 !ok2
		setSlotInflight(b, 1)
		setSlotInflight(0, 1)
		acct := addTestAccount(t, "convlose")
		// 库中保持未绑定。

		claimMu.Lock()
		done := make(chan struct{})
		var res *StreamResult
		var err error
		conv := &convState{isLogin: true, accountID: acct,
			cookie: "SAPISID=convlose; SID=x; __Secure-1PSID=convlose", turn: 0, proxyID: 0}
		go func() {
			defer close(done)
			res, err = streamGenerateConv(context.Background(), "hi",
				Models["gemini-3.6-flash"], conv, nil, nil)
		}()
		waitBLoaned(t, b, done)
		// 主线程以并发胜者落库 A，保持 A 满载后放锁：败方重取必然 !ok2。
		bindAccountProxyLocked(acct, a)
		claimMu.Unlock()
		<-done

		var rle *RateLimitError
		if !errors.As(err, &rle) || !rle.Bound || rle.ProxyID != a {
			t.Fatalf("败方重取胜者失败应报 Bound 429(出口 %d)：err=%v res=%+v", a, err, res)
		}
		if res == nil || res.AccountID != acct {
			t.Fatalf("失败结果必须带会话账号：%+v", res)
		}
		if u := slotUsage(a); u.Inflight != 2 {
			t.Fatalf("胜者 A 保持占用 2（主线程哨兵，goroutine 没拿过 A）：%+v", u)
		}
		if u := slotUsage(b); u.Inflight != 1 {
			t.Fatalf("败方 B 必须单释放回哨兵 1（双释放会跌 0）：%+v", u)
		}
		if u := slotUsage(0); u.Inflight != 1 {
			t.Fatalf("直连 slot 绝不能被误释放（哨兵应保持 1）：%+v", u)
		}
	})
}

// R2 修复 3 回归：真实 streamGenerateConv + fakeUpstreamServer 的续接行为。
//   - 子例 A：续接轮上游回降级行（inner[42]=3.5 Flash-Lite）→ ModelDowngradedError
//     后 convStore 必须为空（脏 head 绝不归还）、coolingAccounts 登记该号、
//     下次冷启动自动跳过它挑到 acct2；
//   - 子例 B：流中 ctx 取消 → convStore 为空（取消不能归还脏 head）。
func TestConvContinuationUpstreamTouchedAndDowngrade(t *testing.T) {
	// 假上游：/app 出 XSRF，StreamGenerate 按请求轮次决定行为。
	var mu sync.Mutex
	var genCalls int
	var cancelHook func()
	// reachedRead 随 cancelHook 一起设置：挂起路径的 blockingBody 首个 Read 关闭它，
	// 子例 B 据此确定取消发生在已进入响应体读取之后（R3-M3，不用 sleep）。
	var reachedRead chan struct{}
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp := func(status int, body string) *http.Response {
			return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d", status),
				Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: req}
		}
		switch req.URL.Path {
		case "/app":
			return resp(200, fakeAppTokens), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			mu.Lock()
			genCalls++
			n := genCalls
			hook := cancelHook
			reached := reachedRead
			mu.Unlock()
			inner := make([]any, innerSlots)
			if hook != nil {
				// 子例 B：返回挂起的流式响应——Body 的 Read 阻塞到 ctx 取消，
				// 让 readBody 真实走到 ctx.Err() 中断。
				inner[1] = []any{"c_b", "r_1"}
				inner[4] = []any{[]any{"rc_x", []any{"never"}}}
			} else if n == 1 {
				// 子例 A 首轮：正常帧带 cid/rid + 自报 3.5 Flash-Lite（降级）。
				inner[1] = []any{"c_down", "r_1"}
				inner[4] = []any{[]any{"rc_x", []any{"partial"}}}
				inner[39], inner[42], inner[43] = hexFlash36, "3.5 Flash-Lite", true
			} else {
				inner[1] = []any{"c_b", "r_1"}
				inner[4] = []any{[]any{"rc_x", []any{"never"}}}
			}
			ib, _ := json.Marshal(inner)
			line, _ := json.Marshal([]any{[]any{"wrb.fr", "rpc1", string(ib), nil}})
			if hook != nil {
				r := resp(200, "")
				r.Body = &blockingBody{ctx: req.Context(), reachedRead: reached}
				return r, nil
			}
			return resp(200, ")]}'\n"+string(line)+"\n"), nil
		}
		return resp(404, ""), nil
	})
	convFakeUpstream(t, "http://fake-exit.invalid:3128", rt)
	// 代理池指向假出口（同 fakeUpstreamServer）：stdlib 代理分支被接管。
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{{ID: 997, Name: "diag-test", URL: "http://fake-exit.invalid:3128", Enabled: true}}
	proxyMu.Unlock()
	t.Cleanup(func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() })

	resetConversationTestState(t)
	resetDowngrades(t)
	mc := Models["gemini-3.6-flash"] // Upstream="3.6 Flash"，自报 3.5 Flash-Lite 判降级
	rtMu.Lock()
	oldCfg := rtVal
	rtVal.AnonFirst = false
	rtVal.StrictModel = true
	rtVal.StrictModelCooldownMin = 300
	rtVal.RetryAttempts, rtVal.RetryDelaySec = 1, 0
	rtMu.Unlock()
	t.Cleanup(func() { rtMu.Lock(); rtVal = oldCfg; rtMu.Unlock() })

	acct1 := addTestAccount(t, "convacct1")
	acct2 := addTestAccount(t, "convacct2")
	// 两个号都带 __Secure-1PSID，browserAccountFromHeaders/反查不会因 "|" 短路。
	for _, id := range []int64{acct1, acct2} {
		_, _ = getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`,
			fmt.Sprintf("SAPISID=c%d; SID=x; __Secure-1PSID=c%d", id, id), id)
	}

	// ── 子例 A：续接降级 → convStore 空 + 冷却登记 + 冷启动换号 ──
	t.Run("downgrade", func(t *testing.T) {
		// 手动种 head：模拟首轮续接已建立（turn=1 之后），绑 acct1 + 假出口 997。
		// 不依赖首个 callGeminiConv 的真实首轮，专注续接失败路径。
		first := []map[string]interface{}{{"role": "user", "content": "down-q"}}
		scope := convScopeKey(mc, nil, nil, "downgrade-scope")
		keys := convPrefixKeys(first, scope)
		convPut(keys[len(keys)-1], &convState{
			cid: "c_down", rid: "r_1", turn: 1, isLogin: true,
			accountID: acct1, cookie: "SAPISID=c" + fmt.Sprint(acct1) + "; SID=x; __Secure-1PSID=c" + fmt.Sprint(acct1),
			proxyID: 997,
		})
		next := append(append([]map[string]interface{}{}, first...),
			map[string]interface{}{"role": "assistant", "content": "partial"},
			map[string]interface{}{"role": "user", "content": "down-next"})

		_, _, _, err := callGeminiConv(context.Background(), next, mc, nil, nil, nil, nil,
			conversationOptions{requestScope: "downgrade-scope"})
		var mde *ModelDowngradedError
		if !errors.As(err, &mde) || mde.AccountID != acct1 {
			t.Fatalf("续接降级应报 ModelDowngradedError(账号 %d)：err=%v", acct1, err)
		}
		// 脏 head 绝不归还：被消费的 key 不能回到 convStore。
		convMu.Lock()
		storeEmpty := len(convStore) == 0
		convMu.Unlock()
		if !storeEmpty {
			t.Fatal("续接碰过上游后 head 绝不能归还到 convStore")
		}
		// 降级已登记该号该模型的冷却：下次冷启动自动跳过它。
		cooling, _ := coolingAccounts(mc)
		if !cooling[acct1] {
			t.Fatalf("降级必须登记冷却：cooling=%v", cooling)
		}
		// 下一次冷启动（fresh）挑号必须跳过冷却中的 acct1 到 acct2。
		fresh1 := []map[string]interface{}{{"role": "user", "content": "down-fresh"}}
		var pickedID int64
		stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
			pickedID = st.accountID
			st.cid = "fresh-conv"
			st.turn++
			return &StreamResult{Raw: conversationTestRaw(t, "fresh-answer")}, nil
		})
		if _, _, _, err := callGeminiConv(context.Background(), fresh1, mc, nil, nil, nil, nil,
			conversationOptions{requestScope: "downgrade-scope"}); err != nil {
			t.Fatal(err)
		}
		if pickedID != acct2 {
			t.Fatalf("冷启动应跳过冷却号 %d 挑到 %d，实际 %d", acct1, acct2, pickedID)
		}
	})

	// ── 子例 B：流中取消 → convStore 空，脏 head 不归还 ──
	t.Run("cancel mid-stream", func(t *testing.T) {
		first := []map[string]interface{}{{"role": "user", "content": "cancel-q"}}
		scope := convScopeKey(mc, nil, nil, "cancel-scope")
		keys := convPrefixKeys(first, scope)
		convPut(keys[len(keys)-1], &convState{
			cid: "c_cancel", rid: "r_1", turn: 1, isLogin: true,
			accountID: acct2, cookie: "SAPISID=c" + fmt.Sprint(acct2) + "; SID=x; __Secure-1PSID=c" + fmt.Sprint(acct2),
			proxyID: 997,
		})
		next := append(append([]map[string]interface{}{}, first...),
			map[string]interface{}{"role": "assistant", "content": "before"},
			map[string]interface{}{"role": "user", "content": "cancel-next"})

		ctx, cancel := context.WithCancel(context.Background())
		reached := make(chan struct{})
		mu.Lock()
		cancelHook = func() {} // 非 nil 标记走挂起路径（见 blockingBody）
		reachedRead = reached
		mu.Unlock()
		// 确定性证明取消发生在 StreamGenerate 已进入响应体读取之后：blockingBody
		// 首个 Read 关闭 reached，等到了才 cancel——不靠 sleep 猜时机（R3-M3）。
		go func() {
			<-reached
			cancel()
		}()
		_, _, _, err := callGeminiConv(ctx, next, mc, nil, nil, nil, nil,
			conversationOptions{requestScope: "cancel-scope"})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("流中取消应返回 context.Canceled，得到 %v", err)
		}
		// 被消费的 head 绝不能归还：cancel-scope 这一路在 convTake 后必须留空
		//（A 子例冷启动成功落的 head 属于另一 scope，不在此断言范围内）。
		convMu.Lock()
		_, restored := convStore[keys[len(keys)-1]]
		convMu.Unlock()
		if restored {
			t.Fatal("流中取消后脏 head 绝不能归还到 convStore")
		}
		mu.Lock()
		cancelHook = nil
		reachedRead = nil
		mu.Unlock()
	})
}

// R2 修复 4 回归：上游模型降级换号预算严格保持 maxCookieTries=3。
// StrictProxyBinding 放宽的只该是给 Bound 受阻号的池大小预算——5 个健康出口
// 账号依次降级（无 Bound）时，总上游尝试次数必须严格等于 3，不能被
// total+maxCookieTries 放大成外发 5 次。
func TestCallGeminiConvDowngradeBudgetStaysCappedAtThree(t *testing.T) {
	resetConvBoundState(t)
	resetDowngrades(t)
	a, b := withTwoProxies(t, true)
	// 5 个号：前 2 绑 A（此刻空闲），后 3 绑 B（空闲），全部健康可挑。
	s := getSlot(a)
	s.mu.Lock()
	s.inflight = 0
	s.mu.Unlock()
	var ids []int64
	for i := 0; i < 2; i++ {
		id := addTestAccount(t, fmt.Sprintf("dega%d", i))
		bindAccountProxy(id, a)
		ids = append(ids, id)
	}
	for i := 0; i < 3; i++ {
		id := addTestAccount(t, fmt.Sprintf("degb%d", i))
		bindAccountProxy(id, b)
		ids = append(ids, id)
	}
	mc := Models["gemini-3.6-flash"]
	rtMu.Lock()
	rtVal.StrictModel = true
	rtVal.StrictModelCooldownMin = 300
	rtMu.Unlock()

	var calls int
	// 桩真实出口（拿到 slot 成功）后返回降级错——等于上游那一发已发出且报了
	// ModelDowngradedError：每个号一次，上层换号预算到顶必须停。
	stubConvTurnRealSlot(t, func(st *convState) (*StreamResult, error) {
		calls++
		return &StreamResult{AccountID: st.accountID},
			&ModelDowngradedError{Requested: mc.Upstream, Reported: "3.5 Flash-Lite", AccountID: st.accountID}
	})
	first := []map[string]interface{}{{"role": "user", "content": "budget-q"}}
	_, _, _, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil,
		conversationOptions{requestScope: "budget-scope"})
	if err == nil {
		t.Fatal("全部降级必须报错")
	}
	if calls != maxCookieTries {
		t.Fatalf("降级换号预算必须严格是 %d 次上游尝试，实际 %d", maxCookieTries, calls)
	}
}

// streamGenerateConv 绑定持久化：挑到的出口跟会话记的不一样时写库同步——
// 首轮定出口与失效重绑共用这一条路径。
func TestStreamGenerateConvPersistsRebind(t *testing.T) {
	a, b := withTwoProxies(t, true)
	s := getSlot(a)
	s.mu.Lock()
	s.inflight = 0
	s.mu.Unlock()
	acct := addTestAccount(t, "convrebind")
	bindAccountProxy(acct, a)
	conv := &convState{isLogin: true, accountID: acct, proxyID: a, cookie: "SAPISID=x; SID=y", turn: 0}
	// 把 A 禁用：acquireSlot(a) 走轮询到 B，会话应就地重绑并持久化。
	setProxyField(a, func(p *Proxy) { p.Enabled = false })
	// getXSRF 会真发请求（127.0.0.1:1 立即失败）——只验证绑定已同步，
	// 不关心上游结果。
	_, _ = streamGenerateConv(context.Background(), "hi", Models["gemini-3.6-flash"], conv, nil, nil)
	if conv.proxyID != b {
		t.Fatalf("绑定出口失效后应就地换成 %d，得到 %d", b, conv.proxyID)
	}
	if acc := accountByID(acct); acc == nil || acc.ProxyID != b {
		t.Fatalf("重绑必须持久化到账号行：%+v", acc)
	}
}
