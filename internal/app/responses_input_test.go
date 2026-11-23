package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func responseInputFixture(t *testing.T, body string) []map[string]interface{} {
	t.Helper()
	var req map[string]interface{}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	return responsesToMessages(req)
}

// Responses replays each call as a top-level output item, before the optional
// text item. They must remain one assistant turn, with every call and result.
func TestResponsesInputStandaloneFunctionCalls(t *testing.T) {
	messages := responseInputFixture(t, `{"instructions":"be brief","input":[
		{"role":"user","content":"check both cities"},
		{"type":"reasoning","summary":[]},
		{"type":"function_call","call_id":"call_paris","name":"weather","arguments":"{\"city\":\"Paris\"}"},
		{"type":"function_call","call_id":"call_rome","name":"weather","arguments":"{\"city\":\"Rome\"}"},
		{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Checking."}]},
		{"type":"function_call_output","call_id":"call_rome","output":"Rome is sunny"},
		{"type":"function_call_output","call_id":"call_paris","output":"Paris is cloudy"}
	]}`)
	if len(messages) != 5 {
		t.Fatalf("want system, user, assistant and two tool results; got %#v", messages)
	}
	if getStr(messages[2], "role") != "assistant" || getStr(messages[2], "content") != "Checking." {
		t.Fatalf("assistant text lost or split: %#v", messages[2])
	}
	calls, ok := messages[2]["tool_calls"].([]interface{})
	if !ok || len(calls) != 2 {
		t.Fatalf("want two Chat-compatible tool calls, got %#v", messages[2]["tool_calls"])
	}
	for i, wantID := range []string{"call_paris", "call_rome"} {
		call := calls[i].(map[string]interface{})
		if getStr(call, "id") != wantID {
			t.Errorf("call %d id = %q, want %q", i, getStr(call, "id"), wantID)
		}
	}
	for i, wantID := range []string{"call_rome", "call_paris"} {
		m := messages[i+3]
		if getStr(m, "role") != "tool" || getStr(m, "name") != "weather" || getStr(m, "tool_call_id") != wantID {
			t.Errorf("tool result identity lost: %#v", m)
		}
	}
	prompt, _ := messagesToPrompt(messages, nil, nil, false)
	for _, want := range []string{"[System instruction]: be brief", "Checking.", `"name": "weather"`, `"city":"Paris"`, `"city":"Rome"`, "[Tool result for weather]: Rome is sunny", "[Tool result for weather]: Paris is cloudy"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("cold-start prompt lost %q: %s", want, prompt)
		}
	}
}

func TestResponsesInputAssistantReplayGrouping(t *testing.T) {
	standalone := responseInputFixture(t, `{"input":[
		{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"},
		{"type":"reasoning","summary":[]},
		{"role":"assistant","content":[{"type":"output_text","text":"Looking "},{"type":"text","text":"now."}]}
	]}`)
	nested := responseInputFixture(t, `{"input":[
		{"role":"assistant","content":[{"type":"output_text","text":"Looking "},{"type":"text","text":"now."},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"}]}
	]}`)
	if len(standalone) != 1 || len(nested) != 1 || !reflect.DeepEqual(standalone, nested) {
		t.Fatalf("equivalent assistant replay should normalize equally: standalone=%#v nested=%#v", standalone, nested)
	}
	if getStr(standalone[0], "content") != "Looking now." {
		t.Fatalf("assistant text blocks lost: %#v", standalone[0])
	}
}

func TestResponsesInputToolNamesAndExtraMessages(t *testing.T) {
	messages := responseInputFixture(t, `{"input":[
		{"type":"function_call_output","call_id":"call_1","output":[{"type":"text","text":"saved"}]},
		{"type":"function_call","call_id":"call_1","name":"write_file","arguments":"{}"},
		{"role":"user","content":[{"type":"input_text","text":"continue"}]},
		{"role":"user","content":"time context"},
		{"role":"developer","content":"environment context"},
		{"type":"reasoning","summary":[]},
		{"type":"unknown_item"}
	]}`)
	if len(messages) != 5 {
		t.Fatalf("must retain all real messages without phantom users: %#v", messages)
	}
	if getStr(messages[0], "name") != "write_file" || getStr(messages[0], "content") != "saved" {
		t.Fatalf("future call name or structured result lost: %#v", messages[0])
	}
	for i, want := range []string{"continue", "time context", "environment context"} {
		if getStr(messages[i+2], "content") != want {
			t.Errorf("extra message %d lost or reordered: %#v", i, messages[i+2])
		}
	}
}

func TestResponsesInputStringAndExplicitToolName(t *testing.T) {
	messages := responseInputFixture(t, `{"instructions":"be helpful","input":"hello"}`)
	want := []map[string]interface{}{{"role": "system", "content": "be helpful"}, {"role": "user", "content": "hello"}}
	if !reflect.DeepEqual(messages, want) {
		t.Fatalf("simple input changed: got %#v, want %#v", messages, want)
	}
	messages = responseInputFixture(t, `{"input":[{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"call_1","name":"explicit_name","output":"ok"}]}`)
	if len(messages) != 2 || getStr(messages[1], "name") != "explicit_name" {
		t.Fatalf("explicit tool result name should be preserved: %#v", messages)
	}
}

// Replay the handler's actual output array, as an Agent does after running
// parallel tools. The continuation must retain one upstream conversation and
// send every new result, even when a synthetic message follows the results.
func TestResponsesHandlerToolReplayContinuesConversation(t *testing.T) {
	resetConversationTestState(t)
	rtMu.Lock()
	rtVal.MultiTurn = true
	rtMu.Unlock()
	var prompts []string
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, prompt string, _ ModelConfig, st *convState) (*StreamResult, error) {
		prompts = append(prompts, prompt)
		turns = append(turns, st.turn)
		st.cid = "handler-conversation"
		st.turn++
		text := "Both results received."
		if len(prompts) == 1 {
			text = "```tool_call\n{\"name\":\"read_file\",\"arguments\":{\"path\":\"one\"}}\n```\n```tool_call\n{\"name\":\"read_file\",\"arguments\":{\"path\":\"two\"}}\n```"
		}
		return &StreamResult{Raw: conversationTestRaw(t, text)}, nil
	})
	firstUser := map[string]interface{}{"role": "user", "content": "original-handler-question"}
	req := map[string]interface{}{
		"model":            "gemini-3.5-flash-lite",
		"instructions":     "stable-handler-system",
		"input":            []interface{}{firstUser},
		"prompt_cache_key": "handler-session",
		"stream":           true,
		"tools": []interface{}{map[string]interface{}{
			"type": "function", "name": "read_file", "parameters": map[string]interface{}{"type": "object"},
		}},
	}
	call := func() map[string]interface{} {
		t.Helper()
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "/v1/responses", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer test-caller")
		w := httptest.NewRecorder()
		handleResponses(w, r)
		if w.Code != 200 {
			t.Fatalf("handler status=%d: %s", w.Code, w.Body.String())
		}
		for _, line := range strings.Split(w.Body.String(), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var event map[string]interface{}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
				t.Fatal(err)
			}
			if getStr(event, "type") == "response.completed" {
				response, _ := event["response"].(map[string]interface{})
				return response
			}
		}
		t.Fatalf("missing successful terminal event: %s", w.Body.String())
		return nil
	}
	first := call()
	output, _ := first["output"].([]interface{})
	if len(output) != 2 {
		t.Fatalf("expected two real handler function calls, got %#v", output)
	}
	replay := append([]interface{}{firstUser}, output...)
	for i, raw := range output {
		item := raw.(map[string]interface{})
		if getStr(item, "type") != "function_call" || getStr(item, "call_id") == "" {
			t.Fatalf("expected function_call identity in actual output: %#v", item)
		}
		replay = append(replay, map[string]interface{}{
			"type": "function_call_output", "call_id": item["call_id"], "output": []string{"result-one", "result-two"}[i],
		})
	}
	replay = append(replay, map[string]interface{}{"role": "user", "content": "synthetic-time-context"})
	req["input"] = replay
	second := call()
	if getStr(second, "status") != "completed" || !reflect.DeepEqual(turns, []int{0, 1}) {
		t.Fatalf("tool replay must continue upstream turn 1, turns=%v response=%#v", turns, second)
	}
	for _, want := range []string{"[Tool result for read_file]: result-one", "[Tool result for read_file]: result-two", "synthetic-time-context"} {
		if !strings.Contains(prompts[1], want) {
			t.Errorf("continuation dropped %q: %s", want, prompts[1])
		}
	}
	for _, old := range []string{"original-handler-question", "stable-handler-system", "Available tools"} {
		if strings.Contains(prompts[1], old) {
			t.Errorf("continuation resent old context %q: %s", old, prompts[1])
		}
	}
}

// 多模态 content 不能被 contentToString 拍平：input_image / input_audio /
// input_video 拍平后就从 rejectUnsupported / collectImages 眼前消失，匿名
// 会直接拿到 200、有 cookie 则悄悄丢图。含非文本 part 的数组必须原样保留。
func TestResponsesInputPreservesMediaParts(t *testing.T) {
	for label, body := range map[string]string{
		"mixed image":    `{"input":[{"role":"user","content":[{"type":"input_text","text":"look"},{"type":"input_image","image_url":"data:image/png;base64,QUJD"}]}]}`,
		"pure image":     `{"input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,QUJD"}]}]}`,
		"audio":          `{"input":[{"role":"user","content":[{"type":"input_audio","data":"AAAA"}]}]}`,
		"video_url":      `{"input":[{"role":"user","content":[{"type":"video_url","video_url":{"url":"https://x/v.mp4"}}]}]}`,
		"unknown future": `{"input":[{"role":"user","content":[{"type":"input_file","file_url":"https://x/f.pdf"}]}]}`,
	} {
		t.Run(label, func(t *testing.T) {
			messages := responseInputFixture(t, body)
			if len(messages) != 1 {
				t.Fatalf("messages=%#v", messages)
			}
			parts, ok := messages[0]["content"].([]interface{})
			if !ok {
				t.Fatalf("media content must stay a []interface{} for rejectUnsupported/collectImages, got %#v", messages[0]["content"])
			}
			if len(parts) == 0 {
				t.Fatalf("media parts lost: %#v", messages[0])
			}
		})
	}
	// 纯文本数组仍拍平成字符串（既有行为与下游不变）。
	text := responseInputFixture(t, `{"input":[{"role":"user","content":[{"type":"input_text","text":"a"},{"type":"text","text":"b"}]}]}`)
	if s, ok := text[0]["content"].(string); !ok || s != "a b" {
		t.Fatalf("pure-text array must still flatten, got %#v", text[0]["content"])
	}
}

// 探针 1A/1B：/v1/responses 的 input_image 匿名要被 rejectUnsupported 拒成
// 400，input_audio 恒拒 400 —— 绝不能拍平后漏成空 prompt 发 200。
func TestResponsesHandlerRejectsUnsupportedMedia(t *testing.T) {
	resetConversationTestState(t)
	rtMu.Lock()
	rtVal.MultiTurn = true
	rtMu.Unlock()
	var upstreamPrompts []string
	stubConversationTestTurn(t, func(_ context.Context, prompt string, _ ModelConfig, st *convState) (*StreamResult, error) {
		upstreamPrompts = append(upstreamPrompts, prompt)
		st.cid = "media-conversation"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "answer")}, nil
	})
	post := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer media-test-caller")
		w := httptest.NewRecorder()
		handleResponses(w, r)
		return w
	}
	// 首轮纯文本成功建会话。
	if w := post(`{"model":"gemini-3.5-flash-lite","input":[{"role":"user","content":"ready"}]}`); w.Code != 200 {
		t.Fatalf("text turn status=%d: %s", w.Code, w.Body.String())
	}
	// 次轮纯 input_image：无 cookie 必须 400，且不能碰到上游桩。
	w := post(`{"model":"gemini-3.5-flash-lite","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,QUJD"}]}]}`)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "image input needs a Google account cookie") {
		t.Fatalf("anonymous input_image must be rejected: %d %s", w.Code, w.Body.String())
	}
	// input_audio 恒拒。
	w = post(`{"model":"gemini-3.5-flash-lite","input":[{"role":"user","content":[{"type":"input_audio","data":"AAAA"}]}]}`)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "audio input not supported") {
		t.Fatalf("input_audio must be rejected: %d %s", w.Code, w.Body.String())
	}
	// 混排里的媒体同样要拦（丢图比报错更糟）。
	w = post(`{"model":"gemini-3.5-flash-lite","input":[{"role":"user","content":[{"type":"input_text","text":"look"},{"type":"input_image","image_url":"data:image/png;base64,QUJD"}]}]}`)
	if w.Code != 400 {
		t.Fatalf("mixed media must be rejected too: %d %s", w.Code, w.Body.String())
	}
	// 三次被拒都不能向上游发过任何空 prompt。
	if len(upstreamPrompts) != 1 {
		t.Fatalf("rejected requests must not reach upstream: %d calls %#v", len(upstreamPrompts), upstreamPrompts)
	}
}

// function_call_output.output 的媒体 part 也要保留供校验（丢 base64 进
// prompt 或丢图都算回归）。纯文本 output 照旧拍平。
func TestResponsesInputFunctionCallOutputMedia(t *testing.T) {
	messages := responseInputFixture(t, `{"input":[
		{"type":"function_call","call_id":"call_1","name":"see","arguments":"{}"},
		{"type":"function_call_output","call_id":"call_1","output":[{"type":"input_image","image_url":"data:image/png;base64,QUJD"}]}
	]}`)
	if len(messages) != 2 {
		t.Fatalf("messages=%#v", messages)
	}
	if _, ok := messages[1]["content"].([]interface{}); !ok {
		t.Fatalf("tool output media must stay []interface{}: %#v", messages[1]["content"])
	}
	// output 含 input_audio 同样保留。
	audio := responseInputFixture(t, `{"input":[
		{"type":"function_call_output","call_id":"call_1","output":[{"type":"input_audio","data":"AAAA"}]}
	]}`)
	if _, ok := audio[0]["content"].([]interface{}); !ok {
		t.Fatalf("audio tool output must stay []interface{}: %#v", audio[0]["content"])
	}
}

// 合成 call_%d 必须避开显式 call_id：预扫描已经登记了 call_1，缺省 ID 的
// function_call 不能也叫 call_1，否则 tool_call_id 撞车、归属错乱。
func TestResponsesInputSyntheticCallIDsAvoidExplicit(t *testing.T) {
	messages := responseInputFixture(t, `{"input":[
		{"type":"function_call","call_id":"call_1","name":"beta","arguments":"{}"},
		{"type":"function_call","name":"alpha","arguments":"{}"},
		{"type":"function_call_output","call_id":"call_1","output":"out"}
	]}`)
	if len(messages) != 2 {
		t.Fatalf("messages=%#v", messages)
	}
	calls, ok := messages[0]["tool_calls"].([]interface{})
	if !ok || len(calls) != 2 {
		t.Fatalf("want two tool_calls, got %#v", messages[0]["tool_calls"])
	}
	id0, id1 := getStr(calls[0].(map[string]interface{}), "id"), getStr(calls[1].(map[string]interface{}), "id")
	if id0 != "call_1" {
		t.Fatalf("explicit call_1 must stay on beta, got %q", id0)
	}
	if id1 == id0 || id1 == "call_1" {
		t.Fatalf("synthesized ID must avoid explicit call_1, got %q", id1)
	}
	// tool 结果要归 beta（显式 call_1），不是归到 alpha 的合成 ID。
	if getStr(messages[1], "tool_call_id") != "call_1" || getStr(messages[1], "name") != "beta" {
		t.Fatalf("function_call_output must resolve to beta via call_1: %#v", messages[1])
	}
	// 嵌套进 assistant content 的 function_call 也参与避让。
	nested := responseInputFixture(t, `{"input":[
		{"role":"assistant","content":[{"type":"function_call","call_id":"call_0","name":"beta","arguments":"{}"}]},
		{"type":"function_call","name":"alpha","arguments":"{}"}
	]}`)
	nCalls := nested[0]["tool_calls"].([]interface{})
	if len(nCalls) != 2 || getStr(nCalls[1].(map[string]interface{}), "id") == "call_0" {
		t.Fatalf("nested explicit call_0 must be avoided by synthesized IDs: %#v", nCalls)
	}
}

// 探针 2B：多轮回放里 function_call_output.output 对象的大整数不能经
// float64 提前舍入 —— 9007199254740993 与 …992 在续接前缀指纹里必须不同。
// 若解码/序列化丢了精度（output 数组被误当媒体保留除外），两条不同历史的
// 前缀会撞同一份指纹导致串话续接；精度保住时对照组命中续接、变异组冷启动。
func TestResponsesInputLargeIntegersKeepPrecision(t *testing.T) {
	resetConversationTestState(t)
	rtMu.Lock()
	rtVal.MultiTurn = true
	rtMu.Unlock()
	var turns []int
	var prompts []string
	stubConversationTestTurn(t, func(_ context.Context, prompt string, _ ModelConfig, st *convState) (*StreamResult, error) {
		prompts = append(prompts, prompt)
		turns = append(turns, st.turn)
		st.cid = "bigint-conversation"
		st.turn++
		text := "ok"
		if len(prompts) == 1 {
			text = "```tool_call\n{\"name\":\"lookup\",\"arguments\":{}}\n```"
		}
		return &StreamResult{Raw: conversationTestRaw(t, text)}, nil
	})
	firstUser := map[string]interface{}{"role": "user", "content": "go"}
	req := map[string]interface{}{
		"model":            "gemini-3.5-flash-lite",
		"prompt_cache_key": "bigint-session",
		"input":            []interface{}{firstUser},
		"tools": []interface{}{map[string]interface{}{
			"type": "function", "name": "lookup", "parameters": map[string]interface{}{"type": "object"},
		}},
	}
	call := func() map[string]interface{} {
		t.Helper()
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "/v1/responses", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer bigint-caller")
		w := httptest.NewRecorder()
		handleResponses(w, r)
		if w.Code != 200 {
			t.Fatalf("status=%d: %s", w.Code, w.Body.String())
		}
		var completed map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &completed); err != nil {
			t.Fatalf("response: %s", w.Body.String())
		}
		return completed
	}
	// 首轮：真实工具往返建会话。回放 handler 输出的 function_call item 原样
	// （带真实 call_id），head=[user go, assistant(tool_calls)]。
	first := call()
	output, _ := first["output"].([]interface{})
	if len(output) == 0 || getStr(output[0].(map[string]interface{}), "type") != "function_call" {
		t.Fatalf("first turn must emit a function_call output item: %#v", first["output"])
	}
	callID := output[0].(map[string]interface{})["call_id"]
	replay := func(id string) []interface{} {
		return []interface{}{firstUser, output[0],
			map[string]interface{}{"type": "function_call_output", "call_id": callID,
				"output": map[string]interface{}{"id": json.Number(id)}},
			map[string]interface{}{"role": "user", "content": "again"}}
	}
	// 对照组次轮：回放首轮 + 真实 function_call + output 大整数 …992 + 新 user ——
	// 与 head 同前缀，必须命中续接（turn 1）。
	req["input"] = replay("9007199254740992")
	call()
	// 变异组第三轮：其余完全相同，仅 output 大整数改为 …993 —— 前缀指纹不同必须
	// 冷启动（turn 0）；若 output 被提前舍入或拍平，两组撞同指纹会错误续接。
	req["input"] = replay("9007199254740993")
	call()
	if len(turns) != 3 || turns[0] != 0 || turns[1] != 1 || turns[2] != 0 {
		t.Fatalf("identical replay must continue, >2^53 mutation must cold-start: turns=%v", turns)
	}
	// output 里的大整数也要以精确字符串进上游 prompt（不许舍成 …992）。
	if !strings.Contains(prompts[2], "9007199254740993") || strings.Contains(prompts[2], "9007199254740992") {
		t.Fatalf("large integer output must stay exact in prompt: %q", prompts[2])
	}
}

func TestResponsesRequestNamespaceIsolation(t *testing.T) {
	request := httptest.NewRequest("POST", "/v1/responses", nil)
	request.Header.Set("Authorization", "Bearer private-test-credential")
	base := conversationRequestScope(request, "session-one", "user-one")
	if len(base) != 64 || strings.Contains(base, "private-test-credential") {
		t.Fatal("request namespace must retain a digest instead of raw credentials")
	}
	if again := conversationRequestScope(request, "session-one", "user-one"); again != base {
		t.Fatal("identical request identity must have a stable scope")
	}
	otherCredential := request.Clone(request.Context())
	otherCredential.Header.Set("Authorization", "Bearer another-test-credential")
	for label, scope := range map[string]string{
		"credential": conversationRequestScope(otherCredential, "session-one", "user-one"),
		"session":    conversationRequestScope(request, "session-two", "user-one"),
		"user":       conversationRequestScope(request, "session-one", "user-two"),
	} {
		if scope == base {
			t.Errorf("different %s must not share a request namespace", label)
		}
	}
}

// Must-Fix 1：req["n"] 经 UseNumber 解成 json.Number 后 n>1 仍要 400 —— 旧断言只认
// float64，两个 OpenAI 入口的 n>1 会静默漏过、让客户端错拿单候选回复。
func TestRejectUnsupportedNAboveOneUseNumber(t *testing.T) {
	for label, path := range map[string]string{
		"chat completions": "/v1/chat/completions",
		"responses":        "/v1/responses",
	} {
		for _, n := range []string{"3", "2.5", "1e20"} {
			t.Run(label+" n="+n, func(t *testing.T) {
				var body string
				if path == "/v1/chat/completions" {
					body = `{"model":"gemini-3.5-flash-lite","n":` + n + `,"messages":[{"role":"user","content":"hi"}]}`
				} else {
					body = `{"model":"gemini-3.5-flash-lite","n":` + n + `,"input":"hi"}`
				}
				r := httptest.NewRequest("POST", path, strings.NewReader(body))
				r.Header.Set("Authorization", "Bearer n-reject-caller")
				w := httptest.NewRecorder()
				if path == "/v1/chat/completions" {
					handleChatCompletions(w, r)
				} else {
					handleResponses(w, r)
				}
				if w.Code != 400 {
					t.Fatalf("n=%s must be rejected before upstream: %d %s", n, w.Code, w.Body.String())
				}
				if !strings.Contains(w.Body.String(), "not supported") {
					t.Fatalf("rejection must explain single-candidate limit: %s", w.Body.String())
				}
			})
		}
	}
	// n=1 与缺省仍放行；Gemini 入口的 float64 candidateCount 同样照拒。
	if err := rejectUnsupported(map[string]interface{}{"n": json.Number("1")}, nil); err != nil {
		t.Fatalf("n=1 must be accepted: %v", err)
	}
	if err := rejectUnsupported(map[string]interface{}{}, nil); err != nil {
		t.Fatalf("missing n must be accepted: %v", err)
	}
	if err := rejectUnsupported(map[string]interface{}{"n": float64(3)}, nil); err == nil {
		t.Fatal("float64 n=3 (Gemini candidateCount path) must still be rejected")
	}
}

// Must-Fix 2A：function_call_output.output 里的普通 JSON 数据数组不是媒体 part，
// 不能被拍平丢字 —— 对象数组与字符串数组都要 json.Marshal 成精确字符串进 prompt，
// 不许变成「无输出」。混排媒体 output 仍保留 []interface{} 交给 rejectUnsupported。
func TestResponsesInputFunctionCallOutputDataArrays(t *testing.T) {
	resetConversationTestState(t)
	rtMu.Lock()
	rtVal.MultiTurn = true
	rtMu.Unlock()
	var prompts []string
	stubConversationTestTurn(t, func(_ context.Context, prompt string, _ ModelConfig, st *convState) (*StreamResult, error) {
		prompts = append(prompts, prompt)
		st.cid = "data-output-conversation"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "ok")}, nil
	})
	post := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer data-output-caller")
		w := httptest.NewRecorder()
		handleResponses(w, r)
		return w
	}
	// 对象数组 output：大整数 id 与字段值必须精确进 prompt。
	if w := post(`{"model":"gemini-3.5-flash-lite","input":[
		{"type":"function_call","call_id":"c1","name":"list","arguments":"{}"},
		{"type":"function_call_output","call_id":"c1","output":[{"id":9007199254740993,"name":"alice"}]},
		{"role":"user","content":"go"}
	]}`); w.Code != 200 {
		t.Fatalf("status=%d: %s", w.Code, w.Body.String())
	}
	// 字符串数组 output 同理。
	if w := post(`{"model":"gemini-3.5-flash-lite","input":[
		{"type":"function_call","call_id":"c2","name":"list","arguments":"{}"},
		{"type":"function_call_output","call_id":"c2","output":["a.go","b.go"]},
		{"role":"user","content":"go"}
	]}`); w.Code != 200 {
		t.Fatalf("status=%d: %s", w.Code, w.Body.String())
	}
	// 数字数组 output 也走序列化，不拍平。
	if w := post(`{"model":"gemini-3.5-flash-lite","input":[
		{"type":"function_call_output","call_id":"c3","output":[1,2,3]},
		{"role":"user","content":"go"}
	]}`); w.Code != 200 {
		t.Fatalf("status=%d: %s", w.Code, w.Body.String())
	}
	if len(prompts) != 3 {
		t.Fatalf("upstream prompts=%v", prompts)
	}
	for i, want := range []string{"9007199254740993", "a.go", "1,2,3"} {
		if !strings.Contains(prompts[i], want) {
			t.Errorf("prompt %d lost exact tool output %q: %s", i, want, prompts[i])
		}
		if strings.Contains(prompts[i], "无输出") {
			t.Errorf("prompt %d must not claim empty tool output: %s", i, prompts[i])
		}
	}
	// 混排媒体 output 仍保留数组形态，input_audio 被 rejectUnsupported 拒成 400。
	if w := post(`{"model":"gemini-3.5-flash-lite","input":[
		{"type":"function_call_output","call_id":"c4","output":[{"type":"output_text","text":"see"},{"type":"input_audio","data":"AAAA"}]},
		{"role":"user","content":"go"}
	]}`); w.Code != 400 || !strings.Contains(w.Body.String(), "audio input not supported") {
		t.Fatalf("mixed media tool output must be rejected: %d %s", w.Code, w.Body.String())
	}
}

// Must-Fix 2B：回放里 function_call_output.output 数据数组改了内容就必须冷启动
// —— 旧实现把数组当媒体保留，contentToString 全拍成空串，两份不同 output 撞同
// 一份指纹串话续接。对照组保持 alpha 必须命中续接。
func TestResponsesInputFunctionCallOutputFingerprint(t *testing.T) {
	resetConversationTestState(t)
	rtMu.Lock()
	rtVal.MultiTurn = true
	rtMu.Unlock()
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		turns = append(turns, st.turn)
		st.cid = "data-fingerprint-conversation"
		st.turn++
		text := "ok"
		if len(turns) == 1 {
			text = "```tool_call\n{\"name\":\"lookup\",\"arguments\":{}}\n```"
		}
		return &StreamResult{Raw: conversationTestRaw(t, text)}, nil
	})
	firstUser := map[string]interface{}{"role": "user", "content": "go"}
	req := map[string]interface{}{
		"model":            "gemini-3.5-flash-lite",
		"prompt_cache_key": "data-session",
		"input":            []interface{}{firstUser},
		"tools": []interface{}{map[string]interface{}{
			"type": "function", "name": "lookup", "parameters": map[string]interface{}{"type": "object"},
		}},
	}
	call := func() map[string]interface{} {
		t.Helper()
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "/v1/responses", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer data-fingerprint-caller")
		w := httptest.NewRecorder()
		handleResponses(w, r)
		if w.Code != 200 {
			t.Fatalf("status=%d: %s", w.Code, w.Body.String())
		}
		var completed map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &completed); err != nil {
			t.Fatalf("response: %s", w.Body.String())
		}
		return completed
	}
	// 首轮建会话，head=[user go, assistant(tool_calls)]。
	first := call()
	output, _ := first["output"].([]interface{})
	if len(output) == 0 || getStr(output[0].(map[string]interface{}), "type") != "function_call" {
		t.Fatalf("first turn must emit a function_call output item: %#v", first["output"])
	}
	callID := output[0].(map[string]interface{})["call_id"]
	replay := func(val string) []interface{} {
		return []interface{}{firstUser, output[0],
			map[string]interface{}{"type": "function_call_output", "call_id": callID,
				"output": []interface{}{map[string]interface{}{"id": 1, "val": val}}},
			map[string]interface{}{"role": "user", "content": "again"}}
	}
	// 对照组次轮：回放首轮 + 真实 function_call + output alpha + 新 user —— 与
	// head 同前缀，必须命中续接（turn 1）。
	req["input"] = replay("alpha")
	call()
	// 变异组第三轮：其余完全相同，仅 output 数据数组内容改 beta —— 数据数组若被
	// 拍平成空串，两组指纹相同会错误续接；序列化保住内容则前缀不同必须冷启动。
	req["input"] = replay("beta")
	call()
	if len(turns) != 3 || turns[0] != 0 || turns[1] != 1 || turns[2] != 0 {
		t.Fatalf("identical replay must continue, changed data output must cold-start: turns=%v", turns)
	}
}

// Must-Fix 3：assistant 消息 content 里回放的媒体 part（input_audio）不能被
// 拍平丢掉 —— 拍平后 rejectUnsupported 看不到它，匿名会拿到 200 上游悄悄丢图。
// 纯文本 assistant 回放照旧拍平成字符串。
func TestResponsesInputAssistantMediaRejects(t *testing.T) {
	resetConversationTestState(t)
	rtMu.Lock()
	rtVal.MultiTurn = true
	rtMu.Unlock()
	var upstream int
	stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		upstream++
		st.cid = "assistant-media-conversation"
		st.turn++
		return &StreamResult{Raw: conversationTestRaw(t, "ok")}, nil
	})
	post := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer assistant-media-caller")
		w := httptest.NewRecorder()
		handleResponses(w, r)
		return w
	}
	// assistant content 数组含 input_audio：恒拒，且不能碰到上游。
	if w := post(`{"model":"gemini-3.5-flash-lite","input":[
		{"role":"assistant","content":[{"type":"output_text","text":"listening"},{"type":"input_audio","data":"AAAA"}]},
		{"role":"user","content":"go"}
	]}`); w.Code != 400 || !strings.Contains(w.Body.String(), "audio input not supported") {
		t.Fatalf("assistant input_audio must be rejected: %d %s", w.Code, w.Body.String())
	}
	if upstream != 0 {
		t.Fatalf("rejected request must not reach upstream: %d calls", upstream)
	}
	// 纯文本 assistant content 数组照旧拍平，走正常续接路径。
	messages := responseInputFixture(t, `{"input":[
		{"role":"assistant","content":[{"type":"output_text","text":"hello "},{"type":"text","text":"there"}]}
	]}`)
	if len(messages) != 1 || getStr(messages[0], "content") != "hello there" {
		t.Fatalf("pure-text assistant replay must still flatten: %#v", messages)
	}
}

// Should-Fix 5：contentToString 与 textPartTypes 对 output_text 口径一致 ——
// 非 assistant 消息与 function_call_output 里的 output_text 不再静默丢字。
func TestResponsesInputOutputTextPartsFlatten(t *testing.T) {
	messages := responseInputFixture(t, `{"input":[
		{"role":"user","content":[{"type":"output_text","text":"hello"}]}
	]}`)
	if len(messages) != 1 || getStr(messages[0], "content") != "hello" {
		t.Fatalf("user output_text must produce text: %#v", messages)
	}
	messages = responseInputFixture(t, `{"input":[
		{"type":"function_call_output","call_id":"c1","output":[{"type":"input_text","text":"a"},{"type":"output_text","text":"b"}]}
	]}`)
	if len(messages) != 1 || getStr(messages[0], "content") != "a b" {
		t.Fatalf("mixed text parts in tool output must flatten in order: %#v", messages)
	}
}
