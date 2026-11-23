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

// 带工具调用历史的请求翻成 prompt 后，函数声明、上一轮的调用和工具结果都得在里面，
// 并且跟 OpenAI 那边同一套 ```tool_call``` 围栏写法。
func TestGeminiAPIToMessagesFunctionCalling(t *testing.T) {
	body := `{
	  "systemInstruction": {"parts": [{"text": "be brief"}]},
	  "contents": [
	    {"role": "user", "parts": [{"text": "weather in Paris?"}]},
	    {"role": "model", "parts": [{"functionCall": {"name": "get_weather", "args": {"city": "Paris"}}}]},
	    {"role": "user", "parts": [{"functionResponse": {"name": "get_weather", "response": {"result": "18C sunny"}}}]}
	  ],
	  "tools": [{"functionDeclarations": [
	    {"name": "get_weather", "description": "Get weather", "parameters": {"type": "OBJECT", "properties": {"city": {"type": "STRING"}}}},
	    {"name": "get_time", "description": "Get time"}
	  ]}],
	  "toolConfig": {"functionCallingConfig": {"mode": "ANY", "allowedFunctionNames": ["get_weather"]}}
	}`
	var req geminiAPIRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	messages, tools, choice, err := geminiAPIToMessages(&req)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 1 {
		t.Fatalf("allowedFunctionNames should filter tools, got %d", len(tools))
	}
	if m, _ := choice.(map[string]interface{}); m == nil {
		t.Fatalf("ANY + one allowed name should force that function, got %v", choice)
	}
	prompt, _ := messagesToPrompt(messages, tools, choice, false)
	for _, want := range []string{
		"[System instruction]: be brief",
		`You MUST call the tool "get_weather"`,
		"weather in Paris?",
		"```tool_call\n{\"name\": \"get_weather\", \"arguments\": {\"city\":\"Paris\"}}\n```",
		"[Tool result for get_weather]: 18C sunny",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt missing %q\n---\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "get_time") {
		t.Error("filtered-out function leaked into the prompt")
	}
}

func TestGeminiAPIToMessagesModes(t *testing.T) {
	for mode, want := range map[string]string{"NONE": "none", "ANY": "required", "AUTO": "auto", "": "auto"} {
		req := geminiAPIRequest{}
		req.ToolConfig.FunctionCallingConfig.Mode = mode
		_, _, choice, _ := geminiAPIToMessages(&req)
		if choice != want {
			t.Errorf("mode %q -> %v, want %s", mode, choice, want)
		}
	}
}

func TestGeminiAPIToMessagesMedia(t *testing.T) {
	body := `{"contents":[{"role":"user","parts":[
	  {"text":"what is this"},
	  {"inlineData":{"mimeType":"image/png","data":"AAAA"}},
	  {"fileData":{"mimeType":"video/mp4","fileUri":"https://x/v.mp4"}}]}]}`
	var req geminiAPIRequest
	_ = json.Unmarshal([]byte(body), &req)
	messages, _, _, err := geminiAPIToMessages(&req)
	if err != nil {
		t.Fatal(err)
	}
	parts := messages[0]["content"].([]interface{})
	if len(parts) != 3 || parts[1].(map[string]interface{})["type"] != "image_url" ||
		parts[2].(map[string]interface{})["type"] != "video_url" {
		t.Fatalf("media parts = %v", parts)
	}

	for _, bad := range []string{
		`{"contents":[{"parts":[{"inlineData":{"mimeType":"application/pdf","data":"AAAA"}}]}]}`,
		`{"contents":[{"parts":[{"fileData":{"mimeType":"image/png","fileUri":"files/abc"}}]}]}`,
	} {
		var r geminiAPIRequest
		_ = json.Unmarshal([]byte(bad), &r)
		if _, _, _, err := geminiAPIToMessages(&r); err == nil {
			t.Errorf("should reject: %s", bad)
		}
	}
}

func stubGeminiCall(t *testing.T, text string, calls []ToolCall, res *StreamResult, err error) *geminiCall {
	t.Helper()
	var seen geminiCall
	saved := runGeminiCall
	runGeminiCall = func(g *geminiCall, onDelta, onReasoning func(string)) (string, []ToolCall, *StreamResult, error) {
		seen = *g
		return text, calls, res, err
	}
	t.Cleanup(func() { runGeminiCall = saved })
	return &seen
}

func TestGenerateContentResponse(t *testing.T) {
	calls := []ToolCall{{ID: "call_1", Type: "function", Function: ToolCallFunction{Name: "get_weather", Arguments: `{"city":"Paris"}`}}}
	stubGeminiCall(t, "let me check", calls, &StreamResult{Reasoning: "thinking..."}, nil)

	body := `{"contents":[{"role":"user","parts":[{"text":"hi"}]}],
	  "tools":[{"functionDeclarations":[{"name":"get_weather"}]}],
	  "generationConfig":{"thinkingConfig":{"includeThoughts":true},"temperature":0.2}}`
	w := httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST", "/v1beta/models/gemini-3.6-flash:generateContent", strings.NewReader(body)))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var out struct {
		Candidates []struct {
			Content struct {
				Role  string                   `json:"role"`
				Parts []map[string]interface{} `json:"parts"`
			} `json:"content"`
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
		UsageMetadata map[string]int `json:"usageMetadata"`
		ModelVersion  string         `json:"modelVersion"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	c := out.Candidates[0]
	if c.Content.Role != "model" || c.FinishReason != "STOP" || out.ModelVersion != "gemini-3.6-flash" {
		t.Fatalf("bad envelope: %s", w.Body)
	}
	if len(c.Content.Parts) != 3 || c.Content.Parts[0]["thought"] != true || c.Content.Parts[1]["text"] != "let me check" {
		t.Fatalf("parts = %v", c.Content.Parts)
	}
	fc := c.Content.Parts[2]["functionCall"].(map[string]interface{})
	if fc["name"] != "get_weather" || fc["args"].(map[string]interface{})["city"] != "Paris" {
		t.Fatalf("functionCall = %v", fc)
	}
	if out.UsageMetadata["thoughtsTokenCount"] == 0 ||
		out.UsageMetadata["totalTokenCount"] != out.UsageMetadata["promptTokenCount"]+
			out.UsageMetadata["candidatesTokenCount"]+out.UsageMetadata["thoughtsTokenCount"] {
		t.Fatalf("usage = %v", out.UsageMetadata)
	}
}

// 没要 includeThoughts 时不回思考摘要；媒体产物走 inlineData，不再拼成 data URL 文本。
func TestGenerateContentHidesThoughtsAndReturnsInlineData(t *testing.T) {
	res := &StreamResult{Reasoning: "hidden", Artifacts: []MediaArtifact{{Mime: "image/png", Data: []byte("xyz")}}}
	stubGeminiCall(t, "![image](data:image/png;base64,eHl6)", nil, res, nil)
	w := httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST", "/v1beta/models/gemini-3.6-flash:generateContent",
		strings.NewReader(`{"contents":[{"parts":[{"text":"draw"}]}]}`)))
	s := w.Body.String()
	if w.Code != 200 || strings.Contains(s, "hidden") || strings.Contains(s, "data:image") ||
		!strings.Contains(s, `"inlineData":{"data":"eHl6","mimeType":"image/png"}`) {
		t.Fatalf("%d %s", w.Code, s)
	}
}

func TestGenerateContentErrors(t *testing.T) {
	stubGeminiCall(t, "", nil, nil, &ModelDowngradedError{Requested: "3.1 Pro", Reported: "3.5 Flash-Lite"})
	w := httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST", "/v1beta/models/gemini-3.6-flash:generateContent",
		strings.NewReader(`{"contents":[{"parts":[{"text":"hi"}]}]}`)))
	if w.Code != 429 || !strings.Contains(w.Body.String(), `"status":"RESOURCE_EXHAUSTED"`) {
		t.Fatalf("downgrade: %d %s", w.Code, w.Body)
	}

	w = httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST", "/v1beta/models/no-such-model:generateContent",
		strings.NewReader(`{"contents":[{"parts":[{"text":"hi"}]}]}`)))
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"status":"INVALID_ARGUMENT"`) {
		t.Fatalf("unknown model: %d %s", w.Code, w.Body)
	}

	w = httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST", "/v1beta/models/gemini-3.6-flash:embedContent", strings.NewReader(`{}`)))
	if w.Code != 404 {
		t.Fatalf("unsupported action: %d", w.Code)
	}
}

func TestGeminiModelsAndCountTokens(t *testing.T) {
	w := httptest.NewRecorder()
	handleGeminiModels(w, httptest.NewRequest("GET", "/v1beta/models", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"name":"models/gemini-3.6-flash"`) {
		t.Fatalf("list: %d %s", w.Code, w.Body)
	}
	w = httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("GET", "/v1beta/models/gemini-3.6-flash", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "streamGenerateContent") {
		t.Fatalf("get: %d %s", w.Code, w.Body)
	}
	w = httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST", "/v1beta/models/gemini-3.6-flash:countTokens",
		strings.NewReader(`{"contents":[{"parts":[{"text":"hello world"}]}]}`)))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"totalTokens":`) {
		t.Fatalf("countTokens: %d %s", w.Code, w.Body)
	}
}

// stubGeminiStream 模拟上游边出边推：先推思考，再分几段推正文（含工具围栏）。
func stubGeminiStream(t *testing.T, reasoning string, deltas []string, err error) {
	t.Helper()
	saved := runGeminiCall
	runGeminiCall = func(g *geminiCall, onDelta, onReasoning func(string)) (string, []ToolCall, *StreamResult, error) {
		res := &StreamResult{Reasoning: reasoning}
		if onReasoning != nil && reasoning != "" {
			onReasoning(reasoning)
			res.EmittedReasoning = reasoning
		}
		full := ""
		for _, d := range deltas {
			onDelta(d)
			full += d
		}
		res.Emitted = full
		if err != nil {
			return "", nil, res, err
		}
		text, calls := full, []ToolCall(nil)
		if len(g.tools) > 0 {
			text, calls = parseToolCalls(full)
		}
		return text, calls, res, nil
	}
	t.Cleanup(func() { runGeminiCall = saved })
}

func sseChunks(t *testing.T, body string) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	for _, ev := range strings.Split(body, "\r\n\r\n") {
		ev = strings.TrimSpace(ev)
		if ev == "" {
			continue
		}
		if !strings.HasPrefix(ev, "data: ") {
			t.Fatalf("bad SSE event %q", ev)
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(ev[6:]), &m); err != nil {
			t.Fatalf("bad chunk %q: %v", ev, err)
		}
		out = append(out, m)
	}
	return out
}

func chunkParts(c map[string]interface{}) []interface{} {
	cand := c["candidates"].([]interface{})[0].(map[string]interface{})
	return cand["content"].(map[string]interface{})["parts"].([]interface{})
}

func TestStreamGenerateContentSSEWithFunctionCall(t *testing.T) {
	stubGeminiStream(t, "hmm", []string{"Checking ", "now.\n```tool", "_call\n{\"name\": \"get_weather\", \"arguments\": {\"city\": \"Paris\"}}\n```"}, nil)
	body := `{"contents":[{"parts":[{"text":"weather?"}]}],
	  "tools":[{"functionDeclarations":[{"name":"get_weather"}]}],
	  "generationConfig":{"thinkingConfig":{"includeThoughts":true}}}`
	w := httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST",
		"/v1beta/models/gemini-3.6-flash:streamGenerateContent?alt=sse", strings.NewReader(body)))
	if w.Code != 200 || w.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("%d %v", w.Code, w.Header())
	}
	if strings.Contains(w.Body.String(), "tool_call") {
		t.Fatalf("fence leaked to the client: %s", w.Body)
	}
	chunks := sseChunks(t, w.Body.String())
	first := chunkParts(chunks[0])[0].(map[string]interface{})
	if first["thought"] != true || first["text"] != "hmm" {
		t.Fatalf("first chunk should be the thought: %v", first)
	}
	var text string
	for _, c := range chunks {
		for _, p := range chunkParts(c) {
			pm := p.(map[string]interface{})
			if pm["thought"] == nil {
				if s, ok := pm["text"].(string); ok {
					text += s
				}
			}
		}
	}
	if strings.TrimSpace(text) != "Checking now." {
		t.Fatalf("streamed text = %q", text)
	}
	last := chunks[len(chunks)-1]
	cand := last["candidates"].([]interface{})[0].(map[string]interface{})
	if cand["finishReason"] != "STOP" || last["usageMetadata"] == nil {
		t.Fatalf("last chunk = %v", last)
	}
	var fc map[string]interface{}
	for _, p := range chunkParts(last) {
		if m, ok := p.(map[string]interface{})["functionCall"].(map[string]interface{}); ok {
			fc = m
		}
	}
	if fc == nil || fc["name"] != "get_weather" {
		t.Fatalf("functionCall missing from last chunk: %v", last)
	}
}

// 不带 alt=sse 时是一个 JSON 数组；不要 includeThoughts 时思考不出现。
func TestStreamGenerateContentJSONArray(t *testing.T) {
	stubGeminiStream(t, "secret", []string{"Hel", "lo"}, nil)
	w := httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST", "/v1beta/models/gemini-3.6-flash:streamGenerateContent",
		strings.NewReader(`{"contents":[{"parts":[{"text":"hi"}]}]}`)))
	var arr []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &arr); err != nil {
		t.Fatalf("not a JSON array: %v\n%s", err, w.Body)
	}
	if len(arr) != 3 || strings.Contains(w.Body.String(), "secret") {
		t.Fatalf("chunks = %s", w.Body)
	}
}

func TestStreamGenerateContentErrors(t *testing.T) {
	// 开流前出错：正常的 HTTP 状态码 + Google 错误体。
	stubGeminiStream(t, "", nil, &ModelDowngradedError{Requested: "3.1 Pro", Reported: "3.5 Flash-Lite"})
	w := httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST", "/v1beta/models/gemini-3.6-flash:streamGenerateContent?alt=sse",
		strings.NewReader(`{"contents":[{"parts":[{"text":"hi"}]}]}`)))
	if w.Code != 429 || !strings.Contains(w.Body.String(), "RESOURCE_EXHAUSTED") {
		t.Fatalf("before start: %d %s", w.Code, w.Body)
	}
	// 开流后出错：状态码已经是 200，错误作为最后一块发出。
	stubGeminiStream(t, "", []string{"partial"}, errMsg("boom"))
	w = httptest.NewRecorder()
	handleGeminiModelItem(w, httptest.NewRequest("POST", "/v1beta/models/gemini-3.6-flash:streamGenerateContent?alt=sse",
		strings.NewReader(`{"contents":[{"parts":[{"text":"hi"}]}]}`)))
	chunks := sseChunks(t, w.Body.String())
	if w.Code != 200 || len(chunks) != 2 || chunks[1]["error"] == nil {
		t.Fatalf("after start: %d %s", w.Code, w.Body)
	}
}

// google-genai 3.x 默认打 /v1/models/{model}:action，要跟 /v1beta 一样处理；
// 不带冒号的 /v1/models/{id} 不归 Gemini 接口，照旧 404。
func TestV1GeminiAction(t *testing.T) {
	stubGeminiCall(t, "hi", nil, &StreamResult{}, nil)
	body := `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`
	w := httptest.NewRecorder()
	handleV1GeminiAction(w, httptest.NewRequest("POST", "/v1/models/gemini-3.6-flash:generateContent", strings.NewReader(body)))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"candidates"`) {
		t.Fatalf("generateContent: %d %s", w.Code, w.Body)
	}
	w = httptest.NewRecorder()
	handleV1GeminiAction(w, httptest.NewRequest("GET", "/v1/models/gemini-3.6-flash", nil))
	if w.Code != 404 {
		t.Fatalf("不带冒号应 404: %d %s", w.Code, w.Body)
	}
}

// 探针 2A：generateContent 入口的 functionCall.args 大整数不能经 float64
// 提前舍入。多轮续接前缀里，9007199254740992 与 …993 在 canonicalToolArguments
// 里是不同参数：入口若先解成 float64 就都成 …992，变异历史会撞同一份前缀
// 指纹错误续接。对照组回放相同历史必须命中续接，变异组必须冷启动。
// 利用 convTake「仅命中才删 head」的语义：先发变异组（必 miss，不消耗首轮
// 留下的 head），再发对照组（必 hit 首轮 head）——若顺序反了，对照组先把
// head 消费掉，变异组即使撞指纹也必 miss，测试就假绿。
func TestGeminiHandlerLargeIntArgumentsKeepDistinctFingerprints(t *testing.T) {
	resetConversationTestState(t)
	rtMu.Lock()
	rtVal.MultiTurn = true
	rtMu.Unlock()
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, _ string, _ ModelConfig, st *convState) (*StreamResult, error) {
		turns = append(turns, st.turn)
		st.cid = "bigint-gemini-conversation"
		st.turn++
		text := "ok"
		if len(turns) == 1 {
			// 首轮回 tool_call 围栏，存 head 的逻辑回复是 assistant(tool_calls)。
			text = "```tool_call\n{\"name\":\"lookup\",\"arguments\":{\"id\":9007199254740992}}\n```"
		}
		return &StreamResult{Raw: conversationTestRaw(t, text)}, nil
	})
	tools := `,"tools":[{"functionDeclarations":[{"name":"lookup","parameters":{"type":"object"}}]}]`
	post := func(body string) map[string]interface{} {
		t.Helper()
		r := httptest.NewRequest("POST", "/v1beta/models/gemini-3.5-flash-lite:generateContent", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer gemini-bigint")
		w := httptest.NewRecorder()
		handleGeminiModelItem(w, r)
		if w.Code != 200 {
			t.Fatalf("status=%d: %s", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("response: %s", w.Body.String())
		}
		return resp
	}
	// 首轮：带 tools 的 user 请求，stub 回围栏，head=[user, assistant(tool_calls)]。
	first := post(`{"contents":[{"role":"user","parts":[{"text":"look it up"}]}]` + tools + `}`)
	// 从响应里取真实的 functionCall.id（客户端回放会带上它）。
	cands, _ := first["candidates"].([]interface{})
	if len(cands) == 0 {
		t.Fatalf("first response must have candidates: %v", first)
	}
	parts, _ := cands[0].(map[string]interface{})["content"].(map[string]interface{})["parts"].([]interface{})
	var fcID string
	for _, p := range parts {
		if fc, ok := p.(map[string]interface{})["functionCall"].(map[string]interface{}); ok {
			fcID = getStr(fc, "id")
		}
	}
	if fcID == "" {
		t.Fatalf("first turn must emit a functionCall with an id: %v", first)
	}
	// 变异组次轮（先发）：其余与首轮相同，仅 args.id 改为 …993 —— 必须 miss
	// （turn 0）且不消耗首轮 head。若入口把 args 解成 float64，…993 与首轮存的
	// …992 撞同一份指纹，这里就会错误续接（turn 1）露出假阳。
	post(`{"contents":[` +
		`{"role":"user","parts":[{"text":"look it up"}]},` +
		`{"role":"model","parts":[{"functionCall":{"id":"` + fcID + `","name":"lookup","args":{"id":9007199254740993}}}]},` +
		`{"role":"user","parts":[{"functionResponse":{"id":"` + fcID + `","name":"lookup","response":{"result":"r1"}}}]},` +
		`{"role":"user","parts":[{"text":"again"}]}` +
		`]` + tools + `}`)
	// 对照组第三轮：回放首轮 + 含同一份 …992 functionCall（带真实 id）与
	// functionResponse 的 model/user 轮 + 新 user —— 首轮 head 还在（变异组
	// miss 没消费它），必须命中续接（turn 1）。
	post(`{"contents":[` +
		`{"role":"user","parts":[{"text":"look it up"}]},` +
		`{"role":"model","parts":[{"functionCall":{"id":"` + fcID + `","name":"lookup","args":{"id":9007199254740992}}}]},` +
		`{"role":"user","parts":[{"functionResponse":{"id":"` + fcID + `","name":"lookup","response":{"result":"r1"}}}]},` +
		`{"role":"user","parts":[{"text":"again"}]}` +
		`]` + tools + `}`)
	if len(turns) != 3 || turns[0] != 0 || turns[1] != 0 || turns[2] != 1 {
		t.Fatalf("args mutation must cold-start without consuming head, identical replay must continue: turns=%v", turns)
	}
}

// 出站 functionCall.args 也要用大整数精确解码（callParts）：解析 tool_call
// 围栏后回给客户端的 args 不能把 9007199254740993 舍成 …992。
func TestGeminiCallPartsKeepLargeIntPrecision(t *testing.T) {
	calls := []ToolCall{{ID: "call_1", Type: "function",
		Function: ToolCallFunction{Name: "lookup", Arguments: `{"id":9007199254740993}`}}}
	parts := callParts(calls)
	if len(parts) != 1 {
		t.Fatalf("parts=%v", parts)
	}
	fc := parts[0].(map[string]interface{})["functionCall"].(map[string]interface{})
	args, _ := fc["args"].(map[string]interface{})
	switch v := args["id"].(type) {
	case json.Number:
		if v.String() != "9007199254740993" {
			t.Fatalf("args.id precision lost: %v", v)
		}
	case float64:
		t.Fatalf("args.id decoded as float64 (precision lost): %v", v)
	default:
		t.Fatalf("unexpected args.id type %T: %v", v, v)
	}
	// 序列化出去必须是原始十进制串，不是舍入值。
	b, _ := json.Marshal(parts)
	if !strings.Contains(string(b), "9007199254740993") || strings.Contains(string(b), "9007199254740992") {
		t.Fatalf("serialized args must carry the exact integer: %s", b)
	}
}

// Task-mode pre-bunking regression via the stable HTTP entrypoint.
//
// Chat-mode prompts pre-bunk the platform's own "built-in tools 0 remaining"
// notice so the model keeps emitting ```tool_call``` blocks. Task mode takes a
// separate web Task channel where that notice never appears, so the pre-bunking
// paragraph must be skipped there; a stray "the platform may warn you" line in
// a task prompt would invent a warning that does not exist. This test drives
// the public /v1beta/models/{model}:generateContent handler (not the internal
// prompt builder) so the assertion fails at the HTTP boundary on a baseline
// that lacks the task-mode skip.
func TestGeminiAPIToolPromptTailSkipsPlatformNoticeForTaskMode(t *testing.T) {
	// Task models only resolve with a signed-in cookie (modelNeedsLogin);
	// withPoolCookie inserts one pool account so all six models resolve.
	withPoolCookie(t)

	const body = `{"contents":[{"role":"user","parts":[{"text":"list files then read README"}]}],
	  "tools":[{"functionDeclarations":[{"name":"run_command","description":"run a shell command"}]}]}`

	// Anchors every tool prompt tail must keep (format contract, both modes).
	anchors := []string{
		"this is the ONLY execution channel",
		"Act, do not explain",
		"Do not repeat a finished step",
		"emit the tool_call block now",
	}
	// Chat-only platform-notice pre-bunking; must be absent in task mode.
	immunity := []string{
		"Heads-up — platform notice",
		"0 remaining",
		"summarize now",
		"tool limit was reached",
	}

	models := []struct {
		name     string
		taskMode bool
	}{
		{"gemini-4-carbon", false},
		{"gemini-4-neon", false},
		{"gemini-4-hydrogen", false},
		{"gemini-4-carbon-task", true},
		{"gemini-4-neon-task", true},
		{"gemini-4-hydrogen-task", true},
	}

	for _, m := range models {
		seen := stubGeminiCall(t, "ok", nil, &StreamResult{}, nil)
		w := httptest.NewRecorder()
		handleGeminiModelItem(w, httptest.NewRequest("POST",
			"/v1beta/models/"+m.name+":generateContent", strings.NewReader(body)))
		if w.Code != 200 {
			t.Fatalf("%s: status %d: %s", m.name, w.Code, w.Body)
		}
		prompt := seen.prompt
		if prompt == "" {
			t.Fatalf("%s: upstream prompt not captured", m.name)
		}
		for _, want := range anchors {
			if !strings.Contains(prompt, want) {
				t.Errorf("%s: prompt missing tool anchor %q\n---\n%s", m.name, want, prompt)
			}
		}
		if m.taskMode {
			for _, banned := range immunity {
				if strings.Contains(prompt, banned) {
					t.Errorf("%s: task-mode prompt must not contain platform notice %q\n---\n%s",
						m.name, banned, prompt)
				}
			}
		} else {
			for _, want := range immunity {
				if !strings.Contains(prompt, want) {
					t.Errorf("%s: chat-mode prompt missing platform notice %q\n---\n%s",
						m.name, want, prompt)
				}
			}
		}
	}
}

// Gemini clients may omit function-call IDs when replaying contents. Exercise
// the real generate/stream handlers across three turns so regenerated adapter
// IDs cannot silently force each new tool result into a fresh conversation.
func TestGeminiHandlerContinuesToolHistoryWithoutCallIDs(t *testing.T) {
	resetConversationTestState(t)
	rtMu.Lock()
	rtVal.MultiTurn = true
	rtMu.Unlock()
	var prompts []string
	var turns []int
	stubConversationTestTurn(t, func(_ context.Context, prompt string, _ ModelConfig, st *convState) (*StreamResult, error) {
		prompts = append(prompts, prompt)
		turns = append(turns, st.turn)
		st.cid = "gemini-handler-conversation"
		st.turn++
		text := "Third turn complete."
		switch len(prompts) {
		case 1:
			text = "```tool_call\n{\"name\":\"read_file\",\"arguments\":{\"path\":\"one\"}}\n```\n```tool_call\n{\"name\":\"read_file\",\"arguments\":{\"path\":\"two\"}}\n```"
		case 2:
			text = "Both Gemini tool results received."
		}
		return &StreamResult{Raw: conversationTestRaw(t, text)}, nil
	})
	firstUser := map[string]interface{}{"role": "user", "parts": []interface{}{map[string]interface{}{"text": "original-gemini-question"}}}
	contents := []interface{}{firstUser}
	req := map[string]interface{}{
		"systemInstruction": map[string]interface{}{"parts": []interface{}{map[string]interface{}{"text": "stable-gemini-system"}}},
		"tools": []interface{}{map[string]interface{}{"functionDeclarations": []interface{}{
			map[string]interface{}{"name": "read_file", "parameters": map[string]interface{}{"type": "object"}},
		}}},
	}
	call := func(action string) []interface{} {
		t.Helper()
		req["contents"] = contents
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		path := "/v1beta/models/gemini-3.5-flash-lite:" + action
		if action == "streamGenerateContent" {
			path += "?alt=sse"
		}
		r := httptest.NewRequest("POST", path, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer gemini-test-caller")
		w := httptest.NewRecorder()
		handleGeminiModelItem(w, r)
		if w.Code != 200 {
			t.Fatalf("%s handler status=%d: %s", action, w.Code, w.Body.String())
		}
		var chunks []map[string]interface{}
		if action == "streamGenerateContent" {
			chunks = sseChunks(t, w.Body.String())
		} else {
			var chunk map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &chunk); err != nil {
				t.Fatal(err)
			}
			chunks = append(chunks, chunk)
		}
		var parts []interface{}
		for _, chunk := range chunks {
			parts = append(parts, chunkParts(chunk)...)
		}
		return parts
	}
	firstParts := call("generateContent")
	if len(firstParts) != 2 {
		t.Fatalf("want two actual Gemini function calls, got %#v", firstParts)
	}
	var results []interface{}
	for i, raw := range firstParts {
		part := raw.(map[string]interface{})
		fc, ok := part["functionCall"].(map[string]interface{})
		if !ok || getStr(fc, "name") != "read_file" {
			t.Fatalf("missing actual Gemini functionCall: %#v", part)
		}
		// SDKs commonly replay only name/args, although our response also offers id.
		delete(fc, "id")
		results = append(results, map[string]interface{}{"functionResponse": map[string]interface{}{
			"name": fc["name"], "response": map[string]interface{}{"result": []string{"gemini-result-one", "gemini-result-two"}[i]},
		}})
	}
	contents = append(contents,
		map[string]interface{}{"role": "model", "parts": firstParts},
		map[string]interface{}{"role": "user", "parts": results},
	)
	secondParts := call("streamGenerateContent")
	contents = append(contents,
		map[string]interface{}{"role": "model", "parts": secondParts},
		map[string]interface{}{"role": "user", "parts": []interface{}{map[string]interface{}{"text": "final-gemini-followup"}}},
	)
	call("generateContent")
	if !reflect.DeepEqual(turns, []int{0, 1, 2}) {
		t.Fatalf("ID-less tool replay and action changes must retain one upstream conversation, turns=%v", turns)
	}
	for _, want := range []string{"gemini-result-one", "gemini-result-two"} {
		if !strings.Contains(prompts[1], want) {
			t.Errorf("second turn dropped %q: %s", want, prompts[1])
		}
	}
	if !strings.Contains(prompts[2], "final-gemini-followup") {
		t.Errorf("third turn dropped final user message: %s", prompts[2])
	}
	for i := 1; i < len(prompts); i++ {
		for _, old := range []string{"original-gemini-question", "stable-gemini-system", "Available tools"} {
			if strings.Contains(prompts[i], old) {
				t.Errorf("turn %d resent old context %q: %s", i+1, old, prompts[i])
			}
		}
	}
}
