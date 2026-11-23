package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type conversationOptions struct {
	requestScope     string
	normalizeToolIDs bool // Gemini permits clients to omit call/result IDs.
}

// conversationRequestScope separates local continuation state. prompt_cache_key
// is a client-supplied conversation namespace, not a Google prompt-cache control.
// Only a digest of credentials and caller identifiers is retained in memory.
func conversationRequestScope(r *http.Request, promptCacheKey, user string) string {
	credential := ""
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		credential = strings.TrimSpace(h[7:])
	}
	for _, candidate := range []string{r.Header.Get("x-api-key"), r.Header.Get("x-goog-api-key"), r.URL.Query().Get("key")} {
		if credential == "" {
			credential = strings.TrimSpace(candidate)
		}
	}
	path := r.URL.Path
	for _, action := range []string{":generateContent", ":streamGenerateContent"} {
		path = strings.TrimSuffix(path, action)
	}
	b, _ := json.Marshal([]string{path, credential, promptCacheKey, user})
	return hashStr(string(b))
}

// Decode numbers without float64 rounding: distinct large integer arguments
// must remain distinct. Invalid argument strings are compared byte-for-byte.
func canonicalToolArguments(s string) string {
	if s == "" {
		return "{}"
	}
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var v interface{}
	if d.Decode(&v) != nil {
		return s
	}
	var extra interface{}
	if d.Decode(&extra) != io.EOF {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return s
	}
	return string(b)
}

func conversationToolCalls(value interface{}) []ToolCall {
	b, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var calls []ToolCall
	if unmarshalJSONUseNumber(b, &calls) != nil {
		return nil
	}
	for i := range calls {
		if calls[i].Type == "" {
			calls[i].Type = "function"
		}
		calls[i].Function.Arguments = canonicalToolArguments(calls[i].Function.Arguments)
	}
	return calls
}

func conversationCallMaps(calls []ToolCall) []interface{} {
	var result []interface{}
	for _, call := range calls {
		result = append(result, map[string]interface{}{
			"id": call.ID, "type": call.Type,
			"function": map[string]interface{}{"name": call.Function.Name, "arguments": call.Function.Arguments},
		})
	}
	return result
}

// Responses may replay one assistant turn as separate function_call/message
// items; the responsesToMessages adapter already coalesces them back into one
// message, and Gemini model turns arrive inside a single message. Chat replays
// keep each message boundary as-is: merging two adjacent assistant messages
// here would give ["first","second"] the same fingerprint as ["firstsecond"],
// so consecutive assistant messages stay separate.
func normalizeConversationMessages(messages []map[string]interface{}) []map[string]interface{} {
	var result []map[string]interface{}
	names := map[string]string{}
	for _, source := range messages {
		role := getStr(source, "role")
		if role == "" {
			role = "user"
		}
		m := map[string]interface{}{"role": role, "content": contentToString(source["content"])}
		for _, field := range []string{"name", "tool_call_id"} {
			if v := getStr(source, field); v != "" {
				m[field] = v
			}
		}
		calls := conversationToolCalls(source["tool_calls"])
		if len(calls) > 0 {
			m["tool_calls"] = conversationCallMaps(calls)
			for _, call := range calls {
				if call.ID != "" {
					names[call.ID] = call.Function.Name
				}
			}
		}
		if role == "tool" && getStr(m, "name") == "" {
			if id := getStr(m, "tool_call_id"); id != "" {
				if name := names[id]; name != "" {
					m["name"] = name
				}
			}
		}
		result = append(result, m)
	}
	return result
}

// Model/tool policy changes require a fresh upstream conversation because the
// retained history contains the original tool instructions and model state.
// TaskMode 也必须进 scope：同名同上游的 Chat 与 Task 模式走不同的网页通道
// （inner 槽位、平台通知都不一样），撞同一路服务端会话会拿到错通道的续接。
func convScopeKey(mc ModelConfig, tools []map[string]interface{}, choice interface{}, requestScope string) string {
	if len(tools) == 0 {
		tools = nil
	}
	mode, forced := parseToolChoice(choice)
	b, _ := json.Marshal(struct {
		Version              int
		Caller, Model        string
		Mode, Thinking, Tool int
		Account              int64
		Tools                []map[string]interface{}
		ToolMode, ForcedTool string
		Task                 bool
		Upstream             string
	}{1, requestScope, mc.HexID, mc.Mode, mc.thinkWire(), mc.Tool, mc.PinAccountID, tools, mode, forced, mc.TaskMode, mc.Upstream})
	return hashStr(string(b))
}

// Incremental hashing is linear in history size, even with many new messages.
// JSON frames preserve message boundaries even when text contains separators.
func convPrefixKeys(messages []map[string]interface{}, scope string, normalizeToolIDs ...bool) []string {
	h := sha256.New()
	h.Write([]byte(scope + "\n"))
	var keys []string
	messages = normalizeConversationMessages(messages)
	if len(normalizeToolIDs) > 0 && normalizeToolIDs[0] {
		messages = conversationWithoutWireIDs(messages)
	}
	for _, m := range messages {
		b, _ := json.Marshal(m)
		h.Write(b)
		h.Write([]byte{'\n'})
		keys = append(keys, hex.EncodeToString(h.Sum(nil)))
	}
	return keys
}

// Gemini functionCall/functionResponse IDs are optional. Match their logical
// positions for fingerprinting only; preserve actual IDs in prompts/responses.
// Explicit result IDs still determine the association when results are reordered.
func conversationWithoutWireIDs(messages []map[string]interface{}) []map[string]interface{} {
	ids := map[string]string{}
	names := map[string][]string{}
	used := map[string]bool{}
	callIndex := 0
	for _, m := range messages {
		calls := conversationToolCalls(m["tool_calls"])
		for i := range calls {
			// 归一化位置 ID 用 "#N" 命名空间，跟客户端送来的原始 wireID
			// （可能是 "call_0" 甚至 "#0"）永远不会撞名。
			id := fmt.Sprintf("#%d", callIndex)
			callIndex++
			if calls[i].ID != "" {
				ids[calls[i].ID] = id
			}
			names[calls[i].Function.Name] = append(names[calls[i].Function.Name], id)
			calls[i].ID = id
		}
		if len(calls) > 0 {
			m["tool_calls"] = conversationCallMaps(calls)
		}
		if getStr(m, "role") != "tool" {
			continue
		}
		wireID := getStr(m, "tool_call_id")
		id := ids[wireID]
		if id == "" && wireID == "" {
			for _, candidate := range names[getStr(m, "name")] {
				if !used[candidate] {
					id = candidate
					break
				}
			}
		}
		if id != "" {
			m["tool_call_id"] = id
			used[id] = true
		} else if wireID != "" {
			// 匹配不到任何调用的原始 wireID 也不能原样保留：原样留下时，
			// 客户端随手编的 "#0"/"call_0" 会跟归一化位置 ID 撞出同一份
			// 指纹。重写成 "raw:" 前缀占另一个命名空间，且不参与 used 去重。
			m["tool_call_id"] = "raw:" + wireID
		}
	}
	return messages
}

func conversationMessagesWithReply(messages []map[string]interface{}, text string, calls []ToolCall) []map[string]interface{} {
	reply := map[string]interface{}{"role": "assistant", "content": text}
	if len(calls) > 0 {
		reply["tool_calls"] = conversationCallMaps(calls)
	}
	full := append(append([]map[string]interface{}{}, messages...), reply)
	return normalizeConversationMessages(full)
}
