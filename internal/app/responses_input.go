package app

import (
	"encoding/json"
	"fmt"
)

// textPartTypes 是 content/output 数组里算纯文本的 part 类型。
var textPartTypes = map[string]bool{"text": true, "input_text": true, "output_text": true}

// nonTextParts 判断 content/output 数组里是否带非纯文本 part（input_image /
// image_url / input_video / video_url / input_audio 等）。这种数组必须保留
// []interface{} 形态交给 rejectUnsupported 与 collectImages：用 contentToString
// 拍平会把媒体悄悄丢掉，绕开匿名校验还把 base64 塞进 prompt 文本。
func nonTextParts(content interface{}) bool {
	parts, ok := content.([]interface{})
	if !ok {
		return false
	}
	for _, c := range parts {
		cm, ok := c.(map[string]interface{})
		if !ok || !textPartTypes[getStr(cm, "type")] {
			return true
		}
	}
	return false
}

// responsesToMessages normalizes output items replayed through Responses input
// into the Chat message shape used by prompt building and conversation matching.
// One assistant turn can be replayed as several call/text items in either order.
func responsesToMessages(req map[string]interface{}) []map[string]interface{} {
	var messages []map[string]interface{}
	if instr := getStr(req, "instructions"); instr != "" {
		messages = append(messages, map[string]interface{}{"role": "system", "content": instr})
	}
	if input, ok := req["input"].(string); ok {
		return append(messages, map[string]interface{}{"role": "user", "content": input})
	}
	input, _ := req["input"].([]interface{})

	// Results normally omit the function name. Scan all calls first so a result
	// can resolve its call_id even if its call appears later in the input array.
	// usedIDs 登记整条回放里出现过的所有 wire ID（含 function_call_output 的
	// call_id），合成的 call_%d 必须避开，不然缺省 ID 的调用会跟显式 ID 撞车。
	callNames := map[string]string{}
	usedIDs := map[string]bool{}
	for _, raw := range input {
		item, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if getStr(item, "type") == "function_call" {
			callNames[responsesCallID(item)] = getStr(item, "name")
		}
		if id := responsesCallID(item); id != "" {
			usedIDs[id] = true
		}
		if getStr(item, "role") == "assistant" {
			parts, _ := item["content"].([]interface{})
			for _, rawPart := range parts {
				part, ok := rawPart.(map[string]interface{})
				if !ok {
					continue
				}
				if getStr(part, "type") == "function_call" {
					callNames[responsesCallID(part)] = getStr(part, "name")
				}
				if id := responsesCallID(part); id != "" {
					usedIDs[id] = true
				}
			}
		}
	}

	callIndex := 0
	toolCall := func(item map[string]interface{}) interface{} {
		id := responsesCallID(item)
		if id == "" {
			for {
				id = fmt.Sprintf("call_%d", callIndex)
				callIndex++
				if !usedIDs[id] {
					break
				}
			}
			usedIDs[id] = true
		}
		args := getStr(item, "arguments")
		if args == "" {
			args = "{}"
			if value := item["arguments"]; value != nil {
				if _, isText := value.(string); !isText {
					if encoded, err := json.Marshal(value); err == nil {
						args = string(encoded)
					}
				}
			}
		}
		return map[string]interface{}{
			"id": id, "type": "function",
			"function": map[string]interface{}{"name": getStr(item, "name"), "arguments": args},
		}
	}
	appendAssistant := func(text string, calls []interface{}) {
		var assistant map[string]interface{}
		if len(messages) > 0 && getStr(messages[len(messages)-1], "role") == "assistant" {
			assistant = messages[len(messages)-1]
		} else {
			assistant = map[string]interface{}{"role": "assistant", "content": ""}
			messages = append(messages, assistant)
		}
		assistant["content"] = getStr(assistant, "content") + text
		if len(calls) > 0 {
			existing, _ := assistant["tool_calls"].([]interface{})
			assistant["tool_calls"] = append(existing, calls...)
		}
	}

	for _, raw := range input {
		if text, ok := raw.(string); ok {
			messages = append(messages, map[string]interface{}{"role": "user", "content": text})
			continue
		}
		item, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		switch getStr(item, "type") {
		case "reasoning":
			continue
		case "function_call":
			appendAssistant("", []interface{}{toolCall(item)})
			continue
		case "function_call_output":
			id, name := getStr(item, "call_id"), getStr(item, "name")
			if name == "" && id != "" {
				name = callNames[id]
			}
			// 含媒体 part 的 output 保留数组形态交给 rejectUnsupported /
			// collectImages 校验收图；纯文本照旧拍平成字符串。
			var content interface{}
			if nonTextParts(item["output"]) {
				content = item["output"]
			} else {
				text := contentToString(item["output"])
				if text == "" && item["output"] != nil {
					if _, isText := item["output"].(string); !isText {
						if encoded, err := json.Marshal(item["output"]); err == nil {
							text = string(encoded)
						}
					}
				}
				content = text
			}
			messages = append(messages, map[string]interface{}{
				"role": "tool", "tool_call_id": id, "name": name, "content": content,
			})
			continue
		case "", "message":
			// Ordinary input and replayed assistant message items are handled below.
		default:
			continue // Unknown output items are not empty user messages.
		}
		role := getStr(item, "role")
		if role == "assistant" {
			text := getStr(item, "content")
			var calls []interface{}
			parts, _ := item["content"].([]interface{})
			for _, rawPart := range parts {
				part, ok := rawPart.(map[string]interface{})
				if !ok {
					continue
				}
				switch getStr(part, "type") {
				case "output_text", "text", "input_text":
					text += getStr(part, "text")
				case "function_call":
					calls = append(calls, toolCall(part))
				}
			}
			appendAssistant(text, calls)
			continue
		}
		if role == "" {
			if _, hasContent := item["content"]; !hasContent {
				continue
			}
			role = "user"
		}
		// 图文混排等含非文本 part 的 content 保留数组形态，让 rejectUnsupported
		// 与 collectImages 生效；纯文本数组照旧拍平成字符串。
		var content interface{}
		if nonTextParts(item["content"]) {
			content = item["content"]
		} else {
			content = contentToString(item["content"])
		}
		messages = append(messages, map[string]interface{}{"role": role, "content": content})
	}
	return messages
}

func responsesCallID(item map[string]interface{}) string {
	if id := getStr(item, "call_id"); id != "" {
		return id
	}
	return getStr(item, "id")
}
