package app

import (
	"encoding/json"
	"fmt"
)

// textPartTypes 是 content/output 数组里算纯文本的 part 类型。
var textPartTypes = map[string]bool{"text": true, "input_text": true, "output_text": true}

// mediaPartTypes 是 content/output 数组里算媒体 part 的已知类型，跟
// rejectUnsupported / collectImages 的 case 名单一致。判不准的后果要么把
// 媒体数组拍平丢图（绕过匿名校验），要么把普通 JSON 数据数组误当媒体保留，
// 让 function_call_output 的内容被拍成空串。
var mediaPartTypes = map[string]bool{
	"image_url": true, "input_image": true,
	"video_url": true, "input_video": true,
	"input_audio": true,
}

// hasMediaParts 只认已知媒体 part：数组里出现 image_url / input_image /
// video_url / input_video / input_audio 才算媒体。普通字符串/数字/对象元素
// 与未知 type 都不是媒体，交给各自的序列化/拍平分支处理。
func hasMediaParts(content interface{}) bool {
	parts, ok := content.([]interface{})
	if !ok {
		return false
	}
	for _, c := range parts {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if mediaPartTypes[getStr(cm, "type")] {
			return true
		}
	}
	return false
}

// nonTextParts 判断 content 数组里是否带非纯文本 part（含媒体与未知类型）。
// 仅用于用户 content：未知 part（input_file 等未来类型）也要保留数组形态，
// 免得拍平成空串后悄悄丢数据、还绕过 rejectUnsupported。
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

// allTextParts 判断数组是否全部是 map 形态的纯文本 part（text / input_text /
// output_text）。空数组返回 false —— 空数组按普通数据走序列化，不当纯文本。
func allTextParts(content interface{}) bool {
	parts, ok := content.([]interface{})
	if !ok || len(parts) == 0 {
		return false
	}
	for _, c := range parts {
		cm, ok := c.(map[string]interface{})
		if !ok || !textPartTypes[getStr(cm, "type")] {
			return false
		}
	}
	return true
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
	// appendAssistantMedia 处理含媒体 part 的 assistant 消息：媒体 part 必须保留
	// []interface{} 形态（含文本 part），否则拍平成文本后就从 rejectUnsupported
	// / collectImages 眼前消失。content 已是数组（上一轮保留过媒体）时往上追加；
	// 是字符串时把非空文本包成 text part 一并保留，不丢字也不覆盖已保留的媒体。
	appendAssistantMedia := func(media []interface{}, calls []interface{}) {
		var assistant map[string]interface{}
		if len(messages) > 0 && getStr(messages[len(messages)-1], "role") == "assistant" {
			assistant = messages[len(messages)-1]
		} else {
			assistant = map[string]interface{}{"role": "assistant", "content": ""}
			messages = append(messages, assistant)
		}
		existing, _ := assistant["content"].([]interface{})
		if existing == nil {
			if text := getStr(assistant, "content"); text != "" {
				existing = []interface{}{map[string]interface{}{"type": "text", "text": text}}
			}
		}
		assistant["content"] = append(existing, media...)
		if len(calls) > 0 {
			existingCalls, _ := assistant["tool_calls"].([]interface{})
			assistant["tool_calls"] = append(existingCalls, calls...)
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
			// 媒体 output 保留数组形态交给 rejectUnsupported / collectImages
			// 校验收图；纯文本 part 数组照旧拍平成字符串；其余一切（普通 JSON
			// 数据数组、对象、标量、空数组）json.Marshal 序列化成精确字符串——
			// 误判成媒体会丢掉数据（拍平成 ""→"无输出"，还撞同一份会话指纹）。
			var content interface{}
			switch out := item["output"]; {
			case hasMediaParts(out):
				content = out
			case allTextParts(out):
				content = contentToString(out)
			default:
				if _, isText := out.(string); isText {
					content = out
				} else {
					if encoded, err := json.Marshal(out); err == nil {
						content = string(encoded)
					}
				}
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
			// content 带媒体 part（input_audio 等）时不能按文本拍平：拍平会把媒体
			// 丢掉，绕过 rejectUnsupported。整组保留 []interface{} 让校验与
			// collectImages 看得见。
			if hasMediaParts(item["content"]) {
				parts, _ := item["content"].([]interface{})
				var calls []interface{}
				var media []interface{}
				for _, rawPart := range parts {
					part, ok := rawPart.(map[string]interface{})
					if !ok {
						continue
					}
					if getStr(part, "type") == "function_call" {
						calls = append(calls, toolCall(part))
						continue
					}
					media = append(media, part)
				}
				appendAssistantMedia(media, calls)
				continue
			}
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
		// 图文混排与未知 part 的 content 保留数组形态：拍平会把媒体/未来类型
		// part 悄悄丢掉，让 rejectUnsupported / collectImages 看不到（丢图、漏拦
		// 不支持类型）。纯文本数组照旧拍平成字符串。
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
