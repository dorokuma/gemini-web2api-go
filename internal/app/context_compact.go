package app

import (
	"fmt"
	"unicode/utf8"
)

// context_compact.go —— context_overflow_mode="compact" 的结构化裁剪。
//
// 只在结构化 messages[] 层裁剪，严禁切扁平字符串（拼好的 prompt 一旦切开，
// tool_calls 围栏、[Assistant] 前缀、tool 结果标签都会被切碎）。
//
// 裁剪规则：
//   - 必保：所有 system 消息、首条 user 任务消息（若其不超预算的 25%）、
//     最后一组完整用户/工具交互、以及 pc.Head/pc.Tail 工具协议（不裁）；
//   - 中间部分按「原子组」整组移除：带 tool_calls 的 assistant 与其紧随的
//     全部 tool 消息绑成一组，绝不拆开（拆开会让客户端回放历史时出现孤儿
//     tool 结果 / 缺结果的 tool_call）；
//   - 先压中间组里单条 >12KB 的 tool 输出（UTF-8 rune 安全边界：头 4KB +
//     尾 4KB + 省略提示）；仍超再从最旧的中间原子组整组移除，并在被移除
//     段位置插一条 [System note: omitted …] 说明（自身计入预算）；
//   - 必保集合仍超限 → fail-closed *PromptTooLongError。

const (
	// compactToolMsgLimit 是中间组单条 tool 输出触发头尾截断的字节阈值。
	compactToolMsgLimit = 12 * 1024
	// compactToolHead/compactToolTail 是截断后保留的头/尾字节数。
	compactToolHead = 4 * 1024
	compactToolTail = 4 * 1024
)

// atomicGroup 是一组不可拆分的消息：要么是单条普通消息，要么是
// 「assistant(tool_calls) + 紧随的全部 tool 消息」的绑定组。
type atomicGroup struct {
	msgs []map[string]interface{}
}

// groupBytes 估算这组消息渲染进 prompt 后的近似字节数（角色标签 + 分隔）。
func (g atomicGroup) groupBytes() int {
	n := 0
	for _, m := range g.msgs {
		n += msgBytes(m) + 2 // "\n\n" 分隔
	}
	return n
}

// msgBytes 估算单条消息渲染后的近似字节数（含角色前缀）。
func msgBytes(m map[string]interface{}) int {
	n := len(contentToString(m["content"]))
	switch getStr(m, "role") {
	case "system":
		n += len("[System instruction]: ")
	case "assistant":
		n += len("[Assistant]: ")
		if tcs, ok := m["tool_calls"].([]interface{}); ok {
			for _, tc := range tcs {
				if tm, ok := tc.(map[string]interface{}); ok {
					fn, _ := tm["function"].(map[string]interface{})
					n += len(getStr(fn, "name")) + len(getStr(fn, "arguments")) + 24
				}
			}
		}
	case "tool":
		n += len("[Tool result]: ")
	}
	return n
}

// buildAtomicGroups 把 messages[] 折成原子组序列。带 tool_calls 的
// assistant 会把紧随其后的全部 tool 消息吸进同一组。
func buildAtomicGroups(messages []map[string]interface{}) []atomicGroup {
	var groups []atomicGroup
	for i := 0; i < len(messages); i++ {
		m := messages[i]
		g := atomicGroup{msgs: []map[string]interface{}{m}}
		if getStr(m, "role") == "assistant" {
			if tcs, ok := m["tool_calls"].([]interface{}); ok && len(tcs) > 0 {
				for i+1 < len(messages) && getStr(messages[i+1], "role") == "tool" {
					i++
					g.msgs = append(g.msgs, messages[i])
				}
			}
		}
		groups = append(groups, g)
	}
	return groups
}

// truncateToolContent 把超长 tool 输出按 UTF-8 rune 安全边界截成头+尾。
// head/tail 是字节数，截点会回退到最近的 rune 边界。
func truncateToolContent(content string, head, tail int) (string, bool) {
	if len(content) <= head+tail {
		return content, false
	}
	h := head
	for h > 0 && !utf8.ValidString(content[:h]) {
		h--
	}
	t := tail
	for t > 0 && !utf8.ValidString(content[len(content)-t:]) {
		t--
	}
	omitted := len(content) - h - t
	if omitted <= 0 {
		return content, false
	}
	return content[:h] +
		fmt.Sprintf("\n[... %d UTF-8 bytes omitted ...]\n", omitted) +
		content[len(content)-t:], true
}

// compactMessages 在结构化 messages[] 层把超长历史裁进附件预算。
// 返回 (裁剪后的 messages, error)。budget 是 max_context_file_bytes；
// inlineOverhead 是必保的 Head/Tail/inline 指令占掉的字节（它们不进
// 附件但占同一总预算）。
//
// 不修改入参切片里的 map（截断 tool 内容时整体替换 map，避免污染调用方
// 还要用的原始 messages——collectImages 等仍持有同一批 map）。
func compactMessages(messages []map[string]interface{}, budget, inlineOverhead int) (
	[]map[string]interface{}, error) {
	if budget <= 0 {
		return messages, nil
	}
	historyBudget := budget - inlineOverhead
	if historyBudget < 0 {
		historyBudget = 0
	}

	groups := buildAtomicGroups(messages)
	if len(groups) == 0 {
		return messages, nil
	}

	// 分组：keep 前缀（system + 首个 user）、最后一组 tail 必保，其余 middle 可裁。
	var keep []atomicGroup
	var middle []atomicGroup
	var tail atomicGroup
	seenFirstUser := false
	firstUserCap := historyBudget / 4
	last := len(groups) - 1

	for i, g := range groups {
		role := getStr(g.msgs[0], "role")
		switch {
		case i == last:
			// 最后一组交互必保（最新的问答/工具调用在里面）。
			tail = g
		case role == "system":
			keep = append(keep, g)
		case !seenFirstUser && (role == "user" || role == "") &&
			g.groupBytes() <= firstUserCap:
			keep = append(keep, g)
			seenFirstUser = true
		default:
			middle = append(middle, g)
		}
	}

	// 第一步：压中间组里单条超长 tool 输出。
	middle = truncateOversizedToolMsgs(middle)

	// 第二步：从旧到新整组移除 middle，直到总字节 <= historyBudget。
	total := groupsBytes(keep) + groupsBytes(middle) + tail.groupBytes()
	var dropped, droppedBytes int
	for total > historyBudget && len(middle) > 0 {
		dropped += len(middle[0].msgs)
		droppedBytes += middle[0].groupBytes()
		middle = middle[1:]
		total = groupsBytes(keep) + groupsBytes(middle) + tail.groupBytes()
	}
	if dropped > 0 {
		note := fmt.Sprintf(
			"[System note: omitted %d earlier messages (%d UTF-8 bytes) to fit the %d-byte upstream attachment limit]",
			dropped, droppedBytes, budget)
		noteMsg := map[string]interface{}{
			"role":    "user",
			"content": note,
		}
		// 说明条插到 keep 前缀之后、幸存 middle 之前：位置与被移除段一致；
		// 必保的最后一组仍留在末尾。
		middle = append([]atomicGroup{{msgs: []map[string]interface{}{noteMsg}}}, middle...)
		total = groupsBytes(keep) + groupsBytes(middle) + tail.groupBytes()
	}
	if total > historyBudget {
		return nil, &PromptTooLongError{
			Bytes: total, Budget: budget, HasCookie: true}
	}

	var out []map[string]interface{}
	for _, g := range keep {
		out = append(out, g.msgs...)
	}
	for _, g := range middle {
		out = append(out, g.msgs...)
	}
	out = append(out, tail.msgs...)
	return out, nil
}

func groupsBytes(gs []atomicGroup) int {
	n := 0
	for _, g := range gs {
		n += g.groupBytes()
	}
	return n
}

// truncateOversizedToolMsgs 对中间组里单条 >compactToolMsgLimit 的 tool
// 消息做头尾截断（整体替换 map，不改入参）。返回新的组序列。
func truncateOversizedToolMsgs(groups []atomicGroup) []atomicGroup {
	var out []atomicGroup
	for _, g := range groups {
		ng := atomicGroup{msgs: make([]map[string]interface{}, len(g.msgs))}
		for i, m := range g.msgs {
			if getStr(m, "role") == "tool" {
				if c := contentToString(m["content"]); len(c) > compactToolMsgLimit {
					if nc, ok := truncateToolContent(c, compactToolHead, compactToolTail); ok {
						cp := map[string]interface{}{}
						for k, v := range m {
							cp[k] = v
						}
						cp["content"] = nc
						ng.msgs[i] = cp
						continue
					}
				}
			}
			ng.msgs[i] = m
		}
		out = append(out, ng)
	}
	return out
}

// responsesContextOverflowCheck 是 handleResponses 流式开流前的确定性
// 超长预检（无副作用）：命中确定会触发 PromptTooLongError 的情形时返回
// 该错误，调用方直接回 400 而不是 WriteHeader(200) 后再吞。
//
// 判定与 streamGenerateOnce 的真实路径同构：
//   - 无启用 cookie 且 len(prompt) > MaxPromptBytes → 必 400；
//   - 有启用 cookie、ContextOverflowMode!="compact"、len(prompt) > MaxPromptBytes，
//     且 planContextFile 规划后 len(inline) > MaxPromptBytes 或
//     （MaxContextFileBytes>0 且 len(history) > MaxContextFileBytes）→ 必 400。
//
// MultiTurn 续接会话不发完整 prompt（只发新消息），不适用本预检。
func responsesContextOverflowCheck(prompt string, pc promptCtx, multiTurn bool) error {
	if multiTurn {
		return nil
	}
	budget := rtCfg().MaxPromptBytes
	if budget <= 0 || len(prompt) <= budget {
		return nil
	}
	if !hasCookie() {
		return &PromptTooLongError{Bytes: len(prompt), Budget: budget}
	}
	if rtCfg().ContextOverflowMode != "compact" {
		history, inline := planContextFile(prompt, pc, budget)
		maxFile := rtCfg().MaxContextFileBytes
		if len(inline) > budget {
			return &PromptTooLongError{Bytes: len(inline), Budget: budget, HasCookie: true}
		}
		if maxFile > 0 && len(history) > maxFile {
			return &PromptTooLongError{Bytes: len(history), Budget: maxFile, HasCookie: true}
		}
	}
	return nil
}

// applyCompactIfNeeded 是 compact 模式接线：仅当 ContextOverflowMode==
// "compact"、prompt 超过 MaxPromptBytes 且有 cookie（走附件路径）时裁剪。
// 返回 (裁剪后的 messages, 是否已裁, error)。不需要裁剪时原样返回。
func applyCompactIfNeeded(messages []map[string]interface{}, prompt string,
	pc promptCtx) ([]map[string]interface{}, bool, error) {
	if rtCfg().ContextOverflowMode != "compact" {
		return messages, false, nil
	}
	budget := rtCfg().MaxPromptBytes
	maxFile := rtCfg().MaxContextFileBytes
	if budget <= 0 || len(prompt) <= budget || maxFile <= 0 || !hasCookie() {
		return messages, false, nil
	}
	// inlineOverhead：pc.Head/pc.Tail 工具协议 + 内联指令占掉的总预算。
	overhead := len(pc.Head) + len(pc.Tail)
	out, err := compactMessages(messages, maxFile, overhead)
	if err != nil {
		return messages, false, err
	}
	return out, true, nil
}
