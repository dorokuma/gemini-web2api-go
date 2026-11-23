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
//   - 必保：前导 system 消息、首条 user 任务消息（无论大小都锚定，
//     绝不把后面的短 user 倒序提到它前面）、中间 system（原位保留，
//     不前置不重排）、最后一组完整用户/工具交互、以及 pc.Head/pc.Tail
//     工具协议（不裁）；
//   - 中间部分按「原子组」整组移除：带 tool_calls 的 assistant 与其紧随的
//     全部 tool 消息绑成一组，绝不拆开（拆开会让客户端回放历史时出现孤儿
//     tool 结果 / 缺结果的 tool_call）；
//   - 先压中间组里单条 >12KB 的 tool 输出（UTF-8 rune 安全边界：头 4KB +
//     尾 4KB + 省略提示）；仍超再从最旧的中间可丢组整组移除，并在首个
//     被删位置原位插一条 [System note: omitted …] 说明（自身计入预算），
//     严格保持幸存消息的原始先后时序；
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

// msgBytes 估算单条消息渲染后的**真实**字节数：tool 消息按 formatToolResult
// 的真实输出长度算（含 [Tool result for name]: 模板与 ✅/❌ 文案），
// assistant 的 tool_calls 按 ```tool_call 围栏真实长度算——低估会让
// compactMessages 误判达标、重拼后仍超 maxFile 触发 400 误杀。
func msgBytes(m map[string]interface{}) int {
	switch getStr(m, "role") {
	case "system":
		return len("[System instruction]: ") + len(contentToString(m["content"]))
	case "assistant":
		return len("[Assistant]: ") + len(contentToString(m["content"])) +
			toolCallFencesBytes(m["tool_calls"])
	case "tool":
		return len(formatToolResult(getStr(m, "name"), contentToString(m["content"])))
	default:
		return len(contentToString(m["content"]))
	}
}

// toolCallFencesBytes 按 buildPromptParts 的 ```tool_call 围栏模板算 assistant
// 消息里 tool_calls 的真实渲染字节（含正文与围栏间的 "\n" 与围栏间分隔）。
func toolCallFencesBytes(tcsRaw interface{}) int {
	tcs, ok := tcsRaw.([]interface{})
	if !ok || len(tcs) == 0 {
		return 0
	}
	n := 1 // 正文与第一个围栏间的 "\n"
	first := true
	for _, tc := range tcs {
		tm, ok := tc.(map[string]interface{})
		if !ok {
			continue
		}
		fn, _ := tm["function"].(map[string]interface{})
		name := getStr(fn, "name")
		args := getStr(fn, "arguments")
		if args == "" {
			args = "{}"
		}
		if !first {
			n++
		}
		first = false
		n += len(fmt.Sprintf(
			"```tool_call\n{\"name\": \"%s\", \"arguments\": %s}\n```",
			name, args))
	}
	if first {
		return 0 // 一个合法 tool_call 都没有
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
// head/tail 是字节数；截点最多回退/前进 3 字节对齐到 rune 边界（O(1)），
// 非法 UTF-8 内容不会退化成 O(N²) 扫前缀、也不会把整段清成 0。
func truncateToolContent(content string, head, tail int) (string, bool) {
	if len(content) <= head+tail {
		return content, false
	}
	h := utf8BoundaryDown(content, head)
	// tailStart 是尾部保留段的起点（从尾部向前取 tail 字节后对齐到 rune 边界）。
	tailStart := utf8BoundaryUp(content, len(content)-tail)
	omitted := tailStart - h
	if omitted <= 0 {
		return content, false
	}
	return content[:h] +
		fmt.Sprintf("\n[... %d UTF-8 bytes omitted ...]\n", omitted) +
		content[tailStart:], true
}

// utf8BoundaryDown 返回 <= pos 的最大 rune 边界（最多回退 3 字节）。
func utf8BoundaryDown(s string, pos int) int {
	if pos >= len(s) {
		return len(s)
	}
	for i := 0; i < 4 && pos-i > 0; i++ {
		if utf8.RuneStart(s[pos-i]) {
			return pos - i
		}
	}
	if pos-4 > 0 {
		return pos - 4
	}
	return 0
}

// utf8BoundaryUp 返回 >= pos 的最小 rune 边界（最多前进 3 字节）。
func utf8BoundaryUp(s string, pos int) int {
	if pos <= 0 {
		return 0
	}
	if pos >= len(s) {
		return len(s)
	}
	for i := 0; i < 4 && pos+i < len(s); i++ {
		if utf8.RuneStart(s[pos+i]) {
			return pos + i
		}
	}
	return len(s)
}

// compactMessages 在结构化 messages[] 层把超长历史裁进附件预算。
// 返回 (裁剪后的 messages, error)。budget 是 max_context_file_bytes；
// inlineOverhead 是必保的 Head/Tail/inline 指令占掉的字节（仅当工具协议
// 实际留在 history 里时才计入——见 applyCompactIfNeeded 的判定）。
//
// 不修改入参切片里的 map（截断 tool 内容时整体替换 map，避免污染调用方
// 还要用的原始 messages——collectImages 等仍持有同一批 map）。
// 不重排消息：必保项原位标记，可丢组按序淘汰，省略说明插在被删段原位。
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

	pinned := pinMustKeepGroups(groups)

	// 第一步：压非 pinned 组里单条超长 tool 输出。
	for i, g := range groups {
		if !pinned[i] {
			groups[i] = truncateOversizedToolMsgs([]atomicGroup{g})[0]
		}
	}

	dropped, droppedMsgs, droppedBytes, firstDropped :=
		dropOldestDroppable(groups, pinned, historyBudget, budget)

	if droppedMsgs > 0 && firstDropped >= 0 {
		noteMsg := map[string]interface{}{"role": "user",
			"content": omittedNote(droppedMsgs, droppedBytes, budget)}
		// 省略说明插到第一个被删组的原位：严格保持幸存消息原始时序。
		groups[firstDropped] = atomicGroup{msgs: []map[string]interface{}{noteMsg}}
		dropped[firstDropped] = false
	}

	var out []map[string]interface{}
	for i, g := range groups {
		if pinned[i] || !dropped[i] {
			out = append(out, g.msgs...)
		}
	}
	// 终检以真实渲染长度为准（含已插入的 noteMsg）：msgBytes 对 tool /
	// tool_calls 已按真实模板算，此处再按 buildPromptParts 口径兜底。
	if total := renderedHistoryBytes(out); total > historyBudget {
		return nil, &PromptTooLongError{
			Bytes: total, Budget: budget, HasCookie: true}
	}
	return out, nil
}

// pinMustKeepGroups 原位标记必保组：前导 system + 首条 user（无论大小都锚定）
// + 中间 system + 最后一组。绝不把后续短 user 倒序提前、不把中间 system 前置。
func pinMustKeepGroups(groups []atomicGroup) []bool {
	pinned := make([]bool, len(groups))
	seenFirstUser := false
	last := len(groups) - 1
	for i, g := range groups {
		role := getStr(g.msgs[0], "role")
		switch {
		case i == last:
			pinned[i] = true // 最后一组交互必保
		case role == "system":
			pinned[i] = true // 前导与中间 system 都原位保留
		case !seenFirstUser && (role == "user" || role == ""):
			pinned[i] = true
			seenFirstUser = true
		}
	}
	return pinned
}

// omittedNote 渲染省略说明（丢组循环的预算记账与最终插入同一模板）。
func omittedNote(droppedMsgs, droppedBytes, budget int) string {
	return fmt.Sprintf(
		"[System note: omitted %d earlier messages (%d UTF-8 bytes) to fit the %d-byte upstream attachment limit]",
		droppedMsgs, droppedBytes, budget)
}

// dropOldestDroppable 从最旧的非 pinned 组开始整组淘汰，直到「幸存组渲染字节
// + 将产生的 note 字节」≤ historyBudget 或无可丢组。返回 dropped 标记、累计
// 被删消息数/字节数与首个被删组下标（-1 = 没丢）。note 字节在循环内同步
// 计入预算——否则「还有组可丢但 total+note 超预算」会假 400，也可能真超 maxFile。
func dropOldestDroppable(groups []atomicGroup, pinned []bool, historyBudget, budget int) (
	dropped []bool, droppedMsgs, droppedBytes, firstDropped int) {
	dropped = make([]bool, len(groups))
	firstDropped = -1
	noteBytes := func() int {
		if droppedMsgs == 0 {
			return 0
		}
		return len(omittedNote(droppedMsgs, droppedBytes, budget)) + 2
	}
	render := func() int {
		n := 0
		for i, g := range groups {
			if !dropped[i] {
				n += g.groupBytes()
			}
		}
		return n
	}
	for render()+noteBytes() > historyBudget {
		progress := false
		for i, g := range groups {
			if !pinned[i] && !dropped[i] {
				dropped[i] = true
				droppedMsgs += len(g.msgs)
				droppedBytes += g.groupBytes()
				if firstDropped < 0 {
					firstDropped = i
				}
				progress = true
				break
			}
		}
		if !progress {
			break
		}
	}
	return dropped, droppedMsgs, droppedBytes, firstDropped
}

// renderedHistoryBytes 是 compactMessages 终检用的真实渲染长度：
// 与 buildPromptParts 的消息体部分同口径（不含 Head/Tail 工具协议）。
func renderedHistoryBytes(messages []map[string]interface{}) int {
	n := 0
	for _, m := range messages {
		n += msgBytes(m) + 2
	}
	return n
}

// truncateOversizedToolMsgs 对给定组里单条 >compactToolMsgLimit 的 tool
// 消息做头尾截断（整体替换 map，不改入参）。返回新的组序列。
// 多模态 content 是 []interface{} 数组时保留非文本 part、只截文本 part。
func truncateOversizedToolMsgs(groups []atomicGroup) []atomicGroup {
	var out []atomicGroup
	for _, g := range groups {
		ng := atomicGroup{msgs: make([]map[string]interface{}, len(g.msgs))}
		for i, m := range g.msgs {
			ng.msgs[i] = truncateToolMsgContent(m)
		}
		out = append(out, ng)
	}
	return out
}

// truncateToolMsgContent 对单条 tool 消息做截断：超限则返回替换后的副本
// （不改入参 map），否则原样返回。非 tool 消息原样返回。
func truncateToolMsgContent(m map[string]interface{}) map[string]interface{} {
	if getStr(m, "role") != "tool" {
		return m
	}
	switch c := m["content"].(type) {
	case string:
		if len(c) <= compactToolMsgLimit {
			return m
		}
		if nc, ok := truncateToolContent(c, compactToolHead, compactToolTail); ok {
			return withReplacedField(m, "content", nc)
		}
		return m
	case []interface{}:
		if np, changed := truncateMultimodalParts(c); changed {
			return withReplacedField(m, "content", np)
		}
		return m
	default:
		return m
	}
}

// withReplacedField 浅拷贝 map 并替换一个字段。
func withReplacedField(m map[string]interface{}, key string, v interface{}) map[string]interface{} {
	cp := map[string]interface{}{}
	for k, val := range m {
		cp[k] = val
	}
	cp[key] = v
	return cp
}

// truncateMultimodalParts 截多模态 []interface{} tool content 里的超长文本
// part（保留非文本 part 与未超限文本原样）。返回 (新数组, 是否有改动)。
func truncateMultimodalParts(c []interface{}) ([]interface{}, bool) {
	total := 0
	for _, p := range c {
		if pm, ok := p.(map[string]interface{}); ok {
			total += len(getStr(pm, "text"))
		}
	}
	if total <= compactToolMsgLimit {
		return nil, false
	}
	var np []interface{}
	changed := false
	for _, p := range c {
		pm, ok := p.(map[string]interface{})
		if !ok {
			np = append(np, p)
			continue
		}
		txt := getStr(pm, "text")
		if txt == "" || len(txt) <= compactToolMsgLimit {
			np = append(np, p)
			continue
		}
		if nc, ok2 := truncateToolContent(txt, compactToolHead, compactToolTail); ok2 {
			np = append(np, withReplacedField(pm, "text", nc))
			changed = true
			continue
		}
		np = append(np, p)
	}
	return np, changed
}

// responsesContextOverflowCheck 是 handleResponses 流式开流前的确定性
// 超长预检（无副作用）：命中确定会触发 PromptTooLongError 的情形时返回
// 该错误，调用方直接回 400 而不是 WriteHeader(200) 后再吞。
//
// 判定与 streamGenerateOnce 的真实路径同构（compact 模式同样终检——
// 裁完后 inline/history 仍超限照样拦）：
//   - 无可用登录态（hasCookie 为假或本次属 anonFirstEligible 匿名路径）
//     且 len(prompt) > MaxPromptBytes → 必 400；
//   - 有登录态且 len(prompt) > MaxPromptBytes：按 planContextFile 规划
//     inline > MaxPromptBytes 或（MaxContextFileBytes>0 且 history 超）→ 必 400。
//
// MultiTurn 续接会话不发完整 prompt（只发新消息），不适用本预检。
func responsesContextOverflowCheck(prompt string, pc promptCtx, multiTurn bool,
	anonEligible ...bool) error {
	if multiTurn {
		return nil
	}
	budget := rtCfg().MaxPromptBytes
	if budget <= 0 || len(prompt) <= budget {
		return nil
	}
	anon := len(anonEligible) > 0 && anonEligible[0]
	if anon || !hasCookie() {
		return &PromptTooLongError{Bytes: len(prompt), Budget: budget}
	}
	history, inline := planContextFile(prompt, pc, budget)
	maxFile := rtCfg().MaxContextFileBytes
	if len(inline) > budget {
		return &PromptTooLongError{Bytes: len(inline), Budget: budget, HasCookie: true}
	}
	if maxFile > 0 && len(history) > maxFile {
		return &PromptTooLongError{Bytes: len(history), Budget: maxFile, HasCookie: true}
	}
	return nil
}

// applyCompactIfNeeded 是 compact 模式接线：仅当 ContextOverflowMode==
// "compact"、prompt 超 MaxPromptBytes、maxFile>0 且有 cookie（走附件路径）、
// 且 planContextFile 规划出的 history 真实超过 maxFile 时才裁剪。
// 不超限时 100% 原样返回（绝不误改写、不误扣 Head+Tail）。
// 返回 (裁剪后的 messages, 是否已裁, error)。
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
	// 先按真实规划判 history 是否真超 maxFile：不超就原样返回。
	history, _ := planContextFile(prompt, pc, budget)
	if len(history) <= maxFile {
		return messages, false, nil
	}
	// inlineOverhead 仅当工具协议实际留在 history 里时才计入：
	// planContextFile 已把 Head+Tail 剥进 inline（stripToolProtocol ok）时
	// 它们不占附件预算；剥离失败（Head+Tail 留在 history）才计入。
	overhead := 0
	if _, ok := stripToolProtocol(prompt, pc); !ok {
		overhead = len(pc.Head) + len(pc.Tail)
	}
	out, err := compactMessages(messages, maxFile, overhead)
	if err != nil {
		return messages, false, err
	}
	return out, true, nil
}
