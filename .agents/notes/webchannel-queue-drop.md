# [pattern:webchannel-queue-drop] WebChannel 退出时排队帧仍会丢

## 问题描述

`doWebChannelRequest` 里 `raw` 只在**主循环从 `events` channel 取出事件后**才累计
（`internal/app/webchannel.go` 主循环 `ev.Line != ""` 分支 `raw.WriteString(ev.Line)`）。
下行 goroutine 把帧塞进 `events` 队列之后，主循环若正阻塞在 `post` / `sendPrompt`、
或者因 `ackTimer`/其它返回路径提前退出，已入队但未取出的帧就被丢弃 —— 帧里可能带
`c_xxx`，`phantomCleanup` 靠 raw 删幽灵会话，帧丢了残片就漏删。

## 精确位置

- `internal/app/webchannel.go` 下行读取 goroutine：`wcReadFrames` 回调里
  `events <- ev`（约 477-480 行一带）—— 帧先进队列。
- 主循环 `for { select { case ev := <-events: ... } }`（约 533-559 行一带）——
  只有走到 `ev.Line` 分支才 `raw.WriteString`。
- 提前返回点（`post` 失败、`ctx.Done`、上行超时等，约 588-615 行一带）返回的是
  `rawBytes()` —— 只含已取出的帧，队列里的残帧全丢。

## 触发前提

当前 `streamGenerateOnce` 里 `useWC := false`，WebChannel 入口整体禁用，
**没有现行生产触发路径**，故按 consider 记录不升级阻断。
**重新启用 WebChannel（`rtCfg().WebChannel` 生效前 / useWC 接线前）必须先处理**：
让 raw 的累计点跟「下行读到帧」绑定而不是「主循环取出」——例如把累计挪进
`wcReadFrames` 回调（注意它跑在下行 goroutine，需要 mutex 或换 thread-safe 收集器），
或在所有提前返回路径先 drain `events`。

## 验证建议

单测构造：下行先吐一条带 `c_xxx` 的帧后挂起；主循环在上行 `post` 阶段失败返回。
断言返回的 raw 里仍能 `extractConversationID` 提取出 cid（现状会返回空，幽灵漏删）。
