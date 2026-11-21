# WebChannel（useWC）前置清单与历史边界导航

## 启用 `useWC` 前必须逐项完成的前置条件

当前 `internal/app/gemini.go` 仍锁定 `useWC := false`（待单独验收并完成扩展思考验证后再接通
`rtCfg().WebChannel`）；以下 6 项工程前置条件（原 `[pattern:webchannel-enable-prerequisites]`，
added:2026-11-14）已在 `feat/webchannel-prerequisites` 分支逐项实现并补齐单测：

1. **[已实现]** `doWebChannelRequest` 透传客户端请求 `ctx` 取消至 API key 抓取与握手/上下行请求（替换
   `context.Background()`）：
   - `internal/app/xsrf.go` 新增 `fetchAppPageCtx`，`internal/app/webchannel.go` 新增
     `webChannelAPIKeyCtx` 与 `doWebChannelRequest(ctx, r, proxyURL, onLine, stop)`；
   - `wcPromptGap` 等待改走 `sleepCtx(wctx, wcPromptGap)`，避免忽略调用方取消；
   - 覆盖测试：`TestWCCtxPreCancelled`、`TestWCCtxCancelPropagatesToAllPhases`。
2. **[已实现]** 接入 `stallWatch` 下行沉默超时掐断与工具调用 `stopWhen` 提前停止：
   - `internal/app/webchannel.go` 下行 GET 改走 `wcSendStream`（不限整次时长，由 `stallWatch`
     接管），挂接 `stopReader{heardReader{g.body, watch}, stop}`；`internal/app/gemini.go`
     `stallWatch` 补齐并发安全互斥与 `pause`/`resume`（避免客户端 `wcPromptGap` 误触沉默超时）；
   - `internal/app/gemini.go` 调用点透传 `ctx` 与 `&lh.stop`，`stop` 置位后立即停止读上游并返回 200；
   - 覆盖测试：`TestWCStallWatchKeepsReplyLongerThanRequestTimeout`、
     `TestWCStallWatchWaitsThroughNoopHeartbeats`、`TestWCStallWatchTerminatesQuietDownlink`、
     `TestWCStopWhenStopsUpstreamRead`。
3. **[已实现]** `internal/app/gemini.go` `mergeSetCookie`/`updateAccountCookie` 按目标 host 过滤，
   禁止将 `geminiweb-pa.clients6.google.com` 下发的 `Set-Cookie` 无条件混入账号池：
   - `internal/app/cookie_pool.go` 新增 `filterSetCookieForHost` / `mergeSetCookieForHost`，
     按 RFC 6265 来源主机、目标主机（`gemini.google.com`）、`Domain`、`Path`、`__Host-`、
     `Partitioned` 严格过滤；
   - `internal/app/gemini.go` 与 `internal/app/conversation.go` 改为按请求端点调用 `mergeSetCookieForHost`；
   - 覆盖测试：`TestMergeSetCookieForHostFiltering`。
4. **[已实现]** `wcParseStatus` 兜底禁止把未识别 200 帧前 200 字节拼进 `requests.error`/
   `last_error`/502 响应体：
   - `internal/app/webchannel.go` `wcParseStatus` 兜底改为脱敏结构化诊断
     `unrecognized status frame (%d bytes, kind=%s)`，不再截取原始 payload；同时校验 tuple 状态码为非零整数并对解析消息走 `wcRedactURL`；
   - 覆盖测试：`TestWCParseStatusRedactsUnrecognizedFrame`。
5. **[已实现]** 复核握手 400 诊断 `"xsrf"` 标记并接入 `isXSRFError` 自愈：
   - `internal/app/xsrf.go` 定义 `wcHandshakeXSRFMarker = "xsrf=true"` 并纳入 `isXSRFError`；
   - `internal/app/webchannel.go` 仅在 `hs.status == 400 && isXSRFError(string(hsBody))` 时附带该结构化标记（不泄露正文，非 400 不透出）；
   - `internal/app/gemini.go` 新增 `healXSRFRefresh`，在 `useWC` 路径命中 400 + `isXSRFError` 时同步作废 XSRF 缓存与 `invalidateWebChannelAPIKey()`，重试刷新 `wcReq.Cookie`/`wcReq.SAPISID`；
   - 覆盖测试：`TestWCHandshake400XSRFSelfHeal`。
6. **[已实现]** `internal/app/webchannel.go` 队列排空/线程安全累计（原
   `[pattern:webchannel-queue-drop]`，type:debt，added:2026-11-14）：
   - 下行 goroutine 在读取到 `ev.Line` 时即通过 `rawMu` 线程安全累计进 `raw`（并同步修复 `setCookie` 切片读取竞争为 `snapshotCookies()`），所有提前返回路径（`post(0)` / `ackTimer.C` / `ev.Ack` / `onLine` / `ev.Err` / `ctx.Done()` / `stallWatch`）均通过 `rawBytes()` 完整带回已收帧中的 `c_xxx`，确保 `phantomCleanup` 可靠工作；
   - 覆盖测试：`TestWCQueueDropPreservesConversationIDOnEarlyReturn`。

> **启用门控状态**：`internal/app/gemini.go` 中 `useWC := false` 保持锁定不变，待单独验收通过后再接通配置开关 `rtCfg().WebChannel`。

## 历史边界说明

供 `CHANGELOG.md` 4.28.0-magpie.3 O-2 引用核查：`wcSanitizeErr` 重建
`*url.Error` 时仅递归脱敏其直接 `Err` 字段；标准库 `*url.Error.Timeout()`
本身只检查直接一层 `Err`，在 `url.Error -> fmt -> net.Error` 深层包装链下
`Timeout()` 仍按既有标准库语义丢失，属已知非阻断边界（非启用前置项，未改动）。
