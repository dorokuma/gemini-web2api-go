# 观察项收件箱（Inbox）

## [pattern:webchannel-enable-prerequisites] | type:redline | added:2026-11-14

当前 `internal/app/gemini.go` 锁定 `useWC := false`；未来将 `useWC` 接通
`rtCfg().WebChannel` 前必须逐项完成：

1. `doWebChannelRequest` 透传客户端请求 `ctx` 取消至握手/上下行请求（替换
   `context.Background()`）。
2. 接入 `stallWatch` 下行沉默超时掐断与工具调用 `stopWhen` 提前停止。
3. `internal/app/gemini.go:853-858` `mergeSetCookie`/`updateAccountCookie`
   按目标 host 过滤，禁止将
   `geminiweb-pa.clients6.google.com` 下发的 `Set-Cookie` 无条件混入账号池。
4. `wcParseStatus` 兜底禁止把未识别 200 帧前 200 字节拼进 `requests.error`/
   `last_error`/502 响应体。
5. 复核握手 400 诊断 `"xsrf"` 标记是否需接入 `isXSRFError` 自愈。

## [pattern:webchannel-queue-drop] | type:debt | added:2026-11-14

`internal/app/webchannel.go`：下行 goroutine 将帧推入 `events` 队列后，主循环仅在
取出 `ev.Line` 分支才追加 `raw`；启用 `useWC` 前须改为读取时线程安全累计或提前
返回路径先排空 `events`，否则上行报错/超时提前返回会丢弃队列残帧，导致漏提
`c_xxx` 会话 ID、幽灵会话清理 `phantomCleanup` 失效。
