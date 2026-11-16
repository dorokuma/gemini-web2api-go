# WebChannel（useWC）前置清单与历史边界导航

## 启用 `useWC` 前必须逐项完成的前置条件

当前 `internal/app/gemini.go` 锁定 `useWC := false`；未来将 `useWC` 接通
`rtCfg().WebChannel` 前必须逐项完成（原 `[pattern:webchannel-enable-prerequisites]`，
added:2026-11-14）：

1. `doWebChannelRequest` 透传客户端请求 `ctx` 取消至握手/上下行请求（替换
   `context.Background()`）。
2. 接入 `stallWatch` 下行沉默超时掐断与工具调用 `stopWhen` 提前停止。
3. `internal/app/gemini.go:853-858` `mergeSetCookie`/`updateAccountCookie`
   按目标 host 过滤，禁止将
   `geminiweb-pa.clients6.google.com` 下发的 `Set-Cookie` 无条件混入账号池。
4. `wcParseStatus` 兜底禁止把未识别 200 帧前 200 字节拼进 `requests.error`/
   `last_error`/502 响应体。
5. 复核握手 400 诊断 `"xsrf"` 标记是否需接入 `isXSRFError` 自愈。
6. `internal/app/webchannel.go` 队列排空/线程安全累计（原
   `[pattern:webchannel-queue-drop]`，type:debt，added:2026-11-14）：下行
   goroutine 将帧推入 `events` 队列后，主循环仅在取出 `ev.Line` 分支才追加
   `raw`；启用 `useWC` 前须改为读取时线程安全累计或提前返回路径先排空
   `events`，否则上行报错/超时提前返回会丢弃队列残帧，导致漏提 `c_xxx`
   会话 ID、幽灵会话清理 `phantomCleanup` 失效。

## 历史边界说明

供 `CHANGELOG.md` 4.28.0-magpie.3 O-2 引用核查：`wcSanitizeErr` 重建
`*url.Error` 时仅递归脱敏其直接 `Err` 字段；标准库 `*url.Error.Timeout()`
本身只检查直接一层 `Err`，在 `url.Error -> fmt -> net.Error` 深层包装链下
`Timeout()` 仍按既有标准库语义丢失，属已知非阻断边界（非启用前置项，未改动）。
