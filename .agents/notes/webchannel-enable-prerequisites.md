# [pattern:webchannel-enable-prerequisites] 打开 WebChannel（useWC）前必须处理的前置项

来源：oracle 对抗审计观察项 O1（分支 feat/pr72-and-webchannel-redaction，HEAD `7c8910175a9db444dd7a40e5e95b0979d2fdbc08`）。
当前 `internal/app/gemini.go` 中 `useWC := false` 锁死走 StreamGenerate，以下问题均无
现行触发路径；**在把 `useWC` 接上 `rtCfg().WebChannel` 之前必须逐项处理**。

## 前置清单（O1 七条，原样）

1. `ctx` 不透传：`doWebChannelRequest` 自建 `context.WithTimeout(context.Background())`，
   客户端请求取消不会传播到 WebChannel 的握手/下行/上行请求。
2. 无 `stallWatch`：新协议未接 #33 的按沉默计时，下行长时间无帧不会按判据掐断。
3. 工具提前收尾未接：工具调用的提前停止（stopWhen）在 WebChannel 路径没有接线。
4. S1/S2 脱敏缺口：握手失败分支把上游响应体原样拼进错误（`["c","<SID>"]` 帧里的 SID
   会进 `proxies.last_error`、`requests.error` 和客户端 502）；`wcSend` 请求构造失败
   （`http.NewRequestWithContext` / `fhttp.NewRequestWithContext`）未脱敏，
   `*url.Error.Error()` 带完整 URL（内含 `X-Goog-Api-Key`、`Authorization:SAPISIDHASH`）。
5. `Set-Cookie` 跨域合并未按 host 过滤：`gemini.go:840-845` 无条件 `mergeSetCookie`，
   且 `cookie_pool.go:662` 只校验 SAPISID+__Secure-1PSID —— WebChannel 主机
   （geminiweb-pa.clients6.google.com）下发的 Set-Cookie 会被无条件合进账号 cookie。
6. 过度脱敏丢排障信息：脱敏后错误里看不到握手/下行的具体内容，服务端行为变化时
   定位变难。
7. `wcRedactParam` 前导规则在 `;`/片段/大小写变体下失效：只认 `&`/`?` 前导，
   `;` 分隔的参数、URL fragment 内的参数、大小写不同的参数名会漏脱敏。

## 处置状态（2026-11-14 更新）

- **第 4 条已修复**（S1/S2），并连带修复审计追加发现的 S3（`wcRedactedError.Unwrap()`
  链上仍指向未脱敏原文）与 M1（S3 重建错误链丢 `net.Error`/`Timeout()` 判定）：
  握手失败改回固定诊断（状态码/字节数/是否取到 sid 的布尔），不再回传响应体；
  **非 200 分支同样不回传响应体**，改给固定诊断（status/字节数/是否疑似含会话帧，
  XSRF 自愈靠 `isXSRFError` 命中的 `"xsrf"` 标记保留）；两处构造错误走
  `wcRedactErr`；`wcSanitizeErr` 让 `Unwrap`/`errors.As` 命中的每个节点都脱敏，
  且链上没有任何节点被脱敏时原样返回原错误，不得不重建时
  `*wcRedactedError`/`*url.Error` 的 `Timeout()`/`Temporary()` 转发给底层。
  回归测试见 `internal/app/webchannel_test.go` 的
  `TestWCHandshakeFailureRedactsBody`（参数化 200/非200）、
  `TestWCSendRedactsRequestBuildError`、`TestWCRedactErrChainIsRedacted`、
  `TestWCRedactErrPreservesTimeout`。
- **第 6、7 条部分缓解**：链上脱敏保留参数名与非敏感参数（`SID=%3Credacted%3E`、
  `VER=8` 等）便于定位；但 `wcRedactParam` 的 `;`/fragment/大小写变体仍未覆盖。
- **第 1/2/3/5 条未动**，`useWC` 接线前必须先处理（当前 `gemini.go` `useWC = false`）。
