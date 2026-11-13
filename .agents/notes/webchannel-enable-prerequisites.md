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
- **本轮收尾（O-1/O-6）**：下行/上行非 200 分支的 `"xsrf"` 标记已删除——WC 路径从不
  发送 XSRF token（`wcHTTPHeaders` 只放 API key/SAPISIDHASH/模型头），且两处返回 err、
  `statusCode=0` 不进 `gemini.go` 的 400 自愈分支，标记无任何消费者；握手分支标记
  保留并注明无消费者可后续删除。同时第 4 条修复说明里“SF1 下行/上行不回传 body、
  `Timeout()/Temporary()` 沿错误链下钻”两点已随上轮落地，此处一并登记。
- **第 6、7 条部分缓解**：链上脱敏保留参数名与非敏感参数（`SID=%3Credacted%3E`、
  `VER=8` 等）便于定位；但 `wcRedactParam` 的 `;`/fragment/大小写变体仍未覆盖。
- **第 1/2/3/5 条未动**，`useWC` 接线前必须先处理（当前 `gemini.go` `useWC = false`）。

## 审计观察项（oracle O-1..O-6，均带 [pattern:webchannel-enable-prerequisites]）

- **O-1**：WC 不参与 XSRF 自愈（握手 `"xsrf"` 标记保留但无消费者，下行/上行已删除）。
  **接线前确认**——接线时若握手 400 诊断需要被 `isXSRFError` 命中，须重新评估该标记。
- **O-2**：`*url.Error` 分支未做深链下钻（`url.Error→fmt→net.Error` 时 `Timeout()`
  仍会丢），理论项，`errors.As` 本就只选外层。→ 观察，接线前不必修。
- **O-3**：`wcSanitizeErr` 不跟随 `errors.Join` 的 `Unwrap() []error`，仓库无此用法。
  → 观察，接线前不必修。
- **O-4**：诊断里「响应体 N 字节」是 `LimitReader` 截断后长度（下行/上行 ≤4096、
  握手 ≤1MB），排障可能误判真实体长。→ 观察，必要时改文案为「已读 N 字节」。
- **O-5**：`wcParseStatus` 兜底 `truncate(string(p), 200)` 仍会把无法识别的
  **200 帧**前 200 字节带进 `requests.error`/502/`last_error`（pre-existing，属
  200 帧路径，不在 SF1 不回传响应体的范围）。→ **接线前必修**。
- **O-6**：本条目（文档同步，即本节与 CHANGELOG 的更新）。
