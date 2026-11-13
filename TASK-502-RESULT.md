# TASK-502 结果记录（事实记录 · 候选已 commit）

> 本文件是事实记录，非门禁证据本身。**当前候选 commit：`c57860d`**（基线 `8647ba1`，
> 由 rebase 前旧快照 `a8d1bc1` + 工作区改动 rebase 后 squash 而来）。
> **重要**：文中「verifier round2 七判据」证据对应的是 **rebase 前的旧快照**（`a8d1bc1`
> + 未提交改动），**不构成本候选 `c57860d` 的门禁证据**；当前候选的三方门禁
> （verifier / reviewer / oracle）仍在进行中，通过结论以新一轮证据为准。
> round1 的 "unauthorized retries" 问题轮次同样不作为最终门禁通过依据。

## 一、四项修改与精确位置

> 以下代码位置已基于候选 `c57860d` 逐一校核。

### 1. 502 响应体结构化
- 新增文件 `internal/app/upstream_err.go`（已含于 `c57860d`）：`upstreamDiag`、`upstreamFrameStats`
  （L48，逐 wrb.fr 条目解析帧统计）、`diagErr`、`upstreamErrorJSON`、`writeUpstreamError`（L199）、
  `geminiErrorJSON`（L205，Google 形状走 `error.details` 块）。
- 接入点：
  - `internal/app/server.go:381`（chat.completions 未开流 502）、`server.go:790`（responses 未开流 502）
  - `internal/app/sse.go:131` `sseWriter.Fail(err, res)`：已开流后的 SSE 错误块带同一份字段
  - `internal/app/gemini_api.go:551`/`700` `recordUpstreamFailure` + `writeGeminiCallError(err, res)`（L283）、
    `geminiStream.fail(err, res)`（L654）：generateContent / streamGenerateContent 两路
- 502 JSON 示例（实测 fixture 形态；message 只含错误串，无 cookie/token 等敏感值）：

```json
{
  "error": {
    "message": "upstream error: upstream returned no content frame (raw 820 bytes)",
    "type": "upstream_error",
    "reason": "no_content_frame",
    "upstream_model": "3.6 Flash",
    "frames": 2,
    "content_frame": false,
    "hint": "上游没有返回正文内容：可能是该账号无此模型权限、登录已过期，或模型名/协议已变更"
  }
}
```

- `frames` 计的是该次响应里 **wrb.fr 条目总数（含结束帧）**，一行多帧逐条目解算；
  `content_frame` 复用 `textsInLine`/`reasoningInLine` 判据。诊断取的是**最终一次**
  尝试的现场（每轮重试先 `lastDiag = upstreamDiagMeta{}` 清零，gemini.go:841 /
  conversation.go:386）。上游自报模型名解析不到时 `upstream_model` 显式 `"unknown"`。

### 2. 失败原因落库
- `recordUpstreamFailure`（upstream_err.go:227）：`requests.error` 写 `[reason] 原错误串`，
  `upstream_model` 列写上游自报显示名；**不受 `log_requests` 开关影响**（该开关只管 stderr）。
- requests.error 前缀示例（实测）：`[no_content_frame] upstream returned no content frame (raw 820 bytes)`、
  `[failure_reply] upstream returned a failure reply (inner[31] set)`、
  `[empty_content] upstream returned a content frame but extracted text is empty (raw N bytes)`、
  `[upstream_timeout] 上游 3m0s 没有任何数据（request_timeout_sec）`（StallError）或
  `processSession 超时（…）: upstream timeout`。
- `admin.go:666` `classifyError` 增补认 `[no_content_frame]`/`[failure_reply]`/`[empty_content]`
  前缀，归 `upstream_rejected`，不再误归 network。

### 3. 空正文不再静默成功
- `server.go:181-192`：`hasContentFrame(res.Raw)` 为真而正文抽空 → `empty_content`；
  真无内容帧 → `no_content_frame`。`server.go:203-205` 坏 tool_call 围栏路径同步改 `empty_content`。
- `conversation.go:572-576`：多轮 callGeminiConv 同样二分；`conversation.go:412-416`
  续接中文报错「续接无内容帧（会话可能已失效）」经 `diagErr` 显式带 `reasonNoContentFrame`
  （错误串无英文特征，串匹配会漏归兜底）。
- **reasoning-only 选择返回 `empty_content` 而非正文**：`reasoning_content` 是正文之外的
  辅助字段，拿它顶替 content 等于回一个「成功但没答案」的假 200，比报错更误导
  （依据：gemini_test.go 中 reasoning 提取相关既有约定与 toolgate_test.go:601 注释里的决策记录）。
- 帧现场来源：gemini.go 与 conversation.go 在拿到完整 raw 时即时 `upstreamFrameStats`
  解算（gemini.go:866 / conversation.go:389），只留计数，raw 原文（含内部 token）不进
  502 body 或 requests 表。

### 4. 版本号不写死
- `server.go:17`：`const Version = "4.28.0-magpie.2"` → `var Version = "dev"`（本地构建为 dev）。
- `.github/workflows/release.yml`：tag 构建 `-ldflags -X gemini-web2api/internal/app.Version=<tag>` 注入；
  原「tag 与常量一致」检查改为 `grep -nx 'var Version = "dev"'` 防写回。
- `Dockerfile`：`ARG VERSION=dev` + `-X …Version=${VERSION}`；
  `.github/workflows/docker.yml`：`Compute image version` 步骤（tag→去 v，分支构建→`dev`）
  经 `build-args` 传入。

## 二、附带修复：video race（用户授权附带项）

- `videos.go:162` 新增 `videoJobSnapshot`：在 `videoJobsMu` 下复制 JSON 字段，
  `handleCreateVideo`(156) / `handleExtendVideo`(413) / `handleVideoItem`(474) 三处改用它返回，
  消除 HTTP 序列化直接读后台 goroutine 正在写的 `*videoJob` 的 data race。
- 基线实测：`TestCreateVideoAcceptsInputReference` 在裸 base 上 `-race -count=3` 报
  `WARNING: DATA RACE`（`writeJSON` server.go:17 读 vs `finishVideoJob` videos.go:315 写）。
  候选上同测试 3 轮 PASS；全包 `-race -count=3` PASS。

## 三、四类合法路径不误伤（区分静态依据与实测）

| 路径 | 依据 |
|---|---|
| canvas（gemini-canvas） | 静态：callGemini 在 `mc.Tool == toolCanvas` 分支（server.go:149-157）先 return，不走空正文守卫。实测：LegitPaths 子用例「canvas 文档」200 PASS |
| 媒体（image/music/video，tool 分支同构） | 静态：server.go:160-170 `toolImage||toolMusic||toolVideo` 分支在守卫前 return，产物 markdown 即正文。**实测仅覆盖 gemini-image 空正文边界**（image 占位链剥空后 `extractResponseText==""` 且 `hasContentFrame==true`，MediaArtifact 用例 200 PASS）；music/video 走同一分支但**未逐一实测**，不伪称全测 |
| 纯 tool_call | 静态+实测：parseToolCalls 解析成功即 `len(toolCalls)>0`，LegitPaths「纯 tool_call」200 PASS |
| 纯代码产物（codeMarkerRe 回退） | 实测：LegitPaths「纯代码产物」200 PASS |

## 四、验证记录（verifier round2，独立 C 档）

证据文件：`.agents/notes/task-502-verifier-round2.txt`（本仓持久路径，
sha256 `ff43bb65c7d7a200988e168180ef938494ba6ed812813c922e346ae898820d9e`；
不以 /tmp 为持久证据）。每判据一个隔离 clone（base a8d1bc1），无自行重试。
**该轮证据基于 rebase 前旧快照 `a8d1bc1` + 未提交改动，不构成候选 `c57860d`
的门禁证据**（见文件顶部状态段）。

| 判据 | 命令 | 退出码（原文） | 结果 |
|---|---|---|---|
| A make check | `make -C <cloneB> check` | RC=0（app 包 ok 18.631s，gofmt 无未格式化，vet 过，build 过） | 通过 |
| B dev-audit | `dev-audit <cloneC>` | RC=0（`L1 OK`） | 通过 |
| C race×3 | `GOTOOLCHAIN=go1.27.1 go test -race ./internal/app -count=3` | RC=0（ok 65.255s，无 DATA RACE，3 轮完整） | 通过 |
| D 502 诊断双快照 | 基线：`verifier-expect-fail … go test -run 'TestChatCompletions502\|MultiTurn\|Legit\|Media' -v` → 外层 RC=0、`inner_exit=1`、4 FAIL 命中 `reason=/requests.error/content_frame` 断言（顶层 6 用例）；候选：同命令 RC=0、6/6 PASS | 外层 0 / 内层 1（基线预期红）；候选 0 | 通过 |
| E videos race 双快照 | 基线：`verifier-expect-fail … go test -race -run '^TestCreateVideoAcceptsInputReference$' -count=3 -v` → 外层 0、`inner_exit=1`、3 RUN 命中 `WARNING: DATA RACE`；候选：同命令 RC=0、3 PASS 无 race | 外层 0 / 内层 1（基线预期红）；候选 0 | 通过 |
| F 受体入口 | `go build -ldflags '-X …Version=task502-probe'` → `--version`/`‑h` 均 RC=0；起服务后 GET `/` HTTP 200，`version="task502-probe"` 严格相等；SERVER_EXIT=143 已关闭 | 全 RC=0 | 通过 |
| G 版本 workflow | 候选 server.go `grep -nx 'var Version = "dev"'` RC=0 命中 1 行（server.go:17）；fixture（注释行/OtherVersion/const 三行）内层 grep RC=1 + `VERSION_DECLARATION_REJECTED`，外层 0 | 候选 0；fixture 外层 0 内层 3 | 通过 |

**如实记录的偏差**：D 候选格在单个 cloneE-cand 内执行了两次同命令——首次实际
`exit 0`（11882ms）为采信依据；第二次明文留档输出 `(cached)`（Go 测试缓存，
881ms），两次均 RC=0、PASS 集合一致，**不伪称复跑是新快照**。oracle 本轮正常派发
（仅去 sbx 包装），初稿的「oracle 缺位」声明已更正——本轮无缺位；但 oracle/reviewer
的最终结论仍待补齐，此处不编造。

## 五、边界声明

- **未执行任何线上真实账号调用**；不对「原用户账号已恢复」下任何结论。本改动交付的是
  **可诊断的失败**（结构化 reason + 落库现场），不凭空承诺权限/协议层面的修复。
- 候选已固化：base `8647ba1` + commit `c57860d`（`fix-upstream-502-diagnostics` 分支，
  由 rebase 前 `a8d1bc1` + 工作区改动 squash 而来）；rebase 后改动内容未变，文中行号
  已按 `c57860d` 复核一致。
