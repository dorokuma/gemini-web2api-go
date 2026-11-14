# TASK-502 结果记录（事实记录 · 候选已 commit，门禁待验收）

> 本文件是事实记录，非门禁证据本身，**不代表任务完成或门禁通过**。
> **当前候选 commit：`c57860d`**（基线 `8647ba1`，由 rebase 前旧快照 `a8d1bc1`
> + 工作区改动 rebase 后 squash 而来）；HEAD `d416789` / `2007fa0` 仅追加文档。
> **状态：待验收。** 当前候选的三方门禁原件均已收齐并落盘（见第六节），
> 收口裁决由协调方做出。

## 〇、证据分类总览（先分清三类，勿混用）

| 类别 | 内容 | 对 `c57860d` 的门禁效力 |
|---|---|---|
| **当前候选验证** | verifier 初轮（8/8 判据通过）+ verifier 补证（全包 race 3 轮 + 受体演练）+ oracle 恢复件（无阻断）+ reviewer 两轮（首轮唯一 must fix 已由补证覆盖） | **有效**，是本次验收依据 |
| **旧轮真实 baseline 红灯历史材料** | `a8d1bc1` 时代 verifier round2（`.agents/notes/task-502-verifier-round2.txt`）的红绿对照 | **历史材料**：是在未提交旧快照上测出的真实差异，**不作为本候选门禁证据** |
| **当前变异敏感性测试** | verifier 初轮判据 7/8：在 `c57860d` 隔离 clone 上**人为改坏**业务文件（去掉空正文守卫 / 去掉落库前缀），测试转红 | 证明测试有效性感，**不是真实基线回归红灯**，反例改动不在候选代码内 |

round1 的 "unauthorized retries" 问题轮次同样不作为最终门禁通过依据。

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
- 502 JSON 示例（fixture 形态；message 只含错误串，无 cookie/token 等敏感值）：

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
- requests.error 前缀（区分实测与未实测，勿混）：
  - **DB 实测断言**（verifier 初轮判据 2/3/8，真实落库校验）：
    `[no_content_frame] upstream returned no content frame (raw 820 bytes)`、
    `[failure_reply] upstream returned a failure reply (inner[31] set)`。
  - **未专门实测落库的格式示例**（按 `recordUpstreamFailure` 实现推导的形状，未逐条落库断言）：
    `[empty_content] upstream returned a content frame but extracted text is empty (raw N bytes)`、
    `[upstream_timeout] 上游 3m0s 没有任何数据（request_timeout_sec）`（StallError）、
    `[upstream_timeout] processSession 超时（…）: upstream timeout`。
    **两个 timeout 示例都必须带 `[upstream_timeout]` 前缀**（与 no_content_frame 同等格式约束）。
- `admin.go:666` `classifyError` 增补认 `[no_content_frame]`/`[failure_reply]`/`[empty_content]`
  前缀，归 `upstream_rejected`，不再误归 network。

### 3. 空正文不再静默成功
- `server.go:181-192`：`hasContentFrame(res.Raw)` 为真而正文抽空 → `empty_content`；
  真无内容帧 → `no_content_frame`。`server.go:203-205` 坏 tool_call 围栏路径同步改 `empty_content`。
- `conversation.go:572-576`：多轮 callGeminiConv 同样二分；`conversation.go:412-416`
  续接中文报错「续接无内容帧（会话可能已失效）」经 `diagErr` 显式带 `reasonNoContentFrame`
  （错误串无英文特征，串匹配会漏归兜底）。
- **reasoning-only 选择返回 `empty_content` 而非正文**：这是**任务明确允许且已回归验证的选择**
  （toolgate_test.go:601 注释决策记录 + verifier 判据 4 PASS）。`reasoning_content` 是正文之外的
  辅助字段，拿它顶替 content 等于回一个「成功但没答案」的假 200，比报错更误导。
- 帧现场来源：gemini.go 与 conversation.go 在拿到完整 raw 时即时 `upstreamFrameStats`
  解算（gemini.go:866 / conversation.go:389），只留计数；新增诊断不输出 `raw` 字段
  （成功形状下 `res.Raw` 在空守卫时仍用于解析，raw 原文不进 502 body 或 requests 表）。

### 4. 版本号不写死
- `server.go:17`：`const Version = "4.28.0-magpie.2"` → `var Version = "dev"`（本地构建为 dev）。
- `.github/workflows/release.yml`：tag 构建 `-ldflags -X gemini-web2api/internal/app.Version=<tag>` 注入；
  原「tag 与常量一致」检查改为 `grep -nx 'var Version = "dev"'` 防写回。
- `Dockerfile`：`ARG VERSION=dev` + `-X …Version=${VERSION}`；
  `.github/workflows/docker.yml`：`Compute image version` 步骤（tag→去 v，分支构建→`dev`）
  经 `build-args` 传入。
- 跨仓耦合：同步进 `magpie-gemini-web` 发行链时，`backend-release.mjs` 先在 `:60-61`
  `git apply`（`patches/argon.patch`）失败，补丁适配后才会到 `:64-66` 的
  `const Version` 正则检查——详见 `.agents/notes/cross-repo-version-coupling.md`
  （分诊：out-of-scope / followup）。

## 二、附带修复：video race（用户授权附带项）

- `videos.go:162` 新增 `videoJobSnapshot`：在 `videoJobsMu` 下复制 JSON 字段，
  `handleCreateVideo`(156) / `handleExtendVideo`(413) / `handleVideoItem`(474) 三处改用它返回，
  消除 HTTP 序列化直接读后台 goroutine 正在写的 `*videoJob` 的 data race。
- 历史实测：`TestCreateVideoAcceptsInputReference` 在旧快照（rebase 前 base）上
  `-race -count=3` 报 `WARNING: DATA RACE`（`writeJSON` 读 vs `finishVideoJob` videos.go:315 写）；
  候选上同测试 3 轮 PASS；verifier 补证已在 `c57860d` 上跑全包 `-race -count=3` PASS
  （见 `.agents/notes/task-502-evidence/`）。

## 三、四类合法路径不误伤（区分静态依据与实测）

| 路径 | 依据 |
|---|---|
| canvas（gemini-canvas） | 静态：callGemini 在 `mc.Tool == toolCanvas` 分支（server.go:149-157）先 return，不走空正文守卫。实测：LegitPaths 子用例「canvas 文档」200 PASS |
| 媒体（image/music/video，tool 分支同构） | 静态：server.go:160-170 `toolImage||toolMusic||toolVideo` 分支在守卫前 return。**实测仅覆盖 gemini-image 空正文边界**（MediaArtifact 用例 200 PASS）；**music/video 与 banner/MCP 路径仅静态核对，未逐一实测** |
| 纯 tool_call | 静态+实测：LegitPaths「纯 tool_call」200 PASS。注意：有效 `toolCall` 是 `parseToolCalls` 之后 `len(toolCalls)>0` 时绕过第二空正文守卫（server.go:191-200），不是所有 `toolCalls` 都在第一守卫前 return |
| 纯代码产物（codeMarkerRe 回退） | 实测：LegitPaths「纯代码产物」200 PASS |

## 四、当前候选验证记录（verifier 初轮 + 补证 + oracle 恢复件 + reviewer）

### 4.1 verifier 初轮（c57860d，8 判据全过）

原件逐字副本：`.agents/notes/task-502-gate-verifier.md`（源 `/tmp/herdr-role-sessions/default/role-verifier-8496f4a0/report.md`，28633 字节，sha256 `bd7648f685071da4d9ccacfa22afd9f2858c47c99be8e40e165f305448b86654`）。要点：

- 判据 1 门禁基础：dev-audit `L1 OK`；make check RC=0（gofmt 0 / vet 0 / test ok 18.087s / build 0）；`-v` 顶层 PASS 183。
- 判据 2/3/4：4 个 502 用例全 PASS，reason/`upstream_model`/`frames`/`content_frame`/`hint` 断言齐备；`log_requests=false` 下 requests 行仍落库。
- 判据 5：Version 注入三处齐备（server.go:17 `var Version = "dev"`、release.yml:47 ldflags、Dockerfile:20/25）。
- 判据 6：预注册 -run 集合 `-race -count=3` 主轮 + 补充轮均 RC=0、DATA RACE 0、FAIL 0。
- 判据 7/8（变异敏感性，非真实基线）：在 c57860d clone 上人为改坏守卫/落库前缀 → 包装器 `inner_exit=1` 命中预期特征；原样 clone 同命令 PASS。

### 4.2 verifier 补证（c57860d 独立快照）

目录 `.agents/notes/task-502-evidence/`（`task-502-gate-verifier-supplement.md`，16694 字节，sha256 `110518e42a00057fc2d2ddf71a012370ae1ce85c015a1b240be9943af4ca9691`，加 criterion-a/b 原始证据文件）：
- 判据 A：`c57860d` 上全包 `go test -race -count=3` PASS（补 reviewer 要的当前快照 -race 3 轮）。
- 判据 B：受体演练——`git archive` 快照注入 `Version=task502-probe` 构建，`--version` 与 `GET /` 运行时 `version` 字段严格相等。

### 4.3 oracle 审计（恢复件）

原件：`.agents/notes/task-502-gate-oracle.md`（正文 2930 字符，sha256 `7fbbe9b7f3fd88db2dc61d72e91ed21d79bbf94f5ba0f594ad9b6dd2f5d30c9f`，恢复方式与 transcript 指纹见文件头）。
结论：**无 must fix / should fix**，本仓改动成立；1 项观察项为跨仓 `backend-release.mjs` 对 `const Version` 的文本替换依赖（→ `.agents/notes/cross-repo-version-coupling.md`，分诊 out-of-scope / followup）。

历史如实记录：oracle 首轮报告正文曾随终端缓冲丢失（hc send 未达），经 pig session
transcript 取文**恢复成功**（transcript 7603212 字节，sha256
`0faf745ffdf5dd556779a0f5c496f36f1f5f7439a433cae835b9f3bed1231bd5`）；t15x 重派
**零执行受阻**（`0 remaining`）。现状是「oracle 完整报告已恢复」，不是「oracle 缺位」。

主代理核对纠正（仅作附注，不改 oracle 原文）：
- (a) `server.go:17` 是 `Version` 声明位置；`writeJSON` 的 `json.Marshal` 在 `server.go:20`，oracle 文中 `:17`/`server.go:13` 行号按现状以此为准。
- (b) oracle「`raw` 统计后丢弃」不能扩写为「整个应用任何路径都不存在载荷泄露」；准确表述：新增诊断不输出 `raw` 字段；成功形状下 `res.Raw` 在空守卫时仍用于解析。
- (c) 有效 `toolCall` 是 `parseToolCalls` 之后 `len(toolCalls)>0` 绕过第二守卫，不是所有 `toolCalls` 都在第一守卫前 return。
- (d) `reasoning-only → 502 empty_content` 是任务明确允许且已回归验证的选择；不采信「所有正常模型必同时输出思考与答案」「客户端必然重试」这两个泛化表述。
- (e) Admin UI **并未**跳过 dev 升级提示：`verCmp('dev', 版本)` 走 `parseInt||0` 比较（index.html:1963-1965），`:2019` 仍可能显示升级提示；此为既有 UI 比较逻辑，非阻断，不改。
- (f) 跨仓执行顺序见 `.agents/notes/cross-repo-version-coupling.md` 第一节（`git apply` 先于版本声明检查）。

### 4.4 reviewer 两轮

原件：`.agents/notes/task-502-gate-reviewer.md`（源 `/tmp/reviewer-502-original.txt`，2054 字节，sha256 `d6b5559ab873ef5ffa9a947388c5bb0cc3cd97dd5f381e8b1795d80265116be6`）。
首轮结论：未回退既有修复、无新增代码阻断；**唯一 must fix = 缺当前快照独立证据**（round2 是 a8d1bc1+旧补丁，非当前候选）。该 must fix 已由 §4.2 verifier 补证覆盖；最终裁决由协调方做出。

### 4.5 旧轮材料（历史，非本候选门禁）

- `.agents/notes/task-502-verifier-round1.txt`（31028 字节，sha256 `7250d8bdad1397ce2007e6eb322583189e5e3a4797b07d5736174bd1608dc0aa`）。
- `.agents/notes/task-502-verifier-round2.txt`（17367 字节，sha256 `ff43bb65c7d7a200988e168180ef938494ba6ed812813c922e346ae898820d9e`）——基于 `a8d1bc1` + 未提交改动，是**真实 baseline 红灯**但属旧快照，仅作历史材料。

## 五、边界声明

- **未执行任何线上真实账号调用，未做线上真实账号恢复演练**；不对「原用户账号已恢复」下任何结论。本改动交付的是**可诊断的失败**（结构化 reason + 落库现场），不凭空承诺权限/协议层面的修复。
- 候选已固化：base `8647ba1` + commit `c57860d`（`fix-upstream-502-diagnostics` 分支，由 rebase 前 `a8d1bc1` + 工作区改动 squash 而来）；文中行号已按 `c57860d` 复核。
