# 任务：修「模型能列出、但调用全部 502」并把失败原因做成可读可留证

你是这个工作区的主代理。本文件是任务书，按它执行，不要扩大范围。

## 工作区

- 目录：`/root/workspace/.wt/gemini-web2api-go/fix-upstream-502-diagnostics`
- 分支：`fix/upstream-502-diagnostics`（基于 `a8d1bc1`，其上游是 `e6c2bd5`）
- 语言 Go（模块 `gemini-web2api`）。先读仓库里的 `AGENTS.md` 与 `Makefile`，遵守其规格（L1 基线，`make check` 必须绿）。
- 只在本分支提交，**不要 merge、不要 push、不要安装任何东西**。

## ⚠️ SBX 的坑（务必先看，否则你会浪费大量时间）

本机派发子代理用的是 `dispatch-agent`。它会按**角色**决定是否把子代理塞进 `sbx --preset reviewer-agent` 沙箱：

- 被塞沙箱的只读角色：`scout`、`planner`、`researcher`、`reviewer`、`oracle`
- 不塞沙箱的：`verifier`、`worker`

**坑在**：沙箱内跑 `pig` 时，pig 加载扩展要在 `/root/.pig/cache/cells/go/.build-*` 创建构建目录，而该目录在沙箱内不可写 →
```
create build scratch: mkdir /root/.pig/cache/cells/go/.build-XXX: permission denied
```
18 个扩展全部失败，pig 直接 exit 1。

**正确做法：照常用 oracle / reviewer，只是不要套 sbx 包装。**
1. 正常跑 `dispatch-agent oracle ...`，拿到它生成的完整命令；
2. 把命令里这一段去掉：`/root/.local/bin/sbx --preset reviewer-agent --workdir <dir> --report-to <id> --`
3. **其余一字节不改**（`CTXMODE_ROLE=oracle PATH=… /root/.local/bin/pig …` 及其所有参数保持原样）后执行。

这样 oracle 能正常起来：它工具层已排除 `edit`/`write`/`bash`，只保留只读检索与 `ctx_run`（它用 `ctx_run` 发 `hc send` 回报）。`verifier`、`worker` 本来就不进沙箱，正常用。

**门禁仍然是三审齐：verifier + reviewer + oracle。不要用 reviewer 顶替 oracle。**

## 线上问题（事实）

- 用户（Google AI Pro 账号）在 Magpie 里 `GET /v1/models` **能看到** `gemini-4-hydrogen*`、`gemini-4-carbon*`、`gemini-4-argon*`，但每次调用返回 **502**；界面上没有任何可读的错误提示。
- 本机（另一账号）同一后端跑得好好的。
- 我们后端 502 的来源：
  - `hasContentFrame == false` 且非 videoAccepted → `lastErr = "no content frame"`（`internal/app/gemini.go:935` 附近）→ `server.go:352` / `:757` 返回 502
  - 上游「出错话术」`failureReplyInLine`（`inner[31]`，`gemini.go:1523` 附近）→ `errUpstreamFailureReply` → 重试用尽后 502
  - 超时且未开 `retry_on_timeout` → 502
- `strict_model` 默认 `false`（`config.go:123`），所以 `argonIdentityError` 不会转成 429，**只会表现为 502 "no content frame"**——真因被吞掉。
- 已知缺陷：`internal/app/server.go` 的 `Version` 常量在补丁里被固化成 `4.28.0-magpie.2`，发布 `.3` 后 `GET /` 仍显示 `.2`。

## 要做的四件事（只做这四件，不夹带）

### 1. 502 响应体结构化（核心）
在返回 502 的路径上让 body 带上可诊断字段（JSON 或清晰文本，选一种并全程一致）：
- `reason`：`no_content_frame` / `failure_reply` / `upstream_timeout` / `empty_content`
- `upstream_model`：解析到上游自报显示名就带上，否则显式 `unknown`
- `frames`：解析到的帧数，以及是否命中过 content frame
- `hint`：一句**中文人话**，例如「上游没有返回正文内容：可能是该账号无此模型权限、登录已过期，或模型名/协议已变更」

HTTP 状态码保持 502 不变。

### 2. 失败原因落库
把 `reason` 与 `upstream_model` 写入 `gemini.db` 的 `requests` 表（`error` 列用可检索前缀，如 `[no_content_frame] ...`；`upstream_model` 列填真值）。即使 `log_requests` 关闭也要落库——这是排查用户现场的唯一线索；日志本身仍受原开关控制。

### 3. 空正文不再静默成功
现有 `callGemini` 守卫（`server.go:185` 附近）只覆盖「坏 tool_call 围栏」。扩展到：**只要有 content 帧、抽取后正文为空且没有 toolCalls**，就返回结构化错误（`reason=empty_content`），不要回 `200 + 空正文`。

**不要误伤**：canvas、媒体（image/music/video）、纯 tool_call、纯代码产物（`codeMarkerRe` 回退）、以及只有 `reasoning_content` 的思考档。逐条核对 `server.go:146-190` 与 `conversation.go` 对应路径；对「只有 reasoning」按实际行为选择（作为正文返回 或 返回结构化错误），并说明理由。

### 4. 版本号不写死
把 `internal/app/server.go` 的 `Version` 改成可被构建注入的形式（如 `var Version = "dev"`），并在仓库 `README`/`CHANGELOG` 说明由发布流程覆盖。**不要**去改 `magpie-gemini-web` 仓（那是另一个仓）。

## 测试

复用现有假 `RoundTripper` / `httptest` 模式（不新建测试文件），为 1/2/3 各加最小回归：
- 上游只回空 content 帧 → 502 body 含 `reason`/`frames`/`hint`，且库里有对应 error 前缀
- 上游回 failure reply → `reason=failure_reply`，body 带自报名（若可解析）
- 合法路径（canvas / 媒体 / 纯 tool_call / 代码产物）→ 不被新守卫误伤
断言要能在旧代码上失败（说明理由，能实测就实测）。

## 交付

- 跑通并留输出：`make check`、`/root/.local/bin/dev-audit .`、`GOTOOLCHAIN=go1.27.1 go test -race ./internal/app -count=3`
- 提交到 `fix/upstream-502-diagnostics`，报告：新 commit SHA、四件事各自的改动位置、502 body 示例、`requests.error` 前缀示例、四类合法路径不误伤的依据、以及 oracle 缺位的说明。
- 零凭据泄露：不要回显任何 cookie / apiKey。
