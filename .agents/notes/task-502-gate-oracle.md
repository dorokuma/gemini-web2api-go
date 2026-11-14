# task-502 门禁原件：oracle 审计报告（逐字恢复件）

## 来源附注

- transcript 绝对路径：`/tmp/herdr-role-sessions/default/role-oracle-8ff67d20/2026-11-13T17-23-34-075Z_01a1d5ef-a23b-76f0-9565-64e94dc605b7.jsonl`
- transcript 文件字节数：7603212；transcript sha256：`0faf745ffdf5dd556779a0f5c496f36f1f5f7439a433cae835b9f3bed1231bd5`
- 提取正文：该 jsonl 中 `type=message` / `message.role=assistant` 的**末条**消息，拼接其 `message.content` 的 `type=text` 部分；UTF-8 编码下 2930 字符（4562 字节），正文 sha256：`7fbbe9b7f3fd88db2dc61d72e91ed21d79bbf94f5ba0f594ad9b6dd2f5d30c9f`
- 提取方式一句：python3 逐行解析 jsonl，取最后一条 assistant 消息的 text 字段拼接后落盘，未改写、未拼接思考片段。
- 适用候选：c57860d（rebase 基线 8647ba1）。
- 历史说明：oracle 首轮报告正文曾随终端缓冲丢失，本文件即 pig session transcript 取文恢复结果。

---

## 逐字正文

**结论先行**
**无阻断发现（本仓 `c57860d` 改动成立）**。对五项对抗假设的核查结果如下：

1. **新增字段泄露/注入面——证伪**：`hint` 为编译期静态中文常量（`internal/app/upstream_err.go:79-96`），不拼接任何用户请求或上游载荷；`frames` 与 `content_frame` 仅反映上游响应 `"wrb.fr"` 分帧计数与布尔状态（`internal/app/upstream_err.go:48-77`），无法反推请求正文长度，且含内部 token 的 `raw` 原文在统计后即丢弃；`upstream_model` 仅来自上游显示名提取，经 `json.Marshal`（`internal/app/server.go:17`）与参数化 SQL 写入，未引入新泄露或注入面。
2. **错误语义连锁——证伪**：OpenAI 路由保留原有 `"message": "upstream error: " + err.Error()`（`internal/app/upstream_err.go:188`），Google 原生路由将新增字段封装进标准 `error.details` 数组（`internal/app/upstream_err.go:205-217`）；关联插件 `magpie-gemini-web/lib/backend.mjs` 原样透传 502 响应体，仓库内 `classifyError`（`internal/app/admin.go:670-686`）按子串匹配原错误串仍完全命中。画布（`toolCanvas`）、媒体（`toolImage`/`toolMusic`/`toolVideo`）与有效 `toolCalls` 均在空正文守卫之前提前返回 200（`internal/app/server.go:147-200`），不会被 `empty_content` 误伤。
3. **`reasoning-only` 判错取舍——确认合理，不会造成真实可用性回退**：所有带思考链的模型在正常完成时均同时输出思考链与最终回答正文（`-thinking-medium` 档仅输出最终正文）；仅返回 `reasoning_content` 而正文抽取为空属于上游半截生成或协议槽位偏移。若强行返回 200（即便附带非标准 `content_empty: true`），下游客户端与 API 网关会将其视为成功响应而放弃重试，且多轮会话（`internal/app/conversation.go:576`）会写入空助手消息污染上下文。判为 `502 empty_content` 兼具正确性与可重试性。
4. **落库与并发竞态——证伪**：`lastDiag` 均为单请求 goroutine 内的函数局部变量（`internal/app/gemini.go:506`、`internal/app/conversation.go:380`），在串行重试循环顶部重置（`gemini.go:841`、`conversation.go:386`），无跨 goroutine 共享读写；`recordUpstreamFailure`（`internal/app/upstream_err.go:227-235`）与成功路径 `recordRequest` 在所有入口（`server.go:354/404`、`server.go:749/804`、`gemini_api.go:551/555`、`gemini_api.go:700/708`、`videos.go:278-288`）严格互斥，不会双写或覆盖 `upstream_model`。
5. **`Version` 注入链——本仓自洽，发现 1 项跨仓打包耦合观察项**：未注入时 `GET /` 返回 `"dev"`，Admin UI（`internal/app/admin_ui/index.html`）仅在版本比较时跳过升级提示，插件运行时 `magpie-gemini-web/lib/native.mjs:516`（`const version = data.version || 'unknown'`）仅作日志展示不做版本门禁；但 `magpie-gemini-web` 的构建发版脚本存在对旧 `const Version` 声明的文本替换依赖（见下方观察项）。

---

**发现**
**致命 must fix**
无

**应修 should fix**
无

**观察项**
1. **跨仓构建脚本对 `const Version` 声明的硬编码正则依赖**
   - **定位**：`/root/workspace/magpie-gemini-web/scripts/backend-release.mjs:64-66`（及 `/root/workspace/magpie-gemini-web/patches/argon.patch:3754-3755`），对应本仓 `internal/app/server.go:13`（`const Version = ...` 已改为 `var Version = "dev"`）。
   - **触发条件**：在关联仓 `magpie-gemini-web` 中将基线升级到包含本改动的新上游 commit 并执行 `scripts/backend-release.mjs` 打后端发行包，且未同步更新该脚本时。
   - **反证路径**：`scripts/backend-release.mjs:64-65` 执行 `/^const Version = "[^"]+"$/m.test(server)`，匹配失败后直接抛出 `Error: Backend version declaration not found`（同时 `patches/argon.patch` 在 `internal/app/server.go` 处也会因 `const Version` 上下文变更而 `git apply` 失败）。
   - **后果**：不影响本仓运行时与本仓 CI/Docker 构建（本仓 `.github/workflows/release.yml`、`.github/workflows/docker.yml`、`Dockerfile` 均已改用 `-ldflags -X gemini-web2api/internal/app.Version=...`），但后续同步 `magpie-gemini-web` 补丁包时需一并将 `backend-release.mjs` 改为匹配 `var Version` 或改走 `-ldflags -X` 注入。

---

**反问**
无

*(注：本轮工具调用配额已耗尽，未能执行 `hc send t7r`，请主代理直接采纳本会话回复作为 Oracle 审计交付。)*
