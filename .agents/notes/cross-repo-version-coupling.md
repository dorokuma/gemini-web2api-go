# 跨仓版本注入耦合：backend-release.mjs 对 `const Version` 的文本替换依赖 [pattern:cross-repo-version-coupling]

## 条件链真实顺序（易误判点）

`magpie-gemini-web/scripts/backend-release.mjs` 的发行流程中，失败点**先到**的是补丁应用，不是版本声明检查：

1. `:60-61` 先执行 `git apply --check` 与 `git apply`（`patches/argon.patch`）。该补丁在 `internal/app/server.go` 处含 `const Version` 上下文，本仓改为 `var Version = "dev"` 后上下文不匹配，**补丁未适配时先在 `git apply` 失败**。
2. 只有补丁已被适配、顺利应用之后，`:64-66` 才会读 `internal/app/server.go` 并用 `/^const Version = "[^"]+"$/m` 匹配；匹配失败才抛 `Error: Backend version declaration not found`。

即：同步本候选进插件仓时，第一个硬失败是 `git apply`；版本声明抛错是补丁适配之后才到达的第二层耦合点。修复发行脚本时两处都要改（补丁 hunks 适配 `var Version` + 正则改为匹配 `var`，或一并改走 `-ldflags -X` 注入）。

## 我方现状

- `internal/app/server.go:17` 为 `var Version = "dev"`（**不是** `:13`）；`server.go:13` 是 `)` 收尾的 import 块，`:14-16` 为注释行。
- 本仓 release/docker 构建已全部改走 `-ldflags -X gemini-web2api/internal/app.Version=<ver>` 注入（release.yml:47、Dockerfile:20/25），发行侧不依赖源码内版本声明形态。

## 触发面

仅在把本候选（`c57860d` 或含此改动的后继 commit）同步进 `magpie-gemini-web` 并运行 `scripts/backend-release.mjs` 打后端发行包时触发；本仓本地构建、CI、Docker 均不受影响。

## 分诊结论

**out-of-scope / followup**：修改点在关联仓 `magpie-gemini-web` 的 `scripts/backend-release.mjs` 与 `patches/argon.patch`，不在本仓授权范围内；同步插件仓时一并处理。

## 出处

- oracle 观察项第 1 条（`.agents/notes/task-502-gate-oracle.md`，原文定位 `backend-release.mjs:64-66` 与 `argon.patch:3754-3755`）。
- 主代理核对纠正：条件链顺序与 `server.go` 行号按本文件第一节为准（oracle 原文 `server.go:13` 为旧声明位置，现状在 `:17`）。
