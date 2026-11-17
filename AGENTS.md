# gemini-web2api-go
档位：L2

## make check 覆盖范围
- `gofmt -l .` 格式检查（有未格式化文件即失败）
- `internal/app/server.go` 中 `var Version = "dev"` 可注入校验
- `go vet ./...`
- `go test ./... -count=1`
- `go build -buildvcs=false -o /dev/null .`

## make release-check TAG=vX.Y.Z 覆盖范围
- `TAG` 必须为严格语义化版本 `vX.Y.Z`（拒绝 `-magpie.N` 等预发布后缀）
- `var Version = "dev"` 同 `check`
- `CHANGELOG.md` 必须存在对应 `## X.Y.Z` 小节且含非空实质变更
- 若本地已存在该 tag，必须为带注解（annotated）对象

## 项目铁律
- 全局流程（分支、worktree、门禁、收尾与提交规范）按 SYSTEM §4。
- 仓库与测试用例中严禁出现真实 Cookie、密钥、Token；配置示例一律用占位符。
- 不留 `*.bak*` 备份文件，回滚靠 git。
- `.agents/notes/` 采用白名单模式：默认忽略入库，值得公开的结论性/缺陷登记性笔记 `git add -f` 逐条入库；入库前确认不含凭据与未脱敏环境细节。
- `internal/app/gemini.go` 当前硬编码锁定 `useWC := false`；未来启用 WebChannel 前必须先逐项完成 `.agents/notes/webchannel-enable-prerequisites.md` 所列全部 6 项前置条件。

## 发版规范（本仓专有）
- 正式版标签使用带注解的标准语义化版本 `vX.Y.Z`（annotated git tag，起点 `v5.0.0`），不再使用 `-magpie.N` 后缀。
- `internal/app/server.go` 中 `var Version = "dev"` 必须保持为可注入变量，禁止改写为常量或硬编码版本号；构建统一以 `-ldflags -X gemini-web2api/internal/app.Version=<去 v 版本号>` 注入。
- 每次发版前必须在中文 `CHANGELOG.md` 补齐对应 `## X.Y.Z` 实质变更小节，并通过 `make release-check TAG=vX.Y.Z`。
- 本仓是 6 平台预编译二进制与 GHCR 多架构镜像的唯一权威发布源；下游插件 `@dorokuma/magpie-gemini-web`（`/root/workspace/magpie-gemini-web`，自 v0.15.0 起）通过 `lib/backend-release.json` 直接固定本仓正式 Release 二进制 SHA-256。

## 关联仓库
- `/root/workspace/magpie-gemini-web` -> `dorokuma/magpie-gemini-web`（下游 Magpie 插件仓 @dorokuma/magpie-gemini-web，自 v0.15.0 起固定本仓正式 Release 二进制）
## Release Policy / 发布规范

本节是本仓库发版的强制规则。仓库已有的更具体版本、构建或发布渠道规则优先，但不得降低以下底线。

1. **版本与变更记录**：遵循 Semantic Versioning，正式版本使用 `vX.Y.Z`。破坏性变更升 MAJOR，新功能升 MINOR，兼容修复升 PATCH。任何用户可见行为、运行时/API/包接口、依赖、安全性或分发产物变化，都必须在同一发版提交中更新版本元数据并写入 CHANGELOG/Release Notes；纯文档且不产生可分发产物时可免升版，但须在规则或 Release Notes 中明确说明。
2. **发布门禁**：发版前工作树必须干净，版本号、CHANGELOG 与发布说明必须一致；必须运行仓库规定的测试、构建、lint、类型检查及必要的安全检查，且全部通过。若仓库提供 `release-check`、`make release-check`、版本检查脚本或同等门禁，必须原样通过，不得绕过。
3. **发布顺序**：先提交并推送发布提交到目标分支 → 在该发布提交上创建 Tag（默认使用带注解的 `git tag -a vX.Y.Z -m "vX.Y.Z"`；仅当现有发布工具明确要求轻量 Tag 时可例外）→ 推送 Tag → 创建**非草稿** GitHub Release → 再发布 npm、PyPI、容器镜像等关联分发渠道。不得只推分支或只打 Tag 就宣称发版完成。
4. **GitHub Release 产物**：标题使用 `vX.Y.Z`，正文必须包含实质性变更摘要、升级/破坏性变更说明及验证结果；二进制、安装包、插件包等适用产物必须作为 Release assets 上传，并提供校验和（如适用）。**Tag 本身不等于完整发版，必须同时存在可访问的 GitHub Release。**
5. **发布后验收**：至少执行并确认 `git ls-remote --tags origin refs/tags/vX.Y.Z`、Tag 指向发布提交、`gh release view vX.Y.Z` 可访问且不是 draft；若发布到包仓库/镜像仓库，还必须核验对应版本、摘要或安装链接可用。
6. **不可变性**：已发布 Tag 不得复用、移动或删除，不得 force push 发布分支或 Tag，不得从未提交、未经测试或未通过门禁的树发布。发布后发现问题须通过新的 patch 版本修复，禁止覆盖历史 Release。
7. **例外必须显式化**：归档、已停用、被替代、仅文档或无可分发产物的仓库，必须在项目规则中明确写出“不发新版本”及历史 Tag/Release 的处置方式；例外不能用于绕过上述发布闭环。

### Repository-specific release contract

- 本仓具体权威流程见 `AGENTS.md` 既有 `make release-check TAG=vX.Y.Z`、CHANGELOG 与后端二进制规则；不得以通用条款替代该门禁。
