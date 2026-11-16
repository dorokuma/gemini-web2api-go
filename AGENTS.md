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
