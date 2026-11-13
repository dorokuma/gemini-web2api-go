# gemini-web2api-go
档位：L1

## make check 覆盖范围
- `gofmt -l .` 格式检查（有未格式化文件即失败）
- `go vet ./...`
- `go test ./... -count=1`
- `go build`（本仓库为可执行程序，构建产物已被 .gitignore 忽略）

## 项目铁律
- 全局流程（分支、worktree、门禁、收口、提交规范）按 SYSTEM §4。
- 仓库与测试用例中严禁出现真实 Cookie、密钥、Token；配置示例一律用占位符。
- 不留 `*.bak*` 备份文件，回滚靠 git。
- `.agents/notes/` 采用白名单模式：默认忽略入库，值得公开的结论性/缺陷登记性笔记 `git add -f` 逐条入库；入库前确认不含凭据与未脱敏环境细节。

## 关联仓库
- `/root/workspace/magpie-gemini-web` -> `dorokuma/magpie-gemini-web`（magpie 插件补丁来源仓）
