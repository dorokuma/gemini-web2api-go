# release/v5.0.0 首轮 C 档门禁审查 — 非阻断观察项登记

来源：第 1～2 轮 C 档门禁审查（Reviewer / Oracle 汇总）的非阻断观察项，本轮已闭环全部
must fix / should fix；以下 8 条为不阻断发版的边界说明与后续维护线索。

## 观察项

1. `[pattern:ci-branch-image-dryrun]`：`docker.yml` 在分支上不传 `inputs.tag`
   手动触发时仍按 `type=ref,event=branch` 推送对应分支镜像标签，但不推送
   `:latest`（`:latest` 仅由全仓最高 SemVer 正式版 tag 经 `latest_guard`
   判定后触发）。

2. `[pattern:reproducible-build-runner-scope]`：`release.yml` 双次编译
   SHA-256 比对用于拦截同 runner 构建期非确定性扰动，`go 1.27.1` 取自
   `go.dev/dl` 官方归档版本；跨不同操作系统/内核补丁 runner 的字节级复现
   不在该单步断言覆盖内。

3. `[pattern:dockerfile-prepare-base-and-doc-healthcheck]`：基线存量
   `Dockerfile:36` `prepare` 阶段使用 `alpine:3.20` 仅执行 `mkdir/chown`，
   且容器健康检查定义在 `docker-compose.yml:47-51` 而非 `Dockerfile`
   `HEALTHCHECK` 指令，留待后续独立维护迭代评估。

4. `[pattern:ci-concurrency-key-normalization]`：`release.yml` 与
   `docker.yml` 顶层 `concurrency.group` 使用 `inputs.tag || github.ref`，
   同一 tag 在 tag push（`refs/tags/vX.Y.Z`）与 `workflow_dispatch`
   （`vX.Y.Z`）同时触发时键名未归一；因 `overwrite_files: false` 与
   draft-first 保护不会产生资产覆写，仅可能导致后跑工作流报 422 判红，
   后续可归一为 `github.ref_name`。

5. `[pattern:ghcr-tag-immutability-policy]`：`release.yml` 已锁定已发布
   二进制资产不可覆写，`docker.yml` 的 `latest_guard` 防止 `:latest`
   回退，但同 `vX.Y.Z` 重跑 `docker.yml` 时仍会重推镜像 tag（且基础镜像
   `distroless`/`alpine:3.20` 为滚动 tag）；是否增加同版本镜像禁重推
   守卫留待后续发布策略迭代。

6. `[pattern:awk-v-escape-preprocessing]`：workflow 层 `awk -v s="$cand"`
   会对反斜杠做 POSIX 转义预处理，使校验值与原始变量值在含 `\` 时
   不同源；当前已被 `git check-ref-format`、`Makefile`
   `ENVIRON["TAG_VAL"]` 与二进制版本号断言三层 fail-closed 拦截，
   后续可前置纯 shell 字符白名单简化边界。

7. `[pattern:dockerfile-gotoolchain-local-sync]`：`Dockerfile` 固定
   `golang:1.27.1-alpine` 与 `ENV GOTOOLCHAIN=local` 后，未来升级
   `go.mod` 的 Go 版本时必须同步升级 `Dockerfile` 基础镜像版本，
   否则容器构建会按预期硬失败而非静默拉取新版工具链。

8. `[pattern:release-latest-guard-symmetry]`：`release.yml` 采用
   `make_latest: legacy`（按提交时间判定 Latest），而 `docker.yml`
   采用 `latest_guard`（按全仓最高正式版 SemVer tag 判定 `:latest`）；
   若未来在更晚提交上为旧主版本发维护补丁，两侧 Latest 口径可能分叉，
   届时可在 `release.yml` 同步引入 `latest_guard`。
