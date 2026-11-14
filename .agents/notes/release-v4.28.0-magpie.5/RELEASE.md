# RELEASE v4.28.0-magpie.5 — A 档事实记录

本文件只记录可核事实与其来源，不含推测。证据原件同目录逐字保存（sha256 对照见 README.md），来源目录：`/tmp/herdr-role-sessions/default/role-worker-gw2api-verify/`（核验会话产物，临时路径仅作来源标注）。

## A. 发布产物

- Release URL：`https://github.com/dorokuma/gemini-web2api-go/releases/tag/v4.28.0-magpie.5`
- `release-view.json`：`tagName`=`v4.28.0-magpie.5`，`isDraft`=false，`isPrerelease`=false，资产共 **7 个**（6 平台二进制 + `SHA256SUMS.txt`）。
- 各资产 name / size（照 `release-view.json` 抄录）：

  | name | size (bytes) |
  |---|---|
  | gemini-web2api-go_v4.28.0-magpie.5_darwin_amd64 | 18315296 |
  | gemini-web2api-go_v4.28.0-magpie.5_darwin_arm64 | 17297714 |
  | gemini-web2api-go_v4.28.0-magpie.5_linux_amd64 | 17940642 |
  | gemini-web2api-go_v4.28.0-magpie.5_linux_arm64 | 16842914 |
  | gemini-web2api-go_v4.28.0-magpie.5_windows_amd64.exe | 18275840 |
  | gemini-web2api-go_v4.28.0-magpie.5_windows_arm64.exe | 16908800 |
  | SHA256SUMS.txt | 692 |

- `sha256sum -c SHA256SUMS.txt` 输出原文（`sha256sum-c.txt`）：

  ```
  gemini-web2api-go_v4.28.0-magpie.5_darwin_amd64: OK
  gemini-web2api-go_v4.28.0-magpie.5_darwin_arm64: OK
  gemini-web2api-go_v4.28.0-magpie.5_linux_amd64: OK
  gemini-web2api-go_v4.28.0-magpie.5_linux_arm64: OK
  gemini-web2api-go_v4.28.0-magpie.5_windows_amd64.exe: OK
  gemini-web2api-go_v4.28.0-magpie.5_windows_arm64.exe: OK
  EXIT=0
  ```

- host linux-arm64 `--version` 输出原文（`version.txt`）：

  ```
  ./gemini-web2api-go_v4.28.0-magpie.5_linux_arm64 --version
  gemini-web2api-go 4.28.0-magpie.5
  EXIT=0
  ```

## B. Docker 镜像

- 版本 tag 实际带 `v`：`ghcr.io/dorokuma/gemini-web2api-go:v4.28.0-magpie.5`（manifest 见 `docker-manifest-v.json`）。不带 `v` 的 `4.28.0-magpie.5` 返回 `manifest unknown`（stderr 原文见 `docker-manifest.err`；当时 stdout 为空输出，该 0 字节文件已剔除）。
- OCI index（`docker-manifest-v.json`）：
  - `linux/amd64` = `sha256:a7406ce68b8975a3f900e7b36d64ad11b78850ff1f506806a491b3cc75751140`
  - `linux/arm64` = `sha256:d2a0ae01bc2e73059a48ab0bdfa24c692bbd492c4d9f20135f33ccea0f0537ae`
  - 另 2 条 `unknown/unknown`：`sha256:c855affa284fe82e813f6691f3e19296cd587fbe7e921b4b98d3fa161cdd75bf`、`sha256:7a11cf2ab64e424dd2c18d721e59284001c5a1b47b60caa23f06be9f8560877a`（attestation 类条目）
- `latest` 的 manifest（`docker-manifest-latest.json`）与 `v4.28.0-magpie.5` 的全部 4 条 digest **逐条相同**——仅作观测记录，不据此宣称 latest 的契约或镜像内版本。

## C. 合并与推送

- merge commit `8836c5efa77821a31074f72ae433098ef16b110f`；两个父：`8647ba1693400ee3dfeea6954d2ea20720ff710f`（原 main）、`38d340ceaf15186bd814126990a96fe0cbdc56c4`（工作分支）。
- merge tree = `38d340c^{tree}` = `86e8a8fa1312e4952b407cb8c2f52f297182cc4d`（`git diff --quiet 38d340c HEAD` EXIT=0）。
- tag 对象 `ec12d2e9f09703bdc2567d18174e67acdbd5ef17`，peeled `8836c5efa77821a31074f72ae433098ef16b110f`。
- push 结果（**逐字引用自 t176 worker 原始回报**，未经本步重验）：
  - `git push origin main` → EXIT=0，`8647ba1..8836c5e  main -> main`
  - `git push origin v4.28.0-magpie.5` → EXIT=0，`* [new tag]`
- 远端读回（t176 worker 回报）：`refs/heads/main`=`8836c5e…`、`refs/tags/v4.28.0-magpie.5`=`ec12d2e…`、`^{}`=`8836c5e…`，三者与本地一致。

## D. CI

- run `43590981269`（Release）：`headSha`=`8836c5efa77821a31074f72ae433098ef16b110f`、`event`=`workflow_dispatch`、`conclusion`=`success`、job `build`=success（`run-release.json`）。
- run `43590984062`（Docker）：同 headSha、同 event、`conclusion`=`success`、job `docker`=success（`run-docker.json`）。
- 触发事实：**push 事件未产生任何 run**（多次查询 `total_count=0`）；手动 `workflow_dispatch` 后立即排队并成功。**原因未确证**（`actions/permissions` 在 PUT 前即为 `enabled:true`，幂等 PUT 前后无变化），本文件不断言「已确证为 fork 权限故障」。

## E. 授权与操作原件

- 授权渠道：用户当次明确授权「你自己开权限」。
- 实际操作：
  - `PUT /repos/dorokuma/gemini-web2api-go/actions/permissions {enabled:true, allowed_actions:"all"}`（幂等，读回无变化）
  - `gh workflow run release.yml --ref v4.28.0-magpie.5`（RC=0，产生 run 43590981269）
  - `gh workflow run docker.yml --ref v4.28.0-magpie.5`（RC=0，产生 run 43590984062）

## F. 已知偏差（不美化）

- merge commit 的 `Co-Authored-By` trailer 缺少 `Opus` 字样（`grep -c Opus`=0）；已发布历史不改写、不补提交、不移动 tag。
- 本节仅记录事实，不作归因推断。
