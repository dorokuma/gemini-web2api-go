# verifier 判据报告 — GW2API-502 基线对照 [MARK-VERIFIER-GW2API-502-BASELINE-20261114]

## 环境
- 副本路径：`/tmp/vfy-a6677a01-base`、`/tmp/vfy-a6677a01-cand`（均已 `git clone --no-hardlinks` 自被审 worktree，均 `git remote remove origin`，交付前已 `rm -rf` 删除，见末节）
- baseline 基线 commit：`8647ba1693400ee3dfeea6954d2ea20720ff710f`
- candidate 检出 commit：`c57860dc0f8b080537c1f4ec586d68e3f3b3ca0d`（`git checkout c57860d`，非 HEAD `a9bbaf3`，无需 md-only 旁证）
- 被审 worktree：`/root/workspace/.wt/gemini-web2api-go/fix-upstream-502-diagnostics`，全程只读，仅 `cp` 其 `internal/app/toolgate_test.go` 至 baseline clone
- 待审快照/文件清单哈希：
  - 源 `internal/app/toolgate_test.go` sha256=`f06c33d712cea2782ffdf679b8c0c17a8ba2805e0459dd8d1abb363f5382fdbb`（37134 字节）
  - baseline 副本同文件 sha256=`f06c33d712cea2782ffdf679b8c0c17a8ba2805e0459dd8d1abb363f5382fdbb`（37134 字节）—— 相同
  - git blob id：`601cb7e8c5bdaaa699f5b707884d231e0a563705`（`git rev-parse c57860d:...` 与 `git hash-object` 一致）
- 隔离方式：两独立本地 clone（`--no-hardlinks`），均去 origin；baseline 侧仅叠加单个测试文件（unstaged），candidate 侧原样 `c57860d` 干净检出
- 干净：candidate 侧 `git status --porcelain` 空（干净）；baseline 侧仅 ` M internal/app/toolgate_test.go` 一处变动，`git diff --name-only` 仅该文件、`--stat` 仅 `1 file changed, 567 insertions(+)`，无其它业务修复引入

## 快照指纹
baseline clone（HEAD=8647ba1）：
- `git rev-parse HEAD` = `8647ba1693400ee3dfeea6954d2ea20720ff710f`
- `git ls-files -s | sha256sum` = `dd4e14cf58c9918a2a82e606369b6ab1436d5b5aef6f48c959e59fbc5ea4053f`
- `git archive HEAD | sha256sum` = `827ed2aa024f268655c56de7b547a90fb71ca6d37462732ec2275fbea4394ca6`
- 注：baseline 判据运行时含一个未暂存覆盖文件 `internal/app/toolgate_test.go`（sha256 同上）；archive/ls-files 指纹为 HEAD 树，叠加文件哈希已单列
candidate clone（HEAD=c57860d）：
- `git rev-parse HEAD` = `c57860dc0f8b080537c1f4ec586d68e3f3b3ca0d`
- `git ls-files -s | sha256sum` = `7eabe6f2f4e465ce59e136a0fae25757ce136c63921cca2909c4e8a31490aa43`
- `git archive HEAD | sha256sum` = `95838c733cc93a5182c3f960756ad8a4a7ca05ac92a88a1422477453b605ccc6`

## 判据（逐条可复制命令）
判据唯一：双快照对照（基线红 → 待审绿）。两侧内层命令逐字一致：
```
GOTOOLCHAIN=go1.27.1 go test -count=1 -v ./internal/app -run '^(TestChatCompletions502StructuredEmptyFrame|TestChatCompletions502FailureReply|TestChatCompletions502ReasoningOnlyIsEmptyContent|TestChatCompletionsMultiTurnNoContentFrame|TestChatCompletionsLegitPathsNotFalsePositive|TestChatCompletionsMediaArtifactNotFalsePositive)$' ; echo "EXIT=$?"
```
baseline 侧经 `/root/.local/bin/verifier-expect-fail` 包装：
- `--expect-re 'reason=|requests.error|upstream_model=|frames=|content_frame|error 缺字段'`
- `--reject-re 'build failed|undefined:|no tests to run'`
- `--min-tests 6`
预注册门限：baseline 外层 RC=0、内层 RC 非零、`=== RUN` 数≥6、命中 expect-re、不命中 reject-re；candidate 退出码 0、六顶层全 PASS、两合法子项三子用例全 PASS。

## 对照判据执行记录
首轮（baseline-inner-output.txt）外层 RC=1：包装器解析 inner_exit 时读子进程退出码，而内层 `sh -c` 末句为 `echo` 使进程退出码 0（`EXIT=1` 行未被当作 inner_exit）。此为判据包装机械条件不符，非被测对象行为异常；按「意外失败即停」记录，随后以 `; RC=$?; echo "EXIT=$RC"; exit $RC` 使内层退出码透传为子进程退出码，重跑同一判据一次（仍同一 baseline clone，属机械包装修正非业务改动）。

第二轮（baseline-inner-output-r2.txt）：
- baseline 外层退出码：0；内层退出码：1；`verifier-expect-fail: PASS inner_exit=1 match="error 缺字段" tests=9`
- candidate 外层退出码：0；内层 `EXIT=0`，`ok gemini-web2api/internal/app`

## 证据
baseline（r2，有效轮）原始输出（全文，未截断，3613 字节）：
```
=== RUN   TestChatCompletions502StructuredEmptyFrame
    toolgate_test.go:498: error 缺字段 "reason": {"error":{"message":"upstream error: upstream returned no content frame (raw 820 bytes)"}}
    toolgate_test.go:498: error 缺字段 "upstream_model": {"error":{"message":"upstream error: upstream returned no content frame (raw 820 bytes)"}}
    toolgate_test.go:498: error 缺字段 "frames": {"error":{"message":"upstream error: upstream returned no content frame (raw 820 bytes)"}}
    toolgate_test.go:498: error 缺字段 "content_frame": {"error":{"message":"upstream error: upstream returned no content frame (raw 820 bytes)"}}
    toolgate_test.go:498: error 缺字段 "hint": {"error":{"message":"upstream error: upstream returned no content frame (raw 820 bytes)"}}
    toolgate_test.go:505: reason="" 应为 no_content_frame（空 text/无 reasoning 的帧不算内容帧）
    toolgate_test.go:508: upstream_model="" 应取到自报 '3.6 Flash'（跟 okRaw 同 hex 同显示名槽位）
    toolgate_test.go:511: frames=0 应为确切 2（一个 wrb.fr 内容帧 + 一个 wrb.fr 结束帧）
    toolgate_test.go:517: hint 应是一句中文提示
    toolgate_test.go:528: requests.error 应带 [no_content_frame] 前缀，得到 "upstream returned no content frame (raw 820 bytes)"
--- FAIL: TestChatCompletions502StructuredEmptyFrame (0.01s)
=== RUN   TestChatCompletions502FailureReply
    toolgate_test.go:573: reason="" 应为 failure_reply
    toolgate_test.go:576: upstream_model="" 应取到自报 '4 Argon'
    toolgate_test.go:579: frames=0 应为确切 1（>4KB 话术帧也只是一个 wrb.fr 帧，截断不能虚报）
    toolgate_test.go:582: hint 应是一句中文提示
    toolgate_test.go:590: requests.error 应带 [failure_reply] 前缀，得到 "upstream returned a failure reply (inner[31] set)"
--- FAIL: TestChatCompletions502FailureReply (0.02s)
=== RUN   TestChatCompletions502ReasoningOnlyIsEmptyContent
    toolgate_test.go:632: reason="" 应为 empty_content（有思考帧但正文为空）
    toolgate_test.go:635: 思考链帧算内容帧，content_frame 应为 true
--- FAIL: TestChatCompletions502ReasoningOnlyIsEmptyContent (0.00s)
=== RUN   TestChatCompletionsMultiTurnNoContentFrame
    toolgate_test.go:711: reason="" 应为 no_content_frame（中文报错串也得归对类，不能漏成 upstream_error）
    toolgate_test.go:727: requests.error 应带 [no_content_frame] 前缀，得到 "续接无内容帧（会话可能已失效，raw 126 字节）"
--- FAIL: TestChatCompletionsMultiTurnNoContentFrame (0.02s)
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive/纯代码产物（codeMarkerRe_回退后非空）
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive/纯_tool_call（合法围栏解析出调用）
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive/canvas_文档（immersive_里抠出_DOCTYPE）
--- PASS: TestChatCompletionsLegitPathsNotFalsePositive (0.00s)
    --- PASS: TestChatCompletionsLegitPathsNotFalsePositive/纯代码产物（codeMarkerRe_回退后非空） (0.00s)
    --- PASS: TestChatCompletionsLegitPathsNotFalsePositive/纯_tool_call（合法围栏解析出调用） (0.00s)
    --- PASS: TestChatCompletionsLegitPathsNotFalsePositive/canvas_文档（immersive_里抠出_DOCTYPE） (0.00s)
=== RUN   TestChatCompletionsMediaArtifactNotFalsePositive
--- PASS: TestChatCompletionsMediaArtifactNotFalsePositive (0.00s)
FAIL
FAIL	gemini-web2api/internal/app	0.085s
FAIL
EXIT=1
verifier-expect-fail: PASS inner_exit=1 match="error 缺字段" tests=9
```
- expect-re 命中原文：`error 缺字段`（亦命中 `reason=`、`requests.error`、`upstream_model=`、`frames=`、`content_frame`）
- reject-re 未命中：无 `build failed`、`undefined:`、`no tests to run`
- 顶层 `=== RUN` 数：9（≥6）；实际执行顶层用例 6 个（另 3 行是 LegitPaths 的子用例 `=== RUN`，按顶层去重为 6 个顶层用例）
- 耗时：约 13s（首轮）/ 1s（次轮，Go 缓存命中）

candidate 原始输出（全文，未截断，1412 字节）：
```
=== RUN   TestChatCompletions502StructuredEmptyFrame
--- PASS: TestChatCompletions502StructuredEmptyFrame (0.01s)
=== RUN   TestChatCompletions502FailureReply
--- PASS: TestChatCompletions502FailureReply (0.00s)
=== RUN   TestChatCompletions502ReasoningOnlyIsEmptyContent
--- PASS: TestChatCompletions502ReasoningOnlyIsEmptyContent (0.00s)
=== RUN   TestChatCompletionsMultiTurnNoContentFrame
--- PASS: TestChatCompletionsMultiTurnNoContentFrame (0.00s)
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive/纯代码产物（codeMarkerRe_回退后非空）
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive/纯_tool_call（合法围栏解析出调用）
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive/canvas_文档（immersive_里抠出_DOCTYPE）
--- PASS: TestChatCompletionsLegitPathsNotFalsePositive (0.00s)
    --- PASS: TestChatCompletionsLegitPathsNotFalsePositive/纯代码产物（codeMarkerRe_回退后非空） (0.00s)
    --- PASS: TestChatCompletionsLegitPathsNotFalsePositive/纯_tool_call（合法围栏解析出调用） (0.00s)
    --- PASS: TestChatCompletionsLegitPathsNotFalsePositive/canvas_文档（immersive_里抠出_DOCTYPE） (0.00s)
=== RUN   TestChatCompletionsMediaArtifactNotFalsePositive
--- PASS: TestChatCompletionsMediaArtifactNotFalsePositive (0.00s)
PASS
ok  	gemini-web2api/internal/app	0.044s
EXIT=0
```
- 六个顶层用例逐个结果：
  - TestChatCompletions502StructuredEmptyFrame → PASS
  - TestChatCompletions502FailureReply → PASS
  - TestChatCompletions502ReasoningOnlyIsEmptyContent → PASS
  - TestChatCompletionsMultiTurnNoContentFrame → PASS
  - TestChatCompletionsLegitPathsNotFalsePositive → PASS
  - TestChatCompletionsMediaArtifactNotFalsePositive → PASS
- 三个合法子项逐个结果（LegitPathsNotFalsePositive 下）：
  - /纯代码产物（codeMarkerRe_回退后非空） → PASS
  - /纯_tool_call（合法围栏解析出调用） → PASS
  - /canvas_文档（immersive_里抠出_DOCTYPE） → PASS
  - （MediaArtifactNotFalsePositive 无子用例，顶层单行 PASS）

## 判据有效性（机械门限比对）
- baseline：外层 RC=0 ✓；内层 RC=1（非零）✓；`=== RUN` 9 ≥6 ✓；expect-re 命中 `error 缺字段` ✓；reject-re 三项均未命中 ✓ → 达标
- candidate：RC=0 ✓；六顶层全 PASS ✓；三合法子项全 PASS ✓ → 达标

## 实测值与门限比对
- baseline 内层 go test 退出码=1（预期失败，包装器确认 inner_exit=1）
- candidate 退出码=0
- 测试用例数：baseline 顶层 `=== RUN` 去重后 6 用例；candidate 6 顶层 + 3 子用例
- 产物：测试无构建产物落盘；证据文件见下

## 空转底线
- 非零用例：两侧均真实执行 ≥6 顶层用例（baseline `tests=9` 含子用例计数，顶层去重=6）；candidate 输出含完整 PASS 序列，非空跑。

## 结果五分类
- 判据（双快照对照）：通过（外层 RC=0、基线红 inner_exit=1、待审绿 RC=0、证据达标）。
  - 首轮基线运行记录为「证据不完整」（inner_exit 未被透传，包装条件未满足），已由次轮机械修正补足，不影响最终判据证据面。

## 证据文件（/tmp/herdr-role-sessions/default/role-verifier-a6677a01/）
- `baseline-inner-output.txt` — 3609 B, sha256=`bb7d52194db1e6dc4ac6e57876e4a184fb2fa80706a580c21ee4be6a80648f49`（首轮，inner_exit 未透传）
- `baseline-inner-output-r2.txt` — 3613 B, sha256=`a6cc3508a1809617fad34a8667915cf097302db9c87c95231736df6ed7195e4c`（有效轮）
- `candidate-output.txt` — 1412 B, sha256=`181462d528db7fededb3e007b6166f9f30113b6059353e0e384eaf9f798f703c`

## clone 清理确认
`rm -rf /tmp/vfy-a6677a01-base /tmp/vfy-a6677a01-cand` 已执行；`ls` 回报两路径均不存在；/tmp tmpfs 余量 4.0G。被审 worktree 未做任何写入。
