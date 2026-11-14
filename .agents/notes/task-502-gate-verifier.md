# task-502 门禁原件：verifier 初轮 C 档门禁报告（逐字）

## 来源附注

- 原路径：`/tmp/herdr-role-sessions/default/role-verifier-8496f4a0/report.md`
- 字节数：28633；sha256：`bd7648f685071da4d9ccacfa22afd9f2858c47c99be8e40e165f305448b86654`
- 说明：本文件为 verifier 初轮原件逐字副本（头部附注之外正文一字未改）；补证件在 `.agents/notes/task-502-evidence/task-502-gate-verifier-supplement.md`，不受本文件影响。

---

# C 档门禁验证报告 — fix-upstream-502-diagnostics / c57860d

**角色**：verifier（复现者，只交证据，不作验收判断）
**会话**：HC_TERMINAL_ID=t142，报告目录 role-verifier-8496f4a0
**工具限制遵守**：未使用 codegraph；仅用 ctx_run + rg；同一命令失败未重复裸试（判据 7/8 首轮因反例代码不可编译而包装失败，属反例构造调整，非判据重试）。

---

## 一、环境

| 项 | 值 |
|---|---|
| 被审工作区（只读） | /root/workspace/.wt/gemini-web2api-go/fix-upstream-502-diagnostics |
| 分支 | fix/upstream-502-diagnostics |
| 待审 commit | c57860dc0f8b080537c1f4ec586d68e3f3b3ca0d |
| rebase 基线 | 8647ba1 |
| tree SHA | 2283cfa21598986de9640ed939343ce601077c3d |
| git archive(c57860d) sha256 | 95838c733cc93a5182c3f960756ad8a4a7ca05ac92a88a1422477453b605ccc6 |
| 工作区 git status --porcelain -uall | 仅 `?? .codegraph/codegraph.lock`（工具锁，非业务改动） |
| git diff HEAD / --cached | 均为空（待审内容全部已在 commit 内） |
| git ls-files -s sha256 | 7eabe6f2f4e465ce59e136a0fae25757ce136c63921cca2909c4e8a31490aa43 |
| 隔离方式 | `git clone --no-hardlinks <主仓> <副本>` + `git checkout c57860d` + `git remote remove origin`；副本为独立 .git 目录，非 worktree，无 origin |
| 副本文件清单 sha256（4 个原样副本一致） | 70dd2999aec12102b60f73c7a7f032135171564a3a7149a7f96ef0ecc65cefa9 |
| 初始状态是否干净 | 是。各副本 `git status --porcelain -uall` 为空，HEAD 与 tree 均等于登记值 |
| Go 工具链 | 系统 go1.26.8；判据 6 用 GOTOOLCHAIN=go1.27.1（`go version` 回显 go1.27.1 linux/arm64） |
| /tmp 挂载 | tmpfs 5.9G，验证期间峰值 56% |

**副本清单（判据 → 副本）**：

| 副本 | 判据 | 形态 | 结局 |
|---|---|---|---|
| /tmp/vfy-502-A | 1（初版） | git archive 解包快照 | 非 git 仓库导致 dev-audit 空转，作废重建 |
| /tmp/vfy-502-A-git | 1、2、3、4、5 | git clone @ c57860d | 已删除 |
| /tmp/vfy-502-B | 6 | git clone @ c57860d | 已删除 |
| /tmp/vfy-502-C | 7 基线轮 | git clone @ c57860d + 反例改动 | 已删除 |
| /tmp/vfy-502-C2 | 7 待审轮 | git clone @ c57860d 原样 | 已删除 |
| /tmp/vfy-502-D | 8 基线轮 | git clone @ c57860d + 反例改动 | 已删除 |
| /tmp/vfy-502-D2 | 8 待审轮 | git clone @ c57860d 原样 | 已删除 |

> 说明：判据 1 首次在 `git archive` 快照（无 .git）上跑 `dev-audit .`，脚本内 `[ -e "$repo/.git" ] || continue` 直接跳过，EXIT=0 且 stdout 0 字节——空转，不满足「含 L1/OK」。已改用真实 git clone 重做；此处置换如实记录，非判据重试。

---

## 二、判据与证据

### 判据 1（门禁基础）— 副本 A-git

**预注册门限**：`dev-audit .` EXIT=0 且输出含 `L1`/`OK`；`make check` EXIT=0（gofmt 0 / vet 0 / test 含 ok / build 0）；`go test -v ./... -count=1` 顶层 `--- PASS:` >= 20。

**命令与结果**：

```
$ /root/.local/bin/dev-audit .          # cwd=/tmp/vfy-502-A-git
.                      L1  OK
DEV_AUDIT_EXIT=0
```
- `L1` 出现 1 次，`OK` 出现 1 次，`FAIL` 出现 0 次，stdout 30 字节（非空转）。

```
$ make check                            # cwd=/tmp/vfy-502-A-git
go vet ./...
go test ./... -count=1
?   	gemini-web2api	[no test files]
ok  	gemini-web2api/internal/app	18.087s
go build -o /dev/null .
MAKE_CHECK_EXIT=0
ELAPSED_SECONDS=32
```

分解项（另跑以取得显式退出码）：

| 子命令 | EXIT | 关键输出 |
|---|---|---|
| `gofmt -l .` | 0 | 输出为空（0 个未格式化文件） |
| `go vet ./...` | 0 | 无输出 |
| `go test ./... -count=1` | 0 | `ok gemini-web2api/internal/app 18.454s` + `? gemini-web2api [no test files]` |
| `go build -o /dev/null .` | 0 | 无输出（产物丢弃，构建成功） |

```
$ go test -v ./... -count=1             # cwd=/tmp/vfy-502-A-git
TEST_V_EXIT=0
TOP_LEVEL_PASS=183
TOP_LEVEL_FAIL=0
TOP_LEVEL_SKIP=3
ok  	gemini-web2api/internal/app	18.124s
?   	gemini-web2api	[no test files]
```

顶层 SKIP 3 个：`TestArgonProtocolProbe`、`TestArgonTimingProbe`、`TestUploadProbe`（均为环境探针默认跳过，非本改动引入）。

**判据有效性**：183 >= 20，达标。

### 判据 2（结构化字段）— 副本 A-git

**定位方式**：`rg 'upstream_model'` 命中 34 处 / 11 文件；`rg '^func Test'` 命中 187 处。502 body 字段断言集中在 `internal/app/toolgate_test.go`（c57860d 新增 567 行）。含 `reason`/`upstream_model`/`frames`/`content_frame`/`hint` 断言的顶层用例 4 个。

**命令与结果**：

```
$ go test -v -run 'TestChatCompletions502StructuredEmptyFrame|TestChatCompletions502FailureReply|TestChatCompletions502ReasoningOnlyIsEmptyContent|TestChatCompletionsMultiTurnNoContentFrame' ./internal/app -count=1
EXIT=0
=== RUN   TestChatCompletions502StructuredEmptyFrame
--- PASS: TestChatCompletions502StructuredEmptyFrame (0.01s)
=== RUN   TestChatCompletions502FailureReply
--- PASS: TestChatCompletions502FailureReply (0.02s)
=== RUN   TestChatCompletions502ReasoningOnlyIsEmptyContent
--- PASS: TestChatCompletions502ReasoningOnlyIsEmptyContent (0.00s)
=== RUN   TestChatCompletionsMultiTurnNoContentFrame
--- PASS: TestChatCompletionsMultiTurnNoContentFrame (0.02s)
PASS
ok  	gemini-web2api/internal/app	0.103s
```

**实际 JSON 形态**（源码 `internal/app/upstream_err.go` `upstreamErrorJSON` 返回值，`writeUpstreamError` 包成 `{"error": …}` 写 502）：

```json
{
  "error": {
    "message":        "upstream error: <原始错误串>",
    "type":           "upstream_error",
    "reason":         "<机读原因码>",
    "upstream_model": "<上游自报显示名，解析不到为 \"unknown\">",
    "frames":         <int, wrb.fr 条目总数>,
    "content_frame":  <bool>,
    "hint":           "<一句中文提示>"
  }
}
```

Google 原生形状（`geminiErrorJSON`）另挂 `details[0]` 同款五字段。

reason 码全集（`upstream_err.go` const 块）：`no_content_frame` / `failure_reply` / `upstream_timeout` / `empty_content` / `upstream_http` / `upstream_error`。

测试断言的实测取值（抄录 `toolgate_test.go` 断言依据）：

| 用例 | reason | upstream_model | frames | content_frame | hint | message |
|---|---|---|---|---|---|---|
| 502StructuredEmptyFrame | `no_content_frame` | `3.6 Flash` | 2 | false | 非空 | 含 `upstream error: ` |
| 502FailureReply | `failure_reply` | `4 Argon` | 1 | — | 非空 | — |
| 502ReasoningOnlyIsEmptyContent | `empty_content` | — | — | true | — | — |
| MultiTurnNoContentFrame | `no_content_frame` | — | — | — | — | 含 `续接无内容帧` |

**判据有效性**：4/4 PASS，EXIT=0，字段断言齐备，达标。

### 判据 3（落库不受 log_requests 影响）— 副本 A-git

**定位方式**：`rg 'upstream_model'` 命中 `toolgate_test.go:433` 的 SELECT；该行属 `waitRequestRow` 辅助函数，被 4 个 502 用例复用。其中 `TestChatCompletions502StructuredEmptyFrame` 显式设置 `rtVal.LogRequests = false`（源码注释：`// 核对字段实名：日志开关只管 stderr，落库不受影响`），正是「关掉 log_requests 仍断言到 requests 行」的用例。

**命令与结果**：

```
$ go test -v -run TestChatCompletions502StructuredEmptyFrame ./internal/app -count=1
EXIT=0
=== RUN   TestChatCompletions502StructuredEmptyFrame
--- PASS: TestChatCompletions502StructuredEmptyFrame (0.02s)
PASS
ok  	gemini-web2api/internal/app	0.091s
```

**断言的列与前缀**（抄录 `toolgate_test.go:430-437` 与 520-530）：

```go
row := getDB().QueryRow(`SELECT status, COALESCE(error,''), COALESCE(upstream_model,'')n    FROM requests WHERE id>? AND endpoint='chat.completions' AND prompt_chars=? ORDER BY id DESC LIMIT 1`, since, promptChars)
```

- 列 1 `status`：断言 == 502，失败信息 `requests.status=%d 应为 502（log_requests=false 也要落库）`。
- 列 2 `error`：断言 `strings.HasPrefix(errStr, "[no_content_frame]")`，失败信息 `requests.error 应带 [no_content_frame] 前缀，得到 %q`。
- 列 3 `upstream_model`：断言 == `3.6 Flash`，失败信息 `requests.upstream_model=%q 应记自报 '3.6 Flash'`。

**schema 依据**（`internal/app/db.go:33-52`）：`CREATE TABLE IF NOT EXISTS requests` 含 `upstream_model TEXT`、`error TEXT`；老库迁移 `ALTER TABLE requests ADD COLUMN upstream_model TEXT`。

**实现依据**（`internal/app/upstream_err.go:221-235`）：`recordUpstreamFailure` 写 `fmt.Sprintf("[%s] %s", errDiagReason(err), err.Error())`，注释明写「落库**不受 log_requests 开关控制**（开关只管 stderr 日志，见 logf）」。

**判据有效性**：PASS，EXIT=0，达标。

### 判据 4（空正文二分）— 副本 A-git

**命令与结果**：

```
$ go test -v -run 'TestChatCompletions502ReasoningOnlyIsEmptyContent|TestChatCompletionsLegitPathsNotFalsePositive|TestChatCompletionsMediaArtifactNotFalsePositive|TestChatCompletions502StructuredEmptyFrame' ./internal/app -count=1
EXIT=0
=== RUN   TestChatCompletions502ReasoningOnlyIsEmptyContent
--- PASS: TestChatCompletions502ReasoningOnlyIsEmptyContent (0.00s)
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive/纯代码产物（codeMarkerRe_回退后非空）
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive/纯_tool_call（合法围栏解析出调用）
=== RUN   TestChatCompletionsLegitPathsNotFalsePositive/canvas_文档（immersive_里抠出_DOCTYPE）
--- PASS: TestChatCompletionsLegitPathsNotFalsePositive (0.01s)
    --- PASS: .../纯代码产物 (0.00s)
    --- PASS: .../纯_tool_call (0.00s)
    --- PASS: .../canvas_文档 (0.00s)
=== RUN   TestChatCompletionsMediaArtifactNotFalsePositive
--- PASS: TestChatCompletionsMediaArtifactNotFalsePositive (0.00s)
=== RUN   TestChatCompletions502StructuredEmptyFrame
--- PASS: TestChatCompletions502StructuredEmptyFrame (0.03s)
PASS
ok  	gemini-web2api/internal/app	0.093s
```

覆盖矩阵：

| 路径 | 用例 | 期望 | 实测 |
|---|---|---|---|
| `empty_content`（reasoning-only） | 502ReasoningOnlyIsEmptyContent | 502 | PASS |
| `no_content_frame`（帧有空结构无正文无 reasoning） | 502StructuredEmptyFrame | 502 | PASS |
| 代码产物 | LegitPathsNotFalsePositive/纯代码产物 | 200，body 含 ```python | PASS |
| 纯 tool_call | LegitPathsNotFalsePositive/纯_tool_call | 200，body 含 tool_calls | PASS |
| canvas 文档 | LegitPathsNotFalsePositive/canvas_文档 | 200，body 含 DOCTYPE | PASS |
| 媒体产物 | MediaArtifactNotFalsePositive | 200，body 含 data:image/png;base64, | PASS |

**reasoning-only 的处理选择**（抄录 `server.go:174-184` 注释与 `toolgate_test.go:597-600` 注释）：

> 「只有思考链（reasoning-only）也报错，不把思考链当正文糊弄出去：reasoning_content 字段是给『正文之外附带的推理过程』用的，拿它顶替 content 等于回一个『成功但没有答案』的假 200，跟空正文一样会误导客户端。报 empty_content 如实。」

即：`hasContentFrame(res.Raw)==true`（reasoningInLine 命中思考链槽位）但 `extractResponseText==""` → 归 `empty_content` 回 502，而非回 200 + 把思考链塞进 content。测试同时断言 `content_frame` 为 true（思考链帧算内容帧）以与 `no_content_frame` 区分。

**判据有效性**：6/6 PASS（含 3 个子用例），EXIT=0，达标。

### 判据 5（Version 注入，静态）— 副本 A-git 只读检索

| # | 检查项 | 门限 | 实测 | 结论 |
|---|---|---|---|---|
| 5-1 | `internal/app/server.go` | `var Version = "dev"`，非 const，不含写死版本号 | 第 17 行 `var Version = "dev"`；`grep 'const Version'` 无命中；`grep -E 'var Version *= *"[0-9]'` 无命中 | 满足 |
| 5-2 | `.github/workflows/release.yml` | 含 `-X …Version=` 注入与 `grep -nx 'var Version = "dev"'` 校验 | 第 47 行 `-ldflags="-s -w -X gemini-web2api/internal/app.Version=${GITHUB_REF_NAME#v}"`；第 29 行 `grep -nx 'var Version = "dev"' internal/app/server.go`（失败时 `echo "::error::…" && exit 1`） | 满足 |
| 5-3 | `Dockerfile` | 含 `ARG VERSION` 与 `-X …Version=` | 第 20 行 `ARG VERSION=dev`；第 25 行 `-ldflags="-s -w -X gemini-web2api/internal/app.Version=${VERSION}"` | 满足 |

逐字证据：

```
# server.go 14-17
// Version 由发布构建注入（-X gemini-web2api/internal/app.Version=<tag>），
// 源码里保持 "dev"：本地 go build 出来的是开发版，release workflow 按 tag 覆盖。
// 之前是个写死的常量，发新版忘了同步时 --version 会报旧版（见 .github/workflows/release.yml）。
var Version = "dev"

# release.yml 29（cat -A 原文，制表符为空格缩进）
          grep -nx 'var Version = "dev"' internal/app/server.go \$

# Dockerfile 20 / 25
ARG VERSION=dev
    -ldflags="-s -w -X gemini-web2api/internal/app.Version=${VERSION}" \
```

三者齐备，无缺项。

**判据有效性**：3/3 子项满足，达标。

### 判据 6（3 轮 -race）— 副本 B

**预注册门限**：EXIT=0、无 `WARNING: DATA RACE`、`--- FAIL` 0、3 轮顶层运行 >= 21（列名与次数）。

**主轮命令与结果**（严格按预注册 -run 正则）：

```
$ GOTOOLCHAIN=go1.27.1 go test -race -v ./internal/app -run 'TestCallGemini|TestWC|TestArgon|TestPhantomCleanup|TestUpstream|TestDiag|TestEmptyContent|TestVersion' -count=3
EXIT=0
ELAPSED_SECONDS=11
WARNING: DATA RACE 计数 = 0
^--- FAIL 计数 = 0
^=== RUN 计数 = 81
^--- PASS（顶层，无缩进）计数 = 63
ok  	gemini-web2api/internal/app	1.725s
```

顶层测试名单与次数（`=== RUN` 去 '/' 后 uniq -c，共 23 个唯一顶层名，其中 2 个为 SKIP）：

| 顶层测试 | 次数 | 结果 |
|---|---|---|
| TestWCStreamUploadFailureRedactsBody | 3 | PASS |
| TestWCSendRedactsURLError | 3 | PASS |
| TestWCSendRedactsRequestBuildError | 3 | PASS |
| TestWCRedactURL | 3 | PASS |
| TestWCRedactErrPreservesTimeout | 3 | PASS |
| TestWCRedactErrChainIsRedacted | 3 | PASS |
| TestWCReadFrames | 3 | PASS |
| TestWCModelHeaderBin | 3 | PASS |
| TestWCInnerWithoutPrompt | 3 | PASS |
| TestWCHTTPHeadersAuth | 3 | PASS |
| TestWCHandshakeFailureRedactsBody | 3 | PASS |
| TestWCClassify | 3 | PASS |
| TestPhantomCleanupGateAndCollect | 3 | PASS |
| TestCallGeminiRejectsEmptyReplyAfterToolFence | 3 | PASS |
| TestArgonValidateIdentity | 3 | PASS |
| TestArgonModelNeedsLoginAndAvailable | 3 | PASS |
| TestArgonMode8Slots | 3 | PASS |
| TestArgonIncrementalForwarding | 3 | PASS |
| TestArgonIdentityWithStrictModel | 3 | PASS |
| TestArgonIdentityByThinkTier | 3 | PASS |
| TestArgonCarbonIdentity | 3 | PASS |
| TestArgonProtocolProbe | 3 | SKIP（每轮 0.00s） |
| TestArgonTimingProbe | 3 | SKIP（每轮 0.00s） |

实际执行顶层测试数：23（21 PASS + 2 SKIP）>= 21，达标。`^--- PASS` 顶层 63 = 21 个 PASS 测试 x 3 轮，与 -count=3 一致；「非 3 次的顶层项」检查为空（awk `$1!=3` 无输出），证明每个顶层测试恰好跑 3 轮。

> 正则覆盖说明：预注册 -run 中 `TestDiag` / `TestEmptyContent` / `TestVersion` / `TestUpstream` 四个前缀在本仓无匹配测试函数（已用 `grep -rn '^func Test' internal/app/*_test.go` 核验：`TestDiag*` 0 个、`TestEmptyContent*` 0 个、`TestVersion*` 0 个、`TestUpstream*` 0 个；实际 502 回归用例命名为 `TestChatCompletions502*`）。故主轮只覆盖 23 个顶层测试。

**补充轮**（为覆盖本改动的 502 诊断回归用例，追加 6 个 502 用例名，仍 -count=3）：

```
$ GOTOOLCHAIN=go1.27.1 go test -race -v ./internal/app -run 'TestCallGemini|TestWC|TestArgon|TestPhantomCleanup|TestUpstream|TestDiag|TestEmptyContent|TestVersion|TestChatCompletions502|TestChatCompletionsMultiTurnNoContentFrame|TestChatCompletionsLegitPathsNotFalsePositive|TestChatCompletionsMediaArtifactNotFalsePositive' -count=3
EXIT=0
ELAPSED_SECONDS=4
WARNING: DATA RACE = 0
^--- FAIL = 0
^=== RUN = 108
^--- PASS（顶层）= 81
ok  	gemini-web2api/internal/app	2.647s
```

补充轮新增顶层测试（各 3 次，全 PASS）：`TestChatCompletions502FailureReply`、`TestChatCompletions502ReasoningOnlyIsEmptyContent`、`TestChatCompletions502StructuredEmptyFrame`、`TestChatCompletionsLegitPathsNotFalsePositive`、`TestChatCompletionsMediaArtifactNotFalsePositive`、`TestChatCompletionsMultiTurnNoContentFrame`。

**判据有效性**：主轮 EXIT=0、DATA RACE 0、FAIL 0、23 个顶层 >= 21，达标；补充轮同样 EXIT=0、DATA RACE 0、FAIL 0。

### 判据 7（双快照对照：空正文守卫）— 副本 C（基线轮）/ C2（待审轮）

**反例改动**（仅改业务文件 `internal/app/server.go`，未动测试）：把 `if text == ""` 内的 `hasContentFrame` 分支

```go
if hasContentFrame(res.Raw) {
	return "", nil, res, &diagErr{
		err:    fmt.Errorf("%w (raw %d bytes)", errEmptyContent, len(res.Raw)),
		reason: reasonEmptyContent, res: res}
}
```

恢复为 `return text, toolCalls, res, nil`（即 200 空正文）。为使反例可编译，同时把 `var toolCalls []ToolCall` 声明提前到 `text := extractResponseText(res.Raw)` 之后（与基线 8647ba1 的结构一致）。diff（vs 原文件，仅 3 处）：

```diffn@@ -143,6 +143,7 @@
 	text := extractResponseText(res.Raw)
+	var toolCalls []ToolCall
@@ -178,10 +179,8 @@
-		if hasContentFrame(res.Raw) {
-			return "", nil, res, &diagErr{
-				err:    fmt.Errorf("%w (raw %d bytes)", errEmptyContent, len(res.Raw)),
-				reason: reasonEmptyContent, res: res}
+		if text == "" {
+			return text, toolCalls, res, nil
 		}
@@ -191,7 +190,6 @@
 	}
-	var toolCalls []ToolCall
```

反例文件 sha256：`232b05f5199e1aa98e2cb9d6f98c71e3aaeb2edf99e3ffeb22c1156ab09d0cca`（原样为 `6746ba85b281afc1f152c532eabb2bec54292a5bcac3cb8da9794ddee06b7df1`）；`gofmt -l` 为空；`go vet ./internal/app` 无输出。

**基线轮（反例）**：

```
$ /root/.local/bin/verifier-expect-fail -expect-re 'empty_content|空正文|200' -min-tests 1 \n    go test -v -run TestChatCompletions502ReasoningOnlyIsEmptyContent ./internal/app -count=1
WRAPPER_EXIT=0
=== RUN   TestChatCompletions502ReasoningOnlyIsEmptyContent
    toolgate_test.go:620: reasoning-only 应回 502，得到 200 body={"choices":[{"finish_reason":"stop","index":0,"message":{"content":null,"reasoning_content":"thinking about the problem thinking about the problem thinking about the problem","role":"assistant"}}],"created":1794591259,"id":"chatcmpl-952338b53ea5","model":"gemini-3.6-flash","object":"chat.completion","usage":{"completion_tokens":0,"prompt_tokens":6,"reasoning_tokens":20,"total_tokens":6}}
--- FAIL: TestChatCompletions502ReasoningOnlyIsEmptyContent (0.01s)
FAIL
FAIL	gemini-web2api/internal/app	0.042s
FAIL
verifier-expect-fail: PASS inner_exit=1 match="200" tests=1
```

- 内层退出码：1（非零）
- 命中特征原文：`reasoning-only 应回 502，得到 200 body=…` → 匹配正则分支 `200`
- 解析执行用例数：1 >= 1
- 外层退出码：0

四条件全满足，基线红灯成立。命中原文明确显示反例形态：HTTP 200 + `content:null` + `reasoning_content` 非空。

**待审轮（副本 C2 原样，files_sha256=70dd2999aec12102，HEAD=c57860d，porcelain 空）**：

```
$ go test -v -run TestChatCompletions502ReasoningOnlyIsEmptyContent ./internal/app -count=1
EXIT=0
=== RUN   TestChatCompletions502ReasoningOnlyIsEmptyContent
--- PASS: TestChatCompletions502ReasoningOnlyIsEmptyContent (0.01s)
PASS
ok  	gemini-web2api/internal/app	0.034s
```

| 轮次 | 快照 | 退出码 | 命中原文 | 用例数 | 外层 |
|---|---|---|---|---|---|
| 基线（反例） | C + server.go 反例（sha 232b05f5…） | 1 | `reasoning-only 应回 502，得到 200` | 1 | 0 |
| 待审 | C2 原样（sha 70dd2999…） | 0 | 不适用（正向通过） | 1 | 不适用 |

**判据有效性**：基线轮四门限全中，待审轮 EXIT=0 且 PASS，达标。

### 判据 8（双快照对照：落库前缀）— 副本 D（基线轮）/ D2（待审轮）

**反例改动**（仅改业务文件 `internal/app/upstream_err.go`，未动测试）：把 `recordUpstreamFailure` 的

```go
recordRequest(endpoint, model, prompt, "", res, status,
	fmt.Sprintf("[%s] %s", errDiagReason(err), err.Error()), stream)
```

改为

```go
recordRequest(endpoint, model, prompt, "", res, status,
	fmt.Sprintf("%s", err.Error()), stream)
```

diff：

```diff
@@ -231,5 +231,5 @@
 	recordRequest(endpoint, model, prompt, "", res, status,
-		fmt.Sprintf("[%s] %s", errDiagReason(err), err.Error()), stream)
+		fmt.Sprintf("%s", err.Error()), stream)
 }
```

反例文件 sha256：`0b4e1cfaf45dbd8546f85547e90b7449f9ea26cc42ffe2216f93ef75763fffa5`（原样为 `abdda7c7bb3195fb7352829c83cbf9fc39d9427e4faaf822d93cf6b5b5cc2384`）；`gofmt -l` 为空；`go vet ./internal/app` 无输出。

> 构造过程如实记录：首次直接删掉 `fmt.Sprintf` 写 `err.Error()` 导致 `"fmt" imported and not used` 编译失败，包装器报 `inner_exit=1 failed=[expect-re,min-tests(0>=1)]`（外层 EXIT=1）。这是反例代码不可编译，不是判据失败；改为保留 `fmt` 使用的等价无前缀写法后重跑。此处置换属反例构造，非判据重试。

**基线轮（反例）**：

```
$ /root/.local/bin/verifier-expect-fail -expect-re '\[no_content_frame\]|\[reason\]|前缀' -min-tests 1 \n    go test -v -run TestChatCompletions502StructuredEmptyFrame ./internal/app -count=1
WRAPPER_EXIT=0
=== RUN   TestChatCompletions502StructuredEmptyFrame
    toolgate_test.go:528: requests.error 应带 [no_content_frame] 前缀，得到 "upstream returned no content frame (raw 820 bytes)"
--- FAIL: TestChatCompletions502StructuredEmptyFrame (0.03s)
FAIL
FAIL	gemini-web2api/internal/app	0.053s
FAIL
verifier-expect-fail: PASS inner_exit=1 match="[no_content_frame]" tests=1
```

- 内层退出码：1
- 命中特征原文：`requests.error 应带 [no_content_frame] 前缀，得到 "upstream returned no content frame (raw 820 bytes)"` → 匹配正则分支 `\[no_content_frame\]`
- 解析执行用例数：1 >= 1
- 外层退出码：0

四条件全满足，基线红灯成立。命中原文明确显示反例形态：落库 error 串无 `[reason]` 前缀。

**待审轮（副本 D2 原样，files_sha256=70dd2999aec12102，HEAD=c57860d，porcelain 空）**：

```
$ go test -v -run TestChatCompletions502StructuredEmptyFrame ./internal/app -count=1
EXIT=0
=== RUN   TestChatCompletions502StructuredEmptyFrame
--- PASS: TestChatCompletions502StructuredEmptyFrame (0.03s)
PASS
ok  	gemini-web2api/internal/app	0.057s
```

| 轮次 | 快照 | 退出码 | 命中原文 | 用例数 | 外层 |
|---|---|---|---|---|---|
| 基线（反例） | D + upstream_err.go 反例（sha 0b4e1cfa…） | 1 | `requests.error 应带 [no_content_frame] 前缀，得到 "upstream returned no content frame…"` | 1 | 0 |
| 待审 | D2 原样（sha 70dd2999…） | 0 | 不适用（正向通过） | 1 | 不适用 |

**判据有效性**：基线轮四门限全中，待审轮 EXIT=0 且 PASS，达标。

---

## 三、判据有效性与实测值汇总

| 判据 | 预注册门限 | 实测值 | 达标 | 依据抄录 |
|---|---|---|---|---|
| 1 | dev-audit EXIT=0 含 L1/OK；make check EXIT=0；-v 顶层 PASS>=20 | dev-audit EXIT=0，`. L1 OK`；make check EXIT=0（gofmt 0/vet 0/test ok/build 0）；顶层 PASS 183 | 是 | `.                      L1  OK` / `ok  	gemini-web2api/internal/app	18.087s` / `TOP_LEVEL_PASS=183` |
| 2 | EXIT=0 且 PASS | EXIT=0，4/4 PASS | 是 | `--- PASS: TestChatCompletions502StructuredEmptyFrame (0.01s)` 等 4 行 |
| 3 | EXIT=0 且 PASS；说明列与前缀 | EXIT=0，PASS；列=`status`/`error`/`upstream_model`，前缀=`[no_content_frame]` | 是 | `SELECT status, COALESCE(error,''), COALESCE(upstream_model,'')`；`strings.HasPrefix(errStr, "[no_content_frame]")` |
| 4 | 全 PASS；说明 reasoning-only 选择 | EXIT=0，6/6 PASS（含 3 子用例）；reasoning-only 报 `empty_content` 502，不拿思考链顶 content | 是 | `reason=%q 应为 empty_content（有思考帧但正文为空）`；`server.go:178-181` 注释 |
| 5 | 三者齐备 | 3/3 齐备 | 是 | `var Version = "dev"`；`grep -nx 'var Version = "dev"'`；`ARG VERSION=dev` + `-X …Version=${VERSION}` |
| 6 | EXIT=0；无 DATA RACE；FAIL 0；3 轮顶层>=21 | 主轮 EXIT=0、DATA RACE 0、FAIL 0、23 个顶层（21 PASS + 2 SKIP）各 3 次；补充轮 EXIT=0、DATA RACE 0、FAIL 0、29 个顶层各 3 次 | 是 | `verifier-expect-fail: PASS inner_exit=1`（判据7/8）；`ok  	gemini-web2api/internal/app	1.725s`；awk `$1!=3` 无输出 |
| 7 | 基线内层非零+命中+用例>=1+外层0；待审 EXIT=0 PASS | 基线 inner_exit=1 命中 `200` tests=1 外层 0；待审 EXIT=0 PASS | 是 | `verifier-expect-fail: PASS inner_exit=1 match="200" tests=1` |
| 8 | 同上 | 基线 inner_exit=1 命中 `[no_content_frame]` tests=1 外层 0；待审 EXIT=0 PASS | 是 | `verifier-expect-fail: PASS inner_exit=1 match="[no_content_frame]" tests=1` |

**空转底线核验**：
- 零用例？无。判据 2/3/4/7/8 均为具名测试且 `--- PASS`/`--- FAIL` 成对出现；判据 6 主轮 63 个顶层 PASS、补充轮 81 个。
- 构建/生成无产物？不适用（判据类型为测试与静态检索，非构建产物校验）。判据 1 的 `go build -o /dev/null .` EXIT=0 属构建存在性核验，产物路径按预注册丢弃。
- 校验未读声明输入？无。判据 2/3/4 均读取真实假上游 raw 并解析（测试内 `extractResponseText`/`hasContentFrame`/`upstreamFrameStats` 实际执行）；判据 5 为静态源码检索，已逐字抄录行内容。
- 低于预注册门限？无。

---

## 四、结果五分类

| 判据 | 分类 |
|---|---|
| 判据 1（门禁基础） | 通过 |
| 判据 2（结构化字段） | 通过 |
| 判据 3（落库不受开关影响） | 通过 |
| 判据 4（空正文二分） | 通过 |
| 判据 5（Version 注入，静态） | 通过 |
| 判据 6（3 轮 -race） | 通过 |
| 判据 7（双快照：空正文守卫） | 通过 |
| 判据 8（双快照：落库前缀） | 通过 |

分类依据：全部判据有效执行、退出码符合门限、证据量达标、无超时、无拒答、无证据缺口。

---

## 五、局限

1. 判据 1 首次执行因使用 `git archive` 快照（无 `.git`）导致 `dev-audit` 空转（EXIT=0 但 stdout 0 字节）。已改用真实 `git clone` 重建并重做；该轮不作数。
2. 判据 6 预注册 -run 正则中的 `TestDiag` / `TestEmptyContent` / `TestVersion` / `TestUpstream` 四个前缀在本仓无匹配测试函数（已 grep 核验），主轮实际覆盖 23 个顶层测试。为使本改动的 502 诊断回归用例进入 -race 覆盖，追加了补充轮（29 个顶层测试，各 3 次）。主轮与补充轮均达标。
3. 判据 7 反例需把 `var toolCalls []ToolCall` 声明提前方可编译（`toolCalls` 在原位置尚未声明），此改动与基线 8647ba1 的结构一致；反例的语义核心（`hasContentFrame` 分支返回 `text, toolCalls, res, nil`）未受声明位置影响。
4. 判据 8 反例首次写作裸 `err.Error()` 触发 `"fmt" imported and not used` 编译失败，改用保留 `fmt` 的等价无前缀写法；反例的语义核心（去掉 `[reason]` 前缀）未变。
5. 本报告只提交证据与判据有效性，不含验收结论、严重度、原因归因或修复建议。

---

## 六、清理确认

- 删除的 `/tmp` 自建副本：`/tmp/vfy-502-A`、`/tmp/vfy-502-A-git`、`/tmp/vfy-502-B`、`/tmp/vfy-502-C`、`/tmp/vfy-502-C2`、`/tmp/vfy-502-D`、`/tmp/vfy-502-D2`。
- 删除的备份/中间文件：`/tmp/vfy-502-C-server.orig`、`/tmp/vfy-502-D-upstream_err.orig`、`/tmp/shape_test.go`（未使用的占位，已删）、`/tmp/c2.out`、`/tmp/c3.out`、`/tmp/c4.out`、`/tmp/c6.out`、`/tmp/c6b.out`、`/tmp/c7-base.out`、`/tmp/c7-rev.out`、`/tmp/c8-base.out`、`/tmp/c8-rev.out`、`/tmp/da.out`、`/tmp/da.err`、`/tmp/mc.out`。
- 删除后核验：`ls -d /tmp/vfy-*` → `ALL_REMOVED`；`ls /tmp/c*.out /tmp/da.out /tmp/da.err /tmp/mc.out` → 全部 `No such file or directory`。
- `/tmp` 挂载：tmpfs 5.9G，清理后 used 55%（清理前峰值 56%）。
- 被审工作区未发生任何写入：验证全程仅 `ctx_git`/`ctx_fs`/`ctx_run` 只读命令 + 副本内改动；被审 worktree 的 `git status --porcelain -uall` 仍仅 `?? .codegraph/codegraph.lock`。

---

**报告路径**：/tmp/herdr-role-sessions/default/role-verifier-8496f4a0/report.md
