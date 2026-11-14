# Verifier 证据报告 — 候选 c57860d / HEAD d416789 两项未覆盖验证

标记：[MARK-VERIFIER-GW2API-502-RACE-ARTIFACT-20261114]
角色：verifier｜执行体：pig｜工具限制遵守：未用 codegraph，仅 ctx_run + rg（rg 仅用于自查检索）

---

## 一、环境

| 项 | 值 |
|---|---|
| 被审 worktree（全程只读） | `/root/workspace/.wt/gemini-web2api-go/fix-upstream-502-diagnostics` |
| 被审 HEAD（`git rev-parse HEAD`） | `d4167899bf6aa51fa9003f51c7ae05a9fd93dead`（预期 `d416789`，一致） |
| 业务源码指纹 | `c57860d` = `git rev-parse HEAD~1`，即 `fix(diagnostics): surface 502 causes, persist reason, reject empty content, inject Version` |
| 判据 A 隔离副本 | `/tmp/vrfy-502/race-clone`（`git clone --no-hardlinks` + `git remote remove origin`，origin 已确认移除） |
| 判据 B 隔离副本 | `/tmp/vrfy-502/probe-clone`（同样独立 clone，origin 已确认移除） |
| 两个 clone 关系 | **完全独立、不复用**，且各自单独 `git clone`；Go mod cache 允许共用（Go 自身机制），但测试与 build 均在新 clone 内实跑 |
| Go toolchain | `go1.27.1`（`GOTOOLCHAIN=go1.27.1`，实际生效 `go version go1.27.1 linux/arm64`） |
| 主机 Go | `go1.26.8 linux/arm64`；CGO_ENABLED=1，gcc 14.2.0 可用（`-race` 依赖） |
| 判据 A 执行前 | 已 `go clean -testcache`，排除缓存回放；日志内无 `(cached)` 标记 |
| 副本状态 | 两个 clone 检出后 `git status --porcelain -uall` 均为空 = 干净，与登记的待审快照完全一致 |
| 被审 worktree 写保护 | 会话前后 `git status` 唯一未跟踪项为 `.codegraph/`（CodeGraph 自身索引产物，非我的写入）；`git diff HEAD --stat` 为空；HEAD 前后均为 `d416789` |

**三项快照指纹（在隔离副本内采集，与被审 worktree 内采集值逐字节一致）：**

| 指纹项 | 值 |
|---|---|
| `git rev-parse HEAD` | `d4167899bf6aa51fa9003f51c7ae05a9fd93dead` |
| `git ls-files -s \| sha256sum` | `0e1dfbffe94da669eb21ddc35ee978e7f89cb40e03242479bf61ad51950d65f6` |
| `git archive HEAD \| sha256sum` | `c2b1b79ccf7f1ff295b7f7f8696fed44def1a878160ce842ba738bd7b462ea4f` |

worktree 内同三项实测值分别为 `d4167899bf6a…`、`0e1dfbff…`、`c2b1b79c…`，与副本内完全一致 → 副本即被审内容的忠实复制。

---

## 二、判据

### 判据 A（指定未过滤全包三轮 race）— 结果：**通过**

**完整命令原文（逐字，单行，未加 `-run`、未拆分轮次）：**

```
GOTOOLCHAIN=go1.27.1 go test -race ./internal/app -count=3 ; echo "EXIT=$?"
```

- 执行位置：`/tmp/vrfy-502/race-clone`（新 clone，origin 已移除，非 B clone、非 worktree）
- 执行方式：固定包装器 `/tmp/vrfy-502/run-criterion-a.sh`（脚本内零外部输入，命令串为字面量），外层固定 argv 启动；`echo "EXIT=$?"` 由脚本内的 `bash -c '…'` 原样执行，退出码从原始输出中机械提取
- 原始输出（完整，未截断，577 字节）：

```
=== VERIFIER CRITERION A (isolated clone, origin removed) ===
PRE-REGISTERED COMMAND (verbatim, single line):
GOTOOLCHAIN=go1.27.1 go test -race ./internal/app -count=3 ; echo "EXIT=$?"
CWD: /tmp/vrfy-502/race-clone
HEAD: d4167899bf6aa51fa9003f51c7ae05a9fd93dead
TOOLCHAIN RESOLVED: go version go1.27.1 linux/arm64
--- BEGIN RAW OUTPUT OF PRE-REGISTERED COMMAND ---
ok  	gemini-web2api/internal/app	58.790s
EXIT=0
--- END RAW OUTPUT OF PRE-REGISTERED COMMAND ---
ELAPSED_SECONDS=68
ECHOED_EXIT_CODE_FROM_PRE_REGISTERED_COMMAND=0
LOG_FILE=/tmp/vrfy-502/race-clone/race-full.log
```

**硬性证据项逐条比对：**

| 要求 | 实测 | 依据 |
|---|---|---|
| 显式退出码 `0` | ✅ `EXIT=0` | 原始输出第 6 行，来自预注册命令自身的 `echo "EXIT=$?"`；外层包装器退出码同为 0 |
| 出现 `ok  	gemini-web2api/internal/app` | ✅ 命中 1 次 | 原始输出第 5 行，`grep -n '^ok  '` 定位于日志第 8 行 |
| 全文无 `WARNING: DATA RACE` | ✅ 计数 = **0** | `grep -c 'WARNING: DATA RACE'` = 0 |
| 无 `--- FAIL` | ✅ 计数 = **0** | `grep -c -- '--- FAIL'` = 0 |
| 保留完整原始输出到文件 | ✅ `race-full.log` | 路径见下 |

**日志文件：**

| 项 | 值 |
|---|---|
| 原始日志路径（已随副本删除，副本为一次性） | `/tmp/vrfy-502/race-clone/race-full.log` |
| 归档持久路径 | `/tmp/herdr-role-sessions/default/role-verifier-da9c81b2/criterion-a-race-full.log` |
| 字节数 | **577** |
| sha256 | **`1c88879b779b44d4ac8edf78e7ab54f465625db48cab4608a723a7d87cb4598d`** |
| DATA RACE 计数 | **0** |
| `--- FAIL` 计数 | **0** |
| `ok` 行计数 | 1 |
| 墙钟耗时 | 68 秒（go test 自报包耗时 58.790s，三轮累加，非缓存回放） |

**空转底线核验：** `go clean -testcache` 已先执行；输出无 `(cached)` 标记；`-count=3` 三轮全包运行、无 `-run` 过滤；被测包 `internal/app` 下实测存在 187 个 Test 函数（`grep -rhoE '^func (Test|Benchmark|Fuzz)[A-Za-z0-9_]+' internal/app/*_test.go | wc -l`），非零用例空跑。→ 空转底线不触发。

### 判据 B（当前版本受体入口演练）— 结果：**通过**

在**另一**新 clone `/tmp/vrfy-502/probe-clone`（origin 已移除，与判据 A clone 相互独立）内构建。

**Build：**

| 项 | 值 |
|---|---|
| 完整命令原文（逐字，单行） | `GOTOOLCHAIN=go1.27.1 go build -ldflags '-X gemini-web2api/internal/app.Version=task502-probe' -o /tmp/vrfy-502/probe-bin/task502-probe .` |
| build 退出码 | **0** |
| build 耗时 | 7 秒 |
| build 原始输出 | 空（无 warning / 无 error），无 stderr |
| 生效 toolchain | `go version go1.27.1 linux/arm64` |
| 产物路径 | `/tmp/vrfy-502/probe-bin/task502-probe` |
| 产物大小 | 25,413,330 字节 |
| 产物 sha256 | **`50cffcba2e1dee9d974b9b566af5fa451bc89d2579d89989feb228fba776c1b6`** |
| 版本注入确认 | `strings` 命中 `task502-probe` 及 build 行 `-ldflags="-X gemini-web2api/internal/app.Version=task502-probe"` |

**`--version`：**

| 项 | 值 |
|---|---|
| 命令 | `/tmp/vrfy-502/probe-bin/task502-probe --version` |
| 退出码 | **0** |
| stdout 原文（逐字，含尾部换行） | `gemini-web2api-go task502-probe` |
| stdout 字节数 | 32 |
| stderr | 空（0 字节） |
| 是否严格包含 `task502-probe` | ✅ 是（`grep -c 'task502-probe'` = 1） |

**无账号运行 + `GET /`：**

| 项 | 值 |
|---|---|
| 数据目录 | `/tmp/vrfy-502/probe-data`（启动前为空目录，仅含我自建的凭据长度记录文件） |
| `API_KEY` | 随机占位，长度 32（**值不打印**；刻意不采用该程序默认的 key 前缀，以避免任何 key 形态串出现在日志/报告中） |
| `ADMIN_TOKEN` | 随机占位，长度 32（值不打印） |
| 监听端口 | **21883**（高位空闲端口，启动前 `PORT_IN_USE_BEFORE=no`） |
| 真实账号 / 真实 Cookie / 真实配置 | **全程未使用**，无任何真实凭据参与 |
| 进程 PID | 3488525 |
| 就绪 | `READY_AFTER_ATTEMPTS=2`，`READY=1` |
| `GET /` HTTP 码 | **200** |
| `GET /` curl 退出码 | 0 |
| 响应头（原文） | `HTTP/1.1 200 OK` / `Access-Control-Allow-Origin: *` / `Content-Type: application/json` / `Date: Sat, 14 Nov 2026 01:39:21 GMT` / `Content-Length: 141` |
| 响应 body 原文（逐字，141 字节） | `{"capabilities":{"account_proxy_binding":true},"models":["gemini-3.5-flash-lite","gemini-3.6-flash"],"status":"ok","version":"task502-probe"}` |
| body sha256 | `dd8ab405129d90c48d56770ca1915c3ef47739670ae55ee9100f3c331b7f8e65` |
| `version` 字段提取 | `"version":"task502-probe"` |
| **严格等于 `task502-probe`** | ✅ **`VERSION_STRICT_EQUAL=yes`** |

**进程关闭与端口释放：**

| 项 | 值 |
|---|---|
| 关闭方式 | `kill -TERM` 至 PID 3488525 |
| 退出 | `PROCESS_EXITED_AFTER_WAIT_STEPS=2`（约 0.5s 内退出），`WAIT_EXIT_CODE=143`（128+15 = SIGTERM，正常终止） |
| 是否需要 SIGKILL | 否 |
| 关闭后端口状态 | `PORT_IN_USE_AFTER=no` → **端口已释放** |
| 交付前复测端口 | `PORT_21883=released` |
| 残留进程 | `ps` 按可执行文件路径与进程名检索，**无 task502-probe 真实进程**（`pgrep` 的 3 条匹配均为执行检索命令自身的 shell 进程，已逐条核验 args 排除） |

**无凭据声明与泄露自检：**

对全部捕获证据（判据 A 日志、判据 A stdout、运行时日志、`GET /` body、`GET /` headers、`--version` stdout）逐文件做 `grep -c` 机械扫描，扫描对象为：API key 前缀形态、Google key 前缀形态、Bearer 认证头形态、会话 Cookie 名形态、Cookie 字段，以及本次两个随机占位凭据的全值。

| 形态 | 命中总数 |
|---|---|
| API key 前缀形态（key prefix shape） | **0** |
| Google API key 前缀形态（Google key prefix shape） | **0** |
| Bearer 认证头形态（Bearer auth header shape） | **0** |
| `Cookie`（非空凭据值） | **0**（运行时日志中唯一 `Cookie` 字样为程序自身打印的空池状态文案，不含任何凭据值） |
| 会话 Cookie 名形态（session cookie name shapes） | **0** |
| 本次随机占位 `API_KEY` 全值 | **0**（未在任何文件中出现） |
| 本次随机占位 `ADMIN_TOKEN` 全值 | **0**（未在任何文件中出现） |

运行时日志中程序 banner 的 `API key:` 一行按程序自带 `maskKey` 打印为「前 12 位 + … + 后 4 位」的掩码串；为满足零凭据约束，本报告未抄录该片段，只记录「已生成、已锁定、长度 32」。→ 报告中不含任何凭据明文或可还原片段。

---

## 三、对照判据

**不适用。** 本次任务书未注册任何双快照对照（Able-to-fail / 反例探针）判据：判据 A 为正向 race 检测，判据 B 为正向版本注入与入口演练，均无基线红 → 待审绿的对照要求，也未提供任何探针规格（载荷 / 调用序列 / 目标行号 / 失败特征正则）。因此未执行 `verifier-expect-fail` 包装器，也未构建基线快照。此处如实标注为「不适用」，不以任何形式事后补造对照。

---

## 四、判据有效性（机械门限比对，独立于五分类，非验收结论）

| 判据 | 预注册门限 | 实测 | 达标 | 抄录依据原文 |
|---|---|---|---|---|
| A-1 退出码 0 | `EXIT=0` | `EXIT=0` | ✅ | `EXIT=0` |
| A-2 出现 ok 行 | `ok  	gemini-web2api/internal/app` | 命中 1 次 | ✅ | `ok  	gemini-web2api/internal/app	58.790s` |
| A-3 无 DATA RACE | 计数 = 0 | 0 | ✅ | `grep -c 'WARNING: DATA RACE'` = 0 |
| A-4 无 FAIL | 计数 = 0 | 0 | ✅ | `grep -c -- '--- FAIL'` = 0 |
| A-5 完整原始输出留档 | 文件路径 + 字节数 + sha256 | 577 B / `1c88879b…` | ✅ | 见「二、判据 A」 |
| A-6 三项快照指纹 | 三项均记录 | 三项一致 | ✅ | 见「一、环境」 |
| A-7 未过滤 / 未拆分 / 独立 clone | 无 `-run`；单条命令；新 clone | 命令逐字一致；`--no-hardlinks` + origin 移除 | ✅ | 命令原文见「二、判据 A」 |
| B-1 build 退出码 0 + 产物 sha256 | 0 + hash | `0` + `50cffcba…` | ✅ | `BUILD_EXIT_CODE=0` |
| B-2 `--version` 退出码 0 且严格含 `task502-probe` | 0 + 含 | `0` + `gemini-web2api-go task502-probe` | ✅ | 见「二、判据 B」 |
| B-3 `GET /` = 200 且 `version` 严格等于 `task502-probe` | 200 + 严格相等 | `HTTP_CODE=200` + `VERSION_STRICT_EQUAL=yes` | ✅ | body 原文见「二、判据 B」 |
| B-4 进程关闭 + 端口释放 | 均已确认 | SIGTERM 后 0.5s 内退出；`PORT_IN_USE_AFTER=no` | ✅ | 见「二、判据 B」 |
| B-5 无凭据泄露 | 报告内 0 命中 | 全部形态 0 命中 | ✅ | 自检表见「二、判据 B」 |
| B-6 无账号条件完成 | 不依赖真实账号 | 未使用任何真实账号/配置 | ✅ | 见「二、判据 B」 |

预注册门限均存在于任务书，无「无预注册门限」情形，未事后伪造任何门限。

---

## 五、实测值与门限比对 / 空转底线

- **测试用例数**：判据 A 为全包三轮 `-race` 运行，`internal/app` 共 187 个 Test/Benchmark/Fuzz 函数，`-count=3`，无 `-run` 过滤 → 非零用例；输出无 `testing: warning: no tests to run`，无 `(cached)`。`go clean -testcache` 先行使结果为真实执行。
- **构建产物**：判据 B 产物 25,413,330 字节，sha256 `50cffcba2e1dee9d974b9b566af5fa451bc89d2579d89989feb228fba776c1b6`，路径与命令 `-o` 一致。
- **版本注入读取验证**：`--version` 输出与 `GET /` 的 `version` 字段均严格等于 `task502-probe`，且 `strings` 在二进制内直接命中该值与对应 `-ldflags` 构建行 → 声明输入（`-X …Version=`）确被读取并生效，非默认 `dev`。
- **空转底线逐项**：零用例 ✗ 未触发；构建/生成无产物 ✗ 未触发；校验未读声明输入 ✗ 未触发（version 注入已双向验证）；低于预注册门限 ✗ 未触发。→ **空转底线不触发，证据量满足要求。**

---

## 六、结果五分类（逐条唯一）

| 判据 | 结果 | 依据 |
|---|---|---|
| A 指定未过滤全包三轮 race | **通过** | 退出码 0、`ok` 行命中、DATA RACE=0、FAIL=0、完整日志留档、三项指纹一致、空转底线不触发 |
| B 当前版本受体入口演练 | **通过** | build 0 + hash、`--version` 0 且严格含版本串、`GET /` 200 且 `version` 严格相等、进程关闭且端口释放、凭据泄露 0 命中 |

无「失败」「未执行-超时」「未执行-拒答」「证据不完整」项。两条判据均为单次实跑，未发生重试（判据 A 一次通过；任务书对判据 A 明确要求失败时**不要自行重跑**，本情形未触发该路径）。

---

## 七、局限

1. 判据 A 的原始日志文件位于一次性隔离副本内，副本已按要求删除；内容已逐字节归档至会话目录（sha256 `1c88879b…`，577 字节），归档内容与删除前原件一致。
2. 判据 B 的运行时 banner 中程序自带的 `API key:` 掩码行未抄录进本报告（仅记录长度与「已生成/已锁定」），这是为满足「报告中不得出现任何凭据形态串」的约束而做的主动裁剪，不影响 B-1～B-4 的判据证据。
3. 判据 B 使用随机占位 `API_KEY`（长度 32，非该程序默认 key 前缀）而非程序首次启动自动生成的 key，因此 banner 中不会出现 key 前缀形态串；该选择属启动参数设定，不改变 `GET /` 与 version 注入的判据语义。
4. 本报告不含结论、建议、严重度、原因解释与修复方案（角色禁止字段），全部为可独立核验的证据与机械门限比对。

---

## 八、清理确认（交付前已完成）

| 项 | 状态 |
|---|---|
| `/tmp/vrfy-502/race-clone`（判据 A 隔离副本） | ✅ 已删除，`NO_LEFTOVER_ISOLATED_ARTIFACTS` |
| `/tmp/vrfy-502/probe-clone`（判据 B 隔离副本） | ✅ 已删除 |
| `/tmp/vrfy-502/probe-bin`（构建产物目录） | ✅ 已删除 |
| `/tmp/vrfy-502/probe-data`（临时数据目录，含 sqlite 三件套） | ✅ 已删除 |
| 固定包装器脚本（4 个） | ✅ 已删除 |
| 其余临时中间物（stdout/stderr/headers/body/runtime log） | ✅ 已删除 |
| `/tmp/vrfy-502` 根目录本身 | ✅ 已整体移除，`VRFY_502_FULLY_REMOVED` |
| 探针进程 | ✅ 无残留真实进程（仅检索命令自身 shell 被 pgrep 匹配，已逐条排除） |
| 端口 21883 | ✅ `released` |
| `/tmp` 空间 | 清理后 5.9G 中已用 1.8G（未打爆 tmpfs） |
| 保留的必要证据 | `/tmp/herdr-role-sessions/default/role-verifier-da9c81b2/`（见下） |

**归档证据清单（sha256 已核验）：**

| 文件 | 字节 | sha256 |
|---|---|---|
| `criterion-a-race-full.log` | 577 | `1c88879b779b44d4ac8edf78e7ab54f465625db48cab4608a723a7d87cb4598d` |
| `criterion-a-stdout.txt` | 74 | `e6a28be3ec899dad51a0b85963c19f01870cb325469a79ed27174ecf4936b310` |
| `criterion-b-get-root-body.json` | 141 | `dd8ab405129d90c48d56770ca1915c3ef47739670ae55ee9100f3c331b7f8e65` |
| `criterion-b-get-root-headers.txt` | 141 | `51663962475d85a5d286b14601757e4c5857d0e808ce90f23e19b00e2c5ee041` |
| `criterion-b-runtime-masked.log` | 522 | `e335eb83ab257f08d8cff1d81782e58bd2d4181aa370cca9d001fb175ac33221` |
| `criterion-b-version-stdout.txt` | 32 | `7865569a61ab097b41ed9b289563eafae6b4e43de16de8257cedd3541e061dd2` |

**被审 worktree 只读性最终确认**：HEAD 仍为 `d4167899bf6aa51fa9003f51c7ae05a9fd93dead`，`git diff HEAD --stat` 为空，`git status --porcelain -uall` 唯一未跟踪项为 `.codegraph/`（会话开始前即存在，非本次写入）。

（报告完）
