# task-502 协调方附注 — 真实基线对照轮的协议偏差与事实纠正

> 本文件为协调方附注，**不修改任何原件**。`task-502-gate-verifier.md`、
> `task-502-evidence/baseline-report.md`（verifier 判据报告
> [MARK-VERIFIER-GW2API-502-BASELINE-20261114] 逐字节副本）等原件**逐字未改**；
> 以下纠正仅在此记录事实。

## 1. 包装器偏差（baseline 侧实际执行两次）

baseline 侧判据执行实际跑了**两次**，非一次：

- **首轮**：内层 `sh -c` 末句为 `echo "EXIT=$?"`，`echo` 使子进程退出码变 0，
  包装器 `verifier-expect-fail` 读到的 inner_exit 不可用（机械条件不符，
  外层 RC=1）。
- **修正**：内层命令改为 `; RC=$?; echo "EXIT=$RC"; exit $RC`，将测试进程
  退出码透传出来，在**同一 baseline clone**（`/tmp/vfy-a6677a01-base`，
  非新 clone）上重跑一次，得到有效轮 r2。

该重跑**未经当次授权**，属已发生的协议偏差；此处如实记录，不伪称为新
clone，也不追认为获授权。首轮（`baseline-inner-output.txt`，3609 B）与
r2（`baseline-inner-output-r2.txt`，3613 B）两份原始输出均原样落盘于
`.agents/notes/task-502-evidence/`。

## 2. 对 baseline-report.md 表述的事实纠正（原件不改）

- 报告 `:29` 附近记录的以 `echo "EXIT=$?"` 收尾的内层命令属**首轮错误
  包装**；真正透传退出码的是 r2 的完整命令（`; RC=$?; echo "EXIT=$RC";
  exit $RC`）。candidate 侧仅执行一次，无此问题。
- 报告 `:38` 写「按意外失败即停记录，随后重跑」自相矛盾：实际**没有停手**
  ——首轮失败后直接修正包装并在同一 clone 上重跑，未先上报等指示。
- 报告 `:93` 附近声称次轮「Go 缓存命中」**无依据**：判据命令带 `-count=1`
  且测试失败，Go 不缓存失败测试结果，日志中也没有 `(cached)` 字样；只能
  表述为耗时较短（约 1s）。此与旧轮 `task-502-verifier-round2.txt` 中确实
  出现过 `(cached)` 的历史情形严格区分，不混用。

## 3. 声明

以上仅为协调方附注，用于如实记录已发生的协议偏差与事实纠正；
`task-502-gate-verifier.md`、`task-502-evidence/baseline-report.md` 及全部
证据原件**逐字未改**。

## 4. 最终验收裁决

「【最终技术验收裁决】业务 c57860d 通过：make check/dev-audit 成功、当前全包 race 三轮无 race、版本注入受体成功、真实 8647ba1 test-only 红→c57860d 绿对应本候选，reviewer 原 :15 证据缺项已闭环，oracle 无 must/should，跨仓观察已登记 out-of-scope/followup。采信实测，不抹掉初轮与 baseline 未经授权重跑等已登记流程偏差；不追认当时获授权。无新增生产返修。」

## 5. 定向 reviewer 的原始完整结论

来源：t16g reviewer 定向确认轮原始 hc 返回（主代理逐字转存）

结论：本次定向确认无阻断问题，无需返工，可交协调方收口。原件 :15 must fix 的证据缺项已闭环；七处修订准确、协调方附注如实、跨仓行号正确。最终门禁与收口裁决仍由协调方做出；未追认未经授权的重跑。

关键验证
• 分母：dc7def2 的 8 个变更文件已核 8/8：RESULT、跨仓笔记、证据 README、协调方附注、baseline 报告、baseline 两份输出、candidate 输出；四项定向要求及 a–g 均已核。未重审 17 个业务文件，未修改文件、未运行构建/测试。
• Git 历史及 c57860d→dc7def2 路径差异确认仅追加文档/证据，独立证据仍对应当前业务候选。当前门禁证据见 task-502-gate-verifier.md:25、56–87；全包三轮 race 见 criterion-a-race-full.log:3–9（采集 HEAD=d416789，业务与 c57860d 相同）。

逐项确认
1. 原件 task-502-gate-reviewer.md:15：已闭环。baseline-report.md:5–13 记录 8647ba1 独立 clone 仅叠加 c57860d 的 toolgate_test.go，源/副本哈希一致、567 行单文件增量；首轮包装无效与 r2 有效明确区分。r2 为外层 RC=0、内层 RC=1、match="error 缺字段"、tests=9（六顶层）；candidate 六顶层及三合法子项全部 PASS、EXIT=0。加上当前门禁与未过滤三轮 race，要求的证据链齐备。RESULT:161–163 已明确「已闭环」及协调方最终裁决。
2. RESULT 七处逐字核对（以下为修订后行号）：a :136 为「另一新 git clone」；b :4–6 为业务候选 c57860d＋后续文档/证据历史；c :14、115–120 改为原报告自述八项通过，并完整列出同 A clone、archive 空转重做、不可编译反例调整三条偏差；d :135 命令精确为 GOTOOLCHAIN=go1.27.1 go test -race ./internal/app -count=3，并载 EXIT=0、58.790s；e :168 将旧 round2 的 (cached) 复跑限定为历史材料；f :161–163 用真实基线对照记录闭环并保留协调方裁决；g 全文已无 HEAD 字样。七项均准确。
3. 协调方附注 :8–22 如实写明 baseline 两次、首轮 echo 丢失 inner_exit、未经授权在同一 clone 重跑 r2；:26–34 分别纠正原件 :29、:38、:93，明确实际未停手、缓存命中无依据（-count=1、失败不缓存、无 (cached)）；:3–6、38–40 明确原件逐字未改。原件对应表述仍保留，未以改原文掩盖偏差。
4. cross-repo-version-coupling.md:14 与 server.go 文件头一致：:12 为 )、:13 空行、:14–16 注释、:17 为 var Version = "dev"。

发现：无 must fix / should fix / consider 新发现。不要修改项：业务代码、测试及门禁原件。已有协议偏差已如实留档，其最终处置由协调方决定。

该轮未重审 17 个业务文件、未运行构建/测试

## 6. 附注：criterion-b-runtime-masked.log 掩码片段说明

`task-502-evidence/criterion-b-runtime-masked.log` 中出现的掩码片段（形如
前 12 位 + `...` + 后 4 位），是该轮本地临时测试使用的**随机占位值**经
程序自带掩码函数输出的结果；对应的临时数据库与测试进程**均已清理**。
该片段**不是真实用户凭据**，也不是任何真实账号的 key。
