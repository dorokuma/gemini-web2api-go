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
