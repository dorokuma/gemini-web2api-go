# task-502 门禁补证证据交付索引

本目录为 **task-502**（upstream 502 diagnostics）门禁补证证据的持久交付副本。

## 证据来源

- 会话临时归档目录：`/tmp/herdr-role-sessions/default/role-verifier-da9c81b2/`（仅为会话期临时归档，非持久路径）。
- 第二轮真实基线对照会话归档：`/tmp/herdr-role-sessions/default/role-verifier-a6677a01/`（同前，仅为会话期临时归档）。
- 持久交付路径：`.agents/notes/task-502-evidence/`。
- 说明：被引用报告（`task-502-gate-verifier-supplement.md`）第 81 行将临时归档目录称为「交付位置」；此处补充：该 `/tmp/...` 路径仅为会话期临时归档，**交付持久路径为本目录**。此为补充说明，**不修改原报告**。

## 文件清单

| 文件名 | 字节数 | sha256 | 来源（源文件名） | 采集时候选 SHA |
|---|---|---|---|---|
| `task-502-gate-verifier-supplement.md` | 16694 | `110518e42a00057fc2d2ddf71a012370ae1ce85c015a1b240be9943af4ca9691` | `report.md` | 业务候选 `c57860d`，HEAD `d416789` |
| `criterion-a-race-full.log` | 577 | `1c88879b779b44d4ac8edf78e7ab54f465625db48cab4608a723a7d87cb4598d` | `criterion-a-race-full.log` | 业务候选 `c57860d`，HEAD `d416789` |
| `criterion-a-stdout.txt` | 74 | `e6a28be3ec899dad51a0b85963c19f01870cb325469a79ed27174ecf4936b310` | `criterion-a-stdout.txt` | 业务候选 `c57860d`，HEAD `d416789` |
| `criterion-b-get-root-body.json` | 141 | `dd8ab405129d90c48d56770ca1915c3ef47739670ae55ee9100f3c331b7f8e65` | `criterion-b-get-root-body.json` | 业务候选 `c57860d`，HEAD `d416789` |
| `criterion-b-get-root-headers.txt` | 141 | `51663962475d85a5d286b14601757e4c5857d0e808ce90f23e19b00e2c5ee041` | `criterion-b-get-root-headers.txt` | 业务候选 `c57860d`，HEAD `d416789` |
| `criterion-b-runtime-masked.log` | 522 | `e335eb83ab257f08d8cff1d81782e58bd2d4181aa370cca9d001fb175ac33221` | `criterion-b-runtime-masked.log` | 业务候选 `c57860d`，HEAD `d416789` |
| `criterion-b-version-stdout.txt` | 32 | `7865569a61ab097b41ed9b289563eafae6b4e43de16de8257cedd3541e061dd2` | `criterion-b-version-stdout.txt` | 业务候选 `c57860d`，HEAD `d416789` |

### 第二轮证据：真实基线对照（baseline `8647ba1` + test-only overlay 红 vs candidate `c57860d` 绿）

来源目录：`/tmp/herdr-role-sessions/default/role-verifier-a6677a01/`（verifier 判据报告 [MARK-VERIFIER-GW2API-502-BASELINE-20261114]）。复制方式：`cp -p` 逐字节复制，目标 sha256 与源 sha256 逐一相同。

| 文件名 | 字节数 | sha256 | 来源（源文件名） | 适用 SHA |
|---|---|---|---|---|
| `baseline-report.md` | 12205 | `c08808635b9d01a5e735d8892b431d293afb46b20b23fd3ab9c070df4b799838` | `report.md` | baseline `8647ba1` + test-only overlay；candidate `c57860d` |
| `baseline-inner-output.txt` | 3609 | `bb7d52194db1e6dc4ac6e57876e4a184fb2fa80706a580c21ee4be6a80648f49` | `baseline-inner-output.txt` | baseline `8647ba1` + test-only overlay（首轮，inner_exit 未透传） |
| `baseline-inner-output-r2.txt` | 3613 | `a6cc3508a1809617fad34a8667915cf097302db9c87c95231736df6ed7195e4c` | `baseline-inner-output-r2.txt` | baseline `8647ba1` + test-only overlay（有效轮 r2） |
| `candidate-output.txt` | 1412 | `181462d528db7fededb3e007b6166f9f30113b6059353e0e384eaf9f798f703c` | `candidate-output.txt` | candidate `c57860d` |

## 脱敏声明

已对本目录内全部 11 个文件逐个 `grep` 扫描 API key 前缀形态（`sk-gemini-`、`sk-`）、Google key 前缀形态（`AIza`）、`Bearer` 认证头形态、会话 Cookie 名形态（`__Secure-`、`__Host-`、`SAPISID`、`PSID` 等）、`Cookie` 字段，以及两个随机占位凭据（`API_KEY` / `ADMIN_TOKEN`）的全值/句柄形态；**凭据命中为 0**（唯一 `Cookie:` 字样为程序自身打印的池状态/掩码行，报告内 `Bearer`/`Cookie` 字样均为描述扫描对象的说明文字，非凭据值）。
