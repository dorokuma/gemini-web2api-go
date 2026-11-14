# release-v4.28.0-magpie.5 — 证据索引

A 档最小证据落盘：已发布版本 `v4.28.0-magpie.5` 的事实记录与证据原件。
事实记录见 `RELEASE.md`；下列 9 个证据文件自核验会话产物目录逐字节复制（源 → 目标 sha256 逐个一致，diff 为空）。

## 文件清单（字节数 / sha256）

| 文件 | 字节数 | sha256 |
|---|---|---|
| docker-manifest.err | 17 | 3f5cd544da258d2ea0515915dc431e867c52a9d7e9596e8579d7009857f5948d |
| docker-manifest-latest.json | 1310 | f8fff24393b16eca4957247435b4b4b13d0979cbc634b1da3de3ce926c711813 |
| docker-manifest-v.json | 1310 | f8fff24393b16eca4957247435b4b4b13d0979cbc634b1da3de3ce926c711813 |
| release-view.json | 4060 | 259da951cbca2226119109a0c11c2529a4d48da74d5195c57ec03c90745e8253 |
| run-docker.json | 2487 | 659a464be8017d3e934fda54be8783f87c8fedd1e526ad02f17d6001ce55b609 |
| run-release.json | 2291 | 497da1752c192702ec6e463e48405d4290bb38e1c1365d09e8d6c8a850b631da |
| sha256sum-all.txt | 773 | 575864eafd04b55b91c770693d0c445d51b0a635dec61db0f056aba99185780c |
| sha256sum-c.txt | 327 | 6dc8117be2554d1e2bcbe68ede228de858903c7e0b4750ea924db23b6752a8b0 |
| version.txt | 100 | 59e6967d54f6a9bcb6a1991912913729bce6b01d933d6197d82189a9d81df166 |

证据文件小计：12675 字节（9 个文件；`docker-manifest.json`（空）已剔除）。本目录另有 `RELEASE.md`、`README.md`（索引自身），共 11 个文件、总计 19886 字节。

## 各文件内容说明

- `release-view.json` — `gh release view` 输出：tag/isDraft/isPrerelease + 7 项资产（name/size/digest/url）
- `sha256sum-c.txt` — `sha256sum -c SHA256SUMS.txt` 全 6 项 OK + EXIT=0
- `sha256sum-all.txt` — 下载产物全部文件（含 SHA256SUMS.txt 自身）的 sha256 清单
- `version.txt` — host linux-arm64 二进制 `--version` 输出 + EXIT=0
- `docker-manifest-v.json` — `ghcr.io/.../gemini-web2api-go:v4.28.0-magpie.5` OCI index
- `docker-manifest-latest.json` — `latest` tag OCI index（digest 与版本 tag 逐条相同，仅观测）
- `docker-manifest.err` — 不带 `v` 的 `4.28.0-magpie.5` 的 stderr：`manifest unknown`（对应空输出文件已剔除）
- `run-release.json` — CI run 43590981269（Release，workflow_dispatch，success）job 明细
- `run-docker.json` — CI run 43590984062（Docker，workflow_dispatch，success）job 明细
