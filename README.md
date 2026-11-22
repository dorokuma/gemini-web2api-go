<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="assets/logo-light.svg">
  <img src="assets/logo-light.svg" alt="gemini-web2api-go logo" width="64" height="64">
</picture>

# gemini-web2api-go

**不用申请 Google API Key、不用绑卡开配额，单文件把 Gemini 网页版变成 OpenAI / Gemini 原生 / MCP 接口——不登录也能免费聊，挂上 Cookie 解锁全系高阶模型、画图与视频生成。**

[![Release](https://img.shields.io/github/v/release/dorokuma/gemini-web2api-go?style=flat-square&labelColor=18181b)](https://github.com/dorokuma/gemini-web2api-go/releases/latest)
[![Go](https://img.shields.io/badge/Go-1.27%2B-00ADD8?style=flat-square&labelColor=18181b&logo=go&logoColor=white)](https://go.dev)
[![Docker](https://img.shields.io/badge/Docker-distroless-2496ED?style=flat-square&labelColor=18181b&logo=docker&logoColor=white)](Dockerfile)
[![MCP](https://img.shields.io/badge/MCP-HTTP%20Server-8b5cf6?style=flat-square&labelColor=18181b)](#more-details)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat-square&labelColor=18181b)](LICENSE)

[快速开始](#quick-start) · [核心特性](#highlights) · [协议接口](#more-details) · [模型列表](#more-details) · [常见问题](#more-details)

**简体中文** · [English](README_EN.md)

</div>

```mermaid
flowchart LR
    A["客户端<br/>Cherry Studio · Cursor · Claude Code · newapi"] -->|"OpenAI /v1 · /v1beta · /v1/videos · /mcp"| B["gemini-web2api-go<br/>单二进制 · 管理面板"]
    B -->|"Chrome 指纹 TLS"| C["Cookie 池 + 代理池<br/>轮转 · 熔断 · 粘性出口"]
    C --> D["gemini.google.com"]
```

**gemini-web2api-go** 是一个自托管反向代理：它不走 Google 官方 API，而是直接反代 gemini.google.com 网页端协议，把网页版的免费模型变成标准的 OpenAI 兼容接口。单文件 Go 二进制，内置中文管理面板，Cookie 池自动续期、代理池熔断自愈，文本、联网搜索、生图、音乐、画布、视频一个端口全包。

> 官方仓库：**[github.com/dorokuma/gemini-web2api-go](https://github.com/dorokuma/gemini-web2api-go)** —— 发版、issue、文档都在这里；各 fork 不由本仓维护。本项目是 [zexadev/gemini-web2api-go](https://github.com/zexadev/gemini-web2api-go) 的维护分支，感谢原作者的开源贡献。

<a id="highlights"></a>

## 核心特性

| 特性 | 说明 |
|---|---|
| **零 API Key，单端口四协议全通** | OpenAI `/v1/chat/completions` 与 `/v1/responses`、Gemini 原生 `/v1beta`、异步视频 `/v1/videos`、HTTP MCP `/mcp`——一个端口同时说四种协议，现有客户端与网关开箱即用。 |
| **匿名即用，挂 Cookie 解锁全系** | 不登录就能跑 Flash 级文本 + 联网搜索；挂上 Cookie 解锁 4 Neon / Carbon / Hydrogen、思考链（`reasoning_content`）、Nano Banana 2K 原图放大、音乐与视频生成。 |
| **防封控与抗降级流水线** | 真实 Chrome TLS 指纹 + 可选常驻浏览器传输层；Cookie 自动保活、账号粘住出口 IP；代理池熔断自愈；`strict_model` 拒绝静默降级的假 200。 |
| **单二进制 + 零外链中文管理面板** | 6 平台预编译单文件与 distroless 镜像；Chart.js 随二进制内嵌、可断网运行；面板地址全相对，支持子路径反代。 |
| **隐私不落盘 + 多数据库** | Prompt 与回答正文永不写入磁盘，只存元数据；SQLite / MySQL / PostgreSQL 一键切换（`SQL_DSN`）。 |
| **开箱对接生态** | 与 Magpie 插件 `@dorokuma/magpie-gemini-web`、Cherry Studio、Cursor、Dify、newapi 无缝集成。 |

<a id="quick-start"></a>

## 快速开始

### 1. 下载或拉镜像

**预编译二进制（最省事）**——[Releases](https://github.com/dorokuma/gemini-web2api-go/releases) 挑对应平台的下载，不用 Go、不用 Docker，单文件就是全部：

```bash
chmod +x gemini-web2api-go_*
./gemini-web2api-go_* --port 8083 --admin-token your-admin-token
```

**Docker（不用源码）**：

```bash
docker run -d --name gemini-web2api \
  -p 127.0.0.1:8083:8083 \
  -v "$PWD/data:/data" \
  -e ADMIN_TOKEN=your-admin-token \
  ghcr.io/dorokuma/gemini-web2api-go:latest
```

**从源码跑**（装了 Go 就不必绕 Docker）：

```bash
git clone https://github.com/dorokuma/gemini-web2api-go
cd gemini-web2api-go
go build -o gemini-web2api-go .
./gemini-web2api-go --port 8083 --admin-token your-admin-token
```

### 2. 启动并打开管理面板

数据默认落在 `./data/gemini.db`，首次启动自动生成 API key（banner 里是打码的，完整值在管理面板「设置」页）。启动后会看到：

```
gemini-web2api-go v<version>
  Listening:   http://0.0.0.0:8083
  Base URL:    http://localhost:8083/v1
  API key:     sk-gemini-<masked>  (mutable in admin UI)
  Admin UI:    http://localhost:8083/admin  (token auth)
  DB:          ./data/gemini.db
  Models:      [gemini-3.5-flash-lite gemini-3.6-flash]
  Cookie:      none (anonymous)
  Proxy:       none (direct)
  Impersonate: chrome_146
  Tokenizer:   tiktoken cl100k_base
  Per-IP 限流: 并发=5 / RPM=30 / RPH=80
  Retry:       3x / 2s
  Browser:     disabled
```

管理面板在 `http://localhost:8083/admin`，用 `--admin-token` 登录。

### 3. 发出第一条请求

```bash
curl http://localhost:8083/v1/chat/completions \
  -H "Authorization: Bearer sk-<your-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.6-flash",
    "messages": [{"role": "user", "content": "你好！介绍一下你自己"}]
  }'
```

> [!TIP]
> 无需任何账号即可先测匿名文本 + 联网搜索；随时去管理面板「Cookie 池」添加 Google Cookie，解锁 Pro 级模型、思考链与媒体生成。

> 跑通了？顺手点个 ⭐——GitHub 的 topic 页按 star 排，这是别人找到这个项目的主要入口。有问题或想法开 issue；在 fork 里改好的功能，欢迎提 PR 回来。

<a id="more-details"></a>

## 完整参考与进阶配置

<details>
<summary><strong>📋 完整模型列表</strong>（与源码注册表严格同步，共 27 个）</summary>

模型跟着 Gemini 网页端的模型选择器走（清单来自 `batchexecute?rpcids=otAQ7b`）：3.6 Flash / 3.5 Flash-Lite / 3.1 Pro 免费号就能用，3.8 Flash / 4 Hydrogen / 4 Carbon / 4 Neon 要登录态（部分要付费号）；其余是它们的思考档、任务模式与媒体工具。

| 模型 | 说明 | 需要登录态 |
|---|---|---|
| `gemini-3.6-flash` | 全方位，默认 | 否（匿名可用，含联网搜索） |
| `gemini-3.5-flash-lite` | 极速、轻量 | 否（匿名可用） |
| `gemini-3.1-pro` | 最强，每次回答都带思考链 | 是（免费号即可；匿名会被静默降级成 3.5 Flash-Lite，所以不暴露） |
| `gemini-3.8-flash` | 3.8 Flash；Google 把原 3.7 那个 hex 原地升级成 3.8 | 是（**付费号专属**，免费号降级成 3.5 Flash-Lite） |
| `gemini-3.7-flash` | `gemini-3.8-flash` 的别名（同 hex，Google 把 3.7→3.8 原地改名） | 是（同上） |
| `gemini-4-hydrogen` | 4 Hydrogen（日常助手，与 3.8 Flash 同槽位原位升级） | 是（登录号即可） |
| `gemini-4-carbon` | 4 Carbon（原 4 Argon 原位改名，复杂任务/代码） | 是（**付费/Ultra 号专属**，免费号降级成 3.5 Flash-Lite） |
| `gemini-4-neon` | 4 Neon（深入分析） | 是（**付费/Ultra 号专属**） |
| `gemini-4-carbon-task` | 4 Carbon 任务模式（网页端「任务」开关，与对话模式分离的独立模型） | 是（付费/Ultra 号） |
| `gemini-4-neon-task` | 4 Neon 任务模式（同上） | 是（付费/Ultra 号） |
| `gemini-4-hydrogen-task` | 4 Hydrogen 任务模式（同上） | 是（登录号即可） |
| `gemini-3.6-flash-thinking` | 3.6 Flash 开扩展思考 | 是 |
| `gemini-3.5-flash-lite-thinking` | 3.5 Flash-Lite 开扩展思考 | 是 |
| `gemini-3.1-pro-thinking` | 3.1 Pro 开扩展思考 | 是 |
| `gemini-3.8-flash-thinking` | 3.8 Flash 开扩展思考 | 是 |
| `gemini-3.7-flash-thinking` | `gemini-3.8-flash-thinking` 的别名 | 是 |
| `gemini-3.8-flash-thinking-medium` | 3.8 Flash 的中等思考档 | 是 |
| `gemini-4-carbon-thinking` | 4 Carbon 高思考档（上游叫 High），回复带 `reasoning_content` | 是（付费/Ultra 号） |
| `gemini-4-carbon-thinking-medium` | 4 Carbon 中思考档（思考在服务端内部完成，不带 `reasoning_content`） | 是（付费/Ultra 号） |
| `gemini-4-neon-thinking` | 4 Neon 高思考档（上游自报 "4 Neon High"，服务端内部思考不带 `reasoning_content`） | 是（付费/Ultra 号） |
| `gemini-4-neon-thinking-medium` | 4 Neon 中思考档（上游自报 "4 Neon Medium"，同上） | 是（付费/Ultra 号） |
| `gemini-4-hydrogen-thinking` | 4 Hydrogen 高思考档，回复带 `reasoning_content` | 是（登录号即可） |
| `gemini-4-hydrogen-thinking-medium` | 4 Hydrogen 中思考档（服务端内部思考，不带 `reasoning_content`） | 是（登录号即可） |
| `gemini-image` | 生图（Nano Banana），产物 base64 data URL | 是 |
| `gemini-music` | 音乐（Lyria，约 30 秒），产物 base64 | 是 |
| `gemini-video` | 生视频（Veo，异步），产物 base64 MP4 | 是（通常要付费号） |
| `gemini-canvas` | 画布：生成交互 HTML 文档（内联返回 ```html 块） | 是 |

没配 cookie 时 `/v1/models` 只返回前两个，选 `gemini-3.1-pro` 会直接报错并说明原因——匿名请求它必然被静默降级成 3.5 Flash-Lite，与其回一个"成功但其实不是 Pro"的假结果，不如在选型时就失败。

**`-thinking`** 是网页 UI 上「扩展思考」的开关，跟模型正交；服务端回报的名字会带 `High`（如 `3.1 Pro High`），**只在登录态生效**，匿名请求会被静默忽略。

**`-task`** 是网页端「任务」（Task）模式的独立模型，与对话模式完全分离：任务模式走另一套协议槽位（`inner[49]=40` 等），不是对话模型的思考档变体。任务模式是账号功能，无 cookie 时不暴露。

**已移除的旧名**：`gemini-3.5-flash` / `gemini-3.5-flash-thinking` / `gemini-3.5-flash-thinking-lite` / `gemini-auto` / `gemini-flash-lite`（传了返回 400）——它们在服务端没有对应条目。

> **`@think=N` 已废弃。** 该后缀写进请求的 `inner[17]`，抓包证明它是会话内的轮次索引而非思考深度，参数从未生效过；仍被接受但直接忽略，不影响路由。

#### 思考链（`reasoning_content`）

`gemini-3.1-pro` 每次回答前先输出推理过程，本项目按事实标准 `reasoning_content` 字段暴露，newapi、Cherry Studio 等客户端渲染成可折叠「思考过程」。流式按 `delta.reasoning_content` 推，且思考块全部推完才开始推正文。`reasoning_tokens` 单独计数、不含在 `completion_tokens` 里（思考链默认折叠不展示，算进去等于让用户为看不见的输出买单）。

#### 已知能力边界

匿名调用只能拿到两个文本模型 + 联网搜索。挂 cookie 额外解锁：3.1 Pro、扩展思考版、读图/读视频、更长上下文（超长对话自动转文本附件），以及生图、音乐、画布、生视频。深度研究仍**未实现**（多步异步流程）。管理面板的「实际模型」列会把服务端实际用了哪个模型标出来，降级一眼可见。

#### 生图 / 音乐

`gemini-image`（Nano Banana）和 `gemini-music`（Lyria，约 30 秒）跟普通对话一样走 `/v1/chat/completions`，产物字节以 base64 data URL 放进 `content`（图片 `![image](data:image/png;base64,…)`、音频 `[audio](data:audio/mpeg;base64,…)`）。不转外链是有意的——Gemini 产物链要带 cookie 才下得到，所以服务端下回字节再转 base64。产物 base64 不计进 `completion_tokens`。`size` / `n` 等参数上游没有对应旋钮，传了被忽略。

打开「设置」页的**生图下载原图/放大**（`image_upscale`）后照网页「下载」按钮取图：Ultra 号放大到宽高各 2 倍（1376×768 → 2752×1536），普通号拿到未重新压缩的原始文件。

#### 上下文长度的两堵墙

默认把 `messages` 拼成单个 prompt 发，单次输入约 **13 万 UTF-8 字节**封顶，超了上游从尾部静默截断。挂 cookie 时超长对话自动转成 `message.txt` 附件，但附件另有约 **16 万字节**的内容总量墙。打开 `multi_turn` 后认出续接只发最新一句、历史留服务端；真正的长对话仍需客户端自己压缩。没挂 cookie 时超限直接返回 400 `context_length_exceeded`，不静默丢数据。

</details>

<details>
<summary><strong>🔌 协议接口矩阵与客户端接入</strong>（OpenAI / Gemini Native / Videos / MCP）</summary>

### OpenAI 协议覆盖

| 路径 | 状态 | 备注 |
|---|---|---|
| `POST /v1/chat/completions` | ✅ | 真流式：上游每出一帧就转发增量。chunk 序列为 `delta{role}` → `delta{content}`×N → 空 `delta`+`finish_reason` → `[DONE]`。带 `tools` 时正文照样逐段推，只把 tool_call 块收完整再发 |
| `POST /v1/responses` | ✅ | OpenAI Responses API（Codex CLI 用）。真流式，工具调用同上 |
| `GET /v1/models` | ✅ | 匿名只列 2 个文本模型，池里有 cookie 时再列出要登录的 |
| `GET /` | ✅ | 健康检查，不鉴权，返回 status/version/models |
| `/v1beta/models/…` | ✅ | Google 原生格式，见下表 |
| `/v1/embeddings`、`/v1/images/*`、`/v1/audio/*` | ❌ | 未实现，返回 404 |
| Function calling | ⚠️ | Prompt 级实现（模型输出 ` ```tool_call``` ` 块按 JSON 解析）。查私有数据可靠；Gemini 自己能做的会直接回答。agentic 客户端（Codex 等）已可用 |
| `tool_choice` | ⚠️ | `none` 不注入工具定义；`required` 加强制措辞。prompt 级无法真正强制 |
| `stream_options.include_usage` | ✅ | 在 `finish_reason` 后补 usage chunk |
| `usage` token 数 | ✅ | tiktoken cl100k_base |
| `n` > 1 | ❌ | 返回 400，上游只给一个候选 |
| 采样参数 | ➖ | `temperature` 等收下即忽略，网页协议没有这些旋钮 |
| `response_format` / `logprobs` | ➖ | 未实现，收下即忽略 |
| Vision / 图片输入 | ⚠️ | 挂 cookie 可用：`image_url` / `input_image`，单张 12MB 封顶。匿名返回 400 |
| Audio | ❌ | `input_audio` 返回 400 |

### Gemini 原生接口（`/v1beta`）

| 路径 | 说明 |
|---|---|
| `POST /v1beta/models/{model}:generateContent` | 非流式 |
| `POST /v1beta/models/{model}:streamGenerateContent` | 真流式；带 `?alt=sse` 回 SSE，不带回 JSON 数组 |
| `POST /v1beta/models/{model}:countTokens` | 本地 tiktoken 估算 |
| `GET /v1beta/models`、`GET /v1beta/models/{model}` | 模型清单，与 `/v1/models` 同一份 |
| `POST /v1/models/{model}:generateContent` 等 | 上面三个动作的 `/v1` 写法，同一套处理 |

鉴权用 `x-goog-api-key` 头或 `?key=` 参数，`Authorization: Bearer` 也认。模型名用本项目的（`gemini-3.6-flash` 等），不是官方的 `gemini-2.5-flash`。支持 `tools[].functionDeclarations`、`thinkingConfig.includeThoughts`（思考摘要以 `thought: true` part 返回）、`inlineData` / `fileData` 图片视频输入（要 cookie）。`generationConfig` 采样参数收下即忽略；错误体是 Google 形状 `{"error":{"code","message","status"}}`。

```python
from google import genai
from google.genai import types

client = genai.Client(api_key="<your-key>",
                      http_options=types.HttpOptions(base_url="http://localhost:8083"))
print(client.models.generate_content(model="gemini-3.6-flash", contents="Hello!").text)
```

### `/v1/videos` 异步视频生成

OpenAI(Sora) 形状的异步视频：`POST` 建任务、`GET /v1/videos/{id}` 轮询、`GET /v1/videos/{id}/content` 下 MP4（要登录态付费号）。支持 `input_reference` 图生视频（data/http(s) URL 或 multipart 文件）、`size`（`1280x720`/`720x1280`）或 `aspect_ratio`、`POST /v1/videos/{id}/extend` 续写（需 `temporary_chat` 与 `auto_delete_conversation` 都关着）。失败立即给原因：`video_quota_exhausted` / `video_refused`。某账号视频额度用完会自动换号，全池冷却回 429 带 `Retry-After`。`GET /admin/api/video-quota` 可查各账号滚动 5 小时窗口的视频用量（每次刷新每个号约 2+N 次上游请求，别频繁轮询）。

### MCP 工具（`/mcp`）

同一进程同一端口挂了 MCP server（Streamable HTTP），把联网搜索、生图、音乐、视频暴露成工具，Claude Desktop / Claude Code / Cursor 直接调用。搜索匿名可用，媒体要 cookie（视频要付费号）。

```json
{
  "mcpServers": {
    "gemini-search": {
      "url": "http://localhost:8083/mcp",
      "headers": { "Authorization": "Bearer sk-<your-key>" }
    }
  }
}
```

Claude Code：`claude mcp add --transport http gemini-search http://localhost:8083/mcp --header "Authorization: Bearer sk-<your-key>"`。

| 工具 | 参数 | 返回 |
|---|---|---|
| `web_search` | `query` | 联网查证后的答案，末尾附 `Sources:` 来源清单 |
| `generate_image` | `prompt`，可选 `image_urls` | 图片（MCP `image` 内容） |
| `generate_music` | `prompt` | 约 30 秒音乐（MCP `audio` 内容） |
| `create_video_task` | `prompt`，可选 `size`、`input_reference` | 任务 id，与 `/v1/videos` 同一套任务 |
| `get_video_status` | `id`，可选 `include_video` | 任务状态；完成且 `include_video=true` 附 MP4 |

### 客户端接入

任何 OpenAI 兼容客户端（Cherry Studio / ChatBox / Open WebUI / Dify / Cursor / …）都是同一套填法：

| 字段 | 值 |
|---|---|
| Base URL | `http://localhost:8083/v1`（部分客户端只要 `http://localhost:8083`，会自己拼 `/v1`） |
| API Key | 管理面板「设置」页的 `sk-<your-key>` |
| 模型 | `gemini-3.6-flash` |

**newapi / one-api 渠道**：类型选 OpenAI，Base URL `http://localhost:8083`（同在 docker 里写 `http://host.docker.internal:8083` 或容器名），密钥填 API key，模型填 `gemini-3.6-flash,gemini-3.5-flash-lite`。**Codex CLI** 走 `/v1/responses`，base url 指到 `http://localhost:8083/v1`。**Google 格式客户端**走 `/v1beta`；Gemini CLI 没实测过。另有健康检查 `GET /`（不鉴权）。

Windows PowerShell 下 `curl` 是 `Invoke-WebRequest` 的别名，要用 `curl.exe` 加 `--%`：

```powershell
curl.exe --% http://127.0.0.1:8083/v1/chat/completions -H "Content-Type: application/json" -H "Authorization: Bearer sk-<your-key>" -d "{\"model\":\"gemini-3.6-flash\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello!\"}]}"
```

</details>

<details>
<summary><strong>⚙️ 配置项与 CLI 参数速查表</strong></summary>

配置分两类，按「改了要不要重启」分：

### 运行时配置 → 管理面板「设置」页

保存**立刻生效**，不用重启；存在数据库里，优先级高于 `config.json` 和命令行参数。

| 项 | 说明 |
|---|---|
| 默认模型 | 客户端没传 `model` 时用哪个 |
| 每 slot 并发 / RPM / RPH | 限流额度，0 = 不限 |
| Prompt 字节上限（`max_prompt_bytes`） | 超了：挂 cookie 转文本附件（约 16 万字节），没挂回 400 `context_length_exceeded`，都不静默截断。按 UTF-8 字节算，默认 128000。0 = 不限 |
| 多轮（`multi_turn`） | 默认关。开后走服务端续接：认出续接只发最新一句、历史留服务端，长会话不撞字节墙。登录/匿名都可用。不放大模型上下文窗口 |
| 自动删会话（`auto_delete_conversation`） | 默认开启（已有数据库中的面板配置会覆盖初值）。每次出完结果自动删掉网页端会话，只登录态生效，异步 best-effort；如需保留历史记录或做视频续写，请关闭该项 |
| 临时对话（`temporary_chat`） | 默认关。开后会话不进账号列表、不用于训练，比自动删更干净。与 `multi_turn` 互斥 |
| 生图下载原图/放大（`image_upscale`） | 默认关。开后照网页「下载」按钮取图：Ultra 号放大 2 倍，普通号拿原始文件。每张多一次请求 |
| 匿名优先（`anon_first`） | 默认关。开后不需登录态的请求走匿名、不占 cookie 账号，省账号额度 |
| 严格模型校验（`strict_model`） | 默认关。开后上游静默降级不当成功：换号重试，全池降级回 `429 insufficient_quota`（`code: model_downgraded`，带 `Retry-After`）。流式在吐首字前判断。媒体模型不参与 |
| 降级冷却（`strict_model_cooldown_min`） | 被降级的号在该模型上歇多久再用，默认 300 分钟。0 = 不记 |
| 重试次数 / 重试间隔 / 上游超时（`retry_attempts` / `retry_delay_sec` / `request_timeout_sec`） | 上游失败时的重试策略：`retry_attempts` 为总尝试次数（1-10），`retry_delay_sec` 为失败后重试间隔秒数，`request_timeout_sec` 是上游连续沉默阈值（5-600 秒）——任何字节或心跳帧到达都会重置计时，并非整次请求的硬截止时间 |
| 超时后重试（`retry_on_timeout`） | 默认开。关掉后上游沉默超限时直接报错、不再重发（重发多半还是会卡沉默）。空响应等其他失败照常重试 |
| 流式保活（`stream_keepalive_sec`） | 默认 0（关）。流式连续这么多秒没写数据就发保活，防中间层 504。建议 15。代价：保活先开流（HTTP 200），之后失败只能在流里报错。非流式无效 |
| 明细保留天数（`retention_days`） | 过期只删明细，聚合数据永久保留 |
| TLS 指纹（`impersonate`） | `chrome_146`（默认）/ `chrome_144` / `chrome_133` / `firefox_147` / `safari_16_0` / `safari_ios_17_0` |
| Firefox 指纹（`firefox_fingerprint`） | 实验，默认关。开后请求伪装 Firefox：直连时 TLS 握手、UA 换 Firefox、去 Chrome 专有 `Sec-CH-UA`。cookie 从 Firefox 导出时建议配套开。走代理时 TLS 层改不了，只有 header 变 |
| Gemini `bl` 版本（`gemini_bl`） | 上游前端版本号，过期时改这里 |
| 自动跟随 bl（`gemini_bl_auto`） | 每 6 小时从 `/app` 页抓最新版本号，抓不到用上面钉的值 |
| 熔断冷却（`proxy_cooldown_min`） | 代理连续失败 5 次后歇多久再自动放回池子，默认 120 分钟（实测被拦出口 106-121 分钟自恢复）。0 = 不自动恢复 |
| 无可用代理时退回直连（`fallback_direct`） | 默认关：代理全满/全熔断直接报错，本机 IP 不暴露。开：可用性优先，但上游看到本机出口 |
| cookie 失效时降级匿名（`fallback_anon`） | 默认关：直接报错，立刻发现该换号。开：继续跑，但 Pro 被静默降级、思考链消失 |
| 新协议（`web_channel`） | **当前版本已锁定禁用**：`gemini.go` 里 `useWC := false` 硬编码，开启此开关**不会改变**实际行为，只在日志里提示被忽略。字段保留仅为面板兼容，启用前置条件见 `.agents/notes/webchannel-enable-prerequisites.md` |
| 浏览器传输（`browser_transport`） | 默认关。开后对话请求走常驻真实浏览器（Chromium CDP + 页内流式 fetch），TLS/JS 指纹与真实浏览器一致，Cookie 由浏览器 jar 接管。常驻约 1.45 GB 内存，需要真实浏览器指纹时才开。策略 M1 单常驻粘性主号 |
| 浏览器缺席时回退 HTTP（`browser_fallback_http`） | 默认关：浏览器传输开着但找不到/拉不起浏览器时回 502 `browser_unavailable`，绝不静默降级。显式开 `true` 才回退纯 Go HTTP |
| 打印请求日志（`log_requests`） | 默认开。关闭后停止向 stderr 打印逐条请求日志 |

所有值都在后端做范围校验（如 `retry_attempts` 只接受 1-10、超时 5-600 秒），非法值被拒绝并说明原因。

### 凭证 → 也在面板

| 项 | 说明 |
|---|---|
| API Key | 首次启动自动生成，面板里可轮换或自定义 |
| Google Cookie | 面板「Cookie 池」页导入，保存即生效，可导入多个账号。池里有账号后要登录的模型才出现在列表里 |

已保存的值不回显（cookie 只显示识别到几个、关键项齐不齐）。

### 部署期配置 → `docker-compose.yml`

| 项 | 位置 |
|---|---|
| 监听端口 | `ports` + `command: --port` |
| 数据库路径 | `volumes` + `command: --db` |
| `ADMIN_TOKEN` | `environment`，面板登录 token |

`API_KEY` 环境变量锁死 API key（面板改不了）。`--proxy` 和 `--cookie-file` 是**播种参数**：启动时导入池子，之后一律从面板管理。优先级：**面板改动 > CLI flag / `config.json` > 内置默认**。不用 Docker 时可把 `config.example.json` 复制成 `config.json` 当启动模板（自动找 `./config.json`，其次 `$HOME/.config/gemini-web2api/config.json`）。

### 命令行参数全集

| flag | 说明 |
|---|---|
| `--port` | 监听端口，默认 8083 |
| `--config` | 指定 `config.json` 路径 |
| `--db` | SQLite 路径，默认 `./data/gemini.db` |
| `--admin-token` | 面板登录 token，留空 = 不鉴权（只有绑 127.0.0.1 才可接受） |
| `--api-key` | 锁定 `/v1/*` 的 key（面板改不了），等价 `API_KEY` 环境变量 |
| `--cookie-file` | 启动时把文件里的 cookie 导入 Cookie 池 |
| `--proxy` | 启动时把这个代理导入代理池 |
| `--impersonate` | TLS 指纹档位 |
| `--browser-path` | 显式 Chromium/Chrome/Edge 可执行路径（部署级只读，等价 `BROWSER_PATH`） |
| `--browser-profile-dir` | 显式浏览器 profile 目录（部署级只读，等价 `BROWSER_PROFILE_DIR`，默认 `<db 目录>/browser-profile`） |
| `--version` | 打印版本退出（Docker healthcheck 用的就是它） |

> `browser_path` / `browser_profile_dir` 是**部署级**配置，刻意不进管理面板：`/admin` 默认免鉴权，面板可写浏览器可执行路径等于本地任意命令执行。

### MySQL / PostgreSQL

设环境变量 `SQL_DSN`（不设 = SQLite）：

```bash
# MySQL（也接受 go-sql-driver 原生 user:pass@tcp(host:3306)/dbname）
SQL_DSN="mysql://user:pass@host:3306/dbname"
# PostgreSQL
SQL_DSN="postgres://user:pass@host:5432/dbname?sslmode=disable"
```

建表自动完成，三种库共用同一套 schema。单机用 SQLite 最省事，多实例共享池子才需要 MySQL/PG。

</details>

<details>
<summary><strong>🛡️ 防封控原理、排障速查与 FAQ</strong></summary>

### 为什么需要代理池

单 IP 突发地打，实测 **80-180 次请求**后被重定向到 `google.com/sorry/index`（CAPTCHA 页）。跨度由连接策略、出口质量、请求节奏共同决定：连接复用值约 60%（并发 10 时复用 172/177 vs 新建 106/109）；**平缓节奏最管用**——同一静态 IP 突发 188 次被拦，改成 10 次/分钟连打 **800 次、跨 110 分钟一次没被拦**。默认 `per_ip_rph=80` 是很保守的下沿。

被拦是硬拦：60 次探测零成功，约 **106-121 分钟**自动恢复（代理池默认 120 分钟冷却就是这么来的）。代理失败会提前消耗额度（失败请求部分已到达 Google 并被计数）。

**解法**：面板「代理池」加多个代理，每个代理是独立 IP slot，独立并发/RPM/RPH 配额，N 个代理 = N 倍容量。支持 `http://` / `https://` / `socks5://` / `socks5h://`。配了代理后默认不退回直连；连续失败 5 次自动熔断；全满回 429，全熔断回 502 `proxy_unavailable`。**不读 `HTTPS_PROXY` / `ALL_PROXY` 环境变量**——代理只从池里取。

### 指纹模拟

直连走 [bogdanfinn/tls-client](https://github.com/bogdanfinn/tls-client)，TLS 握手 + HTTP/2 SETTINGS + ALPS 全对齐真实 Chrome 146，Google 风控视角与真浏览器无别。走代理时换 stdlib `net/http`（兼容性最佳），header 仍按 Chrome 146 伪装——但**代理时 TLS 指纹是 Go 标准库的，不是 Chrome 的**（HTTP CONNECT 只建隧道，TLS 端到端）。这不影响封禁阈值（实测差距 8 次，出口间方差 36%）。实测意外：朴素 SDK 触发风控拿 429，伪装 Chrome 后拿 302 跳 sorry 页——证明 Google 真把我们认成浏览器。

### Cookie 池与保活

挂 cookie 后请求走登录态。面板「Cookie 池」是唯一入口，按最久未用优先轮转；连续 3 次鉴权失败自动停用（网络错误、`/sorry` 拦截不计——否则失败次数变成代理噪音）。**cookie 自动续期**：收下上游 `Set-Cookie` 刷新的 `SIDCC`/`__Secure-1PSIDCC`/`__Secure-3PSIDCC` 写回账号；每 10 分钟往 `accounts.google.com/RotateCookies` 打保活换发 `__Secure-1PSIDTS`（约 30 分钟过期的票）。

> **请用 Firefox 导出 cookie。** 新版 Chrome 开了设备绑定会话（DBSC），导出的会话绑设备，`*PSIDTS` 续不了，半小时到几小时就死。Firefox 没这层绑定可一直续。用 Chrome 的话：全新隐私窗口登录、立刻导出、**马上关窗口**。

**每个账号粘住自己的出口**（否则同一账号从几十个 IP 发请求是账号共享的典型特征）。浏览器登多个 Google 号时用「账号序号」指定（地址栏 `/u/1/` 就填 1）——不填一律落默认号，订阅在别的号上时付费模型被静默降级。

导入方式：DevTools → Application → Cookies 复制 `SID` / `HSID` / `SSID` / `APISID` / `SAPISID` / `__Secure-1PSID` / `__Secure-1PSIDTS`，粘进面板「Cookie 池 → 添加账号」，或写 `cookie.txt` 后 `--cookie-file cookie.txt` 播种。带 `SAPISID` 的请求自动算 `SAPISIDHASH`，所以这项不能少。

### 隐私

- **Prompt 和 response 内容永不入库**——只存元数据：模型、代理 ID、延迟、token 数、状态码、错误类型
- 历史 `prompt_preview` / `response_preview` 列从老版本迁移时自动 DROP
- Token 数用 tiktoken cl100k_base BPE 精确计算（网页端不返回真 token 数，只能本地估算，偏差 ±20% 以内）
- **检查更新由浏览器发起**，服务端不联网查版本——配了代理池的部署不会因查更新从本机 IP 出网

### 排障速查

| 现象 | 多半是什么 | 怎么办 |
|---|---|---|
| 我们返回 **429** | 所有 IP slot 并发/RPM/RPH 占满，**不是 Google 拒绝** | 加代理，或调高限额 |
| 面板诊断 **302 → `google.com/sorry/index`** | 出口 IP 被 Google 拦（80-180 次后） | 换出口/加代理。硬拦不是概率性，原地重试没意义 |
| 偶发空响应 | 上游瞬时拒绝（`1155`），无预测阈值 | 重发一次通常就好，降 RPM 解决不了 |
| **502 `reason: empty_content`** | 上游回了帧但抽不出正文（含只有思考链的情形——思考链不是正文，明返 502 不回假 200） | 重发；反复出现说明协议字段变了，该看代码 |
| 请求全部超时 | 本机到 `gemini.google.com` 不通 | 配代理。注意不读 `HTTPS_PROXY` |
| 启动即退出 `unable to open database file (14)` | 容器 nonroot(uid 65532) 写不进 root 属主的 bind mount | 用具名卷，或 `sudo chown -R 65532:65532 ./data` |
| 选 `gemini-3.1-pro` 直接报错 | 没配 cookie 时不暴露，故意的 | 面板 Cookie 池导入 cookie |
| 挂 cookie 后请求全部 502 | cookie 失效，取不到 XSRF token | 重新导出。判据：`gemini-3.1-pro` 回报 3.5 Flash-Lite 就是失效了 |
| cookie 半小时失效 | Chrome 设备绑定会话，`__Secure-1PSIDTS` 换发 401 | 用 **Firefox** 重新登录导出 |
| 付费号 cookie 但付费模型回报 3.5 Flash-Lite | 浏览器登了多个号，订阅不在默认号 | Cookie 池给这条填「账号序号」 |
| 面板打不开 / 401 | `--admin-token`（或 `ADMIN_TOKEN`）没对上 | token 留空不鉴权，只有绑 127.0.0.1 才可接受 |

上游失败回 502 时 body 带结构化诊断：`reason`（`no_content_frame` / `failure_reply` / `upstream_timeout` / `empty_content` / `upstream_http` / `upstream_error`）+ `upstream_model`（上游自报名，解析不到为 `unknown`）+ `frames` + `content_frame` + `hint`。同一套 `[reason]` 前缀落 `requests.error` 供面板过滤。

Docker 默认 bridge 网络上游可能返回空内容（Google 拒绝某些 NAT 段，本仓没复现过），真遇到试 `network_mode: host` 验证。

### FAQ

**这和 Google 官方 Gemini API 有什么区别？需要 API Key 吗？**
不需要。本项目直接反代 gemini.google.com 网页端协议，没有 API Key、没有付费配额。代价是能力边界跟着网页端走：网页端有的（联网搜索、生图、音乐、画布、生视频）能用，没有的（深度研究）也没有。

**会被封号吗？**
按**出口 IP + TLS 指纹**不按账号。突发 80-180 次触发 sorry 页，约两小时自恢复；平缓打几乎打不满。付费不提高阈值。放大产能靠代理池分散出口，不是换账号。

**免费账号和付费账号差在哪？**
匿名：3.6 Flash / 3.5 Flash-Lite + 联网搜索。免费号 cookie：3.1 Pro、`-thinking` 版、读图/读视频、生图、音乐、画布。付费号才有：3.8 Flash / 4 Carbon / 4 Neon（含各自思考档与 task 模式）和生视频。4 Hydrogen 及其 `-thinking` / `-task` 变体任何登录账号即可用，不要求付费。

**为什么每发一条消息网页端就多一个会话？**
默认每请求独立会话。开 `multi_turn` 后续接复用同一网页会话；不想堆会话开 `temporary_chat`（不进列表、不入训练，推荐）或 `auto_delete_conversation`。

**能当 Cursor / Dify 这类 agent 的后端吗？**
能接（OpenAI 兼容），纯聊天写代码没问题；但网页端单请求约 13 万字节封顶（挂 cookie 约 16 万），agent 塞几万字系统提示容易答非所问——这是网页端模型的先天限制。

**生图能出 2K 吗？**
`gemini-image` 走网页端生图工具位不锁模型。默认约 100 万像素（1376×768）；Ultra 号开 `image_upscale` 拿 2 倍放大 2752×1536（约 2K），普通号拿未重压缩原始文件。

**为什么反代后面流式变成一次性输出？**
本项目是真流式（已带 `X-Accel-Buffering: no`），经过 Nginx 等被缓冲是反代自己的设置问题，直连时逐段输出。

</details>

<details>
<summary><strong>🔧 源码结构与本地开发 / 发版说明</strong></summary>

### 项目结构

```
main.go                    只有一句 app.Run()；入口留在根目录，`go build .` 直接可用
internal/app/              全部实现
  app.go                   flag 解析 + 路由注册 + 启动
  config.go                配置加载 + 默认值
  client.go                tls-client (chrome146) + stdlib (走代理) 双 client
  gemini.go                模型表 + 80 槽 payload + 模型 header + StreamGenerate + wrb.fr 解析
  webchannel.go            新协议 processSession（WebChannel）传输，开关 web_channel（当前版本锁定禁用）
  conversation.go          服务端多轮续接（multi_turn）
  media.go                 生图 / 音乐 / 画布 / 视频：工具位 + 产物抽取与下载
  image_upscale.go         生图下载原图/放大（rpc c8o8Fe，开关 image_upscale）
  search.go                联网搜索引用来源抽取
  strict_model.go          严格模型校验：识别静默降级、按（账号, 模型）冷却
  xsrf.go                  带 cookie 时必需的 XSRF token：抓取 + 按 cookie 缓存 + 过期自愈
  messages.go              OpenAI messages → prompt，tool_call 解析
  server.go                /v1/* + 限流入口 + 参数校验 + metrics 写入
  gemini_api.go            Gemini 原生接口 /v1beta/models/…
  videos.go                /v1/videos 异步视频任务
  video_cooldown.go        视频额度冷却 + 任务内换号
  video_quota.go           /admin/api/video-quota：按账号读历史统计视频用量
  mcp.go                   MCP server（/mcp）
  mcp_media.go             MCP 媒体工具：生图 / 音乐 / 视频
  sse.go                   SSE 写出器（懒发 header，失败仍能返回 502 JSON）
  ratelimit.go             每 IP slot 独立并发 / RPM / RPH 限流
  tokenizer.go             tiktoken cl100k_base 单例
  apikey.go                API key（启动参数锁定 / 面板可轮换 双轨）
  db.go                    数据库 schema（SQLite / MySQL / PostgreSQL 共用）
  dbdialect.go             多数据库方言：SQL_DSN 选 MySQL / PostgreSQL
  proxy.go                 代理池 CRUD + 容量调度 + 熔断
  cookie_pool.go           Cookie 池数据层
  rotate.go                会话保活（RotateCookies）
  upload.go                附件上传（content-push 两步 resumable）
  context_file.go          超长对话转文本附件
  vision.go                图片输入
  bl.go                    上游前端版本号 bl 自动跟随
  scheduler.go             小时/天聚合 + 过期明细清理
  runtime.go               运行时配置快照（面板改完即时生效）
  admin.go                 /admin/api/* 鉴权 + REST
  admin_cookies.go         Cookie 池的 admin REST（返回前脱敏）
  admin_ui/                管理面板前端（单页 + Chart.js，随二进制打包，不走 CDN）
Dockerfile                 多阶段构建（alpine builder → distroless runtime）
docker-compose.yml         单容器，默认拉 ghcr 镜像，sqlite 挂 volume
.github/workflows/         docker.yml 推镜像 / release.yml 打 tag 发二进制
```

### 本地构建与版本注入

源码 `go build` 出来的二进制 `--version` 报 `dev`：版本号不是写在代码里的常量，`internal/app` 里是 `var Version = "dev"`，由发布流程构建时注入——release workflow 用 `-ldflags "-X gemini-web2api/internal/app.Version=<version>"`，Docker 镜像走 `--build-arg VERSION=<version>`（`<version>` 为去 `v` 前缀的版本号）。本地注入：

```bash
go build -ldflags "-X gemini-web2api/internal/app.Version=my-build" -o gemini-web2api-go .
```

改了代码想用容器跑，把 `docker-compose.yml` 里 `build:` 那两行的注释去掉，再 `docker compose up -d --build`。

### 版本与发版规范

- 正式版标签使用标准语义化版本 `vX.Y.Z`（annotated git tag），不再使用 `-magpie.N` 后缀。
- `internal/app/server.go` 中 `var Version = "dev"` 保持为可注入变量，构建统一以 `-ldflags -X` 注入去 `v` 版本号。
- 每次发版前必须在 `CHANGELOG.md` 补齐对应 `## X.Y.Z` 小节，并通过 `make release-check TAG=vX.Y.Z`。
- 本仓是 6 平台预编译二进制与 GHCR 多架构镜像的唯一权威发布源；下游插件 `@dorokuma/magpie-gemini-web`（自 v0.15.0 起）通过 `lib/backend-release.json` 直接固定本仓正式 Release 二进制 SHA-256。

</details>

---

## Star History

读到这儿，说明它对你有用。点个 ⭐ 让更多人找到它；遇到问题开 issue，改好的功能提 PR。

<a href="https://star-history.com/#dorokuma/gemini-web2api-go&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=dorokuma/gemini-web2api-go&type=Date&theme=dark">
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=dorokuma/gemini-web2api-go&type=Date">
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=dorokuma/gemini-web2api-go&type=Date">
  </picture>
</a>

## 致谢

- [bogdanfinn/tls-client](https://github.com/bogdanfinn/tls-client) — Chrome 真指纹 TLS 库
- [pkoukk/tiktoken-go](https://github.com/pkoukk/tiktoken-go) — BPE tokenizer
- [modernc.org/sqlite](https://gitlab.com/cznic/sqlite) — 纯 Go SQLite（CGO-free，alpine 直接编）
- [Sophomoresty/gemini-web2api](https://github.com/Sophomoresty/gemini-web2api) —— 同类的 Python 实现，早期摸 Gemini 网页协议时借鉴过它的思路
- [LINUX DO](https://linux.do) —— 本项目在该社区分享

## License

MIT — 详见 [LICENSE](LICENSE)
