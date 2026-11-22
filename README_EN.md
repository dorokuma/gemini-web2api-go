<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="assets/logo-light.svg">
  <img src="assets/logo-light.svg" alt="gemini-web2api-go logo" width="64" height="64">
</picture>

# gemini-web2api-go

**Turn the Gemini web app into a self-hosted OpenAI, native Gemini & MCP API — no Google API key, single Go binary, built-in admin panel, image & video generation included.**

[![Release](https://img.shields.io/github/v/release/dorokuma/gemini-web2api-go?style=flat-square&labelColor=18181b)](https://github.com/dorokuma/gemini-web2api-go/releases/latest)
[![Go](https://img.shields.io/badge/Go-1.27%2B-00ADD8?style=flat-square&labelColor=18181b&logo=go&logoColor=white)](https://go.dev)
[![Docker](https://img.shields.io/badge/Docker-distroless-2496ED?style=flat-square&labelColor=18181b&logo=docker&logoColor=white)](Dockerfile)
[![MCP](https://img.shields.io/badge/MCP-HTTP%20Server-8b5cf6?style=flat-square&labelColor=18181b)](#more-details)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat-square&labelColor=18181b)](LICENSE)

[Quick Start](#quick-start) · [Highlights](#highlights) · [API Surface](#more-details) · [Model List](#more-details) · [FAQ](#more-details)

[简体中文](README.md) · **English**

</div>

```mermaid
flowchart LR
    A["Clients<br/>Cherry Studio · Cursor · Claude Code · newapi"] -->|"OpenAI /v1 · /v1beta · /v1/videos · /mcp"| B["gemini-web2api-go<br/>single binary · admin panel"]
    B -->|"Chrome-fingerprint TLS"| C["Cookie Pool + Proxy Pool<br/>rotation · circuit breaker · sticky exits"]
    C --> D["gemini.google.com"]
```

**gemini-web2api-go** is a self-hosted reverse proxy: instead of wrapping Google's official API, it proxies the gemini.google.com web-app protocol directly and turns the web app's free models into a standard OpenAI-compatible API. A single Go binary with a built-in admin panel, a self-renewing cookie pool and a self-healing proxy pool — text, web search, image, music, canvas and video generation on one port.

> Official repository: **[github.com/dorokuma/gemini-web2api-go](https://github.com/dorokuma/gemini-web2api-go)** — releases, issues and docs live here; forks are not maintained by this project. This is a maintained fork of [zexadev/gemini-web2api-go](https://github.com/zexadev/gemini-web2api-go); thanks to the original author for open-sourcing the project.

<a id="highlights"></a>

## Highlights

| Feature | What it means |
|---|---|
| **Zero API key, four protocols on one port** | OpenAI `/v1/chat/completions` & `/v1/responses`, native Gemini `/v1beta`, async video `/v1/videos`, HTTP MCP `/mcp` — one port speaks all four, so existing clients and gateways work out of the box. |
| **Anonymous-ready, a cookie unlocks everything** | Text models plus web search with no sign-in at all; attach a cookie to unlock 4 Neon / Carbon / Hydrogen, reasoning chains (`reasoning_content`), Nano Banana 2K full-size images, music and video generation. |
| **Anti-blocking, anti-downgrade pipeline** | Real Chrome TLS fingerprint plus an optional resident-browser transport; cookies kept alive automatically with accounts pinned to their exit IPs; proxy pool with circuit breaking; `strict_model` refuses silently downgraded fake-200 answers. |
| **Single binary + zero-CDN admin panel** | Prebuilt single files for 6 platforms and a distroless image; Chart.js is embedded so the panel works fully offline; every panel URL is relative, so reverse-proxying under a subpath just works. |
| **Privacy-first storage + multi-database** | Prompt and response bodies are never written to disk — metadata only; SQLite / MySQL / PostgreSQL switchable via `SQL_DSN`. |
| **Ecosystem-ready** | Works seamlessly with the Magpie plugin `@dorokuma/magpie-gemini-web`, Cherry Studio, Cursor, Dify and newapi. |

<a id="quick-start"></a>

## Quick Start

### 1. Download or pull the image

**Prebuilt binary (simplest)** — grab your platform's file from [Releases](https://github.com/dorokuma/gemini-web2api-go/releases). No Go, no Docker — the single file is the whole thing:

```bash
chmod +x gemini-web2api-go_*
./gemini-web2api-go_* --port 8083 --admin-token your-admin-token
```

**Docker (no source needed)**:

```bash
docker run -d --name gemini-web2api \
  -p 127.0.0.1:8083:8083 \
  -v "$PWD/data:/data" \
  -e ADMIN_TOKEN=your-admin-token \
  ghcr.io/dorokuma/gemini-web2api-go:latest
```

**From source** (if you have Go, skip Docker entirely):

```bash
git clone https://github.com/dorokuma/gemini-web2api-go
cd gemini-web2api-go
go build -o gemini-web2api-go .
./gemini-web2api-go --port 8083 --admin-token your-admin-token
```

### 2. Run & open the admin panel

Data lands in `./data/gemini.db` by default. An API key is generated on first boot — the banner masks it; the full value is on the panel's **Settings** page. The startup banner:

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

(`Cookie` shows `none (anonymous)` when the pool is empty, or `<enabled>/<total> in pool` once accounts are imported. `Per-IP 限流` is the per-IP rate-limit line — the program prints these Chinese labels verbatim in every locale: `并发` = concurrency, `RPM`/`RPH` = requests per minute/hour.)

The admin panel is at `http://localhost:8083/admin`, sign in with `--admin-token`.

### 3. Send your first request

```bash
curl http://localhost:8083/v1/chat/completions \
  -H "Authorization: Bearer sk-<your-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.6-flash",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

> [!TIP]
> No account needed to try anonymous text + web search; add a Google cookie under **Cookie pool** in the admin panel anytime to unlock the Pro-tier models, reasoning chains and media generation.

> Got it running? A ⭐ helps: GitHub topic pages rank by stars, and that is how most people find this project. Issues and ideas are welcome, and if you have improved something in a fork, send it back as a PR.

<a id="more-details"></a>

## More Details

<details>
<summary><strong>📋 Full model list</strong> (strictly synced with the source registry — 27 models)</summary>

Models follow the Gemini web app's model picker (the list comes from `batchexecute?rpcids=otAQ7b`): 3.6 Flash / 3.5 Flash-Lite / 3.1 Pro work on a free account, while 3.8 Flash / 4 Hydrogen / 4 Carbon / 4 Neon need a signed-in session (some need a paid one); the rest are their thinking tiers, task-mode variants and media tools.

| Model | Description | Sign-in required |
|---|---|---|
| `gemini-3.6-flash` | All-round, default | No (works anonymously, web search included) |
| `gemini-3.5-flash-lite` | Fast and lightweight | No (works anonymously) |
| `gemini-3.1-pro` | Most capable; every reply carries a reasoning chain | Yes (a free account is enough; anonymous calls get silently downgraded to 3.5 Flash-Lite, so it isn't exposed) |
| `gemini-3.8-flash` | 3.8 Flash; Google bumped the former 3.7 hex to 3.8 in place | Yes (**paid accounts only**; free accounts are downgraded to 3.5 Flash-Lite) |
| `gemini-3.7-flash` | Alias of `gemini-3.8-flash` (same hex; Google renamed 3.7→3.8) | Yes (same as above) |
| `gemini-4-hydrogen` | 4 Hydrogen (Everyday help; replaced 3.8 Flash on the same slot) | Yes (any signed-in account) |
| `gemini-4-carbon` | 4 Carbon (renamed in place from 4 Argon; complex tasks/code) | Yes (**paid/Ultra only**; free accounts are downgraded to 3.5 Flash-Lite) |
| `gemini-4-neon` | 4 Neon (deeper analysis) | Yes (**paid/Ultra only**) |
| `gemini-4-carbon-task` | 4 Carbon in task mode (the web "Task" switch — a separate model from chat mode) | Yes (paid/Ultra) |
| `gemini-4-neon-task` | 4 Neon in task mode (same as above) | Yes (paid/Ultra) |
| `gemini-4-hydrogen-task` | 4 Hydrogen in task mode (same as above) | Yes (any signed-in account) |
| `gemini-3.6-flash-thinking` | 3.6 Flash with extended thinking | Yes |
| `gemini-3.5-flash-lite-thinking` | 3.5 Flash-Lite with extended thinking | Yes |
| `gemini-3.1-pro-thinking` | 3.1 Pro with extended thinking | Yes |
| `gemini-3.8-flash-thinking` | 3.8 Flash with extended thinking | Yes |
| `gemini-3.7-flash-thinking` | Alias of `gemini-3.8-flash-thinking` | Yes |
| `gemini-3.8-flash-thinking-medium` | 3.8 Flash at the Medium thinking tier | Yes |
| `gemini-4-carbon-thinking` | 4 Carbon High thinking tier; carries `reasoning_content` | Yes (paid/Ultra) |
| `gemini-4-carbon-thinking-medium` | 4 Carbon Medium thinking tier (thinks server-side, no `reasoning_content`) | Yes (paid/Ultra) |
| `gemini-4-neon-thinking` | 4 Neon High thinking tier (upstream reports "4 Neon High"; server-side, no `reasoning_content`) | Yes (paid/Ultra) |
| `gemini-4-neon-thinking-medium` | 4 Neon Medium thinking tier (upstream reports "4 Neon Medium"; same as above) | Yes (paid/Ultra) |
| `gemini-4-hydrogen-thinking` | 4 Hydrogen High thinking tier; carries `reasoning_content` | Yes (any signed-in account) |
| `gemini-4-hydrogen-thinking-medium` | 4 Hydrogen Medium thinking tier (server-side, no `reasoning_content`) | Yes (any signed-in account) |
| `gemini-image` | Image generation (Nano Banana); base64 data-URL output | Yes |
| `gemini-music` | Music (Lyria, ~30s); base64 output | Yes |
| `gemini-video` | Video generation (Veo, async); base64 MP4 | Yes (usually a paid account) |
| `gemini-canvas` | Canvas: an interactive HTML document, returned inline as a ```html block | Yes |

Without a cookie, `/v1/models` returns only the first two, and asking for `gemini-3.1-pro` fails with an explanation — an anonymous request for it is always silently downgraded to 3.5 Flash-Lite, so it's better to fail at model selection than to hand back a "successful" reply that isn't Pro.

**`-thinking`** is the web UI's "extended thinking" switch, orthogonal to the model; the backend then reports a `High` suffix (e.g. `3.1 Pro High`). **Only effective when signed in** — anonymous requests have it silently ignored.

**`-task`** variants are the web "Task" mode as separate models, fully decoupled from chat mode: task mode uses its own protocol slots (`inner[49]=40`, etc.) rather than a thinking tier of the chat models. Task mode is an account feature and isn't exposed without a cookie.

**Removed legacy names**: `gemini-3.5-flash` / `gemini-3.5-flash-thinking` / `gemini-3.5-flash-thinking-lite` / `gemini-auto` / `gemini-flash-lite` (they return 400) — the backend has no entries for them.

> **`@think=N` is deprecated.** The suffix was written into `inner[17]` and long treated as "thinking depth", but captures show it is the turn index within a conversation — nothing to do with reasoning depth, so it never did anything. Still accepted and ignored; existing client configs don't break.

#### Reasoning chain (`reasoning_content`)

`gemini-3.1-pro` emits its own reasoning before every answer, exposed as the de-facto `reasoning_content` field so clients like newapi and Cherry Studio render a collapsible "thinking" panel. Streaming sends it as `delta.reasoning_content`, and the whole reasoning chain streams before any answer text. `reasoning_tokens` is counted separately and is **not** included in `completion_tokens` (reasoning is collapsed by default — billing it would charge users for output they never see).

#### Known capability boundaries

Anonymous calls reach only the two text models plus web search. A cookie additionally unlocks 3.1 Pro, the `-thinking` variants, image/video input, a longer context (over-long conversations are sent as a text attachment), and image / music / canvas / video generation. Deep research is still **not implemented** (a multi-step async flow). The panel's "actual model" column always shows which model the backend really used, so any downgrade is visible.

#### Image & music

`gemini-image` (Nano Banana) and `gemini-music` (Lyria, ~30s) go through `/v1/chat/completions` like a normal chat; the bytes come back as base64 data URLs inside `content` (`![image](data:image/png;base64,…)` for images, `[audio](data:audio/mpeg;base64,…)` for audio). Not returning a URL is deliberate — Gemini's artifact links need the cookie to download, so the server fetches the bytes and inlines them. Artifact base64 is not counted in `completion_tokens`. Params like `size` / `n` have no upstream knob and are ignored.

Turn on **Full-size image download** (`image_upscale`) in Settings to fetch images the way the web app's Download button does: Ultra accounts get a 2× upscale in each dimension (1376×768 → 2752×1536), other accounts get the original (not recompressed) file.

#### The two context-length walls

By default `messages` are flattened into a single prompt, capped at roughly **130,000 UTF-8 bytes** per request — past that, upstream silently truncates from the tail. With a cookie, an over-long conversation is uploaded as a `message.txt` attachment, but attachments have their own wall of about **160,000 bytes** of total visible content. With `multi_turn` on, a recognised continuation sends only the newest message while history stays server-side; a genuinely long conversation still has to be compacted by the client. Without a cookie, oversize requests return 400 `context_length_exceeded` instead of silently losing data.

</details>

<details>
<summary><strong>🔌 Protocol matrix & client setup</strong> (OpenAI / Gemini Native / Videos / MCP)</summary>

### OpenAI protocol coverage

| Endpoint | Status | Notes |
|---|---|---|
| `POST /v1/chat/completions` | ✅ | Real streaming: every upstream frame is forwarded as a delta. Chunk sequence is `delta{role}` → `delta{content}`×N → empty `delta` + `finish_reason` → `[DONE]`. With `tools`, text still streams; only the tool_call block is held until complete |
| `POST /v1/responses` | ✅ | OpenAI Responses API (used by Codex CLI). Real streaming; tool calls as above |
| `GET /v1/models` | ✅ | Only the 2 text models anonymously; the login-only ones appear once the pool has a cookie |
| `GET /` | ✅ | Health check, unauthenticated, returns status/version/models |
| `/v1beta/models/…` | ✅ | Google native format, see below |
| `/v1/embeddings`, `/v1/images/*`, `/v1/audio/*` | ❌ | Not implemented, return 404 |
| Function calling | ⚠️ | Prompt-level (the model emits a ` ```tool_call``` ` block parsed as JSON). Reliable for private-data lookups; things Gemini can answer itself get answered directly. Agentic clients (Codex, etc.) now work |
| `tool_choice` | ⚠️ | `none` injects no tool definitions; `required` adds mandatory wording. A prompt-level layer cannot truly force it |
| `stream_options.include_usage` | ✅ | Emits a usage chunk after `finish_reason` |
| `usage` token counts | ✅ | tiktoken cl100k_base |
| `n` > 1 | ❌ | Returns 400 — upstream yields a single candidate |
| Sampling params | ➖ | `temperature` etc. are accepted and ignored; the web protocol has no such knobs |
| `response_format` / `logprobs` | ➖ | Not implemented, accepted and ignored |
| Vision / image input | ⚠️ | Works with a cookie: `image_url` / `input_image`, up to 12MB per image. Anonymous returns 400 |
| Audio | ❌ | `input_audio` returns 400 |

### Gemini native API (`/v1beta`)

| Path | Notes |
|---|---|
| `POST /v1beta/models/{model}:generateContent` | Non-streaming |
| `POST /v1beta/models/{model}:streamGenerateContent` | Real streaming; SSE with `?alt=sse`, a JSON array without it |
| `POST /v1beta/models/{model}:countTokens` | Local tiktoken estimate |
| `GET /v1beta/models`, `GET /v1beta/models/{model}` | Model list, same as `/v1/models` |
| `POST /v1/models/{model}:generateContent` etc. | The `/v1` form of the three actions above, handled the same way |

Auth via `x-goog-api-key` header or `?key=` query param; `Authorization: Bearer` works too. Model names are this project's (`gemini-3.6-flash`, …), not the official `gemini-2.5-flash`. Supports `tools[].functionDeclarations`, `thinkingConfig.includeThoughts` (thought summaries come back as `thought: true` parts), and `inlineData` / `fileData` image/video input (needs a cookie). `generationConfig` sampling settings are accepted and ignored; errors use Google's shape `{"error":{"code","message","status"}}`.

```python
from google import genai
from google.genai import types

client = genai.Client(api_key="<your-key>",
                      http_options=types.HttpOptions(base_url="http://localhost:8083"))
print(client.models.generate_content(model="gemini-3.6-flash", contents="Hello!").text)
```

### `/v1/videos` async video generation

OpenAI (Sora)-shaped async video: `POST` to create, `GET /v1/videos/{id}` to poll, `GET /v1/videos/{id}/content` to download the MP4 (needs a signed-in paid account). Supports `input_reference` image→video (data/http(s) URL or a multipart file), `size` (`1280x720`/`720x1280`) or `aspect_ratio`, and `POST /v1/videos/{id}/extend` to continue a finished video (requires `temporary_chat` and `auto_delete_conversation` both off). Failures say why at once: `video_quota_exhausted` / `video_refused`. An account out of video quota is skipped automatically; when the whole pool cools down, `POST` returns 429 with `Retry-After`. `GET /admin/api/video-quota` reports each account's rolling 5-hour video usage (each refresh costs ~2+N upstream requests per account — don't poll it).

### MCP tools (`/mcp`)

The same process and port serves an MCP server (Streamable HTTP) exposing web search, image, music and video as tools for Claude Desktop / Claude Code / Cursor. Search works anonymously; media needs a cookie (video needs a paid account).

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

Claude Code: `claude mcp add --transport http gemini-search http://localhost:8083/mcp --header "Authorization: Bearer sk-<your-key>"`.

| Tool | Arguments | Returns |
|---|---|---|
| `web_search` | `query` | Gemini's web-grounded answer with a `Sources:` list appended |
| `generate_image` | `prompt`, optional `image_urls` | the image (MCP `image` content) |
| `generate_music` | `prompt` | a ~30 s clip (MCP `audio` content) |
| `create_video_task` | `prompt`, optional `size`, `input_reference` | a task id — the same jobs as `/v1/videos` |
| `get_video_status` | `id`, optional `include_video` | the task status; on a finished task with `include_video=true`, the MP4 attached |

### Connecting clients

Every OpenAI-compatible client (Cherry Studio / ChatBox / Open WebUI / Dify / Cursor / …) is configured the same way:

| Field | Value |
|---|---|
| Base URL | `http://localhost:8083/v1` (some clients want just `http://localhost:8083` and append `/v1` themselves) |
| API key | the `sk-<your-key>` shown on the panel's Settings page |
| Model | `gemini-3.6-flash` |

**newapi / one-api channel**: pick the OpenAI type, base URL `http://localhost:8083` (`http://host.docker.internal:8083` or the container name if newapi runs in Docker too), paste the API key, list models as `gemini-3.6-flash,gemini-3.5-flash-lite`. **Codex CLI** uses `/v1/responses` — point its base URL at `http://localhost:8083/v1`. **Google-format clients** use `/v1beta`; Gemini CLI has not been tested. There's also an unauthenticated health check at `GET /`.

On Windows PowerShell `curl` is an alias for `Invoke-WebRequest`, so use `curl.exe` with `--%`:

```powershell
curl.exe --% http://127.0.0.1:8083/v1/chat/completions -H "Content-Type: application/json" -H "Authorization: Bearer sk-<your-key>" -d "{\"model\":\"gemini-3.6-flash\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello!\"}]}"
```

</details>

<details>
<summary><strong>⚙️ Configuration & CLI flag reference</strong></summary>

There are only two places to configure things, split by whether a change needs a restart.

### Runtime config → admin panel, Settings page

Saving takes effect **immediately**, no restart. Values live in the database and take precedence over `config.json` and flags.

| Item | Notes |
|---|---|
| Default model | used when the client doesn't send `model` |
| Per-slot concurrency / RPM / RPH | rate limits, 0 = unlimited |
| Prompt byte cap (`max_prompt_bytes`) | over the cap: with a cookie the history goes as a text attachment (~160,000 bytes), without one the request is rejected with 400 `context_length_exceeded`. Neither path truncates silently. UTF-8 bytes, default 128000. 0 = unlimited |
| Multi-turn (`multi_turn`) | off by default. When on, server-side continuation: a recognised follow-up sends only the newest message while history stays server-side. Works signed-in or anonymous. Does not enlarge the model's context window |
| Auto-delete conversation (`auto_delete_conversation`) | **on by default** (the initial value is overridden by any panel config already saved in the database). Each finished request deletes the conversation it left on the web app — signed-in only, async best-effort. Turn it off if you want to keep history or extend videos |
| Temporary chat (`temporary_chat`) | off by default. When on, the conversation never enters the account's chat list and isn't used for training — cleaner than auto-delete. Mutually exclusive with `multi_turn` |
| Full-size image download (`image_upscale`) | off by default. When on, images are fetched like the web app's Download button: 2× upscale on Ultra accounts, the original file elsewhere. One extra request per image |
| Anonymous-first (`anon_first`) | off by default. When on, requests that don't need sign-in go out anonymously, saving account quota |
| Strict model check (`strict_model`) | off by default. When on, a silently downgraded answer doesn't count as success: retry on the next account; if the whole pool is downgraded, `429 insufficient_quota` (`code: model_downgraded`, with `Retry-After`). Streaming decides before the first token. Media models are not checked |
| Downgrade cooldown (`strict_model_cooldown_min`) | how long a downgraded account rests for that model, default 300 minutes. 0 = do not remember |
| Retry attempts / delay / upstream timeout (`retry_attempts` / `retry_delay_sec` / `request_timeout_sec`) | retry policy for upstream failures: `retry_attempts` is the total attempt count (1-10), `retry_delay_sec` the delay between attempts, and `request_timeout_sec` the upstream *silence* threshold (5-600 s) — any byte or heartbeat frame resets the timer, so it is not a hard deadline for the whole request |
| Retry after timeout (`retry_on_timeout`) | on by default. When off, an upstream silence timeout fails right away instead of being resent (a resend usually stalls again). Other failures are still retried |
| Stream keepalive (`stream_keepalive_sec`) | 0 (off) by default. When a stream writes nothing for this many seconds, send a keepalive so an nginx / Cloudflare / newapi layer doesn't answer 504. 15 is a good value. Trade-off: a keepalive opens the stream (HTTP 200), so a later failure can only be reported inside the stream. No effect on non-streaming requests |
| Detail retention days (`retention_days`) | only request details expire; aggregates are kept forever |
| TLS fingerprint (`impersonate`) | `chrome_146` (default) / `chrome_144` / `chrome_133` / `firefox_147` / `safari_16_0` / `safari_ios_17_0` |
| Firefox fingerprint (`firefox_fingerprint`) | experimental, off by default. When on, requests pose as Firefox: on direct connections the TLS handshake and UA become Firefox, Chrome-only `Sec-CH-UA` is dropped. Recommended with cookies exported from Firefox. Through a proxy the TLS layer can't change — only the headers do |
| Gemini `bl` version (`gemini_bl`) | upstream frontend build id; change it here when it expires |
| Follow `bl` automatically (`gemini_bl_auto`) | fetches the latest build id from the `/app` page every 6 hours; falls back to the pinned value above |
| Breaker cooldown (`proxy_cooldown_min`) | how long a proxy rests after 5 consecutive failures, default 120 minutes (blocked exits were measured to recover in 106-121 min). 0 = never auto-recover |
| Fall back to direct when no proxy is available (`fallback_direct`) | off by default: when every proxy is full or tripped, return an error so the host IP is never exposed. On: availability first, but upstream sees the host's real exit |
| Fall back to anonymous when the cookie is dead (`fallback_anon`) | off by default: return an error so you notice right away. On: keep going, but Pro is silently downgraded and the reasoning chain disappears |
| New protocol (`web_channel`) | **Locked off in this version**: `gemini.go` hardcodes `useWC := false`, so enabling this switch **does not change actual behaviour** — it only logs that the switch was ignored. The field is kept for panel compatibility; see `.agents/notes/webchannel-enable-prerequisites.md` for the prerequisites to enable it |
| Browser transport (`browser_transport`) | off by default. When on, chat requests go through a resident real browser (Chromium CDP + in-page streaming fetch): TLS/JS fingerprint identical to a real browser, cookies live in the browser jar. ~1.45 GB RAM resident — enable when you need a real browser fingerprint. Policy M1 single sticky account |
| Fall back to HTTP when the browser is unavailable (`browser_fallback_http`) | off by default: with `browser_transport` on, if no browser can be found or launched, requests return 502 `browser_unavailable` — never silently degrade. Set `true` to fall back to plain Go HTTP |
| Request logging (`log_requests`) | on by default. When off, per-request log lines to stderr stop |

Every value is range-checked on the server (`retry_attempts` accepts 1-10, timeouts 5-600 s) and illegal values are rejected with a reason.

### Credentials → also in the panel

| Item | Notes |
|---|---|
| API key | generated on first boot, rotatable or replaceable from the panel |
| Google cookie | import on the Cookie pool page, effective on save; several accounts can be imported. Once the pool has an account, the login-only models appear in the model list |

Saved values are never echoed back (a cookie only reports how many entries were recognised and whether the key ones are present).

### Deploy-time config → `docker-compose.yml`

| Item | Where |
|---|---|
| Listening port | `ports` + `command: --port` |
| Database path | `volumes` + `command: --db` |
| `ADMIN_TOKEN` | `environment`, the panel login token |

The `API_KEY` env var pins the API key (the panel can't change it). `--proxy` and `--cookie-file` are **seed parameters**: imported at startup, managed from the panel afterwards. Precedence: **panel changes > CLI flags / `config.json` > built-in defaults**. Without Docker, copy `config.example.json` to `config.json` as a template (looked up at `./config.json`, then `$HOME/.config/gemini-web2api/config.json`).

### All flags

| Flag | Notes |
|---|---|
| `--port` | listening port, default 8083 |
| `--config` | path to `config.json` |
| `--db` | SQLite path, default `./data/gemini.db` |
| `--admin-token` | panel login token; empty = no auth (only acceptable when bound to 127.0.0.1) |
| `--api-key` | pins the `/v1/*` key so the panel can't change it, same as `API_KEY` |
| `--cookie-file` | imports the cookie in this file into the cookie pool at startup |
| `--proxy` | imports this proxy into the proxy pool at startup |
| `--impersonate` | TLS fingerprint profile |
| `--browser-path` | explicit Chromium/Chrome/Edge executable (deploy-only, same as `BROWSER_PATH`) |
| `--browser-profile-dir` | explicit browser profile dir (deploy-only, same as `BROWSER_PROFILE_DIR`, default `<db dir>/browser-profile`) |
| `--version` | print version and exit (this is what the Docker healthcheck runs) |

> `browser_path` / `browser_profile_dir` are **deploy-level** settings and deliberately stay out of the admin panel: `/admin` is unauthenticated by default, so a panel-writable browser executable path would be local arbitrary command execution.

### MySQL / PostgreSQL

Set the `SQL_DSN` env var (unset = SQLite):

```bash
# MySQL (also accepts the native go-sql-driver form user:pass@tcp(host:3306)/dbname)
SQL_DSN="mysql://user:pass@host:3306/dbname"
# PostgreSQL
SQL_DSN="postgres://user:pass@host:5432/dbname?sslmode=disable"
```

Tables are created automatically; all three backends share one schema. SQLite is the easy choice for a single instance; MySQL / PostgreSQL only matter when several instances share one pool.

</details>

<details>
<summary><strong>🛡️ Anti-blocking internals, troubleshooting & FAQ</strong></summary>

### Why you need the proxy pool

A single IP sending in bursts gets redirected to `google.com/sorry/index` (the CAPTCHA page) after a measured **80-180 requests**. The range is driven by connection strategy, exit quality and pacing together: connection reuse is worth about 60% (at concurrency 10, a reused pool reached 172/177 vs 106/109 for fresh connections); **a steady pace matters most** — the same static IP was blocked at 188 in a burst run, yet a steady 10 requests/minute ran **800 requests over 110 minutes without a block**. The default `per_ip_rph=80` is a very conservative floor.

Once blocked, the block is hard: 60 probes, zero successes, recovering after roughly **106-121 minutes** (the proxy pool's default 120-minute cooldown comes from this). Proxy failures consume the budget early (some "failed" requests did reach Google and were counted).

**The fix**: add several proxies on the Proxy pool page — each proxy is an independent IP slot with its own concurrency/RPM/RPH allowance, so N proxies = N× capacity. Supported schemes: `http://` / `https://` / `socks5://` / `socks5h://`. With any proxy configured, requests never fall back to direct; 5 consecutive failures trip the circuit breaker; all full → 429, all tripped → 502 `proxy_unavailable`. **`HTTPS_PROXY` / `ALL_PROXY` env vars are ignored** — proxies come only from the pool.

### Fingerprint simulation

Direct connections go through [bogdanfinn/tls-client](https://github.com/bogdanfinn/tls-client): TLS handshake + HTTP/2 SETTINGS + ALPS all match a real Chrome 146, indistinguishable to Google's risk control. Proxied connections switch to stdlib `net/http` (best compatibility), with headers still spoofed as Chrome 146 — but **through a proxy the TLS fingerprint is Go's stdlib, not Chrome's** (HTTP CONNECT only builds a tunnel; TLS is end-to-end). This does not change the blocking threshold (measured gap: 8 requests; variance between exits: 36%). Observation from testing: a naive SDK call that trips risk control gets 429; disguised as Chrome it gets a 302 to the sorry page — proof Google really takes us for a browser.

### Cookie pool & keepalive

With a cookie attached, requests run as a signed-in session. The Cookie pool page is the only entry point, rotating least-recently-used first; 3 consecutive auth failures disable an account automatically (network errors and `/sorry` blocks don't count — otherwise the failure count becomes proxy noise). **Cookies renew themselves**: refreshed `SIDCC`/`__Secure-1PSIDCC`/`__Secure-3PSIDCC` from upstream `Set-Cookie` are merged back; every 10 minutes we hit `accounts.google.com/RotateCookies` to re-mint `__Secure-1PSIDTS` (the ~30-minute ticket).

> **Export cookies from Firefox.** Recent Chrome enables Device Bound Session Credentials, so an exported session is bound to the machine and `*PSIDTS` cannot be rotated — it dies within 30 minutes to a few hours. Firefox has no such binding. If you must use Chrome: log in in a fresh private window, export immediately, and **close that window**.

**Each account is pinned to its own exit** (otherwise one account emitting from dozens of IPs is exactly what account sharing looks like). With several Google accounts signed in to one browser, set the "account index" (`/u/1/` in the address bar means 1) — unset, every request lands on the default account, and if your subscription is on another, paid models get silently downgraded.

To import: DevTools → Application → Cookies, copy `SID` / `HSID` / `SSID` / `APISID` / `SAPISID` / `__Secure-1PSID` / `__Secure-1PSIDTS`, paste into Cookie pool → Add account, or write `cookie.txt` and seed with `--cookie-file cookie.txt`. Requests carrying `SAPISID` get a computed `SAPISIDHASH`, so that entry cannot be missing.

### Privacy

- **Prompt and response bodies are never stored** — only metadata: model, proxy id, latency, token counts, status code, error type
- Legacy `prompt_preview` / `response_preview` columns are dropped automatically when migrating from older versions
- Token counts use the tiktoken cl100k_base BPE tokenizer (the web app returns no real token counts; local estimation is within ±20%)
- **The update check runs in your browser**, not on the server — a deployment with a proxy pool never reaches out from the host IP just to check versions

### Troubleshooting

| Symptom | Most likely cause | What to do |
|---|---|---|
| We return **429** | Every IP slot is at its concurrency/RPM/RPH limit — **not Google refusing** | Add proxies, or raise the limits |
| Panel diagnostics show **302 → `google.com/sorry/index`** | This exit IP is blocked by Google (after 80-180 requests) | Change exit / add proxies. The block is hard — retrying in place is pointless |
| Occasional empty responses | Upstream transient refusal (`1155`), no predictable threshold | Resend once and it usually works; lowering RPM does not help |
| **502 `reason: empty_content`** | Upstream returned frames but no usable text (including reasoning-only replies — the trace isn't an answer, so we return 502 instead of a fake 200) | Retry once; if it repeats, the wire format changed — file an issue |
| Every request times out | This host can't reach `gemini.google.com` | Configure a proxy. Note `HTTPS_PROXY` is not read |
| Exits at startup with `unable to open database file (14)` | Container runs as nonroot (uid 65532) but the bind-mounted host dir is owned by root | Use a named volume, or `sudo chown -R 65532:65532 ./data` |
| `gemini-3.1-pro` errors out immediately | Not exposed without a cookie, by design | Add an account on the Cookie pool page |
| Every request returns 502 after attaching a cookie | The cookie expired, so the XSRF token can't be fetched | Re-export it. Check: if `gemini-3.1-pro` reports 3.5 Flash-Lite, it's expired |
| Cookie dies in about 30 minutes | Chrome device-bound session; `__Secure-1PSIDTS` rotation returns 401 | Re-login in **Firefox** and export again |
| Paid account cookie, but paid models report 3.5 Flash-Lite | Several Google accounts in the browser; the subscription isn't on the default one | Set the "account index" for that entry in the Cookie pool |
| Panel won't open / 401 | `--admin-token` (or `ADMIN_TOKEN`) doesn't match | An empty token disables auth — only acceptable when bound to 127.0.0.1 |

Upstream failures return 502 with a structured diagnosis: `reason` (`no_content_frame` / `failure_reply` / `upstream_timeout` / `empty_content` / `upstream_http` / `upstream_error`) + `upstream_model` (upstream self-reported name; `unknown` = unparseable) + `frames` + `content_frame` + `hint`. The same `[reason]` prefix lands in `requests.error` for panel filtering.

With Docker's default bridge network, upstream may return empty content (Google rejects certain NAT ranges — never reproduced here); if you hit it, try `network_mode: host` to confirm.

### FAQ

**How is this different from the official Gemini API? Do I need an API key?**
No API key. This project proxies the gemini.google.com web-app protocol directly, so there is no key and no paid quota. The trade-off: capabilities follow the web app — what it can do (web search, image, music, canvas, video) works here; what it can't (deep research) doesn't.

**Will my account get banned?**
Limits are per **exit IP + TLS fingerprint**, not per account. A burst of 80-180 requests triggers the sorry page, clearing in about two hours; a steady pace barely reaches the ceiling. A paid plan does not raise it. Scale by spreading requests across exits with the proxy pool, not by adding accounts.

**Free vs paid account — what's the difference?**
Anonymous: 3.6 Flash / 3.5 Flash-Lite + web search. A free account's cookie adds 3.1 Pro, the `-thinking` variants, image/video input, image, music and canvas. Paid only: 3.8 Flash / 4 Carbon / 4 Neon (including their thinking tiers and task modes) and video generation. 4 Hydrogen and its `-thinking` / `-task` variants work on any signed-in account — no paid plan needed.

**Why does every message create a new conversation on gemini.google.com?**
Each request is its own conversation by default. Enable `multi_turn` and continuations reuse the same web session; to stop conversations piling up, use `temporary_chat` (never enters the list, not used for training — recommended) or `auto_delete_conversation`.

**Can I use it as a backend for Cursor / Dify-style agents?**
Yes (it's OpenAI-compatible), and plain chat and coding work fine; but the web app caps a single request at ~130K bytes (~160K with a cookie), so agents pushing tens of kilobytes of system prompt get off-topic answers — a limit of the web model itself.

**Can image generation output 2K?**
`gemini-image` uses the web app's image tool slot rather than a pinned model. Default output is about one megapixel (1376×768); on an Ultra account, `image_upscale` gets the 2× upscale to 2752×1536 (~2K), other accounts get the original un-recompressed file.

**Why does streaming collapse into one response behind a reverse proxy?**
This project streams for real (responses already carry `X-Accel-Buffering: no`); buffering behind Nginx or similar is the proxy's own setting — connected directly, output arrives chunk by chunk.

</details>

<details>
<summary><strong>🔧 Source layout, local development & release notes</strong></summary>

### Project layout

```
main.go                    just calls app.Run(); the entry stays at the root so `go build .` works
internal/app/              everything else
  app.go                   flag parsing + route registration + startup
  config.go                config loading + defaults
  client.go                tls-client (chrome146) + stdlib (when proxied)
  gemini.go                model table + 80-slot payload + model header + StreamGenerate + wrb.fr parsing
  webchannel.go            new-protocol transport, processSession (WebChannel); switch web_channel (locked off in this version)
  conversation.go          server-side multi-turn (multi_turn)
  media.go                 image / music / canvas / video: tool slot, artifact extraction and download
  image_upscale.go         full-size / upscaled image download (rpc c8o8Fe, switch image_upscale)
  search.go                web-search source extraction
  strict_model.go          strict model check: detects silent downgrades, cools down per (account, model)
  xsrf.go                  XSRF token required with a cookie: fetch + per-cookie cache + self-heal
  messages.go              OpenAI messages → prompt, tool_call parsing
  server.go                /v1/* + rate-limit entry + request validation + metrics
  gemini_api.go            native Gemini API /v1beta/models/…
  videos.go                /v1/videos async video jobs
  video_cooldown.go        cooldown for accounts out of video quota + switching accounts within a job
  video_quota.go           /admin/api/video-quota: per-account video usage read from history
  mcp.go                   MCP server (/mcp)
  mcp_media.go             MCP media tools: image / music / video
  sse.go                   SSE writer (lazy header, so early failures still return 502 JSON)
  ratelimit.go             per-IP-slot concurrency / RPM / RPH
  tokenizer.go             tiktoken cl100k_base singleton
  apikey.go                API key (locked by flag / rotatable from the panel)
  db.go                    database schema (shared by SQLite / MySQL / PostgreSQL)
  dbdialect.go             multi-database dialects: SQL_DSN selects MySQL / PostgreSQL
  proxy.go                 proxy pool CRUD + capacity scheduling + circuit breaker
  cookie_pool.go           cookie pool data layer
  rotate.go                session keepalive (RotateCookies)
  upload.go                attachment upload (content-push, two-step resumable)
  context_file.go          over-long conversations as a text attachment
  vision.go                image input
  bl.go                    upstream frontend version (bl) auto-follow
  scheduler.go             hourly/daily aggregation + retention cleanup
  runtime.go               runtime config snapshot (panel edits apply instantly)
  admin.go                 /admin/api/* auth + REST
  admin_cookies.go         cookie pool admin REST (redacted before returning)
  admin_ui/                admin panel frontend (single page + Chart.js, embedded, no CDN)
Dockerfile                 multi-stage (alpine builder → distroless runtime)
docker-compose.yml         single container, pulls the ghcr image by default, sqlite on a volume
.github/workflows/         docker.yml pushes images / release.yml publishes binaries on tag
```

### Local builds & version injection

A plain `go build` prints `dev` for `--version`: the version isn't a hardcoded constant — `internal/app` has `var Version = "dev"` and the release pipeline injects it at build time (`-ldflags "-X gemini-web2api/internal/app.Version=<version>"` in `release.yml`; Docker images take `--build-arg VERSION=<version>`, where `<version>` is the version without the `v` prefix). To stamp a local build:

```bash
go build -ldflags "-X gemini-web2api/internal/app.Version=my-build" -o gemini-web2api-go .
```

Changed the code and want it in a container? Uncomment the two `build:` lines in `docker-compose.yml`, then `docker compose up -d --build`.

### Versioning and release conventions

- Release tags use strict SemVer `vX.Y.Z` (annotated git tag); the `-magpie.N` suffix is retired.
- `internal/app/server.go` keeps `var Version = "dev"` as an injectable variable; builds inject the version (without the `v` prefix) via `-ldflags -X`.
- Before every release, the corresponding `## X.Y.Z` section must be filled in `CHANGELOG.md`, and `make release-check TAG=vX.Y.Z` must pass.
- This repo is the single authoritative source for 6-platform prebuilt binaries and GHCR multi-arch images; the downstream plugin `@dorokuma/magpie-gemini-web` (since v0.15.0) pins the official release binary SHA-256 via `lib/backend-release.json`.

</details>

---

## Star History

If you read this far, it probably solves something for you. A ⭐ helps others find it; open an issue when something breaks, and send a PR when you have fixed it.

<a href="https://star-history.com/#dorokuma/gemini-web2api-go&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=dorokuma/gemini-web2api-go&type=Date&theme=dark">
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=dorokuma/gemini-web2api-go&type=Date">
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=dorokuma/gemini-web2api-go&type=Date">
  </picture>
</a>

## Acknowledgments

- [bogdanfinn/tls-client](https://github.com/bogdanfinn/tls-client) — real Chrome TLS fingerprints
- [pkoukk/tiktoken-go](https://github.com/pkoukk/tiktoken-go) — BPE tokenizer
- [modernc.org/sqlite](https://gitlab.com/cznic/sqlite) — pure-Go SQLite (CGO-free, builds straight on alpine)
- [Sophomoresty/gemini-web2api](https://github.com/Sophomoresty/gemini-web2api) — a Python implementation of the same idea; its approach informed our early work on the Gemini web protocol
- [LINUX DO](https://linux.do) — the community where this project is shared

## License

MIT — see [LICENSE](LICENSE)
