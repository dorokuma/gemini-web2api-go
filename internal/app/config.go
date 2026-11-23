package app

import (
	"encoding/json"
	"os"
	"sync"
)

type Config struct {
	Port           int    `json:"port"`
	Host           string `json:"host"`
	RetryAttempts  int    `json:"retry_attempts"`
	RetryDelaySec  int    `json:"retry_delay_sec"`
	RequestTimeout int    `json:"request_timeout_sec"`
	RetryOnTimeout bool   `json:"retry_on_timeout"`
	GeminiBL       string `json:"gemini_bl"`
	DefaultModel   string `json:"default_model"`
	LogRequests    bool   `json:"log_requests"`
	CookieFile     string `json:"cookie_file"`
	Proxy          string `json:"proxy"` // 只用于启动时播种代理池，见 seedProxiesFromConfig
	Impersonate    string `json:"impersonate"`
	DBPath         string `json:"db_path"`
	AdminToken     string `json:"admin_token"`
	AdminEnabled   bool   `json:"admin_enabled"`
	RetentionDays  int    `json:"retention_days"`

	// Per-IP rate limit (一个 slot = 直连/或一个代理),0 表示不限。
	// 默认值取实测区间 80-180 的下沿，详见 ratelimit.go 的说明。
	PerIPConcurrent int `json:"per_ip_concurrent"` // 瞬时并发上限
	PerIPRPM        int `json:"per_ip_rpm"`        // 每分钟请求上限
	PerIPRPH        int `json:"per_ip_rph"`        // 每小时请求上限

	// 代理连续失败熔断后隔多久放回池子（分钟），0 = 不恢复。
	ProxyCooldownMin int `json:"proxy_cooldown_min"`
	// 代理池无可用出口时是否退回直连。默认 false。
	FallbackDirect bool `json:"fallback_direct"`
	// 账号绑定的代理不可用时是否只换号、不换出口。默认 false。
	StrictProxyBinding bool `json:"strict_proxy_binding"`
	// cookie 失效时是否降级成匿名继续跑。默认 false。
	FallbackAnon bool `json:"fallback_anon"`
	// 是否自动从 /app 页面抓最新 bl 版本号。默认 true。
	GeminiBLAuto bool `json:"gemini_bl_auto"`
	// 单次请求 prompt 的 UTF-8 字节上限，0 = 不限。
	MaxPromptBytes int `json:"max_prompt_bytes"`
	// 是否走 Gemini 原生 conversation_id 服务端多轮。默认 false，见 conversation.go。
	MultiTurn bool `json:"multi_turn"`
	// 出完结果是否自动删掉 gemini.google.com 上留下的这条会话（#19，rpc GzXR5e）。
	// 只登录态生效（删除要 XSRF）；异步 best-effort，删失败只记日志不影响响应。默认 true：
	// temporary_chat(inner[45]=1) 在带附件/Mode-8 时仍会被服务端落「打不开」的索引残片
	// （#phantom），光靠它不够，得 create-then-delete 双保险；配合 streamGenerateOnce 的
	// phantomCleanup（取消/超时/出错也删）覆盖全路径。
	AutoDeleteConversation bool `json:"auto_delete_conversation"`
	// 匿名优先（#20）：这次请求不需要登录态能力（纯文本、非思考、无工具、无图）时
	// 不占用 cookie 账号，走匿名省额度；需要登录才挑号。默认 false。见 modelNeedsLogin。
	AnonFirst bool `json:"anon_first"`
	// 临时对话（#34）：发请求时塞 inner[45]=1，走 Gemini 网页端原生「临时对话」——
	// 会话不进账号的会话列表、不用于训练，天然取代 auto_delete_conversation（无需再发删除
	// RPC）。只登录态有意义（匿名本就不落账号）。与 multi_turn 冲突：multi_turn 开着时不发，
	// 见 temporaryChatEligible。默认 false。
	TemporaryChat bool `json:"temporary_chat"`
	// 生成图放大（#62）：出图后照网页「下载原图」那样发 rpc c8o8Fe，返回放大版（实测宽高各
	// 2 倍、JPEG）。要登录态；放大失败退回原来那张，不重新生成。默认 false。
	ImageUpscale bool `json:"image_upscale"`
	// 新协议（#43）：对话改走 processSession（WebChannel）而不是老的 StreamGenerate。后端是同一个，
	// 只换传输层；老端点还能用，默认 false。多轮续接仍走老端点，见 webchannel.go。
	WebChannel bool `json:"web_channel"`
	// FirefoxFingerprint 是「浏览器指纹」实验开关（#40 之上）：开 → 强制走 Firefox/Zen 指纹
	// （TLS profile Firefox_147 + Firefox UA + 去掉 Chromium 专有的 Sec-CH-UA）；关 → 用
	// Impersonate（默认 Chrome）。直连才是完整 Firefox 指纹，走代理时 TLS 这层仍是 Go 标准库
	// 的（隧道限制，改不了）。默认 false。见 client.go 的 isFirefoxProfile / effectiveImpersonate。
	FirefoxFingerprint bool `json:"firefox_fingerprint"`
	// 严格模型校验（#41）：服务端自报的模型跟请求的不一致（额度用完被静默降级）时
	// 不把降级回答当成功，换号重试，全池降级就回 429。默认 false。见 strict_model.go。
	StrictModel bool `json:"strict_model"`
	// 被降级的号在该模型上冷却多久（分钟），0 = 不记。网页端用量按 5 小时滚动窗算，默认 300。
	StrictModelCooldownMin int `json:"strict_model_cooldown_min"`
	// 流式保活间隔（秒），0 = 关（默认）。见 keepalive.go。
	StreamKeepaliveSec int `json:"stream_keepalive_sec"`
	// 开流吐字后块间空闲超时（秒，借鉴上游 #81）：首个有效内容帧到达后，连续
	// 这么多秒没有新 chunk 就掐断。仅当 0 < stream_idle_timeout_sec 且
	//（< request_timeout_sec 或 request_timeout_sec == 0）时生效，0 = 关（默认，
	// 全程沿用 request_timeout_sec 的沉默窗口）。掐断后仅在尚未向客户端发出
	// 正文/思考增量时才自动重试；已发过增量的请求在流内快速失败报错（已发的
	// 增量收不回）。开流前（排队心跳期）不生效，见 gemini.go 的
	// stallWatch.markStreaming / streamIdleLimit。
	StreamIdleTimeoutSec int `json:"stream_idle_timeout_sec"`

	// BrowserTransport 启用常驻真实浏览器传输层（Chromium CDP + 页内流式 fetch），
	// 见 browser.go。默认 false：不主动拉起 Chromium（常驻约 1.45 GB 内存），
	// 需要浏览器指纹/页内上下文时才开。
	BrowserTransport bool `json:"browser_transport"`
	// BrowserFallbackHTTP 决定 browser_transport 开着但本机找不到/拉不起浏览器时
	// 怎么办。默认 false：绝不静默降级回纯 Go HTTP（浏览器指纹是开这个功能的
	// 目的，悄悄换回 Go 指纹等于功能名存实亡），直接回 502 browser_unavailable；
	// 显式开 true 才回退原有纯 Go HTTP 路径。
	BrowserFallbackHTTP bool `json:"browser_fallback_http"`
	// BrowserPath 显式指定 Chromium/Chrome/Edge 可执行文件路径，空 = 自动探测。
	// 部署期只读（config.json / --browser-path / BROWSER_PATH），不进 RuntimeConfig：
	// /admin 在 127.0.0.1 默认免鉴权，面板可写它等于本地任意可执行文件执行。
	BrowserPath string `json:"browser_path"`
	// BrowserProfileDir 显式指定浏览器 profile 目录，空 = <db 目录>/browser-profile。
	// 同 BrowserPath，部署期只读。
	BrowserProfileDir string `json:"browser_profile_dir"`
}

var (
	cfg     Config
	cfgOnce sync.Once
)

func defaultConfig() Config {
	return Config{
		Port:           8083,
		Host:           "0.0.0.0",
		RetryAttempts:  3,
		RetryDelaySec:  2,
		RequestTimeout: 180,
		RetryOnTimeout: true,
		GeminiBL:       "boq_gemini-web-uiserver_20261007.01_p0",
		DefaultModel:   "gemini-3.6-flash",
		LogRequests:    true,
		CookieFile:     "",
		Proxy:          "",
		Impersonate:    "chrome_146",
		DBPath:         "./data/gemini.db",
		AdminToken:     "",
		AdminEnabled:   true,
		RetentionDays:  30,
		// 单出口实测 80-180 次不等（连接策略和出口质量决定），静态 IP 上 188。
		// RPH 取下沿 80 保守留量；按低速率跑的部署可以调高很多 ——
		// 10 次/分钟连打 800 次、跨 110 分钟一次没被拦。
		PerIPConcurrent: 5,
		PerIPRPM:        30,
		PerIPRPH:        80,
		// 实测被拦的出口 106-121 分钟自动恢复，冷却取 120 分钟。
		ProxyCooldownMin:   120,
		FallbackDirect:     false,
		StrictProxyBinding: false,
		FallbackAnon:       false,
		GeminiBLAuto:       true,
		// 实测上游的墙在约 13 万 UTF-8 字节：129,950 字节中英文各 3/3 过，
		// 135,990 字节各 1/3，141,920 字节各 1/3。取 128000 留一点余量。
		MaxPromptBytes:         128000,
		MultiTurn:              false,
		AutoDeleteConversation: true,
		AnonFirst:              false,
		TemporaryChat:          false,
		ImageUpscale:           false,
		WebChannel:             false,
		FirefoxFingerprint:     false,
		StrictModel:            false,
		StrictModelCooldownMin: 300,
		StreamIdleTimeoutSec:   0,
		BrowserTransport:       false,
		BrowserFallbackHTTP:    false,
		BrowserPath:            "",
		BrowserProfileDir:      "",
	}
}

func loadConfig(path string) error {
	cfg = defaultConfig()
	if path == "" {
		for _, p := range []string{"./config.json", os.ExpandEnv("$HOME/.config/gemini-web2api/config.json")} {
			if _, err := os.Stat(p); err == nil {
				path = p
				break
			}
		}
	}
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &cfg)
}
