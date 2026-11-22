package app

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"golang.org/x/net/proxy"
)

var (
	clientCache   = map[string]tls_client.HttpClient{}
	clientCacheMu sync.RWMutex
)

// resolveProfile maps a config string to a tls-client ClientProfile.
// Defaults to Chrome_146 (matches Python v2's curl_cffi chrome146).
func resolveProfile(name string) profiles.ClientProfile {
	switch strings.ToLower(name) {
	case "chrome_120":
		return profiles.Chrome_120
	case "chrome_124":
		return profiles.Chrome_124
	case "chrome_131":
		return profiles.Chrome_131
	case "chrome_133":
		return profiles.Chrome_133
	case "chrome_144":
		return profiles.Chrome_144
	case "chrome_146", "chrome146":
		return profiles.Chrome_146
	case "firefox_120":
		return profiles.Firefox_120
	case "firefox_123":
		return profiles.Firefox_123
	case "firefox", "firefox_147", "firefox147", "firefox_157", "zen":
		return profiles.Firefox_147
	case "safari_16_0":
		return profiles.Safari_16_0
	case "safari_ios_17_0":
		return profiles.Safari_IOS_17_0
	default:
		return profiles.Chrome_146
	}
}

// getTLSClient returns a tls-client for direct connections only; its fingerprint follows
// effectiveImpersonate() (default Chrome 146, Firefox_147 when the firefox toggle is on).
// For proxied connections we use stdlib (see getStdlibClient) — tls-client's SOCKS5
// implementation is fragile compared to net/http.
func getTLSClient() tls_client.HttpClient {
	return tlsClientWithin(rtCfg().RequestTimeout)
}

// getTLSStreamClient 同 getTLSClient，但不限整次请求的时长，给 StreamGenerate 用：
// 还在出字的回复可以比 request_timeout_sec 长，上游沉默太久由 stallWatch 掐掉。
func getTLSStreamClient() tls_client.HttpClient {
	return tlsClientWithin(0)
}

// tlsClientWithin 是整次请求限时 timeout 秒（0 = 不限）的 tls-client。
func tlsClientWithin(timeout int) tls_client.HttpClient {
	imp := effectiveImpersonate()
	// 缓存 key 带上超时：超时是建客户端时定死的，面板改了 request_timeout_sec
	// 要换一个新客户端才生效（只按 imp 缓存时要重启进程才生效）。
	key := fmt.Sprintf("%s|%d", imp, timeout)
	clientCacheMu.RLock()
	if c, ok := clientCache[key]; ok {
		clientCacheMu.RUnlock()
		return c
	}
	clientCacheMu.RUnlock()

	clientCacheMu.Lock()
	defer clientCacheMu.Unlock()
	if c, ok := clientCache[key]; ok {
		return c
	}
	client, err := newTLSClient(imp, timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[client] tls-client init failed: %v\n", err)
		os.Exit(1)
	}
	clientCache[key] = client
	return client
}

// newTLSClient 新建一个指纹 imp、整次请求限时 timeout 秒（0 = 不限）的 tls-client，
// 拨号用 tlsDialer（见下）。测试在 extra 里加上不校验证书，别的配置跟线上同一套。
func newTLSClient(imp string, timeout int, extra ...tls_client.HttpClientOption) (tls_client.HttpClient, error) {
	opts := append([]tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(timeout),
		tls_client.WithClientProfile(resolveProfile(imp)),
		tls_client.WithNotFollowRedirects(),
		tls_client.WithProxyDialerFactory(newTLSDialer),
	}, extra...)
	return tls_client.NewHttpClient(tls_client.NewNoopLogger(), opts...)
}

// ─── tls-client 的拨号 ───────────────────────────────────────────────────────
//
// tls-client 的 HTTP/2 连接断了（空闲被关、对端 GOAWAY）之后，下一个请求要重新拨号，
// 这一步却不带请求的 ctx：tls-client 的 dialTLSHTTP2 传的是 context.Background()，fhttp
// 的连接池也只是干等拨号结束。整次请求的超时、stallWatch、客户端断开都管不到它；拨号时
// 它还占着这个客户端的锁，同一个客户端里要新建连接的请求都得等它。tls-client 自带的
// 拨号器只拿整次请求的超时去限 TCP 连接（不限时的客户端就完全不限），TLS 握手从来不限：
// 对方连上了却不回话，这个客户端就一直卡死。
//
// 所以换成自己的拨号器，两步各自限时，数值同 Go 标准库 http.DefaultTransport 的拨号、
// 握手超时：TCP 30 秒连不上算失败；连上后对方 10 秒内一个字节都没回（握手没有回音）就
// 断开。tls-client 只连 Google 的 https，对方回的第一个字节总是握手。
var (
	dialTimeout      = 30 * time.Second
	handshakeTimeout = 10 * time.Second
)

// newTLSDialer 当 tls-client 的 ProxyDialerFactory 用。我们不给 tls-client 配代理（走代理
// 用 stdlib，见下），它就只管直连，tls-client 传进来的参数都用不上。
func newTLSDialer(string, time.Duration, *net.TCPAddr, fhttp.Header, tls_client.Logger) (proxy.ContextDialer, error) {
	return tlsDialer{}, nil
}

type tlsDialer struct{}

func (tlsDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	conn.SetReadDeadline(time.Now().Add(handshakeTimeout))
	return &answeredConn{Conn: conn}, nil
}

// answeredConn 在对方头一回回话时撤掉拨号时设的读超时，之后这条连接怎么用都不再限时。
type answeredConn struct {
	net.Conn
	once sync.Once
}

func (c *answeredConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.once.Do(func() { c.Conn.SetReadDeadline(time.Time{}) })
	}
	return n, err
}

// ─── Stdlib HTTP client for proxied requests ─────────────────────────────────
//
// 走代理时改用 stdlib 而不是 tls-client，因为：
// 1. stdlib 的 http.ProxyURL 原生支持 socks5:// / socks5h:// / http:// / https://，
//    已知能过我们手头的代理。
// 2. tls-client 自带的 SOCKS 实现在某些代理端会 EOF（已实测）。
//
// 代价要写清楚：走代理时 Google 看到的是**我们自己的 TLS 指纹**，即 Go 标准库的，
// 不是 Chrome 146 的。HTTP CONNECT 只建隧道，TLS 是我们跟目标端到端握的。
// JA3 回显实测：stdlib 直连和过代理是同一个 JA3（03117a8e…），而同一个代理下换成
// tls-client 就变成另一个（2d25c563…）——两条判据都说明代理不介入 TLS。
//
// 但这不构成换回 tls-client 的理由：同时起跑的对照里 tls-client Chrome_146 打到
// 111 次被拦、Go stdlib 103 次，差 8 次；而同配置换个出口的方差能到 36%
// （151 vs 111）。指纹差异真实存在，对封禁阈值没有可观测影响。
//
// 不走代理时仍然用 getTLSClient（保留 utls/chrome146 真指纹优势）。

var (
	stdlibClientCache sync.Map // proxyURL|超时秒数 -> *http.Client
)

// getStdlibClient returns an http.Client routed through proxyURL.
// proxyURL must not be empty (caller checks).
func getStdlibClient(proxyURL string) *http.Client {
	return stdlibClientWithin(proxyURL, rtCfg().RequestTimeout)
}

// getStdlibStreamClient 同 getStdlibClient，但不限整次请求的时长（见 getTLSStreamClient）。
func getStdlibStreamClient(proxyURL string) *http.Client {
	return stdlibClientWithin(proxyURL, 0)
}

// stdlibClientWithin 是走 proxyURL、整次请求限时 timeout 秒（0 = 不限）的 http.Client。
func stdlibClientWithin(proxyURL string, timeout int) *http.Client {
	// 同 getTLSClient：超时进 key，面板改超时后换新客户端。
	key := fmt.Sprintf("%s|%d", proxyURL, timeout)
	if cached, ok := stdlibClientCache.Load(key); ok {
		return cached.(*http.Client)
	}
	t := &http.Transport{
		// 跟 Google 的 TLS 握手限时，同 tlsDialer：对话请求不限整次时长，握手没回音时
		// 不必等满 stallWatch 的上限。连代理那步不另外限：配了 DialContext，Transport 会
		// 悄悄关掉 HTTP/2；它本来就带着请求的 ctx，stallWatch 管得到。
		TLSHandshakeTimeout: handshakeTimeout,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		// 走代理时不强求 HTTP/2，部分代理不支持。
		ForceAttemptHTTP2: false,
	}
	if u, err := url.Parse(proxyURL); err == nil {
		t.Proxy = http.ProxyURL(u)
	}
	c := &http.Client{
		Timeout:   time.Duration(timeout) * time.Second,
		Transport: t,
		// 跟 tls-client 一致：不自动跟随重定向（302 是诊断信号）
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	stdlibClientCache.Store(key, c)
	return c
}

// loadCookie reads the cookie file (Netscape one-line format or JSON).
// loadCookie 返回 (cookie 串, SAPISID)。只有旁路探测在用 —— 正式请求那条路
// 在 streamGenerate 里自己挑号，因为它还要挨个换号和绑定出口。
//
// cookie 只有 cookie 池一个来源：挑一个 enabled 账号（最久未用优先，自动轮转
// 分散单 IP 上限）。池空 = 匿名。原来那条「池空回落单 cookie」的路径已经取消，
// 它的值在启动时被 seedCookiesFromConfig 并进池子了。
//
// 第三个返回值是池里那条记录的 ID，请求结束后拿它调 markCookieByStatus 回写健康度。
func loadCookie() (cookie, sapisid string) {
	if a, ok := pickCookieAccount(); ok {
		return a.Cookie, extractSAPISID(a.Cookie)
	}
	return "", ""
}

func makeSAPISIDHash(sapisid string) string {
	ts := time.Now().Unix()
	h := sha1.Sum([]byte(fmt.Sprintf("%d %s https://gemini.google.com", ts, sapisid)))
	return fmt.Sprintf("SAPISIDHASH %d_%s", ts, hex.EncodeToString(h[:]))
}

// ChromeUA 是给 stdlib 走代理时用的 Chrome 146 真实 UA 模板。
const ChromeUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"

// FirefoxUA 是走 Firefox/Zen 指纹时的 UA 模板。版本号对齐底层 TLS profile（tls-client 的
// Firefox_147），不报更高的 157——UA 报的大版本跟 TLS 握手指纹对不上反而更可疑。
const FirefoxUA = "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0"

// isFirefoxImpersonate 是 impersonate 字符串本身的 Firefox 判据（老用法，向后兼容）。
func isFirefoxImpersonate(imp string) bool {
	p := strings.ToLower(imp)
	return strings.HasPrefix(p, "firefox") || p == "zen"
}

// isFirefoxProfile 判断这次该不该走 Firefox 指纹。两个来源，任一成立即是：
//  1. firefox_fingerprint 实验开关打开（设置页里显式 opt-in，见 config.go）；
//  2. impersonate 字符串本身就填成了 firefox*/zen（老用法，向后兼容）。
//
// TLS profile（getTLSClient→effectiveImpersonate）和应用层 header（applyBrowserHeaders）
// 认的是同一个判据，两层不会错位成「Firefox UA + Chrome 握手」。
func isFirefoxProfile() bool {
	c := rtCfg()
	return c.FirefoxFingerprint || isFirefoxImpersonate(c.Impersonate)
}

// effectiveImpersonate 是真正拿去 resolveProfile 的指纹名。开着 Firefox 实验开关时强制
// firefox_147（跟 FirefoxUA 的大版本对齐），否则用用户配的 impersonate（默认 chrome_146）。
func effectiveImpersonate() string {
	c := rtCfg()
	if c.FirefoxFingerprint {
		return "firefox_147"
	}
	return c.Impersonate
}

// impersonateFor 是从一份已取出的配置快照算 effectiveImpersonate，不重新读 rtVal。
func impersonateFor(c RuntimeConfig) string {
	if c.FirefoxFingerprint {
		return "firefox_147"
	}
	return c.Impersonate
}

// chromeMajorFor 是给定指纹名实际用的 Chrome 大版本，直接读 tls-client profile 的名字
// （"Chrome-133"）。跟 resolveProfile 是同一个来源：impersonate 填了表里没有的名字
// （如 chrome_157）会落到 146，这里也跟着报 146，UA / Sec-CH-UA 不会报一个握手里没有
// 的版本。非 Chrome profile 返回 "146"。
func chromeMajorFor(imp string) string {
	s := resolveProfile(imp).GetClientHelloStr()
	if v, ok := strings.CutPrefix(s, "Chrome-"); ok && v != "" {
		return v
	}
	return "146"
}

// chromeMajor 是当前配置下 TLS 指纹实际用的 Chrome 大版本，见 chromeMajorFor。
func chromeMajor() string {
	return chromeMajorFor(effectiveImpersonate())
}

// chromeClientHintKeys 是 Chrome 指纹下要带的三个 Sec-CH-UA 头的名字；Firefox 不发这组，
// 用来把调用方/上一版配置留下的同名头清掉。
var chromeClientHintKeys = []string{"Sec-CH-UA", "Sec-CH-UA-Mobile", "Sec-CH-UA-Platform"}

// chromeHintsFor 按 Chrome 大版本拼三个 Sec-CH-UA 头。
// GREASE 那一项（"Not?A_Brand";v="24"）真 Chrome 每个版本不一样，这里没按版本细分。
func chromeHintsFor(v string) map[string]string {
	return map[string]string{
		"Sec-CH-UA":          `"Chromium";v="` + v + `", "Google Chrome";v="` + v + `", "Not?A_Brand";v="24"`,
		"Sec-CH-UA-Mobile":   "?0",
		"Sec-CH-UA-Platform": `"Windows"`,
	}
}

// chromeClientHints 是 Chrome 指纹下要带的三个 Sec-CH-UA 头；Firefox 不发这组。版本跟 chromeMajor。
func chromeClientHints() map[string]string {
	return chromeHintsFor(chromeMajor())
}

// browserHeaderSnap 是一次请求要发的浏览器身份头快照：UA、Firefox 判据、Sec-CH-UA
// 全部从同一次 rtCfg() 读取派生。rtVal 可热更新（PUT /admin/api/config 随时改
// impersonate / firefox_fingerprint），分次读会把两版配置拼进同一个请求——Chrome/133
// 的 UA 配 v=146 的 Sec-CH-UA，或 Firefox UA 配 Chrome hints——比普通的不一致更可疑。
type browserHeaderSnap struct {
	firefox bool
	ua      string
	hints   map[string]string // nil = 该请求不发这组头（Firefox）
}

// snapshotBrowserHeaders 读一次 rtCfg()，把当次请求的 UA / Firefox 判据 / Sec-CH-UA 定下来。
func snapshotBrowserHeaders() browserHeaderSnap {
	c := rtCfg()
	snap := browserHeaderSnap{firefox: c.FirefoxFingerprint || isFirefoxImpersonate(c.Impersonate)}
	if snap.firefox {
		snap.ua = FirefoxUA
		return snap
	}
	v := chromeMajorFor(impersonateFor(c))
	snap.ua = strings.Replace(ChromeUA, "Chrome/146.", "Chrome/"+v+".", 1)
	snap.hints = chromeHintsFor(v)
	return snap
}

func currentBrowserUA() string {
	return snapshotBrowserHeaders().ua
}

// fetchSite 按目标主机算 Sec-Fetch-Site：所有请求都假定由 gemini.google.com 页面发起。
// 浏览器的规则是同源 same-origin、同一可注册域（google.com）same-site、其余 cross-site。
// 以前一律写 same-origin，发往 accounts / clients6（同站）和 lh3.googleusercontent.com、
// content-push.googleapis.com、用户给的外部图片（跨站）的请求都跟真浏览器对不上。
func fetchSite(host string) string {
	host = strings.ToLower(host)
	switch {
	case host == "gemini.google.com":
		return "same-origin"
	case host == "google.com" || strings.HasSuffix(host, ".google.com"):
		return "same-site"
	default:
		return "cross-site"
	}
}

// writeBrowserHeaders 是浏览器应用层 header 的唯一来源，stdlib 和 fhttp 两种请求都经它写入，
// 两条传输路径不会再各写一份、改一处漏一处。host 是请求的目标主机，用来算 Sec-Fetch-Site。
func writeBrowserHeaders(host string, set func(k, v string), del func(k string)) {
	snap := snapshotBrowserHeaders()
	set("User-Agent", snap.ua)
	if snap.firefox {
		for _, k := range chromeClientHintKeys {
			del(k)
		}
	} else {
		for k, v := range snap.hints {
			set(k, v)
		}
	}
	set("Sec-Fetch-Dest", "empty")
	set("Sec-Fetch-Mode", "cors")
	set("Sec-Fetch-Site", fetchSite(host))
}

// setChromeHeaders 把对应 impersonate 配置的应用层 header 填进 net/http 或 fhttp 的 Header。
// host 是请求的目标主机（Sec-Fetch-Site 按它算）。调用方之后再写自己的 header，同名的以调用方为准。
func setChromeHeaders(h interface {
	Set(key, value string)
	Del(key string)
}, host string) {
	writeBrowserHeaders(host, h.Set, h.Del)
}

// applyChromeHeaders 给 stdlib 请求填上应用层 header（根据 impersonate 配置自动适配 Chrome 或 Firefox）。
func applyChromeHeaders(req *http.Request) {
	applyBrowserHeaders(req)
}

// applyBrowserHeaders 给 stdlib 请求填上对应 impersonate 配置的应用层 header。
func applyBrowserHeaders(req *http.Request) {
	writeBrowserHeaders(req.URL.Hostname(), req.Header.Set, req.Header.Del)
}

// applyBrowserFHeaders 给 fhttp (tls-client 直连) 请求填上对应 impersonate 配置的应用层 header。
func applyBrowserFHeaders(req *fhttp.Request) {
	writeBrowserHeaders(req.URL.Hostname(), req.Header.Set, req.Header.Del)
}
