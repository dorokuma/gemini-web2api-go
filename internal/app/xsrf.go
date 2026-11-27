package app

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
)

// Gemini 在带登录 cookie 时要求 batchexecute 请求多带一个表单字段 at（XSRF token），
// 不带就直接 400，响应体形如 [["er",...,400,...,[{"48448350":["xsrf", ...]}]]]。
// 匿名请求不需要它，所以这个坑一直没暴露——一挂上有效 cookie，所有请求立刻全挂。
//
// token 来自 /app 页面 HTML 里的 "SNlM0e":"<token>:<毫秒时间戳>"，跟 cookie 会话
// 绑定，所以按 cookie 分别缓存。实测同一 token 可以复用，过期后服务端还是回 xsrf
// 错误，调用方拿到这个错要 invalidate 再取一次。
var (
	xsrfMu    sync.Mutex
	xsrfCache = map[string]xsrfEntry{}
)

type xsrfEntry struct {
	token      string
	pushID     string // 上传文件用的 Push-ID 头
	pctx       string // 上传文件用的 X-Client-Pctx 头
	fetched    time.Time
	refreshing bool // 后台刷新正在飞，避免并发请求重复抓页面
}

// 页面上的 token 没写明有效期，取个保守值定期重取。
const xsrfTTL = 20 * time.Minute

// xsrfRefreshAfter 之后命中缓存时照常返回，但在后台重抓一次页面。
//
// /app 页面约 770KB，抓一次 1-3 秒，不能放在请求的关键路径上；但同步重抓顺带
// 起到「cookie 还活着吗」的探测作用（失效的号抓不到 SNlM0e）。后台刷新把这个探测
// 挪出关键路径：刷新失败就丢掉缓存，下一个请求同步重取时照样报出失效、走换号逻辑。
const xsrfRefreshAfter = 5 * time.Minute

var snlm0eRe = regexp.MustCompile(`"SNlM0e":"([^"]{10,200})"`)

// 上传要的两个页面参数，跟 XSRF token 同页取，省一次页面请求。
var pushIDRe = regexp.MustCompile(`"qKIAYe":"([^"]{4,400})"`)

var pctxRe = regexp.MustCompile(`"Ylro7b":"([^"]{4,400})"`)

// cookieKey 用 cookie 的短摘要当缓存键，避免把整串凭证塞进 map key。
//
// 摘要只取账号身份（SAPISID + __Secure-1PSID，见 cookieIdentity），不取整串：
// 上游几乎每个响应都刷新 *SIDCC，streamGenerate 合并 Set-Cookie 后写回账号，
// 按整串取键的话下一个请求必然 miss，每次都要多抓一次 /app 页面（实测登录态每个
// 请求多出约 0.9 秒，且不计入 ttfb_ms / total_ms）。token 跟会话绑定、不跟 SIDCC
// 绑定；真过期了上游回 xsrf 错误，调用方 invalidate 后重取。
func cookieKey(cookie string) string {
	id := cookieIdentity(cookie)
	if id == "|" { // 两项都没有：退回整串，至少不同 cookie 不会撞键
		id = cookie
	}
	sum := sha1.Sum([]byte(id))
	return hex.EncodeToString(sum[:8])
}

// xsrfKey 是缓存键：cookie 摘要 + 账号序号。同一串 cookie 里登着好几个号时，
// 每个号（/u/N）页面给的 SNlM0e 不一样，拿错号的 token 发请求直接 400。
func xsrfKey(cookie string, authuser int) string {
	return cookieKey(cookie) + "/" + strconv.Itoa(authuser)
}

// geminiBase 是某个账号序号对应的 gemini.google.com 前缀。
//
// 浏览器多号登录时 cookie 只有一份，选哪个号看请求路径：/u/N/ 落到第 N 个号，
// 裸路径落到默认号（等同 /u/0/）。实测 StreamGenerate 和 batchexecute 都认路径，
// X-Goog-AuthUser 头被忽略（裸路径 + 头写 1，会话照样落在默认号）。
func geminiBase(authuser int) string {
	if authuser > 0 {
		return "https://gemini.google.com/u/" + strconv.Itoa(authuser)
	}
	return "https://gemini.google.com"
}

// invalidateXSRF 丢掉某个 cookie 的缓存 token，下次取会重新抓页面。
// 同一串 cookie 的各个账号序号一起作废：cookie 一变（轮转）或 token 被拒，
// 哪个号的缓存都不该再信。
func invalidateXSRF(cookie string) {
	if cookie == "" {
		return
	}
	xsrfMu.Lock()
	for n := 0; n <= maxAuthUser; n++ {
		delete(xsrfCache, xsrfKey(cookie, n))
	}
	xsrfMu.Unlock()
}

// xsrfNoWritebackAccountID 是「共享缓存后台刷新」专用的哨兵目标 ID（R7-1）：
// 后台刷新命中缓存 5-20 分钟旧项时用本次调用的 cookie 起后台 HTTP，但共享
// 键缓存项没有单一账号归属——其 Set-Cookie 回收**绝不写账号 DB**（只更新
// XSRF/push 缓存），从根源关闭「A 在飞被停用后 B 成为同身份同序号唯一启用
// 行、反查把后台刷新响应写进 B」的跨行写回。
const xsrfNoWritebackAccountID int64 = -1

// getXSRF 取该 cookie（第 authuser 个号）对应的 XSRF token；命中缓存且没过期就直接返回。
// cookie 为空（匿名）时返回空串——匿名请求不需要这个字段。
func getXSRF(cookie string, authuser int, proxyURL string) (string, error) {
	return getXSRFSwitch(cookie, authuser, proxyURL, true)
}

// getXSRFForAccount 是 getXSRF 的账号透传版（R7-1）：主路径已知账号 ID
// （conversation 多轮 conv.accountID、400 自愈 cookieID 等）必须经它透传，
// 纯 Go 回收写回只校验该行，绝不反查同身份同序号启用兄弟行把旧请求
// 响应写进新会话行。
func getXSRFForAccount(cookie string, authuser int, proxyURL string, targetAccountID int64) (string, error) {
	return getXSRFSwitchForAccount(cookie, authuser, proxyURL, true, targetAccountID)
}

// getXSRFSwitch 是 getXSRF 的辅助入口版：allowSwitch=false 时浏览器路径
// 绝不切驻留 jar（驻留号坏了直接回错，由调用方回退纯 Go HTTP）。
func getXSRFSwitch(cookie string, authuser int, proxyURL string, allowSwitch bool) (string, error) {
	return getXSRFSwitchForAccount(cookie, authuser, proxyURL, allowSwitch, 0)
}

// getXSRFSwitchForAccount 是 getXSRFSwitch 的账号透传版（R6-1，Reviewer MF-1）：
// targetAccountID > 0 时把入口已知的账号 ID 透传到纯 Go /app 回收写回——
// 写回只校验该行（停用/删除直接不写），绝不反查同身份同序号的其它启用行
// 把 A 的响应写进 B。targetAccountID == 0 与 getXSRFSwitch 完全一致。
// R7-1：targetAccountID == xsrfNoWritebackAccountID（-1）是共享缓存后台刷新
// 专用哨兵——缓存命中与共享键的后台刷新无单一账号归属，其 /app 回收**绝不
// 写账号 DB**，只更新 XSRF token 缓存。
func getXSRFSwitchForAccount(cookie string, authuser int, proxyURL string, allowSwitch bool, targetAccountID int64) (string, error) {
	if cookie == "" {
		return "", nil
	}
	key := xsrfKey(cookie, authuser)

	if e, ok := cachedAppTokens(key, cookie, authuser, proxyURL, allowSwitch); ok {
		return e.token, nil
	}

	e, err := fetchAppTokensForAccount(cookie, authuser, proxyURL, allowSwitch, targetAccountID)
	if err != nil {
		return "", err
	}
	xsrfMu.Lock()
	xsrfCache[key] = e
	xsrfMu.Unlock()
	return e.token, nil
}

// cachedAppTokens 返回未过期的缓存项；过了 xsrfRefreshAfter 时顺带踢一次后台刷新。
// 后台刷新是**辅助入口**：无论本次调用方 allowSwitch 是什么，一律固定传
// false——绝不能让主请求的 true 透传进后台 goroutine 去偷换驻留 jar。
func cachedAppTokens(key, cookie string, authuser int, proxyURL string, allowSwitch bool) (xsrfEntry, bool) {
	xsrfMu.Lock()
	defer xsrfMu.Unlock()
	e, ok := xsrfCache[key]
	if !ok || time.Since(e.fetched) >= xsrfTTL {
		return xsrfEntry{}, false
	}
	if !e.refreshing && time.Since(e.fetched) >= xsrfRefreshAfter {
		e.refreshing = true
		xsrfCache[key] = e
		go refreshAppTokens(key, cookie, authuser, proxyURL, false)
	}
	return e, true
}

// refreshAppTokens 后台重抓页面。失败就丢掉缓存，让下一个请求同步重取并报出失效。
// 后台刷新是辅助入口：allowSwitch=false，不打扰驻留 jar。
// R7-1：后台刷新命中的是共享键缓存项，无单一账号归属——其 /app 下发的
// Set-Cookie 回收**绝不写账号 DB**（xsrfNoWritebackAccountID 哨兵透传），
// 只更新 XSRF token / push 缓存本身。
func refreshAppTokens(key, cookie string, authuser int, proxyURL string, allowSwitch bool) {
	if err := storeAppTokens(key, cookie, authuser, proxyURL, allowSwitch); err != nil {
		logf("[xsrf] 后台刷新失败，已作废缓存: %v", err)
	}
}

func storeAppTokens(key, cookie string, authuser int, proxyURL string, allowSwitch bool) error {
	e, err := fetchAppTokensForAccount(cookie, authuser, proxyURL, allowSwitch, xsrfNoWritebackAccountID)
	xsrfMu.Lock()
	defer xsrfMu.Unlock()
	if err != nil {
		delete(xsrfCache, key)
		return err
	}
	xsrfCache[key] = e
	return nil
}

// getUploadTokens 取上传要用的 Push-ID / X-Client-Pctx，跟 XSRF token 同一份缓存。
func getUploadTokens(cookie string, authuser int, proxyURL string) (pushID, pctx string, err error) {
	return getUploadTokensForAccount(cookie, authuser, proxyURL, 0)
}

// getUploadTokensForAccount 是 getUploadTokens 的账号透传版（R7-1）：
// 上传附件/历史附件的账号 ID（gemini.go 挑号后已知的 cookieID）必须透传
// 到底——缓存 miss 同步抓 /app 时的 Set-Cookie 回收只校验该行，绝不反查
// 同身份同序号启用兄弟行。targetAccountID == 0 与 getUploadTokens 完全一致
// （无归属共享读）。
func getUploadTokensForAccount(cookie string, authuser int, proxyURL string, targetAccountID int64) (pushID, pctx string, err error) {
	key := xsrfKey(cookie, authuser)

	if e, ok := cachedAppTokens(key, cookie, authuser, proxyURL, true); ok {
		return e.pushID, e.pctx, nil
	}

	e, err := fetchAppTokensForAccount(cookie, authuser, proxyURL, true, targetAccountID)
	if err != nil {
		return "", "", err
	}
	xsrfMu.Lock()
	xsrfCache[key] = e
	xsrfMu.Unlock()
	return e.pushID, e.pctx, nil
}

// appPageHeaders 是 GET /app 这种「打开网页」请求的 header：浏览器地址栏打开页面是
// document / navigate，不是页面里发的 XHR（empty / cors）。抓 token 和匿名拿会话都用它，
// 盖在 writeBrowserHeaders 的 XHR 默认值上面。
func appPageHeaders() map[string]string {
	return map[string]string{
		"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
		"Accept-Language":           "en-US,en;q=0.9",
		"Upgrade-Insecure-Requests": "1",
		"Sec-Fetch-Dest":            "document",
		"Sec-Fetch-Mode":            "navigate",
		"Sec-Fetch-Site":            "none",
	}
}

// fetchAppPage 抓 gemini.google.com/app 的 HTML。
// 走跟主请求相同的出口：配了代理走 stdlib，没配走 tls-client，
// 免得页面里取到的 token 和后续请求来自两个不同 IP。
// cookie 传空串就是匿名抓（页面照样返回，只是没有登录态字段）。
// authuser>0 抓 /u/N/app，页面里的 token 才是第 N 个号的。
func fetchAppPage(cookie string, authuser int, proxyURL string) ([]byte, error) {
	return fetchAppPageCtx(context.Background(), cookie, authuser, proxyURL)
}

// fetchAppPageCtx 同 fetchAppPage，支持透传调用方 ctx 取消/超时。
func fetchAppPageCtx(ctx context.Context, cookie string, authuser int, proxyURL string) ([]byte, error) {
	body, _, err := fetchAppPageWithCookiesCtx(ctx, cookie, authuser, proxyURL)
	return body, err
}

// fetchAppPageWithCookiesCtx 抓 /app 页面，在 HTTP 200 时顺带带回响应的
// Set-Cookie 切片（Google 常下发新 *SIDCC，纯 Go 模式下必须回收持久化，
// 否则池内账号的票据停在导入时的旧值）。
func fetchAppPageWithCookiesCtx(ctx context.Context, cookie string, authuser int, proxyURL string) ([]byte, []string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	pageURL := geminiBase(authuser) + "/app"
	headers := appPageHeaders()
	if cookie != "" {
		headers["Cookie"] = cookie
	}

	var body []byte
	var setCookies []string
	if proxyURL != "" {
		req, err := http.NewRequestWithContext(ctx, "GET", pageURL, nil)
		if err != nil {
			return nil, nil, err
		}
		applyChromeHeaders(req)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		sc, cerr := getStdlibClient(proxyURL)
		if cerr != nil {
			return nil, nil, cerr
		}
		resp, err := sc.Do(req)
		if err != nil {
			return nil, nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return nil, nil, appPageError(resp.StatusCode, resp.Header.Get("Location"), cookie)
		}
		setCookies = resp.Header.Values("Set-Cookie")
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, nil, err
		}
	} else {
		req, err := fhttp.NewRequestWithContext(ctx, "GET", pageURL, nil)
		if err != nil {
			return nil, nil, err
		}
		setChromeHeaders(req.Header, req.URL.Hostname())
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := getTLSClient().Do(req)
		if err != nil {
			return nil, nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return nil, nil, appPageError(resp.StatusCode, resp.Header.Get("Location"), cookie)
		}
		setCookies = resp.Header.Values("Set-Cookie")
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, nil, err
		}
	}
	return body, setCookies, nil
}

// fetchAppTokens 抓一次 /app 页面，把三个 token 一起抠出来。
// allowSwitch=false 时浏览器路径绝不切驻留 jar（辅助/管理入口）。
func fetchAppTokens(cookie string, authuser int, proxyURL string, allowSwitch bool) (xsrfEntry, error) {
	return fetchAppTokensForAccount(cookie, authuser, proxyURL, allowSwitch, 0)
}

// fetchAppTokensForAccount 是 fetchAppTokens 的账号透传版（R6-1）：
// targetAccountID > 0 时纯 Go 回收写回只校验该行；== 0 保持唯一反查。
func fetchAppTokensForAccount(cookie string, authuser int, proxyURL string, allowSwitch bool, targetAccountID int64) (xsrfEntry, error) {
	// 浏览器传输模式：直接从常驻页面上下文拿（页内 fetch /app 的 HTML 里就有
	// WIZ_global_data），绕开纯 Go HTTP 的另一次 GET /app。
	// R8-2（Reviewer MF-2）：xsrfNoWritebackAccountID（-1）哨兵是共享缓存后台
	// 刷新专用——其回收**绝不写账号 DB**。浏览器分支的 defer rc.Close() 会启动
	// SyncAccountCookiesBack，有 Jar 增量时真实更新账号行；故哨兵目标在此
	// **直接跳过整个浏览器分支**（不触碰任何驻留 Jar），走纯 Go HTTP 仅更新
	// token 缓存，无写回策略贯穿传输选择。
	if shouldUseBrowserTransport() && targetAccountID != xsrfNoWritebackAccountID {
		var acct *CookieAccount
		if targetAccountID > 0 {
			// R8-2：入口已点名目标账号时绝不经 browserAccountFromHeaders
			// 反查——目标行停用后反查会命中同身份同序号的其它启用兄弟行，
			// 页内 /app 借用的是 B 的 Jar，写回同样是 B 的 Jar 增量。
			// 只选 status==enabled 且 cookieIdentity 与 authuser 均精确匹配
			// 的指定行；任一不符回退纯 Go HTTP（原 Cookie 无归属读取不受影响）。
			if a := accountByID(targetAccountID); a != nil &&
				a.Status == "enabled" && a.AuthUser == authuser &&
				cookieIdentity(cookie) != "|" &&
				cookieIdentity(a.Cookie) == cookieIdentity(cookie) {
				cp := *a
				acct = &cp
			}
		} else {
			// 透传调用方真实 authuser：同 cookieIdentity 下 /u/N 的 token 不同，
			// browserAccountFromHeaders 靠这个头精确匹配到对应 authuser 的记录。
			acct = browserAccountFromHeaders(map[string]string{
				"Cookie":          cookie,
				"X-Goog-AuthUser": strconv.Itoa(authuser),
			})
		}
		// 带 Cookie 但反查不到 enabled 账号（status=disabled 的管理员检测、
		// 未入池 Cookie）：绝不能把 acct=nil 传进浏览器当匿名处理——匿名
		// /app 页拿不到 SNlM0e，上层会误报「登录态有效」且重置 authuser。
		// 与 gemini.go / upload.go 的 skipBrowser 语义一致：带 Cookie 的
		// 非池请求走纯 Go HTTP，用原 cookie 与 authuser 完成真实检测。
		if !(cookie != "" && acct == nil) {
			ctx, cancel := context.WithTimeout(context.Background(), browserFetchTokensTimeout)
			defer cancel()
			// proxyURL 传进浏览器层：冷启动刚建好的桥用首航保守出口，本次选定
			// 的 proxyURL 在 ensureRunning 返回后同步（见 doBrowserStreamRequestCtx）。
			e, err := browserMgr.fetchAppTokensViaBrowserCtx(ctx, proxyURL, acct, allowSwitch, nil)
			if err != nil {
				// 辅助入口哨兵：该用哪个 jar 与驻留不符——无条件回退纯 Go HTTP，
				// 不看 browser_fallback_http，也不是浏览器坏了。
				if isBrowserAuxSkip(err) {
					logf("[xsrf] 辅助入口不适配驻留 jar，回退纯 Go HTTP")
				} else if isBrowserLocalError(err) && !rtCfg().BrowserFallbackHTTP {
					// 浏览器不可用且没开回退 → 原样上报（isBrowserLocalError），
					// 上层拿它归 browser_unavailable 而不是当 cookie 失效换号。
					return xsrfEntry{}, err
				} else if isBrowserLocalError(err) {
					logf("[xsrf] 浏览器取 token 失败，回退纯 Go HTTP（browser_fallback_http 已开）: %v", err)
				} else {
					return xsrfEntry{}, err
				}
			} else {
				return e, nil
			}
		}
	}
	body, setCookies, err := fetchAppPageWithCookiesCtx(context.Background(), cookie, authuser, proxyURL)
	if err != nil {
		return xsrfEntry{}, err
	}
	e := xsrfEntry{fetched: time.Now()}
	if m := snlm0eRe.FindSubmatch(body); m != nil {
		e.token = string(m[1])
	} else if cookie != "" {
		// 带 cookie 却拿不到 token = cookie 已失效被当成匿名。匿名本就没这字段，不算错。
		return xsrfEntry{}, fmt.Errorf("no SNlM0e in page (cookie expired or not signed in)")
	}
	if p := pushIDRe.FindSubmatch(body); p != nil {
		e.pushID = string(p[1])
	}
	if p := pctxRe.FindSubmatch(body); p != nil {
		e.pctx = string(p[1])
	}
	// 纯 Go 路径回收 /app 下发的 Set-Cookie：确认抓到有效登录态页面后增量
	// 落库。BrowserTransport 下有活跃或 creating 浏览器槽驻留时跳过——该号的
	// jar 由 SyncAccountCookiesBack 负责，绝不在这里写库误杀在途流/新建槽。
	// 守卫是原子的（mergeAccountSetCookieIfNoActiveBrowser 在 L1 临界区内
	// 检查+写库），消除锁外检查与写库之间的建槽注册 TOCTOU 窗口。
	// R4-3（Reviewer MF-3）：有副作用的写回必须用精确匹配查找——目标账号
	// 停用/删除时 browserAccountFromHeaders 的读路径 fallback 会命中同身份
	// 不同 authuser 的兄弟行 B，把 /u/N 的 Set-Cookie 写进 B。仅当存在
	// enabled + 同身份 + 同 authuser 的精确行才写回；无精确匹配不写回。
	// R6-1（Reviewer MF-1）：入口已点名目标账号（targetAccountID > 0）时
	// 写回只校验该行——该行已停用/删除直接不写，绝不反查同身份同序号的
	// 其它启用行把 A 的响应写进 B；仅未点名（== 0）时才走唯一反查。
	// R7-1（Reviewer MF-1 彻底闭合）：xsrfNoWritebackAccountID（-1）哨兵
	// 直接跳过整个写回——共享缓存后台刷新的 Set-Cookie 只更新 token 缓存，
	// 绝不落账号 DB。
	if cookie != "" && e.token != "" && len(setCookies) > 0 && targetAccountID != xsrfNoWritebackAccountID {
		if targetAccountID > 0 {
			if acct := accountByID(targetAccountID); acct != nil && acct.Status == "enabled" {
				mergeAccountSetCookieIfNoActiveBrowser(acct.ID, setCookies,
					geminiBase(authuser)+"/app", cookieIdentity(cookie), authuser)
			}
		} else if acct := enabledAccountByCookieAndAuthuser(cookie, authuser); acct != nil && acct.ID > 0 {
			mergeAccountSetCookieIfNoActiveBrowser(acct.ID, setCookies,
				geminiBase(authuser)+"/app", cookieIdentity(cookie), authuser)
		}
	}
	return e, nil
}

// wcHandshakeXSRFMarker 是 WebChannel 握手 400 脱敏诊断里透出的 XSRF 信号标记：
// 握手非 200 不回传上游响应体，但 400 正文含 "xsrf" 时附带该结构化标记，
// 让上层 isXSRFError 能稳定触发自愈刷新（不泄露原文）。
const wcHandshakeXSRFMarker = "xsrf=true"

// isXSRFError 判断上游 400 是不是 XSRF token 的问题。
// 老协议响应体形如：[["er",null,...,400,...,[{"48448350":["xsrf","<新token>",...]}]]]
// WebChannel 握手 400 脱敏诊断不回传响应体，只带 wcHandshakeXSRFMarker 标记。
func isXSRFError(raw string) bool {
	return strings.Contains(raw, `"xsrf"`) || strings.Contains(raw, wcHandshakeXSRFMarker)
}

// appPageStatusError 是抓 /app 拿到非 200 时的错误，带上重定向目标。
//
// 两个 client 都不跟随重定向，所以 302 本身说明不了原因：跳 accounts.google.com
// 是登录态没了，跳 www.google.com/sorry/ 是出口被 Google 拦，跟 cookie 无关。
// 只留 host+path：/sorry 的 query 里有长串 continue/q 参数，进日志和面板没用。
// 文本仍以 "fetch /app: HTTP <code>" 开头，按子串判 "HTTP 302" 的地方不受影响。
type appPageStatusError struct {
	Status   int
	Location string // host+path，没有 Location 头时为空
	// MissingOSID：跳的是登录页，且 cookie 里没有 OSID。2026-11-21 实测去掉 OSID
	// 就 302 到 ServiceLogin?osid=1，带上就 200，所以这种组合直接点名缺 OSID。
	MissingOSID bool
}

// appPageError 在 newAppPageStatusError 之上补 MissingOSID 判断（要看 cookie）。
func appPageError(status int, location, cookie string) *appPageStatusError {
	e := newAppPageStatusError(status, location)
	e.MissingOSID = cookie != "" && strings.HasPrefix(e.Location, "accounts.google.com") &&
		!hasCookieName(cookie, "OSID")
	return e
}

func newAppPageStatusError(status int, location string) *appPageStatusError {
	e := &appPageStatusError{Status: status}
	if location != "" {
		if u, err := url.Parse(location); err == nil {
			e.Location = u.Host + u.Path
		} else {
			// R5-5（Reviewer MF-5）：解析失败时绝不把整个原始 Location
			// 头赋进来——原始串的 query/fragment 可能携带私有参数，随
			// 502 错误文本与请求表外泄。只留固定占位，状态码与有效
			// 登录跳转的分类不受影响。
			e.Location = "<invalid-location>"
		}
	}
	return e
}

func (e *appPageStatusError) Error() string {
	if e.MissingOSID {
		return fmt.Sprintf("fetch /app: HTTP %d -> %s (cookie 缺 OSID)", e.Status, e.Location)
	}
	if e.Location != "" {
		return fmt.Sprintf("fetch /app: HTTP %d -> %s", e.Status, e.Location)
	}
	return fmt.Sprintf("fetch /app: HTTP %d", e.Status)
}
