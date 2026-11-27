package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"

	"net/url"
)

// 会话保活。同一条 POST /RotateCookies 有两种 payload，刷的不是同一族 cookie：
//
//  1. 哨兵 `[000,"-0000000000000000000"]`：无条件换发 `__Secure-1PSIDTS` /
//     `__Secure-3PSIDTS`。这是登录态真正的短命票（约 30 分钟），不刷就会被
//     当匿名。payload 必须是这串 JSPB（前导零合法、严格 JSON 非法），
//     json.Marshal 会变成 `[0,"…"]`，服务端不认。
//
//  2. 浏览器 iframe 那条：先 GET RotateCookiesPage 拿会话 id，再 POST
//     `[658,"<id>"]`。只刷新 SIDCC / `__Secure-1PSIDCC` / `__Secure-3PSIDCC`，
//     间隔由页面 init(...) 最后一个参数给出（实测 600 秒）。
//
// 以前只做了第 2 条，所以号大概半小时就死（issue #6）。Chrome 新版本还有
// DBSC 设备绑定（GET /RotateBoundCookies + 签名 JWT），那条我们复刻不了；
// 哨兵这条对 Firefox 导出、以及未绑定设备的会话有效。Chrome 导出的号如果
// 反复 401，换 Firefox 重新登录再导出。

const (
	rotatePageURL = "https://accounts.google.com/RotateCookiesPage" +
		"?og_pid=658&rot=3&origin=https%3A%2F%2Fgemini.google.com&exp_id=0"
	rotatePostURL = "https://accounts.google.com/RotateCookies"
	// og_pid 是产品标识，Gemini 固定 658；它既作为上面页面的 query，也回显在页面里。
	rotateProductID = 658
	// 服务端没给出间隔时的兜底值。
	defaultRotateInterval = 10 * time.Minute
	// 启动后尽快刷一次：导入时 cookie 可能已经快到期，干等 10 分钟会直接过期。
	firstRotateDelay = 15 * time.Second
	// 哨兵 payload。前导零是故意的，见文件头。
	rotate1PSIDTSBody = `[000,"-0000000000000000000"]`
	// 打太勤会 429。Gemini-API / notebooklm-py 都用 60 秒地板。
	min1PSIDTSInterval = 60 * time.Second
)

// 刷新 1PSIDTS 只带这一对。多带实测会 401。
var rotate1PSIDTSCookies = []string{"__Secure-1PSID", "__Secure-1PSIDTS"}

var (
	psidtsMu     sync.Mutex
	psidtsLastAt = map[int64]time.Time{}
)

// 页面里形如：init('4162200486104360679', 658.0, 0.0, 0.0, 600.0)
// 第一个参数是这个会话的标识，最后一个是下次轮转的间隔秒数。
var rotateInitRe = regexp.MustCompile(`init\('([^']{4,64})'\s*,\s*([0-9.]+)\s*,[^)]*?([0-9.]+)\s*\)`)

// rotateProxyURL 为保活决定出口（fail-closed，绝不退回静默直连泄漏 JP/US Cookie）：
//   - a.ProxyID == proxyIDDirect（-1 锁定直连）：允许 proxyURL="" 直连；
//   - a.ProxyID > 0：必须查到存在且 Enabled 的代理 URL，代理被删/禁用直接
//     报错跳过本轮保活（Cookie 绝不从本机 IP 发出）；
//   - a.ProxyID == 0（未绑定）：代理池非空且 fallback_direct 关着时挑一个
//     可用启用代理，挑不到同样 fail-closed 跳过。
func rotateProxyURL(a CookieAccount) (string, error) {
	switch {
	case a.ProxyID == proxyIDDirect:
		return "", nil
	case a.ProxyID > 0:
		// R10-3（Reviewer M3）：单次 proxyMu.RLock 原子快照同时取
		// enabled 与 url——分两次锁读取时中间删除代理会拼出
		// （enabled=true, url=""）并原样返回 ("", nil)，rotateDo 把空串
		// 当直连，携 Cookie 静默直出本机 IP。原子快照下该组合绝不
		// 存在；url 为空（已删/异常）同样按 fail-closed 报错。
		found, enabled, url := proxyAtomicSnapshot(a.ProxyID)
		if !found || !enabled || url == "" {
			return "", fmt.Errorf("账号 #%d 绑定的出口 #%d 已删除或禁用，跳过保活（fail-closed 不直连）",
				a.ID, a.ProxyID)
		}
		// R11-4（Oracle A5）：存量非法库值在使用侧明确报错——url.Parse
		// 失败的代理串绝不交予传输层（stdlibClientWithin 已拒直连回退，
		// 此处提前给出含账号与代理 ID 的明确错误）。
		if err := validateProxyURL(url); err != nil {
			return "", fmt.Errorf("账号 #%d 绑定的出口 #%d 的 URL 非法（%v），跳过保活（fail-closed 不直连）",
				a.ID, a.ProxyID, err)
		}
		return url, nil
	default:
		if hasProxyPool() && !rtCfg().FallbackDirect {
			url := firstUsableProxyURL()
			if url == "" {
				return "", fmt.Errorf("账号 #%d 未绑出口且代理池无可用出口，跳过保活（fail-closed 不直连）", a.ID)
			}
			return url, nil
		}
		return "", nil
	}
}

// firstUsableProxyURL 返回代理池里第一个可用（enabled 且未熔断或已过冷却）的
// 代理 URL，没有返回空串。未绑定账号的后台保活按池规则借用出口用。
func firstUsableProxyURL() string {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	now := time.Now().Unix()
	cooldown := rtCfg().ProxyCooldownMin
	for _, p := range proxyCache {
		if proxyUsable(p, now, cooldown) {
			return p.URL
		}
	}
	return ""
}

// rotateAccount 给一个账号做一次保活：先刷 1PSIDTS，再刷 SIDCC。
// 返回服务端建议的下次间隔，以及这一轮实际刷新的 cookie 名。
func rotateAccount(a CookieAccount) (time.Duration, []string, error) {
	return rotateAccountWithOpts(a, rotateOpts{})
}

// rotateAccountOpts 是 rotateAccount 的兼容包装（无额外选项的普通轮转）。
func rotateAccountOpts(a CookieAccount, skipIfActive bool) (time.Duration, []string, error) {
	return rotateAccountWithOpts(a, rotateOpts{skipIfActive: skipIfActive})
}

// rotateAccountOpts 选项集（R10-3，Reviewer M4）：把「管理/后台按 DB
// 旧绑定快照轮转」与「自愈按本轮实际挑中的 picked 出口轮转」两种出口
// 语义彻底分开——不再靠覆写 a.ProxyID 传递 picked。
type rotateOpts struct {
	// skipIfActive=true 时若该号已有活跃 BrowserContext（或同
	// cookieIdentity 兄弟号活跃）直接跳过。仅后台 rotateAllAccounts 传
	// true；healXSRFByRotation 与 Admin POST /rotate 传 false。
	skipIfActive bool
	// hasPickedProxy=true 表示自愈轮转（gemini.go healXSRFByRotation）：
	// 本轮出口是前台 acquireSlot 实际挑中的 picked（可能是合法溢出
	// 或 FallbackDirect 合法直连），而不是 DB 绑定。pickedProxyID 是
	// 挑中的出口代理 ID（0 = 直连）；expectedDBProxyID 是挑号后冲突
	// 复核确认过的 DB 绑定（picked 出口在 picking 阶段已按它核过）。
	hasPickedProxy    bool
	pickedProxyID     int64
	expectedDBProxyID int64
	// pickedProxyURL 是挑中时刻该代理的原子快照 URL（R11-3，Oracle A4）：
	// 面板可在等锁期间把同一代理 ID 的 URL 从 U1 改成 U2——绑定 ID 不变
	// 但物理出口已撤换。轮转外发前必须核当前原子快照 URL 与本值一致，
	// 不一致即停止自愈（绝不向已撤换旧 URL 发出带会话请求）。
	pickedProxyURL string
}

// pickedExitValidForCur 复核自愈 picked 出口（pickedProxyID 解析出的
// 出口 URL）对当前 DB 行 cur 是否仍合法（R10-3，Reviewer M4）。
// 调用前提：cur.ProxyID == expectedDBProxyID（绑定未变已另行核对）。
//   - cur 锁直连：仅允许直连 picked；
//   - cur 绑定代理：严格模式只认精确一致；非严格模式允许溢出到池中
//     启用代理，且 FallbackDirect=true 时允许合法直连（唯一绑定代理
//     启用但并发槽满时 acquireSlot 合法选直连、xsrfExitConflict 放行——
//     此前被误当「改绑」拒发）
//   - cur 未绑定/绑定已失效：池禁直连时出口必须是池中启用代理；
//     否则允许直连或池中启用代理。
func pickedExitValidForCur(proxyURL string, cur *CookieAccount) bool {
	switch {
	case cur.ProxyID == proxyIDDirect:
		return proxyURL == ""
	case cur.ProxyID > 0 && proxyEnabledByID(cur.ProxyID):
		if proxyURLByID(cur.ProxyID) == proxyURL && proxyURL != "" {
			return true
		}
		if !rtCfg().StrictProxyBinding {
			if proxyURL != "" && poolProxyURLEnabled(proxyURL) {
				return true
			}
			// R10-3：非严格 + FallbackDirect + 绑定代理仍启用（槽满合法
			// 直连自愈）：picked=0 的直连出口合法放行，绝不误当改绑。
			if proxyURL == "" && rtCfg().FallbackDirect {
				return true
			}
		}
		return false
	default:
		// 未绑定，或绑定已失效（删除/禁用——失效绑定不构成出口承诺，
		// 账号按未绑定规则裁决）。
		if proxyURL == "" {
			return !(hasProxyPool() && !rtCfg().FallbackDirect)
		}
		return poolProxyURLEnabled(proxyURL)
	}
}

// rotateAccountWithOpts 是 rotateAccount 的完整选项版（R10-3，Reviewer
// M4）：opts.skipIfActive=true 时若该号已有活跃 BrowserContext（或同
// cookieIdentity 兄弟号活跃）直接跳过，避免双端抢刷。仅后台
// rotateAllAccounts 传 true；healXSRFByRotation 与 Admin POST /rotate 传
// false（管理/自愈动作必须真执行）。
//
// 出口语义（R10-3 彻底分离两类，a.ProxyID 不再混用）：
//   - 普通轮转（面板/后台，hasPickedProxy=false）：等 L1 拿到 cur 后
//     **直接按最新 cur 的绑定重新解析出口**（单次原子快照），绝不沿用
//     等待前的旧快照出口——等待窗口内管理员改绑后，保活流量跟着新
//     绑定走，对旧出口零发送；
//   - 自愈轮转（hasPickedProxy=true）：先核对 DB 绑定是否仍等于
//     expectedDBProxyID（真实改绑立即 fail-closed 拒发），再复核
//     pickedProxyID 解析出的出口对当前 cur 是否合法（pickedExitValidForCur
//     ——严格精确一致 / 非严格溢出 / FallbackDirect 合法直连放行）。
//
// Cookie 串行锁：持 acctCookieLock 与 buildAccountCtx 播种 /
// syncAccountCookiesBackCtx 写回 / updateAccountCookie 严格互斥；写库走
// updateAccountCookieCASLocked（merged==cur 不发 UPDATE）。真实写库成功后
// BumpAccountEpoch + 放锁 InvalidateAccount（cookie rotated）。
//
// 返回值语义（不吞错）：
//   - 1PSIDTS 401/403（Chrome DBSC 设备绑定）绝不静默吞掉：即便 SIDCC 顺带
//     刷新落库，仍返回非 nil 错误（附注本次仅刷新了哪些名）；
//   - 双路均无有效刷新（len(names)==0）返回非 nil 错误（含跳过的原因）；
//   - 仅确有有效刷新时才返回 err==nil。
func rotateAccountWithOpts(a CookieAccount, opts rotateOpts) (time.Duration, []string, error) {
	// 管理/自愈强制保活（skipIfActive=false）且活跃号驻留浏览器时：进 L1 锁前
	// 先把浏览器 Jar 最新 *SIDCC 收回 DB，防止 DB 旧快照抹掉浏览器刚刷的值。
	if !opts.skipIfActive && shouldUseBrowserTransport() {
		browserMgr.SyncAccountCookiesBack(a.ID, true)
	}

	// Cookie 串行锁内重读 cur：并发懒建 Context / 并发回写交错时按
	// 最新库值轮转，消除重入自死锁与陈旧快照覆写。
	unlock, err := acquireAcctCookieLock(context.Background(), a.ID)
	if err != nil {
		return 0, nil, err
	}
	cur := accountByID(a.ID)
	if cur == nil || cur.Status != "enabled" {
		unlock()
		return 0, nil, fmt.Errorf("账号 #%d 已删除或停用", a.ID)
	}
	// R10-3（Reviewer M4）：出口在等锁/同步 Jar 之后按语义来源确定——
	// 普通轮转按最新 cur 重算；自愈复核 picked 出口对 cur 的合法性。
	var proxyURL string
	if opts.hasPickedProxy {
		// 自愈轮转：先核对 DB 绑定是否真实变化——picked 出口仅在
		// picking 阶段核过的那条绑定事实下被授权，真实改绑一律拒发。
		if cur.ProxyID != opts.expectedDBProxyID {
			unlock()
			return 0, nil, fmt.Errorf("账号 #%d 等待窗口内出口绑定已变（#%d -> #%d），本轮已选出口不再合法，停止保活（fail-closed 不走错出口）",
				cur.ID, opts.expectedDBProxyID, cur.ProxyID)
		}
		// picked 出口解析走单次原子快照：pickedProxyID>0 时代理已删/
		// 空 URL 绝不拼出 ("", nil) 静默直连；0 即显式直连。
		if opts.pickedProxyID > 0 {
			found, enabled, url := proxyAtomicSnapshot(opts.pickedProxyID)
			if !found || url == "" {
				unlock()
				return 0, nil, fmt.Errorf("账号 #%d 本轮已选出口 #%d 已删除或不可用，停止保活（fail-closed 不直连）",
					cur.ID, opts.pickedProxyID)
			}
			_ = enabled // 槽满合法溢出的代理可能处于任何状态；合法性由下方复核裁决
			// R11-3（Oracle A4）：代理 ID 不变但 URL 已被面板从 U1 改为 U2
			// 时，picked 快照指向的物理出口已撤换——本轮自愈携带的会话绝不
			// 能经 U2 换发后再用旧 U1 重取 /app。URL 与挑中时刻的原子快照
			// 不一致即停止自愈（绝不向已撤换旧 URL 发出带会话请求）；同时
			// 存量非法 URL 在使用侧明确拒绝（R11-4）。
			if url != opts.pickedProxyURL {
				unlock()
				return 0, nil, fmt.Errorf("账号 #%d 本轮已选出口 #%d 的 URL 已变更（挑中时刻 %q，当前 %q），停止自愈（fail-closed 不向已撤换旧出口发会话请求）",
					cur.ID, opts.pickedProxyID, redactProxyURL(opts.pickedProxyURL), redactProxyURL(url))
			}
			if err := validateProxyURL(url); err != nil {
				unlock()
				return 0, nil, fmt.Errorf("账号 #%d 本轮已选出口 #%d 的 URL 非法（%v），停止保活（fail-closed 不直连）",
					cur.ID, opts.pickedProxyID, err)
			}
			proxyURL = url
		}
		if !pickedExitValidForCur(proxyURL, cur) {
			unlock()
			return 0, nil, fmt.Errorf("账号 #%d 本轮已选出口 %q 对当前绑定 #%d 不再合法，停止保活（fail-closed 不走错出口）",
				cur.ID, redactProxyURL(proxyURL), cur.ProxyID)
		}
	} else {
		// 普通轮转：按等锁后的最新 cur 绑定重新解析出口（原子快照），
		// 绝不沿用等待前的旧快照出口——等待窗口内管理员改绑后保活
		// 流量跟随新绑定，对旧出口零发送。
		proxyURL, err = rotateProxyURL(*cur)
		if err != nil {
			unlock()
			return 0, nil, err
		}
	}
	if opts.skipIfActive && shouldUseBrowserTransport() &&
		browserMgr.HasActiveAccountOrIdentity(a.ID, cookieIdentity(cur.Cookie)) {
		unlock()
		return defaultRotateInterval, nil, nil
	}

	// R12-3（Oracle M3/Reviewer #4）自愈每步独立外发复核：自愈轮转不是
	// 一次外发——1PSIDTS POST 后 SIDCC 还要新发页面 GET 与票据 POST。
	// 首发期间面板同 ID 换 URL/禁用/改绑后，后续每一发都必须用同一
	// picked 契约复核（found && enabled && url == pickedProxyURL，直连
	// picked 核对直连仍合法），不满足立即 fail-closed 停止，绝不向已
	// 撤换或已禁用的旧 URL 发起新请求。普通轮转（hasPickedProxy=false）
	// 为 nil——出口在拿锁后按最新 cur 重算，不受 picked 快照约束。
	var recheckPicked func() error
	if opts.hasPickedProxy {
		recheckPicked = func() error {
			row := accountByID(a.ID)
			if row == nil || row.Status != "enabled" {
				return fmt.Errorf("账号 #%d 轮转期间已删除或停用，停止后续外发（fail-closed）", a.ID)
			}
			if row.ProxyID != opts.expectedDBProxyID {
				return fmt.Errorf("账号 #%d 轮转期间出口绑定已变（#%d -> #%d），停止后续外发（fail-closed 不走错出口）",
					a.ID, opts.expectedDBProxyID, row.ProxyID)
			}
			if opts.pickedProxyID > 0 {
				found, enabled, url := proxyAtomicSnapshot(opts.pickedProxyID)
				if !found || !enabled || url == "" {
					return fmt.Errorf("账号 #%d 本轮已选出口 #%d 已删除或禁用，停止后续外发（fail-closed 不向已禁用旧出口发会话请求）",
						a.ID, opts.pickedProxyID)
				}
				if url != opts.pickedProxyURL {
					return fmt.Errorf("账号 #%d 本轮已选出口 #%d 的 URL 已变更，停止后续外发（fail-closed 不向已撤换旧出口发会话请求）",
						a.ID, opts.pickedProxyID)
				}
				if err := validateProxyURL(url); err != nil {
					return fmt.Errorf("账号 #%d 本轮已选出口 #%d 的 URL 非法（%v），停止后续外发（fail-closed 不直连）",
						a.ID, opts.pickedProxyID, err)
				}
				if !pickedExitValidForCur(url, row) {
					return fmt.Errorf("账号 #%d 本轮已选出口 %q 对当前绑定 #%d 不再合法，停止后续外发（fail-closed 不走错出口）",
						a.ID, redactProxyURL(url), row.ProxyID)
				}
				return nil
			}
			// pickedProxyID == 0：核对当前直连仍合法（锁直连/未绑定且池规
			// 则允许直连/非严格 FallbackDirect 溢出直连同口径）。
			if !pickedExitValidForCur("", row) {
				return fmt.Errorf("账号 #%d 本轮已选直连出口对当前绑定 #%d 不再合法，停止后续外发（fail-closed）",
					a.ID, row.ProxyID)
			}
			return nil
		}
	}

	cookie := cur.Cookie
	var names []string
	var psidtsErr, sidccErr error
	var skipWhy string
	interval := defaultRotateInterval

	c2, _, n, why, err := tryRotate1PSIDTSRawRecheck(a.ID, cookie, proxyURL, recheckPicked)
	skipWhy = why
	if err != nil {
		psidtsErr = err
		logf("[rotate] 账号 #%d 刷新 1PSIDTS 失败: %v", a.ID, err)
	} else {
		cookie = c2
		names = append(names, n...)
	}

	if left := sidccBackoffLeft(a.ID); left > 0 {
		sidccErr = fmt.Errorf("SIDCC 保活被限流，%s 后再试", left.Round(time.Second))
	} else {
		c3, iv, n2, err := rotateSIDCCRecheck(cookie, proxyURL, recheckPicked)
		var rl *rotateRateLimitedError
		switch {
		case errors.As(err, &rl):
			sidccErr = err
			noteSIDCCBackoff(a.ID, rl.RetryAfter)
			logf("[rotate] 账号 #%d SIDCC 保活被限流（RotateCookies 429），%s 内不再请求",
				a.ID, rl.RetryAfter)
		case err != nil:
			sidccErr = err
			logf("[rotate] 账号 #%d SIDCC 保活失败: %v", a.ID, err)
		default:
			cookie = c3
			names = append(names, n2...)
			if iv > 0 {
				interval = iv
			}
		}
	}

	names = uniqueKeepOrder(names)
	// R7-8（Reviewer SF-1）：逐步名清单是两步换发的并集，不是最终落库的
	// 净变化——GET 轮转页把 SIDCC 从 x 改成 y、POST 又改回 x 时两步均入
	// 名单，但最终串等于原串（无 UPDATE），按名单返回就是净零变化谎报。
	// 此处按最初 cur.Cookie 与最终待写 cookie 逐名过滤：仅保留最终值相对
	// 最初值确实变化的票据名；净零变化时 names=nil（连同「本次仅刷新了」
	// 附注一起消失）。删除指令合并保留旧值的规则不变。
	if cookie == cur.Cookie {
		names = nil
	} else {
		net := names[:0]
		for _, n := range names {
			// R11-8（Oracle B2）：净变化判定按完整有序值列表及存在性裁决
			// ——重名尾项的真实更新（`SIDCC=n; SIDCC=y` -> 两项皆 n）绝不
			// 被首值比对误判为净零；同时修复下方还原分支：名单非空即证明
			// 有真实变化，绝不能回滚整串。
			if !cookieNameValuesEqual(cookie, cur.Cookie, n) {
				net = append(net, n)
			}
		}
		names = net
		// R8-8（Reviewer SF-x）：非规范分隔符/无意义键序差异的裸串
		//（如 "a=1;b=2;" vs "a=1; b=2"——仅分隔符空格或键序差别，逐名
		// 比对完全相同）在净零变化下绝不能整串覆写：本处已无真实净变化，
		// 原样写库只会触发无谓 UPDATE + BumpAccountEpoch + InvalidateAccount
		// 拆存活槽，之后又被轮转按原串原样带出去（没有任何凭据语义变化）。
		// 复位回 cur.Cookie——值层面零差异即串层面零差异，CAS/写库路径与
		// 「完全无变化」完全同构。
		if len(names) == 0 {
			cookie = cur.Cookie
		}
	}
	written := false
	if cookie != cur.Cookie {
		written = updateAccountCookieCASLocked(a.ID, cur.Cookie, cookie)
	}
	// MF-6：真实写库成功后先在 acctCookieLock（L1）内 BumpAccountEpoch，
	// 杜绝新刷 Cookie 与并发回写交错时代际漂移；再放锁，最后 Invalidate。
	// R2-NH-3：Bump 是纯内存 O(1) 代际标记，无条件推进（不以
	// shouldUseBrowserTransport 门控，封死刚关开关时与在飞停机写回的窗口）；
	// 仅 InvalidateAccount 仍按开关门控（关开关时没有槽可摘）。
	bump := written
	if bump {
		browserMgr.BumpAccountEpoch(a.ID)
	}
	unlock()
	if written {
		invalidateXSRF(cur.Cookie)
		if bump && shouldUseBrowserTransport() {
			browserMgr.InvalidateAccount(a.ID, false, "cookie rotated")
		}
	}
	// R5-7（Reviewer SF-1）：先拿到 CAS 落库结果再决定打不打成功日志——
	// 仅在 written==true（票据真实落库）时才记录「刷新了」：CAS 未命中
	// 时运维看到的绝不能是与事实相反的成功记录。
	if written && len(names) > 0 {
		logf("[rotate] 账号 #%d 刷新了 %s", a.ID, strings.Join(names, ", "))
	}
	// R4-7（Reviewer SF-1）：确有新票据合并（cookie != cur.Cookie）但 CAS
	// 明确未落库（上游 I/O 期间账号被手动停用或并发身份变更，
	// status='enabled' 条件 0 行命中）——绝不能按上游头部名清单谎报
	// refreshed / ok:true：返回非 nil 错误且 names=nil（未落库的票据
	// 不算已刷新）。
	// R5-7：双失败（1PSIDTS 401/403 同时 SIDCC 换发未落库）时绝不提前
	// 吞掉鉴权错误——以 %w 包装 psidtsErr 保留 401/403 鉴权原因
	//（isPSIDTSAuthErr 沿链仍可识别），同时点名 CAS 未落库原因。
	if cookie != cur.Cookie && !written {
		if isPSIDTSAuthErr(psidtsErr) {
			return 0, nil, fmt.Errorf("%w；且账号 #%d 保活票据未落库（CAS 未命中：账号可能已停用或并发变更），本轮换发不作废上游票据但绝不谎报已刷新",
				psidtsErr, a.ID)
		}
		return 0, nil, fmt.Errorf("账号 #%d 保活票据未落库（CAS 未命中：账号可能已停用或并发变更），本轮换发不作废上游票据但绝不谎报已刷新", a.ID)
	}
	// 规则 1：DBSC / 1PSID 鉴权失效绝不静默吞错——SIDCC 顺带刷新的落库照做
	//（不丢票），但返回非 nil 错误让 DBSC 401 对管理员/日志透明可见。
	if isPSIDTSAuthErr(psidtsErr) {
		if len(names) > 0 {
			return interval, names, fmt.Errorf("%w（本次仅刷新了 %s）", psidtsErr, strings.Join(names, ", "))
		}
		return 0, nil, psidtsErr
	}
	// 规则 2：双路均无有效刷新返回非 nil 错误（走到此处必为真实执行轮转——
	// skipIfActive 命中活跃槽已在上方提前返回）。
	if len(names) == 0 {
		if psidtsErr != nil {
			return 0, nil, psidtsErr
		}
		if sidccErr != nil {
			return 0, nil, sidccErr
		}
		return 0, nil, fmt.Errorf("保活未刷新任何 Cookie（%s）", nonEmptyOr(skipWhy, "上游未下发新票据"))
	}
	// 规则 3：确有有效刷新。
	return interval, names, nil
}

// nonEmptyOr 返回首个非空串。
func nonEmptyOr(s, fallback string) string {
	if s != "" {
		return s
	}
	return fallback
}

// testHookBeforeHotInjectCommit 仅供测试注入（生产恒为 nil）：在
// rotateActiveOrSibling1PSIDTS 的二次授权复核完成之后、prepareHotInject
// 提交判定之前执行一次。存取经原子容器——钩子读取发生在轮转协程，与
// 测试的安装/复位并发交错是测试结构的固有行为；生产恒 nil 时零开销、
// 零行为影响。
var testHookBeforeHotInjectCommit atomic.Value // func(accountID int64)

func loadTestHookBeforeHotInjectCommit() func(accountID int64) {
	if v := testHookBeforeHotInjectCommit.Load(); v != nil {
		return v.(func(accountID int64))
	}
	return nil
}

// psidtsAuthError 是 RotateCookies 哨兵换发 1PSIDTS 返回 401/403 的结构化错误
// （Chrome DBSC 设备绑定限制的典型信号）。错误文本保持原样（兼容性断言按
// "HTTP 401" 子串匹配）。
type psidtsAuthError struct {
	Status  int
	Snippet string
}

func (e *psidtsAuthError) Error() string {
	return fmt.Sprintf("RotateCookies 1PSIDTS 返回 HTTP %d（Chrome 导出的 cookie 可能受设备绑定限制，建议用 Firefox 重新导出）: %s",
		e.Status, e.Snippet)
}

// isPSIDTSAuthErr 判断错误链里有没有 1PSIDTS 鉴权失效（401/403）。
func isPSIDTSAuthErr(err error) bool {
	var pae *psidtsAuthError
	return errors.As(err, &pae)
}

// tryRotate1PSIDTS 用哨兵 payload 换发 1PSIDTS（薄包装，保留原签名兼容）。
func tryRotate1PSIDTS(id int64, cookie, proxyURL string) (string, []string, error) {
	merged, _, names, _, err := tryRotate1PSIDTSRaw(id, cookie, proxyURL)
	return merged, names, err
}

// tryRotate1PSIDTSRaw 换发 1PSIDTS 的完整返回版（无逐步复核的普通轮转入口）。
func tryRotate1PSIDTSRaw(id int64, cookie, proxyURL string) (merged string, setCookie []string, names []string, skipWhy string, err error) {
	return tryRotate1PSIDTSRawRecheck(id, cookie, proxyURL, nil)
}

// tryRotate1PSIDTSRawRecheck 换发 1PSIDTS 的完整返回版：merged 是合并后的整串、
// setCookie 是上游原始下发切片（活跃槽热注入共享复用）、skipWhy 说明未真正
// 发起换发的原因（无 1PSID / 60s 节流）。没 __Secure-1PSID 或距上次不足
// 60 秒就跳过（不算失败，err==nil）。
//
// R12-3（Oracle M3）：recheck 非 nil（自愈模式）时在外发前执行一次 picked
// 契约复核——出口已撤换/禁用/改绑立即 fail-closed 返回错误，绝不向旧 URL
// 发起本次外发。普通轮转传 nil（出口按最新行重算，无 picked 快照约束）。
//
// R12-6（Oracle S2/Reviewer #8）：缺票预检改用「首个非空值」口径——
// `__Secure-1PSID=`（首项空）后跟有效第二项时，cookieSubset 实际带出的与
// cookieIdentity 裁决的都是第二项，预检绝不能按只取首项的 cookieValue
// 误判缺失而整轮静默跳过。
func tryRotate1PSIDTSRawRecheck(id int64, cookie, proxyURL string, recheck func() error) (merged string, setCookie []string, names []string, skipWhy string, err error) {
	if cookieFirstNonEmptyValue(cookie, "__Secure-1PSID") == "" {
		return cookie, nil, nil, "cookie 里没有 __Secure-1PSID", nil
	}
	if ok, _ := allow1PSIDTSRotate(id); !ok {
		logf("[rotate] 账号 #%d 跳过 1PSIDTS 刷新（距上次不足 %s，避免 429）", id, min1PSIDTSInterval)
		return cookie, nil, nil, fmt.Sprintf("距上次 1PSIDTS 换发不足 %s（防 429 节流）", min1PSIDTSInterval), nil
	}
	if recheck != nil {
		if rerr := recheck(); rerr != nil {
			return cookie, nil, nil, "", rerr
		}
	}
	c2, sc, n, err := rotate1PSIDTSRaw(cookie, proxyURL)
	note1PSIDTSAttempt(id, err)
	if err != nil {
		return cookie, nil, nil, "", err
	}
	return c2, sc, n, "", nil
}

// rotate1PSIDTS 是 rotate1PSIDTSRaw 的薄包装（保留原签名兼容测试）。
func rotate1PSIDTS(cookie, proxyURL string) (string, []string, error) {
	merged, _, names, err := rotate1PSIDTSRaw(cookie, proxyURL)
	return merged, names, err
}

// rotate1PSIDTSRaw POST 哨兵 payload，返回合并整串 + 上游原始 Set-Cookie
// 切片 + 刷新名清单。401/403 归 *psidtsAuthError（DBSC 设备绑定信号）。
func rotate1PSIDTSRaw(cookie, proxyURL string) (string, []string, []string, error) {
	subset := cookieSubset(cookie, rotate1PSIDTSCookies)
	headers := map[string]string{
		"Accept":         "*/*",
		"Content-Type":   "application/json",
		"Origin":         "https://accounts.google.com",
		"Cookie":         subset,
		"Cache-Control":  "no-cache",
		"Pragma":         "no-cache",
		"Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "cors",
		"Sec-Fetch-Site": "same-origin",
	}
	status, setCookie, respBody, err := rotatePost(rotatePostURL, headers, []byte(rotate1PSIDTSBody), proxyURL)
	if err != nil {
		return cookie, nil, nil, err
	}
	if status == 401 || status == 403 {
		return cookie, nil, nil, &psidtsAuthError{Status: status, Snippet: truncate(string(respBody), 120)}
	}
	if status != 200 {
		return cookie, nil, nil, fmt.Errorf("RotateCookies 1PSIDTS 返回 HTTP %d: %s", status, truncate(string(respBody), 120))
	}
	merged := mergeSetCookie(cookie, setCookie)
	// R6-6（Reviewer SF-1）：仅记录实际改变了 cookie 串的头名——被
	// 忽略的删除指令头/空值/与现值相同的名绝不进入对外 refreshed
	// 名单，防止空转在面板谎报「刷新了」。
	return merged, setCookie, effectiveSetCookieNames(cookie, merged, setCookie), nil
}

func allow1PSIDTSRotate(id int64) (bool, time.Duration) {
	psidtsMu.Lock()
	defer psidtsMu.Unlock()
	last := psidtsLastAt[id]
	if last.IsZero() {
		return true, 0
	}
	elapsed := time.Since(last)
	if elapsed >= min1PSIDTSInterval {
		return true, 0
	}
	return false, min1PSIDTSInterval - elapsed
}

func note1PSIDTSAttempt(id int64, err error) {
	// 成功、401/403、429 都记时间，避免紧接着再打。网络错误不记，允许立刻重试。
	if err != nil {
		msg := err.Error()
		if !strings.Contains(msg, "HTTP 401") &&
			!strings.Contains(msg, "HTTP 403") &&
			!strings.Contains(msg, "HTTP 429") {
			return
		}
	}
	psidtsMu.Lock()
	psidtsLastAt[id] = time.Now()
	psidtsMu.Unlock()
}

// rotateSIDCC 走浏览器 iframe 那条：GET 轮转页拿会话 id，再 POST [658, id]。
func rotateSIDCC(cookie, proxyURL string) (string, time.Duration, []string, error) {
	return rotateSIDCCRecheck(cookie, proxyURL, nil)
}

// rotateSIDCCRecheck 是 rotateSIDCC 的逐步复核版（R12-3，Oracle M3）：
// recheck 非 nil（自愈模式）时在 GET /RotateCookiesPage 与 POST
// /RotateCookies **每一次独立外发前**各执行一次 picked 契约复核——页面
// GET 成功期间面板同 ID 换 URL/禁用后，票据 POST 绝不再向已撤换或已禁
// 用的旧 URL 发起。复核失败立即 fail-closed 返回错误（已合并的页面增量
// 随 cookie2 返回供调用方决定是否落库，但绝不发出后续请求）。
func rotateSIDCCRecheck(cookie, proxyURL string, recheck func() error) (string, time.Duration, []string, error) {
	if recheck != nil {
		if rerr := recheck(); rerr != nil {
			return cookie, 0, nil, rerr
		}
	}
	id, interval, pageSet, err := fetchRotateParams(cookie, proxyURL)
	if err != nil {
		return cookie, 0, nil, err
	}
	cookie2 := mergeSetCookie(cookie, pageSet)
	// R6-6（Reviewer SF-1）：仅记录各步合并中实际改变了 cookie 串的
	// 头名——被忽略的删除指令头（如 SIDCC=deleted; Max-Age=0）/空值/
	// 与现值相同的名绝不进入对外 refreshed 名单。
	names := effectiveSetCookieNames(cookie, cookie2, pageSet)

	if recheck != nil {
		if rerr := recheck(); rerr != nil {
			return cookie2, 0, nil, rerr
		}
	}
	body := fmt.Sprintf(`[%d,"%s"]`, rotateProductID, id)
	headers := rotatePostHeaders()
	headers["Cookie"] = cookie2
	status, setCookie, respBody, err := rotatePost(rotatePostURL, headers, []byte(body), proxyURL)
	if err != nil {
		return cookie2, 0, nil, err
	}
	if status == 429 {
		return cookie2, 0, nil, &rotateRateLimitedError{RetryAfter: rotateRetryAfter(respBody)}
	}
	if status != 200 {
		return cookie2, 0, nil, fmt.Errorf("RotateCookies 返回 HTTP %d: %s", status, truncate(string(respBody), 120))
	}
	merged := mergeSetCookie(cookie2, setCookie)
	names = append(names, effectiveSetCookieNames(cookie2, merged, setCookie)...)
	return merged, interval, names, nil
}

// rotateRateLimitedError 是 RotateCookies 回 429。实测响应体
// `)]}'\n\n[["identity.hfcr",600],["di",12]]`，600 当作要等的秒数（跟页面给的轮转间隔同值）。
type rotateRateLimitedError struct{ RetryAfter time.Duration }

func (e *rotateRateLimitedError) Error() string {
	return fmt.Sprintf("RotateCookies 返回 HTTP 429（%s 后再试）", e.RetryAfter)
}

var rotateRetryAfterRe = regexp.MustCompile(`"identity\.hfcr"\s*,\s*([0-9]+)`)

// rotateRetryAfter 从 429 响应体里取等待秒数，取不到或越界就用默认轮转间隔。
func rotateRetryAfter(body []byte) time.Duration {
	if m := rotateRetryAfterRe.FindSubmatch(body); m != nil {
		if sec, err := strconv.Atoi(string(m[1])); err == nil && sec >= 60 && sec <= 3600 {
			return time.Duration(sec) * time.Second
		}
	}
	return defaultRotateInterval
}

var (
	sidccMu       sync.Mutex
	sidccResumeAt = map[int64]time.Time{}
)

func noteSIDCCBackoff(id int64, d time.Duration) {
	sidccMu.Lock()
	sidccResumeAt[id] = time.Now().Add(d)
	sidccMu.Unlock()
}

// sidccBackoffLeft 返回还要等多久，不在退避中返回 0。
func sidccBackoffLeft(id int64) time.Duration {
	sidccMu.Lock()
	defer sidccMu.Unlock()
	if left := time.Until(sidccResumeAt[id]); left > 0 {
		return left
	}
	return 0
}

func uniqueKeepOrder(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// effectiveSetCookieNames 返回合并前后 cookie 串中实际发生了变化的
// 头名（R6-6）：被 mergeSetCookie 忽略的删除指令头、空值以及与现值
// 相同的重发都不计入——对外 refreshed 名单只按实际合并变化生成，绝不
// 因上游下发了被忽略的头而谎报刷新。
// R11-8（Oracle B2）：比对按完整有序值列表（cookieNameValuesEqual，与
// cookieValuesEqual 同口径）——原行 `SIDCC=n; SIDCC=y`、上游统一刷成
// `SIDCC=n` 时尾项 y 被真实更新为 n，只认首值会把这次真实更新误判为
// 无变化（名单丢失、净零还原整串，尾部旧票永留 DB）。
func effectiveSetCookieNames(before, after string, headers []string) []string {
	var out []string
	for _, n := range setCookieNames(headers) {
		if !cookieNameValuesEqual(after, before, n) {
			out = append(out, n)
		}
	}
	return out
}

// setCookieNames 把 Set-Cookie 头里的名字抽出来去重，只用于日志。
func setCookieNames(headers []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, h := range headers {
		name := h
		if i := strings.Index(name, "="); i > 0 {
			name = name[:i]
		}
		name = strings.TrimSpace(name)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}

// fetchRotateParams 抓 RotateCookiesPage，取会话标识、服务端指定的间隔，以及这一发
// 自己带回的 Set-Cookie。
func fetchRotateParams(cookie, proxyURL string) (string, time.Duration, []string, error) {
	status, setCookie, body, err := rotateGet(rotatePageURL, cookie, proxyURL)
	if err != nil {
		return "", 0, nil, err
	}
	if status != 200 {
		return "", 0, nil, fmt.Errorf("取轮转页返回 HTTP %d", status)
	}
	m := rotateInitRe.FindSubmatch(body)
	if m == nil {
		// 页面拿到了却没有 init(...)，最可能是 cookie 已失效跳到了登录页。
		return "", 0, nil, fmt.Errorf("轮转页里没有 init(...)（cookie 可能已失效）")
	}
	id := string(m[1])
	interval := defaultRotateInterval
	if sec, e := strconv.ParseFloat(string(m[3]), 64); e == nil && sec >= 60 && sec <= 3600 {
		interval = time.Duration(sec) * time.Second
	}
	return id, interval, setCookie, nil
}

// 下面两组 header 逐项抄自抓包（wireHeaders，不是 headers —— 后者不含 cookie）。
// 抓包里还有 sec-ch-ua-arch / -bitness / -form-factors / -full-version-list /
// -model / -platform-version / -wow64 和 x-browser-* / x-client-data /
// x-chrome-id-consistency-request，那些是 Chrome 自己贴的浏览器身份，我们贴了反而
// 会跟 TLS 指纹对不上，所以不贴。

// rotateGet 取轮转页。它在浏览器里是个 iframe 导航，所以 sec-fetch 那组跟普通
// XHR 完全不同（dest=iframe / mode=navigate / site=same-site），别套用默认值。
func rotateGet(url, cookie, proxyURL string) (int, []string, []byte, error) {
	return rotateDo("GET", url, map[string]string{
		"Cookie":                    cookie,
		"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
		"Referer":                   "https://gemini.google.com/",
		"Sec-Fetch-Dest":            "iframe",
		"Sec-Fetch-Mode":            "navigate",
		"Sec-Fetch-Site":            "same-site",
		"Sec-Fetch-User":            "?1",
		"Upgrade-Insecure-Requests": "1",
		"Priority":                  "u=0, i",
	}, nil, proxyURL)
}

// rotatePostHeaders 是 POST /RotateCookies 那一发的完整头，调用方只补 Cookie。
func rotatePostHeaders() map[string]string {
	return map[string]string{
		"Accept":         "*/*",
		"Content-Type":   "application/json",
		"Origin":         "https://accounts.google.com",
		"Referer":        rotatePageURL,
		"Cache-Control":  "no-cache",
		"Pragma":         "no-cache",
		"Priority":       "u=1, i",
		"Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "same-origin",
		"Sec-Fetch-Site": "same-origin",
	}
}

func rotatePost(url string, headers map[string]string, body []byte, proxyURL string) (
	int, []string, []byte, error) {
	return rotateDo("POST", url, headers, body, proxyURL)
}

// rotateDo 走跟正式请求同一个出口：保活从别的 IP 发，等于告诉上游这个会话在两处活动。
//
// 两条传输路径共用同一份 header。以前只有走代理那条调 applyChromeHeaders，直连那条
// 连 User-Agent 都不发 —— 同一个账号在上游看来会因为走没走代理而呈现两种客户端。
func rotateDo(method, url string, headers map[string]string, body []byte, proxyURL string) (
	int, []string, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = strings.NewReader(string(body))
	}
	// 同一发请求的 UA / Firefox 判据 / Sec-CH-UA 从同一份配置快照出（rtVal 可热更新，
	// 分次读会把两版配置拼进一个请求）。快照只读内存，不持锁跨网络调用。
	snap := snapshotBrowserHeaders()
	merged := map[string]string{
		"User-Agent":      snap.ua,
		"Accept-Language": "en-US,en;q=0.9",
	}
	for k, v := range snap.hints {
		merged[k] = v
	}
	for k, v := range headers {
		merged[k] = v
	}
	if proxyURL != "" {
		req, err := http.NewRequest(method, url, rdr)
		if err != nil {
			return 0, nil, nil, err
		}
		for k, v := range merged {
			req.Header.Set(k, v)
		}
		sc, cerr := getStdlibClient(proxyURL)
		if cerr != nil {
			return 0, nil, nil, cerr
		}
		resp, err := sc.Do(req)
		if err != nil {
			return 0, nil, nil, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Values("Set-Cookie"), b, err
	}
	req, err := fhttp.NewRequest(method, url, rdr)
	if err != nil {
		return 0, nil, nil, err
	}
	for k, v := range merged {
		req.Header.Set(k, v)
	}
	resp, err := getTLSClient().Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Values("Set-Cookie"), b, err
}

// redactProxyURL 只保留代理 URL 的 scheme://host——从解析后的 Scheme 与
// Host **重新构造**，彻底剥离 User/Path/RawPath/RawQuery/Fragment（R4-4：
// 池校验 proxy.go 接受带路径的 URL，Path 同样可能含私有信息）。无
// scheme/host 或解析失败返回 "<redacted>"，空串返回 "<direct>"。绝对
// 不允许凭据随周期保活告警/拒绝错误进入 logf 与 globalLogBuffer（R3-3）。
func redactProxyURL(raw string) string {
	if raw == "" {
		return "<direct>"
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "<redacted>"
	}
	return u.Scheme + "://" + u.Host
}

// poolProxyURLEnabled 报告 raw 是否精确等于代理池中某个**当前仍启用**代理的
// 实时 URL（合法溢出/借用快照放行判定用）。非池 URL 一律 false。
func poolProxyURLEnabled(raw string) bool {
	if raw == "" {
		return false
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.Enabled && p.URL == raw {
			return true
		}
	}
	return false
}

// resolveActiveRotateProxy 为活跃槽（或同身份兄弟号活跃）的保活定出口：
// 优先对齐活跃 BrowserContext 已锁定的出口（snap.proxyURL 非空时严格复用
// ——Go 侧 RotateCookies 必须与页内流量走同一 IP，杜绝非严格模式下双 IP
// 告警或 FallbackDirect 误直连）。**调用方必须已持该账号的 L1
// acctCookieLock 且 cur 是锁内重读的最新账号行**（fix #2）：快照核验以
// 当前账号事实与代理池实时状态为准，绝不拿调用方传入的陈旧快照做判定：
//
//   - 绑定新鲜度：snap.boundProxyID>0 时不仅校验代理 enabled，还必须核对
//     该代理当前实时 URL 与 snap.proxyURL 一致——快照后管理员把绑定代理
//     URL 从 Pold 改成 Pnew（仍 enabled）时旧快照立即失效，fail-closed 跳过；
//   - 账号授权一致性：锁直连（ProxyID==-1）绝不借走代理的快照、绑定了代理
//     的兄弟号绝不借直连/异代理快照；冲突判定跳过本轮，绝不违反锁直连或
//     代理绑定承诺；
//   - 未绑定账号（ProxyID==0）无固定代理授权承诺（Oracle MF-2）：绝不拿
//     不稳定的瞬时 firstUsableProxyURL() 做字符串相等比对——只要快照出口
//     是池中某个当前仍启用且实时 URL 一致的代理即放行；
//   - 自有活跃槽 + 非严格绑定的合法溢出（Reviewer MF-2）：绑定代理并发槽满
//     时 acquireSlot 合法改走其它健康代理（exitCompatCheck / autoBindAccountProxy
//     明确允许原绑定未变的溢出槽）——快照出口是池中启用代理即放行，沿槽
//     实际出口续期；严格模式仍精确匹配绑定代理；
//   - 直连快照（snap.proxyURL==""）不得静默回退到不同出口代理：仅在账号
//     允许直连（锁直连 / 未绑定且池规则允许直连）时使用；
//   - 无槽快照（hasSnap=false）时回退 rotateProxyURL 的 fail-closed 规则。
//
// R3-3 凭据红线：本函数返回的一切错误只含账号 ID / 代理 ID / 脱敏后
// scheme://host 的 URL——userinfo 用户名密码绝不进错误文本（代理 URL 支持
// Basic 认证凭据，logf 与 globalLogBuffer 无脱敏）。
func resolveActiveRotateProxy(cur *CookieAccount, snap activeSlotSnapshot, hasSnap bool) (string, error) {
	if !hasSnap {
		return rotateProxyURL(*cur)
	}
	// 绑定代理新鲜度核验：enabled 且实时 URL 与快照一致。
	if snap.boundProxyID > 0 {
		if !proxyEnabledByID(snap.boundProxyID) {
			return "", fmt.Errorf("账号 #%d 活跃槽绑定出口 #%d 已删除或禁用，跳过保活（fail-closed）",
				cur.ID, snap.boundProxyID)
		}
		if proxyURLByID(snap.boundProxyID) != snap.proxyURL {
			return "", fmt.Errorf("账号 #%d 活跃槽绑定出口 #%d 实时 URL 已变更（快照出口 %q），跳过保活（fail-closed）",
				cur.ID, snap.boundProxyID, redactProxyURL(snap.proxyURL))
		}
	}
	own := snap.accountID == cur.ID
	if snap.proxyURL == "" {
		// 直连快照：snap.allowDirect 为假时是自相矛盾的槽位状态（桥本身会拒绝
		// 一切外发），fail-closed 绝不使用；否则仅当账号允许直连时按直连使用，
		// 绝不静默回退到其它出口代理。
		if !snap.allowDirect {
			return "", fmt.Errorf("账号 #%d 快照为直连但槽位不允许直连（矛盾态），跳过保活（fail-closed）", cur.ID)
		}
		if cur.ProxyID == proxyIDDirect ||
			(cur.ProxyID == 0 && !(hasProxyPool() && !rtCfg().FallbackDirect)) {
			return "", nil
		}
		if !own {
			return "", fmt.Errorf("账号 #%d 不允许直连，跳过借用兄弟号直连快照（fail-closed）", cur.ID)
		}
		// 自有直连快照但账号现已不允许直连：不静默回退，fail-closed 跳过本轮。
		return "", fmt.Errorf("账号 #%d 自有快照为直连但账号不允许直连，跳过保活（fail-closed）", cur.ID)
	}
	// 走代理快照：锁直连账号绝不借用/复用（不违反锁直连承诺）。
	if cur.ProxyID == proxyIDDirect {
		return "", fmt.Errorf("账号 #%d 锁直连但快照走代理出口 %q，跳过保活（fail-closed 不违反锁直连承诺）",
			cur.ID, redactProxyURL(snap.proxyURL))
	}
	if cur.ProxyID > 0 {
		// R4-1 改绑过期拦截（自有槽）：取快照后管理员已改绑
		//（snap.origAccountProxyID != cur.ProxyID）时，快照事实整体过期——
		// 即便快照出口恰好等于新绑定代理，也绝不把过期快照当成当前账号的
		// 合法出口放行，与前台 readySlotExitViolation 同口径 fail-closed。
		// R5-1（Reviewer MF-1）：拦截仅限「建槽时已绑定」的快照——
		// snap.origAccountProxyID == 0 表示建槽时账号尚未绑定，此后非严格
		// 模式的合法首次自动绑定（0 -> 本槽实际出口 P）不是管理员改绑，
		// 交给下方「实时 URL 精确匹配」裁决：绑定代理当前实时 URL 等于
		// 快照出口即放行（合法自动绑到本槽出口），不等（管理员手工绑到
		// 别的代理）则 fall through 到绑定冲突 fail-closed 拒绝。
		if own && snap.origAccountProxyID > 0 && snap.origAccountProxyID != cur.ProxyID {
			return "", fmt.Errorf("账号 #%d 快照后已被管理员改绑（原绑定 #%d -> 当前 #%d），过期快照拒绝（fail-closed）",
				cur.ID, snap.origAccountProxyID, cur.ProxyID)
		}
		// R9-4（Reviewer MF-4）：orig==0 的自有槽「0 -> P 合法首绑」例外
		// 必须验证**可核的自动绑定来源**——冷建读出未绑定、槽尚未注册时
		// 管理员手工 PATCH 恰好绑到本槽出口 P：失效钩子此刻无槽可摘、
		// 步骤 6 对齐因记录被手工清除不生效、发布时 orig 仍是 0，仅凭
		// URL 相等放行会把这次手工改绑洗成合法首绑，后续续期与改绑拦
		// 截全被绕过。仅当存在自动绑定通道（autoBindAccountProxy/
		// claimAccountProxy）真实落库且登记的 (cur.ProxyID, 快照出口)
		// 来源记录时才视为合法首绑；手工 PATCH 清除记录后绝不命中，
		// fail-closed 跳过本轮。
		if own && snap.origAccountProxyID == 0 && proxyEnabledByID(cur.ProxyID) &&
			proxyURLByID(cur.ProxyID) == snap.proxyURL &&
			!isAutoBoundProxyForURL(cur.ID, cur.ProxyID, snap.proxyURL) {
			return "", fmt.Errorf("账号 #%d 建槽后 0->#%d 的绑定无可核自动绑定来源（疑似手工改绑恰落本槽出口），跳过保活（fail-closed）",
				cur.ID, cur.ProxyID)
		}
		// 绑定代理账号：快照精确指向其当前绑定的启用代理——无条件放行。
		if proxyEnabledByID(cur.ProxyID) && proxyURLByID(cur.ProxyID) == snap.proxyURL {
			return snap.proxyURL, nil
		}
		// R5-1：合法溢出同样以「建槽时已绑定且未改绑」为前提——
		// origAccountProxyID == 0 的首次自动绑定场景由上方精确匹配分支
		// 裁决（快照出口即绑定代理实时 URL 才放行），溢出宽限不适用。
		if own && !rtCfg().StrictProxyBinding && poolProxyURLEnabled(snap.proxyURL) &&
			snap.origAccountProxyID == cur.ProxyID {
			// 自有活跃槽 + 非严格绑定：绑定代理并发槽满时 acquireSlot 合法
			// 溢出到池里其它健康代理（autoBindAccountProxy 保留原绑定、
			// exitCompatCheck 明确放行）——快照出口对应池中某个当前仍启用
			// 且实时 URL 一致的代理即沿槽实际出口续期，绝不因「不等于绑定
			// 代理」整轮静默跳过保活（Reviewer MF-2）。兄弟号借用绝不享受
			// 溢出宽限：跨号借用异代理快照仍按绑定冲突 fail-closed。
			// R4-1 对齐前台 readySlotExitViolation 语义：合法溢出必须以
			// 「管理员未改绑」为前提——snap.origAccountProxyID != cur.ProxyID
			// 说明取快照后绑定已变，旧出口不再是当前账号的合法溢出，
			// 一律 fall through 到下方绑定冲突 fail-closed 拒绝。
			return snap.proxyURL, nil
		}
		return "", fmt.Errorf("账号 #%d 绑定出口 #%d 与快照代理出口 %q 不一致，跳过保活（fail-closed）",
			cur.ID, cur.ProxyID, redactProxyURL(snap.proxyURL))
	}
	// 未绑定账号（ProxyID==0）借用兄弟号走代理快照（Oracle MF-2）：未绑定
	// 账号从未承诺过任何固定出口，「授权出口」是池的瞬时状态而非账号属性——
	// 绝不拿 firstUsableProxyURL() 的瞬时结果做字符串相等比对（并发下必然
	// 假阴性导致整轮保活静默跳过）。只要快照出口是池中某个当前仍启用且
	// 实时 URL 一致的代理即放行：同身份兄弟号共享同一 Google 会话，跨号
	// 用同一稳定 IP 反而更安全。
	if poolProxyURLEnabled(snap.proxyURL) {
		return snap.proxyURL, nil
	}
	return "", fmt.Errorf("账号 #%d 借用兄弟号代理出口 %q 非池中启用代理，跳过保活（fail-closed）",
		cur.ID, redactProxyURL(snap.proxyURL))
}

// pickRotateCandidate 在候选快照列表里为账号 cur 选出本轮保活的合法
// 出口（L1 内调用，m.mu 出度 0；快照核验以锁内重读的最新 cur 与代理
// 池实时状态为准）。候选优先级按列表顺序（自有槽在前、同身份兄弟
// 号按 accountID 升序在后——R9-8 确定性挑选）。对每一候选：
//   - R4-1 过期快照复核：snap.bcID 对应的槽实例必须仍驻留且未摘除，
//     否则该快照的出口/身份事实已全部作废；
//   - R5-2 兄弟资格复核：按锁内重读的最新 cur 身份比对 snap.ident，
//     取快照后扩展可能已把本号换绑到另一会话；
//   - 出口授权复核：resolveActiveRotateProxy 对 cur 与实时代理池逐条
//     核验（改绑拦截/锁直连承诺/合法溢出等语义不动）。
//
// 首个全部通过的候选胜出；全部失败返回 ok=false（调用方必须跳过本
// 轮，绝不回退直连）。
func pickRotateCandidate(cur *CookieAccount, cands []activeSlotSnapshot) (string, activeSlotSnapshot, bool) {
	for _, snap := range cands {
		if !browserMgr.slotResidentForSnapshot(snap) {
			continue // 过期快照（已摘除/替换）
		}
		if snap.ident != "" && snap.ident != "|" && cookieIdentity(cur.Cookie) != snap.ident {
			continue // 身份不符（取快照后会话已换绑）
		}
		proxyURL, perr := resolveActiveRotateProxy(cur, snap, true)
		if perr != nil {
			continue // 出口授权冲突/过期——尝试下一候选
		}
		return proxyURL, snap, true
	}
	return "", activeSlotSnapshot{}, false
}

// psidtsOnlySetCookie 过滤增量 Set-Cookie，仅保留 __Secure-1PSIDTS 换发票：
// 热续期只注入这一个票据，上游响应里夹带的任意其它 Cookie 族（SIDCC/
// SAPISID/SID 等）绝不进活跃 Jar 或 DB 热路径——身份级凭据只能走 seed/
// 整号轮换的既有身份守卫通道。
//
// Oracle MF-3：白名单**刻意不含 __Secure-3PSIDTS**——cookieIdentity 与
// rotate1PSIDTSCookies 都只覆盖 1PSID 族而看不见 3PSID 族；同一浏览器
// profile 下不同 authuser 兄弟号（/u/0 vs /u/1）各自有独立的
// __Secure-3PSID，经 sharedPSIDTS 跨号复用会把 A 会话的 3PSIDTS 注入 B
// 的 DB/Jar（异身份串号触发账号关联风控），而身份守卫完全看不见它。
// 3PSIDTS 只服务于第三方嵌入场景，gemini.google.com 第一方会话不需要，
// 宁可不热注入也绝不跨号串票。
func psidtsOnlySetCookie(setCookie []string) []string {
	var out []string
	for _, sc := range setCookie {
		name, _, _ := strings.Cut(sc, "=")
		name = strings.TrimSpace(name)
		if name == "__Secure-1PSIDTS" {
			out = append(out, sc)
		}
	}
	return out
}

// rotateActiveOrSibling1PSIDTS 为活跃槽账号（或同 cookieIdentity 兄弟号活跃）
// 换发 1PSIDTS 并无损热注入活跃 BrowserContext Jar。调用方不持任何锁。
//
// 关键不变量：
//   - 同 cookieIdentity 多兄弟号共享同一次换发结果（sharedPSIDTS 本轮局部缓存，
//     避免同一秒重打同一个 __Secure-1PSID 触发 429，同时让各号 DB 与各自活跃槽
//     同步获得新票）；
//   - 全程逐号单拿单放各自 L1 acctCookieLock，绝不嵌套持两把不同 accountID 的锁；
//   - 热注入成功（injected）严禁 BumpAccountEpoch 与 InvalidateAccount——
//     槽位未拆毁，Bump 会让后续 jar 回写被 isSupersededEpoch 误判丢弃（回写黑洞），
//     Invalidate 会中断在途流；先写 Jar 再 CAS 写 DB 的顺序保证后续
//     syncAccountCookiesBackCtx 读出的已是新票（无竞态同步定理）；
//   - 唯当槽位已消失（slotGone）或 CDP 写入失败时才 Bump（+ 异常时 Invalidate）。
func rotateActiveOrSibling1PSIDTS(a CookieAccount, cands []activeSlotSnapshot, sharedPSIDTS map[string][]string) {
	unlock, err := acquireAcctCookieLock(context.Background(), a.ID)
	if err != nil {
		logf("[rotate] 账号 #%d 活跃槽保活取锁失败: %v", a.ID, err)
		return
	}
	cur := accountByID(a.ID)
	if cur == nil || cur.Status != "enabled" {
		unlock()
		return
	}
	// R9-8（Reviewer SF-x）：同身份多个活跃槽按候选列表逐个复核——优先
	// 选仍驻留、身份一致且出口满足本号授权的槽；A、B 同身份分别走
	// P1/P2 时，非驻留兄弟 C 绑 P1 可借到合法候选 A（绝不因切片里恰
	// 先排 B 就整轮跳过）。找不到任何合法候选必须跳过本轮，绝不可回
	// 退直连（fail-closed 语义不变）。
	proxyURL, chosen, ok := pickRotateCandidate(cur, cands)
	if !ok {
		logf("[rotate] 账号 #%d 活跃槽保活跳过：无合法驻留候选槽（过期/改绑/授权冲突 fail-closed）", a.ID)
		unlock()
		return
	}
	ident := cookieIdentity(cur.Cookie)

	var setCookies []string
	if ident != "|" && len(sharedPSIDTS[ident]) > 0 {
		// 同 Google 会话兄弟号：直接复用本轮刚换发的结果，零额外上游请求。
		setCookies = sharedPSIDTS[ident]
	} else {
		_, sc, _, why, rerr := tryRotate1PSIDTSRaw(a.ID, cur.Cookie, proxyURL)
		if rerr != nil {
			// Chrome DBSC 401 等失败首次在活跃槽下透明可见。
			logf("[rotate] 账号 #%d 活跃槽 1PSIDTS 换发失败: %v", a.ID, rerr)
			unlock()
			return
		}
		if len(sc) == 0 {
			if why != "" {
				logf("[rotate] 账号 #%d 活跃槽 1PSIDTS 本轮跳过（%s）", a.ID, why)
			}
			unlock()
			return
		}
		setCookies = sc
		if ident != "|" {
			sharedPSIDTS[ident] = setCookies
		}
	}

	// fix #6：热续期只注入明确允许的 *PSIDTS 票据族——上游原始 Set-Cookie
	// 里夹带的任意其它 Cookie 族绝不进活跃 Jar；且先合并出结果显式校验
	// 身份——身份有任何变化立即整体拒绝（既不调注入也不写 DB），绝不靠
	// 注入之后的 DB CAS 兜底（那时 Jar 已被污染成异身份）。
	psidtsOnly := psidtsOnlySetCookie(setCookies)
	if len(psidtsOnly) == 0 {
		unlock()
		return
	}
	merged := mergeSetCookie(cur.Cookie, psidtsOnly)
	if merged == cur.Cookie {
		unlock()
		return
	}
	if cookieIdentity(merged) != cookieIdentity(cur.Cookie) {
		logf("[rotate] 账号 #%d 活跃槽热续期合并结果身份不符，整体拒绝（不注入不写库）", cur.ID)
		unlock()
		return
	}
	// 持 L1 锁先热注入 Jar（先写 Jar 再写 DB，见无竞态同步定理）。
	// prepareHotInject 在单个 m.mu 临界区内原子完成「完全无槽 / creating 中 /
	// 已发布」分类与（非就绪时）BumpAccountEpoch（fix #3）：creating 槽写库并
	// Bump 后，旧构建会在发布守卫被 isSupersededEpoch 安全拦截销毁，绝不可能
	// 出现「存活旧 Jar + 新 DB + 永久 SIDCC 回写黑洞」。
	// R10-5（Reviewer M2/Oracle M2）：「借哪个出口」与「写哪个 BC」分离
	// 授权——自有槽的注入授权独立于出口候选的借用授权；自有槽被拒后借
	// 同身份兄弟出口的场景 ownAuthorized=false，新票绝不进刚被拒绝的自有
	// 槽（或换身份后待摘除的旧身份槽）。
	//
	// R12-2（Oracle M1/M2/Reviewer #3）授权与提交原子绑定：注入授权不再
	// 是一枚可过期的布尔值，而是「具体槽实例 + 出口同步事实版本」的绑定
	// 判定。换发拿到新票后、提交注入前重读**最新 DB 行**与驻留槽快照——
	// 行已停用直接判定本号不热注入（绝不向失格 BC 发新票）；随后把已核对
	// 的 (bcID, exitSyncVer) 绑进 prepareHotInject：m.mu 内驻留槽的实例与
	// 版本任一变化（配额改绑同步、停用摘除、槽位替换）都使授权作废，绝不
	// 凭过期判定注入。复核用实时三条件（与 R11-2 同口径）：
	//   ① 自有槽仍驻留已发布（bcID 精确匹配）；
	//   ② 槽身份与锁内身份一致；
	//   ③ 槽当前出口/来源仍合法（resolveActiveRotateProxy 对最新行的实时
	//      裁决——改绑拦截/锁直连承诺/可核自动来源/合法溢出同口径）。
	// 轮初 hasOwn 不再决定摘除与否：无论轮初为何值，提交时发现该账号存在
	// 已发布但未获准注入的驻留槽实例，都在 DB 更新后按具体 bcID 条件摘除；
	// 两次读之间刚发布且经核对完全合法的槽则直接热注入（绝不误 Bump）。
	ownVerified := false
	ownSnap, hasOwn := browserMgr.ownSlotSnapshotForInject(cur.ID)
	if hasOwn {
		identMatch := ident == "" || ident == "|" || ownSnap.ident == "" || ownSnap.ident == ident
		if identMatch {
			if freshRow := accountByID(cur.ID); freshRow != nil && freshRow.Status == "enabled" {
				if _, rerr := resolveActiveRotateProxy(freshRow, ownSnap, true); rerr == nil {
					ownVerified = true
				}
			}
		}
	}
	// 测试钩子（生产恒 nil）：在二次授权复核完成之后、prepareHotInject
	// 提交判定之前执行一次——回归测试借此确定性构造「复核通过后、提交
	// 注入前完成改绑/停用」的 Oracle M1 交错（修复前布尔授权对该交错
	// 完全不可见）。生产路径绝不经由它做任何判断，绝不改变生产行为。
	if hook := loadTestHookBeforeHotInjectCommit(); hook != nil {
		hook(cur.ID)
	}
	plan, client, bcID, detachBCID := browserMgr.prepareHotInject(
		cur.ID, ident, hotInjectBinding{
			verified:    ownVerified,
			bcID:        ownSnap.bcID,
			exitSyncVer: ownSnap.exitSyncVer,
			rebindGen:   ownSnap.rebindGen,
		})
	injected, injErr := false, error(nil)
	if plan == hotInjectReady {
		if err := injectCookiesToBC(context.Background(), client, bcID, psidtsOnly); err != nil {
			injErr = err
		} else {
			injected = true
		}
	}
	written := updateAccountCookieCASLocked(cur.ID, cur.Cookie, merged)
	// R12-2（Oracle M1/M2）：对**已发布但未获准注入**的驻留槽执行条件
	// 摘除——DB 已写入新票但该 BC 零注入时，无论轮初 hasOwn 为何值：
	// 失格槽（出口改绑/身份不符/账号已停用/授权被拒）或两次读之间刚
	// 发布却未能注入的槽，此后的一切回写都被代际拦截、下一轮热注入也
	// 永远被拒，是可授租却永久无法回写的僵尸槽。detachBCID 是
	// prepareHotInject 在 m.mu 提交临界区内核验到的**当前驻留槽实例**
	// （绑定事实过期/未授权的肇事实例），按它精确摘除——复核与摘除
	// 之间槽位被健康新槽替换时 bcID 不符，摘除自动放弃。injErr 路径
	// 沿用原有失败摘除，与此路径互斥（injected/injErr 时 plan 必为 ready）。
	detachIneligible := written && !injected && injErr == nil &&
		plan == hotInjectUnauthorized && detachBCID != ""
	bump := false
	switch {
	case injected:
		// 热注入成功：零 Bump、零 Invalidate——槽位存活，jar 已是新票。
	case injErr != nil:
		// 槽存在但 CDP 写入异常：Bump 拦截任何旧槽残留回写。
		bump = written
	default:
		// 无槽 / creating 中：Bump 已在 prepareHotInject 的同一 m.mu 临界区内
		// 原子完成（与分类无交错窗口），此处不再重复。
	}
	if bump {
		browserMgr.BumpAccountEpoch(cur.ID)
	}
	unlock()
	if written {
		invalidateXSRF(cur.Cookie)
		via := ""
		if chosen.accountID != cur.ID {
			via = fmt.Sprintf("（借同身份兄弟号 #%d 驻留出口）", chosen.accountID)
		}
		logf("[rotate] 账号 #%d 活跃槽 1PSIDTS 已换发%s%s", cur.ID,
			map[bool]string{true: "并热注入浏览器 Jar", false: "（热注入不可用，下次建槽生效）"}[injected], via)
	}
	if injErr != nil && written {
		logf("[rotate] 账号 #%d 活跃槽热注入失败，失效槽位重建: %v", cur.ID, injErr)
		// R4-2：以被注入的槽实例（bcID）为条件摘除——注入 I/O 期间旧槽
		// 可能已被替换为健康新槽，按 ID 无条件摘除会误拆新槽。
		browserMgr.InvalidateAccountIfBC(cur.ID, bcID, "1psidts hot-inject failed")
	}
	if detachIneligible {
		logf("[rotate] 账号 #%d 驻留槽未获准热注入（出口改绑/身份不符/授权被拒/绑定事实过期），条件摘除防僵尸槽", cur.ID)
		// R12-2（Oracle M1/M2）：未获准注入的已发布槽按提交时刻核验到的
		// 驻留实例 bcID 条件摘除——此后它的一切回写都被代际拦截、热注入
		// 永远被拒，是可授租却永久无法回写的僵尸槽；提交与摘除之间被健康
		// 新槽替换时 bcID 不符，摘除自动放弃。
		browserMgr.InvalidateAccountIfBC(cur.ID, detachBCID, "hot-inject target ineligible")
	}
}

// rotateAllAccounts 给池子里每个启用的账号做一次保活，返回下次该等多久。
//
// 失败**不计入健康度**：保活打的是 accounts.google.com，跟对话能不能用是两码事，
// 网络抖一下就把号标成坏的，会让它在挑号时沉底，反而伤可用性。
//
// v6.7.x 活跃槽续期：BrowserTransport 下活跃槽与同身份兄弟号不再整号跳过——
// 先把浏览器 Jar 最新 *SIDCC 收回 DB，再走 rotateActiveOrSibling1PSIDTS
// 单独换发 1PSIDTS 并 CDP 热注入（不跑 rotateSIDCC，页内 Jar 自动维护）。
func rotateAllAccounts() time.Duration {
	next := defaultRotateInterval
	// v6.6.0 多 Context：开头先淘汰空闲超 30m 的备号槽交回 Go rotateAccount 保活。
	var activeByID map[int64]activeSlotSnapshot
	var activeByIdent map[string][]activeSlotSnapshot
	if shouldUseBrowserTransport() {
		browserMgr.EvictIdleContexts()
		activeByID, activeByIdent = browserMgr.ActiveAccountSlotSnapshots()
		// 先把全部活跃槽 Jar 最新 *SIDCC 收回 DB，防止后续 Go 侧合并拿旧快照覆写。
		for id := range activeByID {
			browserMgr.SyncAccountCookiesBack(id, true)
		}
	}
	// 本轮同 cookieIdentity 共享换发结果缓存（兄弟号零重打）。
	sharedPSIDTS := map[string][]string{}
	for _, a := range accountList() {
		if a.Status != "enabled" {
			continue
		}
		var cands []activeSlotSnapshot
		if snap, selfActive := activeByID[a.ID]; selfActive {
			cands = append(cands, snap)
		}
		if shouldUseBrowserTransport() {
			if ident := cookieIdentity(a.Cookie); ident != "|" {
				for _, ssnap := range activeByIdent[ident] {
					if ssnap.accountID == a.ID {
						continue // 自有槽已在候选首位
					}
					cands = append(cands, ssnap)
				}
			}
		}
		if len(cands) > 0 {
			// 活跃槽或同身份兄弟号活跃：走无损热续期，不整号跳过。
			rotateActiveOrSibling1PSIDTS(a, cands, sharedPSIDTS)
			continue
		}
		iv, _, err := rotateAccountOpts(a, true)
		if iv > 0 {
			next = iv
		}
		if err != nil {
			logf("[rotate] 账号 #%d 保活异常: %v", a.ID, err)
			continue
		}
	}
	return next
}
