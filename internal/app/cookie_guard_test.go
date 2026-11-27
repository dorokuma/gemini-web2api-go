package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

// 连续鉴权失败要自动停用，否则死号会一直排在挑号队列里，
// 每个请求都得先为它付一次 XSRF 往返才轮到报错。
func TestAutoDisableAfterAuthFailures(t *testing.T) {
	id, err := accountAdd("dead", "SAPISID=aaa; __Secure-1PSID=bbb", "")
	if err != nil {
		t.Fatalf("插账号失败: %v", err)
	}
	t.Cleanup(func() { _ = accountDelete(id) })

	for i := 1; i < maxCookieAuthFailures; i++ {
		markCookieByStatus(id, 401, "no SNlM0e")
		if a := accountByID(id); a == nil || a.Status != "enabled" {
			t.Fatalf("第 %d 次失败就被停用了，太早", i)
		}
	}
	markCookieByStatus(id, 401, "no SNlM0e")
	if a := accountByID(id); a == nil || a.Status != "disabled" {
		t.Errorf("连续 %d 次鉴权失败后应自动停用, got %+v", maxCookieAuthFailures, a)
	}
}

// 非鉴权类失败不该累加 fail_count，更不该导致停用 ——
// 住宅出口退化率很高，把 302/网络错误算进去会让好 cookie 被误伤成"失败最多"。
func TestNonAuthFailureDoesNotDisable(t *testing.T) {
	id, err := accountAdd("noisy", "SAPISID=ccc; __Secure-1PSID=ddd", "")
	if err != nil {
		t.Fatalf("插账号失败: %v", err)
	}
	t.Cleanup(func() { _ = accountDelete(id) })

	for i := 0; i < maxCookieAuthFailures+2; i++ {
		markCookieByStatus(id, 302, "sorry page")
		markCookieByStatus(id, 0, "network error")
	}
	a := accountByID(id)
	if a == nil || a.Status != "enabled" || a.FailCount != 0 {
		t.Errorf("非鉴权失败不该动健康度, got %+v", a)
	}
}

// 成功要清零，否则偶发失败会累积到停用阈值。
func TestSuccessResetsFailCount(t *testing.T) {
	id, err := accountAdd("flappy", "SAPISID=eee; __Secure-1PSID=fff", "")
	if err != nil {
		t.Fatalf("插账号失败: %v", err)
	}
	t.Cleanup(func() { _ = accountDelete(id) })

	markCookieByStatus(id, 401, "x")
	markCookieByStatus(id, 200, "")
	markCookieByStatus(id, 401, "x")
	markCookieByStatus(id, 401, "x")
	if a := accountByID(id); a == nil || a.Status != "enabled" {
		t.Errorf("中间成功过就该清零，不应停用, got %+v", a)
	}
}

// 刷新结果的身份必须跟库里那份一致才写回。
// 不校验的话，上游若在响应里换了整套会话，A 号的凭据会被静默写进 B 号那一行 ——
// 面板上显示的还是原标签，实际发出去的却是别人的会话。
func TestUpdateAccountCookieIdentityGuard(t *testing.T) {
	orig := "SAPISID=keep; __Secure-1PSID=same; SIDCC=old"
	id, err := accountAdd("guarded", orig, "")
	if err != nil {
		t.Fatalf("插账号失败: %v", err)
	}
	t.Cleanup(func() { _ = accountDelete(id) })

	// 只刷新 SIDCC：身份没变，应该写进去
	refreshed := "SAPISID=keep; __Secure-1PSID=same; SIDCC=new"
	updateAccountCookie(id, refreshed)
	if a := accountByID(id); a == nil || a.Cookie != refreshed {
		t.Errorf("同身份的刷新应写回, got %+v", a)
	}

	// 换了 SAPISID：不是同一个账号了，必须丢弃
	updateAccountCookie(id, "SAPISID=other; __Secure-1PSID=same; SIDCC=x")
	if a := accountByID(id); a == nil || a.Cookie != refreshed {
		t.Errorf("换了 SAPISID 应丢弃不写, got cookie=%q", a.Cookie)
	}

	// 换了 __Secure-1PSID：同理
	updateAccountCookie(id, "SAPISID=keep; __Secure-1PSID=other; SIDCC=x")
	if a := accountByID(id); a == nil || a.Cookie != refreshed {
		t.Errorf("换了 __Secure-1PSID 应丢弃不写, got cookie=%q", a.Cookie)
	}
}

// 取 XSRF 失败只有 cookie 自己的问题才计数。之前任何错误都当 401 记，
// 断网时 GET /app 连着 3 次 EOF，好号就被自动停用了。
func TestXSRFErrBlamesCookie(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"无 SNlM0e", fmt.Errorf("no SNlM0e in page (cookie expired or not signed in)"), true},
		{"401", newAppPageStatusError(401, ""), true},
		{"403", newAppPageStatusError(403, ""), true},
		{"302 跳登录", newAppPageStatusError(302, "https://accounts.google.com/ServiceLogin?continue=x"), true},
		{"302 跳 sorry", newAppPageStatusError(302, "https://www.google.com/sorry/index?continue=x&q=y"), false},
		{"302 无 Location", newAppPageStatusError(302, ""), false},
		{"429", newAppPageStatusError(429, ""), false},
		{"502", newAppPageStatusError(502, ""), false},
		{"EOF", fmt.Errorf(`Get "https://gemini.google.com/app": %w`, io.EOF), false},
		{"包一层的 sorry", fmt.Errorf("wrap: %w", newAppPageStatusError(302, "https://www.google.com/sorry/")), false},
		{"包一层的登录", fmt.Errorf("wrap: %w", newAppPageStatusError(302, "https://accounts.google.com/v3/signin")), true},
		{"nil", nil, false},
		{"其它", errors.New("proxy connect failed"), false},
	}
	for _, c := range cases {
		if got := xsrfErrBlamesCookie(c.err); got != c.want {
			t.Errorf("%s: got %v want %v (err=%v)", c.name, got, c.want, c.err)
		}
	}
}

// 错误文本要保持 "fetch /app: HTTP <code>" 前缀，别处按子串判 "HTTP 302"/"HTTP 429"。
func TestAppPageStatusErrorText(t *testing.T) {
	if s := newAppPageStatusError(302, "https://www.google.com/sorry/index?continue=x").Error(); s != "fetch /app: HTTP 302 -> www.google.com/sorry/index" {
		t.Errorf("got %q", s)
	}
	if s := newAppPageStatusError(429, "").Error(); s != "fetch /app: HTTP 429" {
		t.Errorf("got %q", s)
	}

	// R5-5（Reviewer MF-5）：Location 解析失败（非法转义）时绝不把整个
	// 原始头赋进错误——原始串的 query/fragment 可能携带私有参数，随 502
	// 错误文本与请求表外泄。只留固定占位，secret/frag 绝不出现。
	badLoc := "https://gemini.google.com/%zz?token=secret#frag"
	e := newAppPageStatusError(401, badLoc)
	if strings.Contains(e.Error(), "secret") || strings.Contains(e.Error(), "frag") ||
		strings.Contains(e.Error(), "%zz") {
		t.Errorf("解析失败的 Location 绝不能泄露原始 query/fragment: %q", e.Error())
	}
	if e.Location != "<invalid-location>" {
		t.Errorf("解析失败的 Location 必须落固定占位，得到 %q", e.Location)
	}
	if !strings.HasPrefix(e.Error(), "fetch /app: HTTP 401") {
		t.Errorf("状态码分类前缀必须保留: %q", e.Error())
	}
	// 有效登录跳转不受影响：合法 URL 仍只留 host+path（query 本就剥离），
	// 登录页归类不变。
	login := newAppPageStatusError(302, "https://accounts.google.com/ServiceLogin?osid=1&continue=https%3A%2F%2Fgemini.google.com")
	if login.Location != "accounts.google.com/ServiceLogin" {
		t.Errorf("合法登录跳转必须保留 host+path: %q", login.Location)
	}
	if !xsrfErrBlamesCookie(login) {
		t.Error("合法登录跳转的 cookie 归类绝不能被 R5-5 影响")
	}
	// 401 + 非法 Location 仍是 cookie 账（状态码分类不受影响）。
	if !xsrfErrBlamesCookie(e) {
		t.Error("401 的状态码分类绝不能被 Location 占位影响")
	}
}

// 跳 /sorry 也是 302，文案不能说成「重定向到登录页」。
func TestExplainCookieFailureSorry(t *testing.T) {
	got := explainCookieFailure(newAppPageStatusError(302, "https://www.google.com/sorry/index"))
	if got != "出口被上游限流，换个代理再试（不是 cookie 的问题）" {
		t.Errorf("sorry 302: got %q", got)
	}
	got = explainCookieFailure(newAppPageStatusError(302, "https://accounts.google.com/ServiceLogin"))
	if got != "cookie 无效：被重定向到登录页" {
		t.Errorf("login 302: got %q", got)
	}
}

// 跳登录页且 cookie 里没 OSID 要点名缺 OSID；有 OSID、或跳的是 /sorry，都不能误报。
func TestAppPageErrorMissingOSID(t *testing.T) {
	login := "https://accounts.google.com/ServiceLogin?passive=1209600&osid=1"
	e := appPageError(302, login, "SID=a; SAPISID=b")
	if !e.MissingOSID || !xsrfErrBlamesCookie(e) {
		t.Fatalf("缺 OSID 跳登录：MissingOSID=%v blames=%v", e.MissingOSID, xsrfErrBlamesCookie(e))
	}
	if got := explainCookieFailure(e); got != "cookie 缺 OSID：gemini.google.com 现在要求这一项，请在 gemini.google.com 页面重新导出" {
		t.Errorf("explain: got %q", got)
	}
	if e := appPageError(302, login, "SID=a; OSID=c; SAPISID=b"); e.MissingOSID {
		t.Error("带 OSID 的不该报缺 OSID")
	}
	if e := appPageError(302, login, "SID=a; __Secure-OSID=c"); !e.MissingOSID {
		t.Error("__Secure-OSID 不能当成 OSID")
	}
	if e := appPageError(302, "https://www.google.com/sorry/index", "SID=a"); e.MissingOSID {
		t.Error("跳 /sorry 是出口问题，不该报缺 OSID")
	}
	if e := appPageError(302, login, ""); e.MissingOSID {
		t.Error("匿名没有 cookie，不该报缺 OSID")
	}
}

// TestNavRedirectErrorClassificationChain 验证导航跨源重定向产物的三方联合
// 分类：xsrfErrBlamesCookie（记不记账号账）、classifyFailoverError（跳槽准入）、
// upstreamReason（502 诊断码）对登录页 / /sorry / 真超时三条链全部精确命中，
// 互不混淆。
func TestNavRedirectErrorClassificationChain(t *testing.T) {
	loginErr := fmt.Errorf("navigate /app: %w",
		appPageError(302, "https://accounts.google.com/ServiceLogin?osid=1", "SID=a; SAPISID=b"))
	if !xsrfErrBlamesCookie(loginErr) {
		t.Errorf("登录页重定向必须记账号账: %v", loginErr)
	}
	if got := classifyFailoverError(loginErr); got != classAuthFail {
		t.Errorf("登录页重定向应归 classAuthFail，得到 %v", got)
	}
	if got := upstreamReason(loginErr); got != reasonUpstreamHTTP {
		t.Errorf("upstreamReason=%q, want %q", got, reasonUpstreamHTTP)
	}
	// 缺 OSID 的诊断必须透出到人话提示。
	if hint := diagHint(reasonUpstreamHTTP, loginErr); !strings.Contains(hint, "OSID") {
		t.Errorf("缺 OSID 的诊断提示必须点名 OSID: %q", hint)
	}

	sorryErr := fmt.Errorf("navigate /app: %w",
		appPageError(302, "https://www.google.com/sorry/index?continue=x", "SID=a; SAPISID=b"))
	if xsrfErrBlamesCookie(sorryErr) {
		t.Errorf("/sorry 是出口风控，绝不能记账号账: %v", sorryErr)
	}
	if got := classifyFailoverError(sorryErr); got != classUpstreamLimit {
		t.Errorf("/sorry 应归 classUpstreamLimit，得到 %v", got)
	}
	if got := upstreamReason(sorryErr); got != reasonUpstreamHTTP {
		t.Errorf("upstreamReason=%q, want %q", got, reasonUpstreamHTTP)
	}
	// /sorry 不走 cookie 失效指引（保持原风控/限流提示）。
	if hint := diagHint(reasonUpstreamHTTP, sorryErr); strings.Contains(hint, "OSID") || strings.Contains(hint, "cookie 无效") {
		t.Errorf("/sorry 绝不能误报 cookie 失效指引: %q", hint)
	}

	// 真超时：非 browserLocalError、%w DeadlineExceeded 链完整。
	timeoutErr := fmt.Errorf("navigate /app: %w", fmt.Errorf(
		"page fetch error: navigate /app did not reach gemini.google.com origin (got %q): %w",
		"null/complete", context.DeadlineExceeded))
	if xsrfErrBlamesCookie(timeoutErr) {
		t.Errorf("真超时绝不能记账号账: %v", timeoutErr)
	}
	if got := classifyFailoverError(timeoutErr); got != classNetFail {
		t.Errorf("真超时应归 classNetFail，得到 %v", got)
	}
	if got := upstreamReason(timeoutErr); got != reasonUpstreamTimeout {
		t.Errorf("upstreamReason=%q, want %q", got, reasonUpstreamTimeout)
	}
	if isBrowserLocalError(timeoutErr) {
		t.Errorf("真超时绝不能归 browserLocalError: %v", timeoutErr)
	}
	// 停机/拆槽取消：browserLocalError 归本地终止，禁止跳槽。
	abortErr := browserErr("navigate /app: %w", context.Canceled)
	if got := classifyFailoverError(abortErr); got != classLocalAbort {
		t.Errorf("本地取消应归 classLocalAbort，得到 %v", got)
	}
}

// 账号序号要能存能读，越界要拒。
func TestAccountAuthUser(t *testing.T) {
	id, err := accountAdd("multi", "SAPISID=ggg; __Secure-1PSID=hhh", "")
	if err != nil {
		t.Fatalf("插账号失败: %v", err)
	}
	t.Cleanup(func() { _ = accountDelete(id) })

	if a := accountByID(id); a == nil || a.AuthUser != 0 {
		t.Fatalf("默认序号应为 0, got %+v", a)
	}
	if err := accountSetAuthUser(id, 1); err != nil {
		t.Fatal(err)
	}
	if a := accountByID(id); a == nil || a.AuthUser != 1 {
		t.Errorf("序号应为 1, got %+v", a)
	}
	for _, bad := range []int{-1, maxAuthUser + 1} {
		if err := accountSetAuthUser(id, bad); err == nil {
			t.Errorf("序号 %d 应被拒", bad)
		}
	}
	if a := accountByID(id); a == nil || a.AuthUser != 1 {
		t.Errorf("被拒后应保持 1, got %+v", a)
	}
}

// TestRebindAccountProxyAtomicStrictCommit 复核 R11-6（Oracle A7-2/Reviewer #5）：
// 管理员手工 PATCH 改绑必须严格按 UPDATE 真实命中目标行裁决——行不存在
// （并发 DELETE 抢先删行）或写库失败时返回错误、不清自动来源、绝不向
// 面板谎报 ok:true 并按「已改绑」拆槽中断在途流；真实提交成功才返回
// updated=true 与实际旧值，同值 PATCH（old == new）依旧不拆槽。
func TestRebindAccountProxyAtomicStrictCommit(t *testing.T) {
	a, b := withTwoProxies(t, false)

	t.Run("不存在的账号行改绑必须报错且标记未提交", func(t *testing.T) {
		// Oracle A7-2 无需注入故障的对照：对不存在的正 ID 申请绑定，
		// UPDATE 命中 0 行——修复前忽略 false 并提前清来源，handler 按
		// 「旧值不同」拆掉健康槽还回 200 ok:true（DB 实际从未改绑）。
		missing := addTestAccount(t, "rebindMissing")
		bindAccountProxy(missing, a)
		if err := accountDelete(missing); err != nil {
			t.Fatalf("delete: %v", err)
		}
		old, updated, err := rebindAccountProxyAtomic(missing, b)
		if err == nil {
			t.Fatal("对不存在行的改绑必须返回错误（绝不谎报成功）")
		}
		if updated {
			t.Fatal("0 行命中的改绑绝不能标记为已提交")
		}
		_ = old
	})

	t.Run("真实提交返回实际旧值且同值PATCH判定不拆槽", func(t *testing.T) {
		id := addTestAccount(t, "rebindOK")
		bindAccountProxy(id, a)
		// 登记一条自动来源：真实手工改绑必须清除它（步骤6对齐不再命中）。
		claimMu.Lock()
		noteAutoBoundProxyLocked(id, a, proxyURLByID(a))
		claimMu.Unlock()
		old, updated, err := rebindAccountProxyAtomic(id, b)
		if err != nil || !updated {
			t.Fatalf("存在的行改绑必须真实提交: updated=%v err=%v", updated, err)
		}
		if old != a {
			t.Fatalf("必须返回串行点上的实际旧绑定 %d，得到 %d", a, old)
		}
		if acc := accountByID(id); acc == nil || acc.ProxyID != b {
			t.Fatalf("DB 绑定必须落库为新值 %d：%+v", b, acc)
		}
		if isAutoBoundProxyForURL(id, a, proxyURLByID(a)) {
			t.Fatal("真实手工改绑必须清除自动来源记录")
		}
		// 同值 PATCH（old == new）：old 也是 b——调用方据此判定不拆槽。
		old2, updated2, err2 := rebindAccountProxyAtomic(id, b)
		if err2 != nil || !updated2 || old2 != b {
			t.Fatalf("同值改绑必须成功提交且旧值=新值: old=%d updated=%v err=%v", old2, updated2, err2)
		}
		if acc := accountByID(id); acc == nil || acc.ProxyID != b {
			t.Fatalf("同值改绑后绑定必须保持 %d：%+v", b, acc)
		}
	})

	t.Run("PATCH对不存在账号回400绝不拆槽", func(t *testing.T) {
		// handler 层端到端：对不存在的账号 PATCH 绑定——校验通过（代理
		// 存在），但绑定 UPDATE 0 行命中，必须如实回错误且状态码非 200。
		missing := addTestAccount(t, "patchMissing")
		if err := accountDelete(missing); err != nil {
			t.Fatalf("delete: %v", err)
		}
		r := httptest.NewRequest("PATCH", fmt.Sprintf("/admin/api/cookies/%d", missing),
			strings.NewReader(fmt.Sprintf(`{"proxy_id": %d}`, a)))
		w := httptest.NewRecorder()
		handleAdminCookieItem(w, r)
		if w.Code == 200 {
			var body map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err == nil {
				if ok, _ := body["ok"].(bool); ok {
					t.Fatalf("对不存在行的 PATCH 绝不能回 200 ok:true: %v", body)
				}
			}
			t.Fatalf("对不存在行的 PATCH 绝不能回 200: %s", w.Body)
		}
	})
}
