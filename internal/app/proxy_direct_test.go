package app

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

// proxyIDDirect = -1 锁定本机直连（v6.5.0）的回归：
// - acquireSlot(-1) 只走 slots[0]，绝不进代理池，满了报 Bound=true；
// - claimAccountProxy 遇 -1 原样返回，绝不覆写；
// - rotateProxyURL 对绑定失效的号 fail-closed 跳过保活，绝不静默直连。

func TestAcquireSlotLockedDirect(t *testing.T) {
	withTwoProxies(t, false) // 池里有 A/B 两个出口；锁定直连绝不能借它们
	// slots[0] 空闲：锁定直连拿到直连（Proxy{} 零值，ID=0）
	p, ok, err := acquireSlot(proxyIDDirect)
	if !ok || err != nil || p.ID != 0 {
		t.Fatalf("锁定直连应拿直连槽：p=%v ok=%v err=%v", p, ok, err)
	}
	releaseSlot(p.ID)

	// 占满直连槽：锁定直连满 → Bound=true 且 ProxyID=-1，绝不落进代理池。
	s := getSlot(0)
	s.mu.Lock()
	s.inflight = 99
	s.mu.Unlock()
	_, ok, err = acquireSlot(proxyIDDirect)
	var rle *RateLimitError
	if ok || !errors.As(err, &rle) {
		t.Fatalf("锁定直连满应报 RateLimitError：ok=%v err=%v", ok, err)
	}
	if !rle.Bound || rle.ProxyID != proxyIDDirect {
		t.Fatalf("应 Bound=true ProxyID=-1：%+v", rle)
	}
	s.mu.Lock()
	s.inflight = 0
	s.mu.Unlock()
}

// claimAccountProxy：-1 绑定是「绝不走代理」的显式承诺，仲裁不得覆写。
func TestClaimAccountProxyLockedDirect(t *testing.T) {
	_, b := withTwoProxies(t, true)
	id := addTestAccount(t, "lockedDirect")
	bindAccountProxy(id, proxyIDDirect)
	// 候选是代理 B 也必须返回 -1，绝不覆写绑定
	if got := claimAccountProxy(id, b); got != proxyIDDirect {
		t.Fatalf("锁定直连被覆写成 %d", got)
	}
	if a := accountByID(id); a == nil || a.ProxyID != proxyIDDirect {
		t.Fatalf("库中绑定被改：%+v", a)
	}
}

// rotateProxyURL：绑定代理被删/禁用 → fail-closed 报错，绝不退回直连。
func TestRotateProxyURLFailClosed(t *testing.T) {
	a, _ := withTwoProxies(t, false)
	// 锁定直连号：允许 "" 直连
	u, err := rotateProxyURL(CookieAccount{ID: 1, ProxyID: proxyIDDirect})
	if err != nil || u != "" {
		t.Fatalf("锁定直连应允许直连：url=%q err=%v", u, err)
	}
	// 绑定 A 且 A 可用：拿到 A 的 URL
	u, err = rotateProxyURL(CookieAccount{ID: 2, ProxyID: a})
	if err != nil || u == "" {
		t.Fatalf("绑定可用代理应返回 URL：%q %v", u, err)
	}
	// 绑定不存在的代理：fail-closed 报错
	if _, err := rotateProxyURL(CookieAccount{ID: 3, ProxyID: 424242}); err == nil {
		t.Fatal("绑定失效代理应 fail-closed")
	}
	// 绑定被禁用的代理：同样 fail-closed
	setProxyField(a, func(p *Proxy) { p.Enabled = false })
	if _, err := rotateProxyURL(CookieAccount{ID: 4, ProxyID: a}); err == nil {
		t.Fatal("绑定被禁用代理应 fail-closed")
	}
}

// 未绑定号 + 池非空 + fallback_direct 关：挑可用代理；全禁用时 fail-closed。
func TestRotateProxyURLUnbound(t *testing.T) {
	a, b := withTwoProxies(t, false)
	setProxyField(a, func(p *Proxy) { p.Enabled = false })
	u, err := rotateProxyURL(CookieAccount{ID: 5, ProxyID: 0})
	if err != nil || u == "" {
		t.Fatalf("未绑定号应借可用代理 B：%q %v", u, err)
	}
	_ = b
	setProxyField(b, func(p *Proxy) { p.Enabled = false })
	if _, err := rotateProxyURL(CookieAccount{ID: 5, ProxyID: 0}); err == nil {
		t.Fatal("无可用代理时应 fail-closed，不静默直连")
	}
}

// allSlotUsage：池非空 + fallback 关时直连槽只对绑定 -1 的账号可见。
func TestAllSlotUsageLockedDirectVisible(t *testing.T) {
	withTwoProxies(t, false)
	id := addTestAccount(t, "directWatch")
	bindAccountProxy(id, proxyIDDirect)
	usage := allSlotUsage()
	found := false
	for _, u := range usage {
		if u.ProxyID == 0 {
			found = true
		}
	}
	if !found {
		t.Error("有锁定直连账号时直连槽应在监控里可见")
	}
}

// RateLimitError.Error 的分支顺序回归：AllProxiesFull 必须先于 ProxyID==0 判，
// 否则全池限流（ProxyID 零值）会误报成 "direct IP slot full"；锁定直连（-1）
// 的 Bound 分支也要先报「锁定直连不可用」而不是带 -1 的代理号文案。
func TestRateLimitErrorMessages(t *testing.T) {
	cases := []struct {
		e    *RateLimitError
		want string
	}{
		{&RateLimitError{Bound: true, ProxyID: proxyIDDirect, Reason: "concurrent"},
			"locked direct exit unavailable"},
		{&RateLimitError{Bound: true, ProxyID: 7, Reason: "cooling"},
			"bound proxy #7 unavailable"},
		{&RateLimitError{AllProxiesFull: true, Reason: "concurrent"},
			"all proxy slots full"},
		{&RateLimitError{ProxyID: 0, Reason: "concurrent"},
			"direct IP slot full"},
	}
	for _, c := range cases {
		if got := c.e.Error(); !strings.Contains(got, c.want) {
			t.Errorf("%+v → %q，want substr %q", c.e, got, c.want)
		}
	}
	// 关键回归：全池满 + ProxyID=0 绝不能被 ProxyID==0 分支抢走。
	if got := (&RateLimitError{AllProxiesFull: true}).Error(); strings.Contains(got, "direct IP") {
		t.Fatalf("AllProxiesFull 被误归直连槽：%q", got)
	}
}

// autoBindAccountProxy 绝不覆写 -1：管理员锁定直连的号，撞到什么出口都不写绑。
func TestAutoBindNeverOverwritesDirectLock(t *testing.T) {
	a, b := withTwoProxies(t, true)
	id := addTestAccount(t, "autoBindDirect")
	bindAccountProxy(id, proxyIDDirect)
	autoBindAccountProxy(id, a)
	if acc := accountByID(id); acc == nil || acc.ProxyID != proxyIDDirect {
		t.Fatalf("锁定直连被 autoBind 覆写：%+v", acc)
	}
	// proxyID<=0 也不写（哨兵值只由管理员 PATCH 写）。
	autoBindAccountProxy(id, 0)
	autoBindAccountProxy(id, proxyIDDirect)
	if acc := accountByID(id); acc == nil || acc.ProxyID != proxyIDDirect {
		t.Fatalf("哨兵写绑被放行：%+v", acc)
	}
	// 已删账号不写。
	dead := addTestAccount(t, "autoBindDead")
	_ = accountDelete(dead)
	autoBindAccountProxy(dead, b)
	// 已有可用代理绑定的粘性不打散：先绑 A，再撞 B 时库中仍是 A。
	id2 := addTestAccount(t, "autoBindSticky")
	bindAccountProxy(id2, a)
	autoBindAccountProxy(id2, b)
	if acc := accountByID(id2); acc == nil || acc.ProxyID != a {
		t.Fatalf("粘性绑定被别的出口打散：%+v", acc)
	}
	// 绑定失效（代理被禁用）时才允许重绑。
	setProxyField(a, func(p *Proxy) { p.Enabled = false })
	autoBindAccountProxy(id2, b)
	if acc := accountByID(id2); acc == nil || acc.ProxyID != b {
		t.Fatalf("失效绑定应被重绑到 B：%+v", acc)
	}
}

// xsrfExitConflict 双向隔离：锁定直连号撞代理槽要冲突；池非空+关 fallback 时
// 非锁定直连号撞直连槽也要冲突（含库里查不到绑定 fail-closed）。
func TestXsrfExitConflictBidirectional(t *testing.T) {
	a, _ := withTwoProxies(t, true)
	// 1) 锁定直连号撞上代理槽 → 冲突
	id := addTestAccount(t, "xsrfDirect")
	bindAccountProxy(id, proxyIDDirect)
	cid, conflict, cur := xsrfExitConflict(id, a)
	if !conflict || cid != proxyIDDirect || cur != proxyIDDirect {
		t.Fatalf("锁定直连号撞代理槽应冲突：cid=%d conflict=%v cur=%d", cid, conflict, cur)
	}
	// 2) 锁定直连号撞直连槽 → 不冲突
	if _, conflict, _ := xsrfExitConflict(id, 0); conflict {
		t.Fatal("锁定直连号走直连槽应合法")
	}
	// 3) 池非空+关 fallback：普通绑定号撞直连槽 → 冲突
	rtMu.Lock()
	old := rtVal
	rtVal.FallbackDirect = false
	rtMu.Unlock()
	t.Cleanup(func() { rtMu.Lock(); rtVal = old; rtMu.Unlock() })
	id2 := addTestAccount(t, "xsrfBound")
	bindAccountProxy(id2, a)
	if _, conflict, _ := xsrfExitConflict(id2, 0); !conflict {
		t.Fatal("绑定代理号撞直连槽应冲突")
	}
	// 4) 号已被删（boundKnown=false）撞直连槽：fail-closed 冲突
	dead := addTestAccount(t, "xsrfDead")
	_ = accountDelete(dead)
	if _, conflict, _ := xsrfExitConflict(dead, 0); !conflict {
		t.Fatal("已删账号撞直连槽应 fail-closed 冲突")
	}
	// 5) 未绑定号（curBound=0）撞直连槽：同样冲突（绝不给未绑定号直连）
	id3 := addTestAccount(t, "xsrfUnbound")
	if _, conflict, _ := xsrfExitConflict(id3, 0); !conflict {
		t.Fatal("未绑定号撞直连槽应冲突")
	}
}

// 陈旧快照回归：库中锁定直连、prefer 误传代理 ID（或反之），acquireClaimedSlot
// 都必须先核库再占槽——绝不拿着过期快照去 acquireSlot。
func TestAcquireClaimedSlotStalePrefer(t *testing.T) {
	a, b := withTwoProxies(t, true)
	id := addTestAccount(t, "staleDirect")
	bindAccountProxy(id, proxyIDDirect)
	// prefer 传陈旧代理 ID：必须核库转成直连槽。
	p, release, err := acquireClaimedSlot(id, a)
	defer release()
	if err != nil || p == nil || p.ID != 0 {
		t.Fatalf("锁定直连号的陈旧 prefer 应被核库纠成直连：p=%+v err=%v", p, err)
	}
	// 反向：库中已改绑代理 B、prefer 还是陈旧 -1，也必须按库中代理出口。
	bindAccountProxy(id, b)
	p2, release2, err2 := acquireClaimedSlot(id, proxyIDDirect)
	defer release2()
	if err2 != nil || p2 == nil || p2.ID != b {
		t.Fatalf("改绑代理后陈旧 -1 prefer 应被核库纠成代理 B：p=%+v err=%v", p2, err2)
	}
}

// compatibleProbeAccount 严格匿名回归（M1）：池非空+关 fallback 时测直连
// （target=0）只接受锁定直连号——未绑定(0)的号在「绝不直连」的池规则下借
// 本机 IP 也是串出口；找不到就只能返回 nil 走匿名探测。
func TestCompatibleProbeAccountStrictAnonymity(t *testing.T) {
	a, _ := withTwoProxies(t, false) // fallback_direct 关
	rtMu.Lock()
	old := rtVal
	rtVal.FallbackDirect = false
	rtMu.Unlock()
	t.Cleanup(func() { rtMu.Lock(); rtVal = old; rtMu.Unlock() })

	// 只有未绑定号时：target=0 必须返回 nil（绝不借未绑定号测直连）。
	unbound := addTestAccount(t, "probeUnbound")
	if got := compatibleProbeAccount(0); got != nil {
		t.Fatalf("池非空+关fallback 时测直连只能挑锁定直连号，挑到 #%d", got.ID)
	}
	// 绑定代理的号也不行。
	bound := addTestAccount(t, "probeBound")
	bindAccountProxy(bound, a)
	if got := compatibleProbeAccount(0); got != nil {
		t.Fatalf("绑定代理的号不能测直连，挑到 #%d", got.ID)
	}
	// 加入锁定直连号：target=0 必须优先挑它。
	locked := addTestAccount(t, "probeLocked")
	bindAccountProxy(locked, proxyIDDirect)
	got := compatibleProbeAccount(0)
	if got == nil || got.ID != locked {
		t.Fatalf("测直连应优先挑锁定直连号 #%d，得到 %+v", locked, got)
	}
	// 测代理 A：绑 A 的号优先。
	gotA := compatibleProbeAccount(a)
	if gotA == nil || gotA.ID != bound {
		t.Fatalf("测代理 A 应挑绑 A 的号 #%d，得到 %+v", bound, gotA)
	}
	// 测代理 B：无绑 B 的号时退回未绑定号。
	_, b := a, int64(99901002)
	gotB := compatibleProbeAccount(b)
	if gotB == nil || gotB.ID != unbound {
		t.Fatalf("测代理 B 无绑定号时应退回未绑定号 #%d，得到 %+v", unbound, gotB)
	}
	// 开回 fallback：target=0 未绑定号可以作为 fallback（不优先于 -1）。
	rtMu.Lock()
	rtVal.FallbackDirect = true
	rtMu.Unlock()
	_ = accountDelete(locked)
	got0 := compatibleProbeAccount(0)
	if got0 == nil || got0.ID != unbound {
		t.Fatalf("开 fallback 无锁定号时应退回未绑定号 #%d，得到 %+v", unbound, got0)
	}
}

// R2-S2 回归：probeGeminiWithAccount(..., nil, false) 是严格匿名——池里明明
// 有号也绝不 loadCookie() 借号外发（借号会把锁定直连号的 Cookie 从代理发出去）。
func TestProbeGeminiWithAccountStrictAnonymity(t *testing.T) {
	_ = addTestAccount(t, "probeNoBorrow") // 池里有号，诱惑 loadCookie
	var sawCookie string
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/app" {
			return respJSON(req, 200, fakeAppTokens), nil
		}
		if strings.Contains(req.URL.Path, "StreamGenerate") {
			sawCookie = req.Header.Get("Cookie")
			return respJSON(req, 200, ")]}'\n[]\n"), nil
		}
		return respJSON(req, 404, ""), nil
	})
	proxy := fakeUpstreamServer(t, rt)
	res := probeGeminiWithAccount("hi", proxy, nil, false)
	if sawCookie != "" {
		t.Fatalf("严格匿名探测绝不许带池里账号的 Cookie：%q", sawCookie)
	}
	_ = res
}
