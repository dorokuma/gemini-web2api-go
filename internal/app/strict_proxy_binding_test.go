package app

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 两个出口：A 并发占满，B 空闲。代理地址指向 127.0.0.1:1，真发请求会立刻连不上。
func withTwoProxies(t *testing.T, strict bool) (a, b int64) {
	t.Helper()
	a, b = 99901001, 99901002
	proxyMu.Lock()
	old := proxyCache
	proxyCache = []Proxy{
		{ID: a, Name: "A", URL: "http://127.0.0.1:1", Enabled: true},
		{ID: b, Name: "B", URL: "http://127.0.0.1:1", Enabled: true},
	}
	proxyMu.Unlock()
	rtMu.Lock()
	oldCfg := rtVal
	rtVal = RuntimeConfig{PerIPConcurrent: 1, PerIPRPM: 30, PerIPRPH: 80, ProxyCooldownMin: 120,
		StrictProxyBinding: strict, RequestTimeout: 5}
	rtMu.Unlock()
	s := getSlot(a)
	s.mu.Lock()
	s.inflight = 1
	s.mu.Unlock()
	t.Cleanup(func() {
		proxyMu.Lock()
		proxyCache = old
		proxyMu.Unlock()
		rtMu.Lock()
		rtVal = oldCfg
		rtMu.Unlock()
		slotsMu.Lock()
		delete(slots, a)
		delete(slots, b)
		slotsMu.Unlock()
	})
	return a, b
}

func setProxyField(id int64, f func(*Proxy)) {
	proxyMu.Lock()
	defer proxyMu.Unlock()
	for i := range proxyCache {
		if proxyCache[i].ID == id {
			f(&proxyCache[i])
		}
	}
}

func TestStrictBindingKeepsBoundProxy(t *testing.T) {
	a, b := withTwoProxies(t, true)
	_, ok, err := acquireSlot(a)
	var rle *RateLimitError
	if ok || !errors.As(err, &rle) || !rle.Bound || rle.ProxyID != a || rle.Reason != "concurrent" {
		t.Fatalf("严格绑定下绑定出口满了应报 Bound 错误，不换出口：ok=%v err=%v", ok, err)
	}

	// 冷却中的绑定出口同样不换。
	setProxyField(a, func(p *Proxy) { p.FailCount, p.LastUsed = proxyFailThreshold, time.Now().Unix() })
	if _, ok, err := acquireSlot(a); ok || !errors.As(err, &rle) || rle.Reason != "cooling" {
		t.Fatalf("冷却中应报 cooling：ok=%v err=%v", ok, err)
	}

	// 手动禁用 = 绑定失效，照常轮询到 B（调用方随后会重绑）。
	setProxyField(a, func(p *Proxy) { p.Enabled = false })
	p, ok, err := acquireSlot(a)
	if !ok || p.ID != b {
		t.Fatalf("绑定出口被禁用后应换到 B：p=%d ok=%v err=%v", p.ID, ok, err)
	}
	releaseSlot(p.ID)
}

func TestNonStrictBindingFallsBackToOtherProxy(t *testing.T) {
	a, b := withTwoProxies(t, false)
	p, ok, err := acquireSlot(a)
	if !ok || p.ID != b {
		t.Fatalf("默认（非严格）绑定出口满了应换到 B：p=%d ok=%v err=%v", p.ID, ok, err)
	}
	releaseSlot(p.ID)
}

func TestStrictBoundElsewhere(t *testing.T) {
	a, b := withTwoProxies(t, true)
	acct := &CookieAccount{ID: 1, ProxyID: a}
	if !strictBoundElsewhere(acct, b) {
		t.Error("绑在 A 的号不能借用 B")
	}
	if strictBoundElsewhere(acct, a) {
		t.Error("绑在 A 的号用 A 没问题")
	}
	if strictBoundElsewhere(&CookieAccount{ID: 2}, b) {
		t.Error("没绑定的号用哪个都行")
	}
	setProxyField(a, func(p *Proxy) { p.Enabled = false })
	if strictBoundElsewhere(acct, b) {
		t.Error("绑定的出口被禁用后，绑定不再算数")
	}
	rtMu.Lock()
	rtVal.StrictProxyBinding = false
	rtMu.Unlock()
	setProxyField(a, func(p *Proxy) { p.Enabled = true })
	if strictBoundElsewhere(acct, b) {
		t.Error("开关关着时不拦")
	}
}

// 端到端：号 1 绑 A（满），号 2 绑 B。严格绑定下请求应换成号 2、走 B，而不是让号 1 走 B。
func TestStrictBindingSwitchesAccountNotProxy(t *testing.T) {
	a, b := withTwoProxies(t, true)
	a1 := addTestAccount(t, "strictA")
	a2 := addTestAccount(t, "strictB")
	bindAccountProxy(a1, a)
	bindAccountProxy(a2, b)

	res, err := streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
		Models["gemini-3.6-flash"], nil, nil, nil, nil, map[int64]bool{})
	if err == nil {
		t.Fatal("出口地址是 127.0.0.1:1，请求不该成功")
	}
	if res.ProxyID != b || res.AccountID != a2 {
		t.Fatalf("应换成号 %d 走出口 %d，实际号 %d 出口 %d（err=%v）", a2, b, res.AccountID, res.ProxyID, err)
	}
	if acc := accountByID(a1); acc == nil || acc.ProxyID != a {
		t.Fatalf("号 1 的绑定不该被改：%+v", acc)
	}

	// 号 2 也不在了：只剩号 1，它的出口满着 → 报绑定出口不可用，不能借 B。
	_ = accountDelete(a2)
	_, err = streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
		Models["gemini-3.6-flash"], nil, nil, nil, nil, map[int64]bool{})
	var rle *RateLimitError
	if !errors.As(err, &rle) || !rle.Bound || rle.ProxyID != a {
		t.Fatalf("只剩绑在满出口上的号时应报 Bound 429，得到 %v", err)
	}
}

// 高危冲突 1 回归：browser_transport 的 M1 粘性 + strict_proxy_binding 同时开。
// 驻留号绑的出口满 → Bound 换号必须把受阻驻留号并进 exclude，否则
// stickyResidentAccountFor 会把它原样返回，外层每次重试都撞同一个 Bound 错。
func TestStrictBindingExcludesBlockedResident(t *testing.T) {
	a, _ := withTwoProxies(t, true)
	rtMu.Lock()
	rtVal.BrowserTransport = true
	rtMu.Unlock()
	resident := addTestAccount(t, "resident")
	bindAccountProxy(resident, a) // 驻留号绑在满出口 A 上

	m := browserMgr
	m.mu.Lock()
	oldSeed := m.seed
	m.seed = browserSeedState{AccountID: resident, SeededAt: time.Now().Unix()}
	m.mu.Unlock()
	t.Cleanup(func() { m.mu.Lock(); m.seed = oldSeed; m.mu.Unlock() })

	// 换号重试入口：Bound 错误应把驻留号并入 exclude 并放行重试（未吐过内容）。
	exclude := map[int64]bool{}
	berr := &RateLimitError{Reason: "concurrent", ProxyID: a, Bound: true}
	if !retryOnAnotherAccount(&StreamResult{AccountID: resident}, berr,
		Models["gemini-3.6-flash"], exclude) {
		t.Fatal("Bound 错误且未吐内容时应换号重试")
	}
	if !exclude[resident] {
		t.Fatal("受阻驻留号必须并进 exclude，否则 stickyResidentAccountFor 会反复返回它")
	}
	// 断言核心语义：受阻驻留号进了 exclude 后不再被 M1 粘性挑回。
	if got := stickyResidentAccountFor(exclude, Models["gemini-3.6-flash"].Upstream); got != nil {
		t.Fatalf("受阻驻留号 #%d 还被挑回：%+v", resident, got)
	}

	// 多轮续接同样走这条换号：会话绑的号 Bound 出错 → conv.accountID 进 exclude，
	// 下一轮 stickyResidentAccountFor 不会把同一个受阻驻留号再挑回来。
	exclude2 := map[int64]bool{}
	if !retryOnAnotherAccount(&StreamResult{AccountID: resident}, berr,
		Models["gemini-3.6-flash"], exclude2) || !exclude2[resident] {
		t.Fatal("多轮换号同样要把受阻驻留号并进 exclude")
	}
	if got := stickyResidentAccountFor(exclude2, ""); got != nil {
		t.Fatalf("多轮路径受阻驻留号还被挑回：%+v", got)
	}

	// 换号已吐过内容时不能重试（客户端会收到两遍），Bound 同样受守。
	if retryOnAnotherAccount(&StreamResult{AccountID: resident, Emitted: "部分"}, berr,
		Models["gemini-3.6-flash"], map[int64]bool{}) {
		t.Fatal("已吐过内容的 Bound 不该换号重来")
	}
}

// 面板手动指定绑定：能设、能清（0），不存在的代理拒掉（PATCH 与 POST 都拦）。
func TestPatchAccountProxyID(t *testing.T) {
	a, _ := withTwoProxies(t, false)
	id := addTestAccount(t, "manualBind")
	patch := func(body string) int {
		r := httptest.NewRequest("PATCH", fmt.Sprintf("/admin/api/cookies/%d", id), strings.NewReader(body))
		w := httptest.NewRecorder()
		handleAdminCookieItem(w, r)
		return w.Code
	}
	if c := patch(fmt.Sprintf(`{"proxy_id": %d}`, a)); c != 200 || accountByID(id).ProxyID != a {
		t.Fatalf("设绑定：code=%d proxy=%d", c, accountByID(id).ProxyID)
	}
	if c := patch(`{"proxy_id": 424242}`); c != 400 || accountByID(id).ProxyID != a {
		t.Fatalf("不存在的代理应 400 且不改绑定：code=%d proxy=%d", c, accountByID(id).ProxyID)
	}
	if c := patch(`{"proxy_id": -1}`); c != 400 || accountByID(id).ProxyID != a {
		t.Fatalf("负数 proxy_id 应 400 且不改绑定：code=%d proxy=%d", c, accountByID(id).ProxyID)
	}
	if c := patch(`{"label": "x"}`); c != 200 || accountByID(id).ProxyID != a {
		t.Fatalf("没传 proxy_id 不该动绑定：code=%d proxy=%d", c, accountByID(id).ProxyID)
	}
	if c := patch(`{"proxy_id": 0}`); c != 200 || accountByID(id).ProxyID != 0 {
		t.Fatalf("0 应清掉绑定：code=%d proxy=%d", c, accountByID(id).ProxyID)
	}
}

// POST /admin/api/cookies 同样拦不存在的 proxy_id（导入时指定绑定）。
func TestPostAccountProxyIDNotFound(t *testing.T) {
	withTwoProxies(t, false)
	post := func(body string) int {
		r := httptest.NewRequest("POST", "/admin/api/cookies", strings.NewReader(body))
		w := httptest.NewRecorder()
		handleAdminCookies(w, r)
		return w.Code
	}
	if c := post(`{"label":"p","cookie":"SAPISID=x; SID=y","proxy_id":424242}`); c != 400 {
		t.Fatalf("不存在的 proxy_id 应 400，得到 %d", c)
	}
}
