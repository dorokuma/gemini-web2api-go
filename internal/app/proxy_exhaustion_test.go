package app

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func withTestProxySlot(t *testing.T, proxy Proxy, configure func(*RuntimeConfig)) {
	t.Helper()
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{proxy}
	proxyMu.Unlock()
	rtMu.Lock()
	oldConfig := rtVal
	rtVal = RuntimeConfig{
		PerIPConcurrent: 5, PerIPRPM: 30, PerIPRPH: 80,
		ProxyCooldownMin: 120, FallbackDirect: false,
	}
	if configure != nil {
		configure(&rtVal)
	}
	rtMu.Unlock()
	t.Cleanup(func() {
		proxyMu.Lock()
		proxyCache = oldProxies
		proxyMu.Unlock()
		rtMu.Lock()
		rtVal = oldConfig
		rtMu.Unlock()
		slotsMu.Lock()
		delete(slots, proxy.ID)
		slotsMu.Unlock()
	})
}

func TestProxyCircuitBreakerDoesNotReportRPH(t *testing.T) {
	const id int64 = 99900901
	withTestProxySlot(t, Proxy{
		ID: id, URL: "http://127.0.0.1:7897", Enabled: true,
		FailCount: proxyFailThreshold, LastUsed: time.Now().Unix(),
	}, nil)
	_, ok, err := acquireSlot(id)
	if ok || err == nil || !strings.Contains(err.Error(), "proxy_unavailable") {
		t.Fatalf("want circuit-breaker error, got ok=%v err=%v", ok, err)
	}
	var rle *RateLimitError
	if errors.As(err, &rle) {
		t.Fatalf("circuit breaker wrongly classified as a rate limit: %v", err)
	}
}

func TestProxyConcurrencyLimitReportsConcurrent(t *testing.T) {
	const id int64 = 99900902
	withTestProxySlot(t, Proxy{ID: id, URL: "http://127.0.0.1:7897", Enabled: true},
		func(c *RuntimeConfig) { c.PerIPConcurrent = 1 })
	s := getSlot(id)
	s.mu.Lock()
	s.inflight = 1
	s.mu.Unlock()
	_, ok, err := acquireSlot(id)
	var rle *RateLimitError
	if ok || !errors.As(err, &rle) || rle.Reason != "concurrent" {
		t.Fatalf("want true concurrent rate-limit, got ok=%v err=%v", ok, err)
	}
}

func TestProxyCircuitBreakerReturnsHTTP502Not429(t *testing.T) {
	const id int64 = 99900903
	withTestProxySlot(t, Proxy{
		ID: id, URL: "http://127.0.0.1:7897", Enabled: true,
		FailCount: proxyFailThreshold, LastUsed: time.Now().Unix(),
	}, nil)
	request := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"gemini-3.6-flash","messages":[{"role":"user","content":"hello"}]}`))
	response := httptest.NewRecorder()
	handleChatCompletions(response, request)
	if response.Code != 502 || !strings.Contains(response.Body.String(), "proxy_unavailable") {
		t.Fatalf("want 502 proxy_unavailable, got HTTP %d %s", response.Code, response.Body.String())
	}
}

// ── strict_proxy_binding（自 strict_proxy_binding_test.go 归并 + R1 回归补齐）──

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

// R1 修复 3 真实入口回归：M1 + strict_model 下 Bound 内层换号绝不挑中冷却号。
// 驻留号绑 A（满）受阻 → 内层换号时 cooling 号虽然在池里也必须被 busy 跳过；
// healthy 绑 B 胜出后会去走 getXSRF（M1 下走常驻浏览器 fetch，127.0.0.1:1 立即
// 失败 → cookie 不可用错），它出现在归属字段本身就证明冷却号被跳过。
// 冷却号 + 受阻驻留号之外没有健康号时，返回 *RateLimitError{Bound:true}。
func TestStrictBindingM1BoundSwitchSkipsCoolingAccount(t *testing.T) {
	a, b := withTwoProxies(t, true)
	rtMu.Lock()
	rtVal.BrowserTransport = true
	rtVal.StrictModel = true
	rtVal.StrictModelCooldownMin = 300
	rtVal.FallbackAnon = false
	rtMu.Unlock()
	resetDowngrades(t)
	mc := Models["gemini-3.6-flash"]

	resident := addTestAccount(t, "m1resident")
	cooling := addTestAccount(t, "m1cooling")
	healthy := addTestAccount(t, "m1healthy")
	bindAccountProxy(resident, a) // 驻留号绑在满出口 A
	bindAccountProxy(healthy, b)
	markDowngraded(cooling, mc) // cooling 号在 mc 上冷却：未绑定，绝不能被挑中

	m := browserMgr
	m.mu.Lock()
	oldSeed := m.seed
	m.seed = browserSeedState{AccountID: resident, SeededAt: time.Now().Unix()}
	m.mu.Unlock()
	t.Cleanup(func() { m.mu.Lock(); m.seed = oldSeed; m.mu.Unlock() })

	res, err := streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
		mc, nil, nil, nil, nil, map[int64]bool{})
	if err == nil {
		t.Fatal("127.0.0.1:1 不可达，请求不该成功")
	}
	// 胜出的是 healthy：它的归属必须记在结果里，且绝不能是冷却号。
	if res == nil || res.AccountID != healthy {
		t.Fatalf("Bound 换号应挑中健康号 #%d（冷却号 #%d 必须被跳过），得到 res=%+v err=%v",
			healthy, cooling, res, err)
	}
	var rle *RateLimitError
	if res.AccountID == cooling {
		t.Fatalf("Bound 换号绕过了 strict_model 冷却：挑中了冷却号 #%d", cooling)
	}

	// 健康号也没了：只剩绑 A 的驻留号 + 冷却号 → 全阻应报 Bound 429（A 满），
	// 绝不能报 ModelDowngradedError（排除集里塞的是 Bound 受阻号，不是降级号）。
	_ = accountDelete(healthy)
	_, err = streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
		mc, nil, nil, nil, nil, map[int64]bool{})
	if !errors.As(err, &rle) || !rle.Bound || rle.ProxyID != a {
		t.Fatalf("驻留号受阻、另一号冷却时应报 Bound 429(出口 %d)，得到 %v", a, err)
	}
	var mde *ModelDowngradedError
	if errors.As(err, &mde) {
		t.Fatalf("Bound 耗尽不能谎报 ModelDowngradedError：%v", err)
	}
}

// R1 修复 5 回归：claimAccountProxy 并发首次绑定原子化——两个并发竞争者
// 对同一未绑定账号只能产出一个胜者出口，且绑定落库后与胜者一致。
func TestClaimAccountProxyConcurrentFirstBind(t *testing.T) {
	a, b := withTwoProxies(t, true)
	acct := addTestAccount(t, "racebind")
	const n = 16
	var wg sync.WaitGroup
	winners := make(chan int64, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		cand := a
		if i%2 == 1 {
			cand = b
		}
		wg.Add(1)
		go func(candidate int64) {
			defer wg.Done()
			<-start
			winners <- claimAccountProxy(acct, candidate)
		}(cand)
	}
	close(start)
	wg.Wait()
	close(winners)
	var got []int64
	seen := map[int64]bool{}
	for w := range winners {
		got = append(got, w)
		seen[w] = true
	}
	if len(seen) != 1 {
		t.Fatalf("并发首次绑定必须只产出一个胜者出口，得到 %v", got)
	}
	winner := got[0]
	if winner != a && winner != b {
		t.Fatalf("胜者必须是候选出口之一：%d", winner)
	}
	if acc := accountByID(acct); acc == nil || acc.ProxyID != winner {
		t.Fatalf("落库绑定必须与胜者一致：%+v", acc)
	}
	// 已绑定后再 claim 别的候选：绑定有效时绝不覆写。
	if w := claimAccountProxy(acct, map[bool]int64{true: a, false: b}[winner != a]); w != winner {
		t.Fatalf("已绑定的账号不能再被覆写：winner=%d got=%d", winner, w)
	}
	// 绑定出口被禁用后失效，新候选胜出并重绑。
	setProxyField(winner, func(p *Proxy) { p.Enabled = false })
	other := map[bool]int64{true: a, false: b}[winner == a]
	if w := claimAccountProxy(acct, other); w != other {
		t.Fatalf("绑定出口失效后应重绑新候选：got=%d want=%d", w, other)
	}
}

// R1 修复 7 回归：PATCH 先完成全部字段校验再落库——合法 proxy_id + 非法
// authuser/status 必须 400 且零副作用（原绑定一个字节都不能动）。
func TestPatchAccountAllFieldsValidatedBeforeWrite(t *testing.T) {
	a, b := withTwoProxies(t, false)
	id := addTestAccount(t, "patchAll")
	bindAccountProxy(id, a)

	patch := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("PATCH", fmt.Sprintf("/admin/api/cookies/%d", id), strings.NewReader(body))
		w := httptest.NewRecorder()
		handleAdminCookieItem(w, r)
		return w
	}
	// 合法 proxy_id + 非法 authuser=99：400，且原 proxy_id 未被修改。
	w := patch(fmt.Sprintf(`{"proxy_id": %d, "authuser": 99}`, b))
	if w.Code != 400 {
		t.Fatalf("非法 authuser 应 400，得到 %d %s", w.Code, w.Body)
	}
	if acc := accountByID(id); acc == nil || acc.ProxyID != a {
		t.Fatalf("400 必须零副作用：proxy_id 应保持 %d，得到 %+v", a, acc)
	}
	// 合法 proxy_id + 非法 status：同样 400 零副作用。
	w = patch(fmt.Sprintf(`{"proxy_id": %d, "status": "bogus"}`, b))
	if w.Code != 400 {
		t.Fatalf("非法 status 应 400，得到 %d %s", w.Code, w.Body)
	}
	if acc := accountByID(id); acc == nil || acc.ProxyID != a || acc.Status != "enabled" {
		t.Fatalf("400 必须零副作用：%+v", acc)
	}
	// 三个字段全合法：200 且全部落库。
	w = patch(fmt.Sprintf(`{"proxy_id": %d, "authuser": 3, "status": "disabled"}`, b))
	if w.Code != 200 {
		t.Fatalf("全合法应 200，得到 %d %s", w.Code, w.Body)
	}
	if acc := accountByID(id); acc == nil || acc.ProxyID != b || acc.AuthUser != 3 || acc.Status != "disabled" {
		t.Fatalf("全部字段应统一落库：%+v", acc)
	}
	// 负 authuser 同样拦在写库前。
	w = patch(`{"authuser": -1}`)
	if w.Code != 400 {
		t.Fatalf("负 authuser 应 400，得到 %d", w.Code)
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

// POST /admin/api/cookies 同样拦不存在的 proxy_id（导入时指定绑定），且 400
// 必须零副作用——请求前后账号总数不变（不能插进去一条没绑定的半成品）。
func TestPostAccountProxyIDNotFound(t *testing.T) {
	withTwoProxies(t, false)
	post := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/admin/api/cookies", strings.NewReader(body))
		w := httptest.NewRecorder()
		handleAdminCookies(w, r)
		return w
	}
	before, _ := accountCount()
	w := post(`{"label":"p","cookie":"SAPISID=x; SID=y","proxy_id":424242}`)
	if w.Code != 400 {
		t.Fatalf("不存在的 proxy_id 应 400，得到 %d", w.Code)
	}
	after, _ := accountCount()
	if after != before {
		t.Fatalf("400 请求前后账号总数必须不变：before=%d after=%d", before, after)
	}
	// 负数同样拦。
	w = post(`{"label":"p","cookie":"SAPISID=x; SID=y","proxy_id":-1}`)
	if w.Code != 400 {
		t.Fatalf("负 proxy_id 应 400，得到 %d", w.Code)
	}
	if after2, _ := accountCount(); after2 != before {
		t.Fatalf("400 请求不能新增账号：%d -> %d", before, after2)
	}
}
