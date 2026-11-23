package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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
	// 加严断言（R2）：冷却号必须从未被挑中——既没被首绑仲裁写库（ProxyID 仍为 0），
	// 也没被挑号记录过 last_used_at（仍 0）。任何一次误挑都会留下这两者之一。
	if acc := accountByID(cooling); acc == nil || acc.ProxyID != 0 || acc.LastUsedAt != 0 {
		t.Fatalf("冷却号绝不能被挑中/绑定：%+v", acc)
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

	// R2 修复 1 回归：冷却中的已启用绑定绝不覆写——绑定仍是绑定，冷却后还要回到它。
	// proxyUsableByID 把冷却误判成「绑定失效」会让并发请求把账号抢绑到别的出口。
	setProxyField(winner, func(p *Proxy) { p.FailCount, p.LastUsed = proxyFailThreshold, time.Now().Unix() })
	other := map[bool]int64{true: a, false: b}[winner == a]
	if w := claimAccountProxy(acct, other); w != winner {
		t.Fatalf("冷却中的绑定绝不能被覆写：winner=%d got=%d", winner, w)
	}
	if acc := accountByID(acct); acc == nil || acc.ProxyID != winner {
		t.Fatalf("冷却中的绑定必须保持 %d：%+v", winner, acc)
	}
	// 冷却恢复前恢复状态，接着测原有「失效后重绑」断言。
	setProxyField(winner, func(p *Proxy) { p.FailCount, p.LastUsed = 0, 0 })
	// 绑定出口被禁用后失效，新候选胜出并重绑。
	setProxyField(winner, func(p *Proxy) { p.Enabled = false })
	if w := claimAccountProxy(acct, other); w != other {
		t.Fatalf("绑定出口失效后应重绑新候选：got=%d want=%d", w, other)
	}
}

// R3-M3：与 TestStreamGenerateConvClaimLoserSlotAccounting 同一场景——
// 败方仲裁后 acquireSlot(winner) 失败（ok2=false）绝不双释放、绝不误释放
// slots[0]，Bound 错误归属账号与胜者出口。哨兵计位：
//   - 账号在库中保持未绑定（ProxyID=0），goroutine 拿 B 只是轮询落空；
//   - PerIPConcurrent=2，slots[a]=2 满载迫使 acquireSlot(0) 必选 B，
//     slots[b]=1 哨兵（借出变 2、单释放回 1、双释放跌 0），
//     slots[0]=1 哨兵（误释放直连槽会跌 0）；
//   - 主线程持 claimMu → goroutine 拿到 B 后阻塞在 claimAccountProxy 的锁上
//     （轮询 slotUsage(b)==2 且 done 未关 = 确定性同步点，不用 sleep），
//     主线程以 bindAccountProxyLocked 模拟并发胜者落库、保持 A 满载后放锁。
func TestStreamGenerateOnceClaimLoserFailDoubleRelease(t *testing.T) {
	a, b := withTwoProxies(t, true)
	rtMu.Lock()
	rtVal.PerIPConcurrent = 2
	rtMu.Unlock()
	setSlotInflight := func(id int64, n int) {
		s := getSlot(id)
		s.mu.Lock()
		s.inflight = n
		s.mu.Unlock()
	}
	setSlotInflight(a, 2) // 满载迫使 acquireSlot(0) 必选 B，且败方重取 A 必然 !ok2
	setSlotInflight(b, 1) // 哨兵：借出→2、单释放回 1、双释放跌 0
	setSlotInflight(0, 1) // 哨兵：误释放直连槽会跌 0
	acct := addTestAccount(t, "loserA")
	// 库中保持未绑定：主线程稍后以 bindAccountProxyLocked 落库胜者 A。
	mc := Models["gemini-3.6-flash"]

	claimMu.Lock()
	done := make(chan struct{})
	var res *StreamResult
	var err error
	go func() {
		defer close(done)
		res, err = streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
			mc, nil, nil, nil, nil, map[int64]bool{})
	}()
	// 确定性同步点：slotUsage(b)==2 说明 goroutine 已拿到 B；拿到 B 后唯一的
	// 锁点就是 claimAccountProxy 的 claimMu（主线程持锁），done 未关证明它在等。
	deadline := time.Now().Add(5 * time.Second)
	for slotUsage(b).Inflight != 2 {
		select {
		case <-done:
			t.Fatal("goroutine 在拿到 B 的 slot 之前就退出了")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine 未在 5s 内拿到 B 的 slot：%+v", slotUsage(b))
		}
		time.Sleep(time.Millisecond)
	}
	// 模拟并发胜者落库 A，保持 A 满载（哨兵 2）后放锁：败方重取必然 !ok2——
	// 这条路径旧实现会多 releaseSlot(picked.ID) 一次（B 被双释放跌 0）。
	bindAccountProxyLocked(acct, a)
	claimMu.Unlock()
	<-done

	var rle *RateLimitError
	if !errors.As(err, &rle) || !rle.Bound || rle.ProxyID != a {
		t.Fatalf("败方重取胜者出口失败应报 Bound 429(出口 %d)，得到 err=%v res=%+v", a, err, res)
	}
	// 归属不能误填已释放的败方 B。
	if res != nil && res.ProxyID == b {
		t.Fatalf("错误归属不能记在已释放的败方 B 上：res=%+v", res)
	}
	// 胜者 A 的并发计数保持主线程哨兵 2：goroutine 没从 A 拿成过、更没多释放。
	if u := slotUsage(a); u.Inflight != 2 {
		t.Fatalf("胜者 A 的并发计数应保持哨兵 2（不多不少）：%+v", u)
	}
	// 败方 B 单释放回哨兵 1：双释放会跌 0。
	if u := slotUsage(b); u.Inflight != 1 {
		t.Fatalf("败方 B 必须单释放回哨兵 1（双释放会跌 0）：%+v", u)
	}
	// 直连 slot 绝不能被误释放：哨兵保持 1。
	if u := slotUsage(0); u.Inflight != 1 {
		t.Fatalf("直连 slot 绝不能被误释放（哨兵应保持 1）：%+v", u)
	}
}

// R3-M1 回归：默认非严格模式（StrictProxyBinding=false）下，未绑定账号单轮
// streamGenerateOnce 成功取到 XSRF 后，首绑必须持久化落库——修复前 704-706 行
// 提前覆写 acct.ProxyID = picked.ID，让 743 行落库判定恒假、绑定永不写库。
func TestNonStrictFirstBindPersistsAfterXSRF(t *testing.T) {
	a, b := withTwoProxies(t, false)
	setProxyField(a, func(p *Proxy) { p.Enabled = false }) // 非严格：跳过禁用 A 轮询到 B
	setProxyField(b, func(p *Proxy) { p.URL = "http://127.0.0.1:2" })
	acct := addTestAccount(t, "firstbind")
	mc := Models["gemini-3.6-flash"]

	timeout := rtCfg().RequestTimeout
	for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
		stdlibClientCache.Store("http://127.0.0.1:2"+suf, &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/app" {
				return respJSON(req, 200, fakeAppTokens), nil
			}
			return nil, errors.New("dial refused")
		})})
	}
	defer func() {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Delete("http://127.0.0.1:2" + suf)
		}
	}()

	res, _ := streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
		mc, nil, nil, nil, nil, map[int64]bool{})
	if res == nil || res.ProxyID != b {
		t.Fatalf("应轮询到 B 出口：res=%+v", res)
	}
	if acc := accountByID(acct); acc == nil || acc.ProxyID != res.ProxyID {
		t.Fatalf("默认非严格模式首绑必须落库（ProxyID=%d），得到 %+v", res.ProxyID, acc)
	}
}

// R3-M2 回归：XSRF 换号挑出的第 2+ 个号（原绑已禁用的 B）在出口 A 上 getXSRF
// 失败后触发 rotateAccount——它必须走本次选定的 A，绝不打向已禁用的 B 或直连。
// 修复前 rotateAccount(*acct) 按库里的旧绑定 B 取 URL，从已禁用出口外发。
func TestXSRSwitchRotateUsesPickedNotStaleBinding(t *testing.T) {
	a, b := withTwoProxies(t, false) // 非严格：绑定不拦出口共享
	setProxyField(b, func(p *Proxy) { p.Enabled = false; p.URL = "http://127.0.0.1:2" })
	setProxyField(a, func(p *Proxy) { p.URL = "http://127.0.0.1:3" })
	// A 腾出容量：换号后新号绑已禁用的 B → prefer=B 不可用 → 轮询选中 A。
	s := getSlot(a)
	s.mu.Lock()
	s.inflight = 0
	s.mu.Unlock()

	addTestAccount(t, "switchbad")          // 首个号：未绑定，getXSRF 坏（按 id 序先被挑中）
	acct2 := addTestAccount(t, "switchrot") // 换到的号：绑已禁用的 B，cookie 带 1PSID 触发轮转
	_, _ = getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`,
		"SAPISID=rot2; SID=x; __Secure-1PSID=rot2", acct2)
	bindAccountProxy(acct2, b)
	mc := Models["gemini-3.6-flash"]

	var mu sync.Mutex
	var bPaths, aPaths []string
	timeout := rtCfg().RequestTimeout
	stub := func(proxyURL string, paths *[]string) {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Store(proxyURL+suf, &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				*paths = append(*paths, req.URL.Path)
				mu.Unlock()
				return nil, errors.New("dial refused")
			})})
		}
	}
	stub("http://127.0.0.1:2", &bPaths) // 已禁用的 B：绝不能收到任何请求
	stub("http://127.0.0.1:3", &aPaths) // A：getXSRF + 轮转请求都应落在这里
	defer func() {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Delete("http://127.0.0.1:2" + suf)
			stdlibClientCache.Delete("http://127.0.0.1:3" + suf)
		}
	}()

	_, _ = streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
		mc, nil, nil, nil, nil, map[int64]bool{})
	mu.Lock()
	defer mu.Unlock()
	for _, p := range bPaths {
		t.Fatalf("任何请求都绝不能打向已禁用的 B：path=%s", p)
	}
	rotateHit := false
	for _, p := range aPaths {
		if strings.Contains(p, "RotateCookies") {
			rotateHit = true
		}
	}
	if !rotateHit {
		t.Fatalf("acct2（绑已禁用 B）的轮转必须打向选定出口 A：A paths=%v", aPaths)
	}
}

// R3-S1 回归：StrictProxyBinding + FallbackDirect 全开且代理 A 满载时，
// acquireClaimedSlot(boundAcct, 0) 落到直连兜底后必须先核库——已绑启用出口的
// 号绝不能从本机 IP 外发：释放直连槽去占绑定的 A，满则返回 Bound 429，
// 绝不返回直连槽（ID==0）。
func TestStrictFallbackDirectNeverDirectsBoundAccount(t *testing.T) {
	a, _ := withTwoProxies(t, true) // A 哨兵满载（inflight=1、上限 1）
	rtMu.Lock()
	rtVal.FallbackDirect = true
	rtMu.Unlock()
	acct := addTestAccount(t, "bounddirect")
	bindAccountProxy(acct, a) // 库里已绑启用（但满载）的 A
	// 预置直连哨兵：acquireClaimedSlot 落兜底时必须先释放再核库，
	// 哨兵回 1 证明直连槽被正确归还（0 = 没释放，2 = 释放后又借出）。
	s0 := getSlot(0)
	s0.mu.Lock()
	s0.inflight = 1
	s0.mu.Unlock()

	p, release, err := acquireClaimedSlot(acct, 0)
	release()
	var rle *RateLimitError
	if p != nil {
		t.Fatalf("已绑启用出口的号绝不能拿到直连槽：p=%+v", p)
	}
	if !errors.As(err, &rle) || !rle.Bound || rle.ProxyID != a {
		t.Fatalf("应报 Bound 429(出口 %d)，得到 err=%v", a, err)
	}
	if u := slotUsage(0); u.Inflight != 1 {
		t.Fatalf("直连槽必须先释放再去占绑定出口（哨兵应回 1）：%+v", u)
	}
	if acc := accountByID(acct); acc == nil || acc.ProxyID != a {
		t.Fatalf("库里绑定必须保持 A：%+v", acc)
	}
}

// R2 修复 2 回归：仲裁后 acct.ProxyID 必须立即同步成实际出口——否则
// getXSRF 失败触发的 rotateAccount(*acct) 会按旧绑定（已禁用的 A）取 URL，
// 请求从已禁用的出口甚至直连外发。这里禁用 A 后靠并发首绑重绑到 B，
// 断言整个调用绝不向已禁用的 A 的 URL 发任何请求。
func TestStreamGenerateOnceRotateUsesClaimedProxy(t *testing.T) {
	a, b := withTwoProxies(t, true)
	s := getSlot(a)
	s.mu.Lock()
	s.inflight = 0
	s.mu.Unlock()
	acct := addTestAccount(t, "rotclaim")
	// getXSRF 失败会走 rotateAccount(*acct)：它按 acct.ProxyID 取代理 URL。
	// 带 __Secure-1PSID 让 browserAccountFromHeaders 能反查到账号（避免被
	// 误判成「带 Cookie 的非池请求」）；这里没开 BrowserTransport，不影响。
	_, _ = getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`,
		"SAPISID=rotclaim; SID=x; __Secure-1PSID=rotclaim", acct)
	bindAccountProxy(acct, a)
	// A 被管理员禁用：acquireSlot(a) 轮询到 B，claimAccountProxy 重绑到 B。
	setProxyField(a, func(p *Proxy) { p.Enabled = false })
	// 两个假出口独立端口：A=127.0.0.1:1、B=127.0.0.1:2，各自记录是否被使用。
	setProxyField(b, func(p *Proxy) { p.URL = "http://127.0.0.1:2" })
	var mu sync.Mutex
	hitA, hitB := false, false
	// getStdlibClient / getStdlibStreamClient 两个 key（RequestTimeout 与 0）都要钉。
	timeout := rtCfg().RequestTimeout
	for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
		stdlibClientCache.Store("http://127.0.0.1:1"+suf, &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			hitA = true
			mu.Unlock()
			return nil, errors.New("dial refused")
		})})
		stdlibClientCache.Store("http://127.0.0.1:2"+suf, &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			hitB = true
			mu.Unlock()
			return nil, errors.New("dial refused")
		})})
	}
	defer func() {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Delete("http://127.0.0.1:1" + suf)
			stdlibClientCache.Delete("http://127.0.0.1:2" + suf)
		}
	}()

	_, err := streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
		Models["gemini-3.6-flash"], nil, nil, nil, nil, map[int64]bool{})
	if err == nil {
		t.Fatal("假出口不可达，请求不该成功")
	}
	mu.Lock()
	defer mu.Unlock()
	if hitA {
		t.Fatal("旋转/XSRF 绝不能打向已禁用的 A 出口")
	}
	if !hitB {
		t.Fatal("重绑后的请求应只打向 B 出口")
	}
	if acc := accountByID(acct); acc == nil || acc.ProxyID != b {
		t.Fatalf("禁用后应重绑到 B 并落库：%+v", acc)
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
