package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
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
		// 直连哨兵槽（如 R4 用例的 slots[0].inflight=1）也不能留在共享 map 里
		// 污染后续测试——slotUsage(0) 的读数必须是本用例自己的。
		delete(slots, 0)
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

// R1 修复 3 真实入口回归：M1 + strict_model 下 Bound 内层换号绝不挑中冷却号。
// 驻留号绑 A（满）受阻 → 内层换号时 cooling 号虽然在池里也必须被 busy 跳过；
// healthy 绑 B 胜出后会去走 getXSRF（M1 下走常驻浏览器 fetch，127.0.0.1:1 立即
// 失败 → cookie 不可用错），它出现在归属字段本身就证明冷却号被跳过。
// 冷却号 + 受阻驻留号之外没有健康号时，返回 *RateLimitError{Bound:true}。

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
			w, _ := claimAccountProxy(acct, candidate)
			winners <- w
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
	if w, ok := claimAccountProxy(acct, map[bool]int64{true: a, false: b}[winner != a]); !ok || w != winner {
		t.Fatalf("已绑定的账号不能再被覆写：winner=%d got=%d ok=%v", winner, w, ok)
	}

	// R2 修复 1 回归：冷却中的已启用绑定绝不覆写——绑定仍是绑定，冷却后还要回到它。
	// proxyUsableByID 把冷却误判成「绑定失效」会让并发请求把账号抢绑到别的出口。
	setProxyField(winner, func(p *Proxy) { p.FailCount, p.LastUsed = proxyFailThreshold, time.Now().Unix() })
	other := map[bool]int64{true: a, false: b}[winner == a]
	if w, ok := claimAccountProxy(acct, other); !ok || w != winner {
		t.Fatalf("冷却中的绑定绝不能被覆写：winner=%d got=%d ok=%v", winner, w, ok)
	}
	if acc := accountByID(acct); acc == nil || acc.ProxyID != winner {
		t.Fatalf("冷却中的绑定必须保持 %d：%+v", winner, acc)
	}
	// 冷却恢复前恢复状态，接着测原有「失效后重绑」断言。
	setProxyField(winner, func(p *Proxy) { p.FailCount, p.LastUsed = 0, 0 })
	// 绑定出口被禁用后失效，新候选胜出并重绑。
	setProxyField(winner, func(p *Proxy) { p.Enabled = false })
	if w, ok := claimAccountProxy(acct, other); !ok || w != other {
		t.Fatalf("绑定出口失效后应重绑新候选：got=%d want=%d ok=%v", w, other, ok)
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
				// /app 返回 200 空页触发 "no SNlM0e"（Cookie 类错误，符合
				// xsrfErrBlamesCookie 才有资格进入轮转救号）；其余路径保持
				// 断连。若用断连桩，新契约下网络错误绝不白刷轮转，无法覆盖
				// 本用例的“换号后轮转必走选定出口 A”回归目标。
				if req.URL.Path == "/app" {
					return respJSON(req, 200, ""), nil
				}
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
	// 精确断言：已禁用的 B 零命中；/RotateCookies 必须打向选定出口 A。
	if len(bPaths) != 0 {
		t.Fatalf("任何请求都绝不能打向已禁用的 B：paths=%v", bPaths)
	}
	var rotateHits int
	for _, p := range aPaths {
		if strings.Contains(p, "/RotateCookies") {
			rotateHits++
		}
	}
	if rotateHits == 0 {
		t.Fatalf("acct2（绑已禁用 B）的轮转必须打向选定出口 A：A paths=%v", aPaths)
	}
}

// TestXSRFHealSkipsRotationOnNonCookieErrors 验证轮转救号的触发收敛：
// getXSRF 遇到网络断连、302 -> /sorry 等非 Cookie 类错误时零次请求
// /RotateCookies（不白打 accounts.google.com）；且当 rotateAccount 空跑
// 无新票时，healXSRFByRotation 不二次请求 /app。
func TestXSRFHealSkipsRotationOnNonCookieErrors(t *testing.T) {
	setup := func(t *testing.T, appStub func(req *http.Request) (*http.Response, error)) (rotateHits *int, appHits *int) {
		t.Helper()
		a, b := withTwoProxies(t, false)
		rtMu.Lock()
		rtVal.FallbackDirect = true
		rtVal.FallbackAnon = true
		rtMu.Unlock()
		// 两个出口独立端口：getXSRF / 保活请求都被假 RoundTripper 接管。
		setProxyField(a, func(p *Proxy) { p.URL = "http://127.0.0.1:1" })
		setProxyField(b, func(p *Proxy) { p.URL = "http://127.0.0.1:2" })
		for _, id := range []int64{a, b} {
			s := getSlot(id)
			s.mu.Lock()
			s.inflight = 0
			s.mu.Unlock()
		}
		rh, ah := new(int), new(int)
		var mu sync.Mutex
		record := func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				*rh++
			}
			if req.URL.Path == "/app" {
				*ah++
			}
			mu.Unlock()
			return appStub(req)
		}
		timeout := rtCfg().RequestTimeout
		for _, proxyURL := range []string{"http://127.0.0.1:1", "http://127.0.0.1:2"} {
			for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
				stdlibClientCache.Store(proxyURL+suf, &http.Client{Transport: roundTripperFunc(record)})
			}
		}
		t.Cleanup(func() {
			for _, proxyURL := range []string{"http://127.0.0.1:1", "http://127.0.0.1:2"} {
				for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
					stdlibClientCache.Delete(proxyURL + suf)
				}
			}
		})
		// 直连 TLS 侧（rotateProxyURL 可能选直连兜底）：同样只计数，不回真网络。
		fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
			mu.Lock()
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				*rh++
			}
			mu.Unlock()
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: fhttp.Header{},
				Body:   io.NopCloser(strings.NewReader("")),
			}, nil
		}}
		stubTLSRotateClient(t, fake)
		return rh, ah
	}
	mc := Models["gemini-3.6-flash"]

	t.Run("网络断连不白刷轮转", func(t *testing.T) {
		rh, _ := setup(t, func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("dial refused")
		})
		acct := addTestAccount(t, "nodialrotate")
		_, _ = getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`,
			"SAPISID=nd; SID=x; __Secure-1PSID=nd", acct)
		_, _ = streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
			mc, nil, nil, nil, nil, map[int64]bool{})
		if *rh != 0 {
			t.Fatalf("网络断连不是 cookie 的错，绝不能白刷 /RotateCookies：命中 %d 次", *rh)
		}
	})

	t.Run("302 sorry 不白刷轮转", func(t *testing.T) {
		rh, _ := setup(t, func(req *http.Request) (*http.Response, error) {
			r := respJSON(req, 302, "")
			if req.URL.Path == "/app" {
				r.Header.Set("Location", "https://www.google.com/sorry/index?continue=x")
			}
			return r, nil
		})
		acct := addTestAccount(t, "nosorryrotate")
		_, _ = getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`,
			"SAPISID=ns; SID=x; __Secure-1PSID=ns", acct)
		_, _ = streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
			mc, nil, nil, nil, nil, map[int64]bool{})
		if *rh != 0 {
			t.Fatalf("302 -> /sorry 是出口风控不是 cookie 的错，绝不能白刷轮转：命中 %d 次", *rh)
		}
	})

	t.Run("轮转空跑不二次请求 app", func(t *testing.T) {
		rh, ah := setup(t, func(req *http.Request) (*http.Response, error) {
			// /app 返回 200 空页触发 no SNlM0e（Cookie 类错误，有资格进轮转）；
			// /RotateCookies / RotateCookiesPage 全部 200 空响应——轮转空跑无新票。
			return respJSON(req, 200, ""), nil
		})
		acct := addTestAccount(t, "emptyheal")
		_, _ = getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`,
			"SAPISID=eh; SID=x; __Secure-1PSID=eh", acct)
		before := *ah
		_, _ = streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
			mc, nil, nil, nil, nil, map[int64]bool{})
		if *rh == 0 {
			t.Fatalf("no SNlM0e 应触发轮转救号（/RotateCookies 命中 >=1）：%d", *rh)
		}
		// 轮转空跑（len(names)==0）后 healXSRFByRotation 不二次请求 /app：
		// 全程 /app 最多命中 1 次（首轮 getXSRF），重抓只会让同一 cookie
		// 再失败一次。
		if *ah-before != 1 {
			t.Fatalf("/app 命中 %d 次，轮转空跑后绝不许二次抓取（应为 1）", *ah-before)
		}
	})
}

// R4-M2 回归：直连兜底（picked.ID == 0）下 getXSRF 失败触发的 rotateAccount
// 必须走本轮出口——picked.ID==0 意味着「直连就是本轮出口」，若留着库里的旧绑定
// B（已禁用）就会从禁用出口外发。哨兵：slots[0].inflight=1 → 借出变 2、释放回 1。
func TestStrictFallbackDirectRotateNeverUsesStaleBinding(t *testing.T) {
	a, b := withTwoProxies(t, true)
	rtMu.Lock()
	rtVal.FallbackDirect = true
	rtVal.PerIPConcurrent = 2
	rtVal.FallbackAnon = true
	rtMu.Unlock()
	// A、B 全部满载：acquireSlot(0) 只能落 fallback_direct 直连兜底。
	for _, id := range []int64{a, b} {
		s := getSlot(id)
		s.mu.Lock()
		s.inflight = 2
		s.mu.Unlock()
	}
	s0 := getSlot(0)
	s0.mu.Lock()
	s0.inflight = 1 // 哨兵：兜底借出 2、defer 释放回 1
	s0.mu.Unlock()
	acct := addTestAccount(t, "rotdirect")
	// 带 __Secure-1PSID 触发轮转；库绑已禁用的 B（满载的 A 也不可用）。
	_, _ = getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`,
		"SAPISID=rotdirect; SID=x; __Secure-1PSID=rotdirect", acct)
	bindAccountProxy(acct, b)
	setProxyField(b, func(p *Proxy) { p.Enabled = false; p.URL = "http://127.0.0.1:2" })
	mc := Models["gemini-3.6-flash"]

	var mu sync.Mutex
	var bPaths []string
	var directPaths, directCookies []string
	timeout := rtCfg().RequestTimeout
	for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
		stdlibClientCache.Store("http://127.0.0.1:2"+suf, &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			bPaths = append(bPaths, req.URL.Path)
			mu.Unlock()
			return nil, errors.New("dial refused")
		})})
	}
	defer func() {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Delete("http://127.0.0.1:2" + suf)
		}
	}()
	// R5-M1：直连绝不能真发公网。用 fake tls-client 接管两个缓存 key（限时的
	// getTLSClient 与不限时的 getTLSStreamClient）：/app 回 200 空页面让 getXSRF
	// 确定性失败 → 进 rotateAccount → /RotateCookies 也落在这个 fake 上。
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		mu.Lock()
		directPaths = append(directPaths, req.URL.Path)
		directCookies = append(directCookies, req.Header.Get("Cookie"))
		mu.Unlock()
		return &fhttp.Response{
			StatusCode: 200, Status: "200 OK",
			Header: fhttp.Header{},
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	imp := effectiveImpersonate()
	for _, to := range []int{timeout, 0} {
		key := fmt.Sprintf("%s|%d", imp, to)
		clientCacheMu.Lock()
		prev, had := clientCache[key]
		clientCache[key] = fake
		clientCacheMu.Unlock()
		t.Cleanup(func() {
			clientCacheMu.Lock()
			defer clientCacheMu.Unlock()
			if had {
				clientCache[key] = prev
			} else {
				delete(clientCache, key)
			}
		})
	}

	res, _ := streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
		mc, nil, nil, nil, nil, map[int64]bool{})
	mu.Lock()
	defer mu.Unlock()
	if len(bPaths) != 0 {
		t.Fatalf("直连兜底下轮转绝不能打向已禁用/满载的旧绑定 B：paths=%v", bPaths)
	}
	var rotateHits int
	var rotateCookieOK bool
	for i, p := range directPaths {
		if strings.Contains(p, "/RotateCookies") {
			rotateHits++
			if strings.Contains(directCookies[i], "__Secure-1PSID=rotdirect") {
				rotateCookieOK = true
			}
		}
	}
	if rotateHits == 0 {
		t.Fatalf("直连兜底的 getXSRF 失败必须触发 /RotateCookies 轮转发往直连：directPaths=%v", directPaths)
	}
	if !rotateCookieOK {
		t.Fatalf("/RotateCookies 必须携带该账号的 __Secure-1PSID=rotdirect：cookies=%v", directCookies)
	}
	if res != nil && res.ProxyID != 0 {
		t.Fatalf("出口应是直连兜底（ProxyID=0）：res=%+v", res)
	}
	if u := slotUsage(0); u.RPM != 1 || u.Inflight != 1 {
		t.Fatalf("直连槽应真实借还（RPM=1、Inflight 回哨兵 1）：%+v", u)
	}
}

// R4-M1 回归：严格模式 + FallbackAnon=false + PinAccountID>0 下，拿到出口之后、
// XSRF 循环内仲裁前的窗口里钉住的号被并发改绑到别的启用出口——绝不能拿 picked
// 出口替它外发，也绝不能落到匿名：必须直接返回 *RateLimitError{Bound:true}。
//
// 确定性调度（锁接力，不用 sleep 猜时机）：
//   - 主线程持 claimMu + B 的 slot 锁（sB.mu，B inflight=0）：goroutine 占 A 后
//     claim1 阻塞在 claimMu；主线程落库 acct→B、放 claimMu → claim1 判出 B，
//     goroutine 去 acquireSlot(B) → pickBoundProxy(B).trySlotAcquire(B) 阻塞在
//     sB.mu（严格绑定的重取只走 bound 分支，必然点这把锁）；
//   - 主线程先 bindAccountProxy(acct,a) 把号绑回 A（claimMu 此刻无人持有，
//     立刻完成），再放 sB.mu：goroutine 拿到 B 的 slot → acquire 收敛
//     picked=B → XSRF 循环 claim2 读库 cur=A（启用）≠ picked=B → 冲突；
//   - claim2 严格排在「绑回 A」之后：goroutine 拿到 sB.mu 时这次写库早已完成，
//     无时序侥幸。PinAccountID>0 → 前置仲裁直接 return Bound(A)。
func TestStrictPinnedReboundConflictNeverAnon(t *testing.T) {
	a, b := withTwoProxies(t, true)
	// A 腾出容量：acct 绑 A、prefer=A 直接拿 A；B 保持空闲但由主线程持锁挡驾。
	s := getSlot(a)
	s.mu.Lock()
	s.inflight = 0
	s.mu.Unlock()
	sB := getSlot(b)
	sB.mu.Lock()
	sB.inflight = 0 // 空闲：goroutine 的重取必然卡在 sB.mu 上而不是因满载失败
	sB.mu.Unlock()
	// 直连槽零负载：若逻辑破防匿名/直连外发，必然从这里借出——
	// slots[0] 任何活动都是红线被踩的铁证。
	s0 := getSlot(0)
	s0.mu.Lock()
	s0.inflight = 0
	s0.mu.Unlock()
	acct := addTestAccount(t, "pinnedrebound")
	bindAccountProxy(acct, a)
	mc := Models["gemini-3.6-flash"]
	mc.PinAccountID = acct // 续接语义：换号与匿名都不允许

	claimMu.Lock()
	sB.mu.Lock() // goroutine 的 acquireSlot(B) 必然在 trySlotAcquire 压这把锁
	done := make(chan struct{})
	var res *StreamResult
	var err error
	go func() {
		defer close(done)
		res, err = streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
			mc, nil, nil, nil, nil, map[int64]bool{})
	}()
	// 同步点：goroutine 占 A（inflight 0→1）后唯一锁点是 claim1 的 claimMu。
	deadline := time.Now().Add(5 * time.Second)
	for slotUsage(a).Inflight != 1 {
		select {
		case <-done:
			t.Fatal("goroutine 在拿到 A 的 slot 之前就退出了")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine 未在 5s 内拿到 A 的 slot：%+v", slotUsage(a))
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("goroutine 在拿到 A 后未经 claimMu 阻塞就退出了——同步点失效")
	default:
	}
	// 改绑第一拍：acct→B。claim1 判出 B → 释放 A → acquireSlot(B) 压 sB.mu。
	bindAccountProxyLocked(acct, b)
	claimMu.Unlock()
	// 等败方 A 归还（inflight 1→0）= goroutine 已离开 claimMu 临界区，正在
	// sB.mu 前排队。此刻把号绑回 A：claim2 必然在 sB.mu 释放之后才运行，
	// 而绑回 A 一定先于 sB.mu 释放——读序确定性成立。
	deadline = time.Now().Add(5 * time.Second)
	for slotUsage(a).Inflight != 0 {
		select {
		case <-done:
			t.Fatal("goroutine 在归还 A 之前就退出了")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine 未在 5s 内归还 A：%+v", slotUsage(a))
		}
		time.Sleep(time.Millisecond)
	}
	bindAccountProxy(acct, a) // 锁外入口会拿 claimMu（此刻空闲）：原子落库
	sB.mu.Unlock()            // 放行 goroutine 的 trySlotAcquire(B)
	<-done

	var rle *RateLimitError
	if !errors.As(err, &rle) || !rle.Bound || rle.ProxyID != a || rle.Reason != "rebound" {
		t.Fatalf("钉住号被并发绑回 A 后必须报 Bound 429(出口 %d, rebound)，得到 err=%v res=%+v", a, err, res)
	}
	if res == nil || res.AccountID != acct {
		t.Fatalf("归属必须记在钉住号 %d 上：res=%+v", acct, res)
	}
	// 归属出口是冲突返回的零值：picked=B 的 slot 已被 defer release() 归还，
	// 绝不能把冲突胜者 A 或未结算的 B 记进结果。
	if res.ProxyID != 0 {
		t.Fatalf("Bound 冲突返回不携带已释放的出口：res=%+v", res)
	}
	// 匿名/直连零借出：FallbackAnon=false 的静默降级红线没被踩。
	if u := slotUsage(0); u.RPM != 0 || u.Inflight != 0 {
		t.Fatalf("绝不能发出匿名/直连请求（slots[0] 必须零活动）：%+v", u)
	}
	// 库里最终绑定保持 A（并发写序的最后一写），没被 picked=B 覆写。
	if acc := accountByID(acct); acc == nil || acc.ProxyID != a {
		t.Fatalf("库里绑定必须保持并发写的结果 A：%+v", acc)
	}
	// R5-S1：pin 住的号上收到 Bound 429 绝不能被外层 retryOnAnotherAccount 换号——
	// 不写入 exclude、不打「换下一个号」日志、直接返回 false。
	excl := map[int64]bool{}
	if retryOnAnotherAccount(&StreamResult{AccountID: acct},
		&RateLimitError{Reason: "rebound", ProxyID: b, Bound: true},
		ModelConfig{PinAccountID: acct}, excl) {
		t.Fatal("钉住号的 Bound 错误绝不能换号重试")
	}
	if len(excl) != 0 {
		t.Fatalf("钉住号的 Bound 错误绝不能写 exclude：excl=%v", excl)
	}
}

// R3-S1 回归：StrictProxyBinding + FallbackDirect 全开且代理全部满载时，
// acquireClaimedSlot(boundAcct, 0) 落到直连兜底后必须先核库——已绑启用出口的
// 号绝不能从本机 IP 外发：释放直连槽去占绑定的 A，满则返回 Bound 429，
// 绝不返回直连槽（ID==0）。
// R4-S2：A 和 B 全部占满（PerIPConcurrent=2）是必然落兜底的铁证，slots[0]
// 的 RPM==1 + Inflight 回 1 同时证明「进去过直连槽」且「正确归还」，
// slotUsage(b).RPM==0 证明败方归还后没有误占别的出口。
func TestStrictFallbackDirectNeverDirectsBoundAccount(t *testing.T) {
	a, b := withTwoProxies(t, true)
	rtMu.Lock()
	rtVal.FallbackDirect = true
	rtVal.PerIPConcurrent = 2
	rtMu.Unlock()
	// A、B 全部满载：acquireSlot(0) 必然落 fallback_direct 直连兜底。
	for _, id := range []int64{a, b} {
		s := getSlot(id)
		s.mu.Lock()
		s.inflight = 2
		s.mu.Unlock()
	}
	acct := addTestAccount(t, "bounddirect")
	bindAccountProxy(acct, a) // 库里已绑启用（但满载）的 A
	// 预置直连哨兵：acquireClaimedSlot 落兜底时必须先释放再核库，
	// Inflight 回 1 证明直连槽被正确归还（0 = 双释放，2 = 没释放）。
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
	if u := slotUsage(0); u.RPM != 1 || u.Inflight != 1 {
		t.Fatalf("直连槽必须先借入再释放（RPM=1）且归还（Inflight 回 1）：%+v", u)
	}
	if u := slotUsage(b); u.RPM != 0 {
		t.Fatalf("败方重取绝不能误占 B（RPM 必须为 0）：%+v", u)
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
	if c := patch(`{"proxy_id": -2}`); c != 400 || accountByID(id).ProxyID != a {
		t.Fatalf("非法负数 proxy_id 应 400 且不改绑定：code=%d proxy=%d", c, accountByID(id).ProxyID)
	}
	// -1 是合法的锁定本机直连哨兵（v6.5.0 起面板可锁 TR 出口）。
	if c := patch(`{"proxy_id": -1}`); c != 200 || accountByID(id).ProxyID != -1 {
		t.Fatalf("proxy_id=-1 锁定直连应 200：code=%d proxy=%d", c, accountByID(id).ProxyID)
	}
	// 还原绑回 a，后续断言以它为基准。
	if c := patch(fmt.Sprintf(`{"proxy_id": %d}`, a)); c != 200 || accountByID(id).ProxyID != a {
		t.Fatalf("还原绑定：code=%d proxy=%d", c, accountByID(id).ProxyID)
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
	// 非法负数（<-1）同样拦；-1 是合法的锁定本机直连哨兵，不在此断言。
	w = post(`{"label":"p","cookie":"SAPISID=x; SID=y","proxy_id":-2}`)
	if w.Code != 400 {
		t.Fatalf("非法负 proxy_id 应 400，得到 %d", w.Code)
	}
	if after2, _ := accountCount(); after2 != before {
		t.Fatalf("400 请求不能新增账号：%d -> %d", before, after2)
	}
}

// TestClaimAccountProxyZeroRowsFailsClosed 复核 R8-5（Reviewer MF-5）：
// claimAccountProxy 写库 0 行命中（账号在核库与 UPDATE 之间被并发删除）
// 或 RowsAffected 不可知时，候选出口绝不能被当作已赢得仲裁返回——
// claim 本身报告失败（ok=false），严格仲裁 arbitrateHeldExit 据此释放
// 手上 slot 并 fail-closed 拒绝外发（绝不落到匿名/直连，也不按候选出口
// 外发）。修复前 claimAccountProxy 丢弃 bindAccountProxyLocked 的返回值，
// 0 行命中同样把候选出口当作胜者返回，请求会携一个从未真实落库的绑定
// 从候选 IP 外发。
func TestClaimAccountProxyZeroRowsFailsClosed(t *testing.T) {
	a, b := withTwoProxies(t, true)
	_ = b
	// 已删账号：UPDATE 必然 0 行命中——claim 必须报告失败而非返回候选。
	dead := addTestAccount(t, "zerorowdead")
	if err := accountDelete(dead); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if w, ok := claimAccountProxy(dead, a); ok || w != 0 {
		t.Fatalf("写库 0 行命中的 claim 必须失败（ok=false），得到 winner=%d ok=%v", w, ok)
	}

	// 严格仲裁端到端：acquireClaimedSlot 拿到出口后账号被并发删除——
	// 释放手上 slot 并返回明确错误，绝不按候选出口外发、绝不误释放直连槽。
	acct := addTestAccount(t, "zerorowarb")
	s := getSlot(a)
	s.mu.Lock()
	s.inflight = 0 // 空出 A 槽：acquireSlot(0) 会按策略拿到 B 或 A
	s.mu.Unlock()
	setSlotInflightZero := func(id int64) {
		s0 := getSlot(id)
		s0.mu.Lock()
		s0.inflight = 0
		s0.mu.Unlock()
	}
	setSlotInflightZero(0)
	// 直连哨兵：slots[0].inflight=0 基线，仲裁失败绝不能误释放成负数，
	// 也绝不能泄漏占用（保持 0）；同时 A/B 槽计数必须归零（正确释放）。
	// 先删账号再仲裁：删除发生在 acquireSlot 之后、claimAccountProxy 之前。
	// 本测试直接驱动 arbitrateHeldExit 的完整调用路径（acquireClaimedSlot
	// 内含 reconcileHeldSlot 核库——已删账号 + 手上代理槽 → 核库失败后
	// 调 claimAccountProxy 的路径因 a==nil 走不到候选写库；故用未删号
	// 持有槽、仲裁瞬间并发删除的等价终态：直接对 claim 已失败的事实
	// 断言仲裁释放语义）。
	p, ok1, err1 := acquireSlot(a)
	if !ok1 || err1 != nil {
		t.Fatalf("前置：必须能拿到 A 槽: ok=%v err=%v", ok1, err1)
	}
	// 占用计数快照（A=1，直连 0=0）。
	releaseHeld := func() { releaseSlot(a) }
	released := false
	relOnce := func() {
		if !released {
			released = true
			releaseHeld()
		}
	}
	// 账号此刻被并发删除（acquireSlot 之后、claim 之前的等价终态）。
	if err := accountDelete(acct); err != nil {
		relOnce()
		t.Fatalf("delete: %v", err)
	}
	winner, keep, failErr, reacq := arbitrateHeldExit(acct, p, relOnce)
	if keep || failErr == nil || reacq != nil {
		t.Fatalf("claim 写库 0 行命中时仲裁必须 fail-closed 报错，得到 winner=%d keep=%v err=%v reacq=%v",
			winner, keep, failErr, reacq)
	}
	if !strings.Contains(failErr.Error(), "not available") {
		t.Fatalf("仲裁失败必须点名账号不可用: %v", failErr)
	}
	if !released {
		t.Fatal("仲裁 fail-closed 必须释放手上 slot（绝不泄漏占用继续外发）")
	}
	// 占用计数归零：A 槽被正确释放，直连哨兵绝不被误碰。
	if got := slotUsage(a).Inflight; got != 0 {
		t.Fatalf("A 槽必须被释放归零，得到 %d", got)
	}
	if got := slotUsage(0).Inflight; got != 0 {
		t.Fatalf("直连哨兵槽绝不能被误释放/误占，得到 %d", got)
	}
}

// TestAdminPatchRebindSerializedDecision 复核 R10-2（Reviewer M5/Oracle M5）：
// 手工 PATCH 改绑的「读取实际旧绑定 + 写入新绑定」必须串行化在同一临界区，
// 摘除判定按串行化后的实际旧值做出。固定三段交错（真实自动重绑通道）：
// PATCH 校验通过 → 配额型自动重绑把 DB 推进到 P2 并提交 → PATCH 改绑回 P1。
// 修复前 PATCH 按进入临界区前的早读快照判定「同值不拆槽」，DB 实际从 P2 改
// 回 P1 却跳过失效——旧 P2 槽及在途流继续存活；修复后按实际旧值 P2 != P1
// 判定 proxyChanged，旧槽摘除、在途流终止。
func TestAdminPatchRebindSerializedDecision(t *testing.T) {
	a, b := withTwoProxies(t, false)
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.BrowserTransport = true })
	defer setRuntimeCfg(t, func(c *RuntimeConfig) { c.BrowserTransport = false })

	// 模拟配额型自动重绑交错：先自动绑到 a（登记可核来源），禁用 a 触发
	// 冷却，再自动重绑到 b——严格复刻 acquireSlot 的重绑序列。
	origHook := testHookAfterCookiePatchValidate
	defer func() { testHookAfterCookiePatchValidate = origHook }()

	t.Run("改绑窗口内自动重绑已提交则旧槽必须摘除且在途流终止", func(t *testing.T) {
		id := addTestAccount(t, "patchRebind")
		bindAccountProxy(id, a)
		// 注册旧出口 P2(=b) 的存活槽与在途流——交错终态下 PATCH 改绑前的
		// 合法自动绑定产物。
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(accountByID(id).Cookie),
			browserContextID:   "bc-pb",
			targetID:           "t-pb",
			sessionID:          "sess-pb",
			allowDirect:        false,
			origAccountProxyID: b,
			boundProxyID:       b,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m := browserMgr
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()
		s := newBrowserStream()
		s.ac = ac
		m.registerStream(s)
		defer m.unregisterStream(s)

		// 钩子：PATCH 快照读取之后、进入 claimMu 之前，自动重绑通道把
		// DB 从 a 推进到 b（配额交错：禁用 a 触发冷却重绑）。
		hooked := make(chan struct{})
		var hookOnce sync.Once
		testHookAfterCookiePatchValidate = func(hitID int64) {
			if hitID != id {
				return
			}
			hookOnce.Do(func() {
				setProxyField(a, func(p *Proxy) { p.Enabled = false })
				autoBindAccountProxy(id, b)
				setProxyField(a, func(p *Proxy) { p.Enabled = true })
				close(hooked)
			})
		}

		// PATCH 改绑回 P1(=a)：串行化后实际旧值是 b——proxyChanged 必须为真。
		r := httptest.NewRequest("PATCH", fmt.Sprintf("/admin/api/cookies/%d", id),
			strings.NewReader(fmt.Sprintf(`{"proxy_id": %d}`, a)))
		w := httptest.NewRecorder()
		handleAdminCookieItem(w, r)
		<-hooked
		if w.Code != 200 {
			t.Fatalf("PATCH 应 200，得到 %d %s", w.Code, w.Body)
		}
		if acc := accountByID(id); acc == nil || acc.ProxyID != a {
			t.Fatalf("PATCH 落库必须为 P1(%d)：%+v", a, acc)
		}
		// 核心断言：旧 P2 槽必须摘除（disposed 且从 ctxs 移除）、在途流终止。
		m.mu.Lock()
		_, still := m.ctxs[key]
		disposed := ac.disposed
		m.mu.Unlock()
		if still || !disposed {
			t.Fatalf("实际旧绑定 P2 != 新绑定 P1：旧槽必须被失效摘除（still=%v disposed=%v）", still, disposed)
		}
		s.mu.Lock()
		fatal := s.fatalErr
		s.mu.Unlock()
		if fatal == nil {
			t.Fatal("在途流必须被终止（failStreamsForCtx fatalErr）")
		}
	})

	t.Run("真正同值PATCH绝不拆槽", func(t *testing.T) {
		// 对照：无交错、PATCH 绑定值与实际旧值一致——绝不触发失效。
		id := addTestAccount(t, "patchSame")
		bindAccountProxy(id, a)
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(accountByID(id).Cookie),
			browserContextID:   "bc-ps",
			targetID:           "t-ps",
			sessionID:          "sess-ps",
			allowDirect:        false,
			origAccountProxyID: a,
			boundProxyID:       a,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m := browserMgr
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()
		testHookAfterCookiePatchValidate = nil
		r := httptest.NewRequest("PATCH", fmt.Sprintf("/admin/api/cookies/%d", id),
			strings.NewReader(fmt.Sprintf(`{"proxy_id": %d}`, a)))
		w := httptest.NewRecorder()
		handleAdminCookieItem(w, r)
		if w.Code != 200 {
			t.Fatalf("同值 PATCH 应 200，得到 %d", w.Code)
		}
		m.mu.Lock()
		alive := m.ctxs[key] == ac && !ac.disposed
		m.mu.Unlock()
		if !alive {
			t.Fatal("真正同值 PATCH（实际旧值==新值）绝不触发失效拆槽")
		}
	})
}
