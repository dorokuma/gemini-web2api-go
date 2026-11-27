package app

import (
	"context"
	"encoding/json"
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

// 哨兵必须是这串 JSPB，不能走 json.Marshal：后者会把 000 收成 0，服务端不认。
func TestRotate1PSIDTSBodyIsJSPBSentinel(t *testing.T) {
	const want = `[000,"-0000000000000000000"]`
	if rotate1PSIDTSBody != want {
		t.Fatalf("payload = %q，期望 %q", rotate1PSIDTSBody, want)
	}
	b, err := json.Marshal([]interface{}{0, "-0000000000000000000"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) == rotate1PSIDTSBody {
		t.Fatal("json.Marshal 的结果碰巧跟哨兵一样了，这个测试该改")
	}
}

func TestCookieSubsetKeepsOnlyNamed(t *testing.T) {
	in := "SID=a; __Secure-1PSID=psid; SAPISID=sap; __Secure-1PSIDTS=ts; NID=n"
	got := cookieSubset(in, []string{"__Secure-1PSID", "__Secure-1PSIDTS"})
	if got != "__Secure-1PSID=psid; __Secure-1PSIDTS=ts" {
		t.Fatalf("got %q", got)
	}
}

func TestCookieSubsetSkipsEmptyAndDup(t *testing.T) {
	in := "__Secure-1PSID=psid; __Secure-1PSIDTS=; __Secure-1PSID=other"
	got := cookieSubset(in, rotate1PSIDTSCookies)
	if got != "__Secure-1PSID=psid" {
		t.Fatalf("got %q", got)
	}
}

func TestCookieValue(t *testing.T) {
	in := "SID=a; __Secure-1PSID=psid==; SAPISID=sap"
	if got := cookieValue(in, "__Secure-1PSID"); got != "psid==" {
		t.Fatalf("got %q", got)
	}
	if got := cookieValue(in, "nope"); got != "" {
		t.Fatalf("missing should be empty, got %q", got)
	}
}

// TestRotateAccountOptsRevalidatesExitAfterWait 复核 R9-2 与 R10-3
// （Reviewer MF-2/M4）：等锁结束后、外发前必须按当前绑定事实 cur 重新
// 解析出口（R10-3 原子快照 + 出口语义分离）——等待窗口内管理员改绑/
// 锁直连/解锁时，按等待前快照 a 的出口绝不携 Cookie 走错出口或直出；
// 自愈通道按 picked/expectedDB 仲裁事实独立核验：绑定未变的合法直连
// 自愈放行，等待窗口真实改绑立即拒发。
// 构造：传入的 a 是等待前陈旧快照，DB 行已是改绑后的当前事实——与
// 「等锁期间发生改绑」的生产交错终态完全同构，确定性复现。
func TestRotateAccountOptsRevalidatesExitAfterWait(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 91, Name: "rw1", URL: "http://rw1:8080", Enabled: true},
		{ID: 92, Name: "rw2", URL: "http://rw2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.StrictProxyBinding = false
		c.FallbackDirect = false
		c.BrowserTransport = false
	})

	// 计连：两个代理的 stdlib 缓存键都桩成计数 RoundTripper——任何外发
	// 走错出口都能定位；伪造直连的 tls-client 同理。
	var mu sync.Mutex
	proxyHits := map[string]int{}
	countRT := func(tag string) roundTripperFunc {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			proxyHits[tag]++
			mu.Unlock()
			return respJSON(req, 200, ")]}'\n\n[]"), nil
		})
	}
	timeout := rtCfg().RequestTimeout
	for _, tag := range []string{"http://rw1:8080", "http://rw2:8080"} {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Store(tag+suf, &http.Client{Transport: countRT(tag)})
		}
	}
	defer func() {
		for _, tag := range []string{"http://rw1:8080", "http://rw2:8080"} {
			for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
				stdlibClientCache.Delete(tag + suf)
			}
		}
	}()
	var directHits int
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		mu.Lock()
		directHits++
		mu.Unlock()
		return &fhttp.Response{
			StatusCode: 200, Status: "200 OK", Header: fhttp.Header{},
			Body: io.NopCloser(strings.NewReader(""))}, nil
	}}
	imp := effectiveImpersonate()
	for _, to := range []int{timeout, 0} {
		key := fmt.Sprintf("%s|%d", imp, to)
		clientCacheMu.Lock()
		clientCache[key] = fake
		clientCacheMu.Unlock()
	}
	defer func() {
		for _, to := range []int{timeout, 0} {
			key := fmt.Sprintf("%s|%d", imp, to)
			clientCacheMu.Lock()
			delete(clientCache, key)
			clientCacheMu.Unlock()
		}
	}()
	hits := func() (map[string]int, int) {
		mu.Lock()
		defer mu.Unlock()
		cp := map[string]int{}
		for k, v := range proxyHits {
			cp[k] = v
		}
		return cp, directHits
	}

	mkAcct := func(label, tag string) int64 {
		cookie := "SAPISID=rw" + tag + "; __Secure-1PSID=rwp" + tag + "; SID=rws" + tag + "; __Secure-1PSIDTS=rwt" + tag
		id, err := accountInsert(label, cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		t.Cleanup(func() { _ = accountDelete(id) })
		return id
	}

	t.Run("管理快照锁直连等锁期间改绑代理后对旧直连出口零发送", func(t *testing.T) {
		// R10-3（Reviewer M4）：普通轮转等锁后直接按最新 cur 重新解析
		// 出口——等待前快照锁直连（-1），等锁期间管理员 PATCH 改绑代理
		// 91：保活流量跟随新绑定走 rw1，对等待前快照的直连出口零发送
		//（R9 语义「按旧快照放行/拒绝」已被「按最新绑定定出口」取代）。
		id := mkAcct("rwbind", "b1")
		bindAccountProxy(id, 91)
		stale := accountByID(id)
		staleCp := *stale
		staleCp.ProxyID = proxyIDDirect // 等待前快照：锁直连
		beforeP, beforeD := hits()
		_, _, err := rotateAccountOpts(staleCp, false)
		_ = err // 上游桩空响应下换发可能无新票报错，出口计数才是关键证据
		afterP, afterD := hits()
		if afterP["http://rw1:8080"] <= beforeP["http://rw1:8080"] {
			t.Fatalf("按最新绑定重算后保活必须走新绑定 91 的出口: before=%v after=%v", beforeP, afterP)
		}
		if afterD != beforeD {
			t.Fatalf("绝不能沿等待前快照的直连出口直出（directHits %d -> %d）", beforeD, afterD)
		}
	})

	t.Run("管理快照A等锁改绑空闲B后对A零发送", func(t *testing.T) {
		// R10-3（Reviewer M4 判据②）：管理强制轮转读到绑定 A(91)，
		// 等锁期间手工改绑到空闲可用的 B(92)——旧实现仅凭「A 在启用池
		// 中」放行旧 A；修复后对 A 零发送、按最新 B 外发。
		id := mkAcct("rwsnapab", "ab1")
		bindAccountProxy(id, 92) // DB 已是改绑后的 B
		stale := accountByID(id)
		staleCp := *stale
		staleCp.ProxyID = 91 // 等待前快照：绑定 A
		beforeP, beforeD := hits()
		_, _, err := rotateAccountOpts(staleCp, false)
		_ = err
		afterP, afterD := hits()
		if afterP["http://rw1:8080"] != beforeP["http://rw1:8080"] {
			t.Fatalf("改绑后对旧出口 A 必须零发送（rw1 %d -> %d）", beforeP["http://rw1:8080"], afterP["http://rw1:8080"])
		}
		if afterP["http://rw2:8080"] <= beforeP["http://rw2:8080"] {
			t.Fatalf("改绑后保活必须按最新绑定 B 外发（rw2）: before=%v after=%v", beforeP, afterP)
		}
		if afterD != beforeD {
			t.Fatalf("绝不能直出（directHits %d -> %d）", beforeD, afterD)
		}
	})

	t.Run("自愈真实直连picked且绑定代理启用未变fallback开放行", func(t *testing.T) {
		// R10-3（Reviewer M4 判据③）：非严格 + FallbackDirect=true + 绑定
		// 代理 P 仍启用（槽满时 acquireSlot 合法选直连、xsrfExitConflict
		// 放行）——自愈传 picked=0、expectedDBProxyID=P 且 DB 绑定未变：
		// 修复前被误当「等待窗口内改绑」拒发；修复后合法直连自愈放行。
		id := mkAcct("rwhealpick", "hp1")
		bindAccountProxy(id, 91)
		acct := accountByID(id)
		setRuntimeCfg(t, func(c *RuntimeConfig) { c.FallbackDirect = true })
		defer setRuntimeCfg(t, func(c *RuntimeConfig) { c.FallbackDirect = false })
		beforeP, beforeD := hits()
		_, _, err := rotateAccountWithOpts(*acct, rotateOpts{
			hasPickedProxy:    true,
			pickedProxyID:     0, // picked=直连
			expectedDBProxyID: 91,
		})
		if err != nil && strings.Contains(err.Error(), "不再合法") {
			t.Fatalf("绑定未变的合法直连自愈绝不能被误当改绑拒发: %v", err)
		}
		_ = err
		afterP, afterD := hits()
		if afterD <= beforeD {
			t.Fatalf("合法直连自愈必须真实直出（directHits %d -> %d）", beforeD, afterD)
		}
		for _, k := range []string{"http://rw1:8080", "http://rw2:8080"} {
			if afterP[k] != beforeP[k] {
				t.Fatalf("直连自愈绝不能走代理 %q（%d -> %d）", k, beforeP[k], afterP[k])
			}
		}
	})

	t.Run("自愈等待窗口真实改绑立即拒发且零外发", func(t *testing.T) {
		// R10-3（Reviewer M4 判据④ 反面）：自愈挑中 picked=91、冲突复核
		// 时绑定仍是 91，等锁期间管理员真实改绑到 92——cur.ProxyID !=
		// expectedDBProxyID，fail-closed 拒发，对两个出口零发送零直出。
		id := mkAcct("rwhealrebind", "hr1")
		bindAccountProxy(id, 92) // DB 已是改绑后的 92
		acct := accountByID(id)
		beforeP, beforeD := hits()
		_, _, err := rotateAccountWithOpts(*acct, rotateOpts{
			hasPickedProxy:    true,
			pickedProxyID:     91,
			expectedDBProxyID: 91, // 冲突复核时的绑定事实
		})
		if err == nil {
			t.Fatal("等待窗口真实改绑后自愈必须 fail-closed 拒发")
		}
		if !strings.Contains(err.Error(), "不再合法") {
			t.Fatalf("真实改绑的拒绝必须点名出口不再合法: %v", err)
		}
		afterP, afterD := hits()
		for _, k := range []string{"http://rw1:8080", "http://rw2:8080"} {
			if afterP[k] != beforeP[k] {
				t.Fatalf("被拒的自愈绝不能向代理 %q 外发（%d -> %d）", k, beforeP[k], afterP[k])
			}
		}
		if afterD != beforeD {
			t.Fatalf("被拒的自愈绝不能直出（directHits %d -> %d）", beforeD, afterD)
		}
	})

	t.Run("等待窗口内解锁为未绑定后旧绑定出口被拒而当前出口重算放行", func(t *testing.T) {
		// 对照 1：绑定未变（a.ProxyID == cur.ProxyID）时按 cur 重算出口
		// 正常轮转——本例账号绑定 91 未变，1PSIDTS 换发打向 91 的代理
		// 客户端（stdlib 计数命中）。
		id := mkAcct("rwsame", "s1")
		bindAccountProxy(id, 91)
		acct := accountByID(id)
		beforeP, beforeD := hits()
		_, _, err := rotateAccountOpts(*acct, false)
		// 上游桩回 200 空体：1PSIDTS 换发可能拿到空票据返回错误，但出口
		// 计数必须证明请求走了当前绑定 91 而非其它。
		_ = err
		afterP, afterD := hits()
		if afterP["http://rw1:8080"] <= beforeP["http://rw1:8080"] {
			t.Fatalf("绑定未变时轮转必须走当前绑定 91 的出口: before=%v after=%v", beforeP, afterP)
		}
		if afterP["http://rw2:8080"] != beforeP["http://rw2:8080"] {
			t.Fatal("绑定未变时绝不能走错到代理 92")
		}
		if afterD != beforeD {
			t.Fatalf("绑定未变时绝不能直连外发: directHits %d -> %d", beforeD, afterD)
		}
	})

}

// TestRotateActiveFirstBindRequiresAutoSource 复核 R9-4（Reviewer MF-4）：
// orig=0 的自有槽「0 -> P 合法首绑」例外必须验证可核自动绑定来源
// （isAutoBoundProxyForURL）——管理员手工 PATCH 恰好绑到本槽出口 P 时
// 失效钩子无槽可摘、步骤 6 不对齐、发布时 orig 仍是 0，仅凭 URL 相等
// 放行会把手工改绑洗成合法首绑。同一 P 做自动/手工正反对照。
func TestRotateActiveFirstBindRequiresAutoSource(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 95, Name: "fb1", URL: "http://fb1:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookie := "SAPISID=fbx; __Secure-1PSID=fbp; SID=fbs; __Secure-1PSIDTS=fbt"
	mkSnap := func(id int64) activeSlotSnapshot {
		return activeSlotSnapshot{
			accountID: id, bcID: "bc-fb", proxyURL: "http://fb1:8080",
			allowDirect: false, boundProxyID: 95, origAccountProxyID: 0,
			ident: cookieIdentity(cookie),
		}
	}

	t.Run("自动首绑到本槽出口的合法对照放行", func(t *testing.T) {
		id, err := accountInsert("fbauto", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		// 可核自动绑定通道：autoBindAccountProxy 真实落库并登记来源。
		autoBindAccountProxy(id, 95)
		if !isAutoBoundProxyForURL(id, 95, "http://fb1:8080") {
			t.Fatal("前置：自动首绑必须登记可核来源记录")
		}
		cur := accountByID(id)
		if _, err := resolveActiveRotateProxy(cur, mkSnap(id), true); err != nil {
			t.Fatalf("可核自动首绑（0 -> 本槽出口 P）必须放行续期: %v", err)
		}
	})

	t.Run("手工改绑恰好落到本槽出口绝不放行", func(t *testing.T) {
		id, err := accountInsert("fbmanual", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		// 管理员手工 PATCH：URL 与自动场景完全相同，但来源记录被清除——
		// 修复前仅凭 URL 相等放行，把手工改绑洗成合法首绑。
		bindAccountProxy(id, 95)
		if isAutoBoundProxyForURL(id, 95, "http://fb1:8080") {
			t.Fatal("前置：手工 PATCH 必须清除自动绑定来源记录")
		}
		cur := accountByID(id)
		if _, err := resolveActiveRotateProxy(cur, mkSnap(id), true); err == nil {
			t.Fatal("手工改绑恰好落到本槽出口的 orig=0 自有槽必须 fail-closed 拒绝续期")
		} else if !strings.Contains(err.Error(), "自动绑定来源") {
			t.Fatalf("拒绝必须点名无可核自动绑定来源: %v", err)
		}
	})

	t.Run("同身份兄弟号的独立出口准入不受影响", func(t *testing.T) {
		// 负对照保护：借用兄弟号快照（own=false）不受 R9-4 自有槽首绑
		// 来源门槛影响——未绑定账号借用池中启用代理快照照常放行。
		id, err := accountInsert("fbunbound", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		cur := accountByID(id) // ProxyID=0 未绑定
		snap := activeSlotSnapshot{
			accountID: 999999, bcID: "bc-fb-sib", proxyURL: "http://fb1:8080",
			allowDirect: false, boundProxyID: 95, origAccountProxyID: 0,
			ident: cookieIdentity(cookie),
		}
		if _, err := resolveActiveRotateProxy(cur, snap, true); err != nil {
			t.Fatalf("未绑定账号借用兄弟号启用代理快照必须放行（独立出口准入门）: %v", err)
		}
	})
}

// TestRotateSiblingCandidateListSkipsIllegalAndPicksLegal 复核 R9-8
// （Reviewer SF-x）：同身份多个活跃槽必须按候选列表逐个复核——A、B
// 同身份分别走 P1/P2，非驻留兄弟 C 绑 P1 时，即便候选切片里不合法
// 的 B 在前，也必须借到合法的 A；全部候选不合法必须跳过，绝不回退
// 直连。修复前 byIdent 按 Go map 遍历序互相覆盖只留一个快照：恰留
// B 时 C 被整轮静默跳过。
func TestRotateSiblingCandidateListSkipsIllegalAndPicksLegal(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 96, Name: "cl1", URL: "http://cl1:8080", Enabled: true},
		{ID: 97, Name: "cl2", URL: "http://cl2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookie := "SAPISID=clx; __Secure-1PSID=clp; SID=cls; __Secure-1PSIDTS=clt"
	ident := cookieIdentity(cookie)
	idA, err := accountInsert("clA", cookie, "")
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	defer accountDelete(idA)
	idB, err := accountInsert("clB", cookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	idC, err := accountInsert("clC", cookie, "")
	if err != nil {
		t.Fatalf("insert C: %v", err)
	}
	defer accountDelete(idC)
	// A 走 P1(96)、B 走 P2(97)、非驻留 C 绑 P1(96)。
	bindAccountProxy(idA, 96)
	bindAccountProxy(idB, 97)
	bindAccountProxy(idC, 96)

	// 注册 A、B 的驻留登录槽（跨身份同会话）：快照列表必须能同时看到
	// 两个候选（修复前 byIdent 互相覆盖只剩一个）。
	m := browserMgr
	regSlot := func(id int64, proxyURL string, boundID int64) (browserSlotKey, activeSlotSnapshot) {
		t.Helper()
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     ident,
			browserContextID:   fmt.Sprintf("bc-cl-%d", id),
			targetID:           fmt.Sprintf("t-cl-%d", id),
			sessionID:          fmt.Sprintf("sess-cl-%d", id),
			proxyURL:           proxyURL,
			allowDirect:        false,
			origAccountProxyID: boundID,
			boundProxyID:       boundID,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		return key, activeSlotSnapshot{
			accountID: id, bcID: ac.browserContextID, proxyURL: proxyURL,
			allowDirect: false, boundProxyID: boundID, origAccountProxyID: boundID,
			ident: ident,
		}
	}
	keyA, snapA := regSlot(idA, "http://cl1:8080", 96)
	keyB, snapB := regSlot(idB, "http://cl2:8080", 97)
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, keyA)
		delete(m.ctxs, keyB)
		m.mu.Unlock()
	}()

	t.Run("byIdent保留同身份全部候选且按账号升序", func(t *testing.T) {
		_, byIdent := m.ActiveAccountSlotSnapshots()
		list := byIdent[ident]
		if len(list) != 2 {
			t.Fatalf("同身份两个活跃槽必须全部保留为候选，得到 %d", len(list))
		}
		if list[0].accountID != idA || list[1].accountID != idB {
			t.Fatalf("候选必须按 accountID 升序确定排序: %+v", list)
		}
	})

	t.Run("绑P1的C借到合法A而非被前列B卡死", func(t *testing.T) {
		// 候选顺序 B 在前（最坏情况）：B 出口 P2 与 C 的绑定 P1 冲突，
		// 修复前快照表恰留 B 时 C 被整轮跳过；候选列表版必须跳过 B
		// 选到合法的 A（P1）。
		cur := accountByID(idC)
		proxyURL, chosen, ok := pickRotateCandidate(cur, []activeSlotSnapshot{snapB, snapA})
		if !ok {
			t.Fatal("存在合法候选 A 时绝不能整轮跳过")
		}
		if chosen.accountID != idA || proxyURL != "http://cl1:8080" {
			t.Fatalf("必须借到合法的 A（P1），得到 accountID=%d url=%q", chosen.accountID, proxyURL)
		}
		// 顺序无关：A 在前同样选中 A。
		if _, chosen2, ok2 := pickRotateCandidate(cur, []activeSlotSnapshot{snapA, snapB}); !ok2 || chosen2.accountID != idA {
			t.Fatal("候选顺序不影响合法挑选结果")
		}
	})

	t.Run("全部候选不合法必须跳过绝不回退直连", func(t *testing.T) {
		cur := accountByID(idC)
		if _, _, ok := pickRotateCandidate(cur, []activeSlotSnapshot{snapB}); ok {
			t.Fatal("唯一候选 B 出口与 C 绑定冲突时必须跳过（绝不借非法出口）")
		}
		// 候选槽已摘除（过期快照）同样跳过。
		staleB := snapB
		staleB.bcID = "bc-cl-gone"
		if _, _, ok := pickRotateCandidate(cur, []activeSlotSnapshot{staleB, snapB}); ok {
			t.Fatal("过期快照与非法出口的组合必须整体跳过")
		}
	})
}

func TestMergeSetCookieRefreshes1PSIDTS(t *testing.T) {
	base := "SID=a; __Secure-1PSID=psid; __Secure-1PSIDTS=oldts; SAPISID=sap"
	got := mergeSetCookie(base, []string{
		"__Secure-1PSIDTS=newts; Domain=.google.com; Secure; HttpOnly",
		"__Secure-3PSIDTS=new3; Domain=.google.com; Secure; HttpOnly",
	})
	if !strings.Contains(got, "__Secure-1PSIDTS=newts") {
		t.Errorf("1PSIDTS 没刷新: %s", got)
	}
	if strings.Contains(got, "oldts") {
		t.Errorf("旧 1PSIDTS 还在: %s", got)
	}
	if !strings.Contains(got, "__Secure-3PSIDTS=new3") {
		t.Errorf("3PSIDTS 没追加: %s", got)
	}
	for _, want := range []string{"SID=a", "__Secure-1PSID=psid", "SAPISID=sap"} {
		if !strings.Contains(got, want) {
			t.Errorf("丢了 %q: %s", want, got)
		}
	}
}

func TestAllow1PSIDTSRotateThrottle(t *testing.T) {
	const id int64 = -42
	defer func() {
		psidtsMu.Lock()
		delete(psidtsLastAt, id)
		psidtsMu.Unlock()
	}()
	if ok, _ := allow1PSIDTSRotate(id); !ok {
		t.Fatal("第一次应该放行")
	}
	note1PSIDTSAttempt(id, nil)
	ok, wait := allow1PSIDTSRotate(id)
	if ok {
		t.Fatal("60s 内应拦截")
	}
	if wait <= 0 || wait > min1PSIDTSInterval {
		t.Fatalf("wait=%s", wait)
	}
}

func TestNote1PSIDTSAttemptSkipsNetworkError(t *testing.T) {
	const id int64 = -43
	defer func() {
		psidtsMu.Lock()
		delete(psidtsLastAt, id)
		psidtsMu.Unlock()
	}()
	note1PSIDTSAttempt(id, fmt.Errorf("dial tcp timeout"))
	if ok, _ := allow1PSIDTSRotate(id); !ok {
		t.Fatal("网络错误不该记节流")
	}
	note1PSIDTSAttempt(id, fmt.Errorf("RotateCookies 1PSIDTS 返回 HTTP 401（x）"))
	if ok, _ := allow1PSIDTSRotate(id); ok {
		t.Fatal("401 应该记节流")
	}
}

func TestUniqueKeepOrder(t *testing.T) {
	got := uniqueKeepOrder([]string{"SIDCC", "__Secure-1PSIDTS", "SIDCC", "", "__Secure-1PSIDTS"})
	if strings.Join(got, ",") != "SIDCC,__Secure-1PSIDTS" {
		t.Fatalf("got %v", got)
	}
}

func TestMin1PSIDTSInterval(t *testing.T) {
	if min1PSIDTSInterval < 60*time.Second {
		t.Fatalf("地板太短会 429: %s", min1PSIDTSInterval)
	}
}

// #73 的原样响应体：等待秒数从 identity.hfcr 取；取不到或越界用默认轮转间隔。
func TestRotateRetryAfter(t *testing.T) {
	cases := []struct {
		body string
		want time.Duration
	}{
		{")]}'\n\n[[\"identity.hfcr\",600],[\"di\",12]]", 600 * time.Second},
		{")]}'\n\n[[\"identity.hfcr\",120]]", 120 * time.Second},
		{")]}'\n\n[[\"identity.hfcr\",5]]", defaultRotateInterval},
		{")]}'\n\n[[\"di\",12]]", defaultRotateInterval},
		{"", defaultRotateInterval},
	}
	for _, c := range cases {
		if got := rotateRetryAfter([]byte(c.body)); got != c.want {
			t.Errorf("rotateRetryAfter(%q) = %s, want %s", c.body, got, c.want)
		}
	}
}

func TestSIDCCBackoff(t *testing.T) {
	const id = 987654
	if sidccBackoffLeft(id) != 0 {
		t.Fatal("fresh account should not be in backoff")
	}
	noteSIDCCBackoff(id, time.Minute)
	if left := sidccBackoffLeft(id); left <= 50*time.Second || left > time.Minute {
		t.Fatalf("want ~1m backoff after 429, got %s", left)
	}
	noteSIDCCBackoff(id, -time.Second)
	if sidccBackoffLeft(id) != 0 {
		t.Fatal("backoff should end once the deadline passes")
	}
}

// stubRotateUpstream 按请求路径/body 分派假 RotateCookies 应答：
//   - POST 哨兵 [000,...]（1PSIDTS 换发）→ psidtsStatus + psidtsSetCookie；
//   - GET /RotateCookiesPage → 合法 init(...) 页面；
//   - POST [658,...]（SIDCC 换发）→ 200 + sidccSetCookie。
func stubRotateUpstream(t *testing.T, psidtsStatus int, psidtsSetCookie, sidccSetCookie []string) {
	t.Helper()
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		h := fhttp.Header{}
		resp := func(status int, body string, setCookies []string) (*fhttp.Response, error) {
			for _, sc := range setCookies {
				h.Add("Set-Cookie", sc)
			}
			return &fhttp.Response{
				StatusCode: status, Status: fmt.Sprintf("%d", status),
				Header: h, Body: io.NopCloser(strings.NewReader(body)),
			}, nil
		}
		if req.Method == "GET" && strings.Contains(req.URL.Path, "/RotateCookiesPage") {
			return resp(200, `init('abc1234', 658.0, 0.0, 0.0, 600.0)`, nil)
		}
		body := ""
		if req.Body != nil {
			b, _ := io.ReadAll(req.Body)
			body = string(b)
		}
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			if strings.Contains(body, `[000,`) {
				// 哨兵 1PSIDTS 换发：按注入状态码应答。
				if psidtsStatus != 200 {
					return resp(psidtsStatus, `[["er",401]]`, nil)
				}
				return resp(200, ")]}'\n\n[]", psidtsSetCookie)
			}
			// [658,...] SIDCC 换发。
			return resp(200, ")]}'\n\n[]", sidccSetCookie)
		}
		return resp(404, "", nil)
	}}
	stubTLSRotateClient(t, fake)
}

// TestResolveActiveRotateProxyFreshnessAndAuthorization 验证 fix #2 的
// 快照出口核验：代理 URL 变更（仍 enabled）使快照立即失效；锁直连的
// 兄弟号绝不借走代理的快照、绑定了代理的兄弟号绝不借直连/异代理快照；
// 直连快照绝不静默回退到不同出口代理。
func TestResolveActiveRotateProxyFreshnessAndAuthorization(t *testing.T) {
	// 代理池快照：P1 初始 URL 为 p-old。
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 11, Name: "p1", URL: "http://p-old:8080", Enabled: true},
		{ID: 12, Name: "p2", URL: "http://p2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.FallbackDirect = false })

	mkAcct := func(id, pid int64) *CookieAccount {
		return &CookieAccount{ID: id, ProxyID: pid, Status: "enabled"}
	}
	// R4-1：自有槽快照带上建槽时的原绑定（账号 900 绑定 P1=11）——
	// origAccountProxyID 与当前绑定一致才代表「未改绑」的新鲜快照。
	proxySnap := activeSlotSnapshot{
		accountID: 900, bcID: "bc-x", proxyURL: "http://p-old:8080",
		boundProxyID: 11, origAccountProxyID: 11, ident: "s|p",
	}
	directSnap := activeSlotSnapshot{accountID: 900, bcID: "bc-y", proxyURL: "", allowDirect: true, ident: "s|p"}

	t.Run("绑定代理 URL 变更快照失效", func(t *testing.T) {
		// 快照后管理员把代理 11 的 URL 从 p-old 改成 p-new（代理仍 enabled）：
		// 只验 enabled 不够，必须核对实时 URL——旧快照 fail-closed 拒绝。
		proxyMu.Lock()
		proxyCache[0].URL = "http://p-new:8080"
		proxyMu.Unlock()
		defer func() {
			proxyMu.Lock()
			proxyCache[0].URL = "http://p-old:8080"
			proxyMu.Unlock()
		}()
		cur := mkAcct(900, 11) // 自有快照路径同样核验新鲜度
		if _, err := resolveActiveRotateProxy(cur, proxySnap, true); err == nil {
			t.Fatal("代理实时 URL 变更后旧快照必须失效（fail-closed）")
		}
	})

	t.Run("绑定代理被禁用快照失效", func(t *testing.T) {
		proxyMu.Lock()
		proxyCache[0].Enabled = false
		proxyMu.Unlock()
		defer func() {
			proxyMu.Lock()
			proxyCache[0].Enabled = true
			proxyMu.Unlock()
		}()
		cur := mkAcct(900, 11)
		if _, err := resolveActiveRotateProxy(cur, proxySnap, true); err == nil {
			t.Fatal("绑定代理禁用后快照必须失效（fail-closed）")
		}
	})

	t.Run("锁直连兄弟号不借走代理快照", func(t *testing.T) {
		// B 与 A 同身份但锁定直连（ProxyID=-1）：借 A 的走代理 snap 等于把
		// B 的 Cookie 发向代理，违反锁直连承诺——必须拒绝。
		cur := mkAcct(901, proxyIDDirect)
		if _, err := resolveActiveRotateProxy(cur, proxySnap, true); err == nil {
			t.Fatal("锁直连账号绝不能借用走代理的兄弟号快照")
		}
		// 直连快照对锁直连账号合法。
		if url, err := resolveActiveRotateProxy(cur, directSnap, true); err != nil || url != "" {
			t.Fatalf("锁直连账号借直连快照应为直连，得到 %q err=%v", url, err)
		}
	})

	t.Run("绑定代理账号不借直连或异代理快照", func(t *testing.T) {
		cur := mkAcct(902, 12) // 绑定 p2
		// 借直连快照 → 违反代理绑定承诺。
		if _, err := resolveActiveRotateProxy(cur, directSnap, true); err == nil {
			t.Fatal("绑定代理的账号绝不能借用直连快照")
		}
		// 借指向其它代理（p-old=代理11）的快照 → 拒绝。
		if _, err := resolveActiveRotateProxy(cur, proxySnap, true); err == nil {
			t.Fatal("绑定代理的账号绝不能借用指向其它代理的快照")
		}
		// 借指向自身绑定代理的快照 → 放行。
		ownSnap := activeSlotSnapshot{
			accountID: 900, bcID: "bc-z", proxyURL: "http://p2:8080",
			boundProxyID: 12, ident: "s|p",
		}
		if url, err := resolveActiveRotateProxy(cur, ownSnap, true); err != nil || url != "http://p2:8080" {
			t.Fatalf("绑定代理的账号借自身绑定出口快照应放行，得到 %q err=%v", url, err)
		}
	})

	t.Run("直连快照不静默回退其它出口", func(t *testing.T) {
		// 未绑定账号但池非空且禁直连：直连快照不可用（fail-closed），绝不
		// 静默换成池里的代理。
		cur := mkAcct(903, 0)
		if _, err := resolveActiveRotateProxy(cur, directSnap, true); err == nil {
			t.Fatal("禁直连池规则下直连快照绝不能静默放行/回退代理")
		}
		// 未绑定账号借走代理快照：出口与池规则选出的第一个可用代理一致才放行。
		if url, err := resolveActiveRotateProxy(cur, proxySnap, true); err != nil || url != "http://p-old:8080" {
			t.Fatalf("未绑定账号借用与池规则一致的代理快照应放行，得到 %q err=%v", url, err)
		}
		// Oracle MF-2 语义修正：未绑定账号从未承诺固定出口——借用任何
		// 「池中当前启用且实时 URL 一致」的代理快照（即便不是第一个可用代理）
		// 都必须放行，绝不拿瞬时 firstUsableProxyURL 做相等比对导致整轮静默跳过。
		otherPool := activeSlotSnapshot{accountID: 900, bcID: "bc-w", proxyURL: "http://p2:8080", ident: "s|p"}
		if url, err := resolveActiveRotateProxy(cur, otherPool, true); err != nil || url != "http://p2:8080" {
			t.Fatalf("未绑定账号借用池中另一启用代理的快照必须放行，得到 %q err=%v", url, err)
		}
		// 非池代理的快照仍严格拒绝（fail-closed）。
		foreign := activeSlotSnapshot{accountID: 900, bcID: "bc-v", proxyURL: "http://not-in-pool:8080", ident: "s|p"}
		if _, err := resolveActiveRotateProxy(cur, foreign, true); err == nil {
			t.Fatal("借用非池代理的快照必须拒绝（fail-closed）")
		}
	})

	t.Run("自有走代理快照直接复用", func(t *testing.T) {
		cur := mkAcct(900, 11)
		if url, err := resolveActiveRotateProxy(cur, proxySnap, true); err != nil || url != "http://p-old:8080" {
			t.Fatalf("自有快照对齐出口应放行，得到 %q err=%v", url, err)
		}
	})
}

// TestRedactProxyURLNeverLeaksUserinfo 复核 R3-3（Reviewer MF-3）：携带
// userinfo 用户名/密码的代理 URL 在任何出口拒绝错误与诊断里绝不泄露明文
// 凭据——错误文本只含账号 ID / 代理 ID / 脱敏后的 scheme://host。
func TestRedactProxyURLNeverLeaksUserinfo(t *testing.T) {
	if got := redactProxyURL("http://alice:s3cretPass@10.0.0.1:3128"); got != "http://10.0.0.1:3128" {
		t.Fatalf("redactProxyURL 必须剥离 userinfo，得到 %q", got)
	}
	if strings.Contains(redactProxyURL("http://alice:s3cretPass@10.0.0.1:3128"), "alice") ||
		strings.Contains(redactProxyURL("http://alice:s3cretPass@10.0.0.1:3128"), "s3cretPass") {
		t.Fatal("redactProxyURL 泄露了用户名或密码")
	}
	if got := redactProxyURL("http://%zz"); got != "<redacted>" {
		t.Fatalf("解析失败必须返回 <redacted>，得到 %q", got)
	}
	if got := redactProxyURL(""); got != "<direct>" {
		t.Fatalf("空 URL 必须返回 <direct>，得到 %q", got)
	}
	// R4-4：Path/RawPath/RawQuery/Fragment 同样必须彻底剥离（代理池校验
	// 接受带路径的 URL，私有路径绝不能进保活日志/错误串）。
	if got := redactProxyURL("http://alice:s3cretPass@10.0.0.1:3128/secret-path?token=xyz#frag"); got != "http://10.0.0.1:3128" {
		t.Fatalf("带路径/查询/片段的代理 URL 必须只剩 scheme://host，得到 %q", got)
	}
	if strings.Contains(redactProxyURL("http://alice:s3cretPass@10.0.0.1:3128/secret-path?token=xyz#frag"), "secret-path") ||
		strings.Contains(redactProxyURL("http://alice:s3cretPass@10.0.0.1:3128/secret-path?token=xyz#frag"), "xyz") {
		t.Fatal("redactProxyURL 泄露了路径或查询参数")
	}
	// 无 scheme 或 无 host 同样 <redacted>。
	if got := redactProxyURL("10.0.0.1:3128"); got != "<redacted>" {
		t.Fatalf("无 scheme 的代理 URL 必须返回 <redacted>，得到 %q", got)
	}

	// 行为级：走 resolveActiveRotateProxy 的拒绝分支，错误文本绝不能含凭据。
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 21, Name: "cred1", URL: "http://alice:s3cretPass@10.0.0.1:3128", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.FallbackDirect = false })

	mkAcct := func(id, pid int64) *CookieAccount {
		return &CookieAccount{ID: id, ProxyID: pid, Status: "enabled"}
	}
	check := func(err error, label string) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: 预期拒绝错误", label)
		}
		if strings.Contains(err.Error(), "alice") || strings.Contains(err.Error(), "s3cretPass") {
			t.Fatalf("%s: 错误泄露代理凭据: %v", label, err)
		}
	}

	// (1) 锁直连账号撞上带凭据的走代理快照。
	credSnap := activeSlotSnapshot{
		accountID: 800, bcID: "bc-cred", proxyURL: "http://alice:s3cretPass@10.0.0.1:3128",
		boundProxyID: 21, ident: "s|p",
	}
	check(func() error { _, e := resolveActiveRotateProxy(mkAcct(801, proxyIDDirect), credSnap, true); return e }(),
		"锁直连拒绝")
	// (2) 绑定其它代理的账号撞上带凭据的异代理快照。
	check(func() error { _, e := resolveActiveRotateProxy(mkAcct(802, 999), credSnap, true); return e }(),
		"绑定冲突拒绝")
	// (3) 绑定代理实时 URL 变更分支（快照含凭据 URL）。
	proxyMu.Lock()
	proxyCache[0].URL = "http://10.0.0.1:3128" // 实时 URL 已变（无凭据）
	proxyMu.Unlock()
	check(func() error { _, e := resolveActiveRotateProxy(mkAcct(803, 21), credSnap, true); return e }(),
		"URL 漂移拒绝")
	proxyMu.Lock()
	proxyCache[0].URL = "http://alice:s3cretPass@10.0.0.1:3128"
	proxyMu.Unlock()
	// (4) 未绑定账号借用非池代理快照。
	foreign := activeSlotSnapshot{
		accountID: 800, bcID: "bc-cred2", proxyURL: "http://alice:s3cretPass@notpool:8080",
		ident: "s|p",
	}
	check(func() error { _, e := resolveActiveRotateProxy(mkAcct(804, 0), foreign, true); return e }(),
		"非池快照拒绝")
	// 对照：凭据 URL 作为池中启用代理时可放行且返回的是原始快照 URL
	//（凭据只用于实际拨号，绝不进错误/日志文本）。
	if url, err := resolveActiveRotateProxy(mkAcct(805, 0), credSnap, true); err != nil ||
		url != "http://alice:s3cretPass@10.0.0.1:3128" {
		t.Fatalf("启用池代理快照应放行，得到 %q err=%v", url, err)
	}
}

// TestResolveActiveRotateProxyNonStrictOverflowOwnSlot 复核 R3-4（Reviewer
// MF-2）：非严格模式下账号绑定健康代理 P1 但并发槽满时 acquireSlot 合法
// 溢出到 P2 建槽——自有活跃槽按快照实际出口 P2 放行续期，绝不因「不等于
// 绑定代理」每轮静默跳过；严格模式仍精确匹配绑定代理；兄弟号借用异代理
// 快照仍拒绝（绑定冲突 fail-closed）。
func TestResolveActiveRotateProxyNonStrictOverflowOwnSlot(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 31, Name: "ovp1", URL: "http://ovp1:8080", Enabled: true},
		{ID: 32, Name: "ovp2", URL: "http://ovp2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()

	mkAcct := func(id, pid int64) *CookieAccount {
		return &CookieAccount{ID: id, ProxyID: pid, Status: "enabled"}
	}
	// 自有槽（snap.accountID == cur.ID）实际出口是溢出代理 P2。
	// R4-1：快照带上建槽时的原绑定（P1=31）——合法溢出以「管理员未改绑」
	// 为前提。
	overflowSnap := activeSlotSnapshot{
		accountID: 700, bcID: "bc-ov", proxyURL: "http://ovp2:8080",
		boundProxyID: 32, origAccountProxyID: 31, ident: "s|p",
	}

	t.Run("非严格模式合法溢出自有槽放行", func(t *testing.T) {
		setRuntimeCfg(t, func(c *RuntimeConfig) {
			c.StrictProxyBinding = false
			c.FallbackDirect = false
		})
		cur := mkAcct(700, 31) // 绑定 P1，自有槽实际走 P2
		if url, err := resolveActiveRotateProxy(cur, overflowSnap, true); err != nil || url != "http://ovp2:8080" {
			t.Fatalf("非严格溢出自有槽必须沿实际出口续期，得到 %q err=%v", url, err)
		}
		// 对照：溢出到的代理已禁用时仍 fail-closed。
		proxyMu.Lock()
		proxyCache[1].Enabled = false
		proxyMu.Unlock()
		if _, err := resolveActiveRotateProxy(cur, overflowSnap, true); err == nil {
			t.Fatal("溢出代理已禁用的快照必须拒绝（fail-closed）")
		}
		proxyMu.Lock()
		proxyCache[1].Enabled = true
		proxyMu.Unlock()
	})

	t.Run("严格模式精确匹配绑定代理", func(t *testing.T) {
		setRuntimeCfg(t, func(c *RuntimeConfig) {
			c.StrictProxyBinding = true
			c.FallbackDirect = false
		})
		cur := mkAcct(700, 31)
		if _, err := resolveActiveRotateProxy(cur, overflowSnap, true); err == nil {
			t.Fatal("严格模式下非绑定代理的自有槽快照必须拒绝（fail-closed）")
		}
	})

	t.Run("兄弟号借用异代理快照仍拒绝", func(t *testing.T) {
		setRuntimeCfg(t, func(c *RuntimeConfig) {
			c.StrictProxyBinding = false
			c.FallbackDirect = false
		})
		// B（id 701）绑定 P1，借用兄弟号 A 走 P2 的快照——溢出宽限只适用
		// 自有槽，跨号借用异代理快照仍按绑定冲突 fail-closed。
		cur := mkAcct(701, 31)
		if _, err := resolveActiveRotateProxy(cur, overflowSnap, true); err == nil {
			t.Fatal("兄弟号借用异代理快照必须拒绝（fail-closed）")
		}
	})

	t.Run("取快照后管理员改绑一律拒绝", func(t *testing.T) {
		setRuntimeCfg(t, func(c *RuntimeConfig) {
			c.StrictProxyBinding = false
			c.FallbackDirect = false
		})
		// R4-1：快照后管理员把绑定从 P1 改成 P2——即便 P2 恰好是溢出出口
		// 本身，快照事实也已过期（origAccountProxyID=31 != cur.ProxyID=32），
		// 与前台 readySlotExitViolation 同口径 fail-closed。
		cur := mkAcct(700, 32)
		if _, err := resolveActiveRotateProxy(cur, overflowSnap, true); err == nil {
			t.Fatal("管理员改绑后过期快照绝不能被当成合法溢出放行（R4-1）")
		}
		// 改绑到第三个代理同样拒绝。
		proxyMu.Lock()
		proxyCache = append(proxyCache, Proxy{ID: 33, Name: "ovp3", URL: "http://ovp3:8080", Enabled: true})
		proxyMu.Unlock()
		defer func() {
			proxyMu.Lock()
			proxyCache = proxyCache[:2]
			proxyMu.Unlock()
		}()
		cur = mkAcct(700, 33)
		if _, err := resolveActiveRotateProxy(cur, overflowSnap, true); err == nil {
			t.Fatal("管理员改绑到第三代理后过期快照必须拒绝（R4-1）")
		}
	})
}

// TestResolveActiveRotateProxyUnboundFollowsPoolEnabledSnapshot 复核 R3-4
// （Oracle MF-2）：未绑定账号（ProxyID==0）从未承诺固定出口——即便
// firstUsableProxyURL 因并发 fail_count 熔断/冷却选出了不同的第一个可用
// 代理，借用兄弟号「池中仍启用且实时 URL 一致」的快照必须放行，绝不整轮
// 静默跳过保活。
// 复现失败特征（修复前）：fallback=firstUsableProxyURL()=p2（p1 被熔断）与
// snap.proxyURL=p1 字符串不等 → 拒绝，本轮保活跳过。
func TestResolveActiveRotateProxyUnboundFollowsPoolEnabledSnapshot(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 41, Name: "up1", URL: "http://up1:8080", Enabled: true, FailCount: 0},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.FallbackDirect = false
		c.StrictProxyBinding = false
	})

	cur := &CookieAccount{ID: 900, ProxyID: 0, Status: "enabled"}
	snap := activeSlotSnapshot{
		accountID: 901, bcID: "bc-un", proxyURL: "http://up1:8080",
		boundProxyID: 41, ident: "s|p",
	}

	// 模拟并发请求把 p1 打到熔断冷却，并追加第二个可用代理 p2——
	// firstUsableProxyURL() 此刻返回 p2，与快照 p1 不再相等。
	proxyMu.Lock()
	proxyCache[0].FailCount = proxyFailThreshold
	proxyCache[0].LastUsed = time.Now().Unix()
	proxyCache = append(proxyCache, Proxy{ID: 42, Name: "up2", URL: "http://up2:8080", Enabled: true})
	proxyMu.Unlock()
	if got := firstUsableProxyURL(); got != "http://up2:8080" {
		t.Fatalf("前置：firstUsableProxyURL 应选出 p2，得到 %q", got)
	}

	// 快照出口 p1 仍 enabled 且实时 URL 一致——必须放行（未绑定账号无固定
	// 出口承诺，池选谁是池的事）。
	if url, err := resolveActiveRotateProxy(cur, snap, true); err != nil || url != "http://up1:8080" {
		t.Fatalf("未绑定账号借用仍启用的池代理快照必须放行（Oracle MF-2），得到 %q err=%v", url, err)
	}
}

// TestRotateSharedPSIDTSNeverCrossInjects3PSIDTS 复核 R3-6（Oracle MF-3）：
// sharedPSIDTS 跨同身份兄弟号共享换发结果时，A 会话的 __Secure-3PSIDTS
// 绝不能进 B 的 DB——cookieIdentity 看不见 3PSID 族，同 profile 下 /u/0
// 与 /u/1 各自有独立 3PSID，跨号注入即异身份串号（账号关联风控）。
// 1PSIDTS 共享续期不受影响。
// 复现失败特征（修复前）：psidtsOnlySetCookie 白名单放行 __Secure-3PSIDTS，
// B 的 DB cookie 出现 ts3fromA。
func TestRotateSharedPSIDTSNeverCrossInjects3PSIDTS(t *testing.T) {
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = false // 无槽路径：纯 Go 保活，聚焦共享缓存注入判定
		c.FallbackDirect = true
	})

	// A、B 同 cookieIdentity（SAPISID+__Secure-1PSID 相同）但 3PSID 不同——
	// 同 profile 下 /u/0 与 /u/1 的常态。
	cookieA := "SAPISID=sm3; __Secure-1PSID=pm3; SID=sx3; __Secure-3PSID=psidA; __Secure-1PSIDTS=oldts"
	cookieB := "SAPISID=sm3; __Secure-1PSID=pm3; SID=sx3; __Secure-3PSID=psidB; __Secure-1PSIDTS=oldts"
	idA, err := accountInsert("mf3A", cookieA, "")
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	defer accountDelete(idA)
	idB, err := accountInsert("mf3B", cookieB, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)

	// 白名单本身先断言：只放行 1PSIDTS。
	filtered := psidtsOnlySetCookie([]string{
		"__Secure-1PSIDTS=ts1; Domain=.google.com; Path=/; Secure",
		"__Secure-3PSIDTS=ts3fromA; Domain=.google.com; Path=/; Secure",
	})
	if len(filtered) != 1 || !strings.Contains(filtered[0], "__Secure-1PSIDTS=ts1") {
		t.Fatalf("白名单必须只放行 1PSIDTS，得到 %v", filtered)
	}

	// 模拟 A 先换发成功、结果写入本轮共享缓存，B 复用。R9-8 候选列表版
	// 生产语义：rotateAllAccounts 只在有活跃槽候选时才调用本函数——注册
	// A 的驻留登录槽（同 cookieIdentity）并作为唯一候选传入。
	m := browserMgr
	keyA := slotKeyForAccount(accountByID(idA), "", true)
	acA := &accountBrowserCtx{
		key:              keyA,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookieA),
		browserContextID: "bc-mf3-a",
		targetID:         "t-mf3-a",
		sessionID:        "sess-mf3-a",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	acA.readyCh = make(chan struct{})
	close(acA.readyCh)
	m.mu.Lock()
	m.ctxs[keyA] = acA
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, keyA)
		m.mu.Unlock()
	}()
	snap := activeSlotSnapshot{
		accountID: idA, bcID: "bc-mf3-a", proxyURL: "", allowDirect: true,
		ident: cookieIdentity(cookieA),
	}
	shared := map[string][]string{
		cookieIdentity(cookieA): {
			"__Secure-1PSIDTS=ts1; Domain=.google.com; Path=/; Secure",
			"__Secure-3PSIDTS=ts3fromA; Domain=.google.com; Path=/; Secure",
		},
	}
	rotateActiveOrSibling1PSIDTS(*accountByID(idB), []activeSlotSnapshot{snap}, shared)

	b := accountByID(idB)
	if b == nil {
		t.Fatal("账号 B 丢失")
	}
	if strings.Contains(b.Cookie, "ts3fromA") {
		t.Fatalf("A 会话的 3PSIDTS 绝不能跨号注入 B（Oracle MF-3 异身份串号）: %q", b.Cookie)
	}
	if !strings.Contains(b.Cookie, "__Secure-3PSID=psidB") {
		t.Fatalf("B 自己的 3PSID 必须原样保留: %q", b.Cookie)
	}
	if !strings.Contains(b.Cookie, "__Secure-1PSIDTS=ts1") {
		t.Fatalf("1PSIDTS 共享续期必须照常写库: %q", b.Cookie)
	}
}

// TestRotateSiblingRejectsReboundIdentity 复核 R5-2（Reviewer MF-2）：
// 兄弟槽资格必须按锁内重读的最新 cur 身份复核——调度取快照（A 槽驻留、
// B 与 A 同会话）后，扩展把 B 换绑到新会话（cookieIdentity 变化）时，
// 本轮 B 绝不能借 A 槽出口外发、也绝不能复用 sharedPSIDTS 缓存：
// RotateCookies 零命中、B 的 DB 零写入、A 的 Jar 零注入。
// 复现失败特征（修复前）：驻留检查只核 snap.bcID 仍在，不核快照身份——
// B 拿新会话的 Cookie 沿旧兄弟 A 的出口发出换发（或复用缓存写库）。
func TestRotateSiblingRejectsReboundIdentity(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	// A、B 同 cookieIdentity 的兄弟号（SAPISID+__Secure-1PSID 相同）。
	sharedCookie := "SAPISID=siA; __Secure-1PSID=siP; SID=sis; __Secure-1PSIDTS=oldts"
	idA, err := accountInsert("sibidA", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	defer accountDelete(idA)
	idB, err := accountInsert("sibidB", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	acctA := accountByID(idA)

	// A 槽驻留（直连出口），B 无槽。调度为 B 取到的快照 = A 槽快照。
	keyA := slotKeyForAccount(acctA, "", true)
	acA := &accountBrowserCtx{
		key:              keyA,
		epoch:            1,
		cookieIdentity:   cookieIdentity(sharedCookie),
		browserContextID: "bc-sibid-a",
		targetID:         "t-sibid-a",
		sessionID:        "sess-sibid-a",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	acA.readyCh = make(chan struct{})
	close(acA.readyCh)
	m.mu.Lock()
	m.ctxs[keyA] = acA
	m.sessionIndex["sess-sibid-a"] = acA
	m.targetIndex["t-sibid-a"] = acA
	m.mu.Unlock()
	srv.bcMu.Lock()
	srv.sessToBC["sess-sibid-a"] = "bc-sibid-a"
	srv.bcStore["bc-sibid-a"] = map[string]string{
		"SAPISID": "siA", "__Secure-1PSID": "siP", "SID": "sis",
		"__Secure-1PSIDTS": "oldts",
	}
	srv.bcMu.Unlock()
	snap := activeSlotSnapshot{
		accountID: idA, bcID: "bc-sibid-a", proxyURL: "", allowDirect: true,
		ident: cookieIdentity(sharedCookie),
	}

	// 取快照之后：扩展把 B 换绑到新会话（cookieIdentity 变化，与 accountSync
	// 的换绑写库同构）。
	newCookieB := "SAPISID=siB2; __Secure-1PSID=siP2; SID=sis2; __Secure-1PSIDTS=oldts2"
	unlock, lerr := acquireAcctCookieLock(context.Background(), idB)
	if lerr != nil {
		t.Fatalf("lock B: %v", lerr)
	}
	if _, derr := getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`, newCookieB, idB); derr != nil {
		unlock()
		t.Fatalf("模拟换绑写库: %v", derr)
	}
	unlock()
	newIdent := cookieIdentity(newCookieB)
	if newIdent == cookieIdentity(sharedCookie) {
		t.Fatal("前置：新会话身份必须与原身份不同")
	}

	// 假上游：任何误外发都会被计数（直连出口走 tls-client）。
	var hitsMu sync.Mutex
	hits := 0
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			hitsMu.Lock()
			hits++
			hitsMu.Unlock()
			h := fhttp.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=upstream-ts; Domain=.google.com; Path=/; Secure; HttpOnly")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h,
				Body:   io.NopCloser(strings.NewReader(")]}'\n\n[]")),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404",
			Header: fhttp.Header{},
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)

	beforeB := accountByID(idB).Cookie
	// 陷阱：本轮共享缓存里预置 B 新会话身份的换发结果——若实现不复核快照
	// 身份，B 会直接复用它写库（零上游命中也能观测到污染）。
	shared := map[string][]string{
		newIdent: {"__Secure-1PSIDTS=poison-shared; Domain=.google.com; Path=/; Secure"},
	}
	rotateActiveOrSibling1PSIDTS(*accountByID(idB), []activeSlotSnapshot{snap}, shared)

	hitsMu.Lock()
	gotHits := hits
	hitsMu.Unlock()
	if gotHits != 0 {
		t.Fatalf("B 已换绑新会话，绝不能借旧兄弟 A 槽出口外发换发（RotateCookies 命中 %d 次）", gotHits)
	}
	if b := accountByID(idB); b == nil || b.Cookie != beforeB {
		t.Fatalf("B 已换绑新会话，绝不能复用 sharedPSIDTS 写库: %q vs %q", b.Cookie, beforeB)
	}
	jar := srv.jarFor("sess-sibid-a")
	if jar["__Secure-1PSIDTS"] == "poison-shared" || jar["__Secure-1PSIDTS"] == "upstream-ts" {
		t.Fatalf("B 的换发绝不能注入兄弟 A 的 Jar: %q", jar["__Secure-1PSIDTS"])
	}
	// 对照：A 自己（身份未变）走同一快照必须照常放行——身份复核不误伤
	// 合法续期。shared 清空避免干扰。
	rotateActiveOrSibling1PSIDTS(*accountByID(idA), []activeSlotSnapshot{snap}, map[string][]string{})
	hitsMu.Lock()
	gotHits = hits
	hitsMu.Unlock()
	if gotHits != 1 {
		t.Fatalf("A 身份未变必须照常换发（RotateCookies 命中 1 次），得到 %d", gotHits)
	}
	if jar2 := srv.jarFor("sess-sibid-a"); jar2["__Secure-1PSIDTS"] != "upstream-ts" {
		t.Fatalf("A 自己的合法续期必须热注入 Jar: %q", jar2["__Secure-1PSIDTS"])
	}
	m.InvalidateAccount(idA, false, "test cleanup")
}

// TestRotateAccountOptsDoesNotMask1PSIDTSAuthFailure 验证 rotateAccountOpts
// 不吞 1PSIDTS 401/403（Chrome DBSC 设备绑定）：即便 SIDCC 顺带刷新落库，
// 仍返回非 nil 错误；面板 /rotate 如实回 ok=false；双路均无新票时空跑也
// 返回错误，绝不谎报“保活成功”。
func TestRotateAccountOptsDoesNotMask1PSIDTSAuthFailure(t *testing.T) {
	cookieBase := func(tag string) string {
		return "SAPISID=" + tag + "; SID=x; __Secure-1PSID=" + tag + "; __Secure-1PSIDTS=ts" + tag
	}

	t.Run("401 部分失败不吞错", func(t *testing.T) {
		stubRotateUpstream(t, 401, nil,
			[]string{"SIDCC=newcc; Domain=.google.com; Path=/; Secure"})
		id, err := accountInsert("dbsc401", cookieBase("k401"), "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		acct := accountByID(id)

		_, names, rerr := rotateAccount(*acct)
		if rerr == nil {
			t.Fatal("1PSIDTS 401（DBSC）绝不能静默吞错返回 nil")
		}
		if !strings.Contains(rerr.Error(), "HTTP 401") {
			t.Fatalf("错误必须保留 401 诊断: %v", rerr)
		}
		if !isPSIDTSAuthErr(rerr) {
			t.Fatalf("错误必须可识别为 1PSIDTS 鉴权失效: %v (%T)", rerr, rerr)
		}
		if len(names) == 0 {
			t.Fatal("SIDCC 顺带刷新的名清单不应丢失")
		}
		if !strings.Contains(rerr.Error(), "本次仅刷新了") {
			t.Fatalf("部分失败必须附注实际刷新了什么: %v", rerr)
		}
		// SIDCC 落库照做不丢票。
		if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "SIDCC=newcc") {
			t.Fatalf("SIDCC 部分刷新必须落库: %q", a.Cookie)
		}
	})

	t.Run("面板 rotate 联动 ok false", func(t *testing.T) {
		stubRotateUpstream(t, 401, nil,
			[]string{"SIDCC=panelcc; Domain=.google.com; Path=/; Secure"})
		id, err := accountInsert("dbscpanel", cookieBase("kp"), "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)

		w := httptest.NewRecorder()
		handleAdminCookiePost(w, id, "rotate")
		if w.Code != 200 {
			t.Fatalf("面板 rotate 应 200 回业务结果，得到 %d", w.Code)
		}
		var body map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("响应 JSON 解析失败: %v", err)
		}
		if ok, _ := body["ok"].(bool); ok {
			t.Fatalf("1PSIDTS 401 时面板绝不能谎报 ok=true: %v", body)
		}
		if detail, _ := body["detail"].(string); !strings.Contains(detail, "HTTP 401") {
			t.Fatalf("detail 必须带 401 诊断: %v", body)
		}
		if rf, ok := body["refreshed"].([]interface{}); !ok || len(rf) == 0 {
			t.Fatalf("refreshed 必须透出 SIDCC 部分刷新清单: %v", body)
		}
	})

	t.Run("双路无新票空跑返回错误", func(t *testing.T) {
		stubRotateUpstream(t, 200, nil, nil)
		id, err := accountInsert("emptyrotate", cookieBase("ke"), "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		acct := accountByID(id)

		_, names, rerr := rotateAccount(*acct)
		if rerr == nil {
			t.Fatal("双路均无新票的空跑绝不能返回 nil（谎报保活成功）")
		}
		if len(names) != 0 {
			t.Fatalf("空跑不应有名清单: %v", names)
		}
		if !strings.Contains(rerr.Error(), "未刷新任何 Cookie") {
			t.Fatalf("空跑错误必须点名未刷新: %v", rerr)
		}
	})

	t.Run("被忽略的删除指令头绝不谎报刷新", func(t *testing.T) {
		// R6-6（Reviewer SF-1）：上游两路只下发被合并忽略的删除指令头
		//（SIDCC 空值 + 1970 过期），cookie == cur.Cookie 无任何有效合并
		// ——绝不按头部名清单谎报「刷新了 SIDCC」：无鉴权错时返回空转
		// 错误且 refreshed 为空。Cookie 删除指令的合并保留旧值规则不变。
		stubRotateUpstream(t, 200,
			[]string{"__Secure-1PSIDTS=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Domain=.google.com; Path=/; Secure"},
			[]string{"SIDCC=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Domain=.google.com; Path=/; Secure"})
		id, err := accountInsert("delheader", cookieBase("kh"), "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		before := accountByID(id).Cookie
		acct := accountByID(id)
		_, names, rerr := rotateAccount(*acct)
		if rerr == nil {
			t.Fatal("被忽略删除头造成的空转绝不能返回 nil（谎报保活成功）")
		}
		if !strings.Contains(rerr.Error(), "未刷新任何 Cookie") {
			t.Fatalf("空转错误必须点名未刷新: %v", rerr)
		}
		if strings.Contains(rerr.Error(), "本次仅刷新了") {
			t.Fatalf("被忽略的删除头绝不能报「本次仅刷新了」: %v", rerr)
		}
		if len(names) != 0 {
			t.Fatalf("无有效合并变化时 refreshed 必须为空（不谎报头名）: %v", names)
		}
		// 删除指令合并规则不变：DB cookie 保留旧值（绝不丢票）。
		if a := accountByID(id); a == nil || a.Cookie != before {
			t.Fatalf("删除指令绝不能改库（保留旧值）: %q vs %q", a.Cookie, before)
		}
	})

	t.Run("401 叠加被忽略删除头只报 401", func(t *testing.T) {
		// R6-6（Reviewer SF-1）：1PSIDTS 401 且 SIDCC 仅下发被忽略的
		// 删除指令头——错误必须只报真实 401（无「本次仅刷新了 SIDCC」
		// 尾巴），refreshed 为空。
		stubRotateUpstream(t, 401, nil,
			[]string{"SIDCC=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Domain=.google.com; Path=/; Secure"})
		id, err := accountInsert("delauth", cookieBase("ka"), "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		acct := accountByID(id)
		_, names, rerr := rotateAccount(*acct)
		if rerr == nil {
			t.Fatal("1PSIDTS 401 绝不能返回 nil")
		}
		if !strings.Contains(rerr.Error(), "HTTP 401") {
			t.Fatalf("错误必须保留 401 诊断: %v", rerr)
		}
		if strings.Contains(rerr.Error(), "本次仅刷新了") {
			t.Fatalf("被忽略的删除头绝不能报「本次仅刷新了」: %v", rerr)
		}
		if len(names) != 0 {
			t.Fatalf("无有效合并变化时 refreshed 必须为空: %v", names)
		}
		// 面板联动断言（R7-8，Reviewer SF-1）：必须使用独立未受 60s 节流
		// 账号的首次调用——上方直接 rotate 已把该账号打进 min1PSIDTSInterval
		// 节流，同一账号的面板调用会被节流跳过（不再真正发起 1PSIDTS
		// 换发），断言面板 401 语义必须用不受节流污染的首次调用。
		id2, err := accountInsert("delauth2", cookieBase("ka2"), "")
		if err != nil {
			t.Fatalf("insert fresh: %v", err)
		}
		defer accountDelete(id2)
		w := httptest.NewRecorder()
		handleAdminCookiePost(w, id2, "rotate")
		var body map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("响应 JSON 解析失败: %v", err)
		}
		if ok, _ := body["ok"].(bool); ok {
			t.Fatalf("面板绝不能谎报 ok=true: %v", body)
		}
		if detail, _ := body["detail"].(string); !strings.Contains(detail, "HTTP 401") {
			t.Fatalf("未受节流账号的面板首次调用必须真实透出 401 诊断: %v", body)
		}
		if rf, ok := body["refreshed"].([]interface{}); ok && len(rf) > 0 {
			t.Fatalf("refreshed 必须为空（被忽略的删除头不算刷新）: %v", body)
		}
	})

	t.Run("GET 改 y POST 回 x 净零变化不谎报", func(t *testing.T) {
		// R7-8（Reviewer SF-1）：轮转页 GET 把 SIDCC 从 x 改成 y，随后
		// POST 又改回 x——两步均入逐步名清单，但最终串等于原串（无
		// UPDATE）——绝不按并集名单谎报「刷新了 SIDCC」：names=nil 且
		// 返回空转错误（未刷新任何 Cookie）。Cookie 合并规则不变。
		fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
			h := fhttp.Header{}
			resp := func(status int, body string, setCookies []string) (*fhttp.Response, error) {
				for _, sc := range setCookies {
					h.Add("Set-Cookie", sc)
				}
				return &fhttp.Response{
					StatusCode: status, Status: fmt.Sprintf("%d", status),
					Header: h, Body: io.NopCloser(strings.NewReader(body)),
				}, nil
			}
			if req.Method == "GET" && strings.Contains(req.URL.Path, "/RotateCookiesPage") {
				// GET 轮转页把 SIDCC 从原值 x 改成 y。
				return resp(200, `init('abc1234', 658.0, 0.0, 0.0, 600.0)`,
					[]string{"SIDCC=nzy; Domain=.google.com; Path=/; Secure"})
			}
			body := ""
			if req.Body != nil {
				b, _ := io.ReadAll(req.Body)
				body = string(b)
			}
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				if strings.Contains(body, `[000,`) {
					// 1PSIDTS 换发：不下发任何新票（聚焦 SIDCC 净零变化）。
					return resp(200, ")]}'\n\n[]", nil)
				}
				// [658,...] SIDCC 换发：又改回原值 x——净变化为零。
				return resp(200, ")]}'\n\n[]",
					[]string{"SIDCC=nzx; Domain=.google.com; Path=/; Secure"})
			}
			return resp(404, "", nil)
		}}
		stubTLSRotateClient(t, fake)
		id, err := accountInsert("netzero", cookieBase("kn")+"; SIDCC=nzx", "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		before := accountByID(id).Cookie
		if !strings.Contains(before, "SIDCC=nzx") {
			t.Fatalf("前置：初始 SIDCC 必须为 nzx，得到 %q", before)
		}
		acct := accountByID(id)
		_, names, rerr := rotateAccount(*acct)
		if rerr == nil {
			t.Fatal("净零变化（GET 改 y POST 回 x）绝不能返回 nil 谎报保活成功")
		}
		if len(names) != 0 {
			t.Fatalf("净零变化时 names 必须为 nil（不谎报 SIDCC）: %v", names)
		}
		if strings.Contains(rerr.Error(), "SIDCC") || strings.Contains(rerr.Error(), "本次仅刷新了") {
			t.Fatalf("净零变化错误绝不能点名 SIDCC/本次仅刷新了: %v", rerr)
		}
		if a := accountByID(id); a == nil || a.Cookie != before {
			t.Fatalf("净零变化绝不能改库: %q vs %q", a.Cookie, before)
		}
		// 对照：POST 不回原值（净变化真实存在）时 SIDCC 照常入名单并落库。
		idC, err := accountInsert("netchange", cookieBase("kc2")+"; SIDCC=ncx", "")
		if err != nil {
			t.Fatalf("insert ctl: %v", err)
		}
		defer accountDelete(idC)
		fake2 := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
			h := fhttp.Header{}
			resp := func(status int, body string, setCookies []string) (*fhttp.Response, error) {
				for _, sc := range setCookies {
					h.Add("Set-Cookie", sc)
				}
				return &fhttp.Response{
					StatusCode: status, Status: fmt.Sprintf("%d", status),
					Header: h, Body: io.NopCloser(strings.NewReader(body)),
				}, nil
			}
			if req.Method == "GET" && strings.Contains(req.URL.Path, "/RotateCookiesPage") {
				return resp(200, `init('abc1234', 658.0, 0.0, 0.0, 600.0)`, nil)
			}
			body := ""
			if req.Body != nil {
				b, _ := io.ReadAll(req.Body)
				body = string(b)
			}
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				if strings.Contains(body, `[000,`) {
					return resp(200, ")]}'\n\n[]", nil)
				}
				return resp(200, ")]}'\n\n[]",
					[]string{"SIDCC=ncy; Domain=.google.com; Path=/; Secure"})
			}
			return resp(404, "", nil)
		}}
		stubTLSRotateClient(t, fake2)
		_, names2, rerr2 := rotateAccount(*accountByID(idC))
		if rerr2 != nil {
			t.Fatalf("对照：真实净变化必须保活成功: %v", rerr2)
		}
		found := false
		for _, n := range names2 {
			if n == "SIDCC" {
				found = true
			}
		}
		if !found {
			t.Fatalf("对照：真实净变化 SIDCC 必须入 names: %v", names2)
		}
		if a := accountByID(idC); a == nil || !strings.Contains(a.Cookie, "SIDCC=ncy") {
			t.Fatalf("对照：真实净变化必须落库: %q", a.Cookie)
		}
	})

	t.Run("非规范分隔符裸串净零变化不改库不拆槽", func(t *testing.T) {
		// R8-8：DB 里的裸 Cookie 是非规范分隔符形态（无空格 ";" 分隔）。
		// 合并逻辑按 "; " 规范化拼串，即便逐名值完全净零变化，最终串也与
		// 库中裸串字符串不等——修复前 rotateAccountOpts 会把规范化串
		// 当成「变化」整串写库，连带 BumpAccountEpoch + InvalidateAccount
		// 拆掉完全健康的存活槽，而写进去的串凭据语义与原来一字不差。
		// 本用例在净零变化下断言：DB cookie 逐字节不变、存活槽不被拆。
		srv := newFakeCDPServer(t)
		defer srv.close()
		cleanup := wireTestBrowser(t, srv)
		defer cleanup()
		m := browserMgr
		setRuntimeCfg(t, func(c *RuntimeConfig) {
			c.BrowserTransport = true
			c.FallbackDirect = true
		})
		fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
			h := fhttp.Header{}
			resp := func(status int, body string, setCookies []string) (*fhttp.Response, error) {
				for _, sc := range setCookies {
					h.Add("Set-Cookie", sc)
				}
				return &fhttp.Response{
					StatusCode: status, Status: fmt.Sprintf("%d", status),
					Header: h, Body: io.NopCloser(strings.NewReader(body)),
				}, nil
			}
			if req.Method == "GET" && strings.Contains(req.URL.Path, "/RotateCookiesPage") {
				// GET 轮转页把 SIDCC 从原值改成 y。
				return resp(200, `init('abc1234', 658.0, 0.0, 0.0, 600.0)`,
					[]string{"SIDCC=nzy2; Domain=.google.com; Path=/; Secure"})
			}
			body := ""
			if req.Body != nil {
				b, _ := io.ReadAll(req.Body)
				body = string(b)
			}
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				if strings.Contains(body, `[000,`) {
					return resp(200, ")]}'\n\n[]", nil)
				}
				// [658,...] SIDCC 换发：改回原值 nzx2——净零变化。
				return resp(200, ")]}'\n\n[]",
					[]string{"SIDCC=nzx2; Domain=.google.com; Path=/; Secure"})
			}
			return resp(404, "", nil)
		}}
		stubTLSRotateClient(t, fake)
		// 非规范分隔符裸串：";" 无空格——cookieIdentity/逐名值不变。
		rawCookie := "SAPISID=rnk;SID=sx;__Secure-1PSID=rnp;__Secure-1PSIDTS=rnts;SIDCC=nzx2"
		id, err := accountInsert("netzeroraw", rawCookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		before := accountByID(id).Cookie
		if before != rawCookie {
			t.Fatalf("前置：入库必须保留非规范分隔符原样裸串: %q", before)
		}
		// 注册该账号的存活浏览器槽（jar 与 DB 一致，SyncAccountCookiesBack
		// 零增量）：净零变化下该槽绝不被 Invalidate 拆毁。
		acct := accountByID(id)
		key := slotKeyForAccount(acct, "", true)
		acSlot := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(rawCookie),
			browserContextID: "bc-netzero",
			targetID:         "t-netzero",
			sessionID:        "sess-netzero",
			allowDirect:      true,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
		}
		acSlot.readyCh = make(chan struct{})
		close(acSlot.readyCh)
		m.mu.Lock()
		m.ctxs[key] = acSlot
		m.sessionIndex["sess-netzero"] = acSlot
		m.targetIndex["t-netzero"] = acSlot
		m.mu.Unlock()
		srv.bcMu.Lock()
		srv.sessToBC["sess-netzero"] = "bc-netzero"
		// R9-7：同值**非空** Jar——jar 逐名键值与 DB 完全相等。修复前
		// syncAccountCookiesBackCtx 把 jar 的 "k=v" 行用 "; " 规范化重拼，
		// 非规范分隔符裸串在零键值变化下与库中原串字符串不等，前置同步
		// 就先发起一次无谓 UPDATE 把库中原串覆写为规范化串（「不改库」
		// 不成立）；旧版用例放空 Jar 恰好绕过该分支。修复后逐名等值保留
		// 原串不写库，与后续净零变化断言在同一用例内同时成立。
		jar := map[string]string{}
		for _, kv := range splitCookiePairs(rawCookie) {
			jar[kv[0]] = kv[1]
		}
		srv.bcStore["bc-netzero"] = jar
		srv.bcMu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			delete(m.sessionIndex, "sess-netzero")
			delete(m.targetIndex, "t-netzero")
			m.mu.Unlock()
		}()

		_, names, rerr := rotateAccountOpts(*accountByID(id), false)
		if rerr == nil {
			t.Fatal("净零变化绝不能返回 nil 谎报保活成功")
		}
		if len(names) != 0 {
			t.Fatalf("净零变化时 names 必须为 nil: %v", names)
		}
		// 核心断言：DB cookie 逐字节不变（非规范分隔符裸串绝不被规范化串
		// 整串覆写——R8-8 复位 cookie = cur.Cookie）。
		if a := accountByID(id); a == nil || a.Cookie != before {
			t.Fatalf("净零变化下非规范分隔符裸串绝不能改库: %q vs %q", a.Cookie, before)
		}
		// 存活槽绝不被拆（无无谓 BumpAccountEpoch + InvalidateAccount）。
		m.mu.Lock()
		slotAlive := m.ctxs[key] == acSlot && !acSlot.disposed
		m.mu.Unlock()
		if !slotAlive {
			t.Fatal("净零变化下存活浏览器槽绝不能被拆毁（无无谓写库/Invalidate）")
		}
	})

	t.Run("CAS 未落库绝不谎报已刷新", func(t *testing.T) {
		// R4-7（Reviewer SF-1）：上游 I/O 期间账号被手动停用——CAS 的
		// status='enabled' 条件 0 行命中（written=false）。rotateAccount
		// 必须返回非 nil 错误且 names=nil，面板联动 ok=false，绝不按上游
		// 头部名清单谎报 refreshed。
		id, err := accountInsert("casmiss", cookieBase("kc"), "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		// 假上游：1PSIDTS 换发返回新票；在换发 I/O 期间把账号停用。
		fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
			h := fhttp.Header{}
			resp := func(status int, body string, setCookies []string) (*fhttp.Response, error) {
				for _, sc := range setCookies {
					h.Add("Set-Cookie", sc)
				}
				return &fhttp.Response{
					StatusCode: status, Status: fmt.Sprintf("%d", status),
					Header: h, Body: io.NopCloser(strings.NewReader(body)),
				}, nil
			}
			if req.Method == "GET" && strings.Contains(req.URL.Path, "/RotateCookiesPage") {
				return resp(200, `init('abc1234', 658.0, 0.0, 0.0, 600.0)`, nil)
			}
			body := ""
			if req.Body != nil {
				b, _ := io.ReadAll(req.Body)
				body = string(b)
			}
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				if strings.Contains(body, `[000,`) {
					// 上游 I/O 进行中手动停用账号：CAS status='enabled' 未命中。
					if serr := accountSetStatus(id, "disabled"); serr != nil {
						t.Errorf("模拟停用失败: %v", serr)
					}
					return resp(200, ")]}'\n\n[]", []string{"__Secure-1PSIDTS=casnewts; Domain=.google.com; Path=/; Secure"})
				}
				return resp(200, ")]}'\n\n[]", nil)
			}
			return resp(404, "", nil)
		}}
		stubTLSRotateClient(t, fake)

		before := accountByID(id).Cookie
		acct := accountByID(id)
		_, names, rerr := rotateAccount(*acct)
		if rerr == nil {
			t.Fatal("CAS 未落库时绝不能返回 nil（谎报保活成功，R4-7）")
		}
		if len(names) != 0 {
			t.Fatalf("CAS 未落库时 names 必须为 nil（不声称已刷新未落库的票据）: %v", names)
		}
		if !strings.Contains(rerr.Error(), "未落库") {
			t.Fatalf("错误必须点名未落库: %v", rerr)
		}
		// DB 票据保持不变（CAS 未命中，上游新票不落库）。
		if a := accountByID(id); a == nil || a.Cookie != before {
			t.Fatalf("CAS 未命中时 DB cookie 绝不能被改写: %q vs %q", a.Cookie, before)
		}

		// 面板断言（同一账号复跑）：先恢复启用并清掉 1PSIDTS 节流记录，
		// 面板 rotate 复跑同一上游桩（换发 I/O 期间再次停用）——ok=false、
		// refreshed 为空、detail 带未落库诊断。
		if err := accountSetStatus(id, "enabled"); err != nil {
			t.Fatalf("恢复启用: %v", err)
		}
		psidtsMu.Lock()
		delete(psidtsLastAt, id)
		psidtsMu.Unlock()
		w := httptest.NewRecorder()
		handleAdminCookiePost(w, id, "rotate")
		if w.Code != 200 {
			t.Fatalf("面板 rotate 应 200 回业务结果，得到 %d", w.Code)
		}
		var body map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("响应 JSON 解析失败: %v", err)
		}
		if ok, _ := body["ok"].(bool); ok {
			t.Fatalf("CAS 未落库时面板绝不能谎报 ok=true: %v", body)
		}
		if rf, ok := body["refreshed"].([]interface{}); ok && len(rf) > 0 {
			t.Fatalf("CAS 未落库时 refreshed 必须为空: %v", body)
		}
		if detail, _ := body["detail"].(string); !strings.Contains(detail, "未落库") {
			t.Fatalf("detail 必须带未落库诊断: %v", body)
		}
	})

	t.Run("双失败保留 401 且不打假成功日志", func(t *testing.T) {
		// R5-7（Reviewer SF-1）：1PSIDTS 被 401 拒且 SIDCC 换发因 CAS 未落库
		// 的双失败场景——返回错误必须同时保留 401 鉴权原因（%w 包装，
		// isPSIDTSAuthErr 可识别）与未落库原因，names=nil；且绝不能先打
		// 「刷新了」的假成功日志（运维看到的必须与事实一致）。
		id, err := accountInsert("dualfail", cookieBase("kd"), "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
			h := fhttp.Header{}
			resp := func(status int, body string, setCookies []string) (*fhttp.Response, error) {
				for _, sc := range setCookies {
					h.Add("Set-Cookie", sc)
				}
				return &fhttp.Response{
					StatusCode: status, Status: fmt.Sprintf("%d", status),
					Header: h, Body: io.NopCloser(strings.NewReader(body)),
				}, nil
			}
			if req.Method == "GET" && strings.Contains(req.URL.Path, "/RotateCookiesPage") {
				return resp(200, `init('abc1234', 658.0, 0.0, 0.0, 600.0)`, nil)
			}
			body := ""
			if req.Body != nil {
				b, _ := io.ReadAll(req.Body)
				body = string(b)
			}
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				if strings.Contains(body, `[000,`) {
					// 哨兵 1PSIDTS 换发：401（DBSC 设备绑定）。
					return resp(401, `[["er",401]]`, nil)
				}
				// [658,...] SIDCC 换发 I/O 期间管理员手动停用——CAS 未命中。
				if serr := accountSetStatus(id, "disabled"); serr != nil {
					t.Errorf("模拟停用失败: %v", serr)
				}
				return resp(200, ")]}'\n\n[]", []string{"SIDCC=dualcc; Domain=.google.com; Path=/; Secure"})
			}
			return resp(404, "", nil)
		}}
		stubTLSRotateClient(t, fake)

		globalLogBuffer.Clear()
		defer globalLogBuffer.Clear()
		before := accountByID(id).Cookie
		acct := accountByID(id)
		_, names, rerr := rotateAccount(*acct)
		if rerr == nil {
			t.Fatal("双失败场景绝不能返回 nil")
		}
		// 401 鉴权原因必须保留在错误链（%w 包装后 isPSIDTSAuthErr 仍可识别）。
		if !isPSIDTSAuthErr(rerr) {
			t.Fatalf("双失败必须保留 1PSIDTS 401/403 鉴权原因: %v (%T)", rerr, rerr)
		}
		if !strings.Contains(rerr.Error(), "HTTP 401") {
			t.Fatalf("错误文本必须保留 401 诊断: %v", rerr)
		}
		if !strings.Contains(rerr.Error(), "未落库") {
			t.Fatalf("错误必须同时点名 CAS 未落库: %v", rerr)
		}
		if len(names) != 0 {
			t.Fatalf("双失败 names 必须为 nil（未落库票据不算已刷新）: %v", names)
		}
		// DB 票据不变（CAS 未命中）。
		if a := accountByID(id); a == nil || a.Cookie != before {
			t.Fatalf("CAS 未命中时 DB cookie 绝不能被改写: %q vs %q", a.Cookie, before)
		}
		// 关键日志断言：CAS 失败前绝不打「刷新了」假成功记录。
		for _, e := range globalLogBuffer.Query(LogFilter{Tail: 1000}) {
			if strings.Contains(e.Message, "刷新了") || strings.Contains(e.Raw, "刷新了") {
				t.Fatalf("CAS 未落库时绝不能打「刷新了」假成功日志: %+v", e)
			}
		}
		// 对照：真实落库成功时「刷新了」照常记录（不因 R5-7 误删正常日志）。
		idOK, err := accountInsert("dualok", cookieBase("ko"), "")
		if err != nil {
			t.Fatalf("insert ok: %v", err)
		}
		defer accountDelete(idOK)
		globalLogBuffer.Clear()
		stubRotateUpstream(t, 200,
			[]string{"__Secure-1PSIDTS=oknewts; Domain=.google.com; Path=/; Secure"}, nil)
		if _, _, rerr2 := rotateAccount(*accountByID(idOK)); rerr2 != nil {
			t.Fatalf("对照组正常保活必须成功: %v", rerr2)
		}
		found := false
		for _, e := range globalLogBuffer.Query(LogFilter{Tail: 1000}) {
			if strings.Contains(e.Message, "刷新了") {
				found = true
			}
		}
		if !found {
			t.Fatal("真实落库成功时「刷新了」日志必须照常记录（R5-7 不能误删正常日志）")
		}
	})
}

// TestRotateActiveOrSiblingRejectsStaleSnapshot 复核 R4-1（Reviewer MF-1）
// 的过期快照拒发（端到端经生产 rotateActiveOrSibling1PSIDTS）：
//   - 原绑定不变且槽位仍驻留的合法溢出（非严格模式）：放行换发并热注入；
//   - 取快照后管理员改绑（cur.ProxyID != snap.origAccountProxyID）：拒绝，
//     RotateCookies 零命中、Jar 零注入；
//   - 取快照后槽位被摘除/替换（snap.bcID 不再驻留）：拒绝，零命中零注入。
func TestRotateActiveOrSiblingRejectsStaleSnapshot(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 61, Name: "sp1", URL: "http://sp1:8080", Enabled: true},
		{ID: 62, Name: "sp2", URL: "http://sp2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	var hitsMu sync.Mutex
	hits := 0
	// 溢出出口走 stdlib 代理客户端（proxyURL 非空时 rotateDo 不走
	// tls-client）——把 sp2 的 stdlib 缓存键桩成假 RoundTripper，
	// RotateCookies 应答经它回到保活路径。
	stubUpstream := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			hitsMu.Lock()
			hits++
			hitsMu.Unlock()
			r := respJSON(req, 200, ")]}'\n\n[]")
			r.Header.Set("Set-Cookie", "__Secure-1PSIDTS=stalets; Domain=.google.com; Path=/; Secure; HttpOnly")
			return r, nil
		}
		return respJSON(req, 404, ""), nil
	})
	timeout := rtCfg().RequestTimeout
	for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
		stdlibClientCache.Store("http://sp2:8080"+suf, &http.Client{Transport: stubUpstream})
	}
	defer func() {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Delete("http://sp2:8080" + suf)
		}
	}()
	countHits := func() int {
		hitsMu.Lock()
		defer hitsMu.Unlock()
		return hits
	}

	// setup 注册一个绑定 P1(61)、快照后仍合法溢出在 P2(62) 出口的自有活跃槽，
	// 返回账号 ID / 槽 / 槽位快照。
	setup := func(t *testing.T, label, tag string) (int64, *accountBrowserCtx, activeSlotSnapshot) {
		t.Helper()
		cookie := "SAPISID=st" + tag + "; __Secure-1PSID=sp" + tag + "; SID=ss" + tag + "; __Secure-1PSIDTS=oldts"
		id, err := accountInsert(label, cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		bindAccountProxy(id, 61)
		acct := accountByID(id)
		bcID := "bc-stale-" + tag
		sessID := "sess-stale-" + tag
		tgtID := "t-stale-" + tag
		key := slotKeyForAccount(acct, "http://sp2:8080", false)
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(cookie),
			browserContextID:   bcID,
			targetID:           tgtID,
			sessionID:          sessID,
			proxyURL:           "http://sp2:8080",
			allowDirect:        false,
			origAccountProxyID: 61,
			boundProxyID:       62,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.sessionIndex[sessID] = ac
		m.targetIndex[tgtID] = ac
		m.mu.Unlock()
		srv.bcMu.Lock()
		srv.sessToBC[sessID] = bcID
		srv.bcStore[bcID] = map[string]string{
			"SAPISID":          "st" + tag,
			"__Secure-1PSID":   "sp" + tag,
			"SID":              "ss" + tag,
			"__Secure-1PSIDTS": "oldts",
		}
		srv.bcMu.Unlock()
		snap := activeSlotSnapshot{
			accountID: id, bcID: bcID, proxyURL: "http://sp2:8080", allowDirect: false,
			boundProxyID: 62, origAccountProxyID: 61, ident: cookieIdentity(cookie),
		}
		return id, ac, snap
	}

	t.Run("原绑定不变的合法溢出放行", func(t *testing.T) {
		before := countHits()
		id, ac, snap := setup(t, "stalebase", "bb")
		defer accountDelete(id)
		defer m.InvalidateAccount(id, false, "test cleanup")
		rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{snap}, map[string][]string{})
		if got := countHits(); got != before+1 {
			t.Fatalf("合法溢出必须放行换发（RotateCookies 命中 +1），得到 %d vs %d", got, before)
		}
		jar := srv.jarFor(ac.sessionID)
		if jar["__Secure-1PSIDTS"] != "stalets" {
			t.Fatalf("合法溢出热注入必须命中 Jar: %q", jar["__Secure-1PSIDTS"])
		}
	})

	t.Run("取快照后管理员改绑拒绝", func(t *testing.T) {
		before := countHits()
		id, ac, snap := setup(t, "stalerebind", "rb")
		defer accountDelete(id)
		defer m.InvalidateAccount(id, false, "test cleanup")
		// 取快照后管理员 PATCH 改绑到 P2（恰好是溢出出口本身）：快照过期。
		bindAccountProxy(id, 62)
		rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{snap}, map[string][]string{})
		if got := countHits(); got != before {
			t.Fatalf("管理员改绑后过期快照绝不能换发（RotateCookies 命中 %d 次）", got-before)
		}
		jar := srv.jarFor(ac.sessionID)
		if jar["__Secure-1PSIDTS"] == "stalets" {
			t.Fatalf("管理员改绑后绝不能沿过期快照热注入 Jar: %q", jar["__Secure-1PSIDTS"])
		}
		if a := accountByID(id); a != nil && strings.Contains(a.Cookie, "__Secure-1PSIDTS=stalets") {
			t.Fatalf("过期快照的票据绝不能写库: %q", a.Cookie)
		}
	})

	t.Run("合法自动重绑同步驻留槽事实下轮放行", func(t *testing.T) {
		// R6-4（Reviewer MF-4）：原绑定 P1 禁用/不可用时非严格溢出到
		// P2 建槽（setup 现场），随后 autoBindAccountProxy(id, P2) 合法
		// 自动重绑——驻留槽的 orig/bound 事实被同步到与实际出口一致的
		// P2，下一轮以全新快照续期必须正常放行（修复前 orig 恒为旧值
		// 61，永久被改绑拦截拒绝续期）。
		before := countHits()
		id, ac, _ := setup(t, "autorebind", "ar")
		defer accountDelete(id)
		defer m.InvalidateAccount(id, false, "test cleanup")
		// 模拟原绑定 P1 已被管理员禁用/不可用（非严格溢出场景）。
		proxyMu.Lock()
		for i, p := range proxyCache {
			if p.ID == 61 {
				proxyCache[i].Enabled = false
			}
		}
		proxyMu.Unlock()
		defer func() {
			proxyMu.Lock()
			for i, p := range proxyCache {
				if p.ID == 61 {
					proxyCache[i].Enabled = true
				}
			}
			proxyMu.Unlock()
		}()
		// 合法自动重绑提交：DB 与驻留槽事实同步到 P2。
		autoBindAccountProxy(id, 62)
		if a := accountByID(id); a == nil || a.ProxyID != 62 {
			t.Fatalf("前置：自动重绑必须落库到 P2，得到 %+v", a)
		}
		m.mu.Lock()
		cur := m.ctxs[ac.key]
		if cur == nil || cur.origAccountProxyID != 62 || cur.boundProxyID != 62 {
			m.mu.Unlock()
			t.Fatalf("自动重绑必须同步驻留槽事实到 P2（与实际出口一致），得到 %+v", cur)
		}
		m.mu.Unlock()
		// 下一轮以全新快照续期（与 rotateAllAccounts 每轮重取快照同构）。
		byID, _ := m.ActiveAccountSlotSnapshots()
		freshSnap, ok := byID[id]
		if !ok {
			t.Fatal("前置：驻留槽必须出现在活跃快照里")
		}
		rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{freshSnap}, map[string][]string{})
		if got := countHits(); got != before+1 {
			t.Fatalf("合法自动重绑后下轮热续期必须放行（RotateCookies 命中 +1），得到 %d vs %d", got, before)
		}
		jar := srv.jarFor(ac.sessionID)
		if jar["__Secure-1PSIDTS"] != "stalets" {
			t.Fatalf("自动重绑后热注入必须命中 Jar: %q", jar["__Secure-1PSIDTS"])
		}
		if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=stalets") {
			t.Fatalf("自动重绑后新票必须写库: %q", a.Cookie)
		}
	})

	t.Run("取快照后摘槽或替换拒绝", func(t *testing.T) {
		before := countHits()
		id, _, snap := setup(t, "stalegone", "gn")
		defer accountDelete(id)
		// 取快照后管理员摘槽（PATACH 改绑联动失效）。
		m.InvalidateAccount(id, false, "patched")
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, browserSlotKey{accountID: id})
			m.mu.Unlock()
		}()
		rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{snap}, map[string][]string{})
		if got := countHits(); got != before {
			t.Fatalf("快照槽已摘除后绝不能换发（RotateCookies 命中 %d 次）", got-before)
		}
		if a := accountByID(id); a != nil && strings.Contains(a.Cookie, "__Secure-1PSIDTS=stalets") {
			t.Fatalf("过期快照的票据绝不能写库: %q", a.Cookie)
		}
	})

	// setupUnbound 注册一个「建槽时尚未绑定」（origAccountProxyID=0）、实际
	// 出口为 P2(62) 的自有活跃槽——重现 R5-1 的冷建槽场景。
	setupUnbound := func(t *testing.T, label, tag string) (int64, *accountBrowserCtx, activeSlotSnapshot) {
		t.Helper()
		cookie := "SAPISID=ub" + tag + "; __Secure-1PSID=up" + tag + "; SID=us" + tag + "; __Secure-1PSIDTS=oldts"
		id, err := accountInsert(label, cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		acct := accountByID(id) // ProxyID 保持 0（未绑定）
		bcID := "bc-unbound-" + tag
		sessID := "sess-unbound-" + tag
		tgtID := "t-unbound-" + tag
		key := slotKeyForAccount(acct, "http://sp2:8080", false)
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(cookie),
			browserContextID:   bcID,
			targetID:           tgtID,
			sessionID:          sessID,
			proxyURL:           "http://sp2:8080",
			allowDirect:        false,
			origAccountProxyID: 0, // 建槽时尚未绑定（R5-1 现场）
			boundProxyID:       62,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.sessionIndex[sessID] = ac
		m.targetIndex[tgtID] = ac
		m.mu.Unlock()
		srv.bcMu.Lock()
		srv.sessToBC[sessID] = bcID
		srv.bcStore[bcID] = map[string]string{
			"SAPISID":          "ub" + tag,
			"__Secure-1PSID":   "up" + tag,
			"SID":              "us" + tag,
			"__Secure-1PSIDTS": "oldts",
		}
		srv.bcMu.Unlock()
		snap := activeSlotSnapshot{
			accountID: id, bcID: bcID, proxyURL: "http://sp2:8080", allowDirect: false,
			boundProxyID: 62, origAccountProxyID: 0, ident: cookieIdentity(cookie),
		}
		return id, ac, snap
	}

	t.Run("首次自动绑定到本槽出口下轮放行", func(t *testing.T) {
		// R5-1（Reviewer MF-1）：未绑定账号经 P2 冷建槽（orig=0），随后非严格
		// 模式合法首次自动绑定到本槽实际出口 P2——下一轮真实换发必须放行，
		// 绝不能把合法自动首绑误当管理员改绑永久拦截续期。
		before := countHits()
		id, ac, snap := setupUnbound(t, "autobind", "ab")
		defer accountDelete(id)
		defer m.InvalidateAccount(id, false, "test cleanup")
		// 模拟 acquireXSRFToken 成功后的合法首次自动绑定（0 -> P2=本槽出口）。
		autoBindAccountProxy(id, 62)
		if a := accountByID(id); a == nil || a.ProxyID != 62 {
			t.Fatalf("前置：自动绑定必须落库，得到 %+v", a)
		}
		rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{snap}, map[string][]string{})
		if got := countHits(); got != before+1 {
			t.Fatalf("合法首次自动绑定后下轮真实换发必须放行（RotateCookies 命中 +1），得到 %d vs %d", got, before)
		}
		jar := srv.jarFor(ac.sessionID)
		if jar["__Secure-1PSIDTS"] != "stalets" {
			t.Fatalf("合法首绑后热注入必须命中 Jar: %q", jar["__Secure-1PSIDTS"])
		}
		if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=stalets") {
			t.Fatalf("合法首绑后新票必须写库: %q", a.Cookie)
		}
	})

	t.Run("未绑定快照手工绑到异代理仍拒绝", func(t *testing.T) {
		// 对照（R5-1）：orig=0 的快照若之后被管理员手工绑到**别的**代理
		// P1（其 URL != 快照出口 P2），仍按绑定冲突 fail-closed 拒绝——
		// 首绑宽限绝不是放松手工改绑拦截。
		before := countHits()
		id, ac, snap := setupUnbound(t, "manualbind", "mb")
		defer accountDelete(id)
		defer m.InvalidateAccount(id, false, "test cleanup")
		bindAccountProxy(id, 61) // 管理员手工绑到 P1（URL 与快照出口不符）
		rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{snap}, map[string][]string{})
		if got := countHits(); got != before {
			t.Fatalf("手工绑到异代理后绝不能沿快照出口换发（RotateCookies 命中 %d 次）", got-before)
		}
		jar := srv.jarFor(ac.sessionID)
		if jar["__Secure-1PSIDTS"] == "stalets" {
			t.Fatalf("手工绑到异代理后绝不能热注入 Jar: %q", jar["__Secure-1PSIDTS"])
		}
	})
}

// TestRotateAllAccountsSiblingHotInjectEndToEnd 复核 R10 5a（Reviewer 建议
// 行为断言）：rotateAllAccounts() 整轮端到端——同身份两个驻留槽分别走
// P1/P2，另设两个非驻留同身份兄弟分别绑 P1/P2，一轮后两兄弟行均收到
// 新票（兄弟候选遍历 + 出口授权分离的整轮集成）。
func TestRotateAllAccountsSiblingHotInjectEndToEnd(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 41, Name: "e2p1", URL: "http://e2p1:8080", Enabled: true},
		{ID: 42, Name: "e2p2", URL: "http://e2p2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	// 同身份 cookie（SAPISID/1PSID 相同 → cookieIdentity 相同）。
	mkCookie := func(ts string) string {
		return "SAPISID=e2x; __Secure-1PSID=e2p; SID=e2s; __Secure-1PSIDTS=" + ts
	}
	mkAcct := func(label, ts string) int64 {
		id, err := accountInsert(label, mkCookie(ts), "")
		if err != nil {
			t.Fatalf("insert %s: %v", label, err)
		}
		t.Cleanup(func() { _ = accountDelete(id) })
		return id
	}
	// A、B 有驻留槽分别走 P1/P2；C、D 非驻留分别绑 P1/P2（同身份兄弟）。
	idA := mkAcct("e2a", "tsa")
	idB := mkAcct("e2b", "tsb")
	idC := mkAcct("e2c", "tsc")
	idD := mkAcct("e2d", "tsd")
	bindAccountProxy(idA, 41)
	bindAccountProxy(idB, 42)
	bindAccountProxy(idC, 41)
	bindAccountProxy(idD, 42)

	mkResident := func(id int64, tag, url string, bound int64) {
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(mkCookie("x")),
			browserContextID:   "bc-e2" + tag,
			targetID:           "t-e2" + tag,
			sessionID:          "sess-e2" + tag,
			proxyURL:           url,
			allowDirect:        false,
			origAccountProxyID: bound,
			boundProxyID:       bound,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.sessionIndex[ac.sessionID] = ac
		m.targetIndex[ac.targetID] = ac
		m.mu.Unlock()
		srv.bcMu.Lock()
		srv.sessToBC[ac.sessionID] = ac.browserContextID
		srv.bcStore[ac.browserContextID] = map[string]string{
			"SAPISID": "e2x", "__Secure-1PSID": "e2p", "SID": "e2s",
			"__Secure-1PSIDTS": "oldts",
		}
		srv.bcMu.Unlock()
		t.Cleanup(func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			delete(m.sessionIndex, ac.sessionID)
			delete(m.targetIndex, ac.targetID)
			m.mu.Unlock()
		})
	}
	mkResident(idA, "a", "http://e2p1:8080", 41)
	mkResident(idB, "b", "http://e2p2:8080", 42)

	// P1/P2 代理出口桩：任何经两代理的 1PSIDTS 哨兵换发都回新票
	//（同身份共享缓存会让一次真实换发覆盖全部兄弟）。
	timeout := rtCfg().RequestTimeout
	mkRT := func() roundTripperFunc {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				h := http.Header{}
				h.Add("Set-Cookie", "__Secure-1PSIDTS=e2new; Domain=.google.com; Path=/; Secure; HttpOnly")
				return &http.Response{
					StatusCode: 200, Status: "200 OK",
					Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
					Request: req,
				}, nil
			}
			return respJSON(req, 404, ""), nil
		})
	}
	for _, tag := range []string{"http://e2p1:8080", "http://e2p2:8080"} {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Store(tag+suf, &http.Client{Transport: mkRT()})
		}
	}
	defer func() {
		for _, tag := range []string{"http://e2p1:8080", "http://e2p2:8080"} {
			for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
				stdlibClientCache.Delete(tag + suf)
			}
		}
	}()

	rotateAllAccounts()

	// 核心断言：两个非驻留兄弟行均收到新票（借同身份驻留出口换发）。
	for _, pair := range []struct {
		id    int64
		label string
	}{{idC, "C(绑P1非驻留)"}, {idD, "D(绑P2非驻留)"}} {
		a := accountByID(pair.id)
		if a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=e2new") {
			t.Fatalf("兄弟行 %s 必须收到新票: %q", pair.label, a.Cookie)
		}
	}
	// 驻留槽行同样换发（热续期整轮覆盖）。
	for _, pair := range []struct {
		id    int64
		label string
	}{{idA, "A"}, {idB, "B"}} {
		a := accountByID(pair.id)
		if a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=e2new") {
			t.Fatalf("驻留槽行 %s 必须收到新票: %q", pair.label, a.Cookie)
		}
	}
}

// TestHealRotationAbortsOnSameIDURLChange 复核 R11-3（Oracle A4/Reviewer #1）：
// 自愈必须同时携带 picked ID 与挑中时刻的 URL——面板在等锁/网络 I/O 期间把
// 同一代理 ID 的 URL 从 U1 改成 U2 时：
//
//	① 轮转外发前核出 URL 不一致 → 绝不向任何出口发起换发（双 URL 计数桩零命中）；
//	② 轮转已走 U1 完成后 URL 变更 → 重取 /app 前复核命中，heal 放弃
//	  （healed=false），绝不向已撤换的 U1 发出带会话的 /app 请求。
func TestHealRotationAbortsOnSameIDURLChange(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 501, Name: "hp", URL: "http://u1.example:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = false
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookie := "SAPISID=hex; __Secure-1PSID=hep; SID=hes; __Secure-1PSIDTS=oldts"

	// 双 URL 计数桩：U1/U2 各自统计 rotate 与 /app 命中数。
	type counter struct {
		mu     sync.Mutex
		rotate int
		app    int
	}
	mkCountingRT := func(cnt *counter, flip func()) roundTripperFunc {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case strings.Contains(req.URL.Path, "/RotateCookies"):
				cnt.mu.Lock()
				cnt.rotate++
				cnt.mu.Unlock()
				if flip != nil {
					flip()
				}
				h := http.Header{}
				h.Add("Set-Cookie", "__Secure-1PSIDTS=henew; Domain=.google.com; Path=/; Secure; HttpOnly")
				return &http.Response{
					StatusCode: 200, Status: "200 OK",
					Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
					Request: req,
				}, nil
			case strings.Contains(req.URL.Path, "/RotateCookiesPage"):
				// SIDCC 轮转页：500——聚焦 1PSIDTS 单路成功（names 非空触发 refetch）。
				return respJSON(req, 500, ""), nil
			case strings.Contains(req.URL.Path, "/app"):
				cnt.mu.Lock()
				cnt.app++
				cnt.mu.Unlock()
				return respJSON(req, 200, `"SNlM0e":"tok-abcdefgh"`), nil
			}
			return respJSON(req, 404, ""), nil
		})
	}

	seed := func(u string, rt http.RoundTripper) func() {
		timeout := rtCfg().RequestTimeout
		var keys []string
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			keys = append(keys, u+suf)
			stdlibClientCache.Store(u+suf, &http.Client{Transport: rt})
		}
		return func() {
			for _, k := range keys {
				stdlibClientCache.Delete(k)
			}
		}
	}

	t.Run("轮转外发前URL已变更则零换发请求", func(t *testing.T) {
		id, err := accountInsert("hepre", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 501)
		// 构造终态：URL 已从 U1 改为 U2（面板等锁期间变更），而自愈携带的
		// picked 快照仍是 U1。
		proxyMu.Lock()
		proxyCache[0].URL = "http://u2.example:8080"
		proxyMu.Unlock()
		defer func() {
			proxyMu.Lock()
			proxyCache[0].URL = "http://u1.example:8080"
			proxyMu.Unlock()
		}()
		cnt1, cnt2 := &counter{}, &counter{}
		defer seed("http://u1.example:8080", mkCountingRT(cnt1, nil))()
		defer seed("http://u2.example:8080", mkCountingRT(cnt2, nil))()
		acct := accountByID(id)
		_, _, rerr := rotateAccountWithOpts(*acct, rotateOpts{
			hasPickedProxy:    true,
			pickedProxyID:     501,
			expectedDBProxyID: 501,
			pickedProxyURL:    "http://u1.example:8080",
		})
		if rerr == nil {
			t.Fatal("URL 已变更的自愈必须停止（绝不向任何出口发起换发）")
		}
		if !strings.Contains(rerr.Error(), "URL 已变更") {
			t.Fatalf("错误必须点名 URL 变更: %v", rerr)
		}
		cnt1.mu.Lock()
		cnt2.mu.Lock()
		r1, r2 := cnt1.rotate, cnt2.rotate
		cnt1.mu.Unlock()
		cnt2.mu.Unlock()
		if r1 != 0 || r2 != 0 {
			t.Fatalf("URL 不一致时绝不能向任何出口发起换发: U1=%d U2=%d", r1, r2)
		}
	})

	t.Run("轮转走U1后URL变更则重取app前放弃", func(t *testing.T) {
		id, err := accountInsert("hepost", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 501)
		proxyMu.Lock()
		proxyCache[0].URL = "http://u1.example:8080"
		proxyMu.Unlock()
		cnt1, cnt2 := &counter{}, &counter{}
		var flipOnce sync.Once
		flip := func() {
			flipOnce.Do(func() {
				proxyMu.Lock()
				proxyCache[0].URL = "http://u2.example:8080"
				proxyMu.Unlock()
			})
		}
		defer seed("http://u1.example:8080", mkCountingRT(cnt1, flip))()
		defer seed("http://u2.example:8080", mkCountingRT(cnt2, nil))()
		defer func() {
			proxyMu.Lock()
			proxyCache[0].URL = "http://u1.example:8080"
			proxyMu.Unlock()
		}()

		acct := accountByID(id)
		picked := Proxy{ID: 501, URL: "http://u1.example:8080"}
		heal := healXSRFByRotation(ModelConfig{}, acct, picked, "http://u1.example:8080",
			map[int64]bool{}, map[int64]bool{})
		// 换发确实走了 U1（Oracle A4 判据之一：保活出口是挑中时刻的合法 URL）。
		cnt1.mu.Lock()
		r1 := cnt1.rotate
		cnt1.mu.Unlock()
		if r1 == 0 {
			t.Fatal("前置：保活换发必须经 U1 实际发生")
		}
		// 核心断言：URL 已在轮转 I/O 期间变更——heal 必须放弃（healed=false），
		// 绝不向已撤换的 U1 发出带会话的 /app 请求。
		if heal.healed {
			t.Fatal("URL 变更后 heal 必须放弃（绝不向旧 U1 重取 XSRF）")
		}
		cnt1.mu.Lock()
		a1 := cnt1.app
		cnt1.mu.Unlock()
		cnt2.mu.Lock()
		a2, r2 := cnt2.app, cnt2.rotate
		cnt2.mu.Unlock()
		if a1 != 0 {
			t.Fatalf("绝不能向已撤换的 U1 发出 /app 带会话请求，命中 %d 次", a1)
		}
		if a2 != 0 || r2 != 0 {
			t.Fatalf("自愈绝不许跨两个实际出口（U2 零命中）: app=%d rotate=%d", a2, r2)
		}
	})

	t.Run("URL未变时正常完成自愈对照", func(t *testing.T) {
		id, err := accountInsert("hectl", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 501)
		proxyMu.Lock()
		proxyCache[0].URL = "http://u1.example:8080"
		proxyMu.Unlock()
		cnt1 := &counter{}
		defer seed("http://u1.example:8080", mkCountingRT(cnt1, nil))()
		acct := accountByID(id)
		picked := Proxy{ID: 501, URL: "http://u1.example:8080"}
		heal := healXSRFByRotation(ModelConfig{}, acct, picked, "http://u1.example:8080",
			map[int64]bool{}, map[int64]bool{})
		if !heal.healed {
			t.Fatalf("URL 未变时自愈必须正常完成（对照）: %+v", heal)
		}
		cnt1.mu.Lock()
		r1, a1 := cnt1.rotate, cnt1.app
		cnt1.mu.Unlock()
		if r1 == 0 || a1 == 0 {
			t.Fatalf("对照必须真实经 U1 换发并重取 /app: rotate=%d app=%d", r1, a1)
		}
	})
}

// TestRotateWritesDuplicateTailUpdate 复核 R11-8-B2（Oracle B2/Reviewer #7）：
// 普通轮转的逐步与最终名字表比对按完整有序值列表及存在性裁决——原行
// `SIDCC=n; SIDCC=y`（重名尾部残留旧票），上游只下发 `SIDCC=n` 时合并已把
// 两项都更新为 n，修复前只认首值判净零还原整串，尾部旧票 y 永留 DB 且谎报
// 「未刷新任何 Cookie」。修复后：DB 落两项皆 n（真实尾项更新）；纯格式差异
// 与 GET→POST 还原对照仍零写库。
func TestRotateWritesDuplicateTailUpdate(t *testing.T) {
	t.Run("重名尾项真实更新必须落库", func(t *testing.T) {
		stubRotateUpstream(t, 200, nil,
			[]string{"SIDCC=n; Domain=.google.com; Path=/; Secure"})
		id, err := accountInsert("duptail",
			"SAPISID=dtx; SID=x; __Secure-1PSID=dtp; __Secure-1PSIDTS=ts1; SIDCC=n; SIDCC=y", "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		acct := accountByID(id)
		_, names, rerr := rotateAccount(*acct)
		if rerr != nil {
			t.Fatalf("重名尾项真实更新必须算有效刷新: %v", rerr)
		}
		found := false
		for _, n := range names {
			if n == "SIDCC" {
				found = true
			}
		}
		if !found {
			t.Fatalf("尾项真实更新必须入 refreshed 名单: %v", names)
		}
		a := accountByID(id)
		if a == nil {
			t.Fatal("account vanished")
		}
		if !strings.Contains(a.Cookie, "SIDCC=n; SIDCC=n") {
			t.Fatalf("重名两项必须都更新为 n 落库: %q", a.Cookie)
		}
		if strings.Contains(a.Cookie, "SIDCC=y") {
			t.Fatalf("尾部旧票 y 绝不许残留 DB: %q", a.Cookie)
		}
	})

	t.Run("格式差异与重名等值仍判净零零写库", func(t *testing.T) {
		// 对照：上游下发的 SIDCC 值与现值完全相同——两项皆 n 合并后逐项
		// 等值，必须判净零跳过写库（不触发无谓 UPDATE/不谎报刷新）。
		stubRotateUpstream(t, 200, nil,
			[]string{"SIDCC=n; Domain=.google.com; Path=/; Secure"})
		id, err := accountInsert("dupnet0",
			"SAPISID=dnx; SID=x; __Secure-1PSID=dnp; __Secure-1PSIDTS=ts2; SIDCC=n; SIDCC=n", "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		before := accountByID(id).Cookie
		acct := accountByID(id)
		_, names, rerr := rotateAccount(*acct)
		if rerr == nil {
			t.Fatal("净零变化绝不能返回 nil（谎报保活成功）")
		}
		if len(names) != 0 {
			t.Fatalf("净零变化时 names 必须为空: %v", names)
		}
		if a := accountByID(id); a == nil || a.Cookie != before {
			t.Fatalf("净零变化绝不能改库（逐字节不变）: %q vs %q", a.Cookie, before)
		}
	})
}

// TestCookieIdentityMatchesSubsetOnDuplicateKeys 复核 R11-8-B4（Oracle B4）：
// cookieIdentity（共享缓存/授权身份裁决）与 cookieSubset（换发请求实际带出
// 的子集）对含冲突重复会话主键的输入必须取同一个值——否则标作会话 B 的
// 缓存键背后送出的却是会话 A 的票，共享缓存会让合法兄弟领取不对应自己
// 会话的票。纯函数判定，无需真实票据。
func TestCookieIdentityMatchesSubsetOnDuplicateKeys(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"重复SAPISID异值", "SAPISID=aaa; __Secure-1PSID=ppp; SAPISID=bbb; SID=x"},
		{"重复1PSID异值", "SAPISID=aaa; __Secure-1PSID=ppp; __Secure-1PSID=qqq; SID=x"},
		{"首空值后非空", "SAPISID=; __Secure-1PSID=ppp; SAPISID=aaa; SID=x"},
		{"非空后空值", "SAPISID=aaa; __Secure-1PSID=ppp; SAPISID=; SID=x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ident := cookieIdentity(tc.input)
			sapisid, psid := "", ""
			if idx := strings.Index(ident, "|"); idx >= 0 {
				sapisid, psid = ident[:idx], ident[idx+1:]
			}
			subset := cookieSubset(tc.input, rotate1PSIDTSCookies)
			// 换发请求实际带出的值必须与身份裁决所依据的值一致：
			// 授权身份说是哪个会话，送出的就必须是那个会话的票。
			if subset != "" {
				if got := cookieValue(subset, "__Secure-1PSID"); got != "" && got != psid {
					t.Fatalf("1PSID 身份裁决值 %q 与实际换发带出值 %q 不一致", psid, got)
				}
				if psid != "" && !strings.Contains(subset, "__Secure-1PSID="+psid) {
					t.Fatalf("身份裁决的 1PSID %q 必须是实际换发带出的值: subset=%q", psid, subset)
				}
			}
			// SAPISID 同口径（SAPISIDHASH 授权头按它计算）。
			if firstNonEmpty := func() string {
				for _, kv := range splitCookiePairs(tc.input) {
					if kv[0] == "SAPISID" && kv[1] != "" {
						return kv[1]
					}
				}
				return ""
			}(); sapisid != firstNonEmpty {
				t.Fatalf("SAPISID 身份裁决值 %q 必须等于首个非空项 %q（与 cookieSubset 同口径）", sapisid, firstNonEmpty)
			}
		})
	}
	// 无重复输入行为不变（兼容性对照）。
	if got := cookieIdentity("SAPISID=aaa; __Secure-1PSID=ppp; SID=x"); got != "aaa|ppp" {
		t.Fatalf("无重复输入身份必须不变: %q", got)
	}
}

// TestHealRotationPerStepPickedRecheck 复核 R12-3（Oracle M3/Reviewer #4）：
// 自愈每一次独立外发前都用同一 picked 契约复核（found && enabled &&
// url == pickedProxyURL）。首发（1PSIDTS 哨兵 POST）期间面板同 ID 换
// URL 后：U1 的 GET /RotateCookiesPage 必须为 0（修复前按 Contains 计数
// 把 RotateCookiesPage 吞进 /RotateCookies，漏掉了这发新请求）；仅禁用
// 同 URL（URL 不变、enabled=false）后 /app 对 U1 命中为 0（修复前最终
// 快照丢弃 enabled，非严格模式下 xsrfExitConflict 不拦，/app 照样命中）。
func TestHealRotationPerStepPickedRecheck(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 601, Name: "pp", URL: "http://pu1.example:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = false
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookie := "SAPISID=ppx; __Secure-1PSID=ppp; SID=pps; __Secure-1PSIDTS=oldts"

	// 精准按 path 区分计数：RotateCookiesPage 与 RotateCookies 是两条
	// 独立外发（修复前的 Contains("/RotateCookies") 把前者吞进后者）。
	type counter struct {
		mu      sync.Mutex
		post1p  int // POST /RotateCookies（哨兵与 SIDCC 票据共用该 path）
		pageGet int // GET /RotateCookiesPage
		app     int
	}
	mkCountingRT := func(cnt *counter, flip func()) roundTripperFunc {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.URL.Path == "/RotateCookiesPage":
				cnt.mu.Lock()
				cnt.pageGet++
				cnt.mu.Unlock()
				// 若真到达：回一个合法 init(...) 页面让流程能继续（用于正对照）。
				h := http.Header{}
				h.Add("Set-Cookie", "SIDCC=pgnew; Domain=.google.com; Path=/; Secure")
				return &http.Response{
					StatusCode: 200, Status: "200 OK", Header: h,
					Body:    io.NopCloser(strings.NewReader(`init('abcdef1234', 658.0, 0.0, 0.0, 600.0)`)),
					Request: req,
				}, nil
			case req.URL.Path == "/RotateCookies":
				cnt.mu.Lock()
				cnt.post1p++
				cnt.mu.Unlock()
				if flip != nil {
					flip()
				}
				h := http.Header{}
				h.Add("Set-Cookie", "__Secure-1PSIDTS=ppnew; Domain=.google.com; Path=/; Secure; HttpOnly")
				return &http.Response{
					StatusCode: 200, Status: "200 OK",
					Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
					Request: req,
				}, nil
			case strings.Contains(req.URL.Path, "/app"):
				cnt.mu.Lock()
				cnt.app++
				cnt.mu.Unlock()
				return respJSON(req, 200, `"SNlM0e":"tok-abcdefgh"`), nil
			}
			return respJSON(req, 404, ""), nil
		})
	}

	seed := func(u string, rt http.RoundTripper) func() {
		timeout := rtCfg().RequestTimeout
		var keys []string
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			keys = append(keys, u+suf)
			stdlibClientCache.Store(u+suf, &http.Client{Transport: rt})
		}
		return func() {
			for _, k := range keys {
				stdlibClientCache.Delete(k)
			}
		}
	}

	t.Run("首发内换URL后页面GET对旧U1零命中", func(t *testing.T) {
		id, err := accountInsert("ppurl", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 601)
		proxyMu.Lock()
		proxyCache[0].Enabled = true
		proxyCache[0].URL = "http://pu1.example:8080"
		proxyMu.Unlock()
		cnt1, cnt2 := &counter{}, &counter{}
		var flipOnce sync.Once
		flip := func() {
			flipOnce.Do(func() {
				proxyMu.Lock()
				proxyCache[0].URL = "http://pu2.example:8080"
				proxyMu.Unlock()
			})
		}
		defer seed("http://pu1.example:8080", mkCountingRT(cnt1, flip))()
		defer seed("http://pu2.example:8080", mkCountingRT(cnt2, nil))()
		defer func() {
			proxyMu.Lock()
			proxyCache[0].URL = "http://pu1.example:8080"
			proxyMu.Unlock()
		}()

		acct := accountByID(id)
		_, _, rerr := rotateAccountWithOpts(*acct, rotateOpts{
			hasPickedProxy:    true,
			pickedProxyID:     601,
			expectedDBProxyID: 601,
			pickedProxyURL:    "http://pu1.example:8080",
		})
		_ = rerr // 哨兵已成功，SIDCC 复核失败时错误形态不固定——以计数为准
		cnt1.mu.Lock()
		pg1, post1 := cnt1.pageGet, cnt1.post1p
		cnt1.mu.Unlock()
		cnt2.mu.Lock()
		pg2, post2 := cnt2.pageGet, cnt2.post1p
		cnt2.mu.Unlock()
		if post1 != 1 {
			t.Fatalf("前置：哨兵首发必须经 U1 实际发出一次，得到 %d", post1)
		}
		// 核心断言：首发内换 URL 后，SIDCC 的页面 GET 绝不再向已撤换的
		// U1 新发（修复前 rotateSIDCC 无逐步复核，GET 照常发出）。
		if pg1 != 0 {
			t.Fatalf("换 URL 后 U1 的 GET /RotateCookiesPage 必须为 0，得到 %d", pg1)
		}
		if pg2 != 0 || post2 != 0 {
			t.Fatalf("自愈绝不许跨两个实际出口（U2 零命中）: page=%d post=%d", pg2, post2)
		}
	})

	t.Run("仅禁用同URL后app重取对U1零命中", func(t *testing.T) {
		id, err := accountInsert("ppdis", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 601)
		proxyMu.Lock()
		proxyCache[0].Enabled = true
		proxyCache[0].URL = "http://pu1.example:8080"
		proxyMu.Unlock()
		cnt1 := &counter{}
		var flipOnce sync.Once
		flip := func() {
			flipOnce.Do(func() {
				proxyMu.Lock()
				proxyCache[0].Enabled = false // URL 不变、仅禁用
				proxyMu.Unlock()
			})
		}
		defer seed("http://pu1.example:8080", mkCountingRT(cnt1, flip))()
		defer func() {
			proxyMu.Lock()
			proxyCache[0].Enabled = true
			proxyMu.Unlock()
		}()

		acct := accountByID(id)
		picked := Proxy{ID: 601, URL: "http://pu1.example:8080"}
		heal := healXSRFByRotation(ModelConfig{}, acct, picked, "http://pu1.example:8080",
			map[int64]bool{}, map[int64]bool{})
		if heal.healed {
			t.Fatal("出口已禁用后 heal 必须放弃（绝不向已禁用旧出口重取 XSRF）")
		}
		cnt1.mu.Lock()
		pg1, app1 := cnt1.pageGet, cnt1.app
		cnt1.mu.Unlock()
		// 核心断言：哨兵首发后仅禁用同 URL——页面 GET 与 /app 对 U1 均
		// 零命中（修复前最终快照丢弃 enabled，非严格模式 /app 照样命中）。
		if pg1 != 0 {
			t.Fatalf("禁用后 U1 的 GET /RotateCookiesPage 必须为 0，得到 %d", pg1)
		}
		if app1 != 0 {
			t.Fatalf("禁用后 /app 对 U1 命中必须为 0，得到 %d", app1)
		}
	})

	t.Run("出口未变时正常完成自愈对照", func(t *testing.T) {
		id, err := accountInsert("ppctl", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 601)
		proxyMu.Lock()
		proxyCache[0].Enabled = true
		proxyCache[0].URL = "http://pu1.example:8080"
		proxyMu.Unlock()
		cnt1 := &counter{}
		defer seed("http://pu1.example:8080", mkCountingRT(cnt1, nil))()
		acct := accountByID(id)
		picked := Proxy{ID: 601, URL: "http://pu1.example:8080"}
		heal := healXSRFByRotation(ModelConfig{}, acct, picked, "http://pu1.example:8080",
			map[int64]bool{}, map[int64]bool{})
		if !heal.healed {
			t.Fatal("出口未变更时自愈必须正常完成（正对照）")
		}
		cnt1.mu.Lock()
		pg1, app1 := cnt1.pageGet, cnt1.app
		cnt1.mu.Unlock()
		if pg1 != 1 || app1 != 1 {
			t.Fatalf("正对照：页面 GET 与 /app 各命中一次（逐步复核不误伤合法流程）: page=%d app=%d", pg1, app1)
		}
	})
}
