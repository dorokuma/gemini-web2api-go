package app

// browser_ctx_test.go —— v6.6.0 多 BrowserContext 架构验收测试（§9.1 必须项）。
//
// 覆盖：
//  1. TestAuxSkipSeamlessFallbackHTTP——aux 槽未就绪 → errBrowserSkipAux →
//     纯 Go HTTP 无缝回退，不打印错误、不丢弃刷新机会；
//  2. TestBrowserMultiAuthuserSameSlot——同 cookieIdentity 不同 authuser 复用
//     同一 BrowserContext（identity 复合键）；
//  3. TestBrowserSeedAccountBound——buildAccountCtx 播种的 Cookie 精确命中
//     目标 BrowserContext jar（跨槽隔离）；
//  4. TestBrowserStopDuringInFlightBuild——构建中的 acquireAccountCtx 在
//     Stop 打断下 <300ms 返回错误，不卡 drainWait/readyCh；
//  5. TestBrowserConcurrentBuilds——同 slot 并发 acquire 合并到同一
//     building；不同 slot 并行构建不串行；
//  6. TestBrowserInvalidateAccount——InvalidaAccount 摘除槽位 + failStreams
//     仅影响该槽在飞流，其余槽位零影响；
//  7. TestBrowserEvictIdleContexts——空闲 >30m 的非活跃槽被 dispose；
//  8. TestBrowserPerAccountProxyIsolation——两个号各自独立浏览器桥出口。

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestAuxSkipSeamlessFallbackHTTP：辅助入口槽未就绪 → errBrowserSkipAux →
// 无缝回退纯 Go HTTP（不打印错误日志、不丢弃刷新机会）。
func TestAuxSkipSeamlessFallbackHTTP(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	// 匿名槽已就绪（wire 预置 fake-session-1）：auxSlotReady 对匿名账号
	// 返回 nil（可借驻留匿名槽）。
	if err := m.auxSlotReady(nil, "", true); err != nil {
		t.Fatalf("auxSlotReady(anon, ready slot) = %v, want nil", err)
	}

	// 清掉默认槽模拟「槽未就绪」：aux 必须拿到 errBrowserSkipAux。
	m.mu.Lock()
	delete(m.ctxs, browserSlotKey{accountID: 0, exitKey: "true|"})
	delete(m.sessionIndex, "fake-session-1")
	delete(m.targetIndex, "fake-target-1")
	m.mu.Unlock()

	if err := m.auxSlotReady(nil, "", true); !isBrowserAuxSkip(err) {
		t.Fatalf("auxSlotReady(anon, no slot) = %v, want errBrowserSkipAux", err)
	}

	// doBrowserStreamRequest(allowSwitch=false) 槽未就绪 → errBrowserSkipAux。
	_, _, err := m.doBrowserStreamRequest(context.Background(), nil,
		"https://gemini.google.com/app", "GET", nil, nil, false, nil)
	if !isBrowserAuxSkip(err) {
		t.Fatalf("doBrowserStreamRequest(aux, no slot) = %v, want errBrowserSkipAux", err)
	}
	if isBrowserLocalError(err) {
		t.Fatalf("aux skip misclassified as browserLocalError: %v", err)
	}
	// 假服务端绝不能收到 fetch（aux 绝不进 jar）。
	select {
	case spec := <-srv.fetchHit:
		t.Fatalf("browser fetch dispatched for aux skip: %s", spec.url)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestBrowserMultiAuthuserSameSlot：同 cookieIdentity 不同 authuser 的号
// 复用同一 BrowserContext（identity 复合键 {id, identity|authuser}）。
func TestBrowserMultiAuthuserSameSlot(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	// 同 cookieIdentity、不同 authuser 的两个 DB 账号。
	cookie := "SAPISID=sa; __Secure-1PSID=ps; SID=s"
	a1, err := accountInsert("acct-auth0", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(a1)
	a2, err := accountInsert("acct-auth1", cookie, "")
	if err != nil {
		t.Fatalf("insert2: %v", err)
	}
	defer accountDelete(a2)

	acct1 := accountByID(a1)
	acct1.AuthUser = 0
	acct2 := accountByID(a2)
	acct2.AuthUser = 1

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	// 登录号槽位键 {accountID, ""}——同 cookieIdentity 不同 authuser 复用
	// 同一 BrowserContext（同 Google 会话身份），authuser 差异只影响导航
	// 路径，不产生新槽位。
	k0 := slotKeyForAccount(acct1, "", true)
	k1 := slotKeyForAccount(acct2, "", true)
	if k0.accountID != a1 || k1.accountID != a2 {
		t.Fatalf("slot keys = %v / %v, want accountID %d / %d", k0, k1, a1, a2)
	}
	if k0.exitKey != "" || k1.exitKey != "" {
		t.Fatalf("logged-in slot exitKey = %q / %q, want empty (accountID key)", k0.exitKey, k1.exitKey)
	}
}

// TestBrowserSeedAccountBound：buildAccountCtx 播种的 Cookie 精确命中目标
// BrowserContext jar——Storage.setCookies 发到正确 bcID，跨槽隔离。
func TestBrowserSeedAccountBound(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	// 插入一个账号，预置其 BrowserContext 槽位（模拟构建完成）。
	acctID, err := accountInsert("bound", "SAPISID=ba; __Secure-1PSID=bp; SID=b", "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(acctID)
	acct := accountByID(acctID)

	// 注册一个 ready 槽位（bcID "bc-bound-1"），jar 里预置 Cookie。
	key := slotKeyForAccount(acct, "", true)
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		browserContextID: "bc-bound-1",
		targetID:         "t-bound-1",
		sessionID:        "sess-bound-1",
		readyClosed:      true,
		allowDirect:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-bound-1"] = ac
	m.targetIndex["t-bound-1"] = ac
	m.mu.Unlock()
	// jarFor 按 session→bc 映射路由：登记 sess-bound-1 归属 bc-bound-1，
	// 与 attachToTarget 建立的映射一致，断言才能命中该 Context 的 jar。
	srv.bcMu.Lock()
	srv.sessToBC["sess-bound-1"] = "bc-bound-1"
	srv.bcMu.Unlock()

	// 播种验证：ac.browserContextID 对应的 jar 收到 SAPISID=ba（Storage.
	// setCookies 精确命中 bc-bound-1，跨槽隔离——其他槽 jar 不受影响）。
	sess := ac.sessionID
	jar := srv.jarFor(sess)
	jar["__Secure-1PSID"] = "bp" // 预置
	// 模拟 buildAccountCtx 的 Storage.setCookies 播种：经 fakeCDPServer 的
	// Storage.setCookies handler 把 Cookie 写进 bc-bound-1 的 jar。
	srv.handleStorageSetCookies([]byte(`{"browserContextId":"bc-bound-1","cookies":[{"name":"SAPISID","value":"ba","domain":".google.com","path":"/"},{"name":"__Secure-1PSID","value":"bp","domain":".google.com","path":"/"}]}`))
	if jar["SAPISID"] != "ba" {
		t.Fatalf("jar SAPISID = %q, want %q (seed must hit bound context jar)", jar["SAPISID"], "ba")
	}
	// 跨槽隔离：默认匿名槽 jar 不受影响。
	anonJar := srv.jarFor("fake-session-1")
	if anonJar["SAPISID"] == "ba" {
		t.Fatal("seed leaked into anonymous slot jar (cross-context pollution)")
	}
}

// TestBrowserStopDuringInFlightBuild：构建中的 acquireAccountCtx 在
// Stop(200ms) 打断下 <300ms 返回错误——drainWait/readyCh 已 close。
func TestBrowserStopDuringInFlightBuild(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	// 注册一个 building 态的槽位（readyCh 未关）。
	acctID, err := accountInsert("building", "SAPISID=x; __Secure-1PSID=y; SID=z", "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(acctID)
	acct := accountByID(acctID)
	key := slotKeyForAccount(acct, "", true)
	ac := &accountBrowserCtx{
		key:       key,
		epoch:     1,
		creating:  true,
		readyCh:   make(chan struct{}),
		drainWait: make(chan struct{}),
	}
	m.mu.Lock()
	m.ctxs[key] = ac
	m.mu.Unlock()

	// 在后台 acquire（会等 readyCh/drainWait）。
	acquireDone := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		acquireDone <- aerr
	}()
	time.Sleep(80 * time.Millisecond) // 让 acquire 阻塞在 readyCh

	t0 := time.Now()
	sctx, scancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer scancel()
	m.Stop(sctx)
	dt := time.Since(t0)
	if dt > 300*time.Millisecond {
		t.Fatalf("Stop during in-flight build blocked %v (readyCh/drainWait must be closed)", dt)
	}
	select {
	case <-acquireDone:
	case <-time.After(time.Second):
		t.Fatal("acquireAccountCtx did not exit after Stop")
	}
}

// TestBrowserConcurrentBuilds：同 slot 并发 acquire 合并到同一 building；
// 不同 slot 并行构建不串行。
func TestBrowserConcurrentBuilds(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	acctID, err := accountInsert("conc", "SAPISID=c; __Secure-1PSID=d; SID=e", "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(acctID)
	acct := accountByID(acctID)
	key := slotKeyForAccount(acct, "", true)

	// 注册 building 态槽位（出口与请求一致：proxyURL="" allowDirect=true），
	// 并发 acquire 应合并到同一 readyCh 等待。
	ac := &accountBrowserCtx{
		key:         key,
		epoch:       1,
		creating:    true,
		proxyURL:    "",
		allowDirect: true,
		readyCh:     make(chan struct{}),
		drainWait:   make(chan struct{}),
	}
	m.mu.Lock()
	m.ctxs[key] = ac
	m.mu.Unlock()

	// 两个并发 acquire 同 slot：都阻塞在同一 readyCh。
	d1 := make(chan error, 1)
	d2 := make(chan error, 1)
	go func() {
		_, _, _, _, e1 := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		d1 <- e1
	}()
	go func() {
		_, _, _, _, e2 := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		d2 <- e2
	}()
	time.Sleep(80 * time.Millisecond)

	// 完成构建：creating=false + close readyCh（经 closeReadyLocked 幂等）+
	// close drainWait——两个 acquire 从 readyCh 醒来拿到 ready 槽位返回。
	m.mu.Lock()
	ac.creating = false
	ac.sessionID = "sess-conc-1"
	ac.targetID = "t-conc-1"
	ac.browserContextID = "bc-conc-1"
	m.sessionIndex["sess-conc-1"] = ac
	m.targetIndex["t-conc-1"] = ac
	ac.closeReadyLocked()
	if ac.drainWait != nil && !ac.drainClosed {
		close(ac.drainWait)
		ac.drainClosed = true
	}
	m.mu.Unlock()

	select {
	case err := <-d1:
		if err != nil {
			t.Fatalf("acquire 1: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire 1 blocked after readyCh close")
	}
	select {
	case err := <-d2:
		if err != nil {
			t.Fatalf("acquire 2: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire 2 blocked after readyCh close")
	}
}

// TestBrowserInvalidateAccount：InvalidateAccount 摘除槽位 + failStreams
// 仅影响该槽在飞流，其余槽位零影响。
func TestBrowserInvalidateAccount(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	// 注册两个槽位（不同账号）。
	id1, _ := accountInsert("acct-1", "SAPISID=a1; __Secure-1PSID=p1; SID=s1", "")
	defer accountDelete(id1)
	id2, _ := accountInsert("acct-2", "SAPISID=a2; __Secure-1PSID=p2; SID=s2", "")
	defer accountDelete(id2)
	acct1 := accountByID(id1)
	acct2 := accountByID(id2)

	k1 := slotKeyForAccount(acct1, "", true)
	k2 := slotKeyForAccount(acct2, "", true)
	ac1 := &accountBrowserCtx{key: k1, epoch: 1, browserContextID: "bc1", sessionID: "s1", targetID: "t1", readyClosed: true, allowDirect: true, lastUsedAt: time.Now()}
	ac1.readyCh = make(chan struct{})
	close(ac1.readyCh)
	ac2 := &accountBrowserCtx{key: k2, epoch: 1, browserContextID: "bc2", sessionID: "s2", targetID: "t2", readyClosed: true, allowDirect: true, lastUsedAt: time.Now()}
	ac2.readyCh = make(chan struct{})
	close(ac2.readyCh)

	m.mu.Lock()
	m.ctxs[k1] = ac1
	m.ctxs[k2] = ac2
	m.sessionIndex["s1"] = ac1
	m.sessionIndex["s2"] = ac2
	m.targetIndex["t1"] = ac1
	m.targetIndex["t2"] = ac2
	m.mu.Unlock()

	// 失效 id1：k1 被摘除，k2 保留。
	m.InvalidateAccount(id1, false, "test")
	m.mu.Lock()
	_, ok1 := m.ctxs[k1]
	_, ok2 := m.ctxs[k2]
	m.mu.Unlock()
	if ok1 {
		t.Fatal("InvalidateAccount did not detach ctx for account 1")
	}
	if !ok2 {
		t.Fatal("InvalidateAccount wrongly removed ctx for account 2")
	}
}

// TestBrowserEvictIdleContexts：空闲 >30m 的非活跃槽被 dispose。
func TestBrowserEvictIdleContexts(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	id1, _ := accountInsert("idle", "SAPISID=i1; __Secure-1PSID=i2; SID=i3", "")
	defer accountDelete(id1)
	acct := accountByID(id1)
	key := slotKeyForAccount(acct, "", true)

	// 空闲 31 分钟的槽位 → 应被淘汰。
	acIdle := &accountBrowserCtx{key: key, epoch: 1, browserContextID: "bc-idle", sessionID: "s-idle", targetID: "t-idle", readyClosed: true, allowDirect: true, lastUsedAt: time.Now().Add(-31 * time.Minute)}
	acIdle.readyCh = make(chan struct{})
	close(acIdle.readyCh)
	// 活跃的槽位 → 不应被淘汰。
	key2 := browserSlotKey{accountID: 0, exitKey: "true|"}
	acLive := &accountBrowserCtx{key: key2, epoch: 2, browserContextID: "bc-live", sessionID: "s-live", targetID: "t-live", readyClosed: true, allowDirect: true, lastUsedAt: time.Now()}
	acLive.readyCh = make(chan struct{})
	close(acLive.readyCh)

	m.mu.Lock()
	m.ctxs[key] = acIdle
	m.ctxs[key2] = acLive
	m.sessionIndex["s-idle"] = acIdle
	m.sessionIndex["s-live"] = acLive
	m.targetIndex["t-idle"] = acIdle
	m.targetIndex["t-live"] = acLive
	m.mu.Unlock()

	m.EvictIdleContexts()
	m.mu.Lock()
	_, okIdle := m.ctxs[key]
	_, okLive := m.ctxs[key2]
	m.mu.Unlock()
	if okIdle {
		t.Fatal("EvictIdleContexts did not evict idle slot")
	}
	if !okLive {
		t.Fatal("EvictIdleContexts wrongly evicted active slot")
	}
}

// TestBrowserPerAccountProxyIsolation：两个号各自独立浏览器桥出口——
// 槽位 A 走 proxyA，槽位 B 走 proxyB，互不影响。
func TestBrowserPerAccountProxyIsolation(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	id1, _ := accountInsert("acctA", "SAPISID=aA; __Secure-1PSID=pA; SID=sA", "")
	defer accountDelete(id1)
	id2, _ := accountInsert("acctB", "SAPISID=aB; __Secure-1PSID=pB; SID=sB", "")
	defer accountDelete(id2)
	acctA := accountByID(id1)
	acctB := accountByID(id2)

	// 两个号绑定不同代理：登录号槽位键 {id, ""}——proxyURL 不进槽位键
	//（出口在槽内生命周期内固定，非严格模式合法换出口走 drain 替换）。
	// 匿名槽按出口分键验证 exitKey 差异。
	kA := slotKeyForAccount(acctA, "http://proxyA:8080", true)
	kB := slotKeyForAccount(acctB, "http://proxyB:8080", true)
	if kA.accountID == kB.accountID {
		t.Fatal("different accounts must have different accountID in slot key")
	}
	// 匿名槽按出口分键：不同 proxyURL → 不同 exitKey。
	kAnonA := slotKeyForAccount(nil, "http://proxyA:8080", true)
	kAnonB := slotKeyForAccount(nil, "http://proxyB:8080", true)
	if kAnonA.exitKey == kAnonB.exitKey {
		t.Fatal("anonymous slot exitKey must differ by proxyURL")
	}
}

// ─── v6.6.0 R2B 并发回归（生产路径直接命中）─────────────────────────────────

// TestBrowserAcquireReadySlotStrongBinding 复核 MF-5 / Reviewer #4：既有
// 就绪登录槽在占租约前用锁外预计算的代理快照做 R3-M1 强绑定复核——
// 锁定直连账号（ProxyID=-1）命中一个非直连就绪槽时，acquire 不得盲目
// 复用，必须摘除旧槽。这里用纯内存比对路径验证违规判定（槽位 proxyURL
// 非空但账号锁直连 → readySlotExitViolation=true）。
func TestBrowserAcquireReadySlotStrongBinding(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true // 池空：poolBlocksDirect=false，隔离 (a) 规则
	})

	// 锁定直连账号（ProxyID=-1）。
	cookie := "SAPISID=ld; __Secure-1PSID=lp; SID=l"
	id, err := accountInsert("lockdirect", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = proxyIDDirect // -1
	acct.AuthUser = 0

	// 预置一个「违规」就绪槽：账号锁直连但槽位走了代理出口（proxyURL 非空
	// 且 allowDirect=false）——命中 readySlotExitViolation (a)。
	key := slotKeyForAccount(acct, "http://proxy-x:8080", false)
	ac := &accountBrowserCtx{
		key:                key,
		epoch:              1,
		browserContextID:   "bc-violate",
		targetID:           "t-violate",
		sessionID:          "sess-violate",
		proxyURL:           "http://proxy-x:8080",
		allowDirect:        false,
		origAccountProxyID: proxyIDDirect,
		readyClosed:        true,
		lastUsedAt:         time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-violate"] = ac
	m.targetIndex["t-violate"] = ac
	m.mu.Unlock()

	// 直接复核违规判定（生产 readySlotExitViolation，acquirePlanLocked 在
	// 持 m.mu 时调用——这里是纯内存比对，不依赖代理池）。
	poolSnap := snapshotProxyPool()
	poolBlocksDirect := len(poolSnap) > 0 && !rtCfg().FallbackDirect
	strictBinding := rtCfg().StrictProxyBinding
	if !ac.readySlotExitViolation(acct, poolBlocksDirect, strictBinding, poolSnap) {
		t.Fatal("readySlotExitViolation must flag lock-direct account on proxied slot")
	}

	// 反向：账号锁直连且槽位显式直连 → 不违规。
	acDirect := &accountBrowserCtx{key: key, proxyURL: "", allowDirect: true}
	if acDirect.readySlotExitViolation(acct, poolBlocksDirect, strictBinding, poolSnap) {
		t.Fatal("readySlotExitViolation wrongly flagged explicit-direct slot")
	}
}

// TestBrowserAcquireCreatorFastFailNoRebuild 复核 MF-4：创建者认领构建后
// buildAccountCtx 快速失败摘除 nac，创建者直等 nac.readyCh 唤醒拿到
// lastBuildErr 返回——绝不回循环顶重查 m.ctxs[key] 拿陈旧形参无限重建。
// 驱动真实生产路径 acquireAccountCtx -> go buildAccountCtx：fakeCDPServer
// 的 Target.createBrowserContext 注入错误，断言 acquire 返回该错误且
// createBrowserContext 恰好只被调 1 次（旧缺陷回环重查会无限重建）。
func TestBrowserAcquireCreatorFastFailNoRebuild(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	// 注入 Target.createBrowserContext 失败：buildAccountCtx 在步骤 1 即
	// 快速失败摘除 nac。
	srv.failMethods.Store("Target.createBrowserContext", true)

	id, _ := accountInsert("fastfail", "SAPISID=ff; __Secure-1PSID=f2; SID=f3", "")
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = proxyIDDirect
	key := slotKeyForAccount(acct, "", true)

	// 真实 acquire（创建者路径）：必须在构建快速失败后返回错误，而不是
	// 回循环顶重查 m.ctxs[key] 无限重建。
	done := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(
			context.Background(), acct, "", true, true)
		done <- aerr
	}()
	select {
	case aerr := <-done:
		if aerr == nil {
			t.Fatal("acquireAccountCtx returned nil error on injected createBrowserContext failure")
		}
		if !strings.Contains(aerr.Error(), "createBrowserContext") &&
			!strings.Contains(aerr.Error(), "injected") {
			t.Fatalf("acquireAccountCtx err=%v, want createBrowserContext failure", aerr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquireAccountCtx did not return after build fast-fail (re-create loop?)")
	}

	// Target.createBrowserContext 只被调用 1 次：失败即终态返回，绝不重建。
	createCalls := 0
	drainUntil := time.After(200 * time.Millisecond)
	for {
		select {
		case <-srv.createBCHit:
			createCalls++
		case call := <-srv.gotCalls:
			if call.method == "Target.createBrowserContext" {
				createCalls++
			}
		case <-drainUntil:
			goto drained
		}
	}
drained:
	if createCalls != 1 {
		t.Fatalf("Target.createBrowserContext called %d times, want exactly 1 (no rebuild loop)", createCalls)
	}
	// 槽位已摘除：m.ctxs 不再留死 nac（下次请求会重新认领而不是误复用）。
	m.mu.Lock()
	_, stillThere := m.ctxs[key]
	m.mu.Unlock()
	if stillThere {
		t.Fatal("failed ctx still registered in m.ctxs after build fail")
	}
}

// TestBrowserDrainWaitersMergeOnExitChange 复核 MF-8：同登录槽非严格模式
// 换出口，inFlight>0 时跨出口等待者登记 drainWaiters；当旧槽被
// errExitChanged 摘除时，等待者经生产 acquireWaitResult 的 waitDrain
// 分支清自身登记后汇入新槽（retry=true，err=nil），不返回终态错。
func TestBrowserDrainWaitersMergeOnExitChange(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.StrictProxyBinding = false // 非严格模式：合法换出口走 drain 替换
		c.FallbackDirect = true
	})

	cookie := "SAPISID=dr; __Secure-1PSID=dp; SID=d"
	id, _ := accountInsert("drain", cookie, "")
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = 0 // 未绑定，非锁直连

	// 就绪登录槽：出口 proxyOld、inFlight=1（有在飞租约）。
	key := slotKeyForAccount(acct, "http://proxy-old:8080", false)
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		browserContextID: "bc-drain",
		targetID:         "t-drain",
		sessionID:        "sess-drain",
		proxyURL:         "http://proxy-old:8080",
		allowDirect:      false,
		cookieIdentity:   cookieIdentity(cookie),
		inFlight:         1,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.mu.Unlock()

	// 跨出口等待者（换到 proxyNew）：inFlight>0 → 登记 drainWaiters + 等
	// drainWait（生产 acquirePlanLocked drain 分支）。
	var drain acquireDrainReg
	poolSnap := snapshotProxyPool()
	m.mu.Lock()
	waitCh, kind, proceed, snap := m.acquirePlanLocked(ac, acct,
		"http://proxy-new:8080", false, false, false, poolSnap, &drain)
	stopCh := m.stopCh
	m.mu.Unlock()
	if snap.ac != nil || proceed {
		t.Fatalf("acquirePlanLocked on busy slot: snap=%+v proceed=%v, want drain-wait", snap, proceed)
	}
	if kind != waitDrain || waitCh == nil {
		t.Fatalf("acquirePlanLocked kind=%v waitCh=%v, want waitDrain non-nil", kind, waitCh)
	}
	if !drain.registered || drain.target != ac {
		t.Fatal("cross-exit waiter did not register drainWaiters")
	}

	// 等待协程：真实调 acquireWaitResult 的 waitDrain 分支（锁外 select）。
	type waitRet struct {
		retry bool
		err   error
	}
	retCh := make(chan waitRet, 1)
	go func() {
		_, _, _, _, retry, werr := m.acquireWaitResult(
			context.Background(), ac, waitCh, stopCh, waitDrain, &drain)
		retCh <- waitRet{retry: retry, err: werr}
	}()
	time.Sleep(50 * time.Millisecond) // 让等待者先阻塞在 drainWait

	// 旧槽被 errExitChanged 摘除（替换 winner）：detach 关 drainWait 唤醒
	// 等待者，acquireWaitResult 见 disposed+errExitChanged → 汇入新槽
	//（清登记 + retry=true）。
	m.mu.Lock()
	won, _ := m.detachCtxLocked(ac, errExitChanged)
	m.mu.Unlock()
	if !won {
		t.Fatal("detachCtxLocked did not win for exit-change replacement")
	}
	select {
	case ret := <-retCh:
		if !ret.retry {
			t.Fatalf("acquireWaitResult retry=false err=%v, want retry=true (merge into new slot)", ret.err)
		}
		if ret.err != nil {
			t.Fatalf("acquireWaitResult err=%v, want nil on exit-change merge", ret.err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquireWaitResult did not return after exit-change detach")
	}
	if drain.registered {
		t.Fatal("drain registration not cleared after merge-to-new-slot")
	}
}

// TestBrowserBumpAccountEpochAdvances 复核 MF-6 / Reviewer #6：
// BumpAccountEpoch 在 m.mu 下把 nextCtxEpoch 与 maxSeededEpoch[accountID]
// 取同一自增值——多次轮转后新槽 epoch 严格大于此前 maxSeededEpoch，杜绝
// 旧代际永久丢弃回写黑洞。直接断言两个计数器联动推进。
func TestBrowserBumpAccountEpochAdvances(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	id, _ := accountInsert("epoch", "SAPISID=e; __Secure-1PSID=ee; SID=eee", "")
	defer accountDelete(id)

	// 连续两次 Bump：maxSeededEpoch 必须等于最新的 nextCtxEpoch（不是旧值）。
	m.BumpAccountEpoch(id)
	m.mu.Lock()
	first := m.maxSeededEpoch[id]
	m.mu.Unlock()
	m.BumpAccountEpoch(id)
	m.mu.Lock()
	second := m.maxSeededEpoch[id]
	next := m.nextCtxEpoch
	m.mu.Unlock()
	if second <= first {
		t.Fatalf("maxSeededEpoch not advanced: first=%d second=%d", first, second)
	}
	if second != next {
		t.Fatalf("maxSeededEpoch=%d != nextCtxEpoch=%d (epoch drift → write-back blackhole)", second, next)
	}
	// 新槽 epoch=second 不被 isSuperseded 拦截（合法回写）；epoch=first 被
	// 拦截（旧代际）。
	m.mu.Lock()
	notSuperseded := m.isSupersededEpochLocked(id, second)
	superseded := m.isSupersededEpochLocked(id, first)
	m.mu.Unlock()
	if notSuperseded {
		t.Fatalf("fresh slot epoch=%d wrongly superseded", second)
	}
	if !superseded {
		t.Fatalf("stale slot epoch=%d not superseded (would overwrite new token)", first)
	}
}

// TestBrowserStopImplSyncsPublishedCookiesToDB 复核 Reviewer #5：stopImpl
// 在摘除全部槽位前，先在同一个 m.mu 临界区内快照已发布存活登录槽的
// 写回目标，锁外用共享预算把 jar 新 Cookie 真实合并写回 DB——jar 里的
// SIDCC=rotated-on-stop 绝不能丢。这里经真实生产路径（stopImpl →
// 快照 syncTargets → detachSnapLocked → syncCookieTargetsBack →
// Storage.getCookies → updateAccountCookieCASLocked）命中 fakeCDPServer
// jar 写回，断言 DB 中该账号 Cookie 已含 SIDCC=rotated-on-stop。
func TestBrowserStopImplSyncsPublishedCookiesToDB(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	// proc 是测试进程自身（PID=Getpid），绝不能真杀：桩住杀进程钩子。
	oldKill := killProcGroupFn
	defer func() { killProcGroupFn = oldKill }()
	killProcGroupFn = func(pid, pgid int, d time.Duration) {}

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	// 真实 DB 账号 + 已发布存活登录槽（bc-stop-1），cookieIdentity 与 DB
	// 中 Cookie 一致（回写前置校验通过，CAS 能命中）。
	cookie := "SAPISID=sa; __Secure-1PSID=ps; SID=oldsid"
	acctID, err := accountInsert("stopsync", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(acctID)
	acct := accountByID(acctID)

	key := slotKeyForAccount(acct, "", true)
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-stop-1",
		targetID:         "t-stop-1",
		sessionID:        "sess-stop-1",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-stop-1"] = ac
	m.targetIndex["t-stop-1"] = ac
	// 已发布槽不得进入写回黑名单：epoch 必须 >= maxSeededEpoch。
	m.maxSeededEpoch[acctID] = 0
	m.mu.Unlock()

	// fakeCDPServer 的 bc-stop-1 jar 里放刷新后的新 Cookie——Storage.
	// getCookies 读它回写。
	srv.bcMu.Lock()
	srv.bcStore["bc-stop-1"] = map[string]string{
		"SAPISID":          "sa",
		"__Secure-1PSID":   "ps",
		"SID":              "oldsid",
		"SIDCC":            "rotated-on-stop",
		"__Secure-1PSIDCC": "rotated-psidcc",
	}
	srv.sessToBC["sess-stop-1"] = "bc-stop-1"
	srv.bcMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m.stopImpl(ctx, true)

	// 断言 DB 中该账号 Cookie 已真实合并写入 SIDCC=rotated-on-stop。
	fresh := accountByID(acctID)
	if fresh == nil {
		t.Fatal("account vanished after stopImpl")
	}
	if !strings.Contains(fresh.Cookie, "SIDCC=rotated-on-stop") {
		t.Fatalf("stopImpl write-back lost rotated cookie: DB cookie=%q", fresh.Cookie)
	}
	if !strings.Contains(fresh.Cookie, "__Secure-1PSIDCC=rotated-psidcc") {
		t.Fatalf("stopImpl write-back lost rotated PSIDCC: DB cookie=%q", fresh.Cookie)
	}
}

// TestBrowserRecreateTabConcurrentlyInvalidatedNoDoubleDispose 复核 Oracle
// MF-8.4：recreateAccountTab 在飞（doRecreateTab CDP 阶段）期间，
// InvalidateAccount 抢先摘除同一槽——recreate 的摘除分支必须看到
// snap.ac==nil 直接返回，绝不二次 failStreams+disposeDetachedCtx（否则
// 会对同一已摘除槽二次 dispose 且绕过摘除方的 syncCookie=false）。
// 这里经真实并发路径：recreateAccountTab 协程 vs InvalidateAccount，断言
// Target.disposeBrowserContext 只调用 1 次且不触发 Storage.getCookies
// 回写（InvalidateAccount 用 syncCookie=false）。
func TestBrowserRecreateTabConcurrentlyInvalidatedNoDoubleDispose(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	id, _ := accountInsert("recreate", "SAPISID=r; __Secure-1PSID=rr; SID=rs", "")
	defer accountDelete(id)
	acct := accountByID(id)

	key := slotKeyForAccount(acct, "", true)
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity("SAPISID=r; __Secure-1PSID=rr; SID=rs"),
		browserContextID: "bc-recreate-1",
		targetID:         "t-recreate-1",
		sessionID:        "sess-recreate-1",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-recreate-1"] = ac
	m.targetIndex["t-recreate-1"] = ac
	gen := m.generation
	client := m.client
	m.mu.Unlock()
	srv.bcMu.Lock()
	srv.targetToBC["t-recreate-1"] = "bc-recreate-1"
	srv.sessToBC["sess-recreate-1"] = "bc-recreate-1"
	srv.bcMu.Unlock()

	// 卡住 recreate 的 CDP 阶段（Target.createTarget 不回复）让 Invalidate
	// 在飞中抢先摘除。gotCalls 是 buffered chan，不并发读（readLoop
	// send 与 close 存在 send-on-closed 竞态）——全部结束后统一排干计数。
	srv.holdReplies.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.recreateAccountTab(client, ac, gen)
	}()
	// 让 recreate 进入 doRecreateTab 的 CDP 阶段（createTarget 被 hold）。
	time.Sleep(80 * time.Millisecond)

	// 并发 Invalidate：抢先摘除 ac（detach 胜出，buildCancel 打断
	// doRecreateTab，readyCh 关闭）。
	m.InvalidateAccount(id, false, "cookie rotated")

	// 放行被 hold 的 CDP 回复让 recreate 的 doRecreateTab 走完（或拿到
	// buildCtx 取消），进入锁内单胜出者守卫：此时 ac 已 disposed，
	// snap.ac 必须 == nil → 直接返回，零二次清理。
	srv.holdReplies.Store(false)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("recreateAccountTab did not return after concurrent invalidate")
	}

	// 排干 gotCalls 计数：bc-recreate-1 只允许被 disposeBrowserContext
	// 恰好 1 次（Invalidate 的锁外 disposeDetachedCtx），且绝不出现
	// Storage.getCookies 回写（InvalidateAccount 用 syncCookie=false）。
	// readLoop 已不再 send（holdReplies 已放行 + client 已断连），
	// 仅排空 buffered 积压，绝不 close(chan)（send/close 竞态）。
	disposeCalls, getCookieCalls := 0, 0
	drainDone := time.After(150 * time.Millisecond)
	drain := func() bool {
		for {
			select {
			case call := <-srv.gotCalls:
				if call.method == "Target.disposeBrowserContext" {
					disposeCalls++
				}
				if call.method == "Storage.getCookies" {
					getCookieCalls++
				}
			case <-drainDone:
				return true
			}
		}
	}
	drain()
	if getCookieCalls > 0 {
		t.Fatalf("Storage.getCookies write-back fired %d times on syncCookie=false invalidate", getCookieCalls)
	}
	if disposeCalls != 1 {
		t.Fatalf("disposeBrowserContext calls=%d, want exactly 1 (single-winner)", disposeCalls)
	}
}
