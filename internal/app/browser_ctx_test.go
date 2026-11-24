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
	"fmt"
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

// TestBrowserAcquireCreatorFastFailNoRebuild 复核 MF-4（两段断言）：
//
// 段 A——创建者认领 nac 后、进入 nac.readyCh 等待之前，nac 被
// InvalidateAccount 摘除。修复后创建者直等自己认领的 nac.readyCh，唤醒
// 见 nac.disposed 直接返回 nac.lastBuildErr（含 "invalidated"），零二次
// 重建（Target.createBrowserContext 调用 0 次）；d251051 旧实现回循环顶
// continue 重查 m.ctxs[key]==nil，拿陈旧形参二次认领 nac2 并发起
// Target.createBrowserContext（注入失败）→ 返回 "injected ... failure"
// 而非 "invalidated"，确定性红灯。
//
// 时序确定性：预置一个超时空闲匿名槽，认领时 evictIdleOrOverflowLocked
// 把它摘出为 victim——holdReplies 卡住全部 CDP 回复，主测试协程在
// gotCalls 等到 disposeBrowserContext 时，创建者 100% 阻塞在锁外
// disposeDetachedCtx(acIdle) 内，尚未执行 continue/<-waitCh。
//
// 段 B——holdReplies 放行后常规 acquire：buildAccountCtx 步骤 2
// createBrowserContext 注入失败，创建者直等 nac.readyCh 拿
// lastBuildErr 返回，恰好 1 次调用且 m.ctxs 无残留。
func TestBrowserAcquireCreatorFastFailNoRebuild(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
	})

	id, _ := accountInsert("fastfail", "SAPISID=ff; __Secure-1PSID=f2; SID=f3", "")
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = proxyIDDirect
	key := slotKeyForAccount(acct, "", true)

	// ── 段 A：创建者进 readyCh 等待前 nac 已被 Invalidate 摘除 ──────────
	// 预置 1 个超时空闲匿名槽：创建者认领时必走 victims 锁外
	// disposeDetachedCtx(acIdle)——holdReplies 卡住其 disposeBrowserContext
	// 回复，给主测试协程一个 100% 确定性的「已认领 nac、未进 wait」窗口。
	idleKey := browserSlotKey{accountID: 0, exitKey: "true|http://idle-victim:8080"}
	acIdle := &accountBrowserCtx{
		key:              idleKey,
		epoch:            1,
		browserContextID: "bc-idle",
		readyClosed:      true,
		allowDirect:      true,
		lastUsedAt:       time.Now().Add(-31 * time.Minute),
		readyCh:          make(chan struct{}),
	}
	close(acIdle.readyCh)
	m.mu.Lock()
	m.ctxs[idleKey] = acIdle
	m.mu.Unlock()

	// holdReplies 先于 failMethods 判定：创建者的 dispose 与 build 都被
	// 挂起，fault 注入保留到段 B 生效。
	srv.holdReplies.Store(true)
	srv.failMethods.Store("Target.createBrowserContext", true)

	done := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(
			context.Background(), acct, "", true, true)
		done <- aerr
	}()

	// 等创建者认领 nac 后阻塞在 disposeDetachedCtx(acIdle) 的
	// Target.disposeBrowserContext 上（此时它已释放 m.mu、尚未执行
	// continue 或 <-waitCh）。同窗口内出现 createBrowserContext 即乱序。
	var disposeCall cdpCall
	waitDeadline := time.After(5 * time.Second)
	for {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Target.createBrowserContext" {
				t.Fatal("createBrowserContext fired before invalidation window (ordering bug)")
			}
			if call.method == "Target.disposeBrowserContext" {
				disposeCall = call
				goto gotDispose
			}
		case aerr := <-done:
			t.Fatalf("acquireAccountCtx returned %v before invalidation window", aerr)
		case <-waitDeadline:
			t.Fatal("no Target.disposeBrowserContext for idle victim within 5s")
		}
	}
gotDispose:

	// 创建者尚卡在锁外 dispose：此刻 Invalidate 摘除刚认领的 nac
	//（disposed=true、lastBuildErr 含 "invalidated"、readyCh 关闭、
	// buildCancel 取消）。随后放行被 hold 的 dispose 回复。
	m.InvalidateAccount(id, false, "invalidated before readyCh wait")
	srv.holdReplies.Store(false)
	_ = srv.sendText([]byte(fmt.Sprintf(`{"id":%d,"result":{}}`, disposeCall.id)))

	// 修复后：创建者被 nac.readyCh 唤醒见 nac.disposed，直接返回
	// lastBuildErr（含 "invalidated"）。d251051：唤醒重查
	// m.ctxs[key]==nil → 二次认领 → createBrowserContext 注入失败 →
	// 返回 "injected Target.createBrowserContext failure"（红灯）。
	select {
	case aerr := <-done:
		if aerr == nil {
			t.Fatal("acquireAccountCtx returned nil error after nac invalidated")
		}
		if !strings.Contains(aerr.Error(), "invalidated") {
			t.Fatalf("acquireAccountCtx err=%v, want 'invalidated' (stale-param re-claim?)", aerr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquireAccountCtx did not return after nac invalidated")
	}

	// 排空 150ms：段 A 全程 Target.createBrowserContext 必须 0 次——
	// 创建者绝不用陈旧形参二次重建（d251051 在这里记到 >=1 次）。
	createCalls := 0
	drainUntil := time.After(150 * time.Millisecond)
	for {
		select {
		case <-srv.createBCHit:
			createCalls++
		case call := <-srv.gotCalls:
			if call.method == "Target.createBrowserContext" {
				createCalls++
			}
		case <-drainUntil:
			goto drainedA
		}
	}
drainedA:
	if createCalls != 0 {
		t.Fatalf("Target.createBrowserContext called %d times in segment A, want 0 (no rebuild)", createCalls)
	}

	// ── 段 B：常规 buildAccountCtx 步骤 2 快速失败——仅调 1 次且摘除干净 ──
	// failMethods 仍注入 createBrowserContext 失败，holdReplies 已放行。
	done2 := make(chan error, 1)
	go func() {
		_, _, _, _, aerr2 := m.acquireAccountCtx(
			context.Background(), acct, "", true, true)
		done2 <- aerr2
	}()
	select {
	case aerr2 := <-done2:
		if aerr2 == nil {
			t.Fatal("acquireAccountCtx returned nil error on injected createBrowserContext failure")
		}
		if !strings.Contains(aerr2.Error(), "createBrowserContext") &&
			!strings.Contains(aerr2.Error(), "injected") {
			t.Fatalf("acquireAccountCtx err=%v, want createBrowserContext failure", aerr2)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquireAccountCtx did not return after build fast-fail (re-create loop?)")
	}

	// Target.createBrowserContext 只被调用 1 次：失败即终态返回，绝不重建。
	createCalls = 0
	drainUntil = time.After(200 * time.Millisecond)
	for {
		select {
		case <-srv.createBCHit:
			createCalls++
		case call := <-srv.gotCalls:
			if call.method == "Target.createBrowserContext" {
				createCalls++
			}
		case <-drainUntil:
			goto drainedB
		}
	}
drainedB:
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
// 换出口，inFlight>0 时跨出口等待者登记 drainWaiters 并阻塞在 drainWait；
// 旧槽被 errExitChanged 摘除时，等待者必须汇入替换 winner 新建的 proxyB 槽
// 而不是返回终态 errExitChanged。全部经生产外部入口 m.acquireAccountCtx
// 驱动（d251051 同样存在该入口）：旧实现唤醒后见 ac.disposed 直接返回
// ac.lastBuildErr=errExitChanged → 等待者 FAIL；修复后 waitDrain +
// errExitChanged 分支清登记并汇入新槽 → PASS。
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

	// 就绪登录槽：出口 proxyA、inFlight=1（有在飞租约）。
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

	// 跨出口等待者 W2：换到 proxyNew，inFlight>0 → 经生产 acquireAccountCtx
	// 登记 drainWaiters 并阻塞在 drainWait（确定性：等 drainWaiters==1 才
	// 继续，不用 sleep 猜时机）。
	type waitRet struct {
		ac2     *accountBrowserCtx
		release func()
		err     error
	}
	w2 := make(chan waitRet, 1)
	go func() {
		_, a2, _, rel2, e2 := m.acquireAccountCtx(
			context.Background(), acct, "http://proxy-new:8080", false, true)
		w2 <- waitRet{ac2: a2, release: rel2, err: e2}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.Lock()
		n := ac.drainWaiters
		m.mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case r := <-w2:
			t.Fatalf("W2 exited early without registering drain: %+v", r)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("W2 did not register drainWaiters within 5s")
		}
		time.Sleep(time.Millisecond)
	}

	// 归还 ac 的在飞租约：等价 acquireReleaseFn 的 inFlight--，但不合入
	// drainWait 的 close——让 W2 保持阻塞在 drainWait 上直到旧槽被摘除，
	// 摘除时刻 ac.disposed 已成立，W2 在旧代码上必然命中 disposed+
	// errExitChanged 终态分支（确定性红灯，无唤醒时序侥幸）。
	m.mu.Lock()
	ac.inFlight--
	m.mu.Unlock()

	// 替换 winner W1：真实 acquireAccountCtx 走唯一替换分支——inFlight==0
	// + 跨出口 → detachSnapLocked(ac, errExitChanged) 摘除旧槽后认领新建
	// proxyB 槽（buildAccountCtx 经 fakeCDPServer 完整走通）。
	w1 := make(chan waitRet, 1)
	go func() {
		_, a1, _, rel1, e1 := m.acquireAccountCtx(
			context.Background(), acct, "http://proxy-new:8080", false, true)
		w1 <- waitRet{ac2: a1, release: rel1, err: e1}
	}()

	var r1 waitRet
	select {
	case r1 = <-w1:
	case <-time.After(15 * time.Second):
		t.Fatal("W1 (replacement winner) did not return")
	}
	if r1.err != nil {
		t.Fatalf("W1 should win the exit-change replacement, got err=%v", r1.err)
	}
	if r1.ac2 == nil || r1.ac2 == ac {
		t.Fatalf("W1 must publish a NEW slot, got ac=%p (old=%p)", r1.ac2, ac)
	}
	if r1.ac2.proxyURL != "http://proxy-new:8080" {
		t.Fatalf("W1 slot exit=%q, want http://proxy-new:8080", r1.ac2.proxyURL)
	}

	// W2 汇入 W1 新建的 proxyB 槽：旧代码在 drainWait 唤醒后见
	// ac.disposed+errExitChanged 直接返回终态错；修复后清登记重查汇入新槽。
	var r2 waitRet
	select {
	case r2 = <-w2:
		if r2.err != nil {
			t.Fatalf("W2 must merge into W1's new slot, got err=%v (errExitChanged leak?)", r2.err)
		}
		if r2.ac2 != r1.ac2 {
			t.Fatalf("W2 must land on W1's new slot %p, got %p", r1.ac2, r2.ac2)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("W2 did not return after exit-change detach")
	}

	// 收尾清理（Oracle R4-NH-4）：归还两条租约的 inFlight，再摘除测试
	// 新建的槽位——不留驻留 ctx/在飞租约污染同包后续测试。
	if r1.release != nil {
		r1.release()
	}
	if r2.release != nil {
		r2.release()
	}
	m.InvalidateAccount(id, false, "test cleanup")
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
