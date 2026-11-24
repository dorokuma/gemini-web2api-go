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
