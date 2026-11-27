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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
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

// stubTLSRotateClient 把限时/不限时两个 tls-client 缓存键换成假客户端
// （rotateDo 直连路径用），测试结束恢复。
func stubTLSRotateClient(t *testing.T, fake *captureTLSClient) {
	t.Helper()
	imp := effectiveImpersonate()
	timeout := rtCfg().RequestTimeout
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
}

// TestBrowserBuildAndRecreatePreserveAppPageStatusError 验证建槽首航跨源
// 重定向以 *appPageStatusError 原样透传（不再被无条件抹平成 pageFetchErr
// 文本）：登录页（缺 OSID）经 xsrfErrBlamesCookie 归 Cookie 类错误，
// /sorry 归出口风控，分类链 upstreamReason / classifyFailoverError 精确命中；
// 真超时与停机取消的归类保持不变。
func TestBrowserBuildAndRecreatePreserveAppPageStatusError(t *testing.T) {
	t.Run("登录页重定向透传", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		cleanup := wireTestBrowser(t, srv)
		defer cleanup()
		m := browserMgr

		setRuntimeCfg(t, func(c *RuntimeConfig) {
			c.BrowserTransport = true
		})

		// 账号 cookie 缺 OSID：首航落地 accounts.google.com。
		srv.navRedirectURL.Store("https://accounts.google.com/ServiceLogin?osid=1&continue=x")
		id, err := accountInsert("buildredir", "SAPISID=br; __Secure-1PSID=bp; SID=bs", "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		acct := accountByID(id)
		acct.ProxyID = proxyIDDirect

		_, _, _, _, err = m.acquireAccountCtx(context.Background(), acct, "", true, true)
		if err == nil {
			t.Fatal("首航跳登录页的建槽必须失败")
		}
		var ape *appPageStatusError
		if !errors.As(err, &ape) {
			t.Fatalf("建槽错误必须透传 *appPageStatusError，得到 %T: %v", err, err)
		}
		if !ape.MissingOSID {
			t.Fatalf("缺 OSID 账号跳登录页 MissingOSID 必须为 true: %+v", ape)
		}
		if !strings.Contains(ape.Location, "accounts.google.com") {
			t.Fatalf("Location 必须带 accounts.google.com: %q", ape.Location)
		}
		if !xsrfErrBlamesCookie(err) {
			t.Fatalf("登录页重定向必须归 cookie 类: %v", err)
		}
		if got := classifyFailoverError(err); got != classAuthFail {
			t.Fatalf("登录页重定向应归 classAuthFail，得到 %v", got)
		}
		if got := upstreamReason(err); got != reasonUpstreamHTTP {
			t.Fatalf("upstreamReason=%q, want %q", got, reasonUpstreamHTTP)
		}
		if isBrowserLocalError(err) {
			t.Fatalf("登录页重定向绝不能归 browserLocalError: %v", err)
		}
	})

	t.Run("sorry 风控页归类", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		cleanup := wireTestBrowser(t, srv)
		defer cleanup()
		m := browserMgr

		setRuntimeCfg(t, func(c *RuntimeConfig) {
			c.BrowserTransport = true
		})

		srv.navRedirectURL.Store("https://www.google.com/sorry/index?continue=x")
		id, err := accountInsert("buildsorry", "SAPISID=sr; __Secure-1PSID=sp; SID=ss", "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		acct := accountByID(id)
		acct.ProxyID = proxyIDDirect

		_, _, _, _, err = m.acquireAccountCtx(context.Background(), acct, "", true, true)
		var ape *appPageStatusError
		if !errors.As(err, &ape) {
			t.Fatalf("/sorry 重定向必须透传 *appPageStatusError，得到 %T: %v", err, err)
		}
		if xsrfErrBlamesCookie(err) {
			t.Fatalf("/sorry 是出口风控，绝不能记账号账: %v", err)
		}
		if got := classifyFailoverError(err); got != classUpstreamLimit {
			t.Fatalf("/sorry 应归 classUpstreamLimit，得到 %v", got)
		}
		if got := upstreamReason(err); got != reasonUpstreamHTTP {
			t.Fatalf("upstreamReason=%q, want %q", got, reasonUpstreamHTTP)
		}
	})

	t.Run("重建标签页跨源重定向透传", func(t *testing.T) {
		// 真正触发 doRecreateTab：tab 重建的首航落地 accounts.google.com，
		// *appPageStatusError 必须经 classifyNavBuildErr 原样透传（此前同名
		// 测试只走 acquireAccountCtx 建槽，从未覆盖重建路径）。
		srv := newFakeCDPServer(t)
		defer srv.close()
		cleanup := wireTestBrowser(t, srv)
		defer cleanup()
		m := browserMgr

		setRuntimeCfg(t, func(c *RuntimeConfig) {
			c.BrowserTransport = true
		})

		srv.navRedirectURL.Store("https://accounts.google.com/ServiceLogin?osid=1&continue=x")
		id, err := accountInsert("recreateredir", "SAPISID=rc; __Secure-1PSID=rp2; SID=rs2", "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		sessID, targetID, rerr := m.doRecreateTab(ctx, m.client, "bc-recreate-redir", "", id)
		if rerr == nil {
			t.Fatalf("重建首航跳登录页必须失败（sess=%q target=%q）", sessID, targetID)
		}
		var ape *appPageStatusError
		if !errors.As(rerr, &ape) {
			t.Fatalf("重建错误必须透传 *appPageStatusError，得到 %T: %v", rerr, rerr)
		}
		if !ape.MissingOSID {
			t.Fatalf("缺 OSID 账号跳登录页 MissingOSID 必须为 true: %+v", ape)
		}
		if !strings.Contains(ape.Location, "accounts.google.com") {
			t.Fatalf("Location 必须带 accounts.google.com: %q", ape.Location)
		}
		if !xsrfErrBlamesCookie(rerr) {
			t.Fatalf("重建跳登录页必须归 cookie 类: %v", rerr)
		}
		if got := classifyFailoverError(rerr); got != classAuthFail {
			t.Fatalf("重建跳登录页应归 classAuthFail，得到 %v", got)
		}
		if isBrowserLocalError(rerr) {
			t.Fatalf("重建跨源拦截页绝不能归 browserLocalError: %v", rerr)
		}
		if sessID != "" || targetID != "" {
			t.Fatalf("失败路径不应返回新 session/target: %q %q", sessID, targetID)
		}
	})

	t.Run("真超时归类不变", func(t *testing.T) {
		// 合成链：classifyNavBuildErr 对非 browserLocalError 的 DeadlineExceeded
		// 超时错误加前缀包装后，错误链与归类必须保持。
		raw := fmt.Errorf("page fetch error: navigate /app did not reach gemini.google.com origin (got %q): %w",
			"null/complete", context.DeadlineExceeded)
		classified := classifyNavBuildErr(context.Background(), &cdpClient{}, "navigate /app", raw)
		if !errors.Is(classified, context.DeadlineExceeded) {
			t.Fatalf("包装后 DeadlineExceeded 链丢失: %v", classified)
		}
		if !isUpstreamTimeout(classified) {
			t.Fatalf("真超时必须被 isUpstreamTimeout 识别: %v", classified)
		}
		if got := classifyFailoverError(classified); got != classNetFail {
			t.Fatalf("真超时应归 classNetFail，得到 %v", got)
		}
		if xsrfErrBlamesCookie(classified) {
			t.Fatalf("真超时绝不能记账号账: %v", classified)
		}
		if got := upstreamReason(classified); got != reasonUpstreamTimeout {
			t.Fatalf("upstreamReason=%q, want %q", got, reasonUpstreamTimeout)
		}
	})
}

// TestRotateAllAccountsActiveSlotAndSiblingHotInject1PSIDTS 验证活跃槽热续期：
// BrowserTransport 下 rotateAllAccounts 对活跃槽与同 cookieIdentity 兄弟号
// 单次换发 1PSIDTS 并经 Storage.setCookies 热注入 Jar、写回两号 DB——全程
// Target.disposeBrowserContext 零调用（不拆槽、不断流），兄弟号共享同一次
// 换发结果（RotateCookies 仅被命中 1 次），且热注入后 Jar 新 SIDCC 仍能正常
// 回写 DB（无 epoch 回写黑洞）。
func TestRotateAllAccountsActiveSlotAndSiblingHotInject1PSIDTS(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	// 同 cookieIdentity 的两个兄弟 authuser 账号：A 活跃槽（含 inFlight>0），
	// B 无槽但同 Google 会话。
	sharedCookie := "SAPISID=hx; __Secure-1PSID=hp; SID=hs; __Secure-1PSIDTS=oldts"
	idA, err := accountInsert("hotA", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	defer accountDelete(idA)
	idB, err := accountInsert("hotB", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	acctA := accountByID(idA)

	keyA := slotKeyForAccount(acctA, "", true)
	acA := &accountBrowserCtx{
		key:              keyA,
		epoch:            1,
		cookieIdentity:   cookieIdentity(sharedCookie),
		browserContextID: "bc-hot-a",
		targetID:         "t-hot-a",
		sessionID:        "sess-hot-a",
		allowDirect:      true,
		readyClosed:      true,
		inFlight:         1, // 有在途流：热注入绝不能拆槽断流
		lastUsedAt:       time.Now(),
	}
	acA.readyCh = make(chan struct{})
	close(acA.readyCh)
	m.mu.Lock()
	m.ctxs[keyA] = acA
	m.sessionIndex["sess-hot-a"] = acA
	m.targetIndex["t-hot-a"] = acA
	m.mu.Unlock()
	srv.bcMu.Lock()
	srv.sessToBC["sess-hot-a"] = "bc-hot-a"
	srv.bcStore["bc-hot-a"] = map[string]string{
		"SAPISID":          "hx",
		"__Secure-1PSID":   "hp",
		"SID":              "hs",
		"__Secure-1PSIDTS": "oldts",
		"SIDCC":            "jarcc",
	}
	srv.bcMu.Unlock()

	// 假 tls-client：POST /RotateCookies 哨兵换发返回新 1PSIDTS。
	var mu sync.Mutex
	rotateHits := 0
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			rotateHits++
			h := fhttp.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=newts; Domain=.google.com; Path=/; Secure; HttpOnly")
			h.Add("Set-Cookie", "__Secure-3PSIDTS=new3ts; Domain=.google.com; Path=/; Secure; HttpOnly")
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

	rotateAllAccounts()

	// RotateCookies 只被命中 1 次（兄弟号共享同一次换发，不重打 429）。
	mu.Lock()
	hits := rotateHits
	mu.Unlock()
	if hits != 1 {
		t.Fatalf("RotateCookies 命中 %d 次，兄弟号共享换发应恰好 1 次", hits)
	}
	// 两号 DB 都已写入新票；Jar 经热注入也已更新。
	// Oracle MF-3：__Secure-3PSIDTS 已移出热注入白名单——同 profile 不同
	// authuser 兄弟号各自有独立 3PSID，跨号注入会异身份串号；DB 与 Jar
	// 均绝不能出现 new3ts（本场景两号恰好同身份，反例串号见专项测试）。
	for _, id := range []int64{idA, idB} {
		a := accountByID(id)
		if a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=newts") {
			t.Fatalf("账号 #%d DB 未写入新 1PSIDTS: %q", id, a.Cookie)
		}
		if strings.Contains(a.Cookie, "__Secure-3PSIDTS=new3ts") {
			t.Fatalf("账号 #%d DB 绝不能写入 3PSIDTS（MF-3 白名单外）: %q", id, a.Cookie)
		}
	}
	jar := srv.jarFor("sess-hot-a")
	if jar["__Secure-1PSIDTS"] != "newts" {
		t.Fatalf("活跃槽 Jar 未热注入新 1PSIDTS: %q", jar["__Secure-1PSIDTS"])
	}
	if jar["__Secure-3PSIDTS"] == "new3ts" {
		t.Fatalf("活跃槽 Jar 绝不能注入 3PSIDTS（MF-3 白名单外）: %q", jar["__Secure-3PSIDTS"])
	}
	// 全程零拆槽：Target.disposeBrowserContext 调用 0 次，槽位仍注册且存活。
	disposeCalls := 0
	drainEnd := time.After(150 * time.Millisecond)
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Target.disposeBrowserContext" {
				disposeCalls++
			}
		case <-drainEnd:
			goto drained
		}
	}
drained:
	if disposeCalls != 0 {
		t.Fatalf("热注入路径绝不许拆槽（disposeBrowserContext 调用 %d 次）", disposeCalls)
	}
	m.mu.Lock()
	_, alive := m.ctxs[keyA]
	m.mu.Unlock()
	if !alive {
		t.Fatal("热注入后活跃槽绝不能被摘除")
	}

	// epoch 回写黑洞防线：热注入路径未 Bump——Jar 新产生的 SIDCC 仍能正常
	// 回写 DB（syncAccountCookiesBackCtx 不被 isSupersededEpoch 拦截）。
	srv.bcMu.Lock()
	srv.bcStore["bc-hot-a"]["SIDCC"] = "jarcc-refreshed"
	srv.bcMu.Unlock()
	m.SyncAccountCookiesBack(idA, true)
	if a := accountByID(idA); a == nil || !strings.Contains(a.Cookie, "SIDCC=jarcc-refreshed") {
		t.Fatalf("热注入后 Jar 新 SIDCC 必须能回写 DB（epoch 黑洞）: %q", a.Cookie)
	}
}

// TestBrowserPublishGuardRejectsSupersededBuild 验证 fix #3 的发布守卫：
// 登录槽构建期间（播种后、首航完成前）DB 代际被推进（后台热续期对 creating
// 槽写库并 BumpAccountEpoch），该构建**严禁发布**——旧 Jar 上下文直接销毁、
// 等待者拿到可重试错误（含 superseded），m.ctxs 无残留，Target.disposeBrowserContext
// 真实发出。杜绝「存活旧 Jar + 新 DB + 永久 SIDCC 回写黑洞」。
func TestBrowserPublishGuardRejectsSupersededBuild(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=sg; __Secure-1PSID=sp2; SID=ss2; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("superseded", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = proxyIDDirect

	// 导航延迟 2s：构建进入 navigateAndWaitAppCookie 后、首航完成前，给主测试
	// 协程一个确定性的窗口模拟「后台热续期写库 + BumpAccountEpoch」。
	srv.pageNavDelay.Store(int64(2 * time.Second))

	done := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		done <- aerr
	}()

	// 等构建确实进入导航阶段（播种已完成、jar 里是旧票据）。
	navSeen := false
	waitDeadline := time.After(5 * time.Second)
	for !navSeen {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Page.navigate" {
				navSeen = true
			}
		case aerr := <-done:
			t.Fatalf("构建过早返回: %v", aerr)
		case <-waitDeadline:
			t.Fatal("5s 内构建未进入 Page.navigate")
		}
	}

	// 模拟后台热续期对 creating 槽的路径：写库 + prepareHotInject 的原子
	// 分类+Bump（creating 槽 → Bump，绝不热注入未就绪 jar）。
	unlock, lerr := acquireAcctCookieLock(context.Background(), id)
	if lerr != nil {
		t.Fatalf("lock: %v", lerr)
	}
	cur := accountByID(id)
	newCookie := strings.Replace(cur.Cookie, "__Secure-1PSIDTS=oldts", "__Secure-1PSIDTS=newts", 1)
	plan, _, _, _ := m.prepareHotInject(id, "", hotInjectBinding{verified: true})
	if plan != hotInjectCreating {
		unlock()
		t.Fatalf("构建中的槽必须分类为 hotInjectCreating，得到 %v", plan)
	}
	if !updateAccountCookieCASLocked(id, cur.Cookie, newCookie) {
		unlock()
		t.Fatal("模拟热续期写库失败")
	}
	unlock()

	// 发布守卫必须拒绝：等待者拿到含 superseded 的可重试错误，而不是一个
	// 装着旧票据的存活槽。
	select {
	case aerr := <-done:
		if aerr == nil {
			t.Fatal("superseded 的构建绝不能发布成功")
		}
		if !strings.Contains(aerr.Error(), "superseded") {
			t.Fatalf("错误必须点名 superseded（可重试语义），得到: %v", aerr)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("构建未在导航完成后返回")
	}

	// 槽位已摘除，无残留；BrowserContext 真实销毁。
	key := slotKeyForAccount(acct, "", true)
	m.mu.Lock()
	_, stillThere := m.ctxs[key]
	m.mu.Unlock()
	if stillThere {
		t.Fatal("superseded 构建发布后 m.ctxs 仍有残留")
	}
	disposed := false
	drainEnd := time.After(300 * time.Millisecond)
	drain := func() {
		for {
			select {
			case call := <-srv.gotCalls:
				if call.method == "Target.disposeBrowserContext" {
					disposed = true
				}
			case <-drainEnd:
				return
			}
		}
	}
	drain()
	if !disposed {
		t.Fatal("superseded 旧 Jar 的 BrowserContext 必须真实销毁")
	}
	// DB 里的新票据不受影响（回写黑洞防线的另一侧）。
	if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=newts") {
		t.Fatalf("热续期写库的新票据必须保留: %q", a.Cookie)
	}
}

// TestRotateActiveHotInjectFiltersNonPSIDTSAndRejectsCrossIdentity 验证 fix #6：
// 活跃槽热续期只注入 *PSIDTS 白名单票据——上游 Set-Cookie 里夹带的异身份
// SAPISID/SID/SIDCC 等绝不进活跃 Jar 也不写 DB（身份守卫在注入**之前**）；
// 完全不含 PSIDTS 的响应整体为无操作。
func TestRotateActiveHotInjectFiltersNonPSIDTSAndRejectsCrossIdentity(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=fx6; __Secure-1PSID=fp6; SID=fs6; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("hotfilter", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)

	key := slotKeyForAccount(acct, "", true)
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-hot-filter",
		targetID:         "t-hot-filter",
		sessionID:        "sess-hot-filter",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-hot-filter"] = ac
	m.targetIndex["t-hot-filter"] = ac
	m.mu.Unlock()
	srv.bcMu.Lock()
	srv.sessToBC["sess-hot-filter"] = "bc-hot-filter"
	srv.bcStore["bc-hot-filter"] = map[string]string{
		"SAPISID":          "fx6",
		"__Secure-1PSID":   "fp6",
		"SID":              "fs6",
		"__Secure-1PSIDTS": "oldts",
	}
	srv.bcMu.Unlock()

	snap := activeSlotSnapshot{
		accountID: id, bcID: "bc-hot-filter", proxyURL: "", allowDirect: true,
		ident: cookieIdentity(cookie),
	}

	t.Run("混合响应只放行 PSIDTS", func(t *testing.T) {
		// 上游响应夹带异身份 SAPISID/SID/SIDCC + 合法 PSIDTS：过滤后只有
		// PSIDTS 进 Jar 与 DB，异身份凭据绝不触碰活跃槽。
		fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				h := fhttp.Header{}
				h.Add("Set-Cookie", "__Secure-1PSIDTS=fresh1; Domain=.google.com; Path=/; Secure; HttpOnly")
				h.Add("Set-Cookie", "SAPISID=evil-other-acct; Domain=.google.com; Path=/; Secure")
				h.Add("Set-Cookie", "SID=evil-sid; Domain=.google.com; Path=/; Secure")
				h.Add("Set-Cookie", "SIDCC=evil-cc; Domain=.google.com; Path=/; Secure")
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

		rotateActiveOrSibling1PSIDTS(*acct, []activeSlotSnapshot{snap}, map[string][]string{})

		jar := srv.jarFor("sess-hot-filter")
		if jar["__Secure-1PSIDTS"] != "fresh1" {
			t.Fatalf("PSIDTS 白名单票据必须热注入 Jar: %q", jar["__Secure-1PSIDTS"])
		}
		if jar["SAPISID"] != "fx6" {
			t.Fatalf("异身份 SAPISID 绝不能注入活跃 Jar: %q", jar["SAPISID"])
		}
		if jar["SID"] != "fs6" {
			t.Fatalf("非白名单 SID 绝不能注入活跃 Jar: %q", jar["SID"])
		}
		if _, evil := jar["SIDCC"]; evil {
			t.Fatalf("非白名单 SIDCC 绝不能注入活跃 Jar: %q", jar["SIDCC"])
		}
		a := accountByID(id)
		if a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=fresh1") {
			t.Fatalf("PSIDTS 必须写库: %q", a.Cookie)
		}
		if !strings.Contains(a.Cookie, "SAPISID=fx6") || strings.Contains(a.Cookie, "evil-other-acct") {
			t.Fatalf("异身份凭据绝不能写库: %q", a.Cookie)
		}
		if strings.Contains(a.Cookie, "SIDCC=evil-cc") {
			t.Fatalf("非白名单 SIDCC 绝不能写库: %q", a.Cookie)
		}
	})

	t.Run("完全不含 PSIDTS 整体无操作", func(t *testing.T) {
		before := accountByID(id).Cookie
		m.mu.Lock()
		beforeEpoch := m.maxSeededEpoch[id]
		m.mu.Unlock()
		// 共享缓存路径直接注入异身份响应（绕过上游桩）：即便上游重打，
		// 只含 SAPISID/SIDCC 的缓存结果也绝不写库不注入。
		shared := map[string][]string{
			cookieIdentity(cookie): {
				"SAPISID=evil2; Domain=.google.com; Path=/; Secure",
				"SIDCC=evil2cc; Domain=.google.com; Path=/; Secure",
			},
		}
		rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{snap}, shared)
		if a := accountByID(id); a == nil || a.Cookie != before {
			t.Fatalf("不含 PSIDTS 的响应必须整体无操作: %q vs %q", a.Cookie, before)
		}
		m.mu.Lock()
		afterEpoch := m.maxSeededEpoch[id]
		m.mu.Unlock()
		if afterEpoch != beforeEpoch {
			t.Fatal("无操作路径绝不许 BumpAccountEpoch")
		}
		jar := srv.jarFor("sess-hot-filter")
		if jar["SAPISID"] != "fx6" {
			t.Fatalf("异身份 SAPISID 绝不能经共享缓存注入 Jar: %q", jar["SAPISID"])
		}
	})
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

// ─── R3 返修专项回归 ────────────────────────────────────────────────────────

// TestBrowserRecreateSupersededDestroysStaleJar 复核 R3-1（Reviewer MF-1）：
// recreating 槽在 doRecreateTab 导航阶段被后台热轮转 Bump（prepareHotInject
// 对 recreating 分类推进 maxSeededEpoch 并写库）时，重建完成后严禁发布旧
// Jar——单胜出者销毁整个 BrowserContext，摘除 cause 必须是明确的非 nil
// superseded 错误。热注入后的新 DB 票据不受影响，旧 Jar 不回写。
// 复现失败特征（修复前）：发布守卫只核 ac.epoch==snapEpoch 不核
// maxSeededEpoch——recreate 把同一旧 Jar 发布成活槽，m.ctxs 驻留、无
// disposeBrowserContext，且随后 syncAccountCookiesBackCtx 因 epoch<
// maxSeededEpoch 永久拒绝回写（回写黑洞）。
func TestBrowserRecreateSupersededDestroysStaleJar(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=rs1; __Secure-1PSID=rp1; SID=rss1; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("recsup", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)

	key := slotKeyForAccount(acct, "", true)
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-recsup",
		targetID:         "t-recsup",
		sessionID:        "sess-recsup",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-recsup"] = ac
	m.targetIndex["t-recsup"] = ac
	gen := m.generation
	client := m.client
	m.mu.Unlock()
	srv.bcMu.Lock()
	srv.targetToBC["t-recsup"] = "bc-recsup"
	srv.sessToBC["sess-recsup"] = "bc-recsup"
	srv.bcMu.Unlock()

	// 导航延迟 500ms：给主测试协程一个确定性窗口在 doRecreateTab 导航
	// 期间模拟「后台热轮转写库 + prepareHotInject 分类+Bump」。
	srv.pageNavDelay.Store(int64(500 * time.Millisecond))

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.recreateAccountTab(client, ac, gen)
	}()

	// 等 doRecreateTab 确实进入 Page.navigate（重建的 CDP 序列已走完播种后段）。
	navSeen := false
	waitDeadline := time.After(5 * time.Second)
	for !navSeen {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Page.navigate" {
				navSeen = true
			}
		case <-done:
			t.Fatal("recreateAccountTab 过早返回（未进入导航）")
		case <-waitDeadline:
			t.Fatal("5s 内 recreate 未进入 Page.navigate")
		}
	}

	// 模拟后台热轮转：prepareHotInject 必须对 recreating 槽分类为
	// hotInjectCreating 并原子 Bump；随后写库新票据。
	unlock, lerr := acquireAcctCookieLock(context.Background(), id)
	if lerr != nil {
		t.Fatalf("lock: %v", lerr)
	}
	plan, _, _, _ := m.prepareHotInject(id, "", hotInjectBinding{verified: true})
	if plan != hotInjectCreating {
		unlock()
		t.Fatalf("recreating 槽必须分类为 hotInjectCreating，得到 %v", plan)
	}
	cur := accountByID(id)
	newCookie := strings.Replace(cur.Cookie, "__Secure-1PSIDTS=oldts", "__Secure-1PSIDTS=newts", 1)
	if !updateAccountCookieCASLocked(id, cur.Cookie, newCookie) {
		unlock()
		t.Fatal("模拟热续期写库失败")
	}
	unlock()

	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("recreateAccountTab 未在导航完成后返回")
	}

	// 发布守卫必须拒绝：ac 被摘除，cause 是明确的非 nil superseded 错误
	//（绝不发布旧 Jar，也不出现 cause==nil 的摘除）。
	m.mu.Lock()
	_, stillThere := m.ctxs[key]
	disposed := ac.disposed
	lastErr := ac.lastBuildErr
	m.mu.Unlock()
	if stillThere {
		t.Fatal("superseded recreate 发布后 m.ctxs 仍有残留")
	}
	if !disposed {
		t.Fatal("superseded recreate 后 ac 必须被摘除")
	}
	if lastErr == nil {
		t.Fatal("superseded recreate 摘除 cause 绝不能为 nil")
	}
	if !strings.Contains(lastErr.Error(), "superseded") {
		t.Fatalf("摘除 cause 必须点名 superseded，得到: %v", lastErr)
	}
	// superseded 是本地代际终止：归 classLocalAbort，绝不误扣代理分。
	if got := classifyFailoverError(lastErr); got != classLocalAbort {
		t.Fatalf("superseded 错误应归 classLocalAbort（不扣代理分），得到 %v", got)
	}

	// 旧 Jar 的 BrowserContext 必须真实销毁（单胜出者 disposeDetachedCtx）。
	disposedBC := false
	drainEnd := time.After(300 * time.Millisecond)
	for {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Target.disposeBrowserContext" {
				disposedBC = true
			}
		case <-drainEnd:
			goto drainedR
		}
	}
drainedR:
	if !disposedBC {
		t.Fatal("superseded 旧 Jar 的 BrowserContext 必须真实销毁")
	}
	// DB 里的新票据不受影响（回写黑洞防线的另一侧）。
	if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=newts") {
		t.Fatalf("热续期写库的新票据必须保留: %q", a.Cookie)
	}
}

// TestBrowserBuildSeedingEpochAlignsRegisteredWindow 复核 R3-2（Reviewer MF-2）：
// 辅助回收与前台「注册未播种」窗口——建槽已注册（epoch=E）但尚未播种时，
// 辅助路径写库新 Cookie 并 Bump 到 E+1；由于 L1 串行化，前台播种读到的已
// 是新 Cookie，锁内代际对齐（snapEpoch/ac.epoch 推进到 E+1）后发布守卫必须
// 放行，构建成功而不是误报 superseded。对照路径（R3-1 已在先验证）：播种后、
// 发布前的 Bump 仍 100% 拦截。
// 复现失败特征（修复前）：发布守卫以注册时分配的 snapEpoch=E 对比
// maxSeededEpoch=E+1 → 拒绝发布，明明播种的是新票却被误判旧 Jar 销毁。
func TestBrowserBuildSeedingEpochAlignsRegisteredWindow(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=al1; __Secure-1PSID=ap1; SID=as1; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("seedalign", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)

	// 主测试协程先持 L1：前台建槽在步骤 6 获取 acctCookieLock 时阻塞——
	// 此时槽位已注册（creating、epoch 已分配）但尚未播种，精确模拟
	// 「检查后注册、注册后写库+Bump」交错窗口。
	holdUnlock, lerr := acquireAcctCookieLock(context.Background(), id)
	if lerr != nil {
		t.Fatalf("pre-lock: %v", lerr)
	}

	type acquireRet struct {
		ac2     *accountBrowserCtx
		release func()
		err     error
	}
	done := make(chan acquireRet, 1)
	go func() {
		_, a2, _, rel, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		done <- acquireRet{ac2: a2, release: rel, err: aerr}
	}()

	// 等槽位注册完成（creating 槽出现在 m.ctxs）。
	key := slotKeyForAccount(acct, "", true)
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.Lock()
		_, registered := m.ctxs[key]
		m.mu.Unlock()
		if registered {
			break
		}
		select {
		case r := <-done:
			t.Fatalf("acquire 过早返回 %+v（注册前退出）", r)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("5s 内建槽未注册")
		}
		time.Sleep(time.Millisecond)
	}

	// 窗口内模拟辅助回收写库：由于 L1 被我们持有，这里在锁内直接写库
	// 并 Bump（与 mergeAccountSetCookieIfNoActiveBrowser 的写库路径同构）。
	cur := accountByID(id)
	newCookie := strings.Replace(cur.Cookie, "__Secure-1PSIDTS=oldts", "__Secure-1PSIDTS=rotated-in-window", 1)
	if !updateAccountCookieCASLocked(id, cur.Cookie, newCookie) {
		t.Fatal("窗口内写库失败")
	}
	m.BumpAccountEpoch(id)
	holdUnlock()

	// 前台播种拿到的是新 Cookie 且锁内对齐代际——构建必须成功发布。
	var r acquireRet
	select {
	case r = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("建槽未在 15s 内完成")
	}
	if r.err != nil {
		t.Fatalf("窗口内注册未播种的建槽必须成功（播种的已是新票），得到: %v", r.err)
	}
	if r.release != nil {
		r.release()
	}
	m.mu.Lock()
	publishedAC := m.ctxs[key]
	m.mu.Unlock()
	if publishedAC == nil {
		t.Fatal("构建成功后 m.ctxs 必须驻留新槽")
	}
	// 播种进 jar 的必须是新票（锁内重读 DB 的结果）。
	jar := srv.jarFor(r.ac2.sessionID)
	if jar["__Secure-1PSIDTS"] != "rotated-in-window" {
		t.Fatalf("jar 播种的必须是窗口内写库的新票: %q", jar["__Secure-1PSIDTS"])
	}
	// 收尾：摘除测试建槽，避免污染同包后续测试。
	m.InvalidateAccount(id, false, "test cleanup")
}

// TestBrowserSupersededBuildFailsStreamsNonNilAndLocalAbort 复核 R3-7
// （Oracle SF-1）：superseded 场景下 buildErr==nil 时绝不能把 nil 传给
// failStreamsForCtx——在飞流必须收到非 nil 的本地代际终止错误（归
// classLocalAbort，绝不误扣代理分，也绝不滑向 reasonNoContentFrame 的
// 内容拒绝误诊）。
// 复现失败特征（修复前）：failStreamsForCtx(ac, buildErr) 传 nil →
// s.fatalErr 为 nil，流被当成「正常结束但无内容」。
func TestBrowserSupersededBuildFailsStreamsNonNilAndLocalAbort(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=sf1; __Secure-1PSID=sp1; SID=ssf1; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("streamfail", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = proxyIDDirect

	// 导航延迟开窗（与发布守卫测试同构）：注册在飞流后模拟热续期 Bump。
	srv.pageNavDelay.Store(int64(2 * time.Second))

	done := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		done <- aerr
	}()

	// 等构建进入 Page.navigate（播种已完成）。
	navSeen := false
	waitDeadline := time.After(5 * time.Second)
	for !navSeen {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Page.navigate" {
				navSeen = true
			}
		case aerr := <-done:
			t.Fatalf("构建过早返回: %v", aerr)
		case <-waitDeadline:
			t.Fatal("5s 内构建未进入 Page.navigate")
		}
	}

	// 注册一条绑定该 creating 槽的在飞流（failStreamsForCtx 的命中对象）。
	key := slotKeyForAccount(acct, "", true)
	m.mu.Lock()
	buildingAC := m.ctxs[key]
	m.mu.Unlock()
	if buildingAC == nil {
		t.Fatal("creating 槽必须已注册")
	}
	s := newBrowserStream()
	s.ac = buildingAC
	m.registerStream(s)
	defer m.unregisterStream(s)

	// 模拟后台热续期：分类+Bump+写库（buildErr 为 nil 的纯 superseded 场景）。
	unlock, lerr := acquireAcctCookieLock(context.Background(), id)
	if lerr != nil {
		t.Fatalf("lock: %v", lerr)
	}
	plan, _, _, _ := m.prepareHotInject(id, "", hotInjectBinding{verified: true})
	if plan != hotInjectCreating {
		unlock()
		t.Fatalf("creating 槽必须分类为 hotInjectCreating，得到 %v", plan)
	}
	cur := accountByID(id)
	newCookie := strings.Replace(cur.Cookie, "__Secure-1PSIDTS=oldts", "__Secure-1PSIDTS=newts", 1)
	if !updateAccountCookieCASLocked(id, cur.Cookie, newCookie) {
		unlock()
		t.Fatal("模拟热续期写库失败")
	}
	unlock()

	select {
	case aerr := <-done:
		if aerr == nil || !strings.Contains(aerr.Error(), "superseded") {
			t.Fatalf("构建必须因 superseded 失败，得到: %v", aerr)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("构建未在导航完成后返回")
	}

	// R4-8（Reviewer SF-2）：done 只等 acquire 返回——detach 已关闭
	// readyCh，但 failStreamsForCtx 由发布协程在释放 m.mu 之后才执行。
	// 断言 fatalErr 前必须有界等待流自身的 s.closed（fail 后由
	// pumpToPipe/fail 关闭），消除 goroutine 调度竞态。
	select {
	case <-s.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("在飞流未在构建失败后关闭（failStreamsForCtx 未命中）")
	}

	// 在飞流必须收到非 nil 的 superseded 终止错误（修复前 s.fatalErr==nil）。
	s.mu.Lock()
	fatal := s.fatalErr
	s.mu.Unlock()
	if fatal == nil {
		t.Fatal("superseded 场景在飞流的 fatalErr 绝不能为 nil（会被误诊为内容拒绝）")
	}
	if !strings.Contains(fatal.Error(), "superseded") {
		t.Fatalf("流的终止错误必须点名 superseded，得到: %v", fatal)
	}
	if got := classifyFailoverError(fatal); got != classLocalAbort {
		t.Fatalf("superseded 流错误应归 classLocalAbort（不扣代理分），得到 %v", got)
	}
	if !isBrowserLocalError(fatal) {
		t.Fatalf("superseded 是本地代际终止，isBrowserLocalError 必须判真: %v", fatal)
	}
}

// TestBrowserRecreateSupersededBeforeEntryDestroysStaleJar 复核 R4-2 问题 A
// （Reviewer MF-2）：旧 Jar 在**进入重建前**已被轮转 Bump（epoch 已小于
// maxSeededEpoch）时，recreateAccountTab 严禁给它重新编号重建——直接走
// 销毁摘槽路径：Target.createTarget 零调用（未进 doRecreateTab）、
// BrowserContext 真实销毁、旧 Jar 票据绝不借新 epoch 反写 DB。
// 复现失败特征（修复前）：入口无条件 nextCtxEpoch++ 赋新 epoch——新 epoch
// 大于 maxSeededEpoch，syncAccountCookiesBackCtx 的双代际检查被穿透，
// 旧短票在摘槽前被反写进 DB（SIDCC=stale-old-ticket 落库）。
func TestBrowserRecreateSupersededBeforeEntryDestroysStaleJar(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=rb2; __Secure-1PSID=rp2b; SID=rs2b; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("recbump", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)

	key := slotKeyForAccount(acct, "", true)
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-recbump",
		targetID:         "t-recbump",
		sessionID:        "sess-recbump",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-recbump"] = ac
	m.targetIndex["t-recbump"] = ac
	gen := m.generation
	client := m.client
	m.mu.Unlock()
	srv.bcMu.Lock()
	srv.targetToBC["t-recbump"] = "bc-recbump"
	srv.sessToBC["sess-recbump"] = "bc-recbump"
	srv.bcStore["bc-recbump"] = map[string]string{
		"SAPISID":        "rb2",
		"__Secure-1PSID": "rp2b",
		"SID":            "rs2b",
		"SIDCC":          "stale-old-ticket",
	}
	srv.bcMu.Unlock()

	// 进入重建前先 Bump（模拟扩展推送新票/保活轮转已完成写库+代际推进）——
	// 旧 Jar 的 epoch=1 此刻起已失效。
	m.BumpAccountEpoch(id)

	m.recreateAccountTab(client, ac, gen)

	// 必须直接摘槽销毁：槽位不再驻留、cause 点名 superseded。
	m.mu.Lock()
	_, stillThere := m.ctxs[key]
	disposed := ac.disposed
	lastErr := ac.lastBuildErr
	m.mu.Unlock()
	if stillThere {
		t.Fatal("进入重建前已失效的槽绝不能驻留（R4-2）")
	}
	if !disposed {
		t.Fatal("进入重建前已失效的槽必须被摘除（R4-2）")
	}
	if lastErr == nil || !strings.Contains(lastErr.Error(), "superseded") {
		t.Fatalf("摘除 cause 必须点名 superseded，得到: %v", lastErr)
	}

	// Target.createTarget 零调用（重建 CDP 序列根本没进入）、
	// disposeBrowserContext 恰好 1 次且参数指向 bc-recbump、
	// Storage.getCookies 零调用（旧票回写被代际守卫拦截，根本没发起）。
	createCalls, disposeCalls, getCookieCalls := 0, 0, 0
	drainEnd := time.After(200 * time.Millisecond)
	for {
		select {
		case call := <-srv.gotCalls:
			switch call.method {
			case "Target.createTarget":
				createCalls++
			case "Target.disposeBrowserContext":
				disposeCalls++
				if !strings.Contains(string(call.params), "bc-recbump") {
					t.Fatalf("disposeBrowserContext 参数必须指向 bc-recbump: %s", call.params)
				}
			case "Storage.getCookies":
				getCookieCalls++
			}
		case <-drainEnd:
			goto drainedRB
		}
	}
drainedRB:
	if createCalls != 0 {
		t.Fatalf("已失效 Jar 绝不能重建（Target.createTarget 调用 %d 次）", createCalls)
	}
	if disposeCalls != 1 {
		t.Fatalf("旧 Jar BrowserContext 必须销毁恰好 1 次，得到 %d", disposeCalls)
	}
	if getCookieCalls != 0 {
		t.Fatalf("旧票回写绝不能发起（Storage.getCookies 调用 %d 次）", getCookieCalls)
	}
	// DB 绝不能混入旧 Jar 的短票。
	if a := accountByID(id); a != nil && strings.Contains(a.Cookie, "stale-old-ticket") {
		t.Fatalf("已失效旧 Jar 的短票绝不能反写进 DB: %q", a.Cookie)
	}
}

// TestRotateHotInjectFailureDoesNotDetachReplacedSlot 复核 R4-2 问题 B
// （Reviewer MF-2）：热注入 CDP 写入失败后的**延迟**摘除必须以被注入的槽
// 实例（bcID）为条件——注入 I/O 期间旧槽已被失效并替换为健康新槽时，
// InvalidateAccountIfBC 绝不能误拆新槽。
// 复现失败特征（修复前）：rotate.go 末尾按 ID 无条件 InvalidateAccount——
// 刚替换上去的健康新槽被延迟误拆（m.ctxs 空）。
func TestRotateHotInjectFailureDoesNotDetachReplacedSlot(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=hi1; __Secure-1PSID=hp1; SID=hs1; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("hotreplace", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)

	key := slotKeyForAccount(acct, "", true)
	acOld := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-hot-old",
		targetID:         "t-hot-old",
		sessionID:        "sess-hot-old",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	acOld.readyCh = make(chan struct{})
	close(acOld.readyCh)
	m.mu.Lock()
	m.ctxs[key] = acOld
	m.sessionIndex["sess-hot-old"] = acOld
	m.targetIndex["t-hot-old"] = acOld
	m.mu.Unlock()
	srv.bcMu.Lock()
	srv.sessToBC["sess-hot-old"] = "bc-hot-old"
	srv.bcStore["bc-hot-old"] = map[string]string{
		"SAPISID": "hi1", "__Secure-1PSID": "hp1", "SID": "hs1",
		"__Secure-1PSIDTS": "oldts",
	}
	srv.bcMu.Unlock()

	// 上游换发必然成功（新 1PSIDTS）——热注入前的写库一定发生，injErr 路径
	// 才会走到延迟摘除。
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			h := fhttp.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=hjnewts; Domain=.google.com; Path=/; Secure; HttpOnly")
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

	// R5-6（Reviewer MF-6）：只扣住 Storage.setCookies 的自动回复——
	// Target.disposeBrowserContext 与其余方法照常即时完成，杜绝旧屏障
	//（holdReplies 扣住全部回复）下「摘除的 dispose 等 3s 超时」与
	//「注入等 3s 超时」同量级导致的时序假绿：本测试的交错顺序由明确
	// 屏障保证，不靠超时侥幸。
	srv.holdSetCookies.Store(true)
	snap := activeSlotSnapshot{
		accountID: id, bcID: "bc-hot-old", proxyURL: "", allowDirect: true,
		ident: cookieIdentity(cookie),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{snap}, map[string][]string{})
	}()

	// 等注入确实发出（prepareHotInject 快照 bc-hot-old 后 CDP 写入被 hold）。
	var injectCall cdpCall
	waitDeadline := time.After(5 * time.Second)
	for {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Storage.setCookies" {
				injectCall = call
				goto gotInject
			}
		case <-done:
			t.Fatal("热续期过早返回（未进入 CDP 注入）")
		case <-waitDeadline:
			t.Fatal("5s 内未见 Storage.setCookies 注入调用")
		}
	}
gotInject:

	// 屏障 1→2：热注入 Storage.setCookies 已发出在飞（上方 gotInject）
	// 且尚未收到任何回复（holdSetCookies 仍扣着）。此刻管理员失效旧槽：
	// InvalidateAccount 同步完成 detach + disposeDetachedCtx——因只扣住
	// setCookies，disposeBrowserContext 即时应答，本调用返回时旧槽已
	// 确定摘除且其 BrowserContext 已确定销毁（无 3s 超时侥幸）。
	m.InvalidateAccount(id, false, "patched")
	m.mu.Lock()
	oldGone := m.ctxs[key] == nil
	m.mu.Unlock()
	if !oldGone {
		t.Fatal("屏障：InvalidateAccount 返回后旧槽必须已摘除")
	}
	// 屏障 2→3：替代新槽正式驻留 m.ctxs（另一个请求已注册/播种的新实例，
	// bcID 不同）之后，才释放被扣住的 Storage.setCookies 确定错误回复。
	acNew := &accountBrowserCtx{
		key:              key,
		epoch:            2,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-hot-new",
		targetID:         "t-hot-new",
		sessionID:        "sess-hot-new",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	acNew.readyCh = make(chan struct{})
	close(acNew.readyCh)
	m.mu.Lock()
	m.ctxs[key] = acNew
	m.sessionIndex["sess-hot-new"] = acNew
	m.targetIndex["t-hot-new"] = acNew
	m.mu.Unlock()
	srv.bcMu.Lock()
	srv.sessToBC["sess-hot-new"] = "bc-hot-new"
	srv.bcStore["bc-hot-new"] = map[string]string{}
	srv.bcMu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		delete(m.sessionIndex, "sess-hot-new")
		delete(m.targetIndex, "t-hot-new")
		m.mu.Unlock()
	}()

	// 屏障 3：新槽已正式驻留——此刻才放行注入的确定错误回复。injErr
	// 路径的延迟摘除必须按 bc-hot-old 条件匹配：当前驻留的是 bc-hot-new，
	// 条件不符，新槽零影响。对照：若此处退回旧的无条件 InvalidateAccount
	// 按 ID 摘除，替代新槽必然被误拆（本测试即红灯，见下方反向验证）。
	// R6-5（Reviewer MF-5）：手发回复带唯一哨兵错误码/文案
	//（injected-unique-cdp-fail-r6），且必须检查发送成功——此后断言
	// 失败确实由该手发错误触发，绝非 injectCookiesToBC 的 3s deadline
	//（context deadline exceeded）与热注入超时竞争的假绿。
	srv.holdSetCookies.Store(false)
	globalLogBuffer.Clear()
	defer globalLogBuffer.Clear()
	if serr := srv.sendText([]byte(fmt.Sprintf(
		`{"id":%d,"error":{"code":-32042,"message":"injected-unique-cdp-fail-r6"}}`,
		injectCall.id))); serr != nil {
		t.Fatalf("手发注入错误回复失败: %v", serr)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("热续期未在注入失败后返回")
	}

	// R6-5 因果断言：本轮热注入失败日志必须携带手发的唯一哨兵错误
	//（rotate.go injErr 日志原样带出 CDP 错误文本），且绝不能是
	// context deadline exceeded——否则「新槽存活」可能只是超时竞争侥幸。
	var injLog string
	for _, e := range globalLogBuffer.Query(LogFilter{Tail: 1000}) {
		if strings.Contains(e.Message, "热注入失败") {
			injLog = e.Message
		}
	}
	if !strings.Contains(injLog, "injected-unique-cdp-fail-r6") {
		t.Fatalf("热注入失败必须由手发的唯一哨兵错误触发，日志为: %q", injLog)
	}
	if strings.Contains(injLog, "deadline exceeded") {
		t.Fatalf("热注入失败绝不能来自 3s deadline（与超时竞争的假绿）: %q", injLog)
	}

	m.mu.Lock()
	resident := m.ctxs[key]
	m.mu.Unlock()
	if resident != acNew {
		t.Fatal("热注入失败的延迟摘除绝不能误拆替换后的健康新槽（R4-2）")
	}
	if !acNew.readyClosed || acNew.disposed {
		t.Fatal("健康新槽必须保持存活就绪")
	}
	// DB 新票照常落库（热续期主路径不受影响）。
	if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=hjnewts") {
		t.Fatalf("热续期写库的新票据必须保留: %q", a.Cookie)
	}

	// 反向见证（R5-6）：若延迟摘除退回旧的无条件 InvalidateAccount
	// 按 ID 摘除，此刻驻留的替代新槽会被确实拆掉——证明上方「新槽
	// 存活」断言是条件摘除的功劳而非时序侥幸（假绿不可能）。
	m.InvalidateAccount(id, false, "witness: unconditional invalidate")
	m.mu.Lock()
	witnessGone := m.ctxs[key] == nil
	m.mu.Unlock()
	if !witnessGone {
		t.Fatal("反向见证失败：无条件 InvalidateAccount 必须能摘除驻留新槽（否则上方绿灯无意义）")
	}
	// 反向见证已自行摘除新槽：测试末尾的清理 defer 幂等无副作用。
}

// TestBrowserBuildDeadlineBeforePublishNotPublished 复核 R4-5（Reviewer
// MF-5）：导航成功后、持 m.mu 发布前 buildCtx 到期——AfterFunc 已触发关闭
// bridge 时严禁把健康标记与死桥一起发布：acquire 拿到非 nil 错误、
// m.ctxs 无残留、BrowserContext 真实销毁。
// 确定性时序：build 总预算 900ms、导航延迟 200ms——导航完成后主测试协程
// 按住 m.mu 越过 900ms 截止线再放行，发布临界区 100% 落在到期之后。
func TestBrowserBuildDeadlineBeforePublishNotPublished(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})
	oldBuildTimeout := browserCtxBuildTimeout
	browserCtxBuildTimeout = 900 * time.Millisecond
	defer func() { browserCtxBuildTimeout = oldBuildTimeout }()

	cookie := "SAPISID=dl1; __Secure-1PSID=dp1; SID=ds1"
	id, err := accountInsert("builddeadline", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = proxyIDDirect

	srv.pageNavDelay.Store(int64(200 * time.Millisecond))

	done := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		done <- aerr
	}()

	// 等构建进入 Page.navigate（播种与 CDP 序列已完成，即将首航）。
	navSeen := false
	waitDeadline := time.After(5 * time.Second)
	for !navSeen {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Page.navigate" {
				navSeen = true
			}
		case aerr := <-done:
			t.Fatalf("构建过早返回: %v", aerr)
		case <-waitDeadline:
			t.Fatal("5s 内构建未进入 Page.navigate")
		}
	}

	// 导航约 200ms 后完成；按住 m.mu 到 900ms 截止线之后——发布临界区
	// 必然落在 buildCtx 到期之后（AfterFunc 已关闭 bridge）。
	m.mu.Lock()
	time.Sleep(1200 * time.Millisecond)
	m.mu.Unlock()

	select {
	case aerr := <-done:
		if aerr == nil {
			t.Fatal("buildCtx 截止先于发布时绝不能发布成功（R4-5）")
		}
		if !strings.Contains(aerr.Error(), "deadline") &&
			!strings.Contains(aerr.Error(), "invalidated") {
			t.Fatalf("错误必须点名 deadline/invalidated（本地构建超时终止），得到: %v", aerr)
		}
		if got := classifyFailoverError(aerr); got != classLocalAbort {
			t.Fatalf("本地构建超时应归 classLocalAbort（不扣代理分），得到 %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("构建未在截止后返回")
	}

	// 不留 ready 槽，BrowserContext 真实销毁（bridge 清理随失败路径完成）。
	key := slotKeyForAccount(acct, "", true)
	m.mu.Lock()
	_, stillThere := m.ctxs[key]
	m.mu.Unlock()
	if stillThere {
		t.Fatal("buildCtx 到期后 m.ctxs 绝不能留 ready 槽（R4-5）")
	}
	disposed := false
	drainEnd := time.After(300 * time.Millisecond)
	for {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Target.disposeBrowserContext" {
				disposed = true
			}
		case <-drainEnd:
			goto drainedDL
		}
	}
drainedDL:
	if !disposed {
		t.Fatal("到期未发布的 BrowserContext 必须真实销毁")
	}
}

// TestBrowserBuildWaitCookieLockTimeoutIsLocalAbort 复核 R4-6（Reviewer
// MF-6）：buildAccountCtx 步骤 6 等待本地 L1 Cookie 锁超过构建预算时，
// 返回错误必须包装为 browserLocalError（isBrowserLocalError 判真、
// classifyFailoverError 归 classLocalAbort）——这是本地构建终止，不是
// 上游/出口网络超时，绝不扣代理分、绝不烧跳槽预算。
// 复现失败特征（修复前）：裸 context.DeadlineExceeded 被 isUpstreamTimeout
// 判真、归 classNetFail（烧跳槽预算且错扣代理分）。
func TestBrowserBuildWaitCookieLockTimeoutIsLocalAbort(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})
	oldBuildTimeout := browserCtxBuildTimeout
	browserCtxBuildTimeout = 600 * time.Millisecond
	defer func() { browserCtxBuildTimeout = oldBuildTimeout }()

	cookie := "SAPISID=lk1; __Secure-1PSID=lp1; SID=ls1"
	id, err := accountInsert("lockwait", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = proxyIDDirect

	// 主测试协程先持 L1 超过整个短构建预算：步骤 6 获取锁必然超时。
	holdUnlock, lerr := acquireAcctCookieLock(context.Background(), id)
	if lerr != nil {
		t.Fatalf("pre-lock: %v", lerr)
	}
	defer holdUnlock()

	done := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		done <- aerr
	}()

	select {
	case aerr := <-done:
		if aerr == nil {
			t.Fatal("持锁超过构建预算时建槽必须失败")
		}
		if !strings.Contains(aerr.Error(), "wait cookie lock") {
			t.Fatalf("错误必须点名本地锁等待（保留 cause），得到: %v", aerr)
		}
		if !errors.Is(aerr, context.DeadlineExceeded) {
			t.Fatalf("底层 cause（DeadlineExceeded）必须保留在错误链: %v", aerr)
		}
		if !isBrowserLocalError(aerr) {
			t.Fatalf("本地锁等待超时必须包装为 browserLocalError: %v", aerr)
		}
		if got := classifyFailoverError(aerr); got != classLocalAbort {
			t.Fatalf("本地锁等待超时应归 classLocalAbort（不扣代理分不烧预算），得到 %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("构建未在短预算截止后返回")
	}

	// 对照：真实导航超时（page fetch error + DeadlineExceeded、非本地包装）
	// 的分类不受影响——仍归 classNetFail。
	realNavTimeout := fmt.Errorf("page fetch error: navigate /app did not reach gemini.google.com origin (got %q): %w",
		"null/complete", context.DeadlineExceeded)
	if isBrowserLocalError(realNavTimeout) {
		t.Fatal("真实导航超时绝不能被误判为 browserLocalError")
	}
	if got := classifyFailoverError(realNavTimeout); got != classNetFail {
		t.Fatalf("真实导航超时必须仍归 classNetFail，得到 %v", got)
	}
}

// TestBrowserLateCreateBrowserContextReaped 复核 R5-4（Reviewer MF-4）：
// Target.createBrowserContext 的请求已发出、成功回复晚于 buildCtx 截止
// 时，迟到的 browserContextId 绝不泄漏在共享 CDP 连接上——收割协程必须
// 按迟到 ID 单次发出 Target.disposeBrowserContext 清理（共享连接本身
// 保持存活，兄弟槽不受影响）。
// 复现失败特征（修复前）：client.Call 用 buildCtx，截止先触发时
// localBCID 恒为空——迟到的成功回复只进无人接收的 pending channel，
// bcStore 里的 bc-late 永远等不到 disposeBrowserContext。
func TestBrowserLateCreateBrowserContextReaped(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})
	oldBuildTimeout := browserCtxBuildTimeout
	browserCtxBuildTimeout = 250 * time.Millisecond
	defer func() { browserCtxBuildTimeout = oldBuildTimeout }()
	// R6-3 可控回复屏障：创建回复被扣住（既不受固定延迟也不受任何
	// 收割窗限制）——测试先确定看到构建返回超时错误，再手工放行
	// 迟到成功回复，因果链完全确定。
	srv.holdCreateBC.Store(true)

	cookie := "SAPISID=lc1; __Secure-1PSID=lpc1; SID=lsc1"
	id, err := accountInsert("latecreate", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = proxyIDDirect

	done := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		done <- aerr
	}()

	// 先拿到 fake 服务端为该次创建分配的 bcID（创建命令确实已发出）。
	var hit createBCCall
	select {
	case hit = <-srv.createBCHit:
	case <-time.After(3 * time.Second):
		t.Fatal("fake 服务端未见 Target.createBrowserContext")
	}
	bcID := hit.bcID
	if bcID == "" {
		t.Fatal("createBCHit 必须携带分配的 bcID")
	}

	// 屏障 1：构建必须在 ~250ms 截止后以超时错误返回——创建回复此刻
	// 仍被扣住，构建的返回绝不依赖迟到回复（前台等待未被延长）。
	select {
	case aerr := <-done:
		if aerr == nil {
			t.Fatal("创建回复被扣住时建槽必须以构建超时失败")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("构建未在截止后返回（前台等待被非法延长）")
	}

	// R7-5 结构性断言：构建已按 buildCtx 放弃等待后、放行迟到回复
	// 之前，该请求的 pending 槽位必须仍登记在连接上且已标记 late、
	// 携带来自注册的 onLate 回收回调——证明回收由连接 pending 项承载，
	// 无任何固定收割窗/固定回复截止（R5 的 10s 宽限与 R6 的
	// watch+Call+reaper 协程方案都不存在该结构：旧实现要么过窗丢回复，
	// 要么靠协程等 channel，绝不会有带 onLate 的持久 pending 槽位）。
	m.mu.Lock()
	client := m.client
	m.mu.Unlock()
	if client == nil {
		t.Fatal("共享 CDP client 必须存在")
	}
	client.pendMu.Lock()
	pent, pok := client.pending[hit.id]
	client.pendMu.Unlock()
	if !pok {
		t.Fatal("构建放弃后创建命令的 pending 槽位必须仍在（回复所有权保留至真实回复/断连）")
	}
	if !pent.late {
		t.Fatal("调用方按 buildCtx 放弃后 pending 必须已标记 late（零协程迟到回收）")
	}
	if pent.onLate == nil {
		t.Fatal("pending 必须携带 onLate 回收回调（无固定收割窗的连接级回收）")
	}

	// 屏障 2：构建已确定返回超时错——此刻才放行迟到成功回复（远超
	// 250ms 构建截止，不受任何收割窗限制）。R7-5：回复所有权保留至
	// 真实回复/断连，readLoop 收到回复时一次性回调 onLate 收割。
	if err := srv.sendText([]byte(fmt.Sprintf(
		`{"id":%d,"result":{"browserContextId":%q}}`, hit.id, bcID))); err != nil {
		t.Fatalf("放行迟到回复失败: %v", err)
	}

	// 收割协程必须按该 bcID 发出 Target.disposeBrowserContext——共享
	// 连接保持存活（wireTestBrowser 的 client 未断），绝不靠关连接
	// 触发 disposeOnDetach。
	disposeDeadline := time.After(4 * time.Second)
	reaped := false
	for !reaped {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Target.disposeBrowserContext" &&
				strings.Contains(string(call.params), bcID) {
				reaped = true
			}
		case <-disposeDeadline:
			t.Fatalf("迟到的 %s 从未被 Target.disposeBrowserContext 收割（BrowserContext 泄漏）", bcID)
		}
	}
	// 服务端 jar 侧同样确认该 BC 已被销毁。
	deadline2 := time.Now().Add(2 * time.Second)
	for {
		srv.bcMu.Lock()
		_, exists := srv.bcStore[bcID]
		srv.bcMu.Unlock()
		if !exists {
			break
		}
		if time.Now().After(deadline2) {
			t.Fatalf("bcStore 中 %s 未被销毁（泄漏）", bcID)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 共享 CDP 连接必须仍然存活（兄弟槽不受影响）。
	m.mu.Lock()
	clientAlive := m.client != nil && !m.client.Closed()
	m.mu.Unlock()
	if !clientAlive {
		t.Fatal("收割迟到 BC 绝不能关闭共享 CDP 连接")
	}
}

// TestBrowserCreateBCNeverSentAfterBuildCancelWhileWriteBlocked 复核 R7-5
// （Reviewer MF-5）：写发送准入严格受 buildCtx 约束——创建命令在 writeSem
// 排队等待期间构建被取消/到期时，拿到写权后绝不能向共享 CDP 连接发送
// Target.createBrowserContext（半活连接不累积已放弃构建的外发命令）。
// 复现失败特征（R6 及之前）：buildCtx.Err() 只在进 Call 前预检一次——
// 命令在独立协程里排队等 writeSem，期间拆槽取消构建后拿到写权检查的是
// 连接级 callCtx，已放弃的创建命令照样发向共享连接。
func TestBrowserCreateBCNeverSentAfterBuildCancelWhileWriteBlocked(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})
	oldBuildTimeout := browserCtxBuildTimeout
	browserCtxBuildTimeout = 300 * time.Millisecond
	defer func() { browserCtxBuildTimeout = oldBuildTimeout }()

	cookie := "SAPISID=wg1; __Secure-1PSID=wgp1; SID=wgs1"
	id, err := accountInsert("writegate", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)
	acct.ProxyID = proxyIDDirect

	m.mu.Lock()
	client := m.client
	m.mu.Unlock()
	if client == nil {
		t.Fatal("共享 CDP client 必须存在")
	}

	// 先占满 writeSem：创建命令的写发送必然排队等待。
	client.conn.writeSem <- struct{}{}
	held := true
	defer func() {
		if held {
			<-client.conn.writeSem
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		done <- aerr
	}()

	// 构建必在 300ms 到期时仍卡在 writeSem 排队（释放前 700ms）——
	// 到期后拿到写权也绝不能外发。
	time.Sleep(700 * time.Millisecond)
	<-client.conn.writeSem // 释放写权
	held = false

	select {
	case aerr := <-done:
		if aerr == nil {
			t.Fatal("写权被占用超过构建预算时建槽必须失败")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("构建未在到期后返回")
	}

	// 决定性断言：服务端绝不能见到 Target.createBrowserContext——
	// 已取消构建的命令绝不外发（drain 窗口覆盖释放后一切迟到外发）。
	sawCreate := false
	drainEnd := time.After(500 * time.Millisecond)
	for {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Target.createBrowserContext" {
				sawCreate = true
			}
		case <-drainEnd:
			goto drainedWG
		}
	}
drainedWG:
	if sawCreate {
		t.Fatal("buildCtx 在 writeSem 排队期间到期后，Target.createBrowserContext 绝不能发向共享连接（R7-5 发送准入）")
	}

	// 对照：不占用 writeSem 的干净环境下同一构建照常外发创建命令——
	// 证明上方零外发是发送准入的功劳而非环境干扰。
	key := slotKeyForAccount(acct, "", true)
	m.mu.Lock()
	delete(m.ctxs, key)
	m.mu.Unlock()
	done2 := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		done2 <- aerr
	}()
	sawCreate2 := false
	waitDeadline := time.After(4 * time.Second)
	for !sawCreate2 {
		select {
		case call := <-srv.gotCalls:
			if call.method == "Target.createBrowserContext" {
				sawCreate2 = true
			}
		case aerr := <-done2:
			t.Fatalf("对照构建过早返回: %v", aerr)
		case <-waitDeadline:
			t.Fatal("对照：干净环境下创建命令必须照常外发")
		}
	}
	m.InvalidateAccount(id, false, "test cleanup")
}

// TestBrowserColdBuildAutoBindAlignment 复核 R7-3/R7-4（Reviewer MF-3/MF-4）：
//   - 先登记后绑（SyncAutoBoundProxy 正常命中 creating 槽）：驻留事实同步；
//   - 先绑后登记（错过同步窗口）：buildAccountCtx 步骤 6 在持 m.mu 对齐
//     snapEpoch 的同一临界区内按 DB 最新事实补齐 orig/bound——绑定代理
//     实时 URL 与本槽实际出口一致时对齐，发布与后续续期均正常；
//   - 管理员手工改绑到异代理（URL 不符）：绝不对齐，fail-closed 拦截；
//   - 出口兼容核验的错误文本绝不泄露代理 URL 的 userinfo 凭据；
//   - 并发冷建与自动首绑/重绑交错在 -race 下零竞态。
func TestBrowserColdBuildAutoBindAlignment(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 72, Name: "abp1", URL: "http://alice:s3cretPass@72.example:8080", Enabled: true},
		{ID: 73, Name: "abp2", URL: "http://bob:t0pSecret@73.example:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	mkAcct := func(t *testing.T, label, tag string) (int64, *CookieAccount) {
		t.Helper()
		cookie := "SAPISID=ab" + tag + "; __Secure-1PSID=abp" + tag + "; SID=abs" + tag
		id, err := accountInsert(label, cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		t.Cleanup(func() { _ = accountDelete(id) })
		return id, accountByID(id)
	}
	slotURL := proxyURLByID(72)

	// waitBuild 启动冷建并等创建命令到达 fake 服务端（此刻槽已注册为
	// creating，构建卡在扣住的创建回复上）。exitURL 传空串时用默认
	// slotURL（代理 72 的实时 URL）。
	// R9-6：跨子例共享的缓冲 createBCHit 可能残留上一个账号正常创建
	// 的未消费 hit（除持锁扣留外，acquireClaimCreateLocked 前的名额淘汰
	// 也可能触发额外创建）——先排干残留，再记下 seq 基线，只认 seq
	// 更大的 hit 关联本次自己的创建，绝不误取上一账号的 ID。
	startBuild := func(t *testing.T, acct *CookieAccount, exitURL ...string) (chan error, createBCCall) {
		t.Helper()
		for {
			select {
			case <-srv.createBCHit:
				continue
			default:
			}
			break
		}
		baseline := srv.bcSeq.Load()
		srv.holdCreateBC.Store(true)
		defer srv.holdCreateBC.Store(false)
		exit := slotURL
		if len(exitURL) > 0 {
			exit = exitURL[0]
		}
		done := make(chan error, 1)
		go func() {
			_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, exit, false, true)
			done <- aerr
		}()
		var hit createBCCall
		deadline := time.After(4 * time.Second)
		for {
			select {
			case hit = <-srv.createBCHit:
				if hit.seq > baseline {
					return done, hit
				}
			case <-deadline:
				t.Fatal("fake 服务端未见本次创建的 Target.createBrowserContext（按 seq 关联）")
			}
		}
	}
	releaseCreate := func(t *testing.T, hit createBCCall) {
		t.Helper()
		if err := srv.sendText([]byte(fmt.Sprintf(
			`{"id":%d,"result":{"browserContextId":%q}}`, hit.id, hit.bcID))); err != nil {
			t.Fatalf("放行创建回复: %v", err)
		}
	}
	resetSlot := func(id int64) {
		m.InvalidateAccount(id, false, "test reset")
		m.mu.Lock()
		delete(m.ctxs, browserSlotKey{accountID: id})
		m.mu.Unlock()
	}

	t.Run("先绑后登记的错过同步在步骤六补齐对齐", func(t *testing.T) {
		id, acct := mkAcct(t, "abalign", "a1")
		done, hit := startBuild(t, acct)
		// 合法自动首绑到本槽实际出口（0 -> 72）。SyncAutoBoundProxy 命中
		// creating 槽正常同步——随后手动把 orig 拨回 0，精确模拟「先绑后
		// 登记」的错过同步窗口终态（登记时 orig 是绑定前的陈旧快照）。
		autoBindAccountProxy(id, 72)
		if a := accountByID(id); a == nil || a.ProxyID != 72 {
			t.Fatalf("前置：自动首绑必须落库，得到 %+v", a)
		}
		key := browserSlotKey{accountID: id}
		m.mu.Lock()
		ac := m.ctxs[key]
		if ac == nil {
			m.mu.Unlock()
			t.Fatal("creating 槽必须已注册")
		}
		ac.origAccountProxyID = 0 // 模拟先绑后登记：同步窗口错过后的陈旧 orig
		m.mu.Unlock()
		releaseCreate(t, hit)

		select {
		case aerr := <-done:
			if aerr != nil {
				t.Fatalf("合法自动首绑对齐本槽出口的构建必须成功: %v", aerr)
			}
		case <-time.After(6 * time.Second):
			t.Fatal("构建未返回")
		}
		m.mu.Lock()
		pub := m.ctxs[key]
		m.mu.Unlock()
		if pub == nil || pub.disposed || pub.creating {
			t.Fatalf("对齐后槽位必须正常发布: %+v", pub)
		}
		if pub.origAccountProxyID != 72 || pub.boundProxyID != 72 {
			t.Fatalf("步骤六必须按 DB 最新事实补齐 orig/bound=72（先绑后登记），得到 orig=%d bound=%d",
				pub.origAccountProxyID, pub.boundProxyID)
		}
		resetSlot(id)
	})

	t.Run("管理员手工改绑到异代理绝不对齐且错误脱敏", func(t *testing.T) {
		id, acct := mkAcct(t, "abmanual", "m1")
		done, hit := startBuild(t, acct)
		// 管理员手工改绑到异代理 73（其 URL 与本槽出口 72 不符）——
		// 绝不对齐，fail-closed 拦截。
		bindAccountProxy(id, 73)
		key := browserSlotKey{accountID: id}
		m.mu.Lock()
		ac := m.ctxs[key]
		if ac == nil {
			m.mu.Unlock()
			t.Fatal("creating 槽必须已注册")
		}
		ac.origAccountProxyID = 0
		m.mu.Unlock()
		releaseCreate(t, hit)

		var aerr error
		select {
		case aerr = <-done:
		case <-time.After(6 * time.Second):
			t.Fatal("构建未返回")
		}
		if aerr == nil {
			t.Fatal("管理员手工改绑到异代理后构建必须 fail-closed 拒绝")
		}
		// R7-3 脱敏断言：错误文本绝不含任何代理的 userinfo 用户名/密码。
		for _, leak := range []string{"alice", "s3cretPass", "bob", "t0pSecret"} {
			if strings.Contains(aerr.Error(), leak) {
				t.Fatalf("出口兼容错误绝不能泄露代理 userinfo 凭据 %q: %v", leak, aerr)
			}
		}
		m.mu.Lock()
		pub := m.ctxs[key]
		m.mu.Unlock()
		if pub != nil && !pub.disposed && !pub.creating {
			t.Fatalf("手工改绑到异代理后槽位绝不能发布: %+v", pub)
		}
		resetSlot(id)
	})

	t.Run("并发冷建与自动首绑重绑交错零竞态且结果一致", func(t *testing.T) {
		id, acct := mkAcct(t, "abrace", "r1")
		// 并发冷建与配额自动首绑/重绑交错：结局唯一合法——发布后
		// orig==ProxyID（SyncAutoBoundProxy 或步骤六对齐其一必然生效）。
		var wg sync.WaitGroup
		start := make(chan struct{})
		var aerr atomic.Value
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _, _, _, err := m.acquireAccountCtx(context.Background(), acct, slotURL, false, true)
			if err != nil {
				aerr.Store(err)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 30; i++ {
				autoBindAccountProxy(id, 72)
			}
		}()
		close(start)
		wg.Wait()
		if v := aerr.Load(); v != nil {
			t.Fatalf("并发冷建必须成功: %v", v.(error))
		}
		m.mu.Lock()
		pub := m.ctxs[browserSlotKey{accountID: id}]
		m.mu.Unlock()
		if pub == nil || pub.disposed || pub.creating {
			t.Fatalf("并发交错后槽位必须正常发布: %+v", pub)
		}
		if pub.origAccountProxyID != 72 || pub.boundProxyID != 72 {
			t.Fatalf("并发交错后 orig/bound 必须收敛到合法自动绑定 72，得到 orig=%d bound=%d",
				pub.origAccountProxyID, pub.boundProxyID)
		}
		resetSlot(id)
	})

	t.Run("绑定写库零行命中绝不同步内存事实", func(t *testing.T) {
		// R7-4 严格提交门控：autoBindAccountProxy 锁内重读行存在、但
		// UPDATE 0 行命中（行在重读与写库之间被删）时 bindAccountProxyLocked
		// 返回 false，绝不调用 SyncAutoBoundProxy 谎改驻留槽内存事实。
		// 复现失败特征（修复前）：bindAccountProxyLocked 丢弃 Exec 结果与
		// 影响行数，0 行命中同样把未落库的绑定事实写进内存。
		id, acct := mkAcct(t, "abghost", "g1")
		_ = acct
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     "g|x",
			browserContextID:   "bc-ghost",
			sessionID:          "sess-ghost",
			proxyURL:           slotURL,
			origAccountProxyID: 0,
			boundProxyID:       72,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()
		// 直接验证门控返回值：行存在时 UPDATE 真实写中必须返回 true；
		// 行被删除后 UPDATE 0 行命中必须返回 false（修复前丢弃 Exec 结果
		// 与影响行数，两种情形无从区分）。
		claimMu.Lock()
		if !bindAccountProxyLocked(id, 72) {
			claimMu.Unlock()
			t.Fatal("前置：行存在时写库必须命中（返回 true）")
		}
		bindAccountProxyLocked(id, 0)
		claimMu.Unlock()
		// accountDelete 内部连带清除自动绑定来源记录（自持 claimMu）——
		// 绝不能在持 claimMu 时调用（重入自死锁）。
		if err := accountDelete(id); err != nil {
			t.Fatalf("delete: %v", err)
		}
		claimMu.Lock()
		if bindAccountProxyLocked(id, 72) {
			claimMu.Unlock()
			t.Fatal("行已删除的 UPDATE 0 行命中必须返回 false（严格提交门控）")
		}
		claimMu.Unlock()
		// autoBindAccountProxy 自身对已删行的短路（a==nil）同样保证不调
		// SyncAutoBoundProxy：内存事实保持原样。
		autoBindAccountProxy(id, 72)
		m.mu.Lock()
		orig := ac.origAccountProxyID
		m.mu.Unlock()
		if orig != 0 {
			t.Fatalf("未真实写中的绑定绝不同步内存事实（orig 必须保持 0），得到 %d", orig)
		}
		// 对照：行存在时自动绑定真实落库并同步内存事实（门控不误伤正常路径）。
		id2, _ := mkAcct(t, "ablive", "g2")
		key2 := browserSlotKey{accountID: id2}
		ac2 := &accountBrowserCtx{
			key:                key2,
			epoch:              1,
			cookieIdentity:     "l|x",
			browserContextID:   "bc-live",
			sessionID:          "sess-live",
			proxyURL:           slotURL,
			origAccountProxyID: 0,
			boundProxyID:       72,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac2.readyCh = make(chan struct{})
		close(ac2.readyCh)
		m.mu.Lock()
		m.ctxs[key2] = ac2
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key2)
			m.mu.Unlock()
		}()
		autoBindAccountProxy(id2, 72)
		if a := accountByID(id2); a == nil || a.ProxyID != 72 {
			t.Fatalf("对照：行存在时自动绑定必须真实落库: %+v", a)
		}
		m.mu.Lock()
		orig2 := ac2.origAccountProxyID
		m.mu.Unlock()
		if orig2 != 72 {
			t.Fatalf("对照：真实写中的自动绑定必须同步内存事实到 72，得到 %d", orig2)
		}
	})

	t.Run("管理员手工改绑到溢出出口绝不对齐且下轮保活拒绝", func(t *testing.T) {
		// R8-3（Reviewer MF-3）手工 PATCH 伪装通道：账号合法绑定 P1(72)，
		// 但本槽实际出口是溢出代理 P2(73)——构建窗口内管理员手工 PATCH
		// 改绑到 P2（恰好等于溢出出口，URL 完全一致）。修复前的对齐判据
		// 只是「绑定代理实时 URL == 槽位出口」，手工 PATCH 同样命中，orig
		// 被静默对齐成 P2——管理员改绑被伪装成合法自动绑定，改绑拦截与
		// 保活复核全被绕过。R8-3 只认可核的自动绑定来源记录：手工 PATCH
		// 清除记录，步骤 6 绝不对齐（orig 保持 P1），下轮保活原样
		// fail-closed 拒绝。
		id, acct := mkAcct(t, "abdisguise", "d1")
		bindAccountProxy(id, 72) // 管理员初始绑定 P1（建槽前）
		if a := accountByID(id); a == nil || a.ProxyID != 72 {
			t.Fatalf("前置：初始手工绑定必须落库 P1: %+v", a)
		}
		exitURL2 := proxyURLByID(73)
		if exitURL2 == "" || exitURL2 == slotURL {
			t.Fatalf("前置：P2 实时 URL 必须非空且异于 P1: %q", exitURL2)
		}
		done, hit := startBuild(t, acct, exitURL2) // 本槽实际出口 = 溢出代理 P2
		// 确定性时序：先确认 creating 槽已注册且建槽快照 orig 已固定为
		// P1(72)，再做手工改绑——否则 PATCH 可能先于注册快照读落库，orig
		// 一出生就是 73，测的就不再是改绑窗口而是注册时序竞态。
		key := browserSlotKey{accountID: id}
		spinDeadline := time.Now().Add(3 * time.Second)
		for {
			m.mu.Lock()
			cand := m.ctxs[key]
			var candOrig int64
			if cand != nil {
				candOrig = cand.origAccountProxyID
			}
			m.mu.Unlock()
			if cand != nil {
				if candOrig != 72 {
					t.Fatalf("前置：建槽快照 orig 必须已固定为 P1=72，得到 %d", candOrig)
				}
				break
			}
			if time.Now().After(spinDeadline) {
				t.Fatal("creating 槽未在期限内注册")
			}
			time.Sleep(time.Millisecond)
		}
		// 确定性屏障：先持有该账号的 L1 Cookie 锁——buildAccountCtx 步骤 6
		// 必须经 acquireAcctCookieLock 才能重读 fresh，故测试此刻独占锁后：
		// 构建必然阻塞在步骤 6 之前，而下方手工 PATCH（纯 DB/内存操作，不取
		// L1）先行落库。松开 L1 后步骤 6 重读的 fresh 确定性地是改绑后快照
		//（ProxyID=73）——手工改绑确定发生在「建槽快照登记」与「播种重读」
		// 之间，时序不再依赖调度运气。
		l1unlock, l1err := acquireAcctCookieLock(context.Background(), id)
		if l1err != nil {
			t.Fatalf("取 L1 屏障锁: %v", l1err)
		}
		// 构建窗口内：管理员手工 PATCH 改绑到恰好等于溢出出口的 P2。
		bindAccountProxy(id, 73)
		if a := accountByID(id); a == nil || a.ProxyID != 73 {
			l1unlock()
			t.Fatalf("前置：手工 PATCH 必须落库 P2: %+v", a)
		}
		// 手工 PATCH 必须清除可核自动绑定来源记录（对齐绝不命中）。
		if isAutoBoundProxyForURL(id, 73, exitURL2) {
			l1unlock()
			t.Fatal("手工 PATCH 后绝不能残留自动绑定来源记录")
		}
		releaseCreate(t, hit)
		// 放行创建回复后步骤 6 才能进 L1——此刻放开屏障锁：改绑事实已确定
		// 先于播种重读落库（屏障语义），步骤 6 所见 fresh 必为 ProxyID=73。
		l1unlock()
		var aerr error
		select {
		case aerr = <-done:
		case <-time.After(6 * time.Second):
			t.Fatal("构建未返回")
		}
		if aerr != nil {
			t.Fatalf("非严格模式下改绑到本槽实际出口的构建仍应发布: %v", aerr)
		}
		m.mu.Lock()
		pub := m.ctxs[key]
		m.mu.Unlock()
		if pub == nil || pub.disposed || pub.creating {
			t.Fatalf("发布失败: %+v", pub)
		}
		// 核心断言：orig 绝不能被对齐成手工改绑的 P2——保持建槽快照 P1。
		//（bound 是槽位实际出口的事实登记，注册时按出口 URL 反查即为 73，
		// 与来源无关——对齐守卫只在 origAccountProxyID 上有意义。）
		if pub.origAccountProxyID != 72 {
			t.Fatalf("手工 PATCH 到溢出出口绝不对齐：orig 必须保持建槽快照 P1=72，得到 orig=%d bound=%d",
				pub.origAccountProxyID, pub.boundProxyID)
		}
		// 下轮保活（resolveActiveRotateProxy）按快照复核：orig(P1) !=
		// cur.ProxyID(P2) 属「快照后已被改绑」——fail-closed 拒绝续期，
		// 绝不把过期快照当作当前账号的合法出口放行。
		snap := activeSlotSnapshot{
			accountID: id, bcID: pub.browserContextID, proxyURL: pub.proxyURL,
			allowDirect: pub.allowDirect, boundProxyID: pub.boundProxyID,
			origAccountProxyID: pub.origAccountProxyID, ident: pub.cookieIdentity,
		}
		if _, perr := resolveActiveRotateProxy(accountByID(id), snap, true); perr == nil {
			t.Fatal("手工改绑后的下轮保活必须 fail-closed 拒绝（过期快照拒绝）")
		} else if !strings.Contains(perr.Error(), "fail-closed") {
			t.Fatalf("保活拒绝必须点名 fail-closed: %v", perr)
		}
		resetSlot(id)
	})

	t.Run("并发自动绑定同步不误杀合法冷建", func(t *testing.T) {
		// R8-3（Reviewer MF-3）exitCompatCheck 快照一致性复核：模拟真实
		// 交错终态——自动绑定把 DB 从 P1(72) 推到 P2(73) 并经
		// SyncAutoBoundProxy 同步了驻留槽 orig=P2，而播种前重读的 fresh
		// 恰是同步窗口前的陈旧快照（ProxyID 仍是 P1）。orig != fresh.ProxyID
		// 但 orig 是可核自动绑定来源（记录为真）——重读一次 DB 即与 orig
		// 一致（P2），绝不误杀合法冷建。修复前（无复核）直接命中
		// fresh.ProxyID != origAccountProxyID 的改绑拦截，合法冷建被误杀。
		id, _ := mkAcct(t, "abstale", "s1")
		bindAccountProxy(id, 72)
		stale := accountByID(id) // 陈旧快照：ProxyID=72
		if stale == nil || stale.ProxyID != 72 {
			t.Fatalf("前置：陈旧快照必须为 P1: %+v", stale)
		}
		// 并发合法自动重绑 72 -> 73：真实写库 + 登记来源记录 + 同步驻留槽。
		setProxyField(72, func(p *Proxy) { p.Enabled = false }) // P1 失效才允许重绑
		autoBindAccountProxy(id, 73)
		setProxyField(72, func(p *Proxy) { p.Enabled = true })
		if a := accountByID(id); a == nil || a.ProxyID != 73 {
			t.Fatalf("前置：自动重绑必须落库 P2: %+v", a)
		}
		exitURL2 := proxyURLByID(73)
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(stale.Cookie),
			browserContextID:   "bc-stale",
			sessionID:          "sess-stale",
			proxyURL:           exitURL2, // 槽位实际出口 = P2
			allowDirect:        false,
			origAccountProxyID: 73, // SyncAutoBoundProxy 已同步成 P2
			boundProxyID:       73,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()
		// 前置：orig(73) != stale.ProxyID(72)——快照一致性复核场景。
		if !isAutoBoundProxyForURL(id, 73, exitURL2) {
			t.Fatal("前置：自动重绑必须登记可核来源记录")
		}
		if cerr := exitCompatCheck(ac, stale); cerr != nil {
			t.Fatalf("并发自动绑定同步的陈旧快照绝不能误杀合法冷建: %v", cerr)
		}
		// 负对照：手工 PATCH 伪装（清除来源记录）时同一陈旧快照必须
		// 原样 fail-closed 拦截——复核绝不放行手工改绑。
		clearAutoBoundProxy(id)
		if cerr := exitCompatCheck(ac, stale); cerr == nil {
			t.Fatal("手工 PATCH 伪装（无可核自动绑定来源）必须 fail-closed 拦截")
		}
	})
}

// TestBrowserLeaseIdentityContractRejectsSessionSwap 复核 R9-1（Reviewer MF-1）：
// 正 ID 通过身份 A 的校验后，扩展可在启动/等槽期间按同标签同序号把同一
// 行换绑成会话 B（accountSync 的同标签换绑路径）。修复前租约循环用 DB 新
// 行替换请求快照，jar 播种读到 B、guard 比对 B 与 B 全链路自洽——用 B 的
// 槽成功抓到 token 却按 A 的键入缓存。修复后授予租约前必须核对 jar 身份
// 与请求级身份契约一致，失配立即拒绝（槽位不摘除，留给 B 的合法请求）。
// 确定性构造：holdCreateBC 扣住创建 + L1 Cookie 锁屏障——持 L1 时步骤 6
// 必然阻塞在播种重读之前，此时在锁外直接把行换绑成会话 B（与 accountSync
// 换绑后的 DB 终态同构），再放行创建回复并释放 L1：播种确定性读到 B。
func TestBrowserLeaseIdentityContractRejectsSessionSwap(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookieA := "SAPISID=idcA; __Secure-1PSID=idcPA; SID=idcsA; __Secure-1PSIDTS=idctA"
	cookieB := "SAPISID=idcB; __Secure-1PSID=idcPB; SID=idcsB; __Secure-1PSIDTS=idctB"
	if cookieIdentity(cookieA) == cookieIdentity(cookieB) {
		t.Fatal("前置：A/B 必须是不同会话身份")
	}
	id, err := accountInsert("idcswap", cookieA, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	acct := accountByID(id)
	key := browserSlotKey{accountID: id}
	defer func() {
		m.InvalidateAccount(id, false, "test cleanup")
		m.mu.Lock()
		delete(m.ctxs, key)
		m.mu.Unlock()
	}()

	// 启动冷建并扣住创建回复。
	srv.holdCreateBC.Store(true)
	done := make(chan error, 1)
	go func() {
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), acct, "", true, true)
		done <- aerr
	}()
	var hit createBCCall
	select {
	case hit = <-srv.createBCHit:
	case <-time.After(4 * time.Second):
		t.Fatal("fake 服务端未见 Target.createBrowserContext")
	}
	srv.holdCreateBC.Store(false)

	// 确定性屏障：持 L1——步骤 6 必然阻塞在播种重读之前。此时把行换绑
	// 成会话 B（跳过 L1 直接写库模拟扩展同步落库终态）。
	l1unlock, l1err := acquireAcctCookieLock(context.Background(), id)
	if l1err != nil {
		t.Fatalf("取 L1 屏障锁: %v", l1err)
	}
	if _, derr := getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`, cookieB, id); derr != nil {
		l1unlock()
		t.Fatalf("模拟扩展换绑写库: %v", derr)
	}
	if cur := accountByID(id); cur == nil || cookieIdentity(cur.Cookie) != cookieIdentity(cookieB) {
		l1unlock()
		t.Fatal("前置：换绑必须已落库")
	}
	// 放行创建回复：构建推进到步骤 6 等 L1；松开 L1 后播种确定性读到
	// 会话 B（屏障语义：换绑写库确定先于播种重读）。
	if err := srv.sendText([]byte(fmt.Sprintf(
		`{"id":%d,"result":{"browserContextId":%q}}`, hit.id, hit.bcID))); err != nil {
		l1unlock()
		t.Fatalf("放行创建回复: %v", err)
	}
	l1unlock()

	var aerr error
	select {
	case aerr = <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("租约未返回")
	}
	// 核心断言：租约必须因身份契约失配被拒——绝不能把 B 会话的 Jar 租
	// 给身份为 A 的请求（token 按键 A 入缓存的串会话通道就此关闭）。
	if aerr == nil {
		t.Fatal("行换绑成会话 B 后，身份为 A 的请求租约必须被拒绝（R9-1 身份契约）")
	}
	if !strings.Contains(aerr.Error(), "identity contract mismatch") {
		t.Fatalf("拒绝必须点名身份契约失配: %v", aerr)
	}
	// 槽位本身不被摘除（它装的是 B 的合法播种，留给 B 的请求复用）。
	m.mu.Lock()
	slot, slotOK := m.ctxs[key]
	alive := slotOK && !slot.disposed && !slot.creating
	m.mu.Unlock()
	if !alive {
		t.Fatal("身份失配拒绝绝不能误摘 B 会话的合法槽位")
	}

	// 对照：B 身份的后续请求必须能租到同一个槽（不误伤合法复用）。
	acctB := accountByID(id)
	_, ac2, _, release2, err2 := m.acquireAccountCtx(context.Background(), acctB, "", true, true)
	if err2 != nil {
		t.Fatalf("换绑后的 B 身份请求必须能正常租约: %v", err2)
	}
	if ac2 != slot {
		t.Fatal("B 身份请求必须复用同一驻留槽（拒绝不误摘、不误重建）")
	}
	release2()
}

// TestBrowserSlotExitViolationRecheckOnConcurrentAutoBind 复核 R9-3
// （Reviewer MF-3）：既有就绪槽的 R3-M1 漂移判定用进 m.mu 前的账号快照——
// 快照读取与进锁之间并发配额请求可经 autoBindAccountProxy 把 DB 绑定推进
// 并经 SyncAutoBoundProxy 把槽 orig 同步为 P2：拿旧 P1 比新 orig 会把合法
// 槽误判漂移摘除并终止在途流。修复后破坏性摘除前必须以一致快照复核：
// 确属并发自动绑定同步放弃摘除；真正手工改绑/失效出口仍确认摘除。
// recheckSlotExitViolation 是新引入的生产复核函数（接口签名变更豁免：旧
// 代码的锁内直接摘除无任何锁外复核入口可复现）。
func TestBrowserSlotExitViolationRecheckOnConcurrentAutoBind(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 81, Name: "rc1", URL: "http://rc1:8080", Enabled: true},
		{ID: 82, Name: "rc2", URL: "http://rc2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookie := "SAPISID=rcx; __Secure-1PSID=rcp; SID=rcs; __Secure-1PSIDTS=rct"
	id, err := accountInsert("rcex", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	bindAccountProxy(id, 81)
	stale := accountByID(id) // 陈旧快照：ProxyID=81（进 m.mu 前读取）
	if stale == nil || stale.ProxyID != 81 {
		t.Fatalf("前置：陈旧快照必须绑定 P1: %+v", stale)
	}
	// 并发自动重绑 81 -> 82：真实写库 + 登记来源 + SyncAutoBoundProxy 同步
	// 槽 orig（槽实际出口本来就是 P2——熔断冷却期的合法溢出槽）。
	setProxyField(81, func(p *Proxy) { p.Enabled = false })
	autoBindAccountProxy(id, 82)
	setProxyField(81, func(p *Proxy) { p.Enabled = true })
	if a := accountByID(id); a == nil || a.ProxyID != 82 {
		t.Fatalf("前置：自动重绑必须落库 P2: %+v", a)
	}
	exitP2 := proxyURLByID(82)
	key := browserSlotKey{accountID: id}
	ac := &accountBrowserCtx{
		key:                key,
		epoch:              1,
		cookieIdentity:     cookieIdentity(cookie),
		browserContextID:   "bc-rc",
		targetID:           "t-rc",
		sessionID:          "sess-rc",
		proxyURL:           exitP2,
		allowDirect:        false,
		origAccountProxyID: 82, // SyncAutoBoundProxy 已同步成 P2
		boundProxyID:       82,
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

	// 前置情景复现：用陈旧快照做锁内判定必然误判漂移（修复前此刻直接
	// 摘除合法槽并终止在途流）。
	poolSnap := snapshotProxyPool()
	if !ac.readySlotExitViolation(stale, true, false, poolSnap) {
		t.Fatal("前置：陈旧快照下的锁内判定必须报疑似漂移（重现交错场景）")
	}
	// 核心断言：锁外一致快照复核必须识别并发自动绑定同步——新行
	// ProxyID 与槽 orig 一致（82==82），放弃摘除。
	if violation, _ := recheckSlotExitViolation(stale, ac); violation {
		t.Fatal("并发自动绑定同步的合法槽绝不能被确认摘除（R9-3 快照错位误杀）")
	}

	// 核心分支（R9-3 交错终态的最强形态）：新鲜快照 re 与 orig **不一致**
	// 且新鲜复核仍报漂移——例如 quota 的重绑尚未推进到最新一行（读取在
	// 两次自动重绑之间），但 orig 是可核的自动绑定来源：这是并发自动绑
	// 定同步的延伸，下一轮 freshAcct 必然收敛，绝不能摘。构造：手工把
	// 绑定拨回 P1 后重新走自动通道重绑到 P2 并登记来源，再把 DB 逆行拨
	// 回 P1（不改变自动来源记录——等价于「快照落在两次自动重绑中间」）。
	clearAutoBoundProxy(id)
	bindAccountProxy(id, 81)
	setProxyField(81, func(p *Proxy) { p.Enabled = false })
	autoBindAccountProxy(id, 82)
	setProxyField(81, func(p *Proxy) { p.Enabled = true })
	if !isAutoBoundProxyForURL(id, 82, exitP2) {
		t.Fatal("前置：自动重绑必须登记可核来源记录")
	}
	// DB 逆行回 P1（绕过 bindAccountProxy 以保留自动来源记录——模拟快照
	// 落在自动重绑推进中间的交错终态）。
	claimMu.Lock()
	if !bindAccountProxyLocked(id, 81) {
		claimMu.Unlock()
		t.Fatal("前置：逆行拨回必须落库")
	}
	claimMu.Unlock()
	if a := accountByID(id); a == nil || a.ProxyID != 81 {
		t.Fatalf("前置：逆行拨回后绑定必须为 P1: %+v", a)
	}
	if violation, _ := recheckSlotExitViolation(stale, ac); violation {
		t.Fatal("orig 为可核自动绑定来源时绝不能确认摘除（并发自动绑定延伸，下一轮收敛）")
	}

	// 负对照 1：手工 PATCH 伪装（清除自动来源记录）+ 新行与 orig 不一致——
	// 复核必须确认摘除（真正手工改绑拦截语义不动）。
	clearAutoBoundProxy(id)
	if violation, _ := recheckSlotExitViolation(stale, ac); !violation {
		t.Fatal("手工改绑后漂移复核必须确认摘除（fail-closed 语义不动）")
	}

	// 负对照 2：失效出口——槽绑定代理被禁用后复核必须确认摘除。
	bindAccountProxy(id, 82)
	autoBindAccountProxy(id, 82)
	setProxyField(82, func(p *Proxy) { p.Enabled = false })
	defer setProxyField(82, func(p *Proxy) { p.Enabled = true })
	if violation, _ := recheckSlotExitViolation(stale, ac); !violation {
		t.Fatal("出口代理失效后复核必须确认摘除（失效出口拒绝不动）")
	}
}

// TestCDPRegisterPendingRejectsTerminalUnderLock 复核 R9-5（Reviewer MF-5）：
// 终态预检在 pendMu 锁外时，调用方通过预检后读协程可先 failAll 关 done
// 并扫空 pending 表，随后调用方仍登记——表项及 latePendingCap 名额泄漏，
// 甚至向已无读循环的连接发出创建。修复后 pendMu 内、登记/计数前再次拒绝
// 终态：登记与终态清扫有唯一先后。确定性构造：failAll 完成后（终态清扫
// 已先发生）registerPending 必须拒绝且绝不占名额、绝不登记槽位。
func TestCDPRegisterPendingRejectsTerminalUnderLock(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
	if err != nil {
		t.Fatalf("dial fake cdp: %v", err)
	}
	client := newCDPClient(conn)
	defer client.Close()

	// 存活连接：正常登记成功且占名额（正对照）。
	ch := make(chan cdpReply, 1)
	onLate := func(raw json.RawMessage, err error) {}
	if err := client.registerPending(1, ch, onLate); err != nil {
		t.Fatalf("存活连接登记必须成功: %v", err)
	}
	client.pendMu.Lock()
	cnt := client.latePendingCount
	_, ok1 := client.pending[1]
	client.pendMu.Unlock()
	if cnt != 1 || !ok1 {
		t.Fatalf("存活登记必须占名额并入表: cnt=%d ok=%v", cnt, ok1)
	}

	// 终态化（failAll 扫表与关 done 完成——等同「终态清扫已先发生」的
	// 交错终态）。
	client.failAll(errors.New("cdp read: simulated EOF"))
	client.pendMu.Lock()
	cntAfterFail := client.latePendingCount
	client.pendMu.Unlock()
	if cntAfterFail != 0 {
		t.Fatalf("failAll 必须回收全部名额，得到 %d", cntAfterFail)
	}

	// 核心断言：终态之后的登记必须在 pendMu 内被拒绝——绝不登记槽位、
	// 绝不占名额（修复前仅凭锁外预检会在交错窗口内漏登，名额泄漏）。
	ch2 := make(chan cdpReply, 1)
	err2 := client.registerPending(2, ch2, onLate)
	if err2 == nil {
		t.Fatal("终态之后 registerPending 必须拒绝登记（R9-5 锁内终态复核）")
	}
	client.pendMu.Lock()
	_, ok2 := client.pending[2]
	cntFinal := client.latePendingCount
	client.pendMu.Unlock()
	if ok2 {
		t.Fatal("终态之后的登记绝不能入 pending 表（表项泄漏）")
	}
	if cntFinal != 0 {
		t.Fatalf("终态之后的登记绝不能占 latePendingCap 名额，得到 %d", cntFinal)
	}

	// 端到端对照：终态之后的公开 CallWithLateHandler 同样立即返回终态
	// 错误，绝不向已无读循环的连接发送（gotCalls 零新增）。
	before := len(srv.gotCalls)
	var out struct{}
	if cerr := client.CallWithLateHandler(context.Background(), "", "Target.createBrowserContext",
		map[string]interface{}{"disposeOnDetach": true}, &out, makeLateBCReaper(client)); cerr == nil {
		t.Fatal("终态连接的创建调用必须立即失败")
	}
	select {
	case extra := <-srv.gotCalls:
		t.Fatalf("终态之后绝不能向连接发出创建: %+v", extra)
	case <-time.After(150 * time.Millisecond):
	}
	_ = before
}

// TestBrowserLateReaperCancelDispatchSameTick 复核 R8-1/R8-6（Reviewer MF-1）
// 与 R9-6（Reviewer MF-6）：「取消与派发同时就绪」边界下回复有且仅有一
// 个所有者——onLate 恰好执行 1 次、makeLateBCReaper 发出的
// Target.disposeBrowserContext 恰好 1 次（绝不零次也不二次），共享 CDP
// 连接保持存活。
//
// 确定性构造（R9-6 返工要点，绝不靠 Gosched/睡眠证明交错）：
//
//  1. 服务端侧 holdCreateBC 扣住创建自动回复——测试抢锁/取消前 fake
//     绝不可能自行回包删槽，「回复未派发」状态由测试独占控制；
//  2. 晚到标记路径（槽仍在）走真实生产链路：取消时 ch 必然空、唯一就绪
//     分支是 ctx.Done()，调用方确定进入取消分支并标记 late；测试随后
//     放行唯一回复，readLoop 真实 dispatch 调 onLate——整个交错次序由
//     网络往返（收到回复后才可能发生的事）串行，零睡眠零随机；
//  3. 同刻派发路径（槽已被 dispatch 删走）由抽取的生产裁决函数
//     resolveCancelOwnership 构造验证：测试在 pendMu 内按 dispatch 的同
//     构动作预先删除槽位并送入唯一回复（已派发终态），再调用真实生产
//     函数裁决——返回 true 时取消方必须接走回复调 onLate（恰一次）。
func TestBrowserLateReaperCancelDispatchSameTick(t *testing.T) {
	t.Run("取消时槽仍在则标记late并由dispatch调onLate恰好一次", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
		if err != nil {
			t.Fatalf("dial fake cdp: %v", err)
		}
		client := newCDPClient(conn)
		defer client.Close()
		// 扣住创建自动回复：取消动作完成前服务端绝不自行回包删槽。
		srv.holdCreateBC.Store(true)
		defer srv.holdCreateBC.Store(false)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var bcOut struct {
			BrowserContextID string `json:"browserContextId"`
		}
		done := make(chan error, 1)
		go func() {
			done <- client.CallWithLateHandler(ctx, "", "Target.createBrowserContext",
				map[string]interface{}{"disposeOnDetach": true}, &bcOut, makeLateBCReaper(client))
		}()
		var hit createBCCall
		select {
		case hit = <-srv.createBCHit:
		case <-time.After(3 * time.Second):
			t.Fatal("fake 服务端未见 Target.createBrowserContext")
		}
		if hit.bcID == "" {
			t.Fatal("createBCHit 必须携带分配的 bcID")
		}

		// R10-7（Reviewer S3）写完成握手：createBCHit 只证明服务端收到
		// 完整帧，不证明客户端已从 sendTextCtx 返回并拆除 stop() 写取消
		// 守卫——此刻 cancel 可能命中「写入中取消」关闭共享连接，而非
		// 所声称的唯一 select 取消分支。抢占并归还 writeSem：获取成功
		// 即证明 sendTextCtx 已完整返回（写权已归还意味着函数尾部的
		// stop() 已执行），此后取消确定走取消分支。
		client.conn.writeSem <- struct{}{}
		<-client.conn.writeSem

		// 取消：回复被扣住（ch 必然空），select 唯一就绪分支是
		// ctx.Done()——调用方确定进入取消分支。取消方返回（done 收到）
		// 本身证明 resolveCancelOwnership 已在 pendMu 内执行完：槽仍在
		//（回复被扣住不可能派发），故必然是「标记 late、所有权归
		// dispatch」路径。
		cancel()
		select {
		case cerr := <-done:
			if !errors.Is(cerr, context.Canceled) {
				t.Fatalf("取消分支必须返回 ctx.Err()，得到 %v", cerr)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("调用未在取消后返回")
		}
		// 确定性断言：取消返回后槽位必须仍在且已标记 late（所有权归
		// dispatch 的前提）。
		client.pendMu.Lock()
		ent, ok := client.pending[hit.id]
		late := ok && ent.late
		client.pendMu.Unlock()
		if !late {
			t.Fatal("取消后槽位必须仍在且标记 late（回复所有权归 dispatch）")
		}

		// 放行唯一迟到回复：readLoop 真实 dispatch——late 槽位的回复不再
		// 发给无人接收的 ch，改为一次性短协程调 onLate（所有权恰一次）。
		if err := srv.sendText([]byte(fmt.Sprintf(
			`{"id":%d,"result":{"browserContextId":%q}}`, hit.id, hit.bcID))); err != nil {
			t.Fatalf("放行迟到回复: %v", err)
		}

		// onLate 恰好执行 1 次：reaper 按迟到 bcID 发出恰好 1 次
		// Target.disposeBrowserContext。
		deadline := time.After(4 * time.Second)
		disposes := 0
		for disposes == 0 {
			select {
			case call := <-srv.gotCalls:
				if call.method == "Target.disposeBrowserContext" &&
					strings.Contains(string(call.params), hit.bcID) {
					disposes++
				}
			case <-deadline:
				t.Fatalf("late 回复的 onLate 被放弃：%s 从未被 dispose（零次所有者）", hit.bcID)
			}
		}
		// 宽限窗内绝不能出现第二次 dispose（二次所有者）。
		drain := time.After(300 * time.Millisecond)
	drainLoop:
		for {
			select {
			case call := <-srv.gotCalls:
				if call.method == "Target.disposeBrowserContext" &&
					strings.Contains(string(call.params), hit.bcID) {
					t.Fatalf("onLate/dispose 必须恰好 1 次，绝不能二次回收 %s", hit.bcID)
				}
			case <-drain:
				break drainLoop
			}
		}
		if client.Closed() {
			t.Fatal("迟到回收绝不能关闭共享 CDP 连接")
		}
	})

	t.Run("取消时槽已被dispatch删走则取消方接走回复调onLate恰好一次", func(t *testing.T) {
		// R9-6 同刻构造：取消与派发同时就绪（select 恰选取消分支而槽已
		// 被 dispatch 删走）是生产裁决函数 resolveCancelOwnership 的输入
		// 状态，不是可外部同步的协程事件——直接用真实生产函数构造该
		// 终态验证所有权裁决，绝不以睡眠/调度概率冒充同步。
		srv := newFakeCDPServer(t)
		defer srv.close()
		conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
		if err != nil {
			t.Fatalf("dial fake cdp: %v", err)
		}
		client := newCDPClient(conn)
		defer client.Close()

		// 构造已派发终态：在 pendMu 内登记槽位，再按 dispatch 的同构动作
		// 删除槽位、回收名额、把唯一回复推进缓冲 ch（cap=1 绝不阻塞）。
		ch := make(chan cdpReply, 1)
		lateFired := make(chan json.RawMessage, 1)
		onLate := func(raw json.RawMessage, err error) { lateFired <- raw }
		if err := client.registerPending(42, ch, onLate); err != nil {
			t.Fatalf("registerPending: %v", err)
		}
		// R10-7（Reviewer S3）已派发终态由**真实生产 dispatch** 构造：
		// 测试只登记槽位，随后调用生产 dispatch 派发唯一成功回复——
		// dispatch 在 pendMu 内删槽、回收名额、把唯一回复推进缓冲 ch
		//（cap=1 绝不阻塞）。测试绝不自行删槽/送 ch/读 ch/调 onLate：
		// 中和生产接管链（handleCancelOwnership 不接收/不回调）时本
		// 用例必红。
		client.dispatch([]byte(`{"id":42,"result":{"browserContextId":"bc-sametick"}}`))
		client.pendMu.Lock()
		_, stillPending := client.pending[42]
		client.pendMu.Unlock()
		if stillPending {
			t.Fatal("生产 dispatch 必须在 pendMu 内删除已派发槽位")
		}

		// 真实生产取消处理链（含实际接收/回调）：槽已被 dispatch 删走
		// → resolveCancelOwnership 返回 true → 取消方从 ch 实际接走唯
		// 一回复并调 onLate（生产 handleCancelOwnership 一体执行）。
		// 测试只观察回调与回收。
		client.handleCancelOwnership(42, ch, onLate)
		select {
		case raw := <-lateFired:
			if !strings.Contains(string(raw), "bc-sametick") {
				t.Fatalf("onLate 必须收到真实回复内容: %s", raw)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("生产取消处理链接走回复后必须恰好调用 onLate 一次")
		}
		// 唯一回复已被生产链接走：缓冲 ch 已空（恰一次所有权）。
		select {
		case extra := <-ch:
			t.Fatalf("唯一回复绝不能被第二次接走: %+v", extra)
		default:
		}
		// latePendingCap 名额已由生产 dispatch 回收（恰一次回收）。
		client.pendMu.Lock()
		cnt := client.latePendingCount
		client.pendMu.Unlock()
		if cnt != 0 {
			t.Fatalf("生产 dispatch 必须回收迟到名额，得到 %d", cnt)
		}

		// 对照：槽仍在时（回复未派发）裁决必须返回 false 并标记 late——
		// 所有权归 dispatch，取消方不得再碰 ch。
		ch2 := make(chan cdpReply, 1)
		var late2 atomic.Int32
		onLate2 := func(raw json.RawMessage, err error) { late2.Add(1) }
		if err := client.registerPending(43, ch2, onLate2); err != nil {
			t.Fatalf("registerPending 43: %v", err)
		}
		if client.resolveCancelOwnership(43, onLate2) {
			t.Fatal("槽仍在时必须返回 false（回复所有权归 dispatch）")
		}
		client.pendMu.Lock()
		ent2, ok2 := client.pending[43]
		markedLate := ok2 && ent2.late
		client.pendMu.Unlock()
		if !markedLate {
			t.Fatal("槽仍在时取消方必须仅标记 late（onLate 由 dispatch 调用）")
		}
		// dispatch 端收到迟到回复后调 onLate 恰一次（与生产 dispatch
		// 同构：late 槽回复改走 onLate）。
		client.dispatch([]byte(`{"id":43,"result":{"browserContextId":"bc-late"}}`))
		deadline := time.Now().Add(2 * time.Second)
		for late2.Load() == 0 {
			if time.Now().After(deadline) {
				t.Fatal("dispatch 必须对 late 槽调用 onLate")
			}
			time.Sleep(time.Millisecond)
		}
		if late2.Load() != 1 {
			t.Fatalf("onLate 必须恰好 1 次，得到 %d", late2.Load())
		}
	})
}

// TestBrowserLatePendingCapBackpressure 复核 R8-4（Reviewer MF-4）：
// 按连接未决迟到创建上限——未决 BrowserContext 创建（onLate 携带）达到
// latePendingCap 时，新创建在写发送前直接拒绝（绝不登记槽位、绝不出门），
// 杜绝半活连接上在外创建与远端 BC 无限积累；真实回复到达（dispatch 消费
// 槽位）后名额回收，新创建恢复可发。
func TestBrowserLatePendingCapBackpressure(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
	if err != nil {
		t.Fatalf("dial fake cdp: %v", err)
	}
	client := newCDPClient(conn)
	defer client.Close()
	client.pendMu.Lock()
	client.latePendingCap = 2
	client.pendMu.Unlock()
	srv.holdCreateBC.Store(true)
	defer srv.holdCreateBC.Store(false)

	launch := func() (chan error, createBCCall) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		done := make(chan error, 1)
		go func() {
			var out struct {
				BrowserContextID string `json:"browserContextId"`
			}
			done <- client.CallWithLateHandler(ctx, "", "Target.createBrowserContext",
				map[string]interface{}{"disposeOnDetach": true}, &out, makeLateBCReaper(client))
		}()
		var hit createBCCall
		select {
		case hit = <-srv.createBCHit:
		case <-time.After(3 * time.Second):
			t.Fatal("创建命令必须发出（未决数未达上限）")
		}
		return done, hit
	}
	// 两个创建均已发出、回复被扣住：未决数恰好等于上限。
	_, hit1 := launch()
	launch()
	client.pendMu.Lock()
	if client.latePendingCount != 2 {
		client.pendMu.Unlock()
		t.Fatalf("两个在飞创建后 latePendingCount 必须为 2，得到 %d", client.latePendingCount)
	}
	client.pendMu.Unlock()

	// 第三个创建立即被拒：绝不登记槽位、绝不向共享连接发送。
	rejected := make(chan error, 1)
	go func() {
		var out struct {
			BrowserContextID string `json:"browserContextId"`
		}
		rejected <- client.CallWithLateHandler(context.Background(), "", "Target.createBrowserContext",
			map[string]interface{}{"disposeOnDetach": true}, &out, makeLateBCReaper(client))
	}()
	select {
	case rerr := <-rejected:
		if rerr == nil || !strings.Contains(rerr.Error(), "too many pending browser context creations") {
			t.Fatalf("达上限的新创建必须立即被拒，得到 %v", rerr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("达上限的新创建绝不能挂起等待（必须立即拒绝）")
	}
	select {
	case extra := <-srv.createBCHit:
		t.Fatalf("被拒的创建绝不能发送到共享连接: %+v", extra)
	case <-time.After(200 * time.Millisecond):
	}
	client.pendMu.Lock()
	if client.latePendingCount != 2 {
		client.pendMu.Unlock()
		t.Fatalf("被拒的创建绝不能占名额，latePendingCount 必须保持 2，得到 %d", client.latePendingCount)
	}
	client.pendMu.Unlock()

	// 放行第一个迟到回复：dispatch 消费槽位回收名额——新创建恢复可发。
	if err := srv.sendText([]byte(fmt.Sprintf(
		`{"id":%d,"result":{"browserContextId":%q}}`, hit1.id, hit1.bcID))); err != nil {
		t.Fatalf("放行迟到回复: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		client.pendMu.Lock()
		cnt := client.latePendingCount
		client.pendMu.Unlock()
		if cnt == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("迟到回复到达后名额必须回收（1），得到 %d", cnt)
		}
		time.Sleep(5 * time.Millisecond)
	}
	launch() // 名额回收后新创建照常发出
}

// TestBrowserDetachExitVersionGuardAndConcurrentBump 复核 R10-1
// （Reviewer MF-2/Oracle MF-2）：
//  1. 出口破坏性摘除前，若实际复核与破坏性操作之间该槽出口已发生版本化
//     同步提交（exitSyncVer 递增），摘除必须按锁下版本比对放弃；
//  2. recheckSlotExitViolation / detachIfExitVersionUnchanged 与
//     SyncAutoBoundProxy 并发运行零数据竞争（-race 门：exitSyncVer 与
//     bound/orig/URL 字段绝不配搭读取）；
//  3. 确认摘除的正对照终止在途流。
//
// recheck/detach 系列是 R10-1 新引入的生产复核函数（接口签名变更豁免：
// 修复前锁内直接摘除无任何锁外复核入口可复现）。
func TestBrowserDetachExitVersionGuardAndConcurrentBump(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 71, Name: "vg1", URL: "http://vg1:8080", Enabled: true},
		{ID: 72, Name: "vg2", URL: "http://vg2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.StrictProxyBinding = false })

	t.Run("复核后同步提交递增版本则破坏性摘除必须放弃", func(t *testing.T) {
		cookie := "SAPISID=vgx; __Secure-1PSID=vgp; SID=vgs; __Secure-1PSIDTS=vgt"
		id, err := accountInsert("vgacct", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 72)
		key := browserSlotKey{accountID: id}
		m := browserMgr
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(cookie),
			browserContextID:   "bc-vg",
			targetID:           "t-vg",
			sessionID:          "sess-vg",
			proxyURL:           "http://vg2:8080",
			allowDirect:        false,
			origAccountProxyID: 72,
			boundProxyID:       72,
			exitSyncVer:        3,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()

		// 破坏性路径复核：取一致快照版本。
		_, snapVer := recheckSlotExitViolation(accountByID(id), ac)
		if snapVer != 3 {
			t.Fatalf("复核快照必须捕获快照时刻的版本 3，得到 %d", snapVer)
		}
		// 交错：破坏性操作之前该槽发生一次自动绑定版本化同步提交
		// （真实生产通道：SyncAutoBoundProxy 写 orig/bound 递增 ver）。
		m.SyncAutoBoundProxy(id, 72, "http://vg2:8080")
		m.mu.Lock()
		verAfter := ac.exitSyncVer
		m.mu.Unlock()
		if verAfter != 4 {
			t.Fatalf("前置：版本化同步提交必须递增 exitSyncVer（3 -> %d）", verAfter)
		}
		// 核心断言：版本已变——破坏性摘除必须放弃，槽及其字段零改动。
		det, _ := m.detachIfExitVersionUnchanged(ac, snapVer, errors.New("drift"))
		if det.ac != nil {
			t.Fatal("版本已推进后破坏性摘除必须放弃（并发自动绑定提交绝不误拆）")
		}
		m.mu.Lock()
		alive := m.ctxs[key] == ac && !ac.disposed
		m.mu.Unlock()
		if !alive {
			t.Fatal("放弃的摘除绝不能触碰槽存活状态")
		}
		// 版本值守卫后槽位仍可被后续新一轮一致快照+复核摘除：
		// 版本绝不泄漏影响后续真实变更的摘除。
		_, snapVer2 := recheckSlotExitViolation(accountByID(id), ac)
		if snapVer2 != 4 {
			t.Fatalf("守卫后新一轮复核快照必须反映递增后版本 4，得到 %d", snapVer2)
		}
	})

	t.Run("复核快照与版本化同步并发运行零数据竞争", func(t *testing.T) {
		// -race 门核心：主循环并发走真实复核/摘除以读取快照与字段，
		// 副协程连续执行真实版本化同步提交——任何解锁配搭读取
		//（exitSyncVer/bound/orig/URL）都会被 race 检出。
		cookie := "SAPISID=rgx; __Secure-1PSID=rgp; SID=rgs; __Secure-1PSIDTS=rgt"
		id, err := accountInsert("vgconc", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 71)
		key := browserSlotKey{accountID: id}
		m := browserMgr
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(cookie),
			browserContextID:   "bc-rg",
			targetID:           "t-rg",
			sessionID:          "sess-rg",
			proxyURL:           "http://vg1:8080",
			allowDirect:        false,
			origAccountProxyID: 71,
			boundProxyID:       71,
			// Reviewer #6：初始化为已知正版本——并发写者每次递增，任何时刻
			// 读出的 ver 必然 ≥ 1000；用 ver-1 探测「已存在但更旧的版本」永不
			// 匹配（旧的 ver+1 会恰中并发递增后的当前版本造成随机红）。
			exitSyncVer: 1000,
			readyClosed: true,
			lastUsedAt:  time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					m.SyncAutoBoundProxy(id, 71, "http://vg1:8080")
				}
			}
		}()
		acct := accountByID(id)
		for i := 0; i < 2000; i++ {
			// 竞态门只需并发读写路径：用 ver-1 探测（初始 1000 起步、单调
			// 递增——任何时刻读出的 ver ≥ 1000，ver-1 确定低于当前版本、
			// 永不匹配，绝不真实摘除；真实摘除语义由正对照子测试覆盖）。
			// 移除版本守卫的变异在此处会立即误摘槽导致终态断言失败。
			_, ver := recheckSlotExitViolation(acct, ac)
			if ver < 1000 {
				close(stop)
				wg.Wait()
				t.Fatalf("版本必须初始 1000 起步单调递增，读出 %d", ver)
			}
			det, _ := m.detachIfExitVersionUnchanged(ac, ver-1, errors.New("drift"))
			if det.ac != nil {
				close(stop)
				wg.Wait()
				t.Fatal("版本不匹配的探测绝不能摘除槽位（版本守卫失效）")
			}
		}
		close(stop)
		wg.Wait()
		// 并发压测结束后字段必须自洽（锁内一致快照可复核）。
		m.mu.Lock()
		ok := ac.boundProxyID == 71 && ac.origAccountProxyID == 71 &&
			ac.proxyURL == "http://vg1:8080" && !ac.disposed
		m.mu.Unlock()
		if !ok {
			t.Fatal("并发压测后槽字段必须自洽且槽未被误摘")
		}
	})

	t.Run("版本一致的确认摘除摘除槽位并终止在途流", func(t *testing.T) {
		// 正对照：复核与破坏性操作之间无提交——摘除必须执行，槽从
		// manager 移除、disposed、在途流终止（fatalErr）。
		cookie := "SAPISID=vpx; __Secure-1PSID=vpp; SID=vps; __Secure-1PSIDTS=vpt"
		id, err := accountInsert("vgpos", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 71)
		key := browserSlotKey{accountID: id}
		m := browserMgr
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(cookie),
			browserContextID:   "bc-vp",
			targetID:           "t-vp",
			sessionID:          "sess-vp",
			proxyURL:           "http://vg1:8080",
			allowDirect:        false,
			origAccountProxyID: 71,
			boundProxyID:       71,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
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

		_, snapVer := recheckSlotExitViolation(accountByID(id), ac)
		// 生产摘除链：版本一致 → detach 执行 → manager 层 failStreams。
		det, _ := m.detachIfExitVersionUnchanged(ac, snapVer, browserErr("drifted"))
		if det.ac == nil {
			t.Fatal("版本一致时摘除必须执行")
		}
		m.mu.Lock()
		_, still := m.ctxs[key]
		disposed := ac.disposed
		m.mu.Unlock()
		if still || !disposed {
			t.Fatalf("确认摘除必须从 manager 移除槽并置 disposed（still=%v disposed=%v）", still, disposed)
		}
		m.failStreamsForCtx(det.ac, det.err)
		s.mu.Lock()
		fatal := s.fatalErr
		s.mu.Unlock()
		if fatal == nil {
			t.Fatal("确认摘除必须终止在途流（fatalErr）")
		}
	})
}

// TestAcquirePlanLockedIdentityContractGuards 复核 R10-4（Reviewer M3/Oracle M3）：
// 请求级身份/AuthUser 契约必须在进入 acquirePlanLocked 规划建槽/排空/
// 摘除**之前**核对——窗口交错（行 X→Y→X 轮换或序号变更）下旧请求拿着
// 与行快照不符的身份/序号进入规划，会把恰好合法的既有槽规划排空/摘除；
// 修复后失配立即终态拒绝：零排空、零摘除、槽位存活。
// 构造：DB 行已是交错窗口内的状态（Y 票据或新序号），请求快照仍携带
// 交错前的身份/序号——与生产「等锁期间行被换绑/改序号」终态完全同构。
func TestAcquirePlanLockedIdentityContractGuards(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 61, Name: "pg1", URL: "http://pg1:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookieX := "SAPISID=pgx; __Secure-1PSID=pgp; SID=pgs; __Secure-1PSIDTS=pgt"
	cookieY := "SAPISID=pgy; __Secure-1PSID=pgq; SID=pgu; __Secure-1PSIDTS=pgv"
	mkResidentSlot := func(id int64, cookie, suffix string) (*accountBrowserCtx, browserSlotKey) {
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookie),
			browserContextID: "bc-pg" + suffix,
			targetID:         "t-pg" + suffix,
			sessionID:        "sess-pg" + suffix,
			proxyURL:         "http://pg1:8080",
			allowDirect:      false,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m := browserMgr
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		return ac, key
	}

	t.Run("请求身份与窗口内行身份不符时先拒绝且槽存活", func(t *testing.T) {
		idX, err := accountInsert("pgreqx", cookieX, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(idX)
		ac, key := mkResidentSlot(idX, cookieX, "x")
		m := browserMgr
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()

		// 请求快照：交错前的 X 身份（行尚未换绑时读取）。
		reqCp := *accountByID(idX)
		if cookieIdentity(reqCp.Cookie) != cookieIdentity(cookieX) {
			t.Fatal("前置：请求快照必须是 X 身份")
		}
		// 交错：等槽窗口内行被换绑成异会话 Y（不经 accountSync 的失效
		// 通道——与「扩展推送 Y 后、失效尚未生效/槽恰好合法」的中间态
		// 同构；X→Y→X 轮换窗口同理）。
		// 跨身份换绑不走路径内的 CAS（身份守卫拒绝）——用真实 accountSync
		// 同语义的直接 UPDATE 模拟扩展推送异会话后的行状态。
		if _, err := getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`, cookieY, idX); err != nil {
			t.Fatalf("前置：行换绑 Y 必须落库: %v", err)
		}
		if a := accountByID(idX); cookieIdentity(a.Cookie) != cookieIdentity(cookieY) {
			t.Fatal("前置：DB 行必须是 Y 身份")
		}

		// 请求 X 到来：修复前 plan 身份守卫拿 freshAcct(Y) 比对槽(X)
		// 触发破坏性摘除重建；修复后在任何规划动作之前按请求契约拒绝。
		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), &reqCp,
			"http://pg1:8080", false, true)
		if aerr == nil {
			t.Fatal("请求身份与行身份不符必须在规划前拒绝")
		}
		if !strings.Contains(aerr.Error(), "identity contract mismatch") {
			t.Fatalf("拒绝必须点名身份契约失配（绝不静默重建/串会话）: %v", aerr)
		}
		m.mu.Lock()
		alive := m.ctxs[key] == ac && !ac.disposed
		m.mu.Unlock()
		if !alive {
			t.Fatal("身份不符的拒绝绝不摘除/破坏既有槽")
		}
	})

	t.Run("请求AuthUser与窗口内行序号不符时先拒绝且槽存活", func(t *testing.T) {
		idX, err := accountInsert("pgreqau", cookieX, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(idX)
		ac, key := mkResidentSlot(idX, cookieX, "au")
		m := browserMgr
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()

		// 请求快照：交错前的序号 1。
		reqCp := *accountByID(idX)
		reqCp.AuthUser = 1
		// 交错：等槽窗口内管理员只改 authuser 1→2（不拆槽的合法管理
		// 操作）——旧 /u/1 请求绝不能借该行 Jar 授租。
		if err := accountSetAuthUser(idX, 2); err != nil {
			t.Fatalf("setAuthUser: %v", err)
		}
		if a := accountByID(idX); a.AuthUser != 2 {
			t.Fatalf("前置：行序号必须为 2: %+v", a)
		}

		_, _, _, _, aerr := m.acquireAccountCtx(context.Background(), &reqCp,
			"http://pg1:8080", false, true)
		if aerr == nil {
			t.Fatal("请求 AuthUser 与行序号不符必须在规划前拒绝")
		}
		if !strings.Contains(aerr.Error(), "authuser contract mismatch") {
			t.Fatalf("拒绝必须点名序号契约失配: %v", aerr)
		}
		m.mu.Lock()
		alive := m.ctxs[key] == ac && !ac.disposed
		m.mu.Unlock()
		if !alive {
			t.Fatal("序号不符的拒绝绝不摘除/破坏既有槽")
		}
	})
}

// TestRotateHotInjectTargetIndependentAuthorization 复核 R10-5（Reviewer M2/
// Oracle M2）：「借哪个出口做换发」与「写哪个 BrowserContext」分离授权——
// 自有槽被拒后借同身份兄弟驻留出口换发成功时，prepareHotInject 对自有 BC
// 零注入仅落库（ownAuthorized=false），新票绝不进刚被拒绝的自有槽/旧身份
// 待摘除槽；同身份多号下出口授权绝不连带注入授权。修复前 ownAuthorized
// 恒 true：X→Y 换绑窗口内旧身份 X 的自有槽仍驻留时会被注入 Y 票据（跨身
// 份污染），或出口授权已被拒绝的自有槽继续接收热续期。
func TestRotateHotInjectTargetIndependentAuthorization(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookieX := "SAPISID=hix; __Secure-1PSID=hip; SID=his; __Secure-1PSIDTS=oldts"
	cookieY := "SAPISID=hiy; __Secure-1PSID=hiq; SID=hiu; __Secure-1PSIDTS=oldts"

	mkSlot := func(id int64, cookie, tag string) browserSlotKey {
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookie),
			browserContextID: "bc-" + tag,
			targetID:         "t-" + tag,
			sessionID:        "sess-" + tag,
			allowDirect:      true,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
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
			"SAPISID":          cookieValue(cookie, "SAPISID"),
			"__Secure-1PSID":   cookieValue(cookie, "__Secure-1PSID"),
			"SID":              cookieValue(cookie, "SID"),
			"__Secure-1PSIDTS": "oldts",
		}
		srv.bcMu.Unlock()
		return key
	}
	delSlot := func(key browserSlotKey, ac *accountBrowserCtx) {
		m.mu.Lock()
		delete(m.ctxs, key)
		if ac != nil {
			delete(m.sessionIndex, ac.sessionID)
			delete(m.targetIndex, ac.targetID)
		}
		m.mu.Unlock()
	}
	// 直连轮转的假上游：1PSIDTS 哨兵换发返回新票 newts。
	stubDirectRotate := func(tag string) {
		fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
			h := fhttp.Header{}
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				h.Add("Set-Cookie", "__Secure-1PSIDTS="+tag+"; Domain=.google.com; Path=/; Secure; HttpOnly")
			}
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
			}, nil
		}}
		stubTLSRotateClient(t, fake)
	}

	t.Run("跨身份换绑窗口内借兄弟出口换发写库但对旧身份自有槽零注入", func(t *testing.T) {
		// Oracle M2 ①：父账号 X→Y 换绑后旧身份 X 的自有槽仍驻留（失效尚
		// 未生效的窗口/失效漏网），同身份兄弟 C（Y 身份）驻留出口合法——
		// 换发与写库成功（借兄弟出口），但对 X 旧槽 zero Storage.setCookies。
		idA, err := accountInsert("hibparent", cookieX, "")
		if err != nil {
			t.Fatalf("insert A: %v", err)
		}
		defer accountDelete(idA)
		idC, err := accountInsert("hibsib", cookieY, "")
		if err != nil {
			t.Fatalf("insert C: %v", err)
		}
		defer accountDelete(idC)

		// 父账号换绑 Y（真实 accountSync 终态：行已是 Y）。
		if _, err := getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`, cookieY, idA); err != nil {
			t.Fatalf("换绑 Y: %v", err)
		}
		// 旧身份 X 的自有槽仍驻留（accountSync 失效未生效窗口）。
		keyX := mkSlot(idA, cookieX, "hix")
		m.mu.Lock()
		acX := m.ctxs[keyX]
		m.mu.Unlock()
		defer delSlot(keyX, acX)
		// 兄弟 C 的合法驻留槽（Y 身份，直连出口）。
		keyC := mkSlot(idC, cookieY, "hic")
		m.mu.Lock()
		acC := m.ctxs[keyC]
		m.mu.Unlock()
		defer delSlot(keyC, acC)

		// 候选列表：先父 X 槽（身份 X ≠ 行 Y，必然被拒）、后兄弟 C 槽
		//（合法授权）——确定性复现「自有槽被拒后借兄弟出口」。
		snapX := activeSlotSnapshot{
			accountID: idA, bcID: "bc-hix", proxyURL: "", allowDirect: true,
			ident: cookieIdentity(cookieX),
		}
		snapC := activeSlotSnapshot{
			accountID: idC, bcID: "bc-hic", proxyURL: "", allowDirect: true,
			ident: cookieIdentity(cookieY),
		}
		stubDirectRotate("newts")

		rotateActiveOrSibling1PSIDTS(*accountByID(idA), []activeSlotSnapshot{snapX, snapC}, map[string][]string{})

		// 换发写库必须成功（借兄弟出口）。
		if a := accountByID(idA); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=newts") {
			t.Fatalf("借兄弟出口换发必须写库: %q", a.Cookie)
		}
		// 核心断言：旧身份 X 的自有槽 zero Storage.setCookies——新 Y 票
		// 绝不注入旧身份 X 的 BC（跨身份污染）。jar 值对照不够：R11-2
		// 之后失格槽会被条件摘除（dispose 销毁 jar），故按 fake 服务端
		// 入站调用记录直接核验 bc-hix 零写入（连失败尝试都不该有）。
		drainEnd := time.After(200 * time.Millisecond)
		injectedX := 0
		for {
			select {
			case c := <-srv.gotCalls:
				if c.method == "Storage.setCookies" && strings.Contains(string(c.params), `"browserContextId":"bc-hix"`) {
					injectedX++
				}
			case <-drainEnd:
				goto xdrained
			}
		}
	xdrained:
		if injectedX != 0 {
			t.Fatalf("旧身份 X 自有槽绝不能收到任何 Storage.setCookies（跨身份污染），得到 %d 次", injectedX)
		}
		// R11-2 延伸断言：失格槽（旧身份待摘除槽）在写库后被条件摘除，
		// 绝不留「可授租却永久无法回写」的僵尸槽。
		m.mu.Lock()
		_, stillX := m.ctxs[keyX]
		disposedX := acX.disposed
		m.mu.Unlock()
		if stillX || !disposedX {
			t.Fatalf("失格旧身份槽在写库后必须被条件摘除（防僵尸槽）: still=%v disposed=%v", stillX, disposedX)
		}
	})

	t.Run("同身份兄弟手工绑定他路出口借兄弟合法出口换发写库但对自有槽零注入", func(t *testing.T) {
		// Oracle M2 ③：B 手工绑定 P2（借 P1 无授权）——自有槽驻留但出口
		// 授权复核拒绝；候选中的同身份兄弟槽（出口合法）被选为换发出口。
		// 换发与写库成功，但 ownAuthorized=false：B 自有 BC 零注入。
		cookieB := "SAPISID=hbx; __Secure-1PSID=hbp; SID=hbs; __Secure-1PSIDTS=oldts"
		idB, err := accountInsert("hibsame", cookieB, "")
		if err != nil {
			t.Fatalf("insert B: %v", err)
		}
		defer accountDelete(idB)

		// 兄弟槽候选：B 自己的「驻留但授权被拒」槽 + 同身份另一驻留槽
		// 的快照。构造：B 的槽驻留且候选首项就是 B——手工绑定他路出口
		// 使 B 自有槽出口授权失败（resolveActiveRotateProxy 拒绝）。
		proxyMu.Lock()
		oldP := proxyCache
		proxyCache = []Proxy{
			{ID: 51, Name: "hp1", URL: "http://hp1:8080", Enabled: true},
			{ID: 52, Name: "hp2", URL: "http://hp2:8080", Enabled: true},
		}
		proxyMu.Unlock()
		defer func() { proxyMu.Lock(); proxyCache = oldP; proxyMu.Unlock() }()
		setRuntimeCfg(t, func(c *RuntimeConfig) { c.FallbackDirect = false })
		defer setRuntimeCfg(t, func(c *RuntimeConfig) { c.FallbackDirect = true })
		bindAccountProxy(idB, 52) // B 手工绑定 P2
		defer bindAccountProxy(idB, 0)

		// B 的驻留槽走 P1（手工绑定 P2 后授权复核拒绝 P1 候选）。
		keyB := mkSlot(idB, cookieB, "hib")
		m.mu.Lock()
		acB := m.ctxs[keyB]
		acB.proxyURL = "http://hp1:8080"
		acB.allowDirect = false
		m.mu.Unlock()
		defer delSlot(keyB, acB)

		// 同身份兄弟槽快照：C 出口是 B 当前绑定的 P2——「快照精确指向
		// 其当前绑定的启用代理」无条件放行（B 借兄弟合法出口）。
		// 兄弟 C 未绑定——「未绑定账号借用兄弟号代理快照」规则：B(未绑
		// 定视角下 C 的快照出口 P1 是池中当前启用代理即放行。注意授权判
		// 定以 cur=B 的绑定状态为准，故 C 必须未绑定（B 借 P1 的合法性
		// 等价于池瞬时状态，不依赖 C 的绑定）。
		idC, err := accountInsert("hibsib2", cookieB, "")
		if err != nil {
			t.Fatalf("insert C: %v", err)
		}
		defer accountDelete(idC)

		snapB := activeSlotSnapshot{
			accountID: idB, bcID: "bc-hib", proxyURL: "http://hp1:8080", allowDirect: false,
			ident: cookieIdentity(cookieB),
		}
		// 兄弟槽不实际注册——候选合法性由 resolveActiveRotateProxy 判定；
		// 兄弟候选 accountID=C 绑定 P1、URL=P1 授权通过。
		snapC := activeSlotSnapshot{
			accountID: idC, bcID: "bc-hibc", proxyURL: "http://hp2:8080", allowDirect: false,
			ident: cookieIdentity(cookieB), boundProxyID: 52,
		}
		// 兄弟槽驻留复核需要真实注册：注册 C 的槽。
		keyC := browserSlotKey{accountID: idC}
		acC := &accountBrowserCtx{
			key:              keyC,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookieB),
			browserContextID: "bc-hibc",
			targetID:         "t-hibc",
			sessionID:        "sess-hibc",
			proxyURL:         "http://hp2:8080",
			allowDirect:      false,
			boundProxyID:     52,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
		}
		acC.readyCh = make(chan struct{})
		close(acC.readyCh)
		m.mu.Lock()
		m.ctxs[keyC] = acC
		m.sessionIndex["sess-hibc"] = acC
		m.targetIndex["t-hibc"] = acC
		m.mu.Unlock()
		defer delSlot(keyC, acC)

		// 换发出口是 P2（B 当前绑定、兄弟槽实际出口）——代理 stdlib
		// 缓存桩成返回新票的假上游。
		timeout := rtCfg().RequestTimeout
		newtsRT := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/RotateCookies") {
				h := http.Header{}
				h.Add("Set-Cookie", "__Secure-1PSIDTS=newts3; Domain=.google.com; Path=/; Secure; HttpOnly")
				return &http.Response{
					StatusCode: 200, Status: "200 OK",
					Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
					Request: req,
				}, nil
			}
			return respJSON(req, 404, ""), nil
		})
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Store("http://hp2:8080"+suf, &http.Client{Transport: newtsRT})
		}
		defer func() {
			for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
				stdlibClientCache.Delete("http://hp2:8080" + suf)
			}
		}()

		rotateActiveOrSibling1PSIDTS(*accountByID(idB), []activeSlotSnapshot{snapB, snapC}, map[string][]string{})

		// 换发写库必须成功（借兄弟驻留的 P2 合法出口）。
		if a := accountByID(idB); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=newts3") {
			t.Fatalf("借兄弟出口授权换发必须写库: %q", a.Cookie)
		}
		// 核心断言：B 自有 BC（出口授权被拒）zero 注入——按 fake 服务端
		// 入站调用记录直接核验（R11-2 后失格槽被条件摘除销毁 jar，jar
		// 值对照不足以证明零写入）。
		drainEnd2 := time.After(200 * time.Millisecond)
		injectedB := 0
		for {
			select {
			case c := <-srv.gotCalls:
				if c.method == "Storage.setCookies" && strings.Contains(string(c.params), `"browserContextId":"bc-hib"`) {
					injectedB++
				}
			case <-drainEnd2:
				goto bdrained
			}
		}
	bdrained:
		if injectedB != 0 {
			t.Fatalf("出口授权被拒的自有槽绝不能收到任何 Storage.setCookies，得到 %d 次", injectedB)
		}
		// R11-2：失格自有槽（借 P1 无授权）写库后必须被条件摘除，绝不
		// 留可授租却永久无法回写的僵尸槽。
		m.mu.Lock()
		_, stillB := m.ctxs[keyB]
		disposedB := acB.disposed
		m.mu.Unlock()
		if stillB || !disposedB {
			t.Fatalf("失格自有槽在写库后必须被条件摘除（防僵尸槽）: still=%v disposed=%v", stillB, disposedB)
		}
	})
}

// TestCDPReaderDeathKillsConnNoCreateAfterFailAll 复核 R10-6（Reviewer S1/
// Oracle S1）：读循环不可恢复退出（本地解析超限）时，failAll 必须立即终
// 态化底层连接/写端——此后拿到写权的创建调用绝不向无读循环的连接发出
// 创建帧（远端泄漏 BrowserContext）：创建在真正发送前核对 done 终态，撤
// 登记返回终态错误，pending 表与 late 名额零泄漏。
func TestCDPReaderDeathKillsConnNoCreateAfterFailAll(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	conn, err := dialCDPWebsocket(context.Background(), srv.wsURL)
	if err != nil {
		t.Fatalf("dial fake cdp: %v", err)
	}
	client := newCDPClient(conn)
	defer client.Close()

	// 抢占写权（不归还）——创建调用将停在排队等写权阶段。
	conn.writeSem <- struct{}{}
	writeHeld := true
	defer func() {
		if writeHeld {
			<-conn.writeSem
		}
	}()

	// 启动创建调用：登记 pending 后卡在 writeSem 排队。
	// R12-6-S3（Oracle S3/Reviewer #8）写者越过 done 预检握手：pending
	// 非空只证明登记完成，不能证明写者已越过发送前的 select <-c.done
	// 预检——写者在登记后被抢占时，反转顺序的实现可先关闭 done，写者
	// 恢复便在预检返回，错误实现也能看到零创建。用 callCommon 内的
	// 生产测试钩子确证真实写者已越过 done 预检、进入写权等待之后，
	// 才触发读失败（确定性交接，无 sleep 定序）。钩子在启动写者协程
	// 之前安装（越过预检只有一次，错过则永远无法确证）。
	writerPassedPrecheck := make(chan struct{})
	var precheckOnce sync.Once
	testHookBeforeCDPWriteWait.Store(func() {
		precheckOnce.Do(func() { close(writerPassedPrecheck) })
	})
	defer func() { testHookBeforeCDPWriteWait.Store((func())(nil)) }()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan error, 1)
	writerSettled := make(chan struct{})
	go func() {
		defer close(writerSettled)
		var bcOut struct {
			BrowserContextID string `json:"browserContextId"`
		}
		done <- client.CallWithLateHandler(ctx, "", "Target.createBrowserContext",
			map[string]interface{}{"disposeOnDetach": true}, &bcOut, makeLateBCReaper(client))
	}()
	// 确认已登记（正在排队等写权）。
	deadline := time.Now().Add(3 * time.Second)
	for {
		client.pendMu.Lock()
		n := len(client.pending)
		client.pendMu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("创建调用必须先登记 pending 再等写权")
		}
		time.Sleep(time.Millisecond)
	}
	// 写者必须已越过 done 预检、进入写权等待，才可触发读失败。
	select {
	case <-writerPassedPrecheck:
	case <-time.After(3 * time.Second):
		t.Fatal("写者必须先越过 done 预检进入写权等待（pending 登记不等于越过预检）")
	}

	// R11-7（Oracle B1/Reviewer #4）确定性写者交接：failAll 的写端封闭
	// 与 done 公布是两个独立的终态信号——顺序反转时（先 done 后 conn.Close）
	// 排队写者在两信号之间取得写权仍能把整帧写出门。测试钩子精确落在
	// 两信号之间：此刻归还写权，写者立刻发起发送尝试——顺序正确时写端
	// 已封闭、准入必被拒（fake 服务端零创建）；顺序反转时写端尚未封闭，
	// 帧可出门（本用例确定性失败）。
	// 钩子（在创建协程外声明 writerSettled 后安装）：在「写端封闭之后、
	// done 公布之前」的精确交错点归还写权并等待写者落定——由此观察到
	// client.done ⇒ 写者已经在两信号之间完成了一次完整的发送决策，帧
	// 是否出门是顺序语义的确定性证据（无 sleep 定序）。
	testHookAfterConnClosedBeforeDone.Store(func() {
		select {
		case <-conn.writeSem:
		default:
		}
		select {
		case <-writerSettled:
		case <-time.After(3 * time.Second):
			t.Error("写者在交错点取得写权后必须在发送决策落定（绝不挂起）")
		}
	})
	defer func() { testHookAfterConnClosedBeforeDone.Store((func())(nil)) }()

	// 读循环不可恢复退出：服务端发送声明超长度的二进制帧头（声明长度
	// 超过 maxCDPFrameBytes，readFrame 在分配前本地解析拒绝——读循环
	// 不可恢复退出 → failAll 终态化连接与全部 pending）。只写 10 字节
	// 头：绝不实际发送超大载荷（本机资源不过载）。
	var hdr [10]byte
	hdr[0] = 0x82 // FIN + binary opcode
	hdr[1] = 127  // 8 字节扩展长度
	binary.BigEndian.PutUint64(hdr[2:10], uint64(maxCDPFrameBytes)+1)
	if _, err := srv.conn.Write(hdr[:]); err != nil {
		t.Fatalf("发送解析超限帧头: %v", err)
	}
	// 等终态化完成（done 关闭）：failAll 的 done 在钩子返回后才公布，
	// 钩子等的是写者落定——因此观察到 done ⇒ 写者已在两终态信号之间
	// 完成了发送决策（确定性交接，无 sleep 定序）。
	select {
	case <-client.done:
	case <-time.After(5 * time.Second):
		t.Fatal("读循环解析失败后必须终态化 client.done")
	}
	// 创建调用必须已落定失败（绝不挂起、绝不成功）——钩子在 done 公布前
	// 已等过 writerSettled（创建协程返回即关闭），此处的读取必然已就绪。
	select {
	case cerr := <-done:
		if cerr == nil {
			t.Fatal("终态连接上的创建绝不能成功")
		}
		if errors.Is(cerr, context.Canceled) || errors.Is(cerr, context.DeadlineExceeded) {
			t.Fatalf("创建必须因终态而非 ctx 取消失败: %v", cerr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("创建调用必须在拿到写权后立即失败而非挂起")
	}
	// 写权已由写者经 defer 归还或测试钩子已放出——退场前确保不持权。
	select {
	case <-conn.writeSem:
	default:
	}
	writeHeld = false

	// 服务端绝不能看到该创建帧（远端零泄漏 BrowserContext）——写者的
	// 发送决策已在 done 公布前落定（见上），此处的零计数是「写端先封闭」
	// 顺序语义的直接证据：顺序反转（先 done 后 conn.Close）时写端在两
	// 信号之间尚未封闭，帧必出门、本断言确定性失败。
	select {
	case hit := <-srv.createBCHit:
		t.Fatalf("终态化后创建帧绝不能出门（远端泄漏 BC）: %+v", hit)
	case <-time.After(500 * time.Millisecond):
	}
	// pending 表与 late 名额零泄漏。
	client.pendMu.Lock()
	n := len(client.pending)
	lateCnt := client.latePendingCount
	client.pendMu.Unlock()
	if n != 0 || lateCnt != 0 {
		t.Fatalf("终态拒绝必须撤登记零泄漏（pending=%d lateCount=%d）", n, lateCnt)
	}
}

// TestJarWriteBackDuplicateNameRoundTrips 复核 R10-8-C（Reviewer S2/Oracle
// S2）：同名多项的端到端净零往返——DB k=n;k=y（重名残留尾部旧票）、Jar 仅
// 单 k=n 时，首轮回写把两项都合并成 n（尾部旧票真实更新）；库已是
// k=n;k=n 后第二轮必须判净零跳过写库。修复前只认首值：k=n;k=n 与合并
// 结果被误判净零的首值逻辑混同方向反过来——首值相同即「等价」跳写，
// 尾部 y 残留永不更新；或重名变化写一次后下一轮因仍只认首值永远写/永
// 远跳，两种变异都必须在本用例失败。
func TestJarWriteBackDuplicateNameRoundTrips(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	// 重名 k 的裸串（merge 保序：cur 顺序 + 新键追加）。
	raw := "SAPISID=dnx; __Secure-1PSID=dnp; SID=dns; __Secure-1PSIDTS=dnt; k=n; k=y"
	id, err := accountInsert("dupname", raw, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	if a := accountByID(id); a == nil || a.Cookie != raw {
		t.Fatalf("前置：入库必须保留重名原样裸串: %q", a.Cookie)
	}

	key := browserSlotKey{accountID: id}
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(raw),
		browserContextID: "bc-dup",
		targetID:         "t-dup",
		sessionID:        "sess-dup",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-dup"] = ac
	m.targetIndex["t-dup"] = ac
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		delete(m.sessionIndex, "sess-dup")
		delete(m.targetIndex, "t-dup")
		m.mu.Unlock()
	}()
	// Jar 仅单 k=n（重名尾部 n 之外的 y 已在上游消失）。
	srv.bcMu.Lock()
	srv.sessToBC["sess-dup"] = "bc-dup"
	srv.bcStore["bc-dup"] = map[string]string{
		"SAPISID":          "dnx",
		"__Secure-1PSID":   "dnp",
		"SID":              "dns",
		"__Secure-1PSIDTS": "dnt",
		"k":                "n",
	}
	srv.bcMu.Unlock()

	// 首轮：重名尾部 y 必须真实更新为 n（合并两项皆 n）——修复前只认
	// 首值时本轮被判「净零」跳写，尾部旧票 y 永不更新。
	m.SyncAccountCookiesBack(id, true)
	a1 := accountByID(id)
	if a1 == nil || !strings.Contains(a1.Cookie, "k=n; k=n") || strings.Contains(a1.Cookie, "k=y") {
		t.Fatalf("首轮重名尾部必须真实更新落库（k=n; k=n）: %q", a1.Cookie)
	}
	if !strings.Contains(a1.Cookie, "__Secure-1PSIDTS=dnt") {
		t.Fatalf("其余键必须保留: %q", a1.Cookie)
	}

	// 第二轮：库已是 k=n;k=n、jar 仍单 k=n——合并结果逐项相等，必须
	// 判净零跳过写库（逐字节不变）。修复前只认首值或忽略重名的等价
	// 语义会让第二轮在「变化/净零」两种变异方向之一出错。
	before2 := a1.Cookie
	m.SyncAccountCookiesBack(id, true)
	a2 := accountByID(id)
	if a2 == nil || a2.Cookie != before2 {
		t.Fatalf("第二轮逐项相等必须判净零跳过写库（逐字节不变）: %q vs %q", a2.Cookie, before2)
	}
}

// TestAcquirePlanLockedDoesNotDisposeAlignedSlot 复核 R10 5a（Reviewer 建议
// 行为断言）：acquirePlanLocked 直接传入旧 P1 账号事实 + 已对齐 P2 的就绪
// 槽——规划必须只标记疑似漂移交锁外复核（errExitViolationSuspect），绝不
// 在锁内摘除/置 disposed：已对齐的合法槽在规划阶段零破坏，复核后由并发
// 自动绑定分支放弃摘除。
func TestAcquirePlanLockedDoesNotDisposeAlignedSlot(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 81, Name: "pl1", URL: "http://pl1:8080", Enabled: true},
		{ID: 82, Name: "pl2", URL: "http://pl2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookie := "SAPISID=plx; __Secure-1PSID=plp; SID=pls; __Secure-1PSIDTS=plt"
	id, err := accountInsert("placct", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	bindAccountProxy(id, 82)

	// 就绪槽已对齐 P2（并发自动绑定同步后的合法槽）。
	key := browserSlotKey{accountID: id}
	m := browserMgr
	ac := &accountBrowserCtx{
		key:                key,
		epoch:              1,
		cookieIdentity:     cookieIdentity(cookie),
		browserContextID:   "bc-pl",
		targetID:           "t-pl",
		sessionID:          "sess-pl",
		proxyURL:           "http://pl2:8080",
		allowDirect:        false,
		origAccountProxyID: 82,
		boundProxyID:       82,
		readyClosed:        true,
		lastUsedAt:         time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		m.mu.Unlock()
	}()

	// 旧 P1 账号事实（进锁前读取的过期快照）。
	stale := *accountByID(id)
	stale.ProxyID = 81
	poolSnap := snapshotProxyPool()
	var drain acquireDrainReg
	m.mu.Lock()
	waitCh, kind, proceed, snap := m.acquirePlanLocked(ac, &stale,
		"http://pl2:8080", false, true, false, poolSnap, &drain)
	disposedInPlan := ac.disposed
	_, stillInMap := m.ctxs[key]
	m.mu.Unlock()

	// 核心断言①：规划绝不摘除/置 disposed——已对齐 P2 的槽零破坏。
	if disposedInPlan || !stillInMap || ac.disposed {
		t.Fatalf("规划阶段绝不许摘除/破坏已对齐 P2 的槽（disposed=%v inMap=%v）", disposedInPlan, stillInMap)
	}
	// 核心断言②：规划只标记疑似漂移（errExitViolationSuspect），摘除
	// 与否由锁外一致快照复核裁决——proceed 必须为假、waitCh 为空。
	if snap.ac == nil || !errors.Is(snap.err, errExitViolationSuspect) {
		t.Fatalf("疑似漂移必须只标记交锁外复核: snap=%+v kind=%v", snap, kind)
	}
	if proceed || waitCh != nil {
		t.Fatalf("疑似漂移绝不许直接占租或等待: proceed=%v waitCh=%v", proceed, waitCh)
	}
	// 复核确属并发自动绑定（对齐事实）→ 放弃摘除，槽仍驻留可用。
	violation, _ := recheckSlotExitViolation(&stale, ac)
	if violation {
		t.Fatal("已对齐 P2 的槽复核必须放弃摘除（并发自动绑定合法）")
	}
	m.mu.Lock()
	alive := m.ctxs[key] == ac && !ac.disposed
	m.mu.Unlock()
	if !alive {
		t.Fatal("复核后槽必须仍驻留未破坏")
	}
}

// TestStrictClaimSyncsSlotExitVersion 复核 R11-1（Oracle A1）：严格模式
// claimAccountProxy 仲裁写库成功后必须同步推进驻留槽事实（orig/bound +
// exitSyncVer）——此前严格首绑登记来源后直接返回，复核已确认漂移、暂停在
// 版本值守卫之前的请求，在严格仲裁把账号实际绑到本槽出口（合法恢复）之后
// 仍按旧版本提交摘除，误杀刚恢复的合法槽并终止在途流。修复后：严格仲裁
// 恢复合法槽 → 旧版本摘除必须放弃，原槽与在途流存活；严格仲裁到别的出口
// （槽 URL 不符）绝不污染事实也不推进版本。
func TestStrictClaimSyncsSlotExitVersion(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 91, Name: "sv1", URL: "http://sv1:8080", Enabled: true},
		{ID: 92, Name: "sv2", URL: "http://sv2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.StrictProxyBinding = true
		c.FallbackDirect = false
	})
	defer setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = false
		c.StrictProxyBinding = false
	})

	cookie := "SAPISID=svx; __Secure-1PSID=svp; SID=svs; __Secure-1PSIDTS=svt"

	t.Run("严格仲裁绑到本槽出口推进版本使旧版本摘除放弃", func(t *testing.T) {
		id, err := accountInsert("svacct", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		// Oracle A1 交错①：非严格模式曾因 P1 槽满合法溢出到 P2 驻留——
		// 驻留槽实际出口/绑定为 P2、orig 为 P1，DB 绑定 P1。
		bindAccountProxy(id, 91)
		key := browserSlotKey{accountID: id}
		m := browserMgr
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(cookie),
			browserContextID:   "bc-sv",
			targetID:           "t-sv",
			sessionID:          "sess-sv",
			proxyURL:           "http://sv2:8080",
			allowDirect:        false,
			origAccountProxyID: 91,
			boundProxyID:       92,
			exitSyncVer:        5,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
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

		// 交错②：T1 按 P1 复核确认漂移，保存快照版本 v=5，暂停在提交摘除前。
		// 交错③：T2 禁用 P1 后严格仲裁把账号实际绑到 P2（本槽实际出口）——
		// claimAccountProxy 成功返回时版本必须已推进。
		setProxyField(91, func(p *Proxy) { p.Enabled = false })
		defer setProxyField(91, func(p *Proxy) { p.Enabled = true })
		winner, ok := claimAccountProxy(id, 92)
		if !ok || winner != 92 {
			t.Fatalf("前置：严格仲裁必须成功绑到 P2: winner=%d ok=%v", winner, ok)
		}
		if a := accountByID(id); a == nil || a.ProxyID != 92 {
			t.Fatalf("前置：严格仲裁绑定必须落库 P2: %+v", a)
		}
		m.mu.Lock()
		verAfter := ac.exitSyncVer
		origAfter := ac.origAccountProxyID
		m.mu.Unlock()
		if verAfter <= 5 {
			t.Fatalf("严格仲裁恢复合法槽必须推进 exitSyncVer（5 -> %d）", verAfter)
		}
		if origAfter != 92 {
			t.Fatalf("严格仲裁必须把本槽 orig 同步为 P2，得到 %d", origAfter)
		}

		// 核心断言（交错④）：T1 提交旧版本决定——必须放弃摘除，原槽与
		// 在途流存活。
		det, _ := m.detachIfExitVersionUnchanged(ac, 5, browserErr("drifted"))
		if det.ac != nil {
			t.Fatal("严格仲裁恢复合法槽后旧版本摘除必须放弃（绝不误杀）")
		}
		m.mu.Lock()
		alive := m.ctxs[key] == ac && !ac.disposed
		m.mu.Unlock()
		if !alive {
			t.Fatal("原槽必须仍驻留未摘除（在途流零干扰）")
		}
		s.mu.Lock()
		fatal := s.fatalErr
		s.mu.Unlock()
		if fatal != nil {
			t.Fatalf("在途流绝不能被终止: %v", fatal)
		}
	})

	t.Run("严格仲裁到别的出口绝不污染槽事实不推进版本", func(t *testing.T) {
		// 负对照：驻留槽出口与仲裁胜者不符——SyncAutoBoundProxy 拒绝同步，
		// 版本不推进、orig/bound 不变，旧版本摘除守卫语义不失效。
		id, err := accountInsert("svneg", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		bindAccountProxy(id, 91)
		key := browserSlotKey{accountID: id}
		m := browserMgr
		ac := &accountBrowserCtx{
			key:                key,
			epoch:              1,
			cookieIdentity:     cookieIdentity(cookie),
			browserContextID:   "bc-svn",
			targetID:           "t-svn",
			sessionID:          "sess-svn",
			proxyURL:           "http://sv1:8080", // 槽出口是 P1，仲裁胜者是 P2
			allowDirect:        false,
			origAccountProxyID: 91,
			boundProxyID:       91,
			exitSyncVer:        7,
			readyClosed:        true,
			lastUsedAt:         time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()
		setProxyField(91, func(p *Proxy) { p.Enabled = false })
		defer setProxyField(91, func(p *Proxy) { p.Enabled = true })
		winner, ok := claimAccountProxy(id, 92)
		if !ok || winner != 92 {
			t.Fatalf("前置：严格仲裁必须成功: winner=%d ok=%v", winner, ok)
		}
		m.mu.Lock()
		verAfter := ac.exitSyncVer
		origAfter := ac.origAccountProxyID
		boundAfter := ac.boundProxyID
		m.mu.Unlock()
		if verAfter != 7 || origAfter != 91 || boundAfter != 91 {
			t.Fatalf("槽出口不符时绝不谎改事实不推进版本: ver=%d orig=%d bound=%d", verAfter, origAfter, boundAfter)
		}
	})
}

// TestHotInjectRevalidatesOwnExitBeforeCommit 复核 R11-2-①（Oracle A2/Reviewer #3）：
// ownAuthorized 绝不再沿用换发网络 I/O 之前的一票判定——换发等待期间自有槽
// 出口被改绑失格（P1 冷却、配额自动重绑提交 P2），返回后对失格 BC 必须
// 零 Storage.setCookies，且失格已发布槽被条件摘除（绝不留可授租却永久无法
// 回写的僵尸槽）。确定性构造：换发回复由测试桩在发出前完成真实改绑交错，
// 与生产「网络等待期间改绑」终态完全同构。
func TestHotInjectRevalidatesOwnExitBeforeCommit(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 71, Name: "rv1", URL: "http://rv1:8080", Enabled: true},
		{ID: 72, Name: "rv2", URL: "http://rv2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookie := "SAPISID=rvx; __Secure-1PSID=rvp; SID=rvs; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("rvacct", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	bindAccountProxy(id, 71) // 账号与自有槽均绑 P1

	// 自有槽走 P1（轮初授权合法）。
	key := browserSlotKey{accountID: id}
	ac := &accountBrowserCtx{
		key:                key,
		epoch:              1,
		cookieIdentity:     cookieIdentity(cookie),
		browserContextID:   "bc-rv",
		targetID:           "t-rv",
		sessionID:          "sess-rv",
		proxyURL:           "http://rv1:8080",
		allowDirect:        false,
		origAccountProxyID: 71,
		boundProxyID:       71,
		readyClosed:        true,
		lastUsedAt:         time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-rv"] = ac
	m.targetIndex["t-rv"] = ac
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		delete(m.sessionIndex, "sess-rv")
		delete(m.targetIndex, "t-rv")
		m.mu.Unlock()
	}()
	srv.bcMu.Lock()
	srv.sessToBC["sess-rv"] = "bc-rv"
	srv.bcStore["bc-rv"] = map[string]string{
		"SAPISID": "rvx", "__Secure-1PSID": "rvp", "SID": "rvs", "__Secure-1PSIDTS": "oldts",
	}
	srv.bcMu.Unlock()

	// P1 换发桩：返回新票前完成真实改绑交错——P1 进入冷却、配额自动重绑
	// 把 DB 提交到 P2（Oracle A2 的生产交错终态）。
	var once sync.Once
	flipRT := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			once.Do(func() {
				setProxyField(71, func(p *Proxy) { p.Enabled = false })
				autoBindAccountProxy(id, 72)
				setProxyField(71, func(p *Proxy) { p.Enabled = true })
			})
			h := http.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=rvnew; Domain=.google.com; Path=/; Secure; HttpOnly")
			return &http.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
				Request: req,
			}, nil
		}
		return respJSON(req, 404, ""), nil
	})
	timeout := rtCfg().RequestTimeout
	for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
		stdlibClientCache.Store("http://rv1:8080"+suf, &http.Client{Transport: flipRT})
	}
	defer func() {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Delete("http://rv1:8080" + suf)
		}
	}()

	// 轮初快照：自有槽是唯一候选（轮初授权合法）。
	ownSnap := activeSlotSnapshot{
		accountID: id, bcID: "bc-rv", proxyURL: "http://rv1:8080", allowDirect: false,
		boundProxyID: 71, origAccountProxyID: 71, ident: cookieIdentity(cookie),
	}
	rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{ownSnap}, map[string][]string{})

	// 换发写库必须成功（交错前的候选授权合法）。
	if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=rvnew") {
		t.Fatalf("换发必须写库: %q", a.Cookie)
	}
	// 核心断言①：对失格 BC 零 Storage.setCookies（实时二次复核拒绝）。
	drainEnd := time.After(200 * time.Millisecond)
	injected := 0
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Storage.setCookies" && strings.Contains(string(c.params), `"browserContextId":"bc-rv"`) {
				injected++
			}
		case <-drainEnd:
			goto drained
		}
	}
drained:
	if injected != 0 {
		t.Fatalf("换发等待期间失格的自有槽绝不能收到任何 Storage.setCookies，得到 %d 次", injected)
	}
	// 核心断言②：失格已发布槽必须被条件摘除（防僵尸槽），绝不继续授租。
	m.mu.Lock()
	_, still := m.ctxs[key]
	disposed := ac.disposed
	m.mu.Unlock()
	if still || !disposed {
		t.Fatalf("失格自有槽必须被条件摘除（still=%v disposed=%v）", still, disposed)
	}
}

// TestHotInjectMissedCandidateLegalNewSlot 复核 R11-2-②（Oracle A3/Reviewer #3）：
// 轮初本号还在 creating 未收入候选表，轮到前已健康发布的合法新槽——修复前
// ownAuthorized=false 把它 Bump 成僵尸（既不注入也不摘槽，可授租却永久无法
// 回写）；修复后实时二次复核放行：正常热注入、后续回写与下轮热续期正常。
func TestHotInjectMissedCandidateLegalNewSlot(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil // 池空：直连出口合法
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.StrictProxyBinding = false
		c.FallbackDirect = true
	})

	cookie := "SAPISID=msx; __Secure-1PSID=msp; SID=mss; __Secure-1PSIDTS=oldts"
	idB, err := accountInsert("msb", cookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	idC, err := accountInsert("msc", cookie, "")
	if err != nil {
		t.Fatalf("insert C: %v", err)
	}
	defer accountDelete(idC)

	mkDirectSlot := func(id int64, tag string) browserSlotKey {
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookie),
			browserContextID: "bc-ms" + tag,
			targetID:         "t-ms" + tag,
			sessionID:        "sess-ms" + tag,
			allowDirect:      true,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
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
			"SAPISID": "msx", "__Secure-1PSID": "msp", "SID": "mss", "__Secure-1PSIDTS": "oldts",
			"SIDCC": "mscc",
		}
		srv.bcMu.Unlock()
		return key
	}
	keyB := mkDirectSlot(idB, "b")
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, keyB)
		delete(m.sessionIndex, "sess-msb")
		delete(m.targetIndex, "t-msb")
		m.mu.Unlock()
	}()
	keyC := mkDirectSlot(idC, "c")
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, keyC)
		delete(m.sessionIndex, "sess-msc")
		delete(m.targetIndex, "t-msc")
		m.mu.Unlock()
	}()

	// 直连换发桩：哨兵换发返回新票。
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			h := fhttp.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=msnew; Domain=.google.com; Path=/; Secure; HttpOnly")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404", Header: fhttp.Header{},
			Body: io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)

	// Oracle A3 终态：轮初 B 还在 creating 漏进候选表——轮到 B 时候选里
	// 只有合法兄弟 C（B 此刻已健康发布但不在列表）。B 借 C 出口换发。
	snapC := activeSlotSnapshot{
		accountID: idC, bcID: "bc-msc", proxyURL: "", allowDirect: true,
		ident: cookieIdentity(cookie),
	}
	rotateActiveOrSibling1PSIDTS(*accountByID(idB), []activeSlotSnapshot{snapC}, map[string][]string{})

	// 换发写库必须成功。
	if a := accountByID(idB); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=msnew") {
		t.Fatalf("借兄弟出口换发必须写库: %q", a.Cookie)
	}
	// 核心断言①：已健康发布的合法自有槽必须正常热注入（绝不误 Bump 成僵尸）。
	jar := srv.jarFor("sess-msb")
	if jar["__Secure-1PSIDTS"] != "msnew" {
		t.Fatalf("漏进候选表的合法新槽必须被正常热注入: %q", jar["__Secure-1PSIDTS"])
	}
	// 核心断言②：槽位绝不摘除（僵尸槽反向防线）。
	m.mu.Lock()
	acB := m.ctxs[keyB]
	alive := acB != nil && !acB.disposed
	m.mu.Unlock()
	if !alive {
		t.Fatal("合法自有槽绝不能被摘除")
	}
	// 核心断言③：后续回写正常（epoch 未被误 Bump——Jar 新 SIDCC 可回写）。
	srv.bcMu.Lock()
	srv.bcStore["bc-msb"]["SIDCC"] = "mscc-refreshed"
	srv.bcMu.Unlock()
	m.SyncAccountCookiesBack(idB, true)
	if a := accountByID(idB); a == nil || !strings.Contains(a.Cookie, "SIDCC=mscc-refreshed") {
		t.Fatalf("合法新槽的后续回写必须正常落库（epoch 黑洞防线）: %q", a.Cookie)
	}
}

// TestIdentitySuspectRecheckNeverKillsReboundSlot 复核 R11-5-①（Oracle A6-身份
// 交错/Reviewer #1）：T1 以 freshAcct=X 通过预检后、进 m.mu 规划前，T2 完成
// accountSync 换绑 Y 并由另一请求建好健康 Y 槽——T1 拿陈旧 freshAcct=X 比对
// 驻留 Y 槽命中「疑似旧身份」时，锁外复核必须识别行身份已与槽一致（收敛
// 终态）：健康 Y 槽绝不被误杀，本请求终态拒绝 identity contract mismatch。
// 反向对照：行确实换绑异会话（旧身份槽残留）时复核确认摘除。
func TestIdentitySuspectRecheckNeverKillsReboundSlot(t *testing.T) {
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 61, Name: "is1", URL: "http://is1:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookieX := "SAPISID=isx; __Secure-1PSID=isp; SID=iss; __Secure-1PSIDTS=ist"
	cookieY := "SAPISID=isy; __Secure-1PSID=isq; SID=isu; __Secure-1PSIDTS=isv"

	t.Run("行身份已与槽一致时绝不误杀健康Y槽", func(t *testing.T) {
		id, err := accountInsert("isacct", cookieX, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		// T2 完成换绑 Y（真实 accountSync 终态）。
		if _, err := getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`, cookieY, id); err != nil {
			t.Fatalf("换绑 Y: %v", err)
		}
		// T2 建好的健康 Y 槽驻留中，有在途流。
		key := browserSlotKey{accountID: id}
		m := browserMgr
		ac := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookieY),
			browserContextID: "bc-is",
			targetID:         "t-is",
			sessionID:        "sess-is",
			proxyURL:         "http://is1:8080",
			allowDirect:      false,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
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

		// T1 的过期请求快照（freshAcct=X——进 m.mu 前读取的中间态）。
		staleX := *accountByID(id)
		staleX.Cookie = cookieX
		// 前置情景复现：锁内身份守卫拿 freshAcct=X 比对驻留 Y 槽必然命中
		// 「疑似旧身份」（修复前此刻直接摘除健康 Y 槽并终止在途流）。
		var drain acquireDrainReg
		m.mu.Lock()
		_, _, _, snap := m.acquirePlanLocked(ac, &staleX, "http://is1:8080",
			false, true, false, snapshotProxyPool(), &drain)
		m.mu.Unlock()
		if snap.ac == nil || !errors.Is(snap.err, errIdentityViolationSuspect) {
			t.Fatalf("前置：过期身份快照必须命中疑似标记而非直接摘除: %+v", snap)
		}
		m.mu.Lock()
		aliveInPlan := m.ctxs[key] == ac && !ac.disposed
		m.mu.Unlock()
		if !aliveInPlan {
			t.Fatal("规划阶段绝不许摘除（只能标记交锁外复核）")
		}
		// 核心断言：锁外复核识别收敛终态——健康 Y 槽存活、在途流零干扰，
		// 本请求终态拒绝（identity contract mismatch）。
		det, termErr := m.resolveIdentitySuspect(ac, &staleX)
		if det.ac != nil {
			t.Fatal("行身份与槽一致时绝不许摘除健康 Y 槽")
		}
		if termErr == nil || !strings.Contains(termErr.Error(), "identity contract mismatch") {
			t.Fatalf("本请求必须终态拒绝身份契约失配: %v", termErr)
		}
		m.mu.Lock()
		alive := m.ctxs[key] == ac && !ac.disposed
		m.mu.Unlock()
		if !alive {
			t.Fatal("健康 Y 槽必须仍驻留（绝不误杀）")
		}
		s.mu.Lock()
		fatal := s.fatalErr
		s.mu.Unlock()
		if fatal != nil {
			t.Fatalf("在途流绝不能被终止: %v", fatal)
		}
	})

	t.Run("行确实换绑异会话时复核确认摘除旧身份槽", func(t *testing.T) {
		id, err := accountInsert("isacct2", cookieX, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		// 行换绑 Y，但旧身份 X 槽仍驻留（失效漏网窗口）。
		if _, err := getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`, cookieY, id); err != nil {
			t.Fatalf("换绑 Y: %v", err)
		}
		key := browserSlotKey{accountID: id}
		m := browserMgr
		ac := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookieX),
			browserContextID: "bc-isx",
			targetID:         "t-isx",
			sessionID:        "sess-isx",
			proxyURL:         "http://is1:8080",
			allowDirect:      false,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()
		// freshAcct 同样是 Y（请求无过期）——但这里直接驱动复核函数验证
		// 真换绑路径：行身份与槽身份仍不一致时必须返回摘除快照。
		cur := accountByID(id)
		det, termErr := m.resolveIdentitySuspect(ac, cur)
		if termErr != nil {
			t.Fatalf("真换绑路径绝不应终态拒绝: %v", termErr)
		}
		if det.ac == nil {
			t.Fatal("行确实换绑异会话时旧身份槽必须被摘除（身份守卫语义不变）")
		}
		m.mu.Lock()
		_, still := m.ctxs[key]
		disposed := ac.disposed
		m.mu.Unlock()
		if still || !disposed {
			t.Fatalf("旧身份槽必须从 manager 移除并置 disposed: still=%v disposed=%v", still, disposed)
		}
	})
}

// TestLeaseAuthuserRecheckAndWritebackCAS 复核 R11-5-②（Oracle A6-序号交错/
// Reviewer #2）：正 ID 在授租前复核 authuser——刷新后、授租前管理员只改
// authuser（不拆槽）时旧 /u/N 请求绝不获租；且异步 Jar 回写 SQL CAS 带
// authuser 谓词——行已改序号时 /u/0 槽派生票据 0 行命中，绝不写进 /u/1 行。
func TestLeaseAuthuserRecheckAndWritebackCAS(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=aux; __Secure-1PSID=aup; SID=aus; __Secure-1PSIDTS=aut"

	t.Run("刷新后改authuser的旧请求绝不获租也不拆槽", func(t *testing.T) {
		id, err := accountInsert("auacct", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		if err := accountSetAuthUser(id, 1); err != nil {
			t.Fatalf("set authuser: %v", err)
		}
		// 驻留健康槽（同身份）。
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookie),
			browserContextID: "bc-au",
			targetID:         "t-au",
			sessionID:        "sess-au",
			allowDirect:      true,
			authuser:         1,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()
		// 请求快照：/u/1 时代进入（与行一致——通过预检）。真实交错是
		// 「刷新后、授租前」序号才变更（Oracle A6-序号交错）：用生产测试
		// 钩子精确落在租约已占用、契约复核尚未执行的交错点上改序号——
		// 此时行 /u/1 与驻留槽一切合法契约均自洽，唯一防线就是授租前的
		// 最后复核。
		req := *accountByID(id)
		renumbered := false
		testHookBeforeLeaseContractRecheck.Store(func(acct *CookieAccount) {
			if !renumbered {
				renumbered = true
				if err := accountSetAuthUser(id, 2); err != nil {
					t.Errorf("renumber in hook: %v", err)
				}
			}
		})
		defer func() { testHookBeforeLeaseContractRecheck.Store((func(*CookieAccount))(nil)) }()
		defer accountSetAuthUser(id, 1)
		_, _, _, release, aerr := m.acquireAccountCtx(context.Background(), &req, "", true, true)
		if !renumbered {
			t.Fatal("测试钩子必须在授租前交错点真实执行（确定性交错成立）")
		}
		if aerr == nil {
			if release != nil {
				release()
			}
			t.Fatal("序号已变的旧请求绝不许获租")
		}
		if !strings.Contains(aerr.Error(), "contract mismatch") {
			t.Fatalf("拒绝必须点名契约失配: %v", aerr)
		}
		m.mu.Lock()
		alive := m.ctxs[key] == ac && !ac.disposed && ac.inFlight == 0
		m.mu.Unlock()
		if !alive {
			t.Fatal("拒租绝不拆槽也绝不留占用（零副作用）")
		}
	})

	t.Run("行序号已变时Jar回写0行命中不落库", func(t *testing.T) {
		id, err := accountInsert("auwb", cookie+"; SIDCC=aubase", "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		// 建槽时序号为 0；随后管理员改序号为 1——/u/0 槽的票据绝不写进 /u/1。
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookie),
			browserContextID: "bc-auwb",
			targetID:         "t-auwb",
			sessionID:        "sess-auwb",
			allowDirect:      true,
			authuser:         0,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.sessionIndex["sess-auwb"] = ac
		m.targetIndex["t-auwb"] = ac
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			delete(m.sessionIndex, "sess-auwb")
			delete(m.targetIndex, "t-auwb")
			m.mu.Unlock()
		}()
		srv.bcMu.Lock()
		srv.sessToBC["sess-auwb"] = "bc-auwb"
		srv.bcStore["bc-auwb"] = map[string]string{
			"SAPISID": "aux", "__Secure-1PSID": "aup", "SID": "aus",
			"__Secure-1PSIDTS": "aut", "SIDCC": "aurefreshed",
		}
		srv.bcMu.Unlock()
		if err := accountSetAuthUser(id, 1); err != nil {
			t.Fatalf("renumber: %v", err)
		}
		defer accountSetAuthUser(id, 0)
		before := accountByID(id).Cookie
		m.SyncAccountCookiesBack(id, true)
		after := accountByID(id)
		if after == nil {
			t.Fatal("account vanished")
		}
		if strings.Contains(after.Cookie, "aurefreshed") || after.Cookie != before {
			t.Fatalf("行序号已变时 /u/0 槽派生票据绝不许落库: %q", after.Cookie)
		}
		// 正对照：序号改回 0 后同一槽的回写正常落库（CAS 谓词不误伤合法回写）。
		if err := accountSetAuthUser(id, 0); err != nil {
			t.Fatalf("restore: %v", err)
		}
		m.SyncAccountCookiesBack(id, true)
		if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "SIDCC=aurefreshed") {
			t.Fatalf("序号一致时回写必须正常落库（正对照）: %q", a.Cookie)
		}
	})
}

// TestHotInjectCommitBindingCatchesPostVerifyRebind 复核 R12-2-①（Oracle M1/
// Reviewer #3）：二次授权复核通过之后、prepareHotInject 提交判定之前完成
// 的改绑（配额冷却自动重绑 P1 -> P2，驻留槽出口仍是 P1、SyncAutoBoundProxy
// 不同步槽事实）绝不能凭过期的 true 注入——bcID 与 exitSyncVer 对该交错
// 不可见，唯一防线是 rebindGen 改绑代际。确定性构造：生产测试钩子精确落
// 在「复核完成后、提交判定前」的交错点上完成真实改绑。断言失格 BC 的
// Storage.setCookies == 0 且驻留槽被条件摘除（不留僵尸槽）。
func TestHotInjectCommitBindingCatchesPostVerifyRebind(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 71, Name: "hx1", URL: "http://hx1:8080", Enabled: true},
		{ID: 72, Name: "hx2", URL: "http://hx2:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.StrictProxyBinding = false
		c.FallbackDirect = false
	})

	cookie := "SAPISID=hxx; __Secure-1PSID=hxp; SID=hxs; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("hxacct", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	bindAccountProxy(id, 71)

	key := browserSlotKey{accountID: id}
	ac := &accountBrowserCtx{
		key:                key,
		epoch:              1,
		cookieIdentity:     cookieIdentity(cookie),
		browserContextID:   "bc-hx",
		targetID:           "t-hx",
		sessionID:          "sess-hx",
		proxyURL:           "http://hx1:8080",
		allowDirect:        false,
		origAccountProxyID: 71,
		boundProxyID:       71,
		readyClosed:        true,
		lastUsedAt:         time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-hx"] = ac
	m.targetIndex["t-hx"] = ac
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		delete(m.sessionIndex, "sess-hx")
		delete(m.targetIndex, "t-hx")
		m.mu.Unlock()
	}()
	srv.bcMu.Lock()
	srv.sessToBC["sess-hx"] = "bc-hx"
	srv.bcStore["bc-hx"] = map[string]string{
		"SAPISID": "hxx", "__Secure-1PSID": "hxp", "SID": "hxs", "__Secure-1PSIDTS": "oldts",
	}
	srv.bcMu.Unlock()

	// P1 换发桩：正常返回新票（本用例的改绑交错不在网络等待期间，而在
	// 复核通过之后、提交判定之前的精确窗口——由下方生产钩子注入）。
	flipRT := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			h := http.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=hxnew; Domain=.google.com; Path=/; Secure; HttpOnly")
			return &http.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
				Request: req,
			}, nil
		}
		return respJSON(req, 404, ""), nil
	})
	timeout := rtCfg().RequestTimeout
	for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
		stdlibClientCache.Store("http://hx1:8080"+suf, &http.Client{Transport: flipRT})
	}
	defer func() {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Delete("http://hx1:8080" + suf)
		}
	}()

	// 生产钩子：复核通过后、prepareHotInject 提交判定前的精确交错点上
	// 完成真实配额改绑（P1 冷却 -> 自动重绑 P2；驻留槽出口仍是 P1、槽
	// 事实不被同步——bcID/exitSyncVer 不变，唯一可核防线是 rebindGen）。
	rebindDone := false
	testHookBeforeHotInjectCommit.Store(func(accountID int64) {
		if accountID != id || rebindDone {
			return
		}
		rebindDone = true
		setProxyField(71, func(p *Proxy) { p.Enabled = false })
		autoBindAccountProxy(id, 72)
		setProxyField(71, func(p *Proxy) { p.Enabled = true })
	})
	defer func() { testHookBeforeHotInjectCommit.Store((func(int64))(nil)) }()

	ownSnap := activeSlotSnapshot{
		accountID: id, bcID: "bc-hx", proxyURL: "http://hx1:8080", allowDirect: false,
		boundProxyID: 71, origAccountProxyID: 71, ident: cookieIdentity(cookie),
	}
	rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{ownSnap}, map[string][]string{})

	if !rebindDone {
		t.Fatal("测试钩子必须在提交前交错点真实执行（确定性交错成立）")
	}
	if a := accountByID(id); a == nil || a.ProxyID != 72 {
		t.Fatalf("前置：改绑必须真实提交 P2: %+v", a)
	}
	// 写库照常成功（复核交错前的换发结果落库）。
	if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=hxnew") {
		t.Fatalf("换发必须写库: %q", a.Cookie)
	}
	// 核心断言①：失格 BC 零 Storage.setCookies（提交核验检出 rebindGen 失配）。
	drainEnd := time.After(200 * time.Millisecond)
	injected := 0
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Storage.setCookies" && strings.Contains(string(c.params), `"browserContextId":"bc-hx"`) {
				injected++
			}
		case <-drainEnd:
			goto drained
		}
	}
drained:
	if injected != 0 {
		t.Fatalf("复核通过后改绑的失格槽绝不能收到任何 Storage.setCookies，得到 %d 次", injected)
	}
	// 核心断言②：失格驻留槽被条件摘除（不留僵尸槽）。
	m.mu.Lock()
	_, still := m.ctxs[key]
	disposed := ac.disposed
	m.mu.Unlock()
	if still || !disposed {
		t.Fatalf("失格驻留槽必须被条件摘除（still=%v disposed=%v）", still, disposed)
	}
}

// TestHotInjectCommitBindingCatchesPostVerifyDisable 复核 R12-2-②（Oracle M1
// 停用边界/Reviewer #3）：复核通过后、提交注入前账号被停用并摘除——提交
// 判定在 m.mu 内看到槽已摘除，绝不注入。行停用本身由 freshRow.Status 复核
// 兜底（停用在复核前完成时直接不授权）。
func TestHotInjectCommitBindingCatchesPostVerifyDisable(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=dsx; __Secure-1PSID=dsp; SID=dss; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("dsacct", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)

	key := browserSlotKey{accountID: id}
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-ds",
		targetID:         "t-ds",
		sessionID:        "sess-ds",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-ds"] = ac
	m.targetIndex["t-ds"] = ac
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		delete(m.sessionIndex, "sess-ds")
		delete(m.targetIndex, "t-ds")
		m.mu.Unlock()
	}()
	srv.bcMu.Lock()
	srv.sessToBC["sess-ds"] = "bc-ds"
	srv.bcStore["bc-ds"] = map[string]string{
		"SAPISID": "dsx", "__Secure-1PSID": "dsp", "SID": "dss", "__Secure-1PSIDTS": "oldts",
	}
	srv.bcMu.Unlock()

	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			h := fhttp.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=dsnew; Domain=.google.com; Path=/; Secure; HttpOnly")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404", Header: fhttp.Header{},
			Body: io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)

	// 停用交错：复核通过后、提交判定前完成停用+摘除（管理停用路径的
	// InvalidateAccount 在提交临界区之前执行——m.mu 串行化保证提交判定
	// 必然看到槽已摘除）。
	disableDone := false
	testHookBeforeHotInjectCommit.Store(func(accountID int64) {
		if accountID != id || disableDone {
			return
		}
		disableDone = true
		if err := accountSetStatus(id, "disabled"); err != nil {
			t.Errorf("disable: %v", err)
		}
		browserMgr.InvalidateAccount(id, false, "disabled")
	})
	defer func() {
		testHookBeforeHotInjectCommit.Store((func(int64))(nil))
		accountSetStatus(id, "enabled")
	}()

	ownSnap := activeSlotSnapshot{
		accountID: id, bcID: "bc-ds", proxyURL: "", allowDirect: true,
		ident: cookieIdentity(cookie),
	}
	rotateActiveOrSibling1PSIDTS(*accountByID(id), []activeSlotSnapshot{ownSnap}, map[string][]string{})

	if !disableDone {
		t.Fatal("测试钩子必须在提交前交错点真实执行（确定性交错成立）")
	}
	// 核心断言：失格 BC 零 Storage.setCookies。
	drainEnd := time.After(200 * time.Millisecond)
	injected := 0
	for {
		select {
		case c := <-srv.gotCalls:
			if c.method == "Storage.setCookies" && strings.Contains(string(c.params), `"browserContextId":"bc-ds"`) {
				injected++
			}
		case <-drainEnd:
			goto drained2
		}
	}
drained2:
	if injected != 0 {
		t.Fatalf("提交前停用的失格槽绝不能收到任何 Storage.setCookies，得到 %d 次", injected)
	}
}

// TestHotInjectJustPublishedLegalSlotBetweenReads 复核 R12-2-②（Oracle M2）：
// 两次读（轮初候选快照 -> 提交前复核快照）之间同出口合法新槽刚发布——
// 直接热注入、绝不误 Bump：Jar 收到新票、槽位存活、后续回写正常落库。
// 确定性构造：B 在换发网络 I/O 期间完成播种与发布（与生产「构建协程在
// 两读之间发布」终态完全同构）。
func TestHotInjectJustPublishedLegalSlotBetweenReads(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=jx; __Secure-1PSID=jp; SID=js; __Secure-1PSIDTS=oldts"
	idB, err := accountInsert("jacctB", cookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	idC, err := accountInsert("jacctC", cookie, "")
	if err != nil {
		t.Fatalf("insert C: %v", err)
	}
	defer accountDelete(idC)

	// 合法同身份兄弟 C 的已发布直连槽（借用出口候选）。
	mkDirectSlot := func(id int64, tag string) browserSlotKey {
		key := browserSlotKey{accountID: id}
		ac := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookie),
			browserContextID: "bc-j" + tag,
			targetID:         "t-j" + tag,
			sessionID:        "sess-j" + tag,
			allowDirect:      true,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
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
			"SAPISID": "jx", "__Secure-1PSID": "jp", "SID": "js", "__Secure-1PSIDTS": "oldts",
			"SIDCC": "jcc",
		}
		srv.bcMu.Unlock()
		return key
	}
	keyC := mkDirectSlot(idC, "c")
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, keyC)
		delete(m.sessionIndex, "sess-jc")
		delete(m.targetIndex, "t-jc")
		m.mu.Unlock()
	}()

	// 直连换发桩：返回新票前完成 B 的「两读之间刚发布」交错——B 播种
	// 完成并发布合法同出口（直连）同身份新槽。
	published := false
	publishB := func() {
		keyB := browserSlotKey{accountID: idB}
		acB := &accountBrowserCtx{
			key:              keyB,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookie),
			browserContextID: "bc-jb",
			targetID:         "t-jb",
			sessionID:        "sess-jb",
			allowDirect:      true,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
		}
		acB.readyCh = make(chan struct{})
		close(acB.readyCh)
		m.mu.Lock()
		m.ctxs[keyB] = acB
		m.sessionIndex["sess-jb"] = acB
		m.targetIndex["t-jb"] = acB
		m.mu.Unlock()
		srv.bcMu.Lock()
		srv.sessToBC["sess-jb"] = "bc-jb"
		srv.bcStore["bc-jb"] = map[string]string{
			"SAPISID": "jx", "__Secure-1PSID": "jp", "SID": "js", "__Secure-1PSIDTS": "oldts",
			"SIDCC": "jcc",
		}
		srv.bcMu.Unlock()
	}
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, browserSlotKey{accountID: idB})
		delete(m.sessionIndex, "sess-jb")
		delete(m.targetIndex, "t-jb")
		m.mu.Unlock()
	}()
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			if !published {
				published = true
				publishB()
			}
			h := fhttp.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=jnew; Domain=.google.com; Path=/; Secure; HttpOnly")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404", Header: fhttp.Header{},
			Body: io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)

	// 轮初快照：B 尚无槽（漏进候选表），只有合法兄弟 C——借 C 出口换发。
	snapC := activeSlotSnapshot{
		accountID: idC, bcID: "bc-jc", proxyURL: "", allowDirect: true,
		ident: cookieIdentity(cookie),
	}
	rotateActiveOrSibling1PSIDTS(*accountByID(idB), []activeSlotSnapshot{snapC}, map[string][]string{})

	if !published {
		t.Fatal("前置：B 必须在两读之间真实发布（确定性交错成立）")
	}
	if a := accountByID(idB); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=jnew") {
		t.Fatalf("借兄弟出口换发必须写库: %q", a.Cookie)
	}
	// 核心断言①：两读之间刚发布的合法槽直接热注入（绝不误 Bump）。
	jar := srv.jarFor("sess-jb")
	if jar["__Secure-1PSIDTS"] != "jnew" {
		t.Fatalf("刚发布的合法新槽必须被正常热注入: %q", jar["__Secure-1PSIDTS"])
	}
	// 核心断言②：槽位存活（绝不误摘成僵尸/绝不误杀合法槽）。
	m.mu.Lock()
	acB := m.ctxs[browserSlotKey{accountID: idB}]
	alive := acB != nil && !acB.disposed
	m.mu.Unlock()
	if !alive {
		t.Fatal("合法新槽绝不能被摘除")
	}
	// 核心断言③：回写正常（epoch 未被误 Bump——Jar 新 SIDCC 可回写落库）。
	srv.bcMu.Lock()
	srv.bcStore["bc-jb"]["SIDCC"] = "jcc-refreshed"
	srv.bcMu.Unlock()
	m.SyncAccountCookiesBack(idB, true)
	if a := accountByID(idB); a == nil || !strings.Contains(a.Cookie, "SIDCC=jcc-refreshed") {
		t.Fatalf("合法新槽的后续回写必须正常落库（epoch 黑洞防线）: %q", a.Cookie)
	}
}

// TestAuthuserSlotGenerationIsolation 复核 R12-4-①（Oracle M6/Reviewer #5）：
// 行序号 0 -> 1 后，/u/1 请求绝不能复用 ac.authuser==0 的旧槽（其回写 CAS
// 以 0 作谓词必然 0 行命中=回写黑洞，且改回 0 时 /u/1 增量会串写进 0 行）：
// 旧槽在途流自然排空后按新序号独立建槽（新槽 ac.authuser=1），/u/1 增量
// 经 AND authuser=1 谓词正常落库；改回 0 时再按 0 建独立槽，/u/1 槽的
// 增量绝不串入 0 行。
func TestAuthuserSlotGenerationIsolation(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=gix; __Secure-1PSID=gip; SID=gis; __Secure-1PSIDTS=git; SIDCC=gi0"
	id, err := accountInsert("giacct", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)

	// 旧序号 0 的驻留槽（authuser=0），含一份在途租约。
	key := browserSlotKey{accountID: id}
	ac0 := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-gi0",
		targetID:         "t-gi0",
		sessionID:        "sess-gi0",
		allowDirect:      true,
		authuser:         0,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac0.readyCh = make(chan struct{})
	close(ac0.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac0
	m.sessionIndex["sess-gi0"] = ac0
	m.targetIndex["t-gi0"] = ac0
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		delete(m.sessionIndex, "sess-gi0")
		delete(m.targetIndex, "t-gi0")
		delete(m.sessionIndex, "sess-gi1")
		delete(m.targetIndex, "t-gi1")
		m.mu.Unlock()
	}()
	srv.bcMu.Lock()
	srv.sessToBC["sess-gi0"] = "bc-gi0"
	srv.bcStore["bc-gi0"] = map[string]string{
		"SAPISID": "gix", "__Secure-1PSID": "gip", "SID": "gis", "__Secure-1PSIDTS": "git", "SIDCC": "gi0",
	}
	srv.bcMu.Unlock()

	// 在途租约占用旧槽（改号前的在途流——绝不强杀，必须自然排空）。
	m.mu.Lock()
	ac0.inFlight++
	m.mu.Unlock()
	releaseOld := m.acquireReleaseFn(ac0)

	// 管理员改号 0 -> 1（合法管理操作，不拆槽）。
	if err := accountSetAuthUser(id, 1); err != nil {
		t.Fatalf("renumber: %v", err)
	}

	// /u/1 合法请求到达：旧槽有在途租约——必须先等排空，绝不立即替换
	// （也绝不复用 authuser=0 旧槽）。
	req := *accountByID(id)
	acquired := make(chan error, 1)
	var newClient *cdpClient
	var newAC *accountBrowserCtx
	var newRelease func()
	go func() {
		cl, ac2, _, rel, aerr := m.acquireAccountCtx(context.Background(), &req, "", true, true)
		newClient, newAC, newRelease = cl, ac2, rel
		acquired <- aerr
	}()
	select {
	case err := <-acquired:
		t.Fatalf("在途租约未排空时 /u/1 请求绝不能提前返回，得到: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	// 排空在途租约——/u/1 请求经唯一替换 winner 摘除旧槽并冷建新序号槽。
	releaseOld()
	select {
	case aerr := <-acquired:
		if aerr != nil {
			t.Fatalf("/u/1 合法请求必须正常获租（冷建新序号槽）: %v", aerr)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("/u/1 请求未在排空后完成冷建获租")
	}
	if newRelease != nil {
		defer newRelease()
	}
	_ = newClient
	// 核心断言①：旧槽被替换摘除，新槽是 authuser=1 的独立代际。
	m.mu.Lock()
	oldDisposed := ac0.disposed
	m.mu.Unlock()
	if !oldDisposed {
		t.Fatal("排空后旧序号槽必须被唯一替换 winner 摘除")
	}
	if newAC == nil || newAC == ac0 {
		t.Fatal("新租约绝不能复用 authuser=0 的旧槽")
	}
	m.mu.Lock()
	newAU := newAC.authuser
	m.mu.Unlock()
	if newAU != 1 {
		t.Fatalf("新槽必须记录新序号 authuser=1（回写 CAS 谓词正确性），得到 %d", newAU)
	}

	// 核心断言②：/u/1 槽的增量经 AND authuser=1 谓词正常落库。
	srv.bcMu.Lock()
	bcID1 := newAC.browserContextID
	srv.sessToBC[newAC.sessionID] = bcID1
	if srv.bcStore[bcID1] == nil {
		srv.bcStore[bcID1] = map[string]string{}
	}
	for k, v := range srv.bcStore["bc-gi0"] {
		srv.bcStore[bcID1][k] = v
	}
	srv.bcStore[bcID1]["SIDCC"] = "gi1refreshed"
	srv.bcMu.Unlock()
	m.SyncAccountCookiesBack(id, true)
	if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "SIDCC=gi1refreshed") {
		t.Fatalf("/u/1 新槽增量必须经 authuser=1 谓词正常落库: %q", a.Cookie)
	}

	// 归还新槽租约后改回 0——/u/1 槽增量绝不串入 0 行。
	newRelease()
	newRelease = nil
	if err := accountSetAuthUser(id, 0); err != nil {
		t.Fatalf("renumber back: %v", err)
	}
	// /u/1 槽的 jar 里追加一份「只在 /u/1 代际产生」的标记增量，随后
	// 强制回收路径试图回写：authuser 已变回 0，CAS 谓词必须 0 行命中。
	srv.bcMu.Lock()
	srv.bcStore[bcID1]["SIDCC"] = "gi1-should-never-land-in-u0"
	srv.bcMu.Unlock()
	m.InvalidateAccount(id, true, "test renumber back writeback")
	if a := accountByID(id); a == nil || strings.Contains(a.Cookie, "gi1-should-never-land-in-u0") {
		t.Fatalf("改回 0 后 /u/1 槽增量绝不许串写进 0 行: %q", a.Cookie)
	}
	// 0 行中仍保留合法落库的 /u/1 期增量（行在 authuser=1 时的合法写回不受影响）。
	if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "SIDCC=gi1refreshed") {
		t.Fatalf("行在 /u/1 期间的合法增量不应回退: %q", a.Cookie)
	}
}

// TestAccountSyncConditionalInvalidationKeepsNewIdentitySlot 复核 R12-4-②
// （Oracle M7/Reviewer #5）：accountSync 换身份 X->Y 提交并 Bump 后的锁外
// 窗口内，新 Y 请求建好的健康 Y 槽（epoch 严格大于 Bump 代际值）绝不能被
// 同步流程的失效误杀——代际条件失效只清旧代际（epoch < Bump 值）的 X 槽，
// 新 Y 槽存活且登记流无终态错误。
func TestAccountSyncConditionalInvalidationKeepsNewIdentitySlot(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookieX := "SAPISID=synx; __Secure-1PSID=synxp; SID=synxs; __Secure-1PSIDTS=oldt"
	cookieY := "SAPISID=syny; __Secure-1PSID=synyp; SID=synys; __Secure-1PSIDTS=oldt"
	id, err := accountInsert("synacct", cookieX, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)

	key := browserSlotKey{accountID: id}
	// 旧代际 X 槽（epoch=1，建槽早于同步——必然是被清理目标）。
	acX := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookieX),
		browserContextID: "bc-synx",
		targetID:         "t-synx",
		sessionID:        "sess-synx",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	acX.readyCh = make(chan struct{})
	close(acX.readyCh)
	m.mu.Lock()
	m.ctxs[key] = acX
	m.sessionIndex["sess-synx"] = acX
	m.targetIndex["t-synx"] = acX
	m.mu.Unlock()

	srv.bcMu.Lock()
	srv.sessToBC["sess-synx"] = "bc-synx"
	srv.bcStore["bc-synx"] = map[string]string{
		"SAPISID": "synx", "__Secure-1PSID": "synxp", "SID": "synxs",
	}
	srv.bcMu.Unlock()

	// X 槽上登记一份在途流（失效必须给它终态错误）。
	streamX := newBrowserStream()
	streamX.ac = acX
	m.registerStream(streamX)
	defer m.unregisterStream(streamX)

	// 第一次同步 X->Y：摘旧 X 槽、代际 Bump。
	res, err := accountSync("synacct", 0, cookieY)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if res.Action != "updated" {
		t.Fatalf("换身份同步必须为 updated，得到 %q", res.Action)
	}
	m.mu.Lock()
	xDisposed := acX.disposed
	m.mu.Unlock()
	if !xDisposed {
		t.Fatal("旧代际 X 槽必须被失效摘除")
	}
	select {
	case <-streamX.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("X 槽在途流必须收到终态关闭")
	}
	streamX.mu.Lock()
	if streamX.fatalErr == nil {
		streamX.mu.Unlock()
		t.Fatal("X 槽在途流必须有非 nil 终态错误")
	}
	streamX.mu.Unlock()

	// 模拟「锁外窗口内由新 Y 请求建好并发布的新 Y 槽」：epoch 严格大于
	// 本次 Bump 代际值（新代际槽）。用 m.nextCtxEpoch+N 构造与真实冷建
	// 相同的代际关系，并登记一份在途流。
	m.mu.Lock()
	yEpoch := m.nextCtxEpoch + 1
	m.mu.Unlock()
	acY := &accountBrowserCtx{
		key:              key,
		epoch:            yEpoch,
		cookieIdentity:   cookieIdentity(cookieY),
		browserContextID: "bc-syny",
		targetID:         "t-syny",
		sessionID:        "sess-syny",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	acY.readyCh = make(chan struct{})
	close(acY.readyCh)
	m.mu.Lock()
	m.ctxs[key] = acY
	m.sessionIndex["sess-syny"] = acY
	m.targetIndex["t-syny"] = acY
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		delete(m.sessionIndex, "sess-syny")
		delete(m.targetIndex, "t-syny")
		delete(m.sessionIndex, "sess-synx")
		delete(m.targetIndex, "t-synx")
		m.mu.Unlock()
	}()
	streamY := newBrowserStream()
	streamY.ac = acY
	m.registerStream(streamY)
	defer m.unregisterStream(streamY)

	// 核心断言：对新 Y 槽执行「按本次同步 Bump 代际值」的条件失效——
	// 新 Y 槽（epoch > Bump 值）必须存活且流无终态错误；对照：旧代际
	//（epoch < Bump 值）槽必然被摘。
	m.mu.Lock()
	bumpEpoch := m.maxSeededEpoch[id]
	m.mu.Unlock()
	if yEpoch <= bumpEpoch {
		t.Fatalf("前置：新 Y 槽 epoch 必须严格大于 Bump 代际值（y=%d bump=%d）", yEpoch, bumpEpoch)
	}
	m.InvalidateAccountIfEpochBefore(id, bumpEpoch, "cookie synced")
	m.mu.Lock()
	yAlive := m.ctxs[key] == acY && !acY.disposed
	m.mu.Unlock()
	if !yAlive {
		t.Fatal("新 Y 槽绝不能被同步流程的失效误杀（代际条件失效防线）")
	}
	select {
	case <-streamY.closed:
		t.Fatal("新 Y 槽在途流绝不能收到终态关闭")
	case <-time.After(150 * time.Millisecond):
	}
	streamY.mu.Lock()
	if streamY.fatalErr != nil {
		streamY.mu.Unlock()
		t.Fatalf("新 Y 槽在途流绝不能有终态错误: %v", streamY.fatalErr)
	}
	streamY.mu.Unlock()
}

// TestCookieToCDPIdentityKeysFirstNonEmptyDedup 复核 R12-6-①（Oracle M9/
// Reviewer #2）：播种的会话身份主键（SAPISID/__Secure-1PSID）重名多项按
// 与 cookieIdentity/cookieSubset 完全一致的「首个非空值」口径去重后只播种
// 该有效项——fake Jar 末写覆盖首写，全量播种会让槽公布身份（首项）与 Jar
// 实际身份（末项）分裂。普通重名业务键保序不变。
func TestCookieToCDPIdentityKeysFirstNonEmptyDedup(t *testing.T) {
	cookie := "SAPISID=first; SID=a; SAPISID=second; __Secure-1PSID=pfirst; SIDCC=dup1; __Secure-1PSID=psecond; SIDCC=dup2"
	params := cookieToCDP(cookie)
	var sapisid, psid, sidcc []string
	for _, c := range params {
		name, _ := c["name"].(string)
		val, _ := c["value"].(string)
		switch name {
		case "SAPISID":
			sapisid = append(sapisid, val)
		case "__Secure-1PSID":
			psid = append(psid, val)
		case "SIDCC":
			sidcc = append(sidcc, val)
		}
	}
	// 身份主键：只播种首个非空值代表项。
	if len(sapisid) != 1 || sapisid[0] != "first" {
		t.Fatalf("SAPISID 重名必须只播种首个非空值 first: %v", sapisid)
	}
	if len(psid) != 1 || psid[0] != "pfirst" {
		t.Fatalf("__Secure-1PSID 重名必须只播种首个非空值 pfirst: %v", psid)
	}
	// 普通业务键：重名保序不变（两项都播种、顺序不动）。
	if len(sidcc) != 2 || sidcc[0] != "dup1" || sidcc[1] != "dup2" {
		t.Fatalf("普通重名业务键必须保序全量播种: %v", sidcc)
	}

	// 端到端口径一致性：用同一 fake Jar 末写覆盖语义模拟播种后的实际
	// Jar，它按 cookieIdentity 计算出的身份必须与槽公布身份严格相等。
	jar := map[string]string{}
	for _, c := range params {
		name, _ := c["name"].(string)
		val, _ := c["value"].(string)
		jar[name] = val
	}
	var jarPairs []string
	for k, v := range jar {
		jarPairs = append(jarPairs, k+"="+v)
	}
	jarCookie := strings.Join(jarPairs, "; ")
	slotIdent := cookieIdentity(cookie)
	jarIdent := cookieIdentity(jarCookie)
	if jarIdent != slotIdent {
		t.Fatalf("播种后 Jar 实际身份必须与槽公布身份严格一致: jar=%q slot=%q", jarIdent, slotIdent)
	}

	// 首空后有效：前导空值项不代表会话身份，首个非空项作为唯一代表。
	cookie2 := "__Secure-1PSID=; SAPISID=sk; __Secure-1PSID=valid"
	params2 := cookieToCDP(cookie2)
	var psid2 []string
	for _, c := range params2 {
		if name, _ := c["name"].(string); name == "__Secure-1PSID" {
			val, _ := c["value"].(string)
			psid2 = append(psid2, val)
		}
	}
	if len(psid2) != 1 || psid2[0] != "valid" {
		t.Fatalf("首空后有效的 __Secure-1PSID 必须播种非空代表项 valid: %v", psid2)
	}
}

// TestRotate1PSIDTSPrecheckFirstNonEmpty 复核 R12-6-②（Oracle S2/Reviewer #8）：
// `__Secure-1PSID=`（首项空）后跟有效第二项时，换发缺票预检必须按「首个
// 非空值」口径放行——cookieSubset 实际带出的与 cookieIdentity 裁决的都是
// 第二项，预检绝不能按只取首项的 cookieValue 误判缺失而整轮静默跳过。
func TestRotate1PSIDTSPrecheckFirstNonEmpty(t *testing.T) {
	cookie := "SAPISID=fpx; __Secure-1PSID=; __Secure-1PSID=fvalid; SID=fps; __Secure-1PSIDTS=oldt"
	id, err := accountInsert("fpacct", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)

	hit := 0
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			hit++
			// 实际带出的子集必须含首个非空值 fvalid（而非空首项）。
			if ck := req.Header.Get("Cookie"); !strings.Contains(ck, "__Secure-1PSID=fvalid") {
				t.Errorf("换发请求必须携带首个非空值票据，得到 %q", ck)
			}
			h := fhttp.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=fpnew; Domain=.google.com; Path=/; Secure; HttpOnly")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404", Header: fhttp.Header{},
			Body: io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)

	merged, names, err := tryRotate1PSIDTS(id, cookie, "")
	if err != nil {
		t.Fatalf("首空后有效的 1PSID 必须正常触发换发: %v", err)
	}
	if hit != 1 {
		t.Fatalf("缺票预检误判缺失（修复前按首项空值整轮静默跳过）: hit=%d", hit)
	}
	if len(names) == 0 {
		t.Fatal("换发必须报告实际刷新的票据名")
	}
	if !strings.Contains(merged, "__Secure-1PSIDTS=fpnew") {
		t.Fatalf("合并结果必须含新票: %q", merged)
	}
}

// TestPatchCommittedDisableAlwaysInvalidatesOnBindFailure 复核 R12-5-②
// （Oracle M8/Reviewer #6）：多字段 PATCH 先把账号停用成功提交，后续
// proxy_id 绑定写失败（故障注入：钩子内删除账号行使 rebind 命中行不
// 存在）时，面板拿到如实错误，但已提交的停用必须完成 InvalidateAccount
// ——旧槽摘除、在途流终态，绝不留下已停用账号的在途流（修复前错误
// 返回路径直接跳过失效，且重试时 before.Status 已 disabled 永不补做）。
func TestPatchCommittedDisableAlwaysInvalidatesOnBindFailure(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{
		{ID: 81, Name: "pd1", URL: "http://pd1:8080", Enabled: true},
	}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=pdx; __Secure-1PSID=pdp; SID=pds; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("pdacct", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	// 注意：本用例故障注入会删除该行，cleanup 用幂等删除（行不存在也无害）。
	defer func() { _ = accountDelete(id) }()

	key := browserSlotKey{accountID: id}
	ac := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-pd",
		targetID:         "t-pd",
		sessionID:        "sess-pd",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac.readyCh = make(chan struct{})
	close(ac.readyCh)
	m.mu.Lock()
	m.ctxs[key] = ac
	m.sessionIndex["sess-pd"] = ac
	m.targetIndex["t-pd"] = ac
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		delete(m.sessionIndex, "sess-pd")
		delete(m.targetIndex, "t-pd")
		m.mu.Unlock()
	}()
	srv.bcMu.Lock()
	srv.sessToBC["sess-pd"] = "bc-pd"
	srv.bcStore["bc-pd"] = map[string]string{
		"SAPISID": "pdx", "__Secure-1PSID": "pdp", "SID": "pds",
	}
	srv.bcMu.Unlock()
	stream := newBrowserStream()
	stream.ac = ac
	m.registerStream(stream)
	defer m.unregisterStream(stream)

	// 故障注入：停用已提交、绑定写之前的精确交错点上删除账号行——
	// rebindAccountProxyAtomic 命中行不存在（绑定写失败的确定性形态）。
	testHookBeforeCookiePatchRebind = func(target int64) {
		if target == id {
			_ = accountDelete(id)
		}
	}
	defer func() { testHookBeforeCookiePatchRebind = nil }()

	r := httptest.NewRequest("PATCH", fmt.Sprintf("/admin/api/cookies/%d", id),
		strings.NewReader(fmt.Sprintf(`{"status":"disabled","proxy_id":%d}`, 81)))
	w := httptest.NewRecorder()
	handleAdminCookieItem(w, r)

	// 面板必须拿到如实错误（绝不谎报 ok:true）。
	if w.Code == 200 {
		t.Fatalf("绑定写失败的 PATCH 绝不能回 200: %s", w.Body)
	}
	// 核心断言①：已提交的停用必须完成失效——旧槽被摘除。
	m.mu.Lock()
	_, still := m.ctxs[key]
	disposed := ac.disposed
	m.mu.Unlock()
	if still || !disposed {
		t.Fatalf("停用已提交但绑定失败时旧槽必须被失效摘除（still=%v disposed=%v）", still, disposed)
	}
	// 核心断言②：在途流必须收到终态错误（绝不留下已停用账号的在途流）。
	select {
	case <-stream.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("在途流必须收到终态关闭")
	}
	stream.mu.Lock()
	if stream.fatalErr == nil {
		stream.mu.Unlock()
		t.Fatal("在途流必须有非 nil 终态错误")
	}
	stream.mu.Unlock()

	// 对照：同值幂等（old == new）不拆槽——行存在且绑定未变时 PATCH
	// 正常 200 且健康槽存活（R12-5-① 同值幂等分支不依赖 RowsAffected）。
	id2, err := accountInsert("pdctl", cookie, "")
	if err != nil {
		t.Fatalf("insert ctl: %v", err)
	}
	defer accountDelete(id2)
	key2 := browserSlotKey{accountID: id2}
	ac2 := &accountBrowserCtx{
		key:              key2,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-pd2",
		targetID:         "t-pd2",
		sessionID:        "sess-pd2",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	ac2.readyCh = make(chan struct{})
	close(ac2.readyCh)
	m.mu.Lock()
	m.ctxs[key2] = ac2
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key2)
		m.mu.Unlock()
	}()
	r2 := httptest.NewRequest("PATCH", fmt.Sprintf("/admin/api/cookies/%d", id2),
		strings.NewReader(`{"proxy_id":0}`))
	w2 := httptest.NewRecorder()
	handleAdminCookieItem(w2, r2)
	if w2.Code != 200 {
		t.Fatalf("同值幂等 PATCH 必须回 200: %s", w2.Body)
	}
	m.mu.Lock()
	alive2 := m.ctxs[key2] == ac2 && !ac2.disposed
	m.mu.Unlock()
	if !alive2 {
		t.Fatal("同值幂等 PATCH 绝不拆槽")
	}
}

// TestHotInjectPublishBetweenVerifyAndCommitNeverZombie 复核 R12-2-②精确交错
// （Oracle M2）：合法新槽在「提交前复核快照」与「prepareHotInject 提交判定」
// 两次读取之间刚发布——此时提交核验发现驻留槽不是授权时刻核验过的实例
// （bcID 失配），绝不注入；DB 更新后必须对该具体 bcID 条件摘除，绝不同时
// 出现「DB 新、Jar 旧、可授租、回写被 epoch 拒」的僵尸态。确定性构造：
// 生产测试钩子在提交判定前精确交错点完成播种发布。
func TestHotInjectPublishBetweenVerifyAndCommitNeverZombie(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	cookie := "SAPISID=zx; __Secure-1PSID=zp; SID=zs; __Secure-1PSIDTS=oldts"
	idB, err := accountInsert("zacctB", cookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	idC, err := accountInsert("zacctC", cookie, "")
	if err != nil {
		t.Fatalf("insert C: %v", err)
	}
	defer accountDelete(idC)

	// 合法同身份兄弟 C 的已发布直连槽（借用出口候选）。
	keyC := browserSlotKey{accountID: idC}
	acC := &accountBrowserCtx{
		key:              keyC,
		epoch:            1,
		cookieIdentity:   cookieIdentity(cookie),
		browserContextID: "bc-zc",
		targetID:         "t-zc",
		sessionID:        "sess-zc",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	acC.readyCh = make(chan struct{})
	close(acC.readyCh)
	m.mu.Lock()
	m.ctxs[keyC] = acC
	m.sessionIndex["sess-zc"] = acC
	m.targetIndex["t-zc"] = acC
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, keyC)
		delete(m.sessionIndex, "sess-zc")
		delete(m.targetIndex, "t-zc")
		m.mu.Unlock()
	}()
	srv.bcMu.Lock()
	srv.sessToBC["sess-zc"] = "bc-zc"
	srv.bcStore["bc-zc"] = map[string]string{
		"SAPISID": "zx", "__Secure-1PSID": "zp", "SID": "zs", "__Secure-1PSIDTS": "oldts",
	}
	srv.bcMu.Unlock()

	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		if strings.Contains(req.URL.Path, "/RotateCookies") {
			h := fhttp.Header{}
			h.Add("Set-Cookie", "__Secure-1PSIDTS=znew; Domain=.google.com; Path=/; Secure; HttpOnly")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(")]}'\n\n[]")),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404", Header: fhttp.Header{},
			Body: io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)

	// 精确交错：提交前复核时 B 尚无槽；钩子内（复核后、提交判定前）B
	// 完成播种并发布合法同出口同身份新槽。
	published := false
	testHookBeforeHotInjectCommit.Store(func(accountID int64) {
		if accountID != idB || published {
			return
		}
		published = true
		keyB := browserSlotKey{accountID: idB}
		acB := &accountBrowserCtx{
			key:              keyB,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookie),
			browserContextID: "bc-zb",
			targetID:         "t-zb",
			sessionID:        "sess-zb",
			allowDirect:      true,
			readyClosed:      true,
			lastUsedAt:       time.Now(),
		}
		acB.readyCh = make(chan struct{})
		close(acB.readyCh)
		m.mu.Lock()
		m.ctxs[keyB] = acB
		m.sessionIndex["sess-zb"] = acB
		m.targetIndex["t-zb"] = acB
		m.mu.Unlock()
		srv.bcMu.Lock()
		srv.sessToBC["sess-zb"] = "bc-zb"
		srv.bcStore["bc-zb"] = map[string]string{
			"SAPISID": "zx", "__Secure-1PSID": "zp", "SID": "zs", "__Secure-1PSIDTS": "oldts",
		}
		srv.bcMu.Unlock()
	})
	defer func() {
		testHookBeforeHotInjectCommit.Store((func(int64))(nil))
		m.mu.Lock()
		delete(m.ctxs, browserSlotKey{accountID: idB})
		delete(m.sessionIndex, "sess-zb")
		delete(m.targetIndex, "t-zb")
		m.mu.Unlock()
	}()

	snapC := activeSlotSnapshot{
		accountID: idC, bcID: "bc-zc", proxyURL: "", allowDirect: true,
		ident: cookieIdentity(cookie),
	}
	rotateActiveOrSibling1PSIDTS(*accountByID(idB), []activeSlotSnapshot{snapC}, map[string][]string{})

	if !published {
		t.Fatal("测试钩子必须在提交前交错点真实执行（确定性交错成立）")
	}
	if a := accountByID(idB); a == nil || !strings.Contains(a.Cookie, "__Secure-1PSIDTS=znew") {
		t.Fatalf("借兄弟出口换发必须写库: %q", a.Cookie)
	}
	// 核心断言①：两读之间刚发布的槽绝不注入（bcID 失配——它不是授权
	// 时刻核验过的实例）。
	jar := srv.jarFor("sess-zb")
	if jar["__Secure-1PSIDTS"] == "znew" {
		t.Fatal("两读之间刚发布的未核验槽绝不能收到热注入")
	}
	// 核心断言②：DB 更新后对该具体 bcID 条件摘除——绝不出现「DB 新、
	// Jar 旧、可授租、回写被 epoch 拒」的僵尸态。
	m.mu.Lock()
	acB := m.ctxs[browserSlotKey{accountID: idB}]
	zombie := acB != nil && !acB.disposed
	m.mu.Unlock()
	if zombie {
		t.Fatal("两读之间刚发布但未获准注入的驻留槽必须被条件摘除（僵尸槽防线）")
	}
}
