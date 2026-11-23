package app

import (
	"errors"
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

// acquireClaimedSlot：库中锁定直连的号，即便 prefer 误传代理 ID，
// 也必须释放代理槽重取直连槽。
func TestAcquireClaimedSlotLockedDirect(t *testing.T) {
	a, _ := withTwoProxies(t, true)
	id := addTestAccount(t, "lockedDirect2")
	bindAccountProxy(id, proxyIDDirect)
	p, release, err := acquireClaimedSlot(id, a)
	defer release()
	if err != nil || p == nil || p.ID != 0 {
		t.Fatalf("锁定直连号误持代理槽应重取直连：p=%+v err=%v", p, err)
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
