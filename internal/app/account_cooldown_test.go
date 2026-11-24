package app

import (
	"errors"
	"testing"
	"time"
)

// resetAccountHealthForTest 清空防雪崩全局状态，给每个 case 独立基线。
func resetAccountHealthForTest(t *testing.T) {
	t.Helper()
	accountHealthMu.Lock()
	accountHealth = map[int64]*accountShortHealth{}
	accountHealthMu.Unlock()
	t.Cleanup(func() {
		accountHealthMu.Lock()
		accountHealth = map[int64]*accountShortHealth{}
		accountHealthMu.Unlock()
	})
}

// TestShortCooldownNeedsTwoIndependentReqs 覆盖「同一 reqID 幂等 + 独立 reqID 累计」：
// 同请求重试 N 次最多记 1 次失败，不同 reqID 记到 N=2 才进 45s 冷却。
func TestShortCooldownNeedsTwoIndependentReqs(t *testing.T) {
	resetAccountHealthForTest(t)
	acctID := int64(9001)
	reqA := nextClientReqID()
	now := time.Now()

	// 同 reqID 重复失败 5 次：幂等，failStreak 最多 1。
	for i := 0; i < 5; i++ {
		recordAccountReqFailureAt(acctID, reqA, classUpstreamLimit, now)
	}
	accountHealthMu.Lock()
	st := accountHealth[acctID]
	if st.failStreak != 1 {
		accountHealthMu.Unlock()
		t.Fatalf("同 reqID 应幂等为 1 次失败，得到 streak=%d", st.failStreak)
	}
	accountHealthMu.Unlock()

	if skip, _ := shortCoolingAccounts(); skip[acctID] {
		t.Fatal("1 次独立失败不该进冷却")
	}

	// 第 2 个独立 reqID 失败 -> 进入冷却。
	reqB := nextClientReqID()
	recordAccountReqFailureAt(acctID, reqB, classUpstreamLimit, now.Add(time.Second))
	if skip, soonest := shortCoolingAccounts(); !skip[acctID] || soonest <= 0 {
		t.Fatalf("2 次独立失败应进 45s 冷却，skip=%v soonest=%v", skip, soonest)
	}
}

// TestHalfOpenSingleFlightProbe 覆盖半开单飞：
// 冷却到期后第一条请求认领 probeInFlight；并发第二条仍视为冷却（不黑洞）。
func TestHalfOpenSingleFlightProbe(t *testing.T) {
	resetAccountHealthForTest(t)
	acctID := int64(9002)
	// 直接种一个「刚到期」的冷却 + 认领探测位。
	accountHealthMu.Lock()
	accountHealth[acctID] = &accountShortHealth{
		cooldownUntil: time.Now().Add(-time.Second), // 已到期 = 半开
		failStreak:    accountShortFailLimit - 1,
	}
	accountHealthMu.Unlock()

	probeReq := nextClientReqID()
	if !tryClaimHalfOpenProbe(acctID, probeReq) {
		t.Fatal("半开态第一条请求应认领探测位")
	}
	// 探测位被占：并发请求仍视为冷却（平滑路由到备号）。
	if skip, _ := shortCoolingAccounts(); !skip[acctID] {
		t.Fatal("探测位被占时并发请求应仍视为冷却")
	}
	otherReq := nextClientReqID()
	if tryClaimHalfOpenProbe(acctID, otherReq) {
		t.Fatal("探测位被占时别的 reqID 不该再认领")
	}
	// 探测成功：清零恢复。
	recordAccountReqSuccess(acctID, probeReq)
	if skip, _ := shortCoolingAccounts(); skip[acctID] {
		t.Fatal("探测成功后应出冷却")
	}
	if st := accountHealth[acctID]; st.probeInFlight {
		t.Fatal("探测成功后应放掉探测位")
	}
}

// TestHalfOpenProbeLateReleaseDoesNotClearOther 覆盖 R3-S1：
// 探测位被 reqX 占着，迟到的 reqY 失败结算只在 probeReqID 匹配或冷却续期时清位——
// 不能把别人占着的位误清。
func TestHalfOpenProbeLateReleaseDoesNotClearOther(t *testing.T) {
	resetAccountHealthForTest(t)
	acctID := int64(9003)
	accountHealthMu.Lock()
	accountHealth[acctID] = &accountShortHealth{
		cooldownUntil: time.Now().Add(-time.Second),
		failStreak:    accountShortFailLimit - 1,
		probeInFlight: true,
		probeReqID:    777,
	}
	accountHealthMu.Unlock()

	// 一个跟占位无关的迟到 reqID 失败：streak 记到 limit -> renewedCooldown=true，
	// 此时清位合法（冷却续期）；但下面先看「未续期」的分支——把 streak 放低让它不续期。
	accountHealthMu.Lock()
	accountHealth[acctID].failStreak = 0 // 让这次失败达不到 limit -> 不续期
	accountHealthMu.Unlock()
	recordAccountReqFailureAt(acctID, 888, classUpstreamLimit, time.Now())
	if st := accountHealth[acctID]; !st.probeInFlight || st.probeReqID != 777 {
		t.Fatal("未续期冷却的迟到失败不该清掉别人占着的探测位")
	}
}

// TestPoisonPairClassifiesC1C1b 覆盖 isPoisonPair 双端同落 {C1,C1b} 互认：
// 主备两个不同账号对同一载荷报同一类瞬时拒绝 = 毒载荷，两端零记账。
func TestPoisonPairClassifiesC1C1b(t *testing.T) {
	c1 := &diagErr{err: errors.New("upstream returned no content frame"), reason: reasonNoContentFrame}
	c1b := &diagErr{err: errEmptyContent, reason: reasonEmptyContent}
	if !isPoisonPair(c1, c1b) {
		t.Fatal("C1(no_content_frame) + C1b(empty_content) 应互认毒载荷")
	}
	if !isPoisonPair(c1b, c1) {
		t.Fatal("顺序无关：C1b + C1 也应互认")
	}
	// 一端是网络类就不是毒载荷（责任在出口不在载荷）。
	if isPoisonPair(c1, errors.New("dial tcp timeout")) {
		t.Fatal("C1 + 网络类不该判毒载荷")
	}
	if isPoisonPair(c1, nil) || isPoisonPair(nil, c1) {
		t.Fatal("一端成功不算毒载荷")
	}
}

// TestTryAdvanceHopMixedChainInvariant 覆盖 v2.2 §4 混合链不变量：
//   - 纯 C3 降级链可跳 2 次（触碰 3 号）；
//   - 一旦发生故障跳槽（faultHops>=1），后续遇 C3 禁止再跳；
//   - 故障类跳槽上限 1（含故障请求至多触碰 2 号）。
func TestTryAdvanceHopMixedChainInvariant(t *testing.T) {
	t.Run("纯C3降级链允许跳满3号", func(t *testing.T) {
		f := newReqFailoverState(false)
		f.markTouched(1)
		if !f.tryAdvanceHop(classModelDowngrade, false) {
			t.Fatal("纯 C3 第 1 跳应允许")
		}
		f.markTouched(2)
		if !f.tryAdvanceHop(classModelDowngrade, false) {
			t.Fatal("纯 C3 第 2 跳应允许（触碰第 3 号）")
		}
		f.markTouched(3)
		if f.tryAdvanceHop(classModelDowngrade, false) {
			t.Fatal("已触碰 3 号后 C3 应封顶")
		}
	})

	t.Run("故障跳过后C3禁止再跳第3号", func(t *testing.T) {
		f := newReqFailoverState(false)
		f.markTouched(1)
		if !f.tryAdvanceHop(classUpstreamLimit, false) { // C2 故障：faultHops=1
			t.Fatal("首次故障跳槽应允许")
		}
		f.markTouched(2)
		if f.tryAdvanceHop(classModelDowngrade, false) {
			t.Fatal("faultHops>=1 后遇 C3 禁止再跳第 3 号")
		}
	})

	t.Run("故障类跳槽上限1次", func(t *testing.T) {
		f := newReqFailoverState(false)
		f.markTouched(1)
		if !f.tryAdvanceHop(classContentRefuse, false) {
			t.Fatal("首次 C1 跳槽应允许")
		}
		f.markTouched(2)
		if f.tryAdvanceHop(classUpstreamLimit, false) {
			t.Fatal("faultHops 已满 1，第二次故障应封顶")
		}
	})

	t.Run("已吐字不再跳", func(t *testing.T) {
		f := newReqFailoverState(false)
		f.markTouched(1)
		if f.tryAdvanceHop(classContentRefuse, true) {
			t.Fatal("emitted=true 时任何错误都不该跳槽（客户端会收两遍）")
		}
	})

	t.Run("pinned不跳", func(t *testing.T) {
		f := newReqFailoverState(true)
		f.markTouched(1)
		if f.tryAdvanceHop(classContentRefuse, false) {
			t.Fatal("pinned 会话禁止跳槽")
		}
	})
}

// TestPickAccountForModelPriority 覆盖 accounts.priority 严格主备：
// 数值大的先被挑中，priority=0 时与旧「最久未用」轮转一致。
func TestPickAccountForModelPriority(t *testing.T) {
	resetAccountHealthForTest(t)
	resetDowngrades(t)
	main := addTestAccount(t, "main-acct")
	backup := addTestAccount(t, "backup-acct")
	if _, err := db.Exec(`UPDATE accounts SET priority = ? WHERE id = ?`, 100, main); err != nil {
		t.Fatalf("设主号 priority=100 失败: %v", err)
	}
	if _, err := db.Exec(`UPDATE accounts SET priority = ? WHERE id = ?`, 10, backup); err != nil {
		t.Fatalf("设备号 priority=10 失败: %v", err)
	}

	// 连续挑 5 次：priority=100 的主号必须每次优先于 priority=10 的备号。
	flash := Models["gemini-3.6-flash"]
	for i := 0; i < 5; i++ {
		a, _, err := pickAccountForModel(flash, nil, nil)
		if err != nil || a == nil || a.ID != main {
			t.Fatalf("priority=100 应恒先被挑中，得到 acct=%v err=%v", a, err)
		}
	}
}
