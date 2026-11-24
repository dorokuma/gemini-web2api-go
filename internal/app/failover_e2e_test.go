package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// stubOncePickReal 把单轮上游轮换成「真实挑号 + 按号编剧」的桩：
// 每跳先走真实 pickAccountForModel（吃 exclude/冷却/绑定守卫，e2e 价值全在
// 这里——跳槽准入、排除集、Bound 仲裁都是真路径），挑到号后按脚本返回该号的
// (res, err)。脚本按账号 ID 索引；没登记的号默认返回成功（Raw 非空）。
//
// 这跟 stubConvTurnRealSlot 同形：挑号/仲裁是真实的，只有「上游那一发」
// 被换成了编剧帧。返回的 calls 记录实际挑中的账号顺序。
func stubOncePickReal(t *testing.T,
	script func(acctID int64) (*StreamResult, error)) (calls *[]int64) {
	t.Helper()
	got := &[]int64{}
	old := runStreamGenerateOnce
	runStreamGenerateOnce = func(ctx context.Context, prompt string, pc promptCtx, mc ModelConfig,
		pending []pendingUpload, onDelta, onReasoning func(string), stopWhen func(string) bool,
		opts streamOnceOpts) (*StreamResult, error) {
		if err := ctx.Err(); err != nil {
			return &StreamResult{}, err
		}
		if opts.forceAnon {
			// 外层 FallbackAnon 兜底：不挑号，res.AccountID=0 表示匿名成功。
			return &StreamResult{Raw: conversationTestRaw(t, "anon-ok"), AccountID: 0}, nil
		}
		a, _, err := pickAccountForModel(mc, opts.exclude, opts.fstate)
		if err != nil {
			return &StreamResult{}, err
		}
		if a == nil {
			return &StreamResult{}, errors.New("no eligible account (stub)")
		}
		*got = append(*got, a.ID)
		res, serr := script(a.ID)
		if res == nil {
			res = &StreamResult{}
		}
		res.AccountID = a.ID
		return res, serr
	}
	t.Cleanup(func() { runStreamGenerateOnce = old })
	return got
}

// resetFailoverE2E 清空会影响跨号跳槽判定的全局态（短冷却/降级/探测位）。
// convReasoningOnlyRaw 造一条「有内容帧但抽不出正文」的会话 raw：
// inner[4][0][37][0][0] 放一段思考链（reasoningInLine 读这里），文本位留空——
// hasContentFrame 为真（reasoning 非空），extractResponseText 为空 = C1b empty_content。
func convReasoningOnlyRaw(t *testing.T) string {
	t.Helper()
	inner := make([]interface{}, 44)
	inner[1] = []interface{}{"", ""}
	// 第 0 个候选 part：无文本 pl[1]，但 pl[37] 藏一段思考链。
	p0 := make([]interface{}, 38)
	p0[37] = []interface{}{[]interface{}{"thinking-only-chain"}}
	inner[4] = []interface{}{p0}
	inner[39], inner[42], inner[43] = "cf41b0e0dd7d53e5", "3.5 Flash-Lite", true
	body, err := json.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	line, err := json.Marshal([]interface{}{[]interface{}{"wrb.fr", nil, string(body)}})
	if err != nil {
		t.Fatal(err)
	}
	return string(line)
}

func resetFailoverE2E(t *testing.T) {
	t.Helper()
	resetAccountHealthForTest(t)
	resetDowngrades(t)
	rtMu.Lock()
	old := rtVal
	rtVal.AnonFirst = false   // 关匿名优先：测试要每次都真实挑号
	rtVal.StrictModel = true  // 开 strict_model：C3 降级走 markDowngraded 路径
	rtVal.FallbackAnon = true // 开外层兜底（用例内按需再关）
	rtMu.Unlock()
	t.Cleanup(func() {
		rtMu.Lock()
		rtVal = old
		rtMu.Unlock()
	})
}

// 端到端：streamGenerateWithFilesOpts 单轮外层状态机——第 0 跳 C1
// （no_content_frame）延迟暂挂 pendingHop0，跨号跳到备号成功后落定：
// 备号成功记账、第 0 跳按非毒因果单次提交，最终返回成功。
func TestWithFilesC1HopToSpareSucceeds(t *testing.T) {
	resetFailoverE2E(t)
	a1 := addTestAccount(t, "hop-c1-a")
	a2 := addTestAccount(t, "hop-c1-b")
	calls := stubOncePickReal(t, func(id int64) (*StreamResult, error) {
		if id == a1 {
			// C1：no_content_frame，未吐字（Emitted=""），可跳槽。
			// err 不用 errEmptyContent（那是 C1b）——C1 是「根本无内容帧」。
			return &StreamResult{AccountID: a1},
				&diagErr{err: errors.New("upstream returned no content frame"), reason: reasonNoContentFrame}
		}
		return &StreamResult{AccountID: a2, Raw: conversationTestRaw(t, "answer")}, nil
	})
	mc := Models["gemini-3.6-flash"]
	res, err := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("C1 跳到备号应成功，得到 err=%v", err)
	}
	if res == nil || res.AccountID != a2 {
		t.Fatalf("应落在备号 #%d，得到 res=%+v", a2, res)
	}
	if len(*calls) != 2 || (*calls)[0] != a1 || (*calls)[1] != a2 {
		t.Fatalf("应先试 #%d 再跳 #%d，实际 calls=%v", a1, a2, *calls)
	}
	// 第 0 跳 C1 非毒（备号成功证明载荷可用）：按因果时序单次提交 a1 失败。
	accountHealthMu.Lock()
	st1 := accountHealth[a1]
	accountHealthMu.Unlock()
	if st1 == nil || st1.failStreak != 1 {
		t.Fatalf("第 0 跳 C1 应单次记 a1 失败（streak=1），得到 %+v", st1)
	}
	// 备号成功：建立健康项记 lastSuccessAt。
	accountHealthMu.Lock()
	st2 := accountHealth[a2]
	accountHealthMu.Unlock()
	if st2 == nil || st2.lastSuccessAt.IsZero() {
		t.Fatalf("备号成功应建项写 lastSuccessAt，得到 %+v", st2)
	}
}

// 端到端：双号同落 C1 = 毒载荷实锤——两端零账号记账、零代理扣分。
// 第 0 跳 C1 暂挂，第 1 跳也 C1，isPoisonPair 判定毒载荷返回包装错。
func TestWithFilesTwoAccountPoisonExempt(t *testing.T) {
	resetFailoverE2E(t)
	a1 := addTestAccount(t, "pp-a")
	a2 := addTestAccount(t, "pp-b")
	calls := stubOncePickReal(t, func(id int64) (*StreamResult, error) {
		return &StreamResult{AccountID: id},
			&diagErr{err: errEmptyContent, reason: reasonNoContentFrame}
	})
	mc := Models["gemini-3.6-flash"]
	_, err := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	var ppe *poisonPayloadError
	if !errors.As(err, &ppe) {
		t.Fatalf("双号同落 C1 应判定毒载荷，得到 err=%v", err)
	}
	if len(*calls) != 2 {
		t.Fatalf("毒载荷双跳后应停止（至多碰 2 号），实际 calls=%v", *calls)
	}
	// 双端零记账：两号都不该进 accountHealth 失败态。
	accountHealthMu.Lock()
	s1, s2 := accountHealth[a1], accountHealth[a2]
	accountHealthMu.Unlock()
	if (s1 != nil && s1.failStreak > 0) || (s2 != nil && s2.failStreak > 0) {
		t.Fatalf("毒载荷双端应零账号记账：a1=%+v a2=%+v", s1, s2)
	}
}

// 端到端：C1 -> C3 混合链——第 0 跳 C1 故障暂挂，第 1 跳遇 C3 降级。
// 故障已消耗 faultHops=1，遇 C3 时纯降级预算仍允许第 2 跳换号（纯 C3 链
// 未发生过故障跳槽），但 faultHops>=1 后 C3 禁止再跳第 3 号。
func TestWithFilesC1ThenC3StopsAtSecondAccount(t *testing.T) {
	resetFailoverE2E(t)
	rtMu.Lock()
	rtVal.StrictModelCooldownMin = 300 // markDowngraded 需要冷却分钟数 >0
	rtMu.Unlock()
	a1 := addTestAccount(t, "mix-a")
	a2 := addTestAccount(t, "mix-b") // 备号（脚本默认 C3）
	a3 := addTestAccount(t, "mix-c")
	calls := stubOncePickReal(t, func(id int64) (*StreamResult, error) {
		switch id {
		case a1:
			return &StreamResult{AccountID: a1},
				&diagErr{err: errEmptyContent, reason: reasonNoContentFrame} // C1
		default:
			return &StreamResult{AccountID: id},
				&ModelDowngradedError{Requested: "gemini-3.6-flash", Reported: "flash-lite", AccountID: id}
		}
	})
	mc := Models["gemini-3.6-flash"]
	_, err := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("C1 + C3 混合链无成功号应返回错误")
	}
	// faultHops=1（C1 跳槽）后 C3 禁止再跳第 3 号：a3 绝不该被试到。
	for _, id := range *calls {
		if id == a3 {
			t.Fatalf("faultHops>=1 后 C3 禁止再跳第 3 号，a3 不该被试到：calls=%v", *calls)
		}
	}
	// R2-MF-1 终结路径降级登记（R3-SF-1 一并覆盖）：a2 是混合链第 2 号，
	// faultHops>=1 时 tryAdvanceHop(classModelDowngrade) 返 false 终结，
	// 但终结号同样必须记入模型冷却——e1c2cbe 只在 hop-true 分支登记，
	// a2 漏记必然 FAIL。
	cooling, _ := coolingAccounts(mc)
	if !cooling[a2] {
		t.Fatalf("混合链终结号 a2 必须 markDowngraded：cooling=%v", cooling)
	}
}

// 端到端：外层 FallbackAnon 兜底——故障跳槽预算（faultHops 上限 1）在
// 前一个号消耗尽后，又遇 C4a（鉴权/XSRF 不可用）终态且无附件时，外层
// 统一匿名兜底返回成功（forceAnon 分支，不再换号）。
func TestWithFilesFallbackAnonOnAuthFail(t *testing.T) {
	resetFailoverE2E(t)
	addTestAccount(t, "anon-a")
	addTestAccount(t, "anon-b")
	calls := stubOncePickReal(t, func(id int64) (*StreamResult, error) {
		// 两号都 C4a：第 1 跳消耗 faultHops=1 跳到第二号；第二号再 C4a 时
		// tryAdvanceHop 返 false -> advanceOuterHop 落 FallbackAnon 分支。
		return &StreamResult{AccountID: id}, errors.New("no SNlM0e token")
	})
	mc := Models["gemini-3.6-flash"]
	res, err := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	// C4a 终态 + FallbackAnon + 无附件：外层统一匿名兜底成功（AccountID=0）。
	if err != nil || res == nil || res.AccountID != 0 {
		t.Fatalf("C4a+FallbackAnon 应匿名兜底成功，得到 res=%+v err=%v calls=%v", res, err, *calls)
	}
	// 两号都被试到（第 2 跳消耗完故障预算才落兜底）。
	if len(*calls) != 2 {
		t.Fatalf("FallbackAnon 前应把两号都试过，calls=%v", *calls)
	}
}

// 端到端：单号池 C4a + FallbackAnon=true（R2-MF-2 / Reviewer M1）——
// 唯一账号鉴权失败后，tryAdvanceHop 消耗 faultHops 允许跳号，但
// pickAccountForModel 因 exclude 非空无备号返回 noEligibleAccountError；
// 终结路径必须同样落外层 runAnonFallback 匿名兜底，而不是直接把 C4a
// 错误丢给客户端。
func TestWithFilesFallbackAnonSingleAccountPool(t *testing.T) {
	resetFailoverE2E(t)
	a1 := addTestAccount(t, "anon-single")
	calls := stubOncePickReal(t, func(id int64) (*StreamResult, error) {
		// 单号 C4a：第 0 跳鉴权失败，第 1 跳无备号可挑。
		return &StreamResult{AccountID: id}, errors.New("no SNlM0e token")
	})
	mc := Models["gemini-3.6-flash"]
	res, err := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	// 单号池 C4a 终结路径必须经外层 FallbackAnon 匿名兜底成功
	//（forceAnon 分支 res.AccountID=0），绝不直接向上游报 502。
	if err != nil || res == nil || res.AccountID != 0 {
		t.Fatalf("单号池 C4a+FallbackAnon 应匿名兜底成功，得到 res=%+v err=%v calls=%v", res, err, *calls)
	}
	if len(*calls) != 1 {
		t.Fatalf("单号池只能试到 1 个号，calls=%v", *calls)
	}
	// a1 鉴权失败应被记一次账号失败（C4a 立即记账）。
	accountHealthMu.Lock()
	st1 := accountHealth[a1]
	accountHealthMu.Unlock()
	if st1 == nil || st1.failStreak != 1 {
		t.Fatalf("C4a 应立即给 a1 记一次失败，得到 %+v", st1)
	}
}

// 端到端：终结路径的 C3 降级号必须登记 markDowngraded（R2-MF-1 / R3-SF-1）——
// 三号池三个号都返回 C3 降级：第 1、2 跳降级消耗 downgradeTry 跳号，第 3 号
// （downgradeTry=2, 2+1>=maxCookieTries）tryAdvanceHop 返 false 终结——终结号
// 同样必须记入模型冷却（否则下个请求还会挑中它再降级一次）。修复前
// （e1c2cbe）markDowngraded 只在 tryAdvanceHop 返回 true 的分支里调，a3 漏记
// 冷却必然 FAIL。末尾再发第二条请求断言全池冷却后的降级错带回
// RetryAfter>0（R2-SF-2 的 nee.retryAfter() 回填：nee 的最早解冻取自
// model/short 冷却集合，纯首跳降级错本身不携带）。
func TestWithFilesTerminalDowngradeMarksCooldown(t *testing.T) {
	resetFailoverE2E(t)
	rtMu.Lock()
	rtVal.StrictModelCooldownMin = 300 // markDowngraded 需要冷却分钟数 >0
	rtMu.Unlock()
	a1 := addTestAccount(t, "dg-a")
	a2 := addTestAccount(t, "dg-b")
	a3 := addTestAccount(t, "dg-c")
	calls := stubOncePickReal(t, func(id int64) (*StreamResult, error) {
		return &StreamResult{AccountID: id},
			&ModelDowngradedError{Requested: "gemini-3.6-flash", Reported: "flash-lite", AccountID: id}
	})
	mc := Models["gemini-3.6-flash"]
	_, err := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("三号都 C3 降级应返回降级错误")
	}
	var mde *ModelDowngradedError
	if !errors.As(err, &mde) {
		t.Fatalf("终态应报 ModelDowngradedError，得到 %v", err)
	}
	// 三个号都被试到且都已确认配额耗尽：终结号 a3 同样必须进冷却。
	if len(*calls) != 3 {
		t.Fatalf("纯 C3 链应试满 3 号（降级预算独立于故障预算），calls=%v", *calls)
	}
	cooling, _ := coolingAccounts(mc)
	if !cooling[a1] || !cooling[a2] || !cooling[a3] {
		t.Fatalf("终结路径降级号必须 markDowngraded：cooling=%v (want a1+a2+a3)", cooling)
	}

	// R2-SF-2 / Reviewer R3 #2：全池冷却后加一条新鲜号 dg-d 再来一条请求
	// ——try=0 挑中 dg-d 返回 RetryAfter=0 的 MDE，try=1 因 exclude={dg-d}
	// 且 a1/a2/a3 均在 modelCooling 返回 noEligibleAccountError{modelRetryAfter
	// ≈5h}，100% 独立命中 resolveHopPickErr 的 nee + prevErr!=nil →
	// resolveNoEligibleFallback 回填分支：即便单独回退 R2-SF-2（nee 回填被
	// prevErr 短路）也必然 FAIL，不耦合 R2-MF-1 的冷却登记修复。
	_ = addTestAccount(t, "dg-d")
	_, err2 := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	var mde2 *ModelDowngradedError
	if !errors.As(err2, &mde2) {
		t.Fatalf("全池冷却后应报 ModelDowngradedError，得到 %v", err2)
	}
	if mde2.RetryAfter <= 0 {
		t.Fatalf("全池降级终结错误必须回填 RetryAfter>0，得到 %v", mde2.RetryAfter)
	}
}

// 端到端：上游 429（C2）跨号跳槽——主号 429 立即记账进短冷却，跳备号成功。
func TestWithFilesUpstream429Hops(t *testing.T) {
	resetFailoverE2E(t)
	a1 := addTestAccount(t, "u429-a")
	a2 := addTestAccount(t, "u429-b")
	calls := stubOncePickReal(t, func(id int64) (*StreamResult, error) {
		if id == a1 {
			return &StreamResult{AccountID: a1}, errors.New("upstream HTTP 429")
		}
		return &StreamResult{AccountID: a2, Raw: conversationTestRaw(t, "answer")}, nil
	})
	mc := Models["gemini-3.6-flash"]
	res, err := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	if err != nil || res == nil || res.AccountID != a2 {
		t.Fatalf("429 应跳备号成功，得到 res=%+v err=%v", res, err)
	}
	if len(*calls) != 2 {
		t.Fatalf("429 故障跳槽至多碰 2 号，实际 calls=%v", *calls)
	}
	// C2 立即记账：a1 记一次失败。
	accountHealthMu.Lock()
	st1 := accountHealth[a1]
	accountHealthMu.Unlock()
	if st1 == nil || st1.failStreak != 1 {
		t.Fatalf("C2 应立即给 a1 记一次失败，得到 %+v", st1)
	}
}

// 端到端：callGeminiConv 多轮 C1b 先校验后记账 + 延迟结算——fresh 第 0 跳
// 返回空输出（validateConvTurnOutput 检出 C1b errEmptyContent）暂挂 pendingHop0
// 跨号跳到备号成功后：备号成功记账落定，第 0 跳 C1b 不记账号失败（C1b 不拉黑名单）。
// 关键是 C1b 校验必须在 recordAccountReqSuccess/commitPendingProxy 之前：
// 若先记成功再校验，第 0 跳的空输出号会被误记成成功（failStreak 清零、探测位误放）。
func TestCallGeminiConvC1bValidatesBeforeAccounting(t *testing.T) {
	resetConvBoundState(t)
	resetAccountHealthForTest(t)
	a1 := addTestAccount(t, "conv-c1b-a")
	a2 := addTestAccount(t, "conv-c1b-b")
	var tried []int64
	// 真实挑号 + 按号编剧：第 0 跳有内容帧但抽不出正文（C1b），备号成功。
	stubConversationTestTurn(t, func(ctx context.Context, prompt string, mc ModelConfig, conv *convState) (*StreamResult, error) {
		tried = append(tried, conv.accountID)
		if conv.accountID == a1 {
			// 只含思考链（reasoning）无正文：hasContentFrame=true 但
			// extractResponseText=\"\" -> validateConvTurnOutput 判 C1b empty_content。
			return &StreamResult{AccountID: a1, Raw: convReasoningOnlyRaw(t)}, nil
		}
		conv.cid = "conv-c1b"
		return &StreamResult{AccountID: a2, Raw: conversationTestRaw(t, "answer")}, nil
	})
	first := []map[string]interface{}{{"role": "user", "content": "q"}}
	mc := Models["gemini-3.6-flash"]
	_, _, res, err := callGeminiConv(context.Background(), first, mc, nil, nil, nil, nil,
		conversationOptions{requestScope: "conv-c1b"})
	if err != nil {
		t.Fatalf("C1b 应跳备号成功，得到 err=%v tried=%v", err, tried)
	}
	if res == nil || res.AccountID != a2 {
		t.Fatalf("应落在备号 #%d，得到 res=%+v", a2, res)
	}
	// 第 0 跳 C1b：不记账号失败（C1b 非拉黑类），但跨号跳槽发生证明校验
	// 在记账之前判定——先记成功再校验的话 a1 会被记成成功号。
	accountHealthMu.Lock()
	st1 := accountHealth[a1]
	accountHealthMu.Unlock()
	if st1 != nil && st1.failStreak > 0 {
		t.Fatalf("C1b 不该给第 0 跳号记账号失败，得到 %+v", st1)
	}
	if st1 != nil && !st1.lastSuccessAt.IsZero() && st1.lastSuccessAt.After(time.Now().Add(-time.Minute)) {
		// 校验若在记账之后：a1 会被先记成成功（lastSuccessAt 近当前）。
		t.Fatalf("C1b 校验必须先于成功记账：a1 不应被记成功 %+v", st1)
	}
}

// MF-2：recordAccountReqSuccess 在 st==nil（从未失败过的健康号）时也必须
// 建项写 lastSuccessAt——否则因果时序比对拿不到基准，迟到的旧失败会把
// 新成功倒拨覆盖。
func TestRecordSuccessCreatesEntryGuardingStaleFailure(t *testing.T) {
	resetAccountHealthForTest(t)
	acctID := int64(9101)
	reqNew := nextClientReqID()
	// 新请求在该号上真实成功（健康号此前从未失败，st==nil）：必须建项。
	recordAccountReqSuccess(acctID, reqNew)
	accountHealthMu.Lock()
	st := accountHealth[acctID]
	accountHealthMu.Unlock()
	if st == nil {
		t.Fatal("健康号首次成功也必须建 accountHealth 项（写 lastSuccessAt）")
	}
	if st.lastSuccessAt.IsZero() {
		t.Fatal("st==nil 建项后必须写 lastSuccessAt（因果时序基准）")
	}

	// 模拟一个「迟到提交」的旧失败：occurredAt 早于 lastSuccessAt。
	// 等价串行序 t1 失败 -> t2 成功：迟到失败绝不能倒拨覆盖成功态。
	staleAt := st.lastSuccessAt.Add(-time.Second)
	reqOld := nextClientReqID()
	recordAccountReqFailureAt(acctID, reqOld, classUpstreamLimit, staleAt)
	accountHealthMu.Lock()
	st2 := accountHealth[acctID]
	accountHealthMu.Unlock()
	if st2.failStreak != 0 {
		t.Fatalf("迟到的旧失败不该倒拨覆盖新成功（failStreak 应保持 0），得到 %+v", st2)
	}
}

// MF-2：代理记账「成功优先」——attempt0 失败 attempt1 成功的重试最终成功，
// commitPendingProxy 只能记成功清零，不能让首轮失败覆盖。
func TestProxyRetrySuccessWinsAccounting(t *testing.T) {
	a, _ := withTwoProxies(t, true)
	res := &StreamResult{ProxyID: a}
	// attempt0：失败暂挂。
	noteProxyResult(res, a, false, "dial tcp refused", true)
	if res.PendingProxyFail == "" || res.PendingProxyOK {
		t.Fatalf("首轮失败应暂挂 PendingProxyFail，得到 %+v", res)
	}
	// attempt1：成功——成功优先，清掉首轮失败暂挂。
	noteProxyResult(res, a, true, "", true)
	if !res.PendingProxyOK || res.PendingProxyFail != "" {
		t.Fatalf("成功应清掉失败暂挂并置 PendingProxyOK，得到 %+v", res)
	}
	// 基线：a 当前 fail_count（withTwoProxies 可能带历史值）。
	base := proxyByID(a).FailCount
	commitPendingProxy(res)
	// 成功优先落库：fail_count 记 0（清零），不是首轮失败那次 +1。
	if got := proxyByID(a).FailCount; got != 0 {
		t.Fatalf("成功优先应把 fail_count 清 0（首轮失败不覆盖），得到 %d（基线 %d）", got, base)
	}
	if !res.proxyCommitted {
		t.Fatal("commitPendingProxy 应幂等置 proxyCommitted")
	}
}

// MF-3：callOnVideoAccount 全池 45s 短冷却原地回退——回退挑出的活跃短冷却号
// 绝不能再过 tryClaimHalfOpenProbe（它对活跃冷却恒 false，过了等于 100%
// 拒杀回退号），视频任务必须能原地重试到这些号。
func TestCallOnVideoAccountShortCooldownInPlaceFallback(t *testing.T) {
	resetAccountHealthForTest(t)
	var tried []int64
	ids := withVideoAccounts(t, 2, func(mc ModelConfig) (*StreamResult, error) {
		tried = append(tried, mc.PinAccountID)
		return &StreamResult{AccountID: mc.PinAccountID,
			Artifacts: []MediaArtifact{{Data: []byte("mp4"), Mime: "video/mp4"}}}, nil
	})
	// 全池推进 45s 活跃短冷却：每个号都 now.Before(cooldownUntil)。
	accountHealthMu.Lock()
	for _, id := range ids {
		accountHealth[id] = &accountShortHealth{
			cooldownUntil: time.Now().Add(30 * time.Second),
			failStreak:    accountShortFailLimit,
		}
	}
	accountHealthMu.Unlock()
	j := newTestJob()
	runVideoJob(j, nil)
	if j.Status != "completed" {
		t.Fatalf("全池短冷却原地回退应完成任务，得到 status=%s err=%q tried=%v",
			j.Status, j.Error, tried)
	}
	if len(tried) == 0 {
		t.Fatal("短冷却号应被回退重试到，实际一次都没试")
	}
	// 回退后不被 tryClaimHalfOpenProbe 拒杀：至少有一个号真的进了 videoCall。
	found := false
	for _, id := range ids {
		for _, tr := range tried {
			if tr == id {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("回退号未被 tryClaimHalfOpenProbe 拒杀前应被试到，tried=%v ids=%v", tried, ids)
	}
}

// priority=100 Bound 换号契约（proxy_exhaustion_test.go 迁移）：priority 主备
// 排序下，主号绑在满出口 Bound 受阻后，必须换到绑空闲出口的备号（不是让
// 主号借备号的出口，也不是报全池降级）。
func TestPriorityBoundSwitchContract(t *testing.T) {
	resetFailoverE2E(t)
	a, b := withTwoProxies(t, true)
	main := addTestAccount(t, "prio-main")
	backup := addTestAccount(t, "prio-backup")
	if _, err := db.Exec(`UPDATE accounts SET priority=100 WHERE id=?`, main); err != nil {
		t.Fatalf("设主号 priority=100 失败: %v", err)
	}
	if _, err := db.Exec(`UPDATE accounts SET priority=10 WHERE id=?`, backup); err != nil {
		t.Fatalf("设备号 priority=10 失败: %v", err)
	}
	bindAccountProxy(main, a)   // 主号绑满出口 A
	bindAccountProxy(backup, b) // 备号绑空闲出口 B
	calls := stubOncePickReal(t, func(id int64) (*StreamResult, error) {
		// 主号先被挑中（priority=100），Bound 受阻；备号成功。
		if id == main {
			return &StreamResult{AccountID: main},
				&RateLimitError{Reason: "concurrent", ProxyID: a, Bound: true}
		}
		return &StreamResult{AccountID: backup, Raw: conversationTestRaw(t, "answer")}, nil
	})
	mc := Models["gemini-3.6-flash"]
	res, err := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	if err != nil || res == nil || res.AccountID != backup {
		t.Fatalf("priority=100 主号 Bound 后应换到备号 #%d，得到 res=%+v err=%v calls=%v",
			backup, res, err, *calls)
	}
	if len(*calls) != 2 || (*calls)[0] != main {
		t.Fatalf("主号应先被挑中再 Bound 换备号，calls=%v", *calls)
	}
}

// 端到端：R3-MF-1——单号池末号在 try=0 遇 C4a 被 autoDisableIfDead 停用后，
// FallbackAnon=false 时绝不能静默走第 2 跳匿名请求。8b2b2d3 上
// resolvePoolExhaustedErr 先判 !hasCookie() 返回 (nil,nil)，pickAccountForModel
// 把「跳槽期无号可挑」误报成「合法空池」→ 外层以匿名身份发第 2 跳（本测试桩
// 把它如实观测为 AccountID==0 的匿名结果，与生产 streamGenerateOnceOpts 拿到
// (nil,nil) 后的真实路径同构）；本提交将 len(exclude)>0 提前后返回
// *noEligibleAccountError，终结路径原样回传上一跳真实的 C4a 错误。
func TestWithFilesAutoDisabledLastAccountRespectsFallbackAnonFalse(t *testing.T) {
	resetFailoverE2E(t)
	rtMu.Lock()
	rtVal.FallbackAnon = false // 显式默认：任何情况下绝不匿名兜底
	rtMu.Unlock()
	a1 := addTestAccount(t, "auto-disabled-single")

	// 桩与 stubOncePickReal 同形（挑号走真实 pickAccountForModel，吃
	// exclude/冷却守卫）；差异只在「挑不到号且 err==nil」时按生产语义返回
	// 匿名结果（AccountID=0）——8b2b2d3 上 streamGenerateOnceOpts 拿到
	// (nil,nil) 后真实会走的静默匿名路径，用它直接观测第 2 跳是否匿名。
	authSentinel := errors.New("no SNlM0e token")
	got := &[]int64{}
	old := runStreamGenerateOnce
	runStreamGenerateOnce = func(ctx context.Context, prompt string, pc promptCtx, mc ModelConfig,
		pending []pendingUpload, onDelta, onReasoning func(string), stopWhen func(string) bool,
		opts streamOnceOpts) (*StreamResult, error) {
		a, _, perr := pickAccountForModel(mc, opts.exclude, opts.fstate)
		if perr != nil {
			return &StreamResult{}, perr
		}
		if a == nil {
			// (nil, nil)：生产会静默以匿名身份发本跳——如实建模成匿名结果。
			return &StreamResult{Raw: conversationTestRaw(t, "anon-2nd-hop"), AccountID: 0}, nil
		}
		*got = append(*got, a.ID)
		// a1 第 3 次鉴权失败达到 fail_count 阈值：autoDisableIfDead 的落库
		// 效果即 status='disabled'（等价标记，不经网络路径）。
		_ = accountSetStatus(a.ID, "disabled")
		return &StreamResult{AccountID: a.ID}, authSentinel
	}
	t.Cleanup(func() { runStreamGenerateOnce = old })

	mc := Models["gemini-3.6-flash"]
	res, err := streamGenerateWithFilesOpts(context.Background(), "hi",
		promptCtx{Latest: "hi"}, mc, nil, nil, nil, nil, nil)
	// 必须原样回传上一跳真实的 C4a 错误，绝不返回匿名成功（AccountID=0）。
	if err == nil || (res != nil && res.AccountID == 0) {
		t.Fatalf("FallbackAnon=false 且末号被自动停用后绝不能匿名外发：res=%+v err=%v", res, err)
	}
	if !errors.Is(err, authSentinel) {
		t.Fatalf("终结路径应回传上一跳真实的 C4a 错误，得到 %v", err)
	}
	if len(*got) != 1 || (*got)[0] != a1 {
		t.Fatalf("只有 a1 应被真实挑中一次，calls=%v", *got)
	}
}

// 端到端：R2-SF-1——xsrfHealOrAdvance 在 singleAccountTry=true 下遇
// out.boundErr（XSRF 轮转期间号被并发改绑到另一启用出口）必须立即上抛
// *RateLimitError{Bound:true} 给外层，且先释放本请求在 a1 上占着的半开
// 探测位；绝不在内层偷换到 a2（内层换号会带着陈旧出口槽外发）。
// e1c2cbe 上该分支缺 singleAccountTry 上抛与探测位释放：返回 out.next=a2
// 让内层就地换号（a2 归属进最终错误）且 probeInFlight 残留 → FAIL；
// 最新 HEAD 立即上抛 Bound 冲突并放探测位 → PASS。
func TestAcquireXSRFHealExitConflictReleasesProbeAndBubbles(t *testing.T) {
	resetFailoverE2E(t)
	pA, pB := withTwoProxies(t, true) // strict_proxy_binding：冲突仲裁走 claimAccountProxy
	rtMu.Lock()
	rtVal.FallbackAnon = false // 内层/外层都禁止匿名兜底，错必须原样上抛
	rtMu.Unlock()

	a1 := addTestAccount(t, "heal-conflict-a")
	a2 := addTestAccount(t, "heal-conflict-b")
	// a1/a2 同绑 A（picked 出口）：e1c2cbe 的内层换号会跳过 strictBoundElsewhere
	// 过滤真实挑出 a2——把「偷换号」从不可能变成旧代码的真实可达路径。
	bindAccountProxy(a1, pA)
	bindAccountProxy(a2, pA)

	// a1 置半开探测占用态（本请求 fstate.reqID 认领）+ 冷却刚到期。
	fstate := newReqFailoverState(false)
	defer releaseReqProbes(fstate.reqID)
	accountHealthMu.Lock()
	accountHealth[a1] = &accountShortHealth{
		cooldownUntil:  time.Now().Add(-time.Second),
		probeInFlight:  true,
		probeReqID:     fstate.reqID,
		probeStartedAt: time.Now(),
	}
	accountHealthMu.Unlock()

	// 把出口 A 的 stdlibClientCache 换成假 RoundTripper：
	//   - /app 回 200 空页 → getXSRF 报 no SNlM0e 失败进 heal 路径；
	//   - /app 命中的瞬间把 a1 改绑到另一启用出口 B——模拟管理员在
	//     挑号之后、XSRF 轮转/重取之间的改绑窗口（heal 的冲突复核必然命中，
	//     而入口 resolveXSRFConflict 时 a1 仍绑 A 不会提前冲突）；
	//   - 其余路径（/RotateCookiesPage 等）一律 dial refused → 轮转失败。
	timeout := rtCfg().RequestTimeout
	for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
		stdlibClientCache.Store("http://127.0.0.1:1"+suf, &http.Client{Transport: roundTripperFunc(
			func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/app" {
					bindAccountProxy(a1, pB)           // 改绑窗口：a1 被绑走 B
					return respJSON(req, 200, ""), nil // 空页：无 SNlM0e
				}
				return nil, errors.New("dial refused")
			})})
	}
	defer func() {
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Delete("http://127.0.0.1:1" + suf)
		}
	}()

	xs := &xsrfTokenCtx{
		acct:             accountByID(a1),
		skip:             map[int64]bool{},
		picked:           Proxy{ID: pA, URL: "http://127.0.0.1:1"},
		proxyURL:         "http://127.0.0.1:1",
		mc:               Models["gemini-3.6-flash"],
		fstate:           fstate,
		singleAccountTry: true,
	}
	var attrErr error
	attrib := func(err error) (*StreamResult, error) {
		attrErr = err
		return &StreamResult{AccountID: xs.cookieID}, err
	}

	res, rerr, done := acquireXSRFToken(context.Background(), xs, attrib)
	if !done {
		t.Fatal("出口冲突应终局返回 done=true")
	}
	var rle *RateLimitError
	if !errors.As(rerr, &rle) || !rle.Bound || rle.ProxyID != pB {
		t.Fatalf("冲突必须上抛 *RateLimitError{Bound:true, ProxyID=%d}，得到 err=%v res=%+v", pB, rerr, res)
	}
	// 上抛错误必须经 attrib 原样返回 Bound 冲突（与返回值同源）。
	if !errors.Is(attrErr, rerr) {
		t.Fatalf("上抛错误应经 attrib 原样返回 Bound 冲突：attrErr=%v rerr=%v", attrErr, rerr)
	}
	// 内层绝没偷换到 a2：e1c2cbe 的内层换号路径生效时 next=a2 会继续走
	// getXSRF 失败并把归属落在 a2——这里 a2 的 ID 绝不能成为最终归属。
	if res != nil && res.AccountID == a2 {
		t.Fatalf("singleAccountTry 下绝不能在内层偷换到 a2：res=%+v", res)
	}
	// a1 的半开探测位必须已释放（切走旧号先放探测位）。
	accountHealthMu.Lock()
	st := accountHealth[a1]
	accountHealthMu.Unlock()
	if st != nil && st.probeInFlight {
		t.Fatalf("a1 的探测位在冲突上抛后必须已释放：st=%+v", st)
	}
}
