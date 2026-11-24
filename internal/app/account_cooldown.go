package app

// 防雪崩（Anti-Avalanche）延迟提交状态机（v6.6.0 模块二）。
//
// 解决的问题：毒载荷（内容拒绝类瞬时失败）在主号上失败后，旧实现立刻写全局
// 失败计数并扣代理分；换到备号才发现是载荷本身被拒（双号同错），主号已经
// 被冤枉记账——连续两次 45s 冷却就把健康主号踢出轮转，请求全部压向备号雪崩。
//
// 设计（v2.2 补丁 §4/§5/§7/§8/§10[R3-S1]）：
//   - 账号短冷却：同一账号在 N=2 次独立请求（不同 reqID）真实失败后进入
//     45s 短冷却，冷却期间挑号跳过；到期进入半开态，仅放行一条单飞探测。
//   - 延迟账号失败提交（Deferred Account Failure Commit）：C1/C1b（毒载荷
//     敏感类）第 0 跳失败只暂挂栈变量 pendingHop0，不写全局；第 1 跳落定后
//     按 isPoisonPair 判定——双端同落 {C1,C1b} 是毒载荷，两端零记账零冷却
//     零扣代理分；否则按首次 occurredAt 因果时序单次提交。
//   - reqFailoverState 统一 Hop 准入：故障类（C1/C1b/C2/C4a/C5a）跨号跳槽
//     最多 1 次（含故障的请求至多触碰 2 个号）；纯 C3 模型降级保留
//     maxCookieTries=3 的预算。
//   - 半开单飞 probeInFlight：冷却到期的号一次只放行一条探测请求，其余
//     并发仍视为冷却，杜绝瞬时并发回切打爆未恢复的主号。探测占位带
//     maxHalfOpenProbeHold 兜底租约，超 10m 静默复位，物理封死永久黑洞。

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// maxCrossAccountHops 是故障/毒载荷敏感类（C1/C1b/C2/C4a/C5a）单请求最多
	// 跨号跳槽次数：1 次 = 最多触碰 2 个账号（第 0 跳 + 第 1 跳双号交叉验证）。
	maxCrossAccountHops = 1
	// defaultShortFailLimit 是连续独立真实请求失败触发短冷却的阈值。
	defaultShortFailLimit = 2
	// defaultShortCooldownDur 是账号短冷却时长。
	defaultShortCooldownDur = 45 * time.Second
	// accountFailureStreakWindow 是相邻两次独立失败算「连续」的最大间隔：
	// 隔太久的失败各自独立，不该攒成冷却。
	accountFailureStreakWindow = 10 * time.Minute
	// halfOpenSilentReset 是半开状态的最长存活：冷却到期超过它就静默复位，
	// 不长时间占着「等待探测」的槽位。
	halfOpenSilentReset = 10 * time.Minute
)

var (
	// 可调（测试覆盖用）：生产值恒为 default*。
	accountShortFailLimit   = defaultShortFailLimit
	accountShortCooldownDur = defaultShortCooldownDur

	clientReqSeq    atomic.Uint64
	accountHealthMu sync.Mutex
	accountHealth   = map[int64]*accountShortHealth{}
)

// accountShortHealth 是一个账号的短冷却/半开探测状态（受 accountHealthMu 保护）。
type accountShortHealth struct {
	failStreak    int
	lastFailAt    time.Time
	lastFailReqID uint64    // 本次 reqID 幂等：同请求同号重试 N 次最多计 1 次
	lastSuccessAt time.Time // 延迟提交时做因果时序比对用
	cooldownUntil time.Time
	// 半开单飞探测：冷却到期后第一条请求认领 probeInFlight，其余并发仍算冷却。
	probeInFlight  bool
	probeReqID     uint64
	probeStartedAt time.Time // 兜底租约起点，超 maxHalfOpenProbeHold 自动回收
}

// nextClientReqID 给一次客户端请求分配单调递增的 reqID：同请求内层重试
// 共享同一个，跨号跳槽也共享——账号失败幂等按它判「同请求不算第二次」。
func nextClientReqID() uint64 {
	return clientReqSeq.Add(1)
}

// maxHalfOpenProbeHold 是半开探测占位的兜底租约：探测请求自己崩溃/漏释放时
// 超时自动回收，绝不永久黑洞。
func maxHalfOpenProbeHold() time.Duration {
	d := time.Duration(rtCfg().RequestTimeout)*time.Second + 30*time.Second
	if d < 5*time.Minute {
		d = 5 * time.Minute
	}
	return d
}

// AccountCooldownError 表示所有可挑的登录账号都在 45s 短冷却中。
// HTTP 层统一映射 429 + Retry-After + code=accounts_cooling_down。
type AccountCooldownError struct {
	RetryAfter time.Duration
}

func (e *AccountCooldownError) Error() string {
	return fmt.Sprintf("all cookie accounts are in short cooldown after repeated upstream failures "+
		"(anti-avalanche), retry after %s", e.RetryAfter.Round(time.Second))
}

// noEligibleAccountError 是跳槽选备号时「池非空但没有可挑的号」的内部错误：
// 绝不返回 (nil, skip, nil) 让调用方误退匿名——匿名对登录模型同样是降级。
type noEligibleAccountError struct {
	modelRetryAfter time.Duration // strict_model 全池冷却的最早解冻
	shortRetryAfter time.Duration // 45s 短冷却的最早解冻
}

func (e *noEligibleAccountError) Error() string {
	return "no eligible account left for failover in this request"
}

// retryAfter 取两类冷却里最早能再试的时间（供外层填 Retry-After）。
func (e *noEligibleAccountError) retryAfter() time.Duration {
	d := e.modelRetryAfter
	if d == 0 || (e.shortRetryAfter > 0 && e.shortRetryAfter < d) {
		d = e.shortRetryAfter
	}
	return d
}

// failoverClass 是统一换号准入的五类错误分类（真值表见设计 §3.5）。
type failoverClass int

const (
	classNone           failoverClass = iota // 无错误 / 不可分类（默认不跳槽）
	classContentRefuse                       // C1/C1b：no_content_frame / failure_reply / errEmptyContent（毒载荷敏感）
	classUpstreamLimit                       // C2：上游 429 / 302→sorry（账号立刻记账）
	classModelDowngrade                      // C3：*ModelDowngradedError（降级预算，单独计）
	classAuthFail                            // C4a：401/403 / XSRF 鉴权错（账号立刻记账）
	classLocalBound                          // C4b：门前本地 Bound 满槽/冲突（未碰上游，豁免预算）
	classNetFail                             // C5a：网络/代理/导航/超时/5xx（代理记账不记账号）
	classLocalAbort                          // C5b：本地/客户端终止（禁止跳槽）
)

// classifyFailoverError 把一跳的错误归到统一跳槽分类。
// res 非 nil 时用其 Diag.Reason 与归属辅助判定（attrib 已带结构化现场）。
func classifyFailoverError(err error) failoverClass {
	if err == nil {
		return classNone
	}
	var ppe *poisonPayloadError
	if errors.As(err, &ppe) {
		return classContentRefuse
	}
	var mde *ModelDowngradedError
	if errors.As(err, &mde) {
		return classModelDowngrade
	}
	var ace *AccountCooldownError
	if errors.As(err, &ace) {
		return classLocalAbort // 全池冷却：不换号了，直接报
	}
	var nee *noEligibleAccountError
	if errors.As(err, &nee) {
		return classLocalAbort
	}
	var pae *pinnedAccountError
	if errors.As(err, &pae) {
		return classLocalAbort
	}
	var rle *RateLimitError
	if errors.As(err, &rle) {
		if rle.Bound {
			return classLocalBound // C4b：门前本地出口满/冲突，豁免跳槽预算
		}
		return classLocalAbort // 全 slot 满：换号也拿不到出口
	}
	var ptl *PromptTooLongError
	if errors.As(err, &ptl) {
		return classLocalAbort
	}
	if isBrowserLocalError(err) {
		return classLocalAbort
	}
	if errors.Is(err, errUpstreamFailureReply) {
		return classContentRefuse // C1
	}
	if errors.Is(err, errEmptyContent) {
		return classContentRefuse // C1b
	}
	var de *diagErr
	if errors.As(err, &de) {
		switch de.reason {
		case reasonNoContentFrame:
			return classContentRefuse // C1
		case reasonEmptyContent:
			return classContentRefuse // C1b
		case reasonFailureReply:
			return classContentRefuse // C1
		}
	}
	if strings.Contains(err.Error(), "no content frame") {
		return classContentRefuse
	}
	if xsrfErrBlamesCookie(err) {
		return classAuthFail // C4a：XSRF 鉴权类
	}
	if isUpstreamTimeout(err) || errors.Is(err, errStreamIdle) {
		return classNetFail // C5a
	}
	var se *StallError
	if errors.As(err, &se) {
		return classNetFail
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return classNetFail
	}
	if strings.Contains(err.Error(), "upstream HTTP 429") ||
		strings.Contains(err.Error(), "upstream HTTP 302") ||
		strings.Contains(err.Error(), "sorry") {
		return classUpstreamLimit // C2
	}
	if strings.Contains(err.Error(), "upstream HTTP 401") ||
		strings.Contains(err.Error(), "upstream HTTP 403") {
		return classAuthFail // C4a
	}
	if strings.Contains(err.Error(), "upstream HTTP") {
		return classNetFail // 5xx 等归 C5a
	}
	return classNetFail // 未分类的网络/未知错误按 C5a（不拉黑好账号）
}

// classIsFault 报告该类是否消耗故障跳槽预算（faultHops）。
func classIsFault(c failoverClass) bool {
	switch c {
	case classContentRefuse, classUpstreamLimit, classAuthFail, classNetFail:
		return true
	}
	return false
}

// classAccountsBlame 报告该类是否立刻给账号记短冷却失败。
// C2（上游 429/sorry）与 C4a（鉴权失效）立即提交；C1 走延迟提交；
// C5a/C4b/C3 不记账号（责任在出口/不在号）。
func classAccountsBlame(c failoverClass) bool {
	return c == classUpstreamLimit || c == classAuthFail
}

// isPoisonPair 判定双跳是否同落毒载荷敏感类（C1/C1b 互认）：
// 主备两个不同账号对同一载荷报同一类瞬时拒绝 = 载荷本身被拒，
// 两端零记账零冷却零扣代理分。
func isPoisonPair(err0, err1 error) bool {
	c0, c1 := classifyFailoverError(err0), classifyFailoverError(err1)
	return c0 == classContentRefuse && c1 == classContentRefuse
}

// poisonPayloadError 包一层毒载荷判定结果：Unwrap 透传原始 reason，
// Hint 说明双号均拒绝该载荷且已豁免熔断记账。
type poisonPayloadError struct {
	err error
}

func (e *poisonPayloadError) Error() string {
	return "poison payload: two accounts rejected the same prompt with a content refusal; " +
		"both were exempted from cooldown accounting. inner: " + e.err.Error()
}
func (e *poisonPayloadError) Unwrap() error { return e.err }

func wrapPoisonPayloadError(err error) error {
	if err == nil {
		return nil
	}
	return &poisonPayloadError{err: err}
}

// reqFailoverState 是一条客户端请求跨号跳槽的统一准入状态（顶层共享，
// 贯通单轮外层、streamGenerateOnce XSRF 内层与 conversation 多轮）。
type reqFailoverState struct {
	reqID        uint64
	pinned       bool
	touched      map[int64]bool // 已对上游发过请求的账号
	faultHops    int            // 故障类跳槽次数，上限 maxCrossAccountHops
	downgradeTry int            // 纯 C3 降级跳槽次数，上限 maxCookieTries-1
}

func newReqFailoverState(pinned bool) *reqFailoverState {
	return &reqFailoverState{reqID: nextClientReqID(), pinned: pinned, touched: map[int64]bool{}}
}

// markTouched 在该账号对上游发出任何请求（getXSRF/上传/StreamGenerate）之前登记。
// Bound 门前错误在 touched 之前命中，不算「试过一个号」。
func (f *reqFailoverState) markTouched(id int64) {
	if f != nil && id > 0 {
		f.touched[id] = true
	}
}

// tryAdvanceHop 统一判定「这次失败能不能再跨一个号」。
// cls 是上一跳的错误分类；返回 false 表示预算耗尽/本地终止，必须停。
//
// 混合链不变量（v2.2 §4）：
//   - C4b 门前 Bound：仅当该号尚未 touched（没对上游发过请求）时豁免预算；
//   - C3 降级：faultHops==0 时最多再跳 2 次（合计触碰 <= 3 号）；一旦发生过
//     任意故障跳槽（faultHops>=1），后续即使遇 C3 也禁止再跳；
//   - 故障类（C1/C1b/C2/C4a/C5a）：faultHops==0 且 touched<3 时允许至多 1 次
//     跳槽（faultHops++），之后封顶——含故障的请求至多触碰 2 个账号；
//   - C5b/本地终止：绝不跳槽。
func (f *reqFailoverState) tryAdvanceHop(cls failoverClass, emitted bool) bool {
	if f == nil || f.pinned || emitted {
		return false
	}
	switch cls {
	case classLocalBound:
		return true // 门前 Bound：没碰上游，豁免预算（外层受 boundBudget 截断）
	case classModelDowngrade:
		if f.faultHops >= 1 {
			return false // 含故障的请求禁止再因降级跳第 3 号
		}
		if f.downgradeTry+1 >= maxCookieTries || len(f.touched) >= maxCookieTries {
			return false
		}
		f.downgradeTry++
		return true
	case classContentRefuse, classUpstreamLimit, classAuthFail, classNetFail:
		if f.faultHops >= maxCrossAccountHops || len(f.touched) >= maxCookieTries {
			return false
		}
		f.faultHops++
		return true
	}
	return false
}

// pendingHopFailure 是延迟提交的栈局部暂挂：第 0 跳 C1/C1b 失败只记这里，
// 不写全局 accountHealth；第 1 跳落定后统一结算。
type pendingHopFailure struct {
	acctID     int64
	cls        failoverClass
	occurredAt time.Time // 首次内层失败时刻（因果时序）
	res        *StreamResult
	err        error
}

// shortCoolingAccounts 返回当前处于活跃短冷却或「半开已被认领」的账号集合，
// 以及最早解冻的时长。顺手回收过期项与超时探测占位。
//
// 半开语义（v2.2 §5）：
//   - now < cooldownUntil：活跃冷却，跳过；
//   - 冷却刚到期（半开）：probeInFlight 已被另一条请求认领的仍视为冷却
//     （保底 1s 解冻），防瞬时并发雪崩回切；未认领的不入 skip（允许认领）；
//   - 探测占位超 maxHalfOpenProbeHold 自动回收（封死永久黑洞）；
//   - 冷却到期超 halfOpenSilentReset 无人探测：静默复位整条健康记录。
func shortCoolingAccounts() (map[int64]bool, time.Duration) {
	out := map[int64]bool{}
	var soonest time.Duration
	now := time.Now()
	accountHealthMu.Lock()
	defer accountHealthMu.Unlock()
	for id, st := range accountHealth {
		if now.Before(st.cooldownUntil) {
			out[id] = true
			left := st.cooldownUntil.Sub(now)
			if soonest == 0 || left < soonest {
				soonest = left
			}
			continue
		}
		if st.cooldownUntil.IsZero() {
			continue // 没进过冷却（或已成功清零）
		}
		// 冷却已到期 = 半开态。
		if now.Sub(st.cooldownUntil) > halfOpenSilentReset {
			resetAccountHealthLocked(st) // 超 10m 没人探测：静默复位
			continue
		}
		if st.probeInFlight {
			if now.Sub(st.probeStartedAt) > maxHalfOpenProbeHold() {
				st.probeInFlight, st.probeReqID = false, 0 // 超时租约回收
				continue
			}
			out[id] = true // 已有请求在探测：其余并发仍视为冷却
			if soonest == 0 || time.Second < soonest {
				soonest = time.Second // 保底解冻估计
			}
		}
	}
	return out, soonest
}

// resetAccountHealthLocked 清零一条健康记录（持锁调）。
func resetAccountHealthLocked(st *accountShortHealth) {
	st.failStreak = 0
	st.lastFailAt = time.Time{}
	st.lastFailReqID = 0
	st.cooldownUntil = time.Time{}
	st.probeInFlight = false
	st.probeReqID = 0
}

// tryClaimHalfOpenProbe 在挑中账号 a 后尝试认领它的半开探测位。
// 返回 false 表示该号处于活跃冷却或探测位已被别的 reqID 占着——调用方应
// 把它并入局部 skip 重挑备号。
func tryClaimHalfOpenProbe(accountID int64, reqID uint64) bool {
	accountHealthMu.Lock()
	defer accountHealthMu.Unlock()
	st := accountHealth[accountID]
	if st == nil || st.cooldownUntil.IsZero() {
		return true // 不在冷却/半开：直接放行
	}
	now := time.Now()
	if now.Before(st.cooldownUntil) {
		return false // 活跃冷却中
	}
	if now.Sub(st.cooldownUntil) > halfOpenSilentReset {
		resetAccountHealthLocked(st)
		return true
	}
	if st.probeInFlight {
		if st.probeReqID == reqID {
			return true // 同一请求重入（内层换号前已认领）
		}
		if now.Sub(st.probeStartedAt) > maxHalfOpenProbeHold() {
			st.probeInFlight = true
			st.probeReqID = reqID
			st.probeStartedAt = now
			return true // 旧占位超时回收，归我
		}
		return false // 探测位被别的请求占着
	}
	st.probeInFlight = true
	st.probeReqID = reqID
	st.probeStartedAt = now
	return true
}

// releaseAccountProbeIfMatch 释放指定账号上属于 reqID 的探测位：
// 只清 (probeInFlight && probeReqID == reqID)——别的请求持有的绝不误清。
func releaseAccountProbeIfMatch(accountID int64, reqID uint64) {
	accountHealthMu.Lock()
	defer accountHealthMu.Unlock()
	if st := accountHealth[accountID]; st != nil && st.probeInFlight && st.probeReqID == reqID {
		st.probeInFlight, st.probeReqID = false, 0
	}
}

// releaseReqProbes 统一释放一条请求占着的全部探测位（顶层 defer 兜底，
// 覆盖任何遗漏的退出路径——毒载荷/Bound/取消都适用）。
func releaseReqProbes(reqID uint64) {
	accountHealthMu.Lock()
	defer accountHealthMu.Unlock()
	for _, st := range accountHealth {
		if st.probeInFlight && st.probeReqID == reqID {
			st.probeInFlight, st.probeReqID = false, 0
		}
	}
}

// releaseAccountProbeIfUnsettled 是逐出口兜底：探测没产生结算（成功/失败
// 都没走到 record*）时清占位，不归罪主号——毒载荷、Bound、ctx 取消、C3
// 降级、C5a 纯代理断连都走这条，半开态留给下一条正常请求探测。
func releaseAccountProbeIfUnsettled(accountID int64, reqID uint64) {
	releaseAccountProbeIfMatch(accountID, reqID)
}

// recordAccountReqSuccess 探测/正常成功：清零 failStreak、出冷却、放探测位。
func recordAccountReqSuccess(accountID int64, reqID uint64) {
	accountHealthMu.Lock()
	defer accountHealthMu.Unlock()
	st := accountHealth[accountID]
	if st == nil {
		return
	}
	st.lastSuccessAt = time.Now()
	st.failStreak = 0
	st.cooldownUntil = time.Time{}
	if !st.probeInFlight || st.probeReqID == reqID {
		st.probeInFlight, st.probeReqID = false, 0
	}
}

// recordAccountReqFailureAt 提交一次真实账号失败（带发生时刻，支持延迟提交的
// 因果时序）：
//   - st.lastFailReqID == reqID：同请求同号幂等返回；
//   - lastSuccessAt 晚于 occurredAt：失败后已有并发请求在该号上真实成功并
//     清零过——不倒拨覆盖这次更新的成功状态（等价串行序 t1 失败 -> t2 成功）；
//   - 间隔超 accountFailureStreakWindow：重新计 1（隔太久不算「连续」）；
//   - 满 accountShortFailLimit 次：进入 45s 短冷却，streak 减半续期。
//
// v2.2 §10[R3-S1]：仅在 probeReqID == reqID 或 renewedCooldown 时清探测位——
// 迟到释放不误清他号探测。
func recordAccountReqFailureAt(accountID int64, reqID uint64, cls failoverClass, occurredAt time.Time) {
	if accountID <= 0 {
		return
	}
	now := time.Now()
	accountHealthMu.Lock()
	defer accountHealthMu.Unlock()
	st := accountHealth[accountID]
	if st == nil {
		st = &accountShortHealth{}
		accountHealth[accountID] = st
	}
	if st.lastFailReqID == reqID {
		return // 同请求同号重试幂等
	}
	if !st.lastSuccessAt.IsZero() && st.lastSuccessAt.After(occurredAt) {
		st.lastFailReqID = reqID
		return // 失败后已有更新的成功清零：不倒拨
	}
	if st.lastFailAt.IsZero() || occurredAt.Sub(st.lastFailAt) > accountFailureStreakWindow {
		st.failStreak = 0
	}
	st.failStreak++
	st.lastFailAt = now
	st.lastFailReqID = reqID
	renewedCooldown := false
	if st.failStreak >= accountShortFailLimit {
		st.cooldownUntil = now.Add(accountShortCooldownDur)
		st.failStreak = accountShortFailLimit - 1 // 半开探测失败一次即续期
		renewedCooldown = true
	}
	if st.probeInFlight && (st.probeReqID == reqID || renewedCooldown) {
		st.probeInFlight, st.probeReqID = false, 0
	}
}

// accountShortCooldownLeftSec 返回账号短冷却剩余秒数（面板展示用）。
func accountShortCooldownLeftSec(accountID int64) int {
	accountHealthMu.Lock()
	defer accountHealthMu.Unlock()
	st := accountHealth[accountID]
	if st == nil {
		return 0
	}
	left := time.Until(st.cooldownUntil)
	if left <= 0 {
		return 0
	}
	return int(math.Ceil(left.Seconds()))
}

// commitPendingProxy 把 StreamResult 里暂挂的代理记账一次性落库。
// 延迟提交模式下代理成败先记在 res（不写库），本函数在每条返回路径统一结算；
// proxyCommitted 幂等防双计。
func commitPendingProxy(res *StreamResult) {
	if res == nil || res.proxyCommitted {
		return
	}
	res.proxyCommitted = true
	switch {
	case res.PendingProxyFail != "":
		recordProxyResult(res.ProxyID, false, res.PendingProxyFail)
	case res.PendingProxyOK:
		recordProxyResult(res.ProxyID, true, "")
	}
}

// noteProxyResult 是延迟提交模式下替代 recordProxyResult 直写的入口：
// deferProxyCommit 开着时写进 res 暂挂（每条请求最多累计 1 次），关着时
// 直接落库（存量语义）。
func noteProxyResult(res *StreamResult, proxyID int64, success bool, errStr string, deferCommit bool) {
	if !deferCommit || res == nil {
		recordProxyResult(proxyID, success, errStr)
		return
	}
	if res.proxyCommitted {
		return
	}
	res.ProxyID = proxyID
	if !success && res.PendingProxyFail == "" {
		res.PendingProxyFail = errStr
	}
	if success {
		res.PendingProxyOK = true
	}
}

// writeAccountCooldown 写 429 accounts_cooling_down + Retry-After（未开流分支）。
func writeAccountCooldown(w http.ResponseWriter, e *AccountCooldownError) {
	if e.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(e.RetryAfter.Seconds()))))
	}
	writeJSON(w, 429, map[string]interface{}{"error": map[string]string{
		"message": e.Error(),
		"type":    "rate_limit_exceeded",
		"code":    "accounts_cooling_down",
	}})
}
