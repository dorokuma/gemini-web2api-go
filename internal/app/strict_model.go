package app

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ModelDowngradedError 表示 strict_model 开着时上游把请求静默降级了。
// 它不是上游故障：同一个号重试只会再降级一次，所以重试循环见到它直接停。
type ModelDowngradedError struct {
	Requested string // 请求模型的显示名（ModelConfig.Upstream）
	Reported  string // 服务端自报实际用的
	AccountID int64  // 被降级的号，0 = 匿名
	// RetryAfter 是全池都在冷却时最早那个号解冻还要多久，0 = 不知道。
	RetryAfter time.Duration
}

func (e *ModelDowngradedError) Error() string {
	if e.Reported == "" {
		return fmt.Sprintf("every cookie account was recently downgraded on %s "+
			"(quota exhausted; strict_model is on), retry after %s", e.Requested, e.RetryAfter.Round(time.Second))
	}
	return fmt.Sprintf("requested %s but upstream answered with %s "+
		"(account quota for this model is likely exhausted; strict_model is on)", e.Requested, e.Reported)
}

// 被降级的号按（账号, 模型）冷却：额度按档位算，Argon 用完了 Flash 照样能用，
// 所以不能把整个号停掉。只放内存——重启后每个号最多再被降级一次就重新记上。
type downgradeKey struct {
	accountID int64
	model     string // ModelConfig.Upstream
}

var (
	downgradeMu    sync.Mutex
	downgradeUntil = map[downgradeKey]time.Time{}
)

func markDowngraded(accountID int64, mc ModelConfig) {
	mins := rtCfg().StrictModelCooldownMin
	if accountID == 0 || mins <= 0 {
		return
	}
	downgradeMu.Lock()
	downgradeUntil[downgradeKey{accountID, mc.Upstream}] = time.Now().Add(time.Duration(mins) * time.Minute)
	downgradeMu.Unlock()
}

// coolingAccounts 返回正在为 mc 冷却的号，以及其中最早那个还要多久解冻。顺手清掉过期项。
func coolingAccounts(mc ModelConfig) (map[int64]bool, time.Duration) {
	out := map[int64]bool{}
	var soonest time.Duration
	now := time.Now()
	downgradeMu.Lock()
	defer downgradeMu.Unlock()
	for k, until := range downgradeUntil {
		left := until.Sub(now)
		if left <= 0 {
			delete(downgradeUntil, k)
			continue
		}
		if k.model != mc.Upstream {
			continue
		}
		out[k.accountID] = true
		if soonest == 0 || left < soonest {
			soonest = left
		}
	}
	return out, soonest
}

// pickAccountForModel 挑号，跳过 exclude（本次请求里已经被降级/故障过的）、
// 正在为 mc 做 strict_model 冷却的、以及 45s 短冷却/半开已认领的号。
// 返回的 skip 是三者合集，换号重取时要接着用它。
//
// 错误契约（v6.6.0 防雪崩，闭合 Oracle M4）：
//   - len(exclude)==0 && !hasCookie()（库内无任何 enabled 账号）→
//     (nil, skip, nil)，合法空池走匿名（exclude 非空优先，末号在上一跳
//     刚被 autoDisableIfDead 停用时绝不静默匿名，R3-MF-1）；
//   - hasCookie() && len(exclude)==0（首跳入口全池冷却）→
//     存在未被 modelCooling 覆盖但处于 shortCooling 的启用号时报
//     *AccountCooldownError{RetryAfter: shortSoonest}，否则报
//     *ModelDowngradedError{RetryAfter: modelSoonest}；
//   - hasCookie() && len(exclude)>0（跳槽选备号无号可挑）→
//     一律报 *noEligibleAccountError，绝不返回 (nil,skip,nil) 误退匿名。
//
// 半开单飞：挑中一个冷却刚到期的号时经 tryClaimHalfOpenProbe 认领探测位，
// 认领不到（别的请求在探）就并入局部 skip 重挑，fstate==nil 时 reqID 取 0
// （存量直接调用方不参与探测认领，向后兼容）。
func pickAccountForModel(mc ModelConfig, exclude map[int64]bool, fstate *reqFailoverState) (*CookieAccount, map[int64]bool, error) {
	skip := map[int64]bool{}
	for id := range exclude {
		skip[id] = true
	}
	var modelCooling map[int64]bool
	var modelSoonest, shortSoonest time.Duration
	if rtCfg().StrictModel && mc.Upstream != "" {
		modelCooling, modelSoonest = coolingAccounts(mc)
		for id := range modelCooling {
			skip[id] = true
		}
	}
	shortCooling, shortSoonest := shortCoolingAccounts()
	for id := range shortCooling {
		skip[id] = true
	}
	var reqID uint64
	if fstate != nil {
		reqID = fstate.reqID
	}
	// 逐轮挑号：挑中半开号但探测位被占时并入局部 skip 重挑（不占外层 exclude，
	// 否则下次请求会把刚恢复的号误当「本次已试」）。
	for {
		a, ok := pickCookieAccountExcept(skip)
		if !ok {
			return nil, skip, resolvePoolExhaustedErr(mc, exclude, shortCooling,
				modelCooling, modelSoonest, shortSoonest)
		}
		if reqID == 0 || tryClaimHalfOpenProbe(a.ID, reqID) {
			return a, skip, nil
		}
		// 探测位被占：这个号此刻等同冷却——同步记 shortCooling + 保底 1s，
		// 否则它是唯一剩余号时 resolvePoolExhaustedErr 会误报全池降级。
		skip[a.ID] = true
		shortCooling[a.ID] = true
		if shortSoonest == 0 || shortSoonest > time.Second {
			shortSoonest = time.Second
		}
	}
}

// resolvePoolExhaustedErr 决定「没挑到号」时报什么错（pickAccountForModel 抽出，
// 控制圈复杂度）。hasCookie() 判库内有无 enabled 账号。
func resolvePoolExhaustedErr(mc ModelConfig, exclude map[int64]bool,
	shortCooling, modelCooling map[int64]bool, modelSoonest, shortSoonest time.Duration) error {
	if len(exclude) > 0 {
		// 跳槽选备号无号可挑（含末号在 try=0 刚被 autoDisableIfDead 停用）：
		// 绝不静默退匿名——FallbackAnon=false / 带附件 / 多轮的守卫在外层。
		return &noEligibleAccountError{modelRetryAfter: modelSoonest, shortRetryAfter: shortSoonest}
	}
	if !hasCookie() {
		return nil // 合法空池（首跳 len(exclude)==0）：调用方按匿名处理
	}
	// 首跳入口全池冷却：只要有一个号的拦路原因是 45s 短冷却（而非模型降级
	// 或调用方显式 exclude），报 accounts_cooling_down 更诚实——解冻以秒计，
	// 不是以分钟计。双冷却号（同时被 modelCooling 覆盖）不算：它解冻以分钟计。
	for _, a := range accountList() {
		if a.Status == "enabled" && shortCooling[a.ID] && !modelCooling[a.ID] && !exclude[a.ID] {
			return &AccountCooldownError{RetryAfter: shortSoonest}
		}
	}
	return &ModelDowngradedError{Requested: mc.Upstream, RetryAfter: modelSoonest}
}

// retryOnAnotherAccount 判断一次降级要不要换号重来，要的话先把这个号记进冷却和 exclude。
// 匿名降级没号可换；已经吐过内容的不能重来（客户端会收到两遍）。
func retryOnAnotherAccount(res *StreamResult, err error, mc ModelConfig, exclude map[int64]bool) bool {
	// 会话钉住的号绝不能换：PinAccountID>0 时 Bound 429 不并进 exclude、不打换号日志，
	// 直接返回 false——streamGenerateOnce 内部已经按 pinned 语义拒绝了换号，外层再把它
	// 记进 exclude 换下一个号等于绕过钉号语义（降级冷却同理，先记冷却再 return）。
	if mc.PinAccountID > 0 {
		return false
	}
	// strict_proxy_binding：会话绑的号因绑定出口受限出不去（Bound 429）时，把它并进
	// exclude 再换号——多轮续接的换号循环调本函数。它是「这个号这次用不了」而不是
	// 模型降级，不记 strict_model 冷却；browser_transport 的 stickyResidentAccountFor
	// 读的也是同一个 exclude，不并进的话受阻驻留号会被反复挑回（M1 死循环）。
	var rle *RateLimitError
	if errors.As(err, &rle) && rle.Bound && res != nil && res.AccountID > 0 {
		exclude[res.AccountID] = true
		logf("[strict_proxy_binding] 账号 #%d 绑定出口 #%d 不可用，换下一个号", res.AccountID, rle.ProxyID)
		return res.Emitted == "" && res.EmittedReasoning == ""
	}
	var mde *ModelDowngradedError
	if !errors.As(err, &mde) || mde.AccountID == 0 {
		return false
	}
	markDowngraded(mde.AccountID, mc)
	exclude[mde.AccountID] = true
	logf("[strict_model] 账号 #%d 在 %s 上被降级成 %s", mde.AccountID, mc.Upstream, mde.Reported)
	return res == nil || (res.Emitted == "" && res.EmittedReasoning == "")
}

// writeModelDowngraded 回 429 insufficient_quota：跟 OpenAI 额度用完同一个形状，
// agent 客户端本来就会把它当「换号 / 等一会」处理，而不是当成功。
func writeModelDowngraded(w http.ResponseWriter, e *ModelDowngradedError) {
	if e.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(e.RetryAfter.Seconds()))))
	}
	writeJSON(w, 429, map[string]interface{}{"error": map[string]string{
		"message": e.Error(),
		"type":    "insufficient_quota",
		"code":    "model_downgraded",
	}})
}

// strictLineCheck 返回逐行检查降级的函数；开关关着或模型没登记 Upstream 时返回 nil。
// 每行先过它再转发正文：显示名跟第一段正文在同一行或更早出现（2026-10-24 实测
// 逐行看过），所以先查再发就能在第一个字出去之前掐断。
func strictLineCheck(mc ModelConfig, accountID func() int64) func(line string) error {
	if !rtCfg().StrictModel || mc.Upstream == "" {
		return nil
	}
	return func(line string) error {
		if name := extractUpstreamModel(line); name != "" && modelDowngraded(mc, name) {
			return &ModelDowngradedError{Requested: mc.Upstream, Reported: name, AccountID: accountID()}
		}
		return nil
	}
}

// modelNameFoldRe 把字母数字和点以外的东西都折成空格：连字符、全角后缀（"扩展"）
// 一并抹掉，"3.5 Flash-Lite" 和 "3.5 flash lite" 才比得上。
var modelNameFoldRe = regexp.MustCompile(`[^a-z0-9.]+`)

func foldModelName(s string) string {
	folded := strings.TrimSpace(modelNameFoldRe.ReplaceAllString(strings.ToLower(s), " "))
	return strings.TrimPrefix(folded, "gemini ")
}

// modelDowngraded 判断服务端自报的显示名是不是请求的那个模型。
//
// 按「整词前缀」比：思考档会在名字后面挂后缀（"3.1 Pro High"、"3.6 Flash 扩展"），
// 后缀改过名（扩展 → High），所以只认前缀；但必须断在词边界，否则 "3.5 Flash" 会
// 把 "3.5 Flash-Lite" 当成自己。
//
// 看不见就不判：没自报（reported 为空）或模型没登记 Upstream 一律当没降级——
// 误报会让开了开关的人白白丢请求，比漏判更伤。
// 已确认的原位改名/升级可由 UpstreamAliases 放行（3.8 Flash→4 Hydrogen、
// 3.1 Pro→4 Neon）；显式新模型条目不登记旧名，返回旧名时仍判降级。
func modelDowngraded(mc ModelConfig, reported string) bool {
	want := foldModelName(mc.Upstream)
	got := foldModelName(reported)
	// Google 原位改名：hex 95a4c0abcba24364 / Mode=8 那个槽位 2026-11 起自报从
	// "4 Argon" 换成 "4 Carbon"（同 hex、同档、只是名字变了）。两个名字是同一个
	// 模型，互相不算降级；折成一族后前缀比较照旧，跨档后缀与降级到 Flash 仍判。
	want = strings.ReplaceAll(want, "carbon", "argon")
	got = strings.ReplaceAll(got, "carbon", "argon")
	if want == "" || got == "" {
		return false
	}
	// Neon 中档→高档是同槽升级：中档自报 "4 Neon Medium"，被服务端升成高档时报
	// "4 Neon High"。前缀规则断在词边界会把它当成降级，但升级错档不该触发
	// strict_model 的账号 300 分钟冷却——档位不符仍由 argon.go 的强身份核验拒绝
	// （Neon 三档名字不互通），这里只豁免降级判定。
	if want == "4 neon medium" && (got == "4 neon high" || strings.HasPrefix(got, "4 neon high ")) {
		return false
	}
	if got == want || strings.HasPrefix(got, want+" ") {
		return false
	}
	// 登记的别名（原位升级/改名后的新显示名）同样放行：精确匹配或词边界前缀匹配。
	for _, alias := range mc.UpstreamAliases {
		name := strings.ReplaceAll(foldModelName(alias), "carbon", "argon")
		if name != "" && (got == name || strings.HasPrefix(got, name+" ")) {
			return false
		}
	}
	return true
}
