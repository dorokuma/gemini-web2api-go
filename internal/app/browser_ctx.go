package app

// browser_ctx.go —— 单 Chromium 多 BrowserContext 物理隔离（v6.6.0 模块一）。
//
// 架构：browserManager 全局单例最多 1 个 Chromium 进程 + 1 条根 CDP
// WebSocket；每个登录账号（accountID>0, exitKey=""）与每个匿名出口
// （accountID=0, exitKey="allowDirect|proxyURL"）各占一个独立
// BrowserContext + 独立 browserProxyBridge + 独立 page targetID/sessionID。
// 根 Profile 经 rootBridge（SetUpstream("",false) 恒拒 502）物理封死后台
// 外联；业务流量 100% 走各自 Context 的 proxyServer 桥。
//
// 锁纪律（v2.2 §2.9）：m.mu 出度为 0——持 m.mu 期间绝不调
// acquireAcctCookieLock（L1）、cdpClient.Call、bridge.*、DB、proxyMu 或
// 阻塞等 channel；一切网络/DB/锁外操作都在解锁后执行。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// maxBrowserContexts 是最大并存 BrowserContext 数；新建槽位前按
	// 「匿名槽优先、登录槽最久未用其次」淘汰。
	maxBrowserContexts = 8
	// browserCtxIdleTTL：空闲超过它被淘汰回收（交回 Go rotateAccount 保活）。
	browserCtxIdleTTL = 30 * time.Minute
	// cookieSyncThrottle：单槽常规流结束异步写回最小节流间隔。
	cookieSyncThrottle = 30 * time.Second
)

// errExitChanged 是同登录槽非严格模式合法换出口的摘除终态原因：等待者
// 据此知道「旧出口槽被替换」而非「账号失效」。刻意用 *browserLocalError：
// 它不是上游/出口故障，绝不能误扣健康代理分（Oracle MF-8）。
var errExitChanged = browserErr("account exit changed")

// errAuthUserChanged 是同登录槽序号代际更替的摘除终态原因（R12-4，
// Oracle M6/Reviewer #5）：行序号已改（/u/0 -> /u/1）后，旧序号槽上的
// 既有在途流自然排空（绝不强杀），新序号请求按新 authuser 独立建槽——
// 跨序号等待者据此知道「旧序号槽被替换」而非「账号失效」，与
// errExitChanged 同语义汇入新槽重试。刻意用 *browserLocalError：纯本地
// 代际更替，不扣健康代理分。
var errAuthUserChanged = browserErr("account authuser generation changed")

// errExitViolationSuspect 是 acquirePlanLocked 对既有就绪槽「疑似出口漂移」
// 的内部标记（R9-3，Reviewer MF-3）：R3-M1 复核用进 m.mu 前的账号快照，
// 快照读取与进锁之间并发自动绑定可能已同步槽 orig——绝不依据这份可能
// 陈旧的快照直接破坏性摘除，由调用方锁外以一致快照复核后再定终态。
// 本哨兵绝不作为槽位终态错误外发，仅驱动复核分支。
var errExitViolationSuspect = browserErr("exit binding violation suspect (pre-detach recheck required)")

// errIdentityViolationSuspect 是 acquirePlanLocked 对既有就绪槽「疑似旧身份」
// 的标记（R11-5，Oracle A6）：调用方必须在锁外重读最新行复核后再决定是否
// 摘除——freshAcct 是进 m.mu 前读的，读取与进锁之间 accountSync 可能已完成
// 换绑并建好健康新槽，拿陈旧身份比对会把合法新槽误杀。复核确认行身份与槽
// 身份仍不一致时才摘除（且摘除前在 m.mu 内再次以最新行核验）。
var errIdentityViolationSuspect = browserErr("identity violation suspect (pre-detach recheck required)")

// slotExitFacts 是槽位出口事实的纯值快照（R10-1）：全部字段在 m.mu
// 内一次性拷出，锁外复核只用这份纯值——绝不持锁外裸读 ac.*（与
// SyncAutoBoundProxy 的 m.mu 写构成 Go 数据竞争）。
type slotExitFacts struct {
	origAccountProxyID int64
	boundProxyID       int64
	proxyURL           string
	allowDirect        bool
	exitSyncVer        uint64
	disposed           bool
}

// snapshotSlotExitFactsLocked 在持 m.mu 的临界区内快照槽位出口事实。
func (ac *accountBrowserCtx) snapshotSlotExitFactsLocked() slotExitFacts {
	return slotExitFacts{
		origAccountProxyID: ac.origAccountProxyID,
		boundProxyID:       ac.boundProxyID,
		proxyURL:           ac.proxyURL,
		allowDirect:        ac.allowDirect,
		exitSyncVer:        ac.exitSyncVer,
		disposed:           ac.disposed,
	}
}

// exitViolationForFacts 是 readySlotExitViolation 的纯快照判定版
// （R10-1）：与 readySlotExitViolation 同口径，全部槽位事实取自
// slotExitFacts 纯值，零读取 ac.*——锁外复核无数据竞争。
func exitViolationForFacts(f slotExitFacts, account *CookieAccount,
	poolBlocksDirect, strictBinding bool, poolSnap map[int64]proxySnapEntry) bool {
	// (a) 账号锁直连：槽位必须是显式直连。
	if account.ProxyID == proxyIDDirect && (f.proxyURL != "" || !f.allowDirect) {
		return true
	}
	// (b) 池禁直连：槽位直连但账号不是锁直连 → -1->0 反向泄漏。
	if f.proxyURL == "" && poolBlocksDirect && account.ProxyID != proxyIDDirect {
		return true
	}
	// (c) 严格模式或账号绑定已变（orig 漂移）时，当前绑定代理启用且
	// URL 与槽位出口漂移。
	if account.ProxyID > 0 &&
		(strictBinding || account.ProxyID != f.origAccountProxyID) {
		if ent, ok := poolSnap[account.ProxyID]; ok && ent.enabled &&
			ent.url != f.proxyURL {
			return true
		}
	}
	// (d) 槽位 boundProxyID 已删除/禁用或 URL 漂移。
	if f.boundProxyID > 0 {
		ent, ok := poolSnap[f.boundProxyID]
		if !ok || !ent.enabled || ent.url != f.proxyURL {
			return true
		}
	}
	return false
}

// recheckSlotExitViolation 是 R9-3 的锁外一致快照复核（绝不持 m.mu）：
// 先在 m.mu 内一次性快照槽位出口事实与 exitSyncVer（绝不锁外裸读——
// R10-1 消除与 SyncAutoBoundProxy 写侧的 Go 数据竞争），再重读账号最
// 新行与代理池快照，用新鲜事实重新执行同口径判定。返回
// (violation, snapVer)：violation=true 确认漂移；snapVer 是快照版本号，
// 供调用方在重新加锁执行摘除前做版本比对。并发自动绑定同步场景：新
// 行 ProxyID 与槽 orig 一致时复核不再漂移；新行与 orig 仍不一致但 orig
// 是可核的自动绑定来源（autoBindAccountProxy/claimAccountProxy 真实落库
// 且出口一致）时同样放弃摘除——下一轮 freshAcct 重读必然与 orig 收敛。
// 手工 PATCH 改绑清除自动来源记录，复核绝不放行（fail-closed 语义不动）。
func recheckSlotExitViolation(account *CookieAccount, ac *accountBrowserCtx) (bool, uint64) {
	m := browserMgr
	m.mu.Lock()
	facts := ac.snapshotSlotExitFactsLocked()
	m.mu.Unlock()
	if account == nil || account.ID <= 0 || facts.disposed {
		return false, facts.exitSyncVer
	}
	re := accountByID(account.ID)
	if re == nil || re.Status != "enabled" {
		return false, facts.exitSyncVer // 账号本身已不可用——走既有的账号失效路径，不按漂移摘除
	}
	poolSnap := snapshotProxyPool()
	strictBinding := rtCfg().StrictProxyBinding
	poolBlocksDirect := len(poolSnap) > 0 && !rtCfg().FallbackDirect
	if !exitViolationForFacts(facts, re, poolBlocksDirect, strictBinding, poolSnap) {
		return false, facts.exitSyncVer // 新鲜快照下不再漂移：确属并发自动绑定的快照错位
	}
	// 仍漂移：若漂移成因是「新行又推进了一轮可核自动绑定」而非手工改绑，
	// 下一轮 freshAcct 会与 orig 收敛——放弃本次摘除（属并发自动绑定
	// 同步的延伸）。手工 PATCH 清除来源记录，本分支绝不命中。
	if facts.origAccountProxyID > 0 && re.ProxyID != facts.origAccountProxyID &&
		isAutoBoundProxyForURL(re.ID, facts.origAccountProxyID, facts.proxyURL) {
		return false, facts.exitSyncVer
	}
	return true, facts.exitSyncVer
}

// detachIfExitVersionUnchanged 是 R10-1 的版本守卫条件摘除（调用方不持
// m.mu，返回的 snap 按常规锁外清理）：仅当该槽仍是 map 中的同一实例且
// exitSyncVer 未变化时才执行摘除——复核确认与提交摘除之间的合法自动
// 绑定同步（SyncAutoBoundProxy 写 orig/bound 递增 ver）使版本失配，说明
// 旧决定基于已被推翻的事实，放弃摘除交下一轮 freshAcct 重新规划，绝不
// 误摘刚对齐恢复的合法槽、终止其在途流。
func (m *browserManager) detachIfExitVersionUnchanged(ac *accountBrowserCtx, snapVer uint64, cause error) (detachedCtxSnap, *cdpClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ac == nil || ac.disposed || m.ctxs[ac.key] != ac || ac.exitSyncVer != snapVer {
		return detachedCtxSnap{}, nil
	}
	return m.detachSnapLocked(ac, cause), m.client
}

// seedSupersededErr 构造播种代际被取代（Cookie 已轮转）的本地代际终止错误：
// 刻意用 *browserLocalError——这是纯本地的代际竞态（槽位即将被重建并播种
// 新票），出口与上游零责任，classifyFailoverError 归 classLocalAbort（C5b）、
// isBrowserLocalError 判真，绝不误扣无辜代理的健康分（Oracle SF-1）。
func seedSupersededErr(phase string) error {
	return browserErr("seed epoch superseded during %s (cookie rotated): stale jar destroyed", phase)
}

// browserSlotKey 是复合槽位键：登录账号 {accountID, ""}；匿名按出口分槽
// {0, "allowDirect|proxyURL"}——多代理匿名并发零串出口、零排空、零拆建。
type browserSlotKey struct {
	accountID int64
	exitKey   string
}

// makeBrowserSlotKey 计算本次请求的槽位键。account!=nil && ID>0 归登录
// 槽（出口在槽内生命周期内固定；非严格模式合法换出口走 drain 替换）；
// 其余按真实出口分匿名槽。
func makeBrowserSlotKey(account *CookieAccount, proxyURL string, allowDirect bool) browserSlotKey {
	if account != nil && account.ID > 0 {
		return browserSlotKey{accountID: account.ID}
	}
	return browserSlotKey{accountID: 0, exitKey: fmt.Sprintf("%v|%s", allowDirect, proxyURL)}
}

// slotKeyForAccount 是 makeBrowserSlotKey 的语义化别名（测试与调用点用）：
// 登录号 {id, ""}——同 cookieIdentity 不同 authuser 复用同一 BrowserContext
// （同 Google 会话身份），authuser 差异只影响导航路径；匿名 {0, exitKey}。
func slotKeyForAccount(account *CookieAccount, proxyURL string, allowDirect bool) browserSlotKey {
	return makeBrowserSlotKey(account, proxyURL, allowDirect)
}

// accountBrowserCtx 是一个槽位（BrowserContext + 专属桥 + 专属 page
// session）的全部状态。所有字段受 m.mu 保护；epoch 单调递增代际让迟到
// 的构建/回写认出自己已过时。
type accountBrowserCtx struct {
	key            browserSlotKey
	epoch          uint64 // 创建及每次 recreateAccountTab 在 m.mu 下自增赋新值
	cookieIdentity string // 登录槽为 cookieIdentity(fresh.Cookie)，匿名槽 ""

	// 进 m.mu 前预计算的出口快照（保持 m.mu 出度为 0，绝不在持 m.mu
	// 期间访问 proxyMu）。
	origAccountProxyID int64  // 创建时快照 account.ProxyID
	boundProxyID       int64  // matchProxyIDByURL(account.ProxyID, proxyURL)
	proxyURL           string // 本 Context 专属 bridge 锁定的上游代理 URL
	allowDirect        bool   // 本 Context 专属 bridge 的直连许可

	// authuser 是建槽时该登录槽所属账号行的账号序号（/u/N）快照（R11-5，
	// Oracle A6）：异步 Jar 回写的 SQL CAS 用它做谓词——长流在飞期间管理
	// 员只改序号（不拆槽）时，/u/0 槽派生的票据绝不写进已变为 /u/1 的行。
	authuser int

	// exitSyncVer 是出口同步事实的递增版本号（R10-1，Reviewer M1/Oracle M1）：
	// SyncAutoBoundProxy 与 buildAccountCtx 步骤 6 改写 orig/bound 时在 m.mu
	// 内同步递增。readySlotExitViolation 的锁外复核先快照（字段, ver），确认
	// 漂移后重新加锁执行摘除前必须比对 ver——期间已发生合法同步说明摘
	// 除决定基于旧指针，放弃摘除重新规划，绝不误摘刚恢复的合法槽。
	exitSyncVer uint64

	// rebindGen 是该登录槽所属账号的改绑代际（R12-2-M1）：任何成功的绑定
	// 写提交都经 NoteAccountRebindCommitted 在 m.mu 内递增本字段。热注入
	// 授权复核绑定 (bcID, exitSyncVer, rebindGen) 三元组——改绑不必然同步
	// 槽事实（驻留槽出口与新绑定不符时 SyncAutoBoundProxy 不动槽），仅靠
	// 前两元组对该交错不可见。
	rebindGen uint64

	browserContextID string
	targetID         string
	sessionID        string
	bridge           *browserProxyBridge

	creating    bool
	recreating  bool
	closing     bool
	disposed    bool
	buildCancel context.CancelFunc // 打断当前 creating/recreating
	inFlight    int

	readyCh       chan struct{}
	readyClosed   bool
	lastBuildErr  error // 构建/摘除终态错误，唤醒的等待者读取它退出
	syncOnDispose bool  // 已发布槽摘除时是否需要先写回 Cookie

	// 同登录槽非严格模式换出口排空计数器（彻底废弃布尔 draining）。
	drainWaiters int
	drainWait    chan struct{}
	drainClosed  bool

	lastUsedAt     time.Time
	lastCookieSync time.Time
	syncingCookie  bool
}

// resetReadyLocked 重建就绪通道（创建/重建入口）。调用方持 m.mu。
func (ac *accountBrowserCtx) resetReadyLocked() {
	ac.readyCh = make(chan struct{})
	ac.readyClosed = false
}

// closeReadyLocked 幂等关闭就绪通道（唤醒全部构建等待者）。持 m.mu。
func (ac *accountBrowserCtx) closeReadyLocked() {
	if ac.readyCh != nil && !ac.readyClosed {
		close(ac.readyCh)
		ac.readyClosed = true
	}
}

// ensureDrainLocked 取（或重建）排空等待通道。持 m.mu。
func (ac *accountBrowserCtx) ensureDrainLocked() chan struct{} {
	if ac.drainWait == nil || ac.drainClosed {
		ac.drainWait = make(chan struct{})
		ac.drainClosed = false
	}
	return ac.drainWait
}

// closeDrainLocked 幂等关闭排空通道并置空（唤醒被暂缓的同出口请求）。
// 持 m.mu。
func (ac *accountBrowserCtx) closeDrainLocked() {
	if ac.drainWait != nil && !ac.drainClosed {
		close(ac.drainWait)
		ac.drainClosed = true
		ac.drainWait = nil
	}
}

// drainWaiterLeaveLocked 撤销一份跨出口等待登记；归零时关闭排空通道唤醒
// 被暂缓的同出口请求。持 m.mu。
func (ac *accountBrowserCtx) drainWaiterLeaveLocked() {
	if ac.drainWaiters > 0 {
		ac.drainWaiters--
	}
	if ac.drainWaiters == 0 {
		ac.closeDrainLocked()
	}
}

// detachedCtxSnap 是 detachCtxLocked 的锁内摘除快照：全部字段在持
// m.mu 期间从 ac 拷出（Oracle SF-5 / NH-2），解锁后调用方零读取
// ac.browserContextID / ac.bridge / ac.syncOnDispose / ac.lastBuildErr。
// ac==nil 表示本次摘除竞争落败（槽已被其它摘除方摘走），调用方不得
// 再做任何清理——否则会对同一已摘除槽二次 dispose（MF-8.4）。
// wasCreating=true 表示摘除时槽位仍在构建/重建中：(bcID, bridge) 等
// 局部资源归 buildAccountCtx/recreateAccountTab owner 唯一所有，外部
// 摘除方只调 failStreamsForCtx，**绝不** disposeDetachedCtx（防双重清理）。
type detachedCtxSnap struct {
	ac          *accountBrowserCtx
	bcID        string
	bridge      *browserProxyBridge
	syncCookie  bool
	err         error
	wasCreating bool
}

// detachSnapLocked 是 detachCtxLocked 的锁内快照包装：胜出时把清理所需
// 字段全部在 m.mu 内快照后返回；落败（ac==nil / 已 disposed / 未注册）
// 返回零值 snap。调用方持 m.mu。
func (m *browserManager) detachSnapLocked(ac *accountBrowserCtx, cause error) detachedCtxSnap {
	if ac == nil {
		return detachedCtxSnap{}
	}
	ok, wasCreating := m.detachCtxLocked(ac, cause)
	if !ok {
		return detachedCtxSnap{}
	}
	return detachedCtxSnap{
		ac:          ac,
		bcID:        ac.browserContextID,
		bridge:      ac.bridge,
		syncCookie:  ac.syncOnDispose,
		err:         ac.lastBuildErr,
		wasCreating: wasCreating,
	}
}

// detachCtxLocked 是全部回收路径的唯一锁内摘除原语。返回
// (detached, wasCreating)：detached=true 表示本次调用是「唯一胜出摘除者」
// （ac 从存活变 disposed），调用方负责锁外 disposeDetachedCtx；
// wasCreating 让外部摘除方知道创建中槽位的 (browserContextID, bridge)
// 尚未发布、局部资源由 buildAccountCtx owner 统一清理——绝不按已发布槽
// 重复清理（v2.2 §10(3) / NH-2）。
//
// R3-M1/防呆：必须在清零 creating 之前捕获 wasCreating。
func (m *browserManager) detachCtxLocked(ac *accountBrowserCtx, cause error) (bool, bool) {
	if ac == nil || ac.disposed {
		return false, false
	}
	wasCreating := ac.creating // 先捕获再清：外部摘除方据此知道别碰局部资源
	ac.disposed = true
	ac.closing = true
	ac.creating = false
	ac.recreating = false
	ac.syncOnDispose = !wasCreating // 已发布槽才需要回写 Cookie
	if ac.lastBuildErr == nil && cause != nil {
		ac.lastBuildErr = cause
	}
	if m.ctxs[ac.key] == ac {
		delete(m.ctxs, ac.key)
	}
	if ac.sessionID != "" && m.sessionIndex[ac.sessionID] == ac {
		delete(m.sessionIndex, ac.sessionID)
	}
	if ac.targetID != "" && m.targetIndex[ac.targetID] == ac {
		delete(m.targetIndex, ac.targetID)
	}
	if ac.buildCancel != nil {
		ac.buildCancel()
		ac.buildCancel = nil
	}
	ac.closeReadyLocked()
	ac.closeDrainLocked()
	return true, wasCreating
}

// disposeDetachedCtx 统一清理一个已摘除槽位的资源（锁外执行，绝不持 m.mu）。
// 顺序：先断桥（物理切断旧出口外发）→ 条件 CAS 写回 Cookie → 销毁
// BrowserContext。bcID/bridge 显式判空；client 已断连时跳过销毁调用。
func (m *browserManager) disposeDetachedCtx(ac *accountBrowserCtx,
	bcID string, bridge *browserProxyBridge, syncCookie bool, client *cdpClient) {
	if bridge != nil {
		shCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		bridge.Shutdown(shCtx)
		cancel()
	}
	if syncCookie && bcID != "" && ac.key.accountID > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		m.syncAccountCookiesBackCtx(ctx, client, bcID, ac.key.accountID, ac.epoch, ac.cookieIdentity, ac.authuser)
		cancel()
	}
	if bcID != "" && client != nil && !client.Closed() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = client.Call(ctx, "", "Target.disposeBrowserContext",
			map[string]interface{}{"browserContextId": bcID}, nil)
		cancel()
	}
}

// evictIdleOrOverflowLocked 淘汰候选：空闲超 browserCtxIdleTTL 的槽位；
// 需要腾位时（needSlot=true 且满 maxBrowserContexts）按「匿名优先、登录
// 最久未用其次」追加淘汰一个。返回已摘除槽位的锁内快照（锁外清理）。
// 调用方持 m.mu。
func (m *browserManager) evictIdleOrOverflowLocked(now time.Time, needSlot bool) []detachedCtxSnap {
	var victims []detachedCtxSnap
	for _, ac := range m.ctxs {
		if ac.creating || ac.recreating || ac.inFlight > 0 || ac.drainWaiters > 0 {
			continue
		}
		if now.Sub(ac.lastUsedAt) > browserCtxIdleTTL {
			if snap := m.detachSnapLocked(ac, browserErr("idle evicted")); snap.ac != nil {
				victims = append(victims, snap)
			}
		}
	}
	if !needSlot || len(m.ctxs) < maxBrowserContexts {
		return victims
	}
	var anonVictim, loginVictim *accountBrowserCtx
	for _, ac := range m.ctxs {
		if ac.creating || ac.recreating || ac.inFlight > 0 || ac.drainWaiters > 0 {
			continue
		}
		if ac.key.accountID == 0 {
			if anonVictim == nil || ac.lastUsedAt.Before(anonVictim.lastUsedAt) {
				anonVictim = ac
			}
		} else if loginVictim == nil || ac.lastUsedAt.Before(loginVictim.lastUsedAt) {
			loginVictim = ac
		}
	}
	pick := anonVictim
	if pick == nil {
		pick = loginVictim
	}
	if pick != nil {
		if snap := m.detachSnapLocked(pick, browserErr("overflow evicted")); snap.ac != nil {
			victims = append(victims, snap)
		}
	}
	return victims
}

// EvictIdleContexts 是后台驱动入口（rotateAllAccounts 开头调用）：淘汰
// 全部空闲超 TTL 的槽位，登录槽先写回 Cookie 再销毁。调用方不持锁。
func (m *browserManager) EvictIdleContexts() {
	m.mu.Lock()
	if !m.started || m.stopping || m.shuttingDown || m.cleaning || m.client == nil {
		m.mu.Unlock()
		return
	}
	client := m.client
	var victims []detachedCtxSnap
	for _, ac := range m.ctxs {
		if ac.creating || ac.recreating || ac.inFlight > 0 || ac.drainWaiters > 0 {
			continue
		}
		if time.Since(ac.lastUsedAt) > browserCtxIdleTTL {
			if snap := m.detachSnapLocked(ac, browserErr("idle evicted")); snap.ac != nil {
				victims = append(victims, snap)
			}
		}
	}
	m.mu.Unlock()
	for _, snap := range victims {
		m.failStreamsForCtx(snap.ac, browserErr("idle context evicted"))
		m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, snap.syncCookie, client)
	}
}

// activeSlotSnapshot 是一个已发布存活登录槽的内存快照（全部字段在 m.mu
// 内拷出，锁外零读取 ac.*）。origAccountProxyID 是建槽时快照的账号绑定
// （R4-1）：保活侧合法溢出判定必须对齐前台 readySlotExitViolation 语义
// ——管理员改绑（cur.ProxyID != origAccountProxyID）后旧出口不再是当前
// 账号的合法溢出。
type activeSlotSnapshot struct {
	accountID          int64
	bcID               string
	proxyURL           string
	allowDirect        bool
	boundProxyID       int64
	origAccountProxyID int64
	ident              string
	// exitSyncVer 是快照时刻该槽的出口同步事实版本（R12-2）：热注入
	// 提交绑定的槽实例身份之一——prepareHotInject 在 m.mu 内核验驻留槽
	// 的 (bcID, exitSyncVer) 与授权时刻完全一致，任一变化即放弃注入。
	exitSyncVer uint64
	// rebindGen 是快照时刻该槽的账号改绑代际（R12-2-M1）：任何成功的
	// 绑定写提交（自动/手动首绑、改绑、仲裁）都经 NoteAccountRebindCommitted
	// 在 m.mu 内递增——改绑不必然同步槽事实（驻留槽实际出口与新绑定
	// 不符时 SyncAutoBoundProxy 不动槽），仅靠 (bcID, exitSyncVer) 对
	// 该交错不可见；rebindGen 使「复核通过后、提交注入前完成的改绑」
	// 必然被提交核验检出。
	rebindGen uint64
}

// ActiveAccountSlotSnapshots 单次 m.mu 快照全部已发布存活登录槽：
// activeByID 按 accountID 索引，activeByIdent 按 cookieIdentity 索引
// （同身份兄弟号共享同一 Google 会话）的全部候选快照切片——R9-8
// （Reviewer SF-x）：同身份多个活跃槽必须全部保留，绝不按 Go map
// 遍历顺序互相覆盖只留一个：A、B 同身份分别走 P1/P2 时，非驻留兄弟
// C 绑 P1 需要借用合法驻留候选 A；只剩 B 会把 C 整轮静默跳过。按
// accountID 升序排序让挑选确定性可测。recreating 槽的 BrowserContext
// 仍然存活（重建只换 tab），仍计入快照。m.mu 出度严格为 0。
func (m *browserManager) ActiveAccountSlotSnapshots() (map[int64]activeSlotSnapshot, map[string][]activeSlotSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byID := map[int64]activeSlotSnapshot{}
	byIdent := map[string][]activeSlotSnapshot{}
	for _, ac := range m.ctxs {
		if ac.disposed || ac.creating || ac.key.accountID <= 0 || ac.browserContextID == "" {
			continue
		}
		snap := activeSlotSnapshot{
			accountID:          ac.key.accountID,
			bcID:               ac.browserContextID,
			proxyURL:           ac.proxyURL,
			allowDirect:        ac.allowDirect,
			boundProxyID:       ac.boundProxyID,
			origAccountProxyID: ac.origAccountProxyID,
			ident:              ac.cookieIdentity,
			exitSyncVer:        ac.exitSyncVer,
			rebindGen:          ac.rebindGen,
		}
		byID[ac.key.accountID] = snap
		if ac.cookieIdentity != "" {
			byIdent[ac.cookieIdentity] = append(byIdent[ac.cookieIdentity], snap)
		}
	}
	for ident := range byIdent {
		list := byIdent[ident]
		for i := 1; i < len(list); i++ {
			for j := i; j > 0 && list[j-1].accountID > list[j].accountID; j-- {
				list[j-1], list[j] = list[j], list[j-1]
			}
		}
		byIdent[ident] = list
	}
	return byID, byIdent
}

// slotResidentForSnapshot 在持 m.mu 的短临界区内复核快照对应的槽实例
// 是否仍驻留（R4-1）：snap.accountID 的槽存在、未摘除且 browserContextID
// 与快照一致才为真。取快照后管理员改绑并摘槽/槽位被替换时返回 false——
// 过期快照绝不能再当作当前账号的合法事实外发。锁序：调用方须已持该
// accountID 的 L1 acctCookieLock（L1→L2），m.mu 出度为 0。
func (m *browserManager) slotResidentForSnapshot(snap activeSlotSnapshot) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ac := m.ctxs[browserSlotKey{accountID: snap.accountID}]
	return ac != nil && !ac.disposed && ac.browserContextID == snap.bcID
}

// ownSlotSnapshotForInject 在单个 m.mu 临界区内快照本账号自有驻留槽的
// 实时事实（R11-2，Oracle A2/A3）：供 rotateActiveOrSibling1PSIDTS 在
// 换发返回、提交注入前做**实时二次授权复核**——快照全部字段拷自当前
// 驻留实例，不存在/未发布/creating 中返回 ok=false。与
// ActiveAccountSlotSnapshots 同口径（recreating 槽 BrowserContext 仍存
// 活，计入快照）。
func (m *browserManager) ownSlotSnapshotForInject(accountID int64) (activeSlotSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ac := m.ctxs[browserSlotKey{accountID: accountID}]
	if ac == nil || ac.disposed || ac.creating || ac.browserContextID == "" {
		return activeSlotSnapshot{}, false
	}
	return activeSlotSnapshot{
		accountID:          ac.key.accountID,
		bcID:               ac.browserContextID,
		proxyURL:           ac.proxyURL,
		allowDirect:        ac.allowDirect,
		boundProxyID:       ac.boundProxyID,
		origAccountProxyID: ac.origAccountProxyID,
		ident:              ac.cookieIdentity,
		exitSyncVer:        ac.exitSyncVer,
		rebindGen:          ac.rebindGen,
	}, true
}

// InjectCookiesToActiveSlot 将增量 Set-Cookie 热注入当前存活的 BrowserContext
// Jar（不拆槽、不断流）。锁序红线：调用方必须已持该 accountID 的 L1
// acctCookieLock；本函数内部持 m.mu 仅拷贝 (client, bcID) 标量即刻解锁
// （m.mu 出度为 0），CDP Storage.setCookies 在 m.mu 之外执行。
//
// 返回语义：
//   - injected=true：热注入成功（新 Cookie 已落 Jar，严禁再走 Bump/Invalidate）；
//   - slotGone=true：当前无该账号已发布存活槽（调用方按陈旧路径处理）；
//   - err!=nil：槽存在但 CDP 写入失败。
func (m *browserManager) InjectCookiesToActiveSlot(ctx context.Context, accountID int64, setCookie []string) (injected bool, slotGone bool, err error) {
	m.mu.Lock()
	ac := m.ctxs[browserSlotKey{accountID: accountID}]
	if ac == nil || ac.disposed || ac.creating || ac.browserContextID == "" ||
		m.client == nil || m.client.Closed() {
		m.mu.Unlock()
		return false, true, nil
	}
	client, bcID := m.client, ac.browserContextID
	m.mu.Unlock()

	if err := injectCookiesToBC(ctx, client, bcID, setCookie); err != nil {
		return false, false, err
	}
	return true, false, nil
}

// injectCookiesToBC 把增量 Set-Cookie 经 CDP Storage.setCookies 写进指定
// BrowserContext（锁外网络 I/O，绝不持 m.mu）。
func injectCookiesToBC(ctx context.Context, client *cdpClient, bcID string, setCookie []string) error {
	// 增量 Set-Cookie 先合并出要写入的键值再转 CDP 参数：__Secure-1PSIDTS /
	// __Secure-3PSIDTS 走 domain .google.com（与 buildAccountCtx 播种 100% 同构，
	// 原地覆盖旧值）。
	merged := mergeSetCookie("", setCookie)
	params := cookieToCDP(merged)
	if len(params) == 0 {
		return fmt.Errorf("nothing to inject")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return client.Call(cctx, "", "Storage.setCookies",
		map[string]interface{}{"browserContextId": bcID, "cookies": params}, nil)
}

// hotInjectPlan 是 prepareHotInject 对活跃槽热注入的锁内判定。
type hotInjectPlan int

const (
	// hotInjectGone：完全无该账号槽（或槽已摘除/无 BrowserContext/客户端断连）。
	hotInjectGone hotInjectPlan = iota
	// hotInjectCreating：槽位注册但仍在构建/重建中（jar 未就绪，绝不热注入）。
	hotInjectCreating
	// hotInjectReady：槽位已发布存活且通过独立授权，可在锁外执行
	// Storage.setCookies 热注入。
	hotInjectReady
	// hotInjectUnauthorized：槽位已发布存活但**未通过独立注入授权**
	//（R10-5）——调用方仅落库 DB，对该 BC 零 Storage.setCookies。
	hotInjectUnauthorized
)

// prepareHotInject 在**单个 m.mu 临界区内**原子完成活跃槽分类（完全无槽 /
// creating 中 / 已发布就绪）与——非就绪时——BumpAccountEpoch（fix #3）。
// 原子性是回写黑洞防线的另一半：分类与 Bump 之间绝不可能插入一次槽位发布——
// 发布守卫（buildAccountCtx）与 Bump 同受 m.mu 串行化，Bump 之后的发布必然
// 被 isSupersededEpochLocked 拦截销毁，Bump 之前的发布会让分类看到就绪槽而
// 走热注入路径（不 Bump）。ready 路径返回的 (client, bcID) 供调用方锁外执行
// 注入。锁序：调用方必须已持该 accountID 的 L1 acctCookieLock；m.mu 出度为 0。
//
// R10-5（Reviewer M2/Oracle M2）：「借哪个出口」与「写哪个 BC」是分离的
// 授权对象——出口候选（可能是同身份兄弟槽）经 pickRotateCandidate 独立
// 授权，但待写入的**自有槽**必须在本函数内独立通过注入授权，不得凭兄弟
// 候选的授权借道。
//
// hotInjectBinding 是热注入提交前最后一次授权复核绑定的全部事实（R12-2，
// Oracle M1/M2）：verified=true 表示调用方在换发拿到新票后刚重读最新 DB
// 行（未停用）并验证了快照为 (bcID, exitSyncVer, rebindGen) 的具体槽实例
// 的出口/来源授权。三元组完整覆盖复核与提交之间的一切失格交错：槽位替换
// （bcID 变）、出口事实同步（exitSyncVer 增）、任何改绑提交（rebindGen 增）。
type hotInjectBinding struct {
	verified    bool
	bcID        string
	exitSyncVer uint64
	rebindGen   uint64
}

// prepareHotInject 在**单个 m.mu 临界区内**原子完成活跃槽分类与授权核验。
// R12-2（Oracle M1/M2/Reviewer #3）授权与提交原子绑定：驻留槽的
// (bcID, exitSyncVer, rebindGen) 必须与授权时刻完全一致——复核与提交之间
// 配额改绑、停用摘除或槽位替换任一发生，绑定事实即过期，绝不凭过期判定
// 注入。任一不过返回 hotInjectUnauthorized：与分类原子完成 Bump（旧槽滞后
// 回写被代际拦截丢弃），并返回该驻留槽实例的 bcID（detachBCID）供调用方
// 在 DB 更新后条件摘除——可授租却永久无法回写的僵尸槽绝不留存。
// verified=false（调用方授权未通过）时同样拒绝注入并返回驻留槽 bcID。
func (m *browserManager) prepareHotInject(accountID int64, expectedIdent string,
	bind hotInjectBinding) (
	plan hotInjectPlan, client *cdpClient, bcID string, detachBCID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bump := func() {
		m.nextCtxEpoch++
		m.maxSeededEpoch[accountID] = m.nextCtxEpoch
	}
	ac := m.ctxs[browserSlotKey{accountID: accountID}]
	if ac == nil || ac.disposed || m.client == nil || m.client.Closed() {
		bump()
		return hotInjectGone, nil, "", ""
	}
	if ac.creating || ac.recreating {
		// creating 中槽的 browserContextID 尚未发布（必然为空）——必须先判
		// creating 再判 bcID 空，否则构建中槽被错分类成「完全无槽」。
		bump()
		return hotInjectCreating, nil, "", ""
	}
	if ac.browserContextID == "" {
		bump()
		return hotInjectGone, nil, "", ""
	}
	// R12-2 注入授权核验（m.mu 内，epoch/身份/绑定事实核验与授权原子化）：
	//   - 授权未通过（verified=false：出口改绑/身份不符/账号停用/无来源）；
	//   - 绑定事实过期（驻留槽不是授权时刻核验过的那个实例/版本——改绑
	//     同步、摘除重建任一发生）；
	//   - 身份不符（accountSync 换会话 X→Y 后尚未摘除的旧 X 槽）；
	//   - 代际已被取代（epoch 过期）。
	// 任一命中即拒绝注入：Bump 后交调用方仅落库，对该 BC 零写入，并把
	// 驻留槽 bcID 交给调用方做 DB 更新后的条件摘除（绝不留僵尸槽）。
	identOK := expectedIdent == "" || expectedIdent == "|" ||
		ac.cookieIdentity == "" || ac.cookieIdentity == expectedIdent
	boundOK := bind.verified && bind.bcID != "" &&
		ac.browserContextID == bind.bcID && ac.exitSyncVer == bind.exitSyncVer &&
		ac.rebindGen == bind.rebindGen
	if !boundOK || !identOK || m.isSupersededEpochLocked(accountID, ac.epoch) {
		bump()
		return hotInjectUnauthorized, nil, "", ac.browserContextID
	}
	return hotInjectReady, m.client, ac.browserContextID, ""
}

// ActiveAccountsAndIdentities 返回当前全部存活登录槽的 accountID 集合与
// cookieIdentity 集合（rotateAllAccounts 据此跳过活跃槽/同身份兄弟号）。
func (m *browserManager) ActiveAccountsAndIdentities() (map[int64]bool, map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := map[int64]bool{}
	idents := map[string]bool{}
	for _, ac := range m.ctxs {
		if ac.disposed || ac.key.accountID <= 0 {
			continue
		}
		ids[ac.key.accountID] = true
		if ac.cookieIdentity != "" {
			idents[ac.cookieIdentity] = true
		}
	}
	return ids, idents
}

// SyncAutoBoundProxy 把非严格模式下合法自动绑定/重绑提交后的驻留槽
// 绑定事实同步到新 proxyID（R6-4，Reviewer MF-4）：仅当该账号驻留槽
// 未摘除且其**实际出口**（ac.proxyURL）与新绑定代理 URL 精确一致时，
// 才把 ac.origAccountProxyID / ac.boundProxyID 改为 proxyID。同步只发生
// 在「该槽事实上的出口正是新绑定」时——下轮保活的改绑拦截
// （snap.origAccountProxyID != cur.ProxyID）与 exitCompatCheck 看见的都
// 是真实事实，合法自动首绑（0 -> P）与合法自动重绑（P旧 -> P新）均可
// 续期；管理员手工 PATCH 改绑不经本函数，驻留槽 orig 保持旧值，改绑
// 拦截原样生效（负对照完整保留）。短持 m.mu，出度为 0，绝不摘槽/反写。
func (m *browserManager) SyncAutoBoundProxy(accountID, proxyID int64, proxyURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ac := m.ctxs[browserSlotKey{accountID: accountID}]
	if ac == nil || ac.disposed {
		return
	}
	if ac.proxyURL != proxyURL {
		return // 驻留槽实际出口不是新绑定代理：绝不谎改事实
	}
	ac.origAccountProxyID = proxyID
	ac.boundProxyID = proxyID
	// R10-1（Reviewer M1/Oracle M1）：出口同步事实改写必须递增版本——
	// detachIfExitVersionUnchanged 依赖版本失配放弃基于旧事实的摘除，
	// 漏递增会让复核确认后发生的合法同步对版本守卫不可见。
	ac.exitSyncVer++
}

// NoteAccountRebindCommitted 在账号的一次绑定写**真实提交成功**后
// （autoBindAccountProxy / claimAccountProxy 首绑仲裁 / bindAccountProxy
// 手工改绑 / rebindAccountProxyAtomic，调用方持 claimMu 调用——锁序
// claimMu -> m.mu 与 SyncAutoBoundProxy 一致）递增驻留槽的改绑代际
// （R12-2-M1）。热注入授权复核把 rebindGen 绑进提交判定：复核通过后、
// prepareHotInject 的 m.mu 临界区之前完成的任何改绑都必然使代际失配
// ——绝不凭复核时刻的旧绑定事实向已失格 BC 注入新票。驻留槽不存在/
// 已摘除时无操作（之后新建的槽本身就是在新绑定事实下规划构建的）。
func (m *browserManager) NoteAccountRebindCommitted(accountID int64) {
	if accountID <= 0 || !shouldUseBrowserTransport() {
		return
	}
	m.mu.Lock()
	if ac := m.ctxs[browserSlotKey{accountID: accountID}]; ac != nil && !ac.disposed {
		ac.rebindGen++
	}
	m.mu.Unlock()
}

// HasActiveOrCreatingAccount 报告本账号自身（accountID）是否存在活跃或
// creating 中的浏览器槽（Oracle MF-1：辅助 /app 回收守卫粒度修正）。
// 写库 CAS 只针对本账号 id 自身，守卫绝不能因同身份兄弟号有槽而误伤本账号
// 自己的写库路径——兄弟号有无槽与本账号无关。短持 m.mu，L1→L2 锁序，
// m.mu 出度为 0。
func (m *browserManager) HasActiveOrCreatingAccount(accountID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ac := range m.ctxs {
		if ac.disposed {
			continue
		}
		if ac.key.accountID == accountID {
			return true
		}
	}
	return false
}

// HasActiveAccountOrIdentity 报告该 accountID 或 cookieIdentity 是否有活跃
// 登录槽驻留（rotateAccountOpts 持锁二次复核用）。
func (m *browserManager) HasActiveAccountOrIdentity(accountID int64, identity string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ac := range m.ctxs {
		if ac.disposed {
			continue
		}
		if ac.key.accountID == accountID {
			return true
		}
		if identity != "" && ac.cookieIdentity == identity && ac.key.accountID > 0 {
			return true
		}
	}
	return false
}

// BumpAccountEpoch 在 Cookie 真实轮转（merged != expectedOld 且 CAS 写库
// 成功）后联动推进该账号代际：全局 nextCtxEpoch 与 maxSeededEpoch[accountID]
// 取同一个自增值——任何后建槽位 epoch 严格大于此前全部 maxSeededEpoch，
// 杜绝多次轮转后新槽被误判为旧代际永久丢弃回写（Oracle MF-6）。
// 调用方不持 m.mu（L1 acctCookieLock 内可调）。返回推进后的新代际值
// （R12-4-M7：accountSync 据此做条件失效，绝不误杀 Bump 之后由新身份
// 请求建好的新槽）。
func (m *browserManager) BumpAccountEpoch(accountID int64) uint64 {
	m.mu.Lock()
	m.nextCtxEpoch++
	m.maxSeededEpoch[accountID] = m.nextCtxEpoch
	newEpoch := m.nextCtxEpoch
	m.mu.Unlock()
	return newEpoch
}

// InvalidateAccountIfEpochBefore 是 InvalidateAccount 的代际条件版（R12-4，
// Oracle M7/Reviewer #5）：仅当当前驻留槽存在、未摘除且其 epoch 严格小于
// epochBefore（通常是本次换身份/轮转 Bump 产生的新代际值）时才摘除。
// accountSync 换身份 X→Y 提交并 Bump 后释放 L1，锁外窗口内新 Y 请求可能
// 已建好并发布健康 Y 槽（其 epoch 严格大于 Bump 值）——按账号 ID 无条件
// 摘除会把新 Y 槽连同在途流一起误杀；代际条件失效下新 Y 槽存活，仅旧
// 代际（epoch < Bump 值）的 X 槽被清理。驻留槽不存在或已更新代际时本
// 调用是无操作。
func (m *browserManager) InvalidateAccountIfEpochBefore(accountID int64, epochBefore uint64, reason string) {
	m.mu.Lock()
	ac := m.ctxs[browserSlotKey{accountID: accountID}]
	if ac == nil || ac.disposed || ac.epoch >= epochBefore {
		m.mu.Unlock()
		return
	}
	snap := m.detachSnapLocked(ac,
		browserErr("account #%d invalidated: %s", accountID, reason))
	client := m.client
	m.mu.Unlock()
	if snap.ac == nil {
		return
	}
	m.failStreamsForCtx(snap.ac, snap.err)
	if snap.wasCreating {
		return
	}
	m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, snap.syncCookie, client)
}

// isSupersededEpochLocked 报告 acEpoch 是否已被更新代际取代。持 m.mu。
func (m *browserManager) isSupersededEpochLocked(accountID int64, acEpoch uint64) bool {
	return acEpoch < m.maxSeededEpoch[accountID]
}

// matchProxyIDByURL 计算 proxyURL 实际对应的代理池 ID：account.ProxyID>0
// 且 proxyURLByID(account.ProxyID)==proxyURL 时返回 account.ProxyID；否则
// 按 URL 反查池（单测非池 URL / 直连返回 0）。进 m.mu 前调用（出度 0）。
func matchProxyIDByURL(accountProxyID int64, proxyURL string) int64 {
	if accountProxyID > 0 && proxyURL != "" && proxyURLByID(accountProxyID) == proxyURL {
		return accountProxyID
	}
	if proxyURL == "" {
		return 0
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.URL == proxyURL {
			return p.ID
		}
	}
	return 0
}

// acquireAccountCtx 是主入口：为 (account, proxyURL, allowDirect) 取一个
// 就绪的 BrowserContext 租约。返回绑定具体 ac 指针的 (client, ac, sessID,
// release, err)；release 用 sync.Once 幂等归还 inFlight 并触发排空唤醒。
//
// allowCreate=false（辅助入口）在 m.mu 内拦截新建：槽位不存在或未就绪
// 直接返回 errBrowserSkipAux，绝不冷启动 Context。
//
// 单飞构造：第一个请求认领创建（起 buildAccountCtx 协程），并发等待者
// 阻塞在 ac.readyCh；客户端取消只退自己（不 buildCancel 连坐共享等待者）。
// 唤醒后第一时间核对 ac.disposed：已摘除立即返回 ac.lastBuildErr（严禁
// continue 用陈旧形参重建，v2.2 §1）。创建者本人直接等自己认领的
// nac.readyCh（绝不重查 m.ctxs[key] 引发快速失败死循环）。
// acquireDrainReg 跟踪跨出口排空等待名额的跨出口状态：同一调用方最多
// 持有一个名额，registered/target 标记当前登记在哪个 ac。
type acquireDrainReg struct {
	registered bool
	target     *accountBrowserCtx
}

// leave 在持 m.mu 时撤销登记（每条错误/取消/汇入新槽出口调用）。
func (d *acquireDrainReg) leave() {
	if d.registered && d.target != nil {
		d.target.drainWaiterLeaveLocked()
	}
	d.registered = false
	d.target = nil
}

// refreshAcquireSnapshot 是 acquireAccountCtx 每轮进 m.mu 之前的锁外
// 账号/代理池快照刷新（m.mu 出度 0，本函数绝不持 m.mu）：
//   - poolSnap/strictBinding/poolBlocksDirect 取自 proxyMu 与 rtCfg；
//   - account.ID>0 时 freshAcct 从 DB 读最新行（Reviewer #4：管理员可能
//     在等待窗口内改绑代理或停用账号，绝不能用调用方传入的旧指针做
//     R3-M1 复核）；freshAcct 失效时先 InvalidateAccount 摘掉旧槽再清
//     drain 登记返回终态错；
//   - origPID/boundID 按 freshAcct.ProxyID 重算；
//   - 请求级 (proxyURL, allowDirect) 对 freshAcct 已违反 R3-M1 时直接给
//     终态错（不拿陈旧出口白跑一趟必败的 buildAccountCtx）。
//
// 返回 termErr 非空时调用方原样返回它（drain 登记已在锁内清掉）。
func (m *browserManager) refreshAcquireSnapshot(account *CookieAccount,
	proxyURL string, allowDirect bool, drain *acquireDrainReg) (
	freshAcct *CookieAccount, origPID, boundID int64,
	poolSnap map[int64]proxySnapEntry, strictBinding, poolBlocksDirect bool,
	termErr error) {
	poolSnap = snapshotProxyPool()
	strictBinding = rtCfg().StrictProxyBinding
	poolBlocksDirect = len(poolSnap) > 0 && !rtCfg().FallbackDirect
	freshAcct = account
	if account != nil && account.ID > 0 {
		freshAcct = accountByID(account.ID)
		if freshAcct == nil || freshAcct.Status != "enabled" {
			// 等待窗口内账号被停用/删除：摘掉可能存在的旧槽，清自身
			// drain 登记后直接终态返回，绝不拿死号跑 buildAccountCtx。
			m.InvalidateAccount(account.ID, false, "disabled")
			m.mu.Lock()
			drain.leave()
			m.mu.Unlock()
			return nil, 0, 0, nil, false, false,
				browserErr("account #%d unavailable", account.ID)
		}
		origPID = freshAcct.ProxyID
		boundID = matchProxyIDByURL(origPID, proxyURL)
	}
	// 请求级出口复核：管理员在等待窗口内改绑后，调用方携带的旧
	// (proxyURL, allowDirect) 可能已违反 R3-M1——在摘除旧槽（上一轮
	// acquirePlanLocked 已完成）后快速终态返回，绝不拿陈旧出口白跑
	// 一趟必败的 buildAccountCtx。
	if requestExitViolation(freshAcct, proxyURL, allowDirect,
		poolBlocksDirect, strictBinding, poolSnap) {
		m.mu.Lock()
		drain.leave()
		m.mu.Unlock()
		return nil, 0, 0, nil, false, false,
			browserErr("account #%d exit binding changed", freshAcct.ID)
	}
	return freshAcct, origPID, boundID, poolSnap, strictBinding, poolBlocksDirect, nil
}

// acquireAccountCtx 是主入口：为 (account, proxyURL, allowDirect) 取一个
// 就绪的 BrowserContext 租约。返回绑定具体 ac 指针的 (client, ac, sessID,
// release, err)；release 用 sync.Once 幂等归还 inFlight 并触发排空唤醒。
//
// allowCreate=false（辅助入口）在 m.mu 内拦截新建：槽位不存在或未就绪
// 直接返回 errBrowserSkipAux，绝不冷启动 Context。
//
// 单飞构造：第一个请求认领创建（起 buildAccountCtx 协程），并发等待者
// 阻塞在 ac.readyCh；客户端取消只退自己（不 buildCancel 连坐共享等待者）。
// 唤醒后第一时间核对 ac.disposed：已摘除立即返回 ac.lastBuildErr（严禁
// continue 用陈旧形参重建，v2.2 §1）。创建者本人直接等自己认领的
// nac.readyCh（绝不重查 m.ctxs[key] 引发快速失败死循环）。
func (m *browserManager) acquireAccountCtx(ctx context.Context,
	account *CookieAccount, proxyURL string, allowDirect bool, allowCreate bool) (
	*cdpClient, *accountBrowserCtx, string, func(), error) {

	key := makeBrowserSlotKey(account, proxyURL, allowDirect)
	// R9-1（Reviewer MF-1）：请求级身份契约——调用方入口快照的
	// cookieIdentity 是本次请求全程唯一认可的身份。等槽/构建窗口内扩展
	// 可按同标签同序号把该行换绑成另一会话 B（accountSync）：租约循环
	// 用 DB 新行替换请求快照，jar 播种读到 B、guard 比对的也是 B 与 B，
	// 全链路自洽却与请求方会话无关——用 B 的槽抓 token 还会按 A 的键
	// 入缓存（xsrf.go 共享键）。授予租约前必须核对 jar 身份与请求级
	// 身份一致；失配立即放还租约（绝不以旧键缓存新会话的 token）。
	// 纯 HTTP 哨兵（-1）与 0 兼容入口不受影响：它们不携带池账号快照
	// 或身份契约为空时本复核自动跳过。同身份票据轮转（SAPISID/1PSID
	// 不变）身份不变，热续期在飞请求不受影响。
	reqIdent := ""
	if account != nil && account.ID > 0 {
		reqIdent = cookieIdentity(account.Cookie)
	}

	// 首轮进 m.mu 前的出口快照（每轮循环开头都会用 freshAcct 重算——
	// 管理员可能在等待窗口内改绑代理/停用账号，绝不能用上一轮的陈旧
	// 指针做 R3-M1 复核，Reviewer #4）。
	var drain acquireDrainReg
	for {
		// ── 每轮进 m.mu 之前（锁外）刷新账号与代理池快照：poolSnap /
		// strictBinding / poolBlocksDirect 取自 proxyMu 与 rtCfg，freshAcct
		// 从 DB 读最新行——全部在持 m.mu 之前完成（m.mu 出度严格为 0，
		// v2.2 §10(1) / Oracle MF-5）。账号被停用/删除或请求级出口改绑
		// 违规时 helper 直接给出终态 err。
		freshAcct, origPID, boundID, poolSnap, strictBinding, poolBlocksDirect,
			termErr := m.refreshAcquireSnapshot(account, proxyURL, allowDirect, &drain)
		if termErr != nil {
			return nil, nil, "", nil, termErr
		}
		// R10-4（Reviewer M6/Oracle M6）：请求级身份与序号契约必须在
		// 进入 acquirePlanLocked 规划建槽/排空/摘除**之前**核对——否则
		// 行已换绑异会话 B（或序号已改）的旧 A 请求会先被规划排空/
		// 摘除走 P2 的合法 B 槽再谈拒租；正 ID 请求借行的 Jar 借完后
		// Close 还会把旧序号增量写回已改序号的行。身份/序号失配立即
		// 终态返回：零排空、零摘除、零全局拆槽。热续期（同身份同序号
		// 票据轮转身份不变）与 -1 纯 HTTP（reqIdent 为空）、0 兼容
		// 入口不受影响。
		if reqIdent != "" && freshAcct != nil {
			freshIdent := cookieIdentity(freshAcct.Cookie)
			if freshIdent != reqIdent {
				m.mu.Lock()
				drain.leave()
				m.mu.Unlock()
				return nil, nil, "", nil, browserErr(
					"account #%d identity contract mismatch: row rebound to a different session during lease",
					account.ID)
			}
			// 正 ID 携带请求入口序号契约：等槽期间管理员只改 authuser
			//（不拆槽）时，旧 /u/N 请求绝不能借该行 Jar 授租。
			if account.AuthUser != freshAcct.AuthUser {
				m.mu.Lock()
				drain.leave()
				m.mu.Unlock()
				return nil, nil, "", nil, browserErr(
					"account #%d authuser contract mismatch: row renumbered /u/%d -> /u/%d during lease",
					account.ID, account.AuthUser, freshAcct.AuthUser)
			}
		}

		m.mu.Lock()
		if err := ctx.Err(); err != nil {
			drain.leave()
			m.mu.Unlock()
			return nil, nil, "", nil, err
		}
		if m.stopping || m.shuttingDown || m.cleaning || m.client == nil {
			drain.leave()
			m.mu.Unlock()
			return nil, nil, "", nil, browserErr("browser not running")
		}

		ac := m.ctxs[key]
		if ac == nil {
			client, ac2, sessID, release, retry, err := m.acquireClaimOrWait(
				ctx, key, freshAcct, proxyURL, allowDirect, allowCreate,
				origPID, boundID, &drain)
			if retry {
				continue
			}
			return client, ac2, sessID, release, err
		}

		// ── 等待判定：锁内调度出 (waitCh, kind, proceed, detachedSnap) 四元 ──
		waitCh, kind, proceed, snap := m.acquirePlanLocked(ac, freshAcct,
			proxyURL, allowDirect, poolBlocksDirect, strictBinding, poolSnap, &drain)
		if snap.ac != nil && snap.err != nil && errors.Is(snap.err, errIdentityViolationSuspect) {
			m.mu.Unlock()
			det, termErr := m.resolveIdentitySuspect(ac, account)
			if termErr != nil {
				m.mu.Lock()
				drain.leave()
				m.mu.Unlock()
				return nil, nil, "", nil, termErr
			}
			if det.ac != nil {
				m.mu.Lock()
				client := m.client
				m.mu.Unlock()
				m.failStreamsForCtx(det.ac, det.err)
				if !det.wasCreating {
					m.disposeDetachedCtx(det.ac, det.bcID, det.bridge, det.syncCookie, client)
				}
			}
			continue
		}
		if snap.ac != nil && snap.err != nil && errors.Is(snap.err, errExitViolationSuspect) {
			// R9-3（Reviewer MF-3）：破坏性摘除前必须用一致快照复核——
			// freshAcct 是进 m.mu 前读的，读取与进锁之间并发配额请求可
			// 经 autoBindAccountProxy 把 DB 绑定推进并经 SyncAutoBoundProxy
			// 同步槽 orig：拿旧绑定比新 orig 会把合法槽误判成漂移摘除并
			// 终止在途流。锁外重取账号与代理池快照复核：确属并发自动绑
			// 定同步（或可核自动来源）则放弃摘除重试下一轮；真正手工改
			// 绑/失效出口仍确认摘除。复核不成立时绝不能摘槽。
			m.mu.Unlock()
			violation, snapVer := recheckSlotExitViolation(freshAcct, ac)
			if violation {
				// R10-1（Reviewer M1/Oracle M1）：重新加锁后仅当槽仍是
				// 同一实例且 exitSyncVer 未变才摘除——复核确认与提交摘除
				// 之间的合法自动绑定同步（P3 冷却不占用→自动绑回 P2 并对
				// 齐同一槽）递增版本号，失配即放弃摘除交下一轮重新规划，
				// 绝不按旧指针误摘刚恢复的合法槽、终止其在途流。
				snap2, client := m.detachIfExitVersionUnchanged(ac, snapVer, browserErr(
					"account #%d exit binding drifted", ac.key.accountID))
				if snap2.ac != nil {
					m.failStreamsForCtx(snap2.ac, snap2.err)
					if !snap2.wasCreating {
						m.disposeDetachedCtx(snap2.ac, snap2.bcID, snap2.bridge, snap2.syncCookie, client)
					}
				}
			}
			continue
		}
		if snap.ac != nil {
			client := m.client
			m.mu.Unlock()
			m.failStreamsForCtx(snap.ac, snap.err)
			if !snap.wasCreating {
				m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, snap.syncCookie, client)
			}
			continue
		}
		if proceed {
			// R9-1（Reviewer MF-1）：授予租约前核对 jar 身份与请求级身份
			// 契约一致——等槽/构建窗口内该行可能已被扩展按同标签同序号
			// 换绑成另一会话（租约循环的 freshAcct 是新行，guard 比对 B
			// 与 B 全链路自洽）：只有本处拿请求入口快照身份比对 jar 才能
			// 拦住「用 B 的槽抓 token 却按 A 的键入缓存」。失配直接终态
			// 返回（槽位留给新身份的合法请求复用，不摘除），绝不以旧键
			// 缓存新会话 token。ac.cookieIdentity 发布时定型后不可变，
			// 且本次 proceed 在持 m.mu 内读取，无数据竞争。
			if reqIdent != "" && ac.cookieIdentity != "" && ac.cookieIdentity != reqIdent {
				drain.leave()
				m.mu.Unlock()
				return nil, nil, "", nil, browserErr(
					"account #%d identity contract mismatch: leased jar is a different session (row rebound during lease)",
					account.ID)
			}
			ac.inFlight++
			ac.lastUsedAt = time.Now()
			client := m.client
			sessID := ac.sessionID
			m.mu.Unlock()
			release := m.acquireReleaseFn(ac)
			// R11-5（Oracle A6-序号交错/Reviewer #2）：授租前最后复核请求级
			// 身份与序号契约——m.mu 内不能做 DB I/O，而 freshAcct 读取与占租
			// 之间存在窗口：管理员可在窗口内只改 authuser（不拆槽的合法管
			// 理操作）或行身份再次轮换。在锁外重读最新行核验（不占租使用的
			// 请求尚未生效，归还租约即零副作用）：失配立即归还租约并终态
			// 拒绝，绝不把旧 /u/N 请求授予已改序号的行，也绝不因此拆槽。
			// 测试钩子（生产恒 nil）：精确落在「租约已占用、契约复核尚未执行」
			// 的交错点上——回归测试借此确定性构造「授租前窗口内行序号已变更」
			// 的 Oracle A6 序号交错，不再依赖 sleep 定序。生产路径绝不经由它
			// 做任何判断，绝不改变生产行为。
			if hook := loadTestHookBeforeLeaseContractRecheck(); hook != nil {
				hook(account)
			}
			if account != nil && account.ID > 0 {
				if cur := accountByID(account.ID); cur == nil || cur.Status != "enabled" ||
					cur.AuthUser != account.AuthUser ||
					(reqIdent != "" && cookieIdentity(cur.Cookie) != reqIdent) {
					release()
					m.mu.Lock()
					drain.leave()
					m.mu.Unlock()
					return nil, nil, "", nil, browserErr(
						"account #%d authuser contract mismatch: row renumbered or rebound during lease",
						account.ID)
				}
			}
			return client, ac, sessID, release, nil
		}
		if waitCh == nil {
			m.mu.Unlock()
			continue
		}
		stopCh := m.stopCh
		m.mu.Unlock()

		if client, ac2, sessID, release, retry, err := m.acquireWaitResult(
			ctx, ac, waitCh, stopCh, kind, &drain); !retry {
			return client, ac2, sessID, release, err
		}
	}
}

// testHookBeforeLeaseContractRecheck 仅供测试注入（生产恒为 nil）：在
// 租约占用之后、授租前契约复核（重读最新行）之前执行一次。生产路径
// 绝不经由它做任何判断，绝不改变生产行为。存取经原子容器——钩子读取
// 发生在请求协程，与测试的安装/复位并发交错是测试结构的固有行为；
// 生产恒 nil 时零开销、零行为影响。
var testHookBeforeLeaseContractRecheck atomic.Value // func(account *CookieAccount)

func loadTestHookBeforeLeaseContractRecheck() func(account *CookieAccount) {
	if v := testHookBeforeLeaseContractRecheck.Load(); v != nil {
		return v.(func(account *CookieAccount))
	}
	return nil
}

// resolveIdentitySuspect 复核 errIdentityViolationSuspect 标记（R11-5，
// Oracle A6-身份交错/Reviewer #1）：freshAcct 是进 m.mu 前读的，读取与进
// 锁之间 accountSync 可能已完成换绑 X→Y、摘掉旧 X 槽并由另一请求建好健康
// Y 槽——拿陈旧 freshAcct=X 比对驻留槽身份会把健康 Y 槽误判为「旧身份槽」。
// 锁外重读最新行复核（调用方不持 m.mu）：
//   - 行身份与槽身份一致（行未换绑，或又换回了同会话）：freshAcct 是中间
//     态陈旧快照，误杀成立——返回 termErr（identity contract mismatch），
//     健康槽绝不被摘；
//   - 行身份与槽身份仍不一致：在 m.mu 内以最新行**再次核验**后才返回
//     摘除快照（复核与摘除之间行又变/槽被替换/已摘除均自动放弃）。
//
// 返回 (det, termErr)：det.ac != nil 时调用方负责锁外 failStreams+dispose；
// 两者皆空表示行已删/停用（走下一轮 freshAcct 终态路径）。
func (m *browserManager) resolveIdentitySuspect(ac *accountBrowserCtx,
	account *CookieAccount) (detachedCtxSnap, error) {
	latest := accountByID(account.ID)
	if latest == nil || latest.Status != "enabled" {
		return detachedCtxSnap{}, nil
	}
	if cookieIdentity(latest.Cookie) == ac.cookieIdentity {
		return detachedCtxSnap{}, browserErr(
			"account #%d identity contract mismatch: row rebound to a different session during lease",
			account.ID)
	}
	var det detachedCtxSnap
	m.mu.Lock()
	if !ac.disposed && m.ctxs[ac.key] == ac &&
		cookieIdentity(latest.Cookie) != ac.cookieIdentity {
		det = m.detachSnapLocked(ac, browserErr("account #%d identity changed", ac.key.accountID))
	}
	m.mu.Unlock()
	return det, nil
}

// acquireClaimOrWait 处理「槽位不存在」分支：辅助入口拦截 / 认领创建后
// 直等 nac.readyCh（MF-4 绝不回循环顶重查 m.ctxs[key]）。返回语义与
// acquireAccountCtx 相同，retry=true 表示调用方 continue。调用方持 m.mu。
func (m *browserManager) acquireClaimOrWait(ctx context.Context,
	key browserSlotKey, account *CookieAccount, proxyURL string,
	allowDirect, allowCreate bool, origPID, boundID int64, drain *acquireDrainReg) (
	*cdpClient, *accountBrowserCtx, string, func(), bool, error) {

	if !allowCreate {
		drain.leave()
		m.mu.Unlock()
		return nil, nil, "", nil, false, errBrowserSkipAux
	}
	nac, victims, snapClient, snapGen, snapEpoch, buildCtx, waitCh, stopCh, err :=
		m.acquireClaimCreateLocked(key, account, proxyURL, allowDirect, origPID, boundID)
	if err != nil {
		drain.leave()
		m.mu.Unlock()
		return nil, nil, "", nil, false, err
	}
	m.mu.Unlock()
	for _, v := range victims {
		m.failStreamsForCtx(v.ac, browserErr("context evicted"))
		m.disposeDetachedCtx(v.ac, v.bcID, v.bridge, v.syncCookie, snapClient)
	}
	go m.buildAccountCtx(buildCtx, snapClient, snapGen, nac, snapEpoch, account)
	// 创建者直等自己认领的 nac.readyCh：绝不回循环顶重查
	// m.ctxs[key]——快速失败时 detach 已把它摘除，重查只会拿陈旧
	// 形参无限重建（Oracle MF-4）。
	select {
	case <-waitCh:
		m.mu.Lock()
		if nac.disposed {
			err := nac.lastBuildErr
			m.mu.Unlock()
			if err == nil {
				err = browserErr("account context invalidated during build")
			}
			return nil, nil, "", nil, false, err
		}
		m.mu.Unlock()
		return nil, nil, "", nil, true, nil
	case <-ctx.Done():
		m.mu.Lock()
		drain.leave()
		m.mu.Unlock()
		return nil, nil, "", nil, false, ctx.Err()
	case <-stopCh:
		m.mu.Lock()
		drain.leave()
		m.mu.Unlock()
		return nil, nil, "", nil, false, browserErr("browser is stopping")
	}
}

// acquireWaitResult 处理 acquirePlanLocked 返回 waitCh 后的阻塞等待与
// 唤醒分流（MF-8 drainWaiters 状态机）。retry=true 表示调用方 continue。
// 锁外调用（调用方已释放 m.mu）。
func (m *browserManager) acquireWaitResult(ctx context.Context,
	ac *accountBrowserCtx, waitCh, stopCh chan struct{},
	kind acquireWaitKind, drain *acquireDrainReg) (
	*cdpClient, *accountBrowserCtx, string, func(), bool, error) {

	select {
	case <-waitCh:
		m.mu.Lock()
		defer m.mu.Unlock()
		if !ac.disposed {
			return nil, nil, "", nil, true, nil // 重查最新槽位
		}
		// waitDrain：因换出口（errExitChanged）/序号代际更替
		//（errAuthUserChanged）摘除时清自身登记后汇入新槽；因
		// Invalidate/停机摘除则返回终态错（v2.2 §3 / MF-8）。
		if kind == waitDrain && (errors.Is(ac.lastBuildErr, errExitChanged) ||
			errors.Is(ac.lastBuildErr, errAuthUserChanged)) {
			if drain.registered && drain.target == ac {
				drain.registered = false
				drain.target = nil
			}
			return nil, nil, "", nil, true, nil
		}
		err := ac.lastBuildErr
		drain.leave()
		if err == nil {
			err = browserErr("account context invalidated during build")
		}
		return nil, nil, "", nil, false, err
	case <-ctx.Done():
		m.mu.Lock()
		drain.leave()
		m.mu.Unlock()
		return nil, nil, "", nil, false, ctx.Err()
	case <-stopCh:
		m.mu.Lock()
		drain.leave()
		m.mu.Unlock()
		return nil, nil, "", nil, false, browserErr("browser is stopping")
	}
}

// acquireWaitKind 区分 acquirePlanLocked 返回的阻塞通道语义。
type acquireWaitKind int

const (
	waitNone  acquireWaitKind = iota // 不阻塞（waitCh==nil）
	waitReady                        // 等 ac.readyCh（创建/重建完成）
	waitDrain                        // 等 ac.drainWait（排空换出口）
)

// acquireClaimCreateLocked 认领空槽位：淘汰腾位 → 注册 creating 槽 →
// 返回新槽 / 被淘汰 victims / 启动 buildAccountCtx 所需全部快照 /
// 创建者直等的 waitCh（nac.readyCh）与 stopCh。
//
// origPID/boundID 是进 m.mu 前锁外预计算的出口快照（纯内存赋值，本函数
// 绝不再访问 proxyMu）。调用方持 m.mu 且不持锁外锁；err 非空时调用方
// 直接返回。
func (m *browserManager) acquireClaimCreateLocked(key browserSlotKey,
	account *CookieAccount, proxyURL string, allowDirect bool,
	origPID, boundID int64) (
	nac *accountBrowserCtx, victims []detachedCtxSnap,
	snapClient *cdpClient, snapGen, snapEpoch uint64,
	buildCtx context.Context, waitCh chan struct{}, stopCh chan struct{}, err error) {

	victims = m.evictIdleOrOverflowLocked(time.Now(), true)
	if len(m.ctxs) >= maxBrowserContexts {
		return nil, nil, nil, 0, 0, nil, nil, nil, browserErr(
			"browser context limit (%d) reached; all contexts busy", maxBrowserContexts)
	}
	nac = &accountBrowserCtx{
		key:                key,
		origAccountProxyID: origPID,
		boundProxyID:       boundID,
		proxyURL:           proxyURL,
		allowDirect:        allowDirect,
		creating:           true,
	}
	if account != nil {
		nac.authuser = account.AuthUser
	}
	m.nextCtxEpoch++
	nac.epoch = m.nextCtxEpoch
	// 独立 buildCtx 用 browserCtxBuildTimeout（25s）兜底：既保持单飞共享构建
	// 不被单个短命客户端 ctx 取消连坐（TestBrowserConcurrentBuilds 契约），
	// 又确保调用方放弃后孤儿构建严格有界、到期经 AfterFunc 自动断桥回收。
	buildCtx, buildCancel := context.WithTimeout(context.Background(), browserCtxBuildTimeout)
	nac.buildCancel = buildCancel
	nac.resetReadyLocked()
	m.ctxs[key] = nac
	return nac, victims, m.client, m.generation, nac.epoch, buildCtx,
		nac.readyCh, m.stopCh, nil
}

// acquireReleaseFn 构造绑定 ac 指针的 sync.Once 幂等租约归还闭包：归还
// inFlight 并在归零且有跨出口等待者时关闭 drainWait 唤醒被暂缓请求。
func (m *browserManager) acquireReleaseFn(ac *accountBrowserCtx) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			if ac.inFlight > 0 {
				ac.inFlight--
			}
			if ac.inFlight == 0 && ac.drainWaiters > 0 {
				ac.closeDrainLocked()
			}
			ac.lastUsedAt = time.Now()
			m.mu.Unlock()
		})
	}
}

// proxySnapEntry 是单个代理在锁外快照里的启用态与 URL。
type proxySnapEntry struct {
	enabled bool
	url     string
}

// snapshotProxyPool 在进 m.mu 前一次性快照代理池（id → enabled/url）。
// 供 acquirePlanLocked 的 R3-M1 强绑定复核做纯内存比对（m.mu 出度 0）。
func snapshotProxyPool() map[int64]proxySnapEntry {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	out := make(map[int64]proxySnapEntry, len(proxyCache))
	for _, p := range proxyCache {
		out[p.ID] = proxySnapEntry{enabled: p.Enabled, url: p.URL}
	}
	return out
}

// acquirePlanLocked 是 acquireAccountCtx 的锁内判定段（调用方持 m.mu）。
// 返回 (waitCh, kind, proceed, snap)：
//   - snap.ac != nil：本调用是摘除 winner（身份变更 / 唯一替换 / 强绑定
//     违规），快照字段已在 m.mu 内核出，调用方锁外 failStreams 后按
//     wasCreating 决定是否 disposeDetachedCtx，再 continue；
//   - proceed=true：可立即占 inFlight 租约返回；
//   - waitCh != nil：按 kind 阻塞等 readyCh（构建）或 drainWait（排空）。
//
// poolBlocksDirect/strictBinding/poolSnap 是锁外预计算的代理快照，本
// 函数只做纯内存比对（m.mu 出度 0）；account 是本轮从 DB 读出的
// freshAcct（Reviewer #4）。
//
// drain 跟踪本调用方的跨出口等待名额（跨出口状态机）。跨出口等待
// 名额只在三处递减：客户端 ctx.Done()/stopCh 放弃、成为替换 winner、或
// ac 已 disposed——绝不在 drainWaiters>0 且 inFlight>0 时放行同出口新
// 请求（v2.2 §3 唯一替换权状态机）。
func (m *browserManager) acquirePlanLocked(ac *accountBrowserCtx, account *CookieAccount,
	proxyURL string, allowDirect bool, poolBlocksDirect, strictBinding bool,
	poolSnap map[int64]proxySnapEntry, drain *acquireDrainReg) (
	waitCh chan struct{}, kind acquireWaitKind, proceed bool, snap detachedCtxSnap) {

	// 构建/重建中：一律等 readyCh（构建失败由唤醒者读 lastBuildErr）。
	if ac.creating || ac.recreating {
		// 等待期间若曾登记跨出口名额，先清（槽位状态已变）。
		if drain.registered && drain.target == ac {
			drain.leave()
		}
		return ac.readyCh, waitReady, false, detachedCtxSnap{}
	}

	// 匿名槽：同 key 即同出口，无跨出口排空竞争。
	if ac.key.accountID == 0 {
		if ac.drainWaiters == 0 {
			return nil, waitNone, true, detachedCtxSnap{}
		}
		return ac.ensureDrainLocked(), waitDrain, false, detachedCtxSnap{}
	}

	// 登录槽身份守卫：jar 身份不符 → 摘除旧槽为自己腾位。
	// R11-5（Oracle A6/Reviewer #1）：绝不依据进 m.mu 前读取的 freshAcct
	// 直接破坏性摘除——读取与进锁之间 accountSync 可能已完成换绑 X→Y、
	// 摘掉旧 X 槽并由另一请求建好健康 Y 槽：拿陈旧 freshAcct=X 比对会把
	// 健康 Y 槽误判为「旧身份槽」直接摘除并终止其在途流。改以
	// errIdentityViolationSuspect 标记交调用方锁外重读最新行复核：行身份
	// 已与槽身份一致（同会话收敛）则放弃摘除；确已换绑异会话才执行摘除。
	if ac.cookieIdentity != "" && account != nil &&
		cookieIdentity(account.Cookie) != ac.cookieIdentity {
		return nil, waitNone, false, detachedCtxSnap{ac: ac, err: errIdentityViolationSuspect}
	}

	// R3-M1 强绑定复核：既有就绪槽位在占租约前用锁外预计算的代理快照
	// + freshAcct 做纯内存比对，命中违规**疑似**漂移（Oracle MF-5 /
	// Reviewer #4）。R9-3（Reviewer MF-3）：绝不依据进 m.mu 前读取的
	// 账号快照直接破坏性摘除——读取与进锁之间并发自动绑定可能已同步
	// 槽 orig，用旧绑定比新 orig 会把合法槽误杀并终止在途流。改以
	// errExitViolationSuspect 标记交调用方锁外以一致快照复核后再定
	// 摘除；真正手工改绑/失效出口在复核后仍确认摘除，拦截语义不变。
	if account != nil {
		if ac.readySlotExitViolation(account, poolBlocksDirect, strictBinding, poolSnap) {
			return nil, waitNone, false, detachedCtxSnap{ac: ac, err: errExitViolationSuspect}
		}
	}

	// R12-4（Oracle M6/Reviewer #5）authuser 槽代际隔离：行序号与建槽
	// 序号不一致（管理员已改号 /u/0 -> /u/1）时，正 ID 请求绝不能复用旧
	// 序号槽——它的 Jar 写回 CAS 以旧序号作谓词必然 0 行命中（回写黑洞），
	// 且改回旧序号时新序号产生的增量反而会串写进旧序号行。按「换出口」
	// 同构处理：inFlight==0 时唯一替换 winner 摘除旧槽建新序号独立槽
	//（nac.authuser 记录新序号），inFlight>0 时登记排空等待——旧槽上的
	// 既有在途流自然排空（绝不强杀改号前的在途流）。纯 HTTP 哨兵（-1）
	// 与 0 兼容入口（accountID<=0）不携带序号契约，不受影响。
	sameAuthuser := account == nil || account.ID <= 0 || account.AuthUser == ac.authuser
	sameExit := ac.proxyURL == proxyURL && ac.allowDirect == allowDirect && sameAuthuser
	if sameExit {
		if ac.drainWaiters == 0 {
			// 此前误登记的跨出口名额先清（状态已变）。
			if drain.registered && drain.target == ac {
				drain.leave()
			}
			return nil, waitNone, true, detachedCtxSnap{}
		}
		// 有跨出口等待者：同出口被暂缓，不占名额等 drainWait。
		return ac.ensureDrainLocked(), waitDrain, false, detachedCtxSnap{}
	}

	// 同登录槽非严格模式合法换出口 / 序号代际更替：inFlight==0 时唯一
	// 替换 winner。R12-4：序号不符（sameAuthuser=false）用专属终态原因——
	// 跨序号等待者与换出口等待者同语义汇入新槽，诊断上可区分两类更替。
	if ac.inFlight == 0 {
		if drain.registered && drain.target == ac {
			drain.leave()
		}
		cause := errExitChanged
		if !sameAuthuser && ac.proxyURL == proxyURL && ac.allowDirect == allowDirect {
			cause = errAuthUserChanged
		}
		if snap := m.detachSnapLocked(ac, cause); snap.ac != nil {
			return nil, waitNone, false, snap
		}
		return nil, waitNone, false, snap
	}

	// 有在飞：登记跨出口排空等待名额。
	if !drain.registered || drain.target != ac {
		ac.drainWaiters++
		drain.registered = true
		drain.target = ac
	}
	return ac.ensureDrainLocked(), waitDrain, false, detachedCtxSnap{}
}

// readySlotExitViolation 是既有就绪登录槽的 R3-M1 强绑定复核（纯内存
// 比对，全部代理事实由调用方锁外预计算的 poolSnap + freshAcct 提供；
// 调用方持 m.mu，绝不自行加锁）。
// 返回 true 表示该槽位出口已违反绑定规则，应立即摘除重建。规则：
//   - (a) 账号已锁直连（proxyIDDirect）但槽位不是显式直连；
//   - (b) 池禁直连时槽位直连但账号并非锁定直连（-1 -> 0 反向防泄漏）；
//   - (c) 严格模式或「原绑定已变」时账号新绑定代理启用且其 URL 与槽位
//     出口漂移（Reviewer #4 与 exitCompatCheck 同口径）；
//   - (d) 槽位 boundProxyID 已删除/禁用或与槽位出口 URL 漂移。
func (ac *accountBrowserCtx) readySlotExitViolation(account *CookieAccount,
	poolBlocksDirect, strictBinding bool, poolSnap map[int64]proxySnapEntry) bool {
	// 调用方必须已持 m.mu（acquirePlanLocked 锁内判定段的直接调用）——
	// 槽位字段在本临界区内本就可安全读取，快照只用于统一走纯值判定。
	return exitViolationForFacts(ac.snapshotSlotExitFactsLocked(),
		account, poolBlocksDirect, strictBinding, poolSnap)
}

// requestExitViolation 是请求级 (proxyURL, allowDirect) 对 freshAcct 的
// R3-M1 复核（纯内存，锁外调用）：管理员在等待窗口内改绑后，调用方
// 携带的旧出口可能已不合法——此时直接终态返回比拿陈旧出口跑一趟
// 必败的 buildAccountCtx 更优。规则与 readySlotExitViolation 同口径：
//   - (a) 账号锁直连但请求不是显式直连；
//   - (b) 池禁直连时请求直连但账号并非锁定直连；
//   - (c) 严格模式下账号新绑定代理启用且其 URL 与请求出口漂移。
func requestExitViolation(account *CookieAccount, proxyURL string,
	allowDirect, poolBlocksDirect, strictBinding bool,
	poolSnap map[int64]proxySnapEntry) bool {
	if account == nil || account.ID <= 0 {
		return false
	}
	// (a) 账号锁直连：请求必须是显式直连。
	if account.ProxyID == proxyIDDirect && (proxyURL != "" || !allowDirect) {
		return true
	}
	// (b) 池禁直连：请求直连但账号不是锁直连 → 反向泄漏。
	if proxyURL == "" && poolBlocksDirect && account.ProxyID != proxyIDDirect {
		return true
	}
	// (c) 严格模式：账号绑定代理启用且 URL 与请求出口漂移。
	if strictBinding && account.ProxyID > 0 {
		if ent, ok := poolSnap[account.ProxyID]; ok && ent.enabled &&
			ent.url != proxyURL {
			return true
		}
	}
	return false
}

// buildAccountCtx 7 步构造（全程 m.mu 外，用 buildCtx 执行；buildCancel
// 毫秒级打断）：
//  1. newBrowserProxyBridge + SetUpstream(ac.proxyURL, ac.allowDirect)，挂
//     AfterFunc 断桥守卫；
//  2. Target.createBrowserContext({proxyServer, disposeOnDetach:true})；
//  3. Target.createTarget(about:blank, bcID)；4. attachToTarget(flatten)；
//  5. enablePageDomainFor + installBridgeFor；
//  6. 登录槽：acctCookieLock 读 fresh + R3-M1 4 条出口兼容核验 + 登记
//     maxSeededEpoch + seedContextCookies；
//  7. navigateAndWaitApp 首航 /app（非断连错误包装 pageFetchErr）；
//  8. 回 m.mu 强代际提交守卫。
//
// 局部 (localBCID, localBridge) 由本函数唯一拥有：退出或提交核对发现
// ac.disposed 时统一走 disposeDetachedCtx(ac, localBCID, localBridge,
// ac.syncOnDispose, client)。
func (m *browserManager) buildAccountCtx(buildCtx context.Context,
	client *cdpClient, gen uint64, ac *accountBrowserCtx, snapEpoch uint64,
	account *CookieAccount) {

	var localBCID string
	var localBridge *browserProxyBridge
	var buildErr error
	var freshIdent string
	var freshCookie string // 步骤 6 锁内读出的最新账号 Cookie，步骤 7 首航跨源判定用
	var sessID, targetID string
	// stopKill 解除步骤 1 的 AfterFunc 取消守卫；先声明让发布 defer 能
	// 在成功分支先调用它。未赋值时为 nil——发布只在步骤 1 完成后发生，
	// 故发布分支调用时必非 nil。
	var stopKill func() bool

	defer func() {
		// Reviewer #3：成功发布前先 stopKill() 解除取消守卫——否则
		// ac.buildCancel() 会触发 AfterFunc 调 bridge.Shutdown 关掉刚发布
		// 的健康 ac.bridge（竞态产生带死桥的活槽）。
		m.mu.Lock()
		// R4-5（Reviewer MF-5）：发布前必须解除 buildCtx 的 AfterFunc 断桥
		// 守卫并**核对解除结果与 buildCtx 未到期**——stopKill() 返回 false
		//（定时器已到期、kill 回调已启动或正在关闭 bridge）或 buildCtx
		// 已到期时，bridge 即将/已经被 Shutdown，严禁把健康标记与一座死桥
		// 一起发布：统一走下方失败清理路径（detach + 销毁局部资源）。
		stopped := false
		if stopKill != nil {
			stopped = stopKill()
		}
		expired := buildCtx.Err() != nil
		// 发布守卫（fix #3）：构建期间 DB 代际可能已被推进（后台对同身份兄弟号
		// 热续期时本槽尚在 creating，写库并 BumpAccountEpoch）——此时本槽 jar
		// 里播种的是轮转前的旧票据。一经 superseded 严禁发布：直接销毁旧 Jar
		// 上下文并返回可重试错误，彻底杜绝「存活旧 Jar + 新 DB + 永久 SIDCC
		// 回写黑洞」。本检查与 BumpAccountEpoch 同受 m.mu 串行化：要么发布先
		// 完成（热注入方看到就绪槽走热注入、不 Bump），要么 Bump 先完成
		//（此处必然拒绝发布），交错窗口不存在。
		superseded := ac.key.accountID > 0 && m.isSupersededEpochLocked(ac.key.accountID, snapEpoch)
		ok := buildErr == nil && m.client == client && m.generation == gen &&
			!m.stopping && !m.shuttingDown && !ac.disposed && !superseded &&
			stopped && !expired &&
			ac.epoch == snapEpoch && m.ctxs[ac.key] == ac
		if ok {
			if ac.buildCancel != nil {
				ac.buildCancel()
				ac.buildCancel = nil
			}
			ac.bridge = localBridge
			ac.browserContextID = localBCID
			ac.targetID = targetID
			ac.sessionID = sessID
			ac.cookieIdentity = freshIdent
			ac.creating = false
			ac.lastUsedAt = time.Now()
			m.sessionIndex[sessID] = ac
			m.targetIndex[targetID] = ac
			ac.closeReadyLocked()
			m.mu.Unlock()
			return
		}
		// 失败或已过时：若仍是本代际且未摘除，走 detach 写 lastBuildErr
		// 唤醒等待者；随后统一清理局部资源。
		// streamErr 在锁内一次性定型（Oracle SF-1）：superseded 场景
		// buildErr==nil 时绝不能把 nil 传给 failStreamsForCtx——s.fail(nil)
		// 会让在飞流 fatalErr 为 nil，被下游误当「正常结束但无内容」归
		// reasonNoContentFrame（classContentRefuse 白烧跳槽预算且诊断方向全错）。
		streamErr := buildErr
		if streamErr == nil {
			switch {
			case superseded:
				// 可重试错误：下次请求会重新认领构建并播种新 Cookie。
				// browserLocalError 本地代际终止：归 classLocalAbort 不扣代理分。
				streamErr = seedSupersededErr("build")
			case expired || !stopped:
				// R4-5：导航成功但 buildCtx 截止先于发布（bridge 已被/将被
				// AfterFunc 关闭）——本地构建超时终止，browserLocalError 归
				// classLocalAbort 不扣代理分；可重试（下次请求重新认领构建）。
				streamErr = browserErr("build deadline expired before publish: bridge torn down, not publishing")
			default:
				streamErr = browserErr("account context invalidated during build")
			}
		}
		if !ac.disposed && ac.epoch == snapEpoch && m.ctxs[ac.key] == ac {
			m.detachCtxLocked(ac, streamErr)
		}
		syncCookie := ac.syncOnDispose
		bcID, bridge := localBCID, localBridge
		m.mu.Unlock()
		m.failStreamsForCtx(ac, streamErr)
		m.disposeDetachedCtx(ac, bcID, bridge, syncCookie, client)
	}()

	// 步骤 1：专属桥 + 取消断桥守卫。
	bridge, err := newBrowserProxyBridge()
	if err != nil {
		buildErr = err
		return
	}
	localBridge = bridge
	if err := bridge.SetUpstream(ac.proxyURL, ac.allowDirect); err != nil {
		buildErr = err
		return
	}
	stopKill = context.AfterFunc(buildCtx, func() {
		bridge.Shutdown(context.Background())
	})
	// R4-5：stopKill 只允许在发布 defer 里调用一次——defer 是 LIFO，
	// 发布 defer 注册在最前反而最后执行；任何更早注册的兜底 defer 若先
	// 调 stopKill()，发布临界区拿到的返回值恒为 false（已停止），既无法
	// 区分「定时器未到期」与「到期 kill 已启动」，也会让 success 路径误判。
	// 发布 defer 覆盖全部返回路径（成功解除守卫/失败清理），无需额外兜底。

	// 步骤 2：独立 BrowserContext（disposeOnDetach 连带关其下 target）。
	// R7-5/R7-6（Reviewer MF-5/MF-6）：创建命令经 CallWithLateHandler 发送——
	//  1. 写发送准入（排队 writeSem 与写入 WS）严格受 buildCtx 约束：
	//     构建已取消/到期时绝不向共享连接发送，半活连接不累积外发命令；
	//  2. buildCtx 先于回复到期时，pending 槽位不删除、无固定收割窗口：
	//     连接存活时迟到多久的真实回复到达都由 readLoop 一次性回调
	//     onLate——解码出非空 browserContextId 立即发 3s 超时的
	//     Target.disposeBrowserContext 单次清理（绝不关闭兄弟槽共用的
	//     CDP 连接）；回复永远不到或连接断开时 failAll 清理 pending 自动
	//     释放，零常驻等待协程（不再每次超时换 watch+Call+reaper 三条）。
	var bcRes struct {
		BrowserContextID string `json:"browserContextId"`
	}
	err = client.CallWithLateHandler(buildCtx, "", "Target.createBrowserContext",
		map[string]interface{}{
			"proxyServer":     fmt.Sprintf("http://127.0.0.1:%d", bridge.Port()),
			"disposeOnDetach": true,
		}, &bcRes, makeLateBCReaper(client))
	if err != nil {
		buildErr = browserErr("createBrowserContext: %w", err)
		return
	}
	localBCID = bcRes.BrowserContextID
	if localBCID == "" {
		buildErr = browserErr("createBrowserContext: empty browserContextId")
		return
	}

	// 步骤 3：本 Context 内建空白页。
	var created struct {
		TargetID string `json:"targetId"`
	}
	if err := client.Call(buildCtx, "", "Target.createTarget",
		map[string]interface{}{"url": "about:blank", "browserContextId": localBCID},
		&created); err != nil {
		buildErr = browserErr("createTarget: %w", err)
		return
	}
	targetID = created.TargetID

	// 步骤 4：flatten attach 到本 page。
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := client.Call(buildCtx, "", "Target.attachToTarget",
		map[string]interface{}{"targetId": targetID, "flatten": true}, &attached); err != nil {
		buildErr = browserErr("attachToTarget: %w", err)
		return
	}
	sessID = attached.SessionID

	// 步骤 5：Page 域 + 注入桥。
	if err := m.enablePageDomainFor(buildCtx, client, sessID); err != nil {
		buildErr = err
		return
	}
	if err := m.installBridgeFor(buildCtx, client, sessID); err != nil {
		buildErr = err
		return
	}

	// 步骤 6：登录槽播种 Cookie + R3-M1 出口兼容核验。
	if account != nil && account.ID > 0 {
		unlockCookie, err := acquireAcctCookieLock(buildCtx, account.ID)
		if err != nil {
			// R4-6（Reviewer MF-6）：等待本地 L1 Cookie 锁超时/取消是**本地**
			// 构建终止，不是上游/出口网络故障——包装为 browserLocalError
			//（%w 保留底层 cause），isBrowserLocalError 判真、
			// classifyFailoverError 归 classLocalAbort：绝不扣无辜代理分、
			// 绝不烧跳槽预算。真实 Page.navigate 超时的网络分类不受影响。
			buildErr = browserErr("wait cookie lock: %w", err)
			return
		}
		fresh := accountByID(account.ID)
		if fresh == nil || fresh.Status != "enabled" {
			unlockCookie()
			buildErr = browserErr("account #%d unavailable during seed", account.ID)
			return
		}
		freshIdent = cookieIdentity(fresh.Cookie)
		freshCookie = fresh.Cookie
		if cerr := exitCompatCheck(ac, fresh); cerr != nil {
			unlockCookie()
			buildErr = cerr
			return
		}
		// R7-4（Reviewer MF-4）：消除「先绑后登记」窗口——冷建锁外取到
		// 旧 orig 后、槽位注册前，并发配额查询的合法自动首绑/重绑可能
		// 已将 DB 绑定落到本槽实际出口（SyncAutoBoundProxy 查无槽直接
		// 返回）。此刻 fresh 是 L1 锁内重读的 DB 最新事实。
		// R8-3（Reviewer MF-3）：对齐绝不再以「绑定代理实时 URL 与本槽
		// 出口一致」为依据——管理员手工 PATCH 恰好改绑到本槽溢出出口
		// 时 URL 同样一致，快照错位会被当作自动绑定放行，等于给手工
		// 改绑开了一条绕过改绑检测的伪装通道。只认可核的自动绑定来源
		// 记录（isAutoBoundProxyForURL：autoBindAccountProxy/claimAccountProxy
		// 真实写中目标行后才登记，管理员手工 PATCH 与账号删除一并清除），
		// 且该记录的 proxyURL 与本槽实际出口精确一致——在对齐 snapEpoch
		// 的同一 m.mu 临界区内一并对齐 orig/bound，先绑后登记与先登记
		// 后绑均得到正确事实。claimMu 与 proxyMu 查询在持 m.mu 之前完成
		//（m.mu 出度严格为 0，绝不持 m.mu 做 DB/代理查询或拿 claimMu）。
		autoBoundAligned := fresh.ProxyID > 0 &&
			isAutoBoundProxyForURL(fresh.ID, fresh.ProxyID, ac.proxyURL)
		m.mu.Lock()
		// R3-2 代际对齐（仍在 L1 锁内，L1→L2 锁序，m.mu 出度 0）：注册后、
		// 播种前发生的写 DB+Bump 已由 L1 串行化保证播种读到的 fresh 是新
		// Cookie——对齐后的发布守卫必然放行；播种后、发布前发生的 Bump
		// 则仍由发布守卫 100% 拦截（旧 Jar 绝不发布）。辅助回收路径的
		// 「检查无槽 → 写库 → Bump」与前台「注册槽 → 播种」交错窗口由此
		// 闭合：两事件在 L1 下严格串行，播种所见 DB 与代际判定永远一致。
		if m.maxSeededEpoch[account.ID] > snapEpoch {
			snapEpoch = m.maxSeededEpoch[account.ID]
			ac.epoch = snapEpoch
		}
		if snapEpoch > m.maxSeededEpoch[account.ID] {
			m.maxSeededEpoch[account.ID] = snapEpoch
		}
		// R7-4：合法自动绑定落到本槽实际出口时对齐槽事实（仅当本槽尚未
		// 被 SyncAutoBoundProxy 同步过——已对齐时赋值幂等无副作用）。
		if autoBoundAligned && ac.origAccountProxyID != fresh.ProxyID {
			ac.origAccountProxyID = fresh.ProxyID
			ac.boundProxyID = fresh.ProxyID
			ac.exitSyncVer++ // R10-1：对齐同样推进出口同步版本
		}
		m.mu.Unlock()
		if err := m.seedContextCookies(buildCtx, client, localBCID, fresh.ID, fresh.Cookie); err != nil {
			unlockCookie()
			buildErr = err
			return
		}
		unlockCookie()
	}

	// 步骤 7：首航 /app。跨源重定向（accounts.google.com / /sorry）
	// 毫秒级快速失败并以 *appPageStatusError 原样透传（含 MissingOSID），
	// 真超时保留 %w DeadlineExceeded 链，仅断连/停机才归 browserLocalError。
	if err := m.navigateAndWaitAppCookie(buildCtx, client, sessID, freshCookie); err != nil {
		buildErr = classifyNavBuildErr(buildCtx, client, "navigate /app", err)
		return
	}
}

// makeLateBCReaper 构造 Target.createBrowserContext 的迟到回复回收回调
// （R7-5/R7-6）：构建方已按 buildCtx 放弃等待但创建命令早已发出，若
// Chromium 稍后返回新建的 browserContextId，无人回收会让它泄漏在共享
// CDP 连接上（disposeOnDetach 要等连接断开才生效）。收到非空 bcID 且
// err==nil 时按 ID 发一次带 3s 超时的 Target.disposeBrowserContext 单次
// 清理——绝不关闭兄弟槽共用的 CDP 连接；连接已断/回复出错/空 ID 时
// 自然终结。本回调由 cdpClient 的 readLoop 在真实回复到达时经一次性
// 短协程调用，无固定收割窗口、无常驻等待协程。
func makeLateBCReaper(client *cdpClient) func(raw json.RawMessage, err error) {
	return func(raw json.RawMessage, err error) {
		if err != nil || client == nil || client.Closed() {
			return
		}
		var bcRes struct {
			BrowserContextID string `json:"browserContextId"`
		}
		if json.Unmarshal(raw, &bcRes) != nil || bcRes.BrowserContextID == "" {
			return
		}
		bcID := bcRes.BrowserContextID
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = client.Call(ctx, "", "Target.disposeBrowserContext",
				map[string]interface{}{"browserContextId": bcID}, nil)
		}()
	}
}

// classifyNavBuildErr 归一化 buildAccountCtx 与 doRecreateTab 的导航错误
// （替代旧的无条件 pageFetchErr 字符串抹平）：
//   - *appPageStatusError（含 MissingOSID）原样透传，供 xsrfErrBlamesCookie /
//     classifyFailoverError / upstreamReason 精确归类；
//   - client 断连或停机/拆槽主动取消：原样保留 browserLocalError / Canceled；
//   - 非 browserLocalError（pageFetchErr / %w DeadlineExceeded 真超时）：
//     加前缀包装并保留错误链；
//   - 其余非断连 CDP 错：剥离 browserLocalError 归页内上游故障；若底层
//     errors.Is(err, context.DeadlineExceeded)（Page.navigate RPC 自身超时被
//     browserErr 包装），剥离时以 %w 保留 DeadlineExceeded 链——否则
//     isUpstreamTimeout 判假、upstreamReason 落 upstream_error 且字符串残留
//     browser: 前缀被管理面板归 browser_unavailable。真断连/主动取消已在
//     上方提前返回，本分支绝不误伤（不扣代理分语义不变）。
func classifyNavBuildErr(buildCtx context.Context, client *cdpClient, prefix string, err error) error {
	var ape *appPageStatusError
	if errors.As(err, &ape) {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	if client == nil || client.Closed() || errors.Is(buildCtx.Err(), context.Canceled) {
		return err
	}
	if !isBrowserLocalError(err) {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		// RPC 超时被本地类型包装：剥离 browserLocalError（顺带去掉 browser:
		// 前缀），保留 DeadlineExceeded 错误链。
		var be *browserLocalError
		if errors.As(err, &be) {
			return fmt.Errorf("page fetch error: %s: %w", prefix, be.err)
		}
		return fmt.Errorf("page fetch error: %s: %w", prefix, err)
	}
	return pageFetchErr(fmt.Sprintf("%s: %v", prefix, err))
}

// exitCompatCheck 是 R3-M1 4 条出口兼容核验（播种前调用，此时持
// acctCookieLock 不持 m.mu）。ac.proxyURL/allowDirect 建槽后不可变可免锁读；
// R7-3（Reviewer MF-3）：origAccountProxyID/boundProxyID 会被并发配额
// 自动首绑/重绑的 SyncAutoBoundProxy 在 m.mu 下改写——此处必须短持 m.mu
// 拷贝这两个标量后立即解锁（绝不持 m.mu 做 proxyMu/DB 查询），消除两把
// 锁不互斥的 Go 数据竞争。错误文本中的代理 URL 一律经 redactProxyURL
// 脱敏（携带 userinfo 认证信息的完整 URL 绝不进错误/日志/面板）。
func exitCompatCheck(ac *accountBrowserCtx, fresh *CookieAccount) error {
	m := browserMgr
	m.mu.Lock()
	origAccountProxyID := ac.origAccountProxyID
	boundProxyID := ac.boundProxyID
	m.mu.Unlock()
	// (a) 账号锁定直连（-1）但槽位不是显式直连：防穿透改绑窗口。
	if fresh.ProxyID == proxyIDDirect && (ac.proxyURL != "" || !ac.allowDirect) {
		return browserErr("account #%d locked direct but slot exit=%q allowDirect=%v",
			fresh.ID, redactProxyURL(ac.proxyURL), ac.allowDirect)
	}
	// (b) 池非空 + FallbackDirect=false 时，槽位直连但账号并非锁定直连：
	// 拦截「-1 -> 0」反向解锁漏 IP。
	if ac.proxyURL == "" && hasProxyPool() && !rtCfg().FallbackDirect &&
		fresh.ProxyID != proxyIDDirect {
		return browserErr("account #%d slot direct but pool forbids direct (bound=#%d)",
			fresh.ID, fresh.ProxyID)
	}
	// (c) fresh.ProxyID>0 且启用：绑定出口与槽位出口不一致时——严格模式
	// 或「原绑定已变」才中止；非严格模式下原绑代理被禁用且合法溢出到
	// 健康新代理时放行（autoBindAccountProxy 自愈）。
	// R8-3（Reviewer MF-3）快照一致性复核：读到 orig != fresh.ProxyID 时，
	// 若当前 orig 本身是「刚被并发自动绑定同步进本槽」的可核来源
	//（isAutoBoundProxyForURL 为真——autoBindAccountProxy 已把 DB 绑定推进
	// 到 orig 并同步了驻留槽，而本次重读的 fresh 恰是同步窗口前的陈旧
	// 快照），重读一次 DB 最新事实复核：新快照与 orig 一致则绝不误杀
	// 合法冷建。管理员手工 PATCH 会清除自动绑定来源记录，复核绝不命中，
	// fail-closed 拦截不变（步骤 6 不对齐 + 本处不重读放行）。
	if origAccountProxyID != fresh.ProxyID &&
		isAutoBoundProxyForURL(fresh.ID, origAccountProxyID, ac.proxyURL) {
		if re := accountByID(fresh.ID); re != nil && re.ProxyID == origAccountProxyID {
			fresh = re
		}
	}
	freshBoundURL := proxyURLByID(fresh.ProxyID)
	if fresh.ProxyID > 0 && proxyEnabledByID(fresh.ProxyID) &&
		freshBoundURL != ac.proxyURL &&
		(rtCfg().StrictProxyBinding || fresh.ProxyID != origAccountProxyID) {
		return browserErr("account #%d bound exit #%d (%q) != slot %q",
			fresh.ID, fresh.ProxyID, redactProxyURL(freshBoundURL), redactProxyURL(ac.proxyURL))
	}
	// (d) 槽位 boundProxyID 已失效或 URL 漂移。
	if boundProxyID > 0 &&
		(!proxyEnabledByID(boundProxyID) || proxyURLByID(boundProxyID) != ac.proxyURL) {
		return browserErr("slot bound proxy #%d invalid for account #%d",
			boundProxyID, fresh.ID)
	}
	return nil
}

// seedContextCookies 把账号 Cookie 播进指定 BrowserContext（Storage.setCookies）。
// 整批失败且错误含 "Invalid cookie" / -32602 时逐条降级重试；最终核对
// SAPISID + __Secure-1PSID 已写入，否则 fail-closed。
func (m *browserManager) seedContextCookies(ctx context.Context, client *cdpClient,
	bcID string, accountID int64, cookie string) error {
	params := cookieToCDP(cookie)
	if len(params) == 0 {
		return browserErr("account #%d cookie empty after parse", accountID)
	}
	payload := map[string]interface{}{
		"browserContextId": bcID,
		"cookies":          params,
	}
	err := client.Call(ctx, "", "Storage.setCookies", payload, nil)
	if err != nil && (strings.Contains(err.Error(), "Invalid cookie") ||
		strings.Contains(err.Error(), "-32602")) {
		logf("[browser] account #%d batch setCookies rejected (%v), falling back per-item", accountID, err)
		return m.seedCookiesFallbackPerItem(ctx, client, bcID, accountID, params)
	}
	if err != nil {
		return browserErr("Storage.setCookies: %w", err)
	}
	return nil
}

// seedCookiesFallbackPerItem 逐条 setCookies 重试并核对 SAPISID/1PSID 已写入。
func (m *browserManager) seedCookiesFallbackPerItem(ctx context.Context, client *cdpClient,
	bcID string, accountID int64, params []map[string]interface{}) error {
	written := map[string]bool{}
	for _, c := range params {
		name, _ := c["name"].(string)
		err := client.Call(ctx, "", "Storage.setCookies",
			map[string]interface{}{"browserContextId": bcID, "cookies": []map[string]interface{}{c}}, nil)
		if err == nil {
			written[name] = true
			continue
		}
		if !strings.Contains(err.Error(), "Invalid cookie") && !strings.Contains(err.Error(), "-32602") {
			return browserErr("Storage.setCookies per-item %q: %w", name, err)
		}
		logf("[browser] account #%d cookie %q rejected during per-item seed", accountID, name)
	}
	if !written["SAPISID"] || !written["__Secure-1PSID"] {
		return browserErr("account #%d seed missing SAPISID/__Secure-1PSID after per-item retry", accountID)
	}
	return nil
}

// cookieToCDP 把 "k=v; k=v" 串转成 Storage.setCookies 的 cookieParam 数组。
// __Host-* / OSID / __Secure-OSID（host-only 敏感项）走 url:
// "https://gemini.google.com/"（不设 domain）；其余走 domain: ".google.com"。
//
// R12-6（Oracle M9/Reviewer #2）会话身份主键代际一致：SAPISID /
// __Secure-1PSID 的重名多项按与 cookieIdentity / cookieSubset 完全一致
// 的「首个非空值」口径去重后**只播种该有效项**——同一 name/domain/path
// 位置上浏览器 Jar 末写覆盖首写，全量播种会让槽公布的身份（首项）与
// Jar 实际身份（末项）分裂：授权/缓存按首项裁决，页内会话与热注入却
// 落在末项上。普通重名业务键保序不变（非身份键不涉及身份裁决）。
func cookieToCDP(cookie string) []map[string]interface{} {
	var out []map[string]interface{}
	seenIdentity := map[string]bool{}
	for _, kv := range splitCookiePairs(cookie) {
		name, val := kv[0], kv[1]
		if name == "" {
			continue
		}
		if name == "SAPISID" || name == "__Secure-1PSID" {
			if seenIdentity[name] {
				continue // 重名身份主键：只播种首个非空值代表项，其余丢弃
			}
			// 首个非空值口径：前导空值项不代表会话身份（cookieSubset 换发
			// 也不会把它带出去），跳过；第一个非空项作为唯一身份代表播种。
			if val == "" {
				continue
			}
			seenIdentity[name] = true
		}
		isHostOnly := strings.HasPrefix(name, "__Host-") ||
			name == "OSID" || name == "__Secure-OSID"
		if isHostOnly {
			out = append(out, map[string]interface{}{
				"name":     name,
				"value":    val,
				"url":      "https://gemini.google.com/",
				"path":     "/",
				"secure":   true,
				"httpOnly": strings.HasPrefix(name, "__Secure-") || strings.HasPrefix(name, "__Host-"),
			})
		} else {
			out = append(out, map[string]interface{}{
				"name":     name,
				"value":    val,
				"domain":   ".google.com",
				"path":     "/",
				"secure":   true,
				"httpOnly": strings.HasPrefix(name, "__Secure-"),
			})
		}
	}
	return out
}

// cookieBelongsToGeminiJar 判定一条回读 Cookie 是否属于本池 gemini jar：
//   - path != "" && path != "/" → false（非根路径不混入）；
//   - rawDomain == "" → true（测试桩兼容）；
//   - host-only（无前导点）：仅 gemini.google.com 放行（严拒 google.com /
//     accounts.google.com 等 host-only 项）；
//   - 域 Cookie（有前导点）：去点后 cookieDomainMatches("gemini.google.com",
//     d)（仅 .google.com 与 .gemini.google.com，严拒 .googleusercontent.com /
//     .gstatic.com）。
func cookieBelongsToGeminiJar(rawDomain, path string) bool {
	if path != "" && path != "/" {
		return false
	}
	d := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(rawDomain)), ".")
	if d == "" {
		return true
	}
	if !strings.HasPrefix(d, ".") {
		return d == "gemini.google.com"
	}
	return cookieDomainMatches("gemini.google.com", strings.TrimPrefix(d, "."))
}

// cdpJarCookie 是 Storage.getCookies 返回的单条 Cookie 结构。
type cdpJarCookie struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Domain string `json:"domain"`
	Path   string `json:"path"`
}

// sortCDPJarCookies 回写合并排序：域 Cookie（.google.com）在前、
// gemini.google.com host-only 在后——同名 Cookie（如 OSID）恒以
// gemini.google.com 权威值覆盖 .google.com 旧值。
func sortCDPJarCookies(cookies []cdpJarCookie) {
	for i := 1; i < len(cookies); i++ {
		for j := i; j > 0; j-- {
			if domainRank(cookies[j-1].Domain) > domainRank(cookies[j].Domain) {
				cookies[j-1], cookies[j] = cookies[j], cookies[j-1]
			} else {
				break
			}
		}
	}
}

func domainRank(d string) int {
	if strings.HasPrefix(d, ".") {
		return 0 // 域 Cookie 在前
	}
	return 1 // host-only 在后
}

// syncAccountCookiesBackCtx 把指定 BrowserContext 的 jar 写回 DB（CAS +
// 代际拦截）。acEpoch 是槽位代际：小于 maxSeededEpoch 时直接丢弃（新代际
// 已接管），杜绝旧槽滞后回写覆盖新令牌。
// R11-5（Oracle A6）：expectedAuthuser 是建槽时该行的账号序号快照——SQL
// CAS 追加 `AND authuser = ?` 谓词：长流在飞/停机写回期间管理员只改序号
// （不拆槽）时，/u/0 槽派生的票据 0 行命中，绝不写进已变为 /u/1 的行。
func (m *browserManager) syncAccountCookiesBackCtx(ctx context.Context, client *cdpClient,
	bcID string, accountID int64, acEpoch uint64, expectedIdent string, expectedAuthuser int) {
	if client == nil || bcID == "" || accountID <= 0 {
		return
	}
	m.mu.Lock()
	if m.isSupersededEpochLocked(accountID, acEpoch) {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	unlock, err := acquireAcctCookieLock(ctx, accountID)
	if err != nil {
		return
	}
	defer unlock()

	m.mu.Lock()
	superseded := m.isSupersededEpochLocked(accountID, acEpoch)
	m.mu.Unlock()
	if superseded {
		return
	}
	cur := accountByID(accountID)
	if cur == nil || cur.Status != "enabled" ||
		(expectedIdent != "" && cookieIdentity(cur.Cookie) != expectedIdent) {
		return
	}

	var res struct {
		Cookies []cdpJarCookie `json:"cookies"`
	}
	if err := client.Call(ctx, "", "Storage.getCookies",
		map[string]interface{}{"browserContextId": bcID}, &res); err != nil {
		return
	}
	var filtered []cdpJarCookie
	for _, c := range res.Cookies {
		if cookieBelongsToGeminiJar(c.Domain, c.Path) {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		return
	}
	sortCDPJarCookies(filtered)
	var lines []string
	for _, c := range filtered {
		lines = append(lines, c.Name+"="+c.Value)
	}
	merged := mergeSetCookie(cur.Cookie, lines)
	if cookieIdentity(merged) != cookieIdentity(cur.Cookie) {
		logf("[browser] account #%d jar identity changed, skip write-back", accountID)
		return
	}
	// R9-7（Reviewer SF-x）：jar 逐名键值与库中完全等值时保留库中原串、
	// 绝不写库——mergeSetCookie 一律以 "; " 规范化重拼，非规范分隔符裸串
	// 在零键值变化下也会与库中原串字符串不等，仅凭 merged != cur 判定
	// 会把格式化差异写成一次无谓 UPDATE（连带 BumpAccountEpoch 与
	// InvalidateAccount 风险）。真实新增/刷新票据（逐名值确实变化）照常
	// 回收写库。
	if cookieValuesEqual(merged, cur.Cookie) {
		return
	}
	// R11-5：SQL CAS 带 authuser 谓词——持锁等待/上游 I/O 期间行序号被
	// 管理员改变时 0 行命中，旧序号槽的票据绝不落进新序号行。
	updateAccountCookieCASLockedAuthuser(accountID, cur.Cookie, merged, expectedAuthuser)
}

// SyncAccountCookiesBack 是后台强制写回入口（rotateAllAccounts 对活跃槽调用）。
// force=true 绕过 cookieSyncThrottle。
func (m *browserManager) SyncAccountCookiesBack(accountID int64, force bool) {
	m.mu.Lock()
	ac := m.ctxs[browserSlotKey{accountID: accountID}]
	if ac == nil || ac.disposed || ac.browserContextID == "" {
		m.mu.Unlock()
		return
	}
	if !force && time.Since(ac.lastCookieSync) < cookieSyncThrottle {
		m.mu.Unlock()
		return
	}
	ac.lastCookieSync = time.Now()
	client, bcID, epoch, ident, au := m.client, ac.browserContextID, ac.epoch, ac.cookieIdentity, ac.authuser
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m.syncAccountCookiesBackCtx(ctx, client, bcID, accountID, epoch, ident, au)
}

// cookieSyncTarget 是一条停机/强制写回目标的锁内快照（全部字段在
// m.mu 内核出，锁外零读取 ac.*）。
type cookieSyncTarget struct {
	bcID     string
	accID    int64
	epoch    uint64
	ident    string
	authuser int // R11-5：建槽时行序号快照，回写 CAS 谓词
}

// syncCookieTargetsBack 对给定目标列表执行共享预算 Cookie 写回
// （锁外调用）。MF-7：所有目标共用同一个绝对截止（封顶 800ms 单一
// 总预算），绝不给每条各发一份 800ms 导致停机写回拖长。
func (m *browserManager) syncCookieTargetsBack(ctx context.Context,
	client *cdpClient, targets []cookieSyncTarget) {
	if len(targets) == 0 {
		return
	}
	budget := 800 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		if rem := time.Until(deadline); rem < budget {
			budget = rem
		}
	}
	if budget <= 0 {
		return
	}
	deadline := time.Now().Add(budget)
	for _, tg := range targets {
		rem := time.Until(deadline)
		if rem <= 0 {
			break
		}
		cctx, cancel := context.WithDeadline(ctx, deadline)
		m.syncAccountCookiesBackCtx(cctx, client, tg.bcID, tg.accID, tg.epoch, tg.ident, tg.authuser)
		cancel()
	}
}

// InvalidateAccountIfBC 是 InvalidateAccount 的槽实例条件版（R4-2）：
// 仅当当前驻留槽存在、未摘除且 browserContextID 精确等于 bcID 时才摘除——
// 延迟的失效动作（如热注入失败后的清理）在 I/O 期间槽位可能已被替换为
// 健康新槽，按 ID 无条件摘除会误拆新槽；bcID 不符时本调用是无操作。
func (m *browserManager) InvalidateAccountIfBC(accountID int64, bcID string, reason string) {
	m.mu.Lock()
	ac := m.ctxs[browserSlotKey{accountID: accountID}]
	if ac == nil || ac.disposed || ac.browserContextID != bcID {
		m.mu.Unlock()
		return
	}
	snap := m.detachSnapLocked(ac,
		browserErr("account #%d invalidated: %s", accountID, reason))
	client := m.client
	m.mu.Unlock()
	if snap.ac == nil {
		return
	}
	m.failStreamsForCtx(snap.ac, snap.err)
	if snap.wasCreating {
		return
	}
	m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, false, client)
}

// InvalidateAccount 精准失效指定账号的登录槽（deleted/disabled/proxy 变更 /
// cookie rotated）。callerSync=true 且已发布槽（syncCookie）时先写回 Cookie；
// 构建中槽位（wasCreating）只 failStreams，局部资源归 buildAccountCtx。
func (m *browserManager) InvalidateAccount(accountID int64, callerSync bool, reason string) {
	m.mu.Lock()
	snap := m.detachSnapLocked(m.ctxs[browserSlotKey{accountID: accountID}],
		browserErr("account #%d invalidated: %s", accountID, reason))
	client := m.client
	m.mu.Unlock()
	if snap.ac == nil {
		return
	}
	m.failStreamsForCtx(snap.ac, snap.err)
	// 构建中槽位的局部资源归 buildAccountCtx owner——外部摘除方只
	// failStreams，绝不 dispose（NH-2）；callerSync 只在已发布槽生效。
	if snap.wasCreating {
		return
	}
	m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, callerSync && snap.syncCookie, client)
}

// InvalidateByProxy 精准失效绑定 proxyID（或 URL 匹配 oldURL）的全部槽位
// （含登录槽、溢出槽与匿名复合键槽）。proxyUpdate/proxyDelete 在删除、URL
// 变更或禁用时调用。
func (m *browserManager) InvalidateByProxy(proxyID int64, oldURL string, reason string) {
	m.mu.Lock()
	var victims []detachedCtxSnap
	for _, ac := range m.ctxs {
		if ac.disposed {
			continue
		}
		if ac.boundProxyID == proxyID || (oldURL != "" && ac.proxyURL == oldURL) {
			if snap := m.detachSnapLocked(ac, browserErr("proxy #%d invalidated: %s", proxyID, reason)); snap.ac != nil {
				victims = append(victims, snap)
			}
		}
	}
	client := m.client
	m.mu.Unlock()
	for _, snap := range victims {
		m.failStreamsForCtx(snap.ac, snap.err)
		if !snap.wasCreating {
			m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, snap.syncCookie, client)
		}
	}
}

// recreateAccountTab 单槽崩溃自愈：在 m.mu 下核对指针与代际后，在原
// browserContextID 内重建空白页（createTarget→attach→enablePage→
// installBridge→navigate），失败仅摘除本槽（绝不误杀整机/兄弟槽）。
func (m *browserManager) recreateAccountTab(c *cdpClient, ac *accountBrowserCtx, gen uint64) {
	m.mu.Lock()
	if m.client != c || m.generation != gen || m.stopping || m.shuttingDown ||
		ac.disposed || ac.creating || ac.recreating || ac.closing ||
		m.ctxs[ac.key] != ac {
		m.mu.Unlock()
		return
	}
	// R4-2（Reviewer MF-2）：进入重建前旧 Jar 若已被轮转 Bump 取代，严禁
	// 重建——重建复用同一个 BrowserContext（不重新播种），给它任何新发布
	// 都会让 streamReadCloser.Close 的后台回写借双代际检查把旧短票反写
	// 进 DB。直接走既有摘除销毁路径（与发布后守卫同一终态语义）。
	if ac.key.accountID > 0 && m.isSupersededEpochLocked(ac.key.accountID, ac.epoch) {
		snap := m.detachSnapLocked(ac, seedSupersededErr("recreate entry"))
		m.mu.Unlock()
		if snap.ac == nil {
			return
		}
		m.failStreamsForCtx(snap.ac, snap.err)
		if snap.wasCreating {
			return
		}
		m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, snap.syncCookie, c)
		return
	}
	// R4-2：重建不重新播种，播种代际绝不能抬高超过 maxSeededEpoch——保持
	// ac.epoch 原值（snapEpoch 仅作本代际一致性快查），这样重建期间或之前
	// 的任何轮转 Bump 都能被发布守卫的 isSupersededEpochLocked 100% 拦截，
	// 已失效 Jar 永远拿不到能穿透双代际回写检查的更大 epoch。
	snapEpoch := ac.epoch
	ac.recreating = true
	if ac.sessionID != "" {
		delete(m.sessionIndex, ac.sessionID)
	}
	if ac.targetID != "" {
		delete(m.targetIndex, ac.targetID)
	}
	oldTarget := ac.targetID
	ac.sessionID = ""
	ac.targetID = ""
	buildCtx, buildCancel := context.WithTimeout(context.Background(), browserCtxBuildTimeout)
	ac.buildCancel = buildCancel
	ac.resetReadyLocked()
	bcID := ac.browserContextID
	m.mu.Unlock()

	m.failStreamsForCtx(ac, browserErr("tab crashed for slot %v; stream aborted", ac.key))

	sessID, targetID, recreateErr := m.doRecreateTab(buildCtx, c, bcID, oldTarget, ac.key.accountID)

	m.mu.Lock()
	// R3-1 发布守卫：recreating 槽重建期间若已被热轮转 Bump（prepareHotInject
	// 对 recreating 分类推进 maxSeededEpoch），本槽 BrowserContext 里装的是
	// 轮转前的旧票据——严禁发布：旧 Jar 上下文必须走单胜出者销毁，否则
	// syncAccountCookiesBackCtx 因 epoch<maxSeededEpoch 永久拒绝回写，形成
	// 「存活旧 Jar + 新 DB + SIDCC 回写黑洞」。本检查与 Bump 同受 m.mu 串行化。
	superseded := ac.key.accountID > 0 && m.isSupersededEpochLocked(ac.key.accountID, snapEpoch)
	ok := recreateErr == nil && m.client == c && m.generation == gen &&
		!m.stopping && !m.shuttingDown && !ac.disposed && !superseded &&
		ac.epoch == snapEpoch && m.ctxs[ac.key] == ac
	if ok {
		ac.recreating = false
		if ac.buildCancel != nil {
			ac.buildCancel()
			ac.buildCancel = nil
		}
		ac.targetID = targetID
		ac.sessionID = sessID
		ac.lastUsedAt = time.Now()
		m.sessionIndex[sessID] = ac
		m.targetIndex[targetID] = ac
		ac.closeReadyLocked()
		m.mu.Unlock()
		return
	}
	dead := c.Closed()
	m.mu.Unlock()
	if dead {
		m.handleDeadLocked(c, gen)
		return
	}
	// client 未断连（如单账号代理断流）：仅摘除本槽，兄弟槽零影响。
	// 单胜出者守卫（Oracle MF-8.4）：InvalidateAccount/stopImpl 可能已抢先
	// 摘除 ac——snap.ac==nil 时绝不再 failStreams+disposeDetachedCtx（否则
	// 二次 dispose 且绕过摘除方的 syncCookie=false）。
	// R3-1：superseded 时绝不能让终态 cause 为 nil——recreateErr 为空的
	// 纯代际命中必须落显式非 nil 错误（本地代际终止，不扣代理分）。
	cause := recreateErr
	if cause == nil && superseded {
		cause = seedSupersededErr("recreate")
	}
	m.mu.Lock()
	var snap detachedCtxSnap
	if !ac.disposed && m.ctxs[ac.key] == ac {
		snap = m.detachSnapLocked(ac, cause)
	}
	m.mu.Unlock()
	if snap.ac == nil {
		return
	}
	m.failStreamsForCtx(snap.ac, snap.err)
	if snap.wasCreating {
		return
	}
	m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, snap.syncCookie, c)
}

// doRecreateTab 执行重建的 CDP 序列（锁外，buildCtx 可被取消打断）。
// accountID>0 时只读查库取当前 Cookie 传入首航跨源判定（含 MissingOSID）。
// 返回新 (sessionID, targetID, err)。
func (m *browserManager) doRecreateTab(buildCtx context.Context, c *cdpClient,
	bcID, oldTarget string, accountID int64) (string, string, error) {
	if oldTarget != "" {
		cctx, cancel := context.WithTimeout(buildCtx, 5*time.Second)
		_ = c.Call(cctx, "", "Target.closeTarget",
			map[string]interface{}{"targetId": oldTarget}, nil)
		cancel()
	}
	var created struct {
		TargetID string `json:"targetId"`
	}
	if err := c.Call(buildCtx, "", "Target.createTarget",
		map[string]interface{}{"url": "about:blank", "browserContextId": bcID},
		&created); err != nil {
		return "", "", browserErr("recreate createTarget: %w", err)
	}
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := c.Call(buildCtx, "", "Target.attachToTarget",
		map[string]interface{}{"targetId": created.TargetID, "flatten": true},
		&attached); err != nil {
		return "", "", browserErr("recreate attach: %w", err)
	}
	if err := m.enablePageDomainFor(buildCtx, c, attached.SessionID); err != nil {
		return "", "", err
	}
	if err := m.installBridgeFor(buildCtx, c, attached.SessionID); err != nil {
		return "", "", err
	}
	cookie := ""
	if accountID > 0 {
		if cur := accountByID(accountID); cur != nil {
			cookie = cur.Cookie
		}
	}
	if err := m.navigateAndWaitAppCookie(buildCtx, c, attached.SessionID, cookie); err != nil {
		return "", "", classifyNavBuildErr(buildCtx, c, "recreate navigate /app", err)
	}
	return attached.SessionID, created.TargetID, nil
}

// auxSlotReady 是辅助入口（allowSwitch=false）的锁内前置判定：浏览器已
// 启动且对应复合键槽位已存在、已就绪（非 creating/recreating/disposed/
// closing、sessionID 非空、drainWaiters==0）且出口匹配。任一不满足返回
// errBrowserSkipAux（绝不冷启动 Chromium/Context）。
func (m *browserManager) auxSlotReady(account *CookieAccount, proxyURL string, allowDirect bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started || m.stopping || m.shuttingDown || m.cleaning ||
		m.client == nil || m.client.Closed() {
		return errBrowserSkipAux
	}
	ac := m.ctxs[makeBrowserSlotKey(account, proxyURL, allowDirect)]
	if ac == nil || ac.disposed || ac.creating || ac.recreating || ac.closing ||
		ac.sessionID == "" || ac.drainWaiters > 0 {
		return errBrowserSkipAux
	}
	if ac.proxyURL != proxyURL || ac.allowDirect != allowDirect {
		return errBrowserSkipAux
	}
	// 登录槽身份复核：jar 身份必须与请求点名账号一致（异身份不借）。
	if account != nil && account.ID > 0 && ac.cookieIdentity != "" &&
		cookieIdentity(account.Cookie) != ac.cookieIdentity {
		return errBrowserSkipAux
	}
	return nil
}

// ─── 每账号 Cookie 串行锁（L1，先于 m.mu L2，绝不在持 m.mu 时获取）────────

var (
	acctCookieLocksMu sync.Mutex
	acctCookieSems    = map[int64]chan struct{}{}
)

// acquireAcctCookieLock 获取账号 id 的 Cookie 串行锁（可取消）。
// 返回幂等 unlock；id<=0 时返回空 unlock。覆盖范围：rotateAccountOpts、
// buildAccountCtx 读库播种快照、syncAccountCookiesBackCtx 合并落库、
// 纯 Go HTTP 流 updateAccountCookie——四者对同一 accountID 严格互斥。
func acquireAcctCookieLock(ctx context.Context, id int64) (func(), error) {
	if id <= 0 {
		return func() {}, nil
	}
	acctCookieLocksMu.Lock()
	sem := acctCookieSems[id]
	if sem == nil {
		sem = make(chan struct{}, 1)
		sem <- struct{}{}
		acctCookieSems[id] = sem
	}
	acctCookieLocksMu.Unlock()
	select {
	case <-sem:
		var once sync.Once
		return func() { once.Do(func() { sem <- struct{}{} }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// updateAccountCookieCASLocked 是持 acctCookieLock 内的无锁 CAS 原语：
// merged == expectedOld 直接返回 true 不发无意义 UPDATE；变化时执行
// `UPDATE accounts SET cookie=? WHERE id=? AND status='enabled' AND cookie=?`
// 条件更新，仅 RowsAffected>0 才算真实写库成功（已禁用/删除/被新代际改写
// 时 0 行受影响，绝不静默倒退令牌）。调用方必须已持 acctCookieLock。
func updateAccountCookieCASLocked(id int64, expectedOld, merged string) bool {
	return updateAccountCookieCASLockedAuthuser(id, expectedOld, merged, -1)
}

// updateAccountCookieCASLockedAuthuser 是 updateAccountCookieCASLocked 的
// authuser 谓词版（R5-3，Reviewer MF-3）：expectedAuthuser >= 0 时 SQL CAS
// 条件追加 `AND authuser = ?`——持锁等待期间管理员 PATCH 改了 authuser
// 的行 0 行命中，绝不把 /u/0 的票据写进已变为 /u/1 的账号；< 0 时与
// 原行为完全一致（不校验 authuser）。调用方必须已持 acctCookieLock。
func updateAccountCookieCASLockedAuthuser(id int64, expectedOld, merged string, expectedAuthuser int) bool {
	if id <= 0 || merged == "" {
		return false
	}
	if merged == expectedOld {
		return true
	}
	if cookieIdentity(merged) != cookieIdentity(expectedOld) {
		logf("[cookie] 账号 #%d 的刷新结果身份对不上，已丢弃不写回", id)
		return false
	}
	var res sql.Result
	var err error
	if expectedAuthuser >= 0 {
		res, err = getDB().Exec(
			`UPDATE accounts SET cookie=? WHERE id=? AND status='enabled' AND cookie=? AND authuser=?`,
			merged, id, expectedOld, expectedAuthuser)
	} else {
		res, err = getDB().Exec(
			`UPDATE accounts SET cookie=? WHERE id=? AND status='enabled' AND cookie=?`,
			merged, id, expectedOld)
	}
	if err != nil {
		return false
	}
	n, err := res.RowsAffected()
	return err == nil && n > 0
}
