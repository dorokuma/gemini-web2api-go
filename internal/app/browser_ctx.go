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
	"errors"
	"fmt"
	"strings"
	"sync"
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
		m.syncAccountCookiesBackCtx(ctx, client, bcID, ac.key.accountID, ac.epoch, ac.cookieIdentity)
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
// 调用方不持 m.mu（L1 acctCookieLock 内可调）。
func (m *browserManager) BumpAccountEpoch(accountID int64) {
	m.mu.Lock()
	m.nextCtxEpoch++
	m.maxSeededEpoch[accountID] = m.nextCtxEpoch
	m.mu.Unlock()
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
			ac.inFlight++
			ac.lastUsedAt = time.Now()
			client := m.client
			sessID := ac.sessionID
			m.mu.Unlock()
			return client, ac, sessID, m.acquireReleaseFn(ac), nil
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
		// waitDrain：因换出口（errExitChanged）摘除时清自身登记后汇入
		// 新槽；因 Invalidate/停机摘除则返回终态错（v2.2 §3 / MF-8）。
		if kind == waitDrain && errors.Is(ac.lastBuildErr, errExitChanged) {
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
	m.nextCtxEpoch++
	nac.epoch = m.nextCtxEpoch
	buildCtx, buildCancel := context.WithCancel(context.Background())
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
	if ac.cookieIdentity != "" && account != nil &&
		cookieIdentity(account.Cookie) != ac.cookieIdentity {
		if snap := m.detachSnapLocked(ac, browserErr("account #%d identity changed", ac.key.accountID)); snap.ac != nil {
			return nil, waitNone, false, snap
		}
		return nil, waitNone, false, snap
	}

	// R3-M1 强绑定复核：既有就绪槽位在占租约前用锁外预计算的代理快照
	// + freshAcct 做纯内存比对，命中任一违规立即摘除重建
	//（Oracle MF-5 / Reviewer #4）。
	if account != nil {
		if ac.readySlotExitViolation(account, poolBlocksDirect, strictBinding, poolSnap) {
			if snap := m.detachSnapLocked(ac, browserErr(
				"account #%d exit binding drifted", ac.key.accountID)); snap.ac != nil {
				return nil, waitNone, false, snap
			}
			return nil, waitNone, false, snap
		}
	}

	sameExit := ac.proxyURL == proxyURL && ac.allowDirect == allowDirect
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

	// 同登录槽非严格模式合法换出口：inFlight==0 时唯一替换 winner。
	if ac.inFlight == 0 {
		if drain.registered && drain.target == ac {
			drain.leave()
		}
		if snap := m.detachSnapLocked(ac, errExitChanged); snap.ac != nil {
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
// 比对，全部代理事实由调用方锁外预计算的 poolSnap + freshAcct 提供）。
// 返回 true 表示该槽位出口已违反绑定规则，应立即摘除重建。规则：
//   - (a) 账号已锁直连（proxyIDDirect）但槽位不是显式直连；
//   - (b) 池禁直连时槽位直连但账号并非锁定直连（-1 -> 0 反向防泄漏）；
//   - (c) 严格模式或「原绑定已变」时账号新绑定代理启用且其 URL 与槽位
//     出口漂移（Reviewer #4 与 exitCompatCheck 同口径）；
//   - (d) 槽位 boundProxyID 已删除/禁用或与槽位出口 URL 漂移。
func (ac *accountBrowserCtx) readySlotExitViolation(account *CookieAccount,
	poolBlocksDirect, strictBinding bool, poolSnap map[int64]proxySnapEntry) bool {
	// (a) 账号锁直连：槽位必须是显式直连。
	if account.ProxyID == proxyIDDirect && (ac.proxyURL != "" || !ac.allowDirect) {
		return true
	}
	// (b) 池禁直连：槽位直连但账号不是锁直连 → -1->0 反向泄漏。
	if ac.proxyURL == "" && poolBlocksDirect && account.ProxyID != proxyIDDirect {
		return true
	}
	// (c) 与 exitCompatCheck 同口径：严格模式或账号绑定已变（orig 漂移）
	// 时，当前绑定代理启用且 URL 与槽位出口漂移 → 改绑后的旧槽必须摘除
	// 重建，绝不复用陈旧出口（非严格模式下管理员改绑同样生效）。
	if account.ProxyID > 0 &&
		(strictBinding || account.ProxyID != ac.origAccountProxyID) {
		if ent, ok := poolSnap[account.ProxyID]; ok && ent.enabled &&
			ent.url != ac.proxyURL {
			return true
		}
	}
	// (d) 槽位 boundProxyID 已删除/禁用或 URL 漂移。
	if ac.boundProxyID > 0 {
		ent, ok := poolSnap[ac.boundProxyID]
		if !ok || !ent.enabled || ent.url != ac.proxyURL {
			return true
		}
	}
	return false
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
		ok := buildErr == nil && m.client == client && m.generation == gen &&
			!m.stopping && !m.shuttingDown && !ac.disposed &&
			ac.epoch == snapEpoch && m.ctxs[ac.key] == ac
		if ok {
			if stopKill != nil {
				stopKill() // 先解除 AfterFunc 断桥守卫再发布
			}
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
		if !ac.disposed && ac.epoch == snapEpoch && m.ctxs[ac.key] == ac {
			m.detachCtxLocked(ac, buildErr)
		}
		syncCookie := ac.syncOnDispose
		bcID, bridge := localBCID, localBridge
		m.mu.Unlock()
		m.failStreamsForCtx(ac, buildErr)
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
	// 失败路径的 defer 兜底：发布分支已在锁内先 stopKill()，这里仅兜底
	// 非发布路径（幂等，二次调用无副作用）。
	defer func() {
		if stopKill != nil {
			stopKill()
		}
	}()

	// 步骤 2：独立 BrowserContext（disposeOnDetach 连带关其下 target）。
	var bcRes struct {
		BrowserContextID string `json:"browserContextId"`
	}
	if err := client.Call(buildCtx, "", "Target.createBrowserContext",
		map[string]interface{}{
			"proxyServer":     fmt.Sprintf("http://127.0.0.1:%d", bridge.Port()),
			"disposeOnDetach": true,
		}, &bcRes); err != nil {
		buildErr = browserErr("createBrowserContext: %w", err)
		return
	}
	localBCID = bcRes.BrowserContextID

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
			buildErr = err
			return
		}
		fresh := accountByID(account.ID)
		if fresh == nil || fresh.Status != "enabled" {
			unlockCookie()
			buildErr = browserErr("account #%d unavailable during seed", account.ID)
			return
		}
		freshIdent = cookieIdentity(fresh.Cookie)
		if cerr := exitCompatCheck(ac, fresh); cerr != nil {
			unlockCookie()
			buildErr = cerr
			return
		}
		m.mu.Lock()
		if snapEpoch > m.maxSeededEpoch[account.ID] {
			m.maxSeededEpoch[account.ID] = snapEpoch
		}
		m.mu.Unlock()
		if err := m.seedContextCookies(buildCtx, client, localBCID, fresh.ID, fresh.Cookie); err != nil {
			unlockCookie()
			buildErr = err
			return
		}
		unlockCookie()
	}

	// 步骤 7：首航 /app。非断连的导航失败/超时统一包装 pageFetchErr
	//（C5a 网络/代理故障），仅断连/停机才报 browserErr。
	if err := m.navigateAndWaitApp(buildCtx, client, sessID); err != nil {
		if client != nil && !client.Closed() && buildCtx.Err() == nil {
			buildErr = pageFetchErr(fmt.Sprintf("navigate /app: %v", err))
		} else {
			buildErr = err
		}
		return
	}
}

// exitCompatCheck 是 R3-M1 4 条出口兼容核验（播种前调用，此时持
// acctCookieLock 不持 m.mu）。ac 字段 origAccountProxyID/boundProxyID/
// proxyURL/allowDirect 均为进 m.mu 前的预计算快照。
func exitCompatCheck(ac *accountBrowserCtx, fresh *CookieAccount) error {
	// (a) 账号锁定直连（-1）但槽位不是显式直连：防穿透改绑窗口。
	if fresh.ProxyID == proxyIDDirect && (ac.proxyURL != "" || !ac.allowDirect) {
		return browserErr("account #%d locked direct but slot exit=%q allowDirect=%v",
			fresh.ID, ac.proxyURL, ac.allowDirect)
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
	freshBoundURL := proxyURLByID(fresh.ProxyID)
	if fresh.ProxyID > 0 && proxyEnabledByID(fresh.ProxyID) &&
		freshBoundURL != ac.proxyURL &&
		(rtCfg().StrictProxyBinding || fresh.ProxyID != ac.origAccountProxyID) {
		return browserErr("account #%d bound exit #%d (%q) != slot %q",
			fresh.ID, fresh.ProxyID, freshBoundURL, ac.proxyURL)
	}
	// (d) 槽位 boundProxyID 已失效或 URL 漂移。
	if ac.boundProxyID > 0 &&
		(!proxyEnabledByID(ac.boundProxyID) || proxyURLByID(ac.boundProxyID) != ac.proxyURL) {
		return browserErr("slot bound proxy #%d invalid for account #%d",
			ac.boundProxyID, fresh.ID)
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
func cookieToCDP(cookie string) []map[string]interface{} {
	var out []map[string]interface{}
	for _, kv := range splitCookiePairs(cookie) {
		name, val := kv[0], kv[1]
		if name == "" {
			continue
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
func (m *browserManager) syncAccountCookiesBackCtx(ctx context.Context, client *cdpClient,
	bcID string, accountID int64, acEpoch uint64, expectedIdent string) {
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
	updateAccountCookieCASLocked(accountID, cur.Cookie, merged)
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
	client, bcID, epoch, ident := m.client, ac.browserContextID, ac.epoch, ac.cookieIdentity
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m.syncAccountCookiesBackCtx(ctx, client, bcID, accountID, epoch, ident)
}

// cookieSyncTarget 是一条停机/强制写回目标的锁内快照（全部字段在
// m.mu 内核出，锁外零读取 ac.*）。
type cookieSyncTarget struct {
	bcID  string
	accID int64
	epoch uint64
	ident string
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
		m.syncAccountCookiesBackCtx(cctx, client, tg.bcID, tg.accID, tg.epoch, tg.ident)
		cancel()
	}
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
	m.nextCtxEpoch++
	ac.epoch = m.nextCtxEpoch
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
	buildCtx, buildCancel := context.WithCancel(context.Background())
	ac.buildCancel = buildCancel
	ac.resetReadyLocked()
	bcID := ac.browserContextID
	m.mu.Unlock()

	m.failStreamsForCtx(ac, browserErr("tab crashed for slot %v; stream aborted", ac.key))

	sessID, targetID, recreateErr := m.doRecreateTab(buildCtx, c, bcID, oldTarget)

	m.mu.Lock()
	ok := recreateErr == nil && m.client == c && m.generation == gen &&
		!m.stopping && !m.shuttingDown && !ac.disposed &&
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
	m.mu.Lock()
	var snap detachedCtxSnap
	if !ac.disposed && m.ctxs[ac.key] == ac {
		snap = m.detachSnapLocked(ac, recreateErr)
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
// 返回新 (sessionID, targetID, err)。
func (m *browserManager) doRecreateTab(buildCtx context.Context, c *cdpClient,
	bcID, oldTarget string) (string, string, error) {
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
	if err := m.navigateAndWaitApp(buildCtx, c, attached.SessionID); err != nil {
		if c != nil && !c.Closed() && buildCtx.Err() == nil {
			return "", "", pageFetchErr(fmt.Sprintf("recreate navigate /app: %v", err))
		}
		return "", "", err
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
	res, err := getDB().Exec(
		`UPDATE accounts SET cookie=? WHERE id=? AND status='enabled' AND cookie=?`,
		merged, id, expectedOld)
	if err != nil {
		return false
	}
	n, err := res.RowsAffected()
	return err == nil && n > 0
}
