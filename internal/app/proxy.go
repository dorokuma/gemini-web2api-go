package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// proxyIDDirect（-1 锁定直连）三条核心不变量（分散实现易复发，统一在此固化）：
//  1. 库值优先于快照：任何 acct.ProxyID/conv.proxyID 快照都可能陈旧，
//     判出口归属必须先按 claimMu/核库读 accounts.proxy_id 当前值；
//  2. 自动写绑绝不覆写 -1：非管理员写绑一律走 autoBindAccountProxy，
//     它在 claimMu 锁内重查——库里 -1 / 已删 / proxyID<=0 / 已有有效绑定
//     都绝不写；管理员写绑走 bindAccountProxy；
//  3. 直连槽 0 的语义随池态变化：hasProxyPool() && !FallbackDirect 时
//     slots[0] 只归锁定直连号（cur==proxyIDDirect），其它账号/匿名借用
//     即泄漏本机 IP；池空或 fallback 开时按普通直连语义。
type Proxy struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	URL       string `json:"url"`
	Enabled   bool   `json:"enabled"`
	Weight    int    `json:"weight"`
	FailCount int    `json:"fail_count"`
	LastUsed  int64  `json:"last_used"`
	LastError string `json:"last_error"`
	CreatedAt int64  `json:"created_at"`
}

var (
	proxyMu     sync.RWMutex
	proxyCache  []Proxy
	proxyCursor uint64
)

// loadProxies 从 DB 刷新内存里的代理列表。
//
// 读一半失败时**保留上一次的池子**，绝不用半截结果覆盖。
// 旧写法有两个静默失效点：Scan 出错 continue（悄悄漏掉一个代理）、rows.Err()
// 完全不查（遍历中断当成正常读完）。两者都会让 proxyCache 变短甚至变空，而
// acquireSlot 用 len(proxyCache)==0 判断"没配代理池"，于是**池子一空就退回直连**
// —— 部署者的真实 IP 直接暴露给上游，日志上只看到偶发的直连请求。
//
// 这条路径每个请求都会走（recordProxyResult 结束就调），而 WAL 模式下并发
// UPDATE 期间 rows.Next() 完全可能返回 SQLITE_BUSY，所以"偶发"就是这么来的。
func loadProxies() {
	rows, err := getDB().Query(`SELECT id, name, url, enabled, weight, fail_count,
        COALESCE(last_used,0), COALESCE(last_error,''), created_at FROM proxies ORDER BY id`)
	if err != nil {
		logf("[proxy] 读取失败，保留上一次的代理池: %v", err)
		return
	}
	defer rows.Close()
	var list []Proxy
	for rows.Next() {
		var p Proxy
		var enabled int
		if err := rows.Scan(&p.ID, &p.Name, &p.URL, &enabled, &p.Weight, &p.FailCount,
			&p.LastUsed, &p.LastError, &p.CreatedAt); err != nil {
			logf("[proxy] 有行读不出来，保留上一次的代理池: %v", err)
			return
		}
		p.Enabled = enabled == 1
		list = append(list, p)
	}
	if err := rows.Err(); err != nil {
		logf("[proxy] 遍历中断，保留上一次的代理池: %v", err)
		return
	}
	proxyMu.Lock()
	proxyCache = list
	proxyMu.Unlock()
}

// 连续失败到这个次数就把代理熔断，等冷却期过了再放回池子。
const proxyFailThreshold = 5

// proxyUsable 判断这条代理现在能不能用。
//
// 熔断不是永久除名：被 Google 拦掉的出口实测 106-121 分钟就自动恢复，而旧写法
// 只认 fail_count<5，一旦超了就再也选不中，也就永远等不到一次成功把 fail_count
// 清零 —— 只能去面板手动重置。冷却期从 last_used（也就是最后一次尝试）算起，
// 放回去再失败一次就重新计时。
//
// cooldownMin<=0 表示关掉冷却，退回"熔断即永久除名"的旧行为。
func proxyUsable(p Proxy, now int64, cooldownMin int) bool {
	if !p.Enabled {
		return false
	}
	if p.FailCount < proxyFailThreshold {
		return true
	}
	if cooldownMin <= 0 {
		return false
	}
	return now-p.LastUsed >= int64(cooldownMin)*60
}

// kv 里记迁移/播种状态的键。
const (
	kvLegacyProxyDone = "legacy_static_proxy_migrated"
	kvSeededProxyID   = "seeded_proxy_id"
	kvSeededProxyURL  = "seeded_proxy_url"
)

// seedProxiesFromConfig 把启动参数和历史遗留的「静态代理」并进代理池。
//
// 以前代理有两个入口：代理池，和「设置」页那个单独的静态代理文本框（池空时才用）。
// 后者不是"简单版"而是**残废版** —— 它走的是 picked.ID=0 这条路，于是跟直连共用
// 同一个限流 slot、recordProxyResult 压根不会被调用，也就没有失败计数、没有熔断、
// 没有冷却、面板上看不到任何状态。池子里放一个，处处严格更好。
//
// 现在请求路径只认池子，这个函数负责把旧入口的值搬进来。
func seedProxiesFromConfig() error {
	migrateLegacyStaticProxy()
	return syncSeededProxy()
}

// migrateLegacyStaticProxy 一次性把 kv 里遗留的静态代理搬进池子。
//
// 两条保命规则，都是针对"用户的库已经在跑"这个前提：
//
//  1. **入池成功才标记完成**，失败就原样留着下次再试。旧版的
//     validateRuntimeConfig 对这个字段零校验，用户完全可能存的是 `1.2.3.4:8080`
//     这种缺 scheme 的值，而 proxyCreate 会拒收它 —— 先清值再入池的话，用户升级
//     后代理凭空消失，流量全转直连然后被上游拦。
//  2. 用**独立的迁移标记**，不去改写 runtime_config 那个 JSON。少动一次已有数据
//     就少一分把别的字段写坏的风险；顺带回滚到旧版时那条静态代理还在，行为不变。
func migrateLegacyStaticProxy() {
	if kvGet(kvLegacyProxyDone) == "1" {
		return
	}
	v := strings.TrimSpace(legacyStaticProxy())
	if v == "" {
		_ = kvSet(kvLegacyProxyDone, "1") // 本来就没有，标记掉免得每次启动都解析一遍
		return
	}
	if !poolHasProxyURL(v) {
		if _, err := proxyCreate("原静态代理", v, 1); err != nil {
			logf("[proxy] 「设置」页的静态代理迁入池子失败，原值保留、下次启动重试: %v", err)
			return
		}
		logf("[proxy] 「设置」页的静态代理已迁入代理池")
	}
	_ = kvSet(kvLegacyProxyDone, "1")
}

// legacyStaticProxy 只读地取 kv 里遗留的 runtime_config.proxy。
// RuntimeConfig 已经没有这个字段了，所以只能直接看原始 JSON。
func legacyStaticProxy() string {
	raw := kvGet(runtimeConfigKey)
	if raw == "" {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return ""
	}
	v, _ := m["proxy"].(string)
	return v
}

// syncSeededProxy 让池子里跟着 --proxy / config.json 走一条记录。
//
// 启动参数是**声明式**的：值变了就更新同一条，而不是再加一条。按 URL 去重的写法
// 挡不住这个 —— 用户把 compose 里的代理换掉，旧那条会留在池子里继续 enabled、
// 继续接流量，成了一条谁也不知道还在用的僵尸出口。
//
// 值没变时**完全不碰池子**：用户在面板上对这条记录的增删改停用，都以面板为准。
//
// R12-1（Oracle M4/Reviewer #1）fail-closed：非空的 --proxy 必须先通过严格
// 校验才能触碰池子——此前顺序相反（先 dropSeededProxy 撤掉旧播种行，再入池），
// 新值非法时旧行已删、新行入池失败仅记日志：显式要求代理的部署静默退化为
// 池空直连（或后台轮转拿到空 URL），把非法显式配置解释成用户选择直连。
// 修复后：非空且非法直接返回配置错误（旧播种行原样保留，绝不先删），由
// 启动入口 fail-closed 终止；DB 写入类的入池失败（非校验错误）沿用旧的
// 「记日志、下次重试」语义返回 nil。
func syncSeededProxy() error {
	url := strings.TrimSpace(cfg.Proxy)
	prev := kvGet(kvSeededProxyURL)
	if url == prev {
		return nil
	}
	if url != "" {
		if err := validateProxyURL(url); err != nil {
			return fmt.Errorf("--proxy 配置的代理 URL 非法（%v），拒绝启动（绝不静默退化直连）", err)
		}
	}
	dropSeededProxy(prev)
	if url == "" {
		_ = kvSet(kvSeededProxyURL, "")
		_ = kvSet(kvSeededProxyID, "")
		logf("[proxy] --proxy 已从启动参数移除，对应的池子记录一并撤下")
		return nil
	}
	if poolHasProxyURL(url) {
		// 用户自己已经在面板加过同一个出口，不重复建，只记下来
		_ = kvSet(kvSeededProxyURL, url)
		_ = kvSet(kvSeededProxyID, "")
		return nil
	}
	id, err := proxyCreate("启动参数", url, 1)
	if err != nil {
		logf("[proxy] --proxy 入池失败: %v", err)
		return nil // 不记 URL，下次启动还会再试
	}
	_ = kvSet(kvSeededProxyURL, url)
	_ = kvSet(kvSeededProxyID, strconv.FormatInt(id, 10))
	logf("[proxy] --proxy / config.json 的代理已加入代理池")
	return nil
}

// dropSeededProxy 撤掉上一次由启动参数建的那条。
// 只在这条记录**还是我们建时那个 URL** 时才删 —— 用户在面板上把它改成别的出口了，
// 就说明他接管了这条记录，不该被启动参数的变更连坐删掉。
func dropSeededProxy(prevURL string) {
	idStr := kvGet(kvSeededProxyID)
	if idStr == "" || prevURL == "" {
		return
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		return
	}
	proxyMu.RLock()
	var found *Proxy
	for i := range proxyCache {
		if proxyCache[i].ID == id {
			p := proxyCache[i]
			found = &p
			break
		}
	}
	proxyMu.RUnlock()
	if found == nil || found.URL != prevURL {
		return
	}
	if err := proxyDelete(id); err != nil {
		logf("[proxy] 撤下旧的启动参数代理失败: %v", err)
	}
}

// poolHasProxyURL 池子里有没有这个 URL。
func poolHasProxyURL(url string) bool {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.URL == url {
			return true
		}
	}
	return false
}

// pickProxyWithCapacity 找一个可用（enabled + 没熔断或已过冷却）且限流没满的代理。
// 返回 (proxy, ok)。所有代理都不可用或都满时返回 ok=false。
//
// 跟旧的 pickProxy 区别：会问 trySlotAcquire 看 slot 是否有容量；
// 调用方拿到的 slot 必须配套调 slotRelease(proxy.ID)。
func pickProxyWithCapacity() (Proxy, bool) {
	p, ok, _ := pickProxyPreferring(0)
	return p, ok
}

// pickProxyPreferring 优先挑 preferID 那个出口，它不可用或没容量时才轮询别的。
//
// 为什么要粘住：cookie 池和代理池各自独立轮转的话，同一个 Google 账号会在几十个
// 出口 IP 之间来回跳 —— 这在 Google 眼里正是账号共享的特征。粘不住时宁可换出口
// 也不排队等，可用性优先。
// When no route can be selected, reason is either a true slot limit
// (concurrent/rpm/rph/mixed), or "unavailable" when all proxy circuits
// are disabled or cooling down. Do not conflate those cases.
func pickProxyPreferring(preferID int64) (Proxy, bool, string) {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	if len(proxyCache) == 0 {
		return Proxy{}, false, "unavailable"
	}
	now := time.Now().Unix()
	cooldown := rtCfg().ProxyCooldownMin
	var pool []Proxy
	for _, p := range proxyCache {
		if proxyUsable(p, now, cooldown) {
			pool = append(pool, p)
		}
	}
	if len(pool) == 0 {
		return Proxy{}, false, "unavailable"
	}
	blockedReason := ""
	addReason := func(reason string) {
		if blockedReason == "" {
			blockedReason = reason
		} else if blockedReason != reason {
			blockedReason = "mixed"
		}
	}
	if preferID > 0 {
		for _, p := range pool {
			if p.ID == preferID {
				if ok, reason := trySlotAcquire(p.ID); ok {
					return p, true, ""
				} else {
					addReason(reason)
				}
				break // 绑的那个满了，往下走正常轮询
			}
		}
	}
	// 从轮询起点开始,找第一个 slot 没满的
	start := atomic.AddUint64(&proxyCursor, 1) - 1
	for i := 0; i < len(pool); i++ {
		p := pool[(int(start)+i)%len(pool)]
		if ok, reason := trySlotAcquire(p.ID); ok {
			return p, true, ""
		} else {
			addReason(reason)
		}
	}
	return Proxy{}, false, blockedReason
}

// recordProxyResult 回写一次请求的结果，并同步更新内存里那一条。
//
// 只改内存里的那一条，不整表重读：这个函数每个请求都会调，重读一次就是一次全表
// SELECT，而它自己刚发起过 UPDATE —— 高并发下正是这对读写在 WAL 上撞出 SQLITE_BUSY，
// 也就是代理池被读空、请求退回直连的触发条件。
//
// 代价是内存里的 FailCount++ 和 DB 的 fail_count+1 各算各的，别的进程直接改库会
// 让两边漂移。单进程持有这个库，重启也会从 DB 重新加载，可以接受。
func recordProxyResult(id int64, success bool, errStr string) {
	if id == 0 {
		return
	}
	now := time.Now().Unix()
	if success {
		_, _ = getDB().Exec(`UPDATE proxies SET fail_count=0, last_used=?, last_error='' WHERE id=?`, now, id)
	} else {
		_, _ = getDB().Exec(`UPDATE proxies SET fail_count=fail_count+1, last_used=?, last_error=? WHERE id=?`,
			now, errStr, id)
	}
	proxyMu.Lock()
	for i := range proxyCache {
		if proxyCache[i].ID != id {
			continue
		}
		proxyCache[i].LastUsed = now
		if success {
			proxyCache[i].FailCount = 0
			proxyCache[i].LastError = ""
		} else {
			proxyCache[i].FailCount++
			proxyCache[i].LastError = errStr
		}
		break
	}
	proxyMu.Unlock()
}

// CRUD ───────────────────────────────────────────────────────────────────────

func proxyCreate(name, url string, weight int) (int64, error) {
	if name == "" || url == "" {
		return 0, errors.New("name and url required")
	}
	if err := validateProxyURL(url); err != nil {
		return 0, err
	}
	if weight <= 0 {
		weight = 1
	}
	id, err := insertID(`INSERT INTO proxies(name, url, enabled, weight, created_at)
        VALUES (?,?,?,?,?)`, name, url, 1, weight, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	loadProxies()
	return id, nil
}

// validateProxyURL 严格校验代理 URL（R11-4，Oracle A5）：
// 支持 http / https / socks5 / socks5h。
//
// 除 scheme 前缀外还必须通过 net/url.Parse 完整解析且 Host 非空——
// 此前只查前缀：带合法前缀但解析失败的录入错误（如 `http://user:pa ss@host`
// 或控制字符）能进入启用池，而 stdlibClientWithin 对解析失败的 URL 会退回
// Transport.Proxy == nil 的**直连** transport（携 Cookie 静默直出本机 IP）。
// 严格校验把这类值挡在录入/更新边界；存量非法库值由使用侧
// （stdlibClientWithin / rotateProxyURL 等）在使用时明确报错拒绝，同样绝不
// 静默直连。
//
// scheme 按大小写不敏感比：URL 的 scheme 本来就不区分大小写，url.Parse 会统一转小写，
// 所以 HTTP:// 在 4.0.0 那条不做校验的静态代理路径上是能正常工作的。校验若大小写
// 敏感，升级时就会把这种值拦下来 —— 用户什么都没改，代理却不工作了。
func validateProxyURL(s string) error {
	low := strings.ToLower(s)
	schemeOK := false
	for _, p := range []string{"http://", "https://", "socks5://", "socks5h://"} {
		if strings.HasPrefix(low, p) {
			schemeOK = true
			break
		}
	}
	if !schemeOK {
		return errors.New("代理 URL 必须以 http:// / https:// / socks5:// / socks5h:// 开头")
	}
	u, err := url.Parse(s)
	if err != nil {
		// R12-1（Oracle M5/Reviewer #1）：net/url 的 *url.Error 会把**原始
		// 输入串**放进 URL 字段并格式化进错误文本——带认证信息或私有路径
		// 的代理一旦录入出错，原串会越过配置边界进入错误/运行日志。此处
		// 统一返回脱敏占位原因，绝不含原始 URL 字符串或其任何片段。
		return errors.New("代理 URL 解析失败（绝不静默直连）")
	}
	// R12-1（Oracle S4/Reviewer #1）：必须核 Hostname 而非 Host——
	// `http://:8080` 这类仅端口无主机名的 authority 解析出非空 Host
	// 但空 Hostname，照样能逃过 Host 检查进入启用池。
	if u.Hostname() == "" {
		return errors.New("代理 URL 缺少主机名（绝不静默直连）")
	}
	return nil
}

func proxyUpdate(id int64, name, url string, enabled *bool, weight *int) error {
	// R11-4（Oracle A5）：更新同样严格校验 URL——此前只校验新增，存量
	// 记录可经 PATCH 写入解析失败的 URL 进入启用池，使用侧静默直连。
	// 非法值直接报错零写库（不改名称/权重/启用态的任何组合写入）。
	if url != "" {
		if err := validateProxyURL(url); err != nil {
			return err
		}
	}
	// 快照旧值：InvalidateByProxy 按 oldURL/oldEnabled 精准判定（仅删除、
	// URL 变更或禁用时失效；仅改名称/权重不打断在途流）。
	old := proxyByID(id)
	oldURL, oldEnabled := "", false
	if old != nil {
		oldURL, oldEnabled = old.URL, old.Enabled
	}
	q := `UPDATE proxies SET `
	args := []interface{}{}
	parts := []string{}
	if name != "" {
		parts = append(parts, "name=?")
		args = append(args, name)
	}
	if url != "" {
		parts = append(parts, "url=?")
		args = append(args, url)
	}
	if enabled != nil {
		v := 0
		if *enabled {
			v = 1
		}
		parts = append(parts, "enabled=?")
		args = append(args, v)
	}
	if weight != nil {
		parts = append(parts, "weight=?")
		args = append(args, *weight)
	}
	if len(parts) == 0 {
		return nil
	}
	q += joinComma(parts) + " WHERE id=?"
	args = append(args, id)
	_, err := getDB().Exec(q, args...)
	if err == nil {
		loadProxies()
		urlChanged := url != "" && oldURL != "" && url != oldURL
		disabled := enabled != nil && !*enabled && (old == nil || oldEnabled)
		if shouldUseBrowserTransport() && (urlChanged || disabled) {
			browserMgr.InvalidateByProxy(id, oldURL, "proxy updated")
		}
	}
	return err
}

// proxyByID 按 id 取代理（含 disabled），取不到返回 nil。
func proxyByID(id int64) *Proxy {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.ID == id {
			cp := p
			return &cp
		}
	}
	return nil
}

func proxyDelete(id int64) error {
	old := proxyByID(id)
	oldURL := ""
	if old != nil {
		oldURL = old.URL
	}
	_, err := getDB().Exec(`DELETE FROM proxies WHERE id=?`, id)
	if err == nil {
		loadProxies()
		if shouldUseBrowserTransport() {
			browserMgr.InvalidateByProxy(id, oldURL, "proxy deleted")
		}
	}
	return err
}

func proxyResetFailures(id int64) error {
	_, err := getDB().Exec(`UPDATE proxies SET fail_count=0, last_error='' WHERE id=?`, id)
	if err == nil {
		loadProxies()
	}
	return err
}

func listProxies() []Proxy {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	out := make([]Proxy, len(proxyCache))
	copy(out, proxyCache)
	return out
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}

// proxyDisplayName 是「绑定出口」展示名：-1 = 本机直连（锁定），0 = 未绑定，
// >0 = 代理名（取不到回空串）。
func proxyDisplayName(id int64) string {
	if id == proxyIDDirect {
		return "本机直连（锁定）"
	}
	return proxyNameByID(id)
}

// proxyNameByID 按 id 找代理名，找不到返回空串（0 = 还没绑 / 直连）。
func proxyNameByID(id int64) string {
	if id <= 0 {
		return ""
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.ID == id {
			return p.Name
		}
	}
	return ""
}

// pickBoundProxy 只试账号绑定的那个出口（strict_proxy_binding 用）。
// bound=false 表示这个绑定已经不算数了（代理被删除或被手动禁用），调用方应照常轮询并重绑；
// bound=true 时 ok 说明拿没拿到它的 slot，reason 是没拿到的原因（熔断冷却记成 "cooling"）。
func pickBoundProxy(id int64) (p Proxy, ok bool, reason string, bound bool) {
	if id <= 0 {
		return Proxy{}, false, "", false
	}
	proxyMu.RLock()
	var found *Proxy
	for i := range proxyCache {
		if proxyCache[i].ID == id {
			c := proxyCache[i]
			found = &c
			break
		}
	}
	proxyMu.RUnlock()
	if found == nil || !found.Enabled {
		return Proxy{}, false, "", false
	}
	if !proxyUsable(*found, time.Now().Unix(), rtCfg().ProxyCooldownMin) {
		return *found, false, "cooling", true
	}
	if ok, reason := trySlotAcquire(found.ID); !ok {
		return *found, false, reason, true
	}
	return *found, true, "", true
}

func proxyExists(id int64) bool {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.ID == id {
			return true
		}
	}
	return false
}

// strictBoundElsewhere：严格绑定开着时，这个号绑在另一个仍然有效的出口上，就不能借用当前出口。
func strictBoundElsewhere(a *CookieAccount, proxyID int64) bool {
	if a == nil || !rtCfg().StrictProxyBinding || a.ProxyID <= 0 || a.ProxyID == proxyID {
		return false
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.ID == a.ProxyID {
			return p.Enabled
		}
	}
	return false
}

// proxyEnabledByID 只看这个出口是否仍然存在且 enabled（不看 FailCount/冷却）。
// claimAccountProxy 用它判绑定是否还有效：冷却中的绑定仍然是绑定，绝不能被覆写。
func proxyEnabledByID(id int64) bool {
	if id <= 0 {
		return false
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.ID == id {
			return p.Enabled
		}
	}
	return false
}

// hasProxyPool 报告代理池是否配置了任何代理（不看 enabled/熔断）。
// 「池非空 = 配了池」是所有「绝不直连」判定的共用口径（acquireSlot /
// syncBridgeUpstream / allSlotUsage / acquireClaimedSlot 的 -1 快照纠偏）。
func hasProxyPool() bool {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	return len(proxyCache) > 0
}

// proxyUsableByID 这个出口现在还能不能用（存在 + enabled + 没熔断或已过冷却）。
// 判断账号绑定的出口是不是还有效，无效才该重新绑。
func proxyUsableByID(id int64) bool {
	if id <= 0 {
		return false
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	now := time.Now().Unix()
	cooldown := rtCfg().ProxyCooldownMin
	for _, p := range proxyCache {
		if p.ID == id {
			return proxyUsable(p, now, cooldown)
		}
	}
	return false
}

// proxyURLByID 按 id 取代理 URL，找不到返回空串（直连）。
func proxyURLByID(id int64) string {
	if id <= 0 {
		return ""
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.ID == id {
			return p.URL
		}
	}
	return ""
}

// proxyAtomicSnapshot 在**单次** proxyMu.RLock 内原子读取代理的存在性、
// 启用态与 URL（R10-3，Reviewer M3）：rotateProxyURL 此前分两次独立
// RLock 分别读 enabled 与 URL——两次读取之间代理被删除时会拼出
// 「enabled=true 但 url=\"\"」的 (\"\", nil)，该组合在前后任何一个真实
// 状态中都不存在，下游 rotateDo 把空串当直连，携 Cookie 静默直出。
// 单次原子快照彻底杜绝该组合：返回的 (found, enabled, url) 必然来自
// 同一时刻的同一行。
func proxyAtomicSnapshot(id int64) (found, enabled bool, url string) {
	if id <= 0 {
		return false, false, ""
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.ID == id {
			return true, p.Enabled, p.URL
		}
	}
	return false, false, ""
}
