package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CookieAccount 是 cookie 池里的一行：一个 Google 登录态账号。
type CookieAccount struct {
	ID         int64  `json:"id"`
	Label      string `json:"label"`
	Cookie     string `json:"cookie"` // 完整串，API 层按需脱敏后再返回
	Status     string `json:"status"`
	Note       string `json:"note"`
	CreatedAt  int64  `json:"created_at"`
	LastUsedAt int64  `json:"last_used_at"`
	LastOkAt   int64  `json:"last_ok_at"`
	LastError  string `json:"last_error"`
	FailCount  int64  `json:"fail_count"`
	ProxyID    int64  `json:"proxy_id"` // 绑定的出口，0 = 还没绑
	// AuthUser 是浏览器里多号登录时的账号序号：同一串 cookie 里登着好几个号，
	// 请求路径带 /u/N 才落到第 N 个号，0 = 默认号（裸路径）。
	AuthUser int `json:"authuser"`
	// Priority 是主备优先级（v6.6.0）：数值越大越先被挑中，建议主力号 100、
	// 备用号 10 / 5，默认 0 与旧行为一致（同级之间仍按失败数与最久未用轮转）。
	Priority int `json:"priority"`
	// AutoDisabled（v6.7.0）为 true 表示本行是被连续鉴权失败自动停用的，
	// 扩展推来新 cookie 可以自动恢复 enabled；管理员手动停用/启用一律为 false。
	AutoDisabled bool `json:"auto_disabled"`
}

// 账号优先级的合法范围：面板/API 越界直接 400 零写库。
const (
	minAccountPriority = -1000
	maxAccountPriority = 10000
)

// accountSetPriority 改主备优先级（PATCH 用；值已由调用方校验过范围）。
func accountSetPriority(id int64, priority int) error {
	_, err := getDB().Exec(`UPDATE accounts SET priority=? WHERE id=?`, priority, id)
	return err
}

// splitCookiePairs 把 "k=v; k=v" 拆成键值对。
//
// 按 ";" 切再逐段 TrimSpace，不按 "; " 切：从 DevTools 复制出来的串不一定带空格，
// 而按 "; " 切的话 "SID=a;SAPISID=b" 会整段当成一个键 —— 于是 extractSAPISID
// 取不到值、不发 Authorization 头、请求被上游当匿名处理，用户毫不知情。
//
// 值里可能含 "="（base64 补位），所以只在第一个等号处切。
func splitCookiePairs(cookie string) [][2]string {
	var out [][2]string
	for _, p := range strings.Split(cookie, ";") {
		p = strings.TrimSpace(p)
		if i := strings.Index(p, "="); i > 0 {
			out = append(out, [2]string{p[:i], p[i+1:]})
		}
	}
	return out
}

// cookieValue 从一整串 cookie 里取指定名字的值，取不到返回空串。
func cookieValue(cookie, name string) string {
	for _, kv := range splitCookiePairs(cookie) {
		if kv[0] == name {
			return kv[1]
		}
	}
	return ""
}

// extractSAPISID 从一整串 cookie 里取 SAPISID 的值，取不到返回空串。
func extractSAPISID(cookie string) string {
	return cookieValue(cookie, "SAPISID")
}

// cookieSubset 只留下指定名字的项，顺序跟原串一致。刷新 1PSIDTS 时不能把整串
// 都带上：多带 Chrome DBSC / 其它主机的 cookie 实测会让 RotateCookies 回 401。
func cookieSubset(cookie string, names []string) string {
	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[n] = true
	}
	var parts []string
	seen := map[string]bool{}
	for _, kv := range splitCookiePairs(cookie) {
		if !want[kv[0]] || kv[1] == "" || seen[kv[0]] {
			continue
		}
		seen[kv[0]] = true
		parts = append(parts, kv[0]+"="+kv[1])
	}
	return strings.Join(parts, "; ")
}

// cookieNames 返回 cookie 串里出现的所有 cookie 名（顺序保留），供 UI 展示。
func cookieNames(cookie string) []string {
	var names []string
	for _, kv := range splitCookiePairs(cookie) {
		names = append(names, kv[0])
	}
	return names
}

// hasCookieName 判断 cookie 串里有没有指定名字的项（精确匹配，__Secure-OSID 不算 OSID）。
func hasCookieName(cookie, name string) bool {
	for _, n := range cookieNames(cookie) {
		if n == name {
			return true
		}
	}
	return false
}

// warnMissingOSID 只记日志不拦。判据（2026-11-21 实测）：去掉 OSID 后 /app 302 到
// ServiceLogin?osid=1，带上就 200。只验过一个号，所以不硬拦，免得挡掉能用的号。
func warnMissingOSID(label, cookie string) {
	if hasCookieName(cookie, "OSID") {
		return
	}
	logf("[cookie] %s 里没有 OSID，gemini.google.com 会跳去登录页；请在 gemini.google.com 页面重新导出", label)
}

// accountAdd 往池里插一条。cookie 必须能取到非空的 SAPISID。
// 这是面板/API 手工添加走的路，用户当场看得到错误提示，拦下来是对的。
//
// 判据是**取不取得到值**，不是"字符串里出没出现过 SAPISID"：后者会把
// 一份没填的逐项模板（键名在、值全空）放进池子，存成一整段 JSON，
// 之后每次轮到它都注定失败。
func accountAdd(label, cookie, note string) (int64, error) {
	c, ok := normalizeCookie(cookie, "手工添加的 cookie")
	if !ok {
		return 0, fmt.Errorf("cookie 解析失败：既不是 \"k=v; k=v\" 串，也不是可识别的 JSON")
	}
	if extractSAPISID(c) == "" {
		return 0, fmt.Errorf("cookie 里没有 SAPISID，多半没复制全（需要 gemini.google.com 下的完整 cookie，至少含 SID / HSID / SSID / APISID / SAPISID / __Secure-1PSID）")
	}
	warnMissingOSID(label, c)
	return accountInsert(label, c, note)
}

// accountAdopt 是启动时导入既有配置专用的：**不做 SAPISID 检查**。
//
// 判据是「升级不能改变用户已有配置的可用性」。缺 SAPISID 的 cookie 在旧版是被
// 原样当 Cookie 头发出去的（只是算不出 SAPISIDHASH 授权头），能不能用由上游说了算 ——
// 不该在升级时被我们新加的校验拦下来，让用户悄无声息地退回匿名。只警告，
// 健康度会在面板上如实体现。
func accountAdopt(label, cookie, note string) (int64, error) {
	cookie, ok := normalizeCookie(cookie, label)
	if !ok {
		return 0, fmt.Errorf("cookie 解析失败")
	}
	if extractSAPISID(cookie) == "" {
		logf("[cookie] %s 里没有 SAPISID，算不出 SAPISIDHASH 授权头，可能只当匿名处理；"+
			"先按原样导入，请到面板核对", label)
	}
	warnMissingOSID(label, cookie)
	return accountInsert(label, cookie, note)
}

func accountInsert(label, cookie, note string) (int64, error) {
	cookie = strings.TrimSpace(cookie)
	if cookie == "" {
		return 0, fmt.Errorf("cookie 不能为空")
	}
	return insertID(
		`INSERT INTO accounts(label, cookie, status, note, created_at) VALUES (?,?,?,?,?)`,
		strings.TrimSpace(label), cookie, "enabled", strings.TrimSpace(note), time.Now().Unix())
}

// accountList 返回池里全部账号，按 id 升序。
func accountList() []CookieAccount {
	rows, err := getDB().Query(
		`SELECT id, label, cookie, status, note, created_at, last_used_at, last_ok_at, last_error, fail_count, proxy_id, authuser, priority, auto_disabled
		 FROM accounts ORDER BY priority DESC, id`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []CookieAccount
	for rows.Next() {
		var a CookieAccount
		if err := rows.Scan(&a.ID, &a.Label, &a.Cookie, &a.Status, &a.Note,
			&a.CreatedAt, &a.LastUsedAt, &a.LastOkAt, &a.LastError, &a.FailCount, &a.ProxyID, &a.AuthUser, &a.Priority, &a.AutoDisabled); err != nil {
			continue
		}
		out = append(out, a)
	}
	return out
}

// accountByID 按 id 取一条，取不到返回 nil。
//
// 轮转会把新 cookie 写回库里，之后要用**库里那份**继续发请求 —— 手上那个
// CookieAccount 是轮转之前的快照，接着用等于把刚刷新的值扔掉。
func accountByID(id int64) *CookieAccount {
	var a CookieAccount
	err := getDB().QueryRow(
		`SELECT id, label, cookie, status, note, created_at, last_used_at, last_ok_at, last_error, fail_count, proxy_id, authuser, priority, auto_disabled
		 FROM accounts WHERE id=?`, id).
		Scan(&a.ID, &a.Label, &a.Cookie, &a.Status, &a.Note,
			&a.CreatedAt, &a.LastUsedAt, &a.LastOkAt, &a.LastError, &a.FailCount, &a.ProxyID, &a.AuthUser, &a.Priority, &a.AutoDisabled)
	if err != nil {
		return nil
	}
	return &a
}

// accountDelete 删除一条。
func accountDelete(id int64) error {
	_, err := getDB().Exec(`DELETE FROM accounts WHERE id=?`, id)
	return err
}

// accountSetStatus 改状态（enabled / disabled）。
// 本入口只被管理员操作（toggle / PATCH）调用：不论启用还是停用，一律清掉
// auto_disabled——从管理员点下去这一刻起，该行的停用不再算「自动停用」，
// 扩展再推新 cookie 也不得越过这条手动决定把它启用回来。
func accountSetStatus(id int64, status string) error {
	if status != "enabled" && status != "disabled" {
		return fmt.Errorf("非法状态 %q", status)
	}
	_, err := getDB().Exec(`UPDATE accounts SET status=?, auto_disabled=0 WHERE id=?`, status, id)
	return err
}

// accountUpdateMeta 改 label / note（不动 cookie 本身）。
func accountUpdateMeta(id int64, label, note string) error {
	_, err := getDB().Exec(`UPDATE accounts SET label=?, note=? WHERE id=?`,
		strings.TrimSpace(label), strings.TrimSpace(note), id)
	return err
}

// maxAuthUser 是账号序号上限。Google 一个浏览器最多同时登 10 个号，序号 0..9。
const maxAuthUser = 9

// accountSetAuthUser 改账号序号（/u/N 里的 N）。
func accountSetAuthUser(id int64, n int) error {
	if n < 0 || n > maxAuthUser {
		return fmt.Errorf("账号序号要在 0..%d 之间，收到 %d", maxAuthUser, n)
	}
	_, err := getDB().Exec(`UPDATE accounts SET authuser=? WHERE id=?`, n, id)
	return err
}

// accountCount 返回 (总数, enabled 数)。
func accountCount() (int, int) {
	var total, enabled int
	_ = getDB().QueryRow(`SELECT COUNT(*), SUM(CASE WHEN status='enabled' THEN 1 ELSE 0 END) FROM accounts`).
		Scan(&total, &enabled)
	return total, enabled
}

// kv 里记迁移/播种状态的键。
const (
	kvLegacyCookieDone = "legacy_single_cookie_migrated"
	kvSeededCookieID   = "seeded_cookie_id"
	kvSeededCookieVal  = "seeded_cookie_value"
)

// seedCookiesFromConfig 把启动参数和历史遗留的单 cookie 并进 cookie 池。
//
// cookie 原来也有两个入口：cookie 池，和「设置」页那个池空时才用的单 cookie 输入
// （值存 kv 的 google_cookie，或来自 --cookie-file）。跟静态代理同一个毛病 ——
// 单 cookie 路径返回的账号 ID 是 0，而 markAccountResult 开头就是 id<=0 直接返回，
// 于是**健康度一个字都不写**：fail_count 恒为 0、last_ok_at 恒为空，也没有轮转。
//
//   - kv 里的 google_cookie：一次性迁入，用独立标记记"迁过了"，原值不动
//   - cfg.CookieFile（--cookie-file）：声明式跟随，内容变了替换同一条记录
func seedCookiesFromConfig() {
	migrateLegacyCookie()
	syncSeededCookieFile()
}

// migrateLegacyCookie 一次性把 kv 里遗留的单 cookie 搬进池子。
// 三条保命规则：走 accountAdopt 不做 SAPISID 校验（旧路径不校验，升级不该改变
// 可用性）、入池成功才标记完成、不去动 kv 里原来的值（回滚到旧版仍可用）。
func migrateLegacyCookie() {
	if kvGet(kvLegacyCookieDone) == "1" {
		return
	}
	raw := strings.TrimSpace(kvGet("google_cookie"))
	if raw == "" {
		_ = kvSet(kvLegacyCookieDone, "1")
		return
	}
	cookie, ok := normalizeCookie(raw, "「设置」页的单 cookie")
	if !ok {
		return
	}
	if !poolHasCookie(cookie) {
		if _, err := accountAdopt("原单 cookie", cookie, "从「设置」页迁入"); err != nil {
			logf("[cookie] 「设置」页的单 cookie 迁入池子失败，原值保留、下次启动重试: %v", err)
			return
		}
		logf("[cookie] 「设置」页的单 cookie 已迁入 cookie 池")
	}
	_ = kvSet(kvLegacyCookieDone, "1")
}

// syncSeededCookieFile 让池子里跟着 --cookie-file 走一条记录。
//
// 跟 --proxy 同一套：文件内容变了就撤下旧的那条再建新的，而不是又加一条。
// 按内容去重挡不住这个 —— 定期轮换 cookie.txt 的部署会一次次往池子里堆死 cookie，
// 而它们仍然 enabled，仍然参与轮转，于是每 N 个请求就有一个注定失败。
// 内容没变时完全不碰池子，面板上的增删改停用都以面板为准。
func syncSeededCookieFile() {
	var cur string
	if cfg.CookieFile != "" {
		data, err := os.ReadFile(cfg.CookieFile)
		if err != nil {
			// 读不到就当没配过：宁可保持现状，也不能因为容器少挂一个卷
			// 就把用户在用的 cookie 撤下来。
			logf("[cookie] 读不了 --cookie-file %s，池子保持不变: %v", cfg.CookieFile, err)
			return
		}
		cur = strings.TrimSpace(string(data))
	}
	if cur != "" {
		if c, ok := normalizeCookie(cur, "--cookie-file"); ok {
			cur = c
		} else {
			return
		}
	}

	prev := kvGet(kvSeededCookieVal)
	if cur == prev {
		return
	}
	dropSeededCookie(prev)
	if cur == "" {
		_ = kvSet(kvSeededCookieVal, "")
		_ = kvSet(kvSeededCookieID, "")
		logf("[cookie] --cookie-file 已移除，对应的池子记录一并撤下")
		return
	}
	if poolHasCookie(cur) {
		_ = kvSet(kvSeededCookieVal, cur)
		_ = kvSet(kvSeededCookieID, "")
		return
	}
	id, err := accountAdopt("cookie-file", cur, "来自 --cookie-file")
	if err != nil {
		logf("[cookie] --cookie-file 入池失败: %v", err)
		return
	}
	_ = kvSet(kvSeededCookieVal, cur)
	_ = kvSet(kvSeededCookieID, strconv.FormatInt(id, 10))
	logf("[cookie] --cookie-file 的 cookie 已加入 cookie 池")
}

// dropSeededCookie 撤掉上一次由 --cookie-file 建的那条。
// 只在内容还是我们写进去的那份时才删——用户在面板改过就说明他接管了。
func dropSeededCookie(prevCookie string) {
	idStr := kvGet(kvSeededCookieID)
	if idStr == "" || prevCookie == "" {
		return
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		return
	}
	for _, a := range accountList() {
		if a.ID == id {
			if a.Cookie == prevCookie {
				if err := accountDelete(id); err != nil {
					logf("[cookie] 撤下旧的 --cookie-file 记录失败: %v", err)
				}
			}
			return
		}
	}
}

// 面板逐项填写模式列出的 cookie，同时也是拼串时的固定顺序。
// 顺序必须确定：同一份 cookie 若因键顺序不同拼出两种串，按内容去重就失效了。
var cookieTemplateOrder = []string{
	"SID", "HSID", "SSID", "APISID", "SAPISID", "__Secure-1PSID", "__Secure-1PSIDTS",
	"OSID",
}

// normalizeCookie 把各种输入形态归一化成池子要的裸 "k=v; k=v" 串。
//
// 吃这几种：
//   - 裸串，原样返回
//   - 旧单 cookie 路径的 {"cookie":"k=v; k=v","sapisid":"..."}
//   - 面板逐项模式的 {"SID":"a","SAPISID":"b",...}
//   - 浏览器扩展导出的数组 [{"name":"SID","value":"a","domain":".google.com"},...]
//     以及 Playwright storageState {"cookies":[...]}，按 domain 过滤到 gemini.google.com
//
// 不归一化的话池子里会存进一整段 JSON，SAPISID 提取和后续请求全错。
func normalizeCookie(raw, who string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "[") {
		var arr []cookieJSONEntry
		if err := json.Unmarshal([]byte(raw), &arr); err != nil {
			logf("[cookie] %s 是 JSON 数组但解析失败，跳过: %v", who, err)
			return "", false
		}
		return joinCookieEntries(arr, who)
	}
	if !strings.HasPrefix(raw, "{") {
		return raw, true
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		logf("[cookie] %s 是 JSON 但解析失败，跳过: %v", who, err)
		return "", false
	}
	// Playwright / Puppeteer 的 storageState：{"cookies":[{name,value,domain}...], "origins":[...]}
	if _, ok := m["cookies"].([]interface{}); ok {
		var st struct {
			Cookies []cookieJSONEntry `json:"cookies"`
		}
		if err := json.Unmarshal([]byte(raw), &st); err != nil {
			logf("[cookie] %s 的 cookies 数组解析失败，跳过: %v", who, err)
			return "", false
		}
		return joinCookieEntries(st.Cookies, who)
	}
	if c, ok := m["cookie"].(string); ok && strings.TrimSpace(c) != "" {
		return strings.TrimSpace(c), true
	}

	str := func(k string) string {
		s, _ := m[k].(string)
		return strings.TrimSpace(s)
	}
	var parts []string
	inTemplate := map[string]bool{}
	for _, k := range cookieTemplateOrder {
		inTemplate[k] = true
		if v := str(k); v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	// 用户从 DevTools 多复制几项进来不能丢，附在模板字段后面按名字排序
	var extra []string
	for k := range m {
		if !inTemplate[k] && k != "sapisid" && str(k) != "" {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	for _, k := range extra {
		parts = append(parts, k+"="+str(k))
	}
	if len(parts) == 0 {
		logf("[cookie] %s 是 JSON 但一项非空值都没有，跳过", who)
		return "", false
	}
	return strings.Join(parts, "; "), true
}

// cookieJSONEntry 是浏览器扩展（Cookie-Editor / EditThisCookie / Get cookies.txt 的 JSON 版）
// 和 Playwright storageState 导出的单条 cookie。只用得到这三个字段。
type cookieJSONEntry struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Domain string `json:"domain"`
}

// cookieExportDomainOK 按 RFC 6265 的 domain-match 规则判断浏览器会不会把这条 cookie
// 发给 gemini.google.com。导出「全部 cookie」时会混进 youtube.com、
// accounts.google.com 之类的，那些不该进串里——尤其 accounts.google.com 也有一个
// OSID，混进来会顶掉真正的那个。domain 为空（有些导出不带）就当匹配。
// 复用下方 RFC 6265 §5.1.3 的 cookieDomainMatches(host, domain)。
func cookieExportDomainOK(domain string) bool {
	d := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(domain), "."))
	if d == "" {
		return true
	}
	return cookieDomainMatches(geminiCookieTargetHost, d)
}

// joinCookieEntries 把导出的 cookie 数组拼成 "k=v; k=v"。保留导出里的原始顺序（接近浏览器
// 实际发送的顺序）；同名的按 domain 更具体的那条为准——host-only 的 gemini.google.com
// 优先于 .google.com。
func joinCookieEntries(arr []cookieJSONEntry, who string) (string, bool) {
	type kept struct {
		value  string
		domLen int
	}
	var order []string
	byName := map[string]kept{}
	for _, e := range arr {
		name := strings.TrimSpace(e.Name)
		val := strings.TrimSpace(e.Value)
		if name == "" || val == "" || !cookieExportDomainOK(e.Domain) {
			continue
		}
		dl := len(strings.TrimPrefix(strings.TrimSpace(e.Domain), "."))
		if prev, ok := byName[name]; ok {
			if dl > prev.domLen {
				byName[name] = kept{val, dl}
			}
			continue
		}
		order = append(order, name)
		byName[name] = kept{val, dl}
	}
	if len(order) == 0 {
		logf("[cookie] %s 是 cookie 数组但没有发往 %s 的有效项，跳过", who, geminiCookieTargetHost)
		return "", false
	}
	parts := make([]string, 0, len(order))
	for _, n := range order {
		parts = append(parts, n+"="+byName[n].value)
	}
	return strings.Join(parts, "; "), true
}

func poolHasCookie(cookie string) bool {
	for _, a := range accountList() {
		if a.Cookie == cookie {
			return true
		}
	}
	return false
}

// pickMu 把「SELECT 最久未用的号 + UPDATE 标记它刚用过」串成原子操作。
// 不加锁时两个并发请求会 SELECT 到同一个"最久未用"的号、各自 UPDATE，于是同一瞬间
// 双双用它，轮转形同虚设（CLAUDE.md 记的已知缺陷）。本进程内一把锁就够——限流器、
// 轮转调度、代理池都是进程内状态，这个反代天生单实例，不存在跨进程并发挑号；也就
// 不必为此上跨方言的事务/行锁（sqlite 无 FOR UPDATE、mysql 无 RETURNING，那条路
// 全是方言分支）。挑号只是一次极快的 SELECT+UPDATE，串行化的争用可忽略。
var pickMu sync.Mutex

// pickCookieAccount 从池里挑一个 enabled 账号，按 last_used_at 最久优先，
// 挑中后立刻把 last_used_at 记为现在（下次轮到别人）。池空返回 (nil,false)。
func pickCookieAccount() (*CookieAccount, bool) {
	return pickCookieAccountExcept(nil)
}

// pickCookieAccountExcept 同上，但跳过本次已经试过的账号。
//
// 存在的理由：一个 cookie 失效不该让整个请求失败。池子里 2 个号坏 1 个，
// 轮转会让大约一半请求撞上坏号 —— 表现就是"成功率莫名其妙很低"，而每次失败
// 看起来都像是上游的问题。
func pickCookieAccountExcept(skip map[int64]bool) (*CookieAccount, bool) {
	pickMu.Lock()
	defer pickMu.Unlock()
	// 健康的排前面，同样健康的按最久未用轮转。
	//
	// 不这么排的话坏号会跟好号平起平坐地轮到，而挑到坏号时它没有绑定的出口，
	// 出口就按"无偏好"选了；等换号换到好号，出口已经定死 —— 于是好号的出口
	// 粘性被坏号带偏。实测 1 好 2 坏时出口在两个代理间对半分。
	rows, err := getDB().Query(
		`SELECT id, label, cookie, status, note, created_at, last_used_at, last_ok_at, last_error, fail_count, proxy_id, authuser, priority, auto_disabled
		 FROM accounts WHERE status='enabled' ORDER BY priority DESC, fail_count ASC, last_used_at ASC, id ASC`)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	for rows.Next() {
		var a CookieAccount
		if err := rows.Scan(&a.ID, &a.Label, &a.Cookie, &a.Status, &a.Note,
			&a.CreatedAt, &a.LastUsedAt, &a.LastOkAt, &a.LastError, &a.FailCount, &a.ProxyID, &a.AuthUser, &a.Priority, &a.AutoDisabled); err != nil {
			continue
		}
		if skip[a.ID] {
			continue
		}
		_, _ = getDB().Exec(`UPDATE accounts SET last_used_at=? WHERE id=?`, time.Now().Unix(), a.ID)
		return &a, true
	}
	return nil, false
}

// markCookieByStatus 按上游返回回写 cookie 健康度。
//
// 只把明确的鉴权失败（401/403）算作 cookie 的错。网络错误、代理失败、302 → sorry
// （IP 被 Google 拦）一律不计——实测住宅代理出口退化率高达 75%，把这些算进
// fail_count 会让它变成代理噪音，好 cookie 会被误伤成"失败最多"。
//
// statusCode 为 0 表示压根没拿到响应（网络层失败）。
func markCookieByStatus(id int64, statusCode int, errStr string) {
	switch {
	case statusCode == 200:
		markAccountResult(id, true, "")
	case statusCode == 401 || statusCode == 403:
		markAccountResult(id, false, errStr)
	default:
		// 其余情况责任不在 cookie，不动它的健康度
	}
}

// markAccountResult 请求结束后回写结果：成功清零 fail_count 并记 last_ok_at；
// 失败累加 fail_count 并记 last_error。
//
// 注意 last_ok_at 的语义是"这个 cookie 参与的请求成功过"，**不等于"cookie 仍然
// 有效"**：cookie 过期后 Gemini 不报错，只是把你当匿名用户，纯文本请求照样 200。
// 要真正验有效性，最省事的判据是请求 gemini-3.1-pro：cookie 有效时服务端回报
// "3.1 Pro" 并带思考链，失效时静默降级成 3.5 Flash-Lite。（xsrf.go 取 token 时
// 若页面里没有 SNlM0e，也会当场判定 cookie 失效。）
func markAccountResult(id int64, ok bool, errStr string) {
	if id <= 0 {
		return
	}
	if ok {
		_, _ = getDB().Exec(
			`UPDATE accounts SET last_ok_at=?, fail_count=0, last_error='', auto_disabled=0 WHERE id=?`,
			time.Now().Unix(), id)
		return
	}
	_, _ = getDB().Exec(
		`UPDATE accounts SET fail_count=fail_count+1, last_error=? WHERE id=?`,
		truncate(errStr, 200), id)
	autoDisableIfDead(id)
}

// maxCookieAuthFailures 是连续几次鉴权失败之后自动停用账号。
//
// 只有 401/403 会累加 fail_count（见 markCookieByStatus），网络错误和被 Google 拦
// 都不算，所以连着 3 次基本等于 cookie 真的没了。取 3 不取 1：偶发的 XSRF 页面抖动
// 也会走到这条路上，一次就停用会误伤。
const maxCookieAuthFailures = 3

// autoDisableIfDead 把连续失败到头的账号停用。
//
// 为什么要自动停：挑号是 `ORDER BY fail_count ASC` ——坏号排在最后，但**池子里只剩
// 坏号时它照样会被选中**，于是每个请求都要把它试一遍才轮到报错。而 fail_count 只有
// 成功才清零，死号永远不会自己好，等于让每个请求都为它付一次 XSRF 往返。
//
// 停用而不是删除：cookie 是用户导入的数据，判断可能出错（比如出口连续被拦也可能
// 表现成鉴权失败），留着让用户在面板上看到并自己决定。
func autoDisableIfDead(id int64) {
	var fails int64
	var status string
	err := getDB().QueryRow(`SELECT fail_count, status FROM accounts WHERE id=?`, id).
		Scan(&fails, &status)
	if err != nil || status != "enabled" || fails < maxCookieAuthFailures {
		return
	}
	if _, err := getDB().Exec(
		`UPDATE accounts SET status='disabled', auto_disabled=1 WHERE id=? AND status='enabled'`, id); err == nil {
		logf("[cookie] 账号 #%d 连续 %d 次鉴权失败，已自动停用", id, fails)
		// SF-1：停用成功后精准失效其常驻 BrowserContext，避免停用槽
		// 继续被懒建复用。
		if shouldUseBrowserTransport() {
			browserMgr.InvalidateAccount(id, false, "disabled")
		}
	}
}

// CookieCheck 是一次 cookie 有效性检测的结果。
type CookieCheck struct {
	OK        bool   `json:"ok"`
	Detail    string `json:"detail"`
	ProxyName string `json:"proxy_name"`
	TookMs    int64  `json:"took_ms"`
}

// checkAccountCookie 判断一条 cookie 还有没有登录态。
//
// 判据是 /app 页面里有没有 SNlM0e：cookie 失效时 Gemini 不报错，只是把你当匿名
// 用户，纯文本请求照样 200 —— 所以不能拿"请求成功"当有效性判据。这个页面没有
// SNlM0e 就说明服务端没认这个登录态。
//
// 只抓页面，不发对话，不消耗生成配额。
func checkAccountCookie(a CookieAccount) CookieCheck {
	t0 := time.Now()
	// 调用方手上是 accountList() 的陈旧快照：并发请求或手动改绑之后，库里的
	// 绑定可能已经变了，必须先按库里当前值走（同 viaSlot 的刷新），绝不能拿
	// 旧 ProxyID 去挑出口——严格绑定下拿旧值仲裁会误抢并发请求赢下的绑定。
	if a.ID > 0 {
		if cur := accountByID(a.ID); cur != nil {
			a.ProxyID = cur.ProxyID
		}
	}
	picked, release, err := acquireClaimedSlot(a.ID, a.ProxyID)
	if picked == nil {
		return CookieCheck{Detail: "拿不到出口：" + err.Error()}
	}
	defer release()
	proxyURL := picked.URL
	name := picked.Name
	if name == "" {
		name = "直连"
	}

	// 先作废缓存，否则可能拿到几分钟前的旧结论，检测就没意义了。
	// 管理面板的 Cookie 检测是辅助入口：allowSwitch=false，浏览器路径
	// 驻留号坏了直接回错回退纯 Go HTTP，不偷换驻留 jar。
	invalidateXSRF(a.Cookie)
	_, err = getXSRFSwitch(a.Cookie, a.AuthUser, proxyURL, false)
	took := time.Since(t0).Milliseconds()
	if err != nil {
		// 断网、被拦这类不是 cookie 的错，不动健康度（也不算成功）
		if xsrfErrBlamesCookie(err) {
			markAccountResult(a.ID, false, err.Error())
		}
		return CookieCheck{Detail: explainCookieFailure(err), ProxyName: name, TookMs: took}
	}
	markAccountResult(a.ID, true, "")
	// 严格绑定下绑定仲裁已在 acquireClaimedSlot 里完成：这里再无条件覆写会把
	// 已仲裁出的胜者改成本次挑到的出口。非严格模式保留原写库语义（首次绑定/失效重绑）。
	if !rtCfg().StrictProxyBinding && picked.ID > 0 && a.ProxyID != picked.ID {
		autoBindAccountProxy(a.ID, picked.ID)
	}
	detail := "登录态有效"
	if extractSAPISID(a.Cookie) == "" {
		detail += "，但缺 SAPISID（算不出授权头，部分接口会被当匿名）"
	}
	return CookieCheck{OK: true, Detail: detail, ProxyName: name, TookMs: took}
}

// explainCookieFailure 把底层错误翻成运维看得懂的结论。
func explainCookieFailure(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "no SNlM0e"):
		return "cookie 已失效：页面能打开但没有登录态，请求会被当匿名处理"
	// sorry 要排在 302 前面：跳 /sorry 也是 302，那是出口被拦不是 cookie 失效
	case strings.Contains(msg, "HTTP 429"), strings.Contains(msg, "sorry"):
		return "出口被上游限流，换个代理再试（不是 cookie 的问题）"
	case strings.Contains(msg, "缺 OSID"):
		return "cookie 缺 OSID：gemini.google.com 现在要求这一项，请在 gemini.google.com 页面重新导出"
	case strings.Contains(msg, "HTTP 302"):
		return "cookie 无效：被重定向到登录页"
	default:
		return "检测失败：" + msg
	}
}

// bindAccountProxyLocked 记住这个账号这次用的出口，下次优先复用。
// 调用方必须持有 claimMu——锁外写库会覆盖 claimAccountProxy 刚仲裁出的胜者。
func bindAccountProxyLocked(accountID, proxyID int64) {
	if accountID <= 0 {
		return
	}
	_, _ = getDB().Exec(`UPDATE accounts SET proxy_id=? WHERE id=?`, proxyID, accountID)
}

// bindAccountProxy 是锁外入口（管理员 PATCH/POST 改绑、非严格模式的首绑/重绑落库）：
// 加 claimMu 后与 claimAccountProxy 完全互斥——手动改绑不能插在「核库→写库」之间。
func bindAccountProxy(accountID, proxyID int64) {
	if accountID <= 0 {
		return
	}
	claimMu.Lock()
	defer claimMu.Unlock()
	bindAccountProxyLocked(accountID, proxyID)
}

// autoBindAccountProxy 是**非管理员自动写绑**的统一入口：在 claimMu 锁内先
// 重查 DB 当前绑定，绝不拿锁外陈旧快照决定写不写——管理员在 getXSRF 等
// 网络 I/O 期间刚把账号锁成 proxyIDDirect（-1）时，这里能拦下不覆写。
//
// 绝不覆写的情形：
//   - 账号已删（a == nil）或库里已锁 proxyIDDirect；
//   - proxyID <= 0（自动写绑只认真实代理出口，不写 -1/0 哨兵）；
//   - 库里已有仍有效的启用代理绑定（a.ProxyID > 0 且 proxyUsableByID）——
//     粘性不能拿本轮撞到的别的出口打散。
func autoBindAccountProxy(accountID, proxyID int64) {
	if accountID <= 0 || proxyID <= 0 {
		return
	}
	claimMu.Lock()
	defer claimMu.Unlock()
	a := accountByID(accountID)
	if a == nil || a.ProxyID == proxyIDDirect {
		return // 已删 / 锁定直连：绝不覆写
	}
	if a.ProxyID > 0 && proxyUsableByID(a.ProxyID) {
		return // 已有仍有效的启用代理绑定：不打散粘性
	}
	bindAccountProxyLocked(accountID, proxyID)
}

// claimMu 把「查当前绑定 → 决定胜者 → 必要时写库」串成原子操作。
// 不加锁时两个并发请求同时拿到同一未绑定账号、各自 acquireSlot 选了不同出口，
// 发完 getXSRF 才先后写 bindAccountProxy——后写覆盖先写，这个号已经从两个 IP
// 外发过（strict_proxy_binding 想防的正是这个）。进程内一把锁就够（同 pickMu）。
var claimMu sync.Mutex

// claimAccountProxy 原子决定这个账号本次该走哪个出口（strict_proxy_binding 首绑仲裁）。
//   - 已绑定且绑定出口仍然存在且 enabled：返回绑定值，不覆写——熔断冷却中的绑定
//     照样算数（冷却会过），绝不能因为暂时不可用就覆写（否则并发请求会把一个
//     还在冷却的号的绑定抢改成自己的出口）；
//   - 绑定为 proxyIDDirect（锁定本机直连）：原样返回 proxyIDDirect，严禁覆写——
//     这是显式的「绝不走代理」承诺，仲裁也不能把它改成别的出口；
//   - 未绑定 / 绑定已失效（删除或禁用）：把 candidateProxyID 写库并返回它（本次请求胜出）。
//
// 调用方在拿到 slot 后、发任何出站请求之前调；返回值 != 手上的 picked.ID 时
// 必须释放手上的 slot、按返回值重新 acquireSlot——胜者是别的并发请求绑的。
func claimAccountProxy(accountID, candidateProxyID int64) int64 {
	claimMu.Lock()
	defer claimMu.Unlock()
	cur := int64(0)
	if a := accountByID(accountID); a != nil {
		cur = a.ProxyID
	}
	if cur == proxyIDDirect {
		return proxyIDDirect // 锁定直连：任何候选出口都不得覆写
	}
	if cur > 0 && proxyEnabledByID(cur) {
		return cur
	}
	bindAccountProxyLocked(accountID, candidateProxyID)
	return candidateProxyID
}

// accountDisplayName 面板/记录里显示的账号名，没填标签就用 #id。
func accountDisplayName(a *CookieAccount) string {
	if a.Label != "" {
		return a.Label
	}
	return fmt.Sprintf("#%d", a.ID)
}

// geminiCookieTargetHost 是主账号池 cookie 的目标主机：只有按 RFC 6265 对该主机生效
// 的 Set-Cookie 才允许合并进主账号池，防止跨主机（如 WebChannel 的
// geminiweb-pa.clients6.google.com）下发的 host-only 或 clients6 作用域 cookie 混入。
const geminiCookieTargetHost = "gemini.google.com"

// mergeSetCookieForHost 先按 RFC 6265 来源主机与目标主机过滤 setCookie，再合并进现有 cookie 串。
// 任一 host 缺失或非法时 fail-closed，不做合并。
func mergeSetCookieForHost(cookie string, setCookie []string, sourceURLOrHost, targetHost string) string {
	filtered := filterSetCookieForHost(setCookie, sourceURLOrHost, targetHost)
	if len(filtered) == 0 {
		return cookie
	}
	return mergeSetCookie(cookie, filtered)
}

// filterSetCookieForHost 仅保留来自 sourceURLOrHost 且按 RFC 6265 对 targetHost 根路径生效的 Set-Cookie：
//   - host-only（无 Domain 属性）仅在 sourceHost == targetHost 时保留；
//   - 带 Domain 属性时，sourceHost 与 targetHost 必须同时 domain-match 该 Domain（且拒绝单段顶级域）；
//   - __Host- 前缀 cookie 禁止带 Domain、仅允许同源且根路径；
//   - 跨主机时（如 geminiweb-pa.clients6.google.com -> gemini.google.com），拒绝 Partitioned
//     且要求显式 Path=/（否则按请求路径 /v1 默认作用域不覆盖目标根路径）。
func filterSetCookieForHost(setCookie []string, sourceURLOrHost, targetHost string) []string {
	if len(setCookie) == 0 {
		return nil
	}
	srcHost, srcPath := parseCookieSource(sourceURLOrHost)
	tgtHost, _ := parseCookieSource(targetHost)
	if srcHost == "" || tgtHost == "" {
		return nil
	}
	sameHost := srcHost == tgtHost
	defaultPath := cookieDefaultPath(srcPath)

	var out []string
	for _, sc := range setCookie {
		if setCookieAppliesToTarget(sc, srcHost, defaultPath, tgtHost, sameHost) {
			out = append(out, sc)
		}
	}
	return out
}

func parseCookieSource(raw string) (host, path string) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", ""
	}
	if strings.Contains(s, "://") {
		u, err := url.Parse(s)
		if err != nil {
			return "", ""
		}
		h := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
		return h, u.Path
	}
	// 裸 host（可选端口）：通过 dummy scheme 解析以可靠剥离端口与非法字符。
	u, err := url.Parse("https://" + s)
	if err != nil || u.Path != "" {
		return "", ""
	}
	h := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	return h, "/"
}

// cookieDefaultPath 按 RFC 6265 §5.1.4 计算请求路径的默认 cookie Path。
func cookieDefaultPath(reqPath string) string {
	if reqPath == "" || !strings.HasPrefix(reqPath, "/") {
		return "/"
	}
	i := strings.LastIndexByte(reqPath, '/')
	if i <= 0 {
		return "/"
	}
	return reqPath[:i]
}

// cookieDomainMatches 判断 host 是否按 RFC 6265 §5.1.3 匹配已规范化的 domain（无前导点、小写）。
func cookieDomainMatches(host, domain string) bool {
	return host == domain || strings.HasSuffix(host, "."+domain)
}

func setCookieAppliesToTarget(sc, srcHost, defaultPath, tgtHost string, sameHost bool) bool {
	parts := strings.Split(sc, ";")
	first := strings.TrimSpace(parts[0])
	eq := strings.IndexByte(first, '=')
	if eq <= 0 {
		return false
	}
	name := strings.TrimSpace(first[:eq])
	if name == "" {
		return false
	}

	var domainAttr, pathAttr string
	var hasDomain, hasPath, partitioned bool
	for _, rawAttr := range parts[1:] {
		attr := strings.TrimSpace(rawAttr)
		if attr == "" {
			continue
		}
		k, v, hasEq := strings.Cut(attr, "=")
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		switch {
		case strings.EqualFold(k, "domain") && hasEq:
			hasDomain = true
			domainAttr = v
		case strings.EqualFold(k, "path") && hasEq:
			hasPath = true
			pathAttr = v
		case strings.EqualFold(k, "partitioned"):
			partitioned = true
		}
	}

	if partitioned && !sameHost {
		return false
	}
	if strings.HasPrefix(name, "__Host-") {
		if hasDomain || !sameHost {
			return false
		}
		if hasPath && pathAttr != "/" {
			return false
		}
		return true
	}

	// Path 校验：目标池 cookie 会随所有 gemini.google.com 请求发出，非根路径 cookie 不应混入。
	if hasPath {
		if pathAttr != "/" {
			return false
		}
	} else if !sameHost && defaultPath != "/" {
		return false
	}

	if !hasDomain || domainAttr == "" {
		// host-only cookie：仅同主机适用。
		return sameHost
	}
	d := strings.TrimPrefix(strings.ToLower(strings.TrimSuffix(domainAttr, ".")), ".")
	if d == "" || !strings.Contains(d, ".") {
		return false
	}
	return cookieDomainMatches(srcHost, d) && cookieDomainMatches(tgtHost, d)
}

// mergeSetCookie 把响应下发的 Set-Cookie 合并进现有 cookie 串。
//
// 服务端几乎每个响应都在刷新 SIDCC / __Secure-1PSIDCC / __Secure-3PSIDCC
// （2 小时抓包里 batchexecute 就刷了 468 次），浏览器收下再带回去。一直发旧值的
// 客户端会被判定为过期会话 —— 实测不合并的号活一两小时就失效。
//
// 顺序保持原样、新键追加在后：cookie 顺序本身不影响语义，但保持稳定能让
// poolHasCookie 那类按内容比对的地方不至于每次都认成新值。
func mergeSetCookie(cookie string, setCookie []string) string {
	if len(setCookie) == 0 {
		return cookie
	}
	updates := map[string]string{}
	for _, sc := range setCookie {
		first := strings.TrimSpace(strings.SplitN(sc, ";", 2)[0])
		i := strings.Index(first, "=")
		if i <= 0 {
			continue
		}
		name, val := first[:i], first[i+1:]
		// 删除指令（过期时间在过去 + 空值）不能当成新值写进去
		if val == "" && strings.Contains(strings.ToLower(sc), "expires=thu, 01 jan 1970") {
			continue
		}
		updates[name] = val
	}
	if len(updates) == 0 {
		return cookie
	}
	var parts []string
	seen := map[string]bool{}
	for _, kv := range splitCookiePairs(cookie) {
		seen[kv[0]] = true
		if v, ok := updates[kv[0]]; ok {
			parts = append(parts, kv[0]+"="+v)
			continue
		}
		parts = append(parts, kv[0]+"="+kv[1])
	}
	for name, val := range updates {
		if !seen[name] {
			parts = append(parts, name+"="+val)
		}
	}
	return strings.Join(parts, "; ")
}

// cookieIdentity 取能代表"这是哪个账号"的那几项。
//
// SAPISID 用来算授权头、__Secure-1PSID 是会话主键，两者都不随刷新变化（2 小时
// 抓包里 0 次变动，变的只有 *SIDCC 和 *SIDTS 那两族）。所以它们变了就说明这份
// cookie 已经不是原来那个账号了。
func cookieIdentity(cookie string) string {
	var sapisid, psid string
	for _, kv := range splitCookiePairs(cookie) {
		switch kv[0] {
		case "SAPISID":
			sapisid = kv[1]
		case "__Secure-1PSID":
			psid = kv[1]
		}
	}
	return sapisid + "|" + psid
}

// updateAccountCookie 把刷新后的 cookie 写回账号。
//
// 写之前先比对身份：合并 Set-Cookie 时上游理论上可以把整套会话换掉（比如响应里
// 带了另一个账号的 SID），照单全收就等于把 A 号的凭据写进 B 号那一行。之后这个号
// 在面板上显示的还是原来的标签，实际发出去的却是别人的会话，而且**完全静默**。
// 身份对不上就不写，宁可让这次刷新白费。
//
// v6.6.0：串行经 acquireAcctCookieLock（L1）与 rotateAccountOpts / 浏览器
// buildAccountCtx 播种 / syncAccountCookiesBackCtx 写回严格互斥；写库走
// updateAccountCookieCASLocked（merged==cur 不发 UPDATE，变化时 CAS 条件更新）。
// 真实写库成功后 BumpAccountEpoch + 放锁 InvalidateAccount（`cookie rotated`）——
// 无变化快路径绝不空转误杀健康槽。
func updateAccountCookie(id int64, cookie string) {
	if id <= 0 || cookie == "" {
		return
	}
	unlock, err := acquireAcctCookieLock(context.Background(), id)
	if err != nil {
		return
	}
	var cur string
	if err := getDB().QueryRow(`SELECT cookie FROM accounts WHERE id=?`, id).Scan(&cur); err != nil {
		unlock()
		return
	}
	written := updateAccountCookieCASLocked(id, cur, cookie)
	// MF-6：真实写库成功后先在 acctCookieLock（L1）内 BumpAccountEpoch，
	// 杜绝新刷 Cookie 与并发回写交错时代际漂移；再放锁，最后 Invalidate。
	// R2-NH-3：Bump 无条件推进（不门控 shouldUseBrowserTransport），
	// Invalidate 仍按开关门控。
	bump := written && cookie != cur
	if bump {
		browserMgr.BumpAccountEpoch(id)
	}
	unlock()
	if bump && shouldUseBrowserTransport() {
		browserMgr.InvalidateAccount(id, false, "cookie rotated")
	}
}

// accountSyncResult 是 accountSync 的结果：动作是 added / updated / unchanged。
type accountSyncResult struct {
	ID     int64  `json:"id"`
	Action string `json:"action"`
}

// findSyncTargetAccountID 按「同会话 + 同账号序号 → 同标签 + 同账号序号」
// 在账号池里找 accountSync 要更新的那一行，找不到返回 0。
// 无锁：并发首推时调用方须先拿 accountSyncCreateMu 再重查（见 accountSync）。
func findSyncTargetAccountID(authuser int, ident, label string) int64 {
	accts := accountList()
	for i := range accts {
		if accts[i].AuthUser == authuser && cookieIdentity(accts[i].Cookie) == ident {
			return accts[i].ID
		}
	}
	if label == "" {
		return 0
	}
	for i := range accts {
		if accts[i].AuthUser == authuser && strings.EqualFold(strings.TrimSpace(accts[i].Label), label) {
			return accts[i].ID
		}
	}
	return 0
}

// accountSyncCreateMu 把并发首推的「查重 → 插入」串成原子操作。
// 不加它时两个并发同步会同时查得 hitID==0 各自 accountInsert：accounts 表
// 三种方言都没有唯一约束兜底，同 label/authuser/会话的重复 enabled 账号
// 就进了库，之后匹配只 break 到首行，另一行成了不可维护的额外登录号。
// 进程内一把锁就够（同 pickMu）：本反代天生单实例，同步端点只有本进程入口。
//
// 锁序红线：acquireAcctCookieLock（L1）只在**释放本锁之后**才允许获取，
// 绝不持本锁去拿 L1，防锁序反转。
var accountSyncCreateMu sync.Mutex

// accountSync 是浏览器扩展定时推 cookie 用的「有就更新、没有就新建」。
//
// 找哪一行，按顺序：
//  1. 同一会话（cookieIdentity：SAPISID + __Secure-1PSID）且同一账号序号 —— 正常的定时刷新
//  2. 同标签（不分大小写、非空）且同一账号序号 —— 用户重新登录过，会话换了但还是这个号
//  3. 都没有就新建
//
// 账号序号必须一起比：同一串 cookie 可以按不同 /u/N 导入成几个号，
// 只比会话会把 /u/1 的刷新写进 /u/0 那行。
//
// v6.7.0 适配 v6.6.0 锁序：命中更新时走 acquireAcctCookieLock（L1）持锁
// 快照→写库→BumpAccountEpoch 三段，再放锁后 invalidateXSRF + 清短冷却 +
// InvalidateAccount——与 updateAccountCookie / rotateAccountOpts 严格互斥，
// 绝不误杀健康 BrowserContext（unchanged 快路径零副作用），也绝不覆写原账号的
// proxy_id 与 priority。
func accountSync(label string, authuser int, cookie string) (accountSyncResult, error) {
	if authuser < 0 || authuser > maxAuthUser {
		return accountSyncResult{}, fmt.Errorf("账号序号要在 0..%d 之间", maxAuthUser)
	}
	c, ok := normalizeCookie(cookie, "扩展推送的 cookie")
	if !ok {
		return accountSyncResult{}, fmt.Errorf("cookie 解析失败：既不是 \"k=v; k=v\" 串，也不是可识别的 JSON")
	}
	if extractSAPISID(c) == "" {
		return accountSyncResult{}, fmt.Errorf("cookie 里没有 SAPISID，浏览器可能没登录 Google")
	}
	label = strings.TrimSpace(label)
	warnMissingOSID(label, c)

	ident := cookieIdentity(c)
	hitID := findSyncTargetAccountID(authuser, ident, label)
	if hitID == 0 {
		// 首次匹配落空：拿进程级互斥再查一次。并发胜者在锁外先插好，
		// 败者进锁后重查必命中同一行，转走下方的 L1 更新/unchanged 路径。
		accountSyncCreateMu.Lock()
		hitID = findSyncTargetAccountID(authuser, ident, label)
		if hitID == 0 {
			id, err := accountInsert(label, c, "由浏览器扩展同步")
			if err != nil {
				accountSyncCreateMu.Unlock()
				return accountSyncResult{}, err
			}
			if authuser != 0 {
				if err := accountSetAuthUser(id, authuser); err != nil {
					accountSyncCreateMu.Unlock()
					return accountSyncResult{}, err
				}
			}
			accountSyncCreateMu.Unlock()
			return accountSyncResult{ID: id, Action: "added"}, nil
		}
		// 锁内重查命中并发刚插入的行：立即释放 accountSyncCreateMu，
		// 绝不持它去拿 acquireAcctCookieLock（锁序反转红线）。
		accountSyncCreateMu.Unlock()
	}

	// L1 串行锁内做「重读快照 → 判定 → 写库 → Bump」：与懒建 Context 播种、
	// syncAccountCookiesBackCtx 回写、rotateAccountOpts、updateAccountCookie 严格互斥，
	// 拿陈旧快照比较会把刚刷新的 cookie 覆盖回去。
	unlock, err := acquireAcctCookieLock(context.Background(), hitID)
	if err != nil {
		return accountSyncResult{}, err
	}
	cur := accountByID(hitID)
	if cur == nil {
		unlock()
		return accountSyncResult{}, fmt.Errorf("账号 #%d 已删除", hitID)
	}
	// reenable：只对**自动停用**（auto_disabled=1，fail_count 到阈值由
	// autoDisableIfDead 写入）的号恢复——它停用的原因就是 cookie 失效；
	// 管理员手动停用（auto_disabled=0，即使 fail_count>=3）绝不动。
	reenable := cur.Status == "disabled" && cur.AutoDisabled
	// unchanged 快路径：cookie 完全未变、无失败计数、无需恢复——零写库零 Bump
	// 零失效，绝不空转误杀健康槽。
	if cur.Cookie == c && cur.FailCount == 0 && !reenable {
		unlock()
		return accountSyncResult{ID: hitID, Action: "unchanged"}, nil
	}
	status := cur.Status
	if reenable {
		status = "enabled"
	}
	// 保留 proxy_id 与 priority：UPDATE 只写
	// cookie/fail_count/last_error/status/auto_disabled（新凭据在手，
	// 停用来源记一律清零）。
	if _, err := getDB().Exec(
		`UPDATE accounts SET cookie=?, fail_count=0, last_error='', status=?, auto_disabled=0 WHERE id=?`,
		c, status, hitID); err != nil {
		unlock()
		return accountSyncResult{}, err
	}
	// MF-6：真实写库成功后在 L1 内 BumpAccountEpoch（无条件推进，不门控
	// shouldUseBrowserTransport），杜绝新 cookie 与并发回写交错时代际漂移。
	browserMgr.BumpAccountEpoch(hitID)
	unlock()

	if reenable {
		logf("[cookie] 账号 #%d 收到新 cookie，已从自动停用中恢复", hitID)
	}
	// 新旧两份的 XSRF 缓存都作废：新串等懒建后重抓，旧串绝不再信。
	// 顺手清掉 45s 防雪崩短冷却——换上新 cookie 的号不该再背着旧凭据的失败记录。
	invalidateXSRF(cur.Cookie)
	invalidateXSRF(c)
	recordAccountReqSuccess(hitID, nextClientReqID())
	if shouldUseBrowserTransport() {
		browserMgr.InvalidateAccount(hitID, false, "cookie synced")
	}
	return accountSyncResult{ID: hitID, Action: "updated"}, nil
}

// xsrfErrBlamesCookie 判断取 XSRF（抓 /app）失败是不是 cookie 的错。
//
// 只有这几种算：页面打开了但没有 SNlM0e（被当匿名）、401/403、3xx 跳到
// accounts.google.com（登录页）。网络层错误（EOF、超时、代理连不上）、跳
// /sorry、429、5xx 都是出口或上游的问题，算进 fail_count 会让好号在断网或
// 代理抖动时连续 3 次被自动停用。
func xsrfErrBlamesCookie(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "no SNlM0e") {
		return true
	}
	var se *appPageStatusError
	if !errors.As(err, &se) {
		return false
	}
	switch {
	case se.Status == 401 || se.Status == 403:
		return true
	case se.Status >= 300 && se.Status < 400:
		return strings.HasPrefix(se.Location, "accounts.google.com")
	}
	return false
}
