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

// cookieValuesForName 从一整串 cookie 里按出现顺序取指定名字的**全部**
// 值（R11-8，Oracle B2）：重名多项保序返回；缺失返回 nil。与
// cookieValuesEqual 同口径——同名多项的存在性与逐项值都是净变化判定的
// 一部分，只认首项会把 `SIDCC=n; SIDCC=y` 被上游统一成 `SIDCC=n` 的
// 真实尾项更新误判为净零（旧票 y 残留 DB 永不更新）。
func cookieValuesForName(cookie, name string) []string {
	var out []string
	for _, kv := range splitCookiePairs(cookie) {
		if kv[0] == name {
			out = append(out, kv[1])
		}
	}
	return out
}

// cookieNameValuesEqual 判定两份 cookie 串中指定名字的完整有序值列表
// 是否逐项相等（R11-8）：缺失与空值严格区分、重名多项逐项比对。
func cookieNameValuesEqual(a, b, name string) bool {
	va, vb := cookieValuesForName(a, name), cookieValuesForName(b, name)
	if len(va) != len(vb) {
		return false
	}
	for i := range va {
		if va[i] != vb[i] {
			return false
		}
	}
	return true
}

// extractSAPISID 从一整串 cookie 里取 SAPISID 的值，取不到返回空串。
func extractSAPISID(cookie string) string {
	return cookieValue(cookie, "SAPISID")
}

// cookieFirstNonEmptyValue 按「首个非空值」口径取指定名字的值（R12-6，
// Oracle S2/B4）：与 cookieIdentity / cookieSubset / cookieToCDP 的身份
// 主键口径严格一致——重名多项时跳过空值项取第一个非空值；全部为空或
// 缺失返回空串。缺票预检（换发 `__Secure-1PSID` 存在性判定）必须用本
// 口径：首空后有效的票据与换发请求实际带出的子集是同一项，绝不能按
// 只取首项的 cookieValue 误判缺失而整轮静默跳过。
func cookieFirstNonEmptyValue(cookie, name string) string {
	for _, kv := range splitCookiePairs(cookie) {
		if kv[0] == name && kv[1] != "" {
			return kv[1]
		}
	}
	return ""
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

// accountInsertWithAuthUser 一步建号：与 accountInsert 相同校验，但单条
// INSERT 直接带上 authuser——消除「先按 /u/0 插入再补序号」的半成品窗口
// （R2 consider），窗口内新行可能被挑号器按 /u/0 挑中。
func accountInsertWithAuthUser(label, cookie, note string, authuser int) (int64, error) {
	cookie = strings.TrimSpace(cookie)
	if cookie == "" {
		return 0, fmt.Errorf("cookie 不能为空")
	}
	if authuser < 0 || authuser > maxAuthUser {
		return 0, fmt.Errorf("账号序号要在 0..%d 之间，收到 %d", maxAuthUser, authuser)
	}
	return insertID(
		`INSERT INTO accounts(label, cookie, status, note, created_at, authuser) VALUES (?,?,?,?,?,?)`,
		strings.TrimSpace(label), cookie, "enabled", strings.TrimSpace(note), time.Now().Unix(), authuser)
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
// R8-3：连带清除可核自动绑定来源记录（行已不存在，来源事实一并失效）。
func accountDelete(id int64) error {
	_, err := getDB().Exec(`DELETE FROM accounts WHERE id=?`, id)
	clearAutoBoundProxy(id)
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
	// R6-1：面板按 ID 检测（允许停用号）——已知的 a.ID 透传到底，纯 Go
	// 回收写回只校验该行；A 停用而 B 同身份同序号启用时绝不把 A 的
	// 响应反查写进 B。
	invalidateXSRF(a.Cookie)
	_, err = getXSRFSwitchForAccount(a.Cookie, a.AuthUser, proxyURL, false, a.ID)
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
// R7-3/R7-4（Reviewer MF-3/MF-4）：严格提交门控——Exec 出错或未写中任何
// 行（RowsAffected==0：账号已删等）返回 false，调用方（autoBindAccountProxy
// 等）绝不把未真实落库的绑定事实同步进驻留槽内存。
// R8-5（Reviewer MF-5）：RowsAffected 自身出错同样返回 false——「驱动拿不到
// 影响行数」不是已写中目标行的证据，绝不把不明行数当作成功。
func bindAccountProxyLocked(accountID, proxyID int64) bool {
	if accountID <= 0 {
		return false
	}
	res, err := getDB().Exec(`UPDATE accounts SET proxy_id=? WHERE id=?`, proxyID, accountID)
	if err != nil {
		logf("[cookie] 账号 #%d 绑定出口 #%d 落库失败: %v", accountID, proxyID, err)
		return false
	}
	n, err := res.RowsAffected()
	if err != nil {
		logf("[cookie] 账号 #%d 绑定出口 #%d 影响行数不可知，按未写中处理: %v", accountID, proxyID, err)
		return false
	}
	return n > 0
}

// autoBoundRecord 是一条可核的自动绑定来源事实（R8-3，Reviewer MF-3）：
// 记录某账号最近一次经**自动绑定通道**（autoBindAccountProxy/claimAccountProxy
// 首绑）真实落库的 (proxyID, proxyURL)。用途只有一个：冷建槽步骤 6 对齐与
// exitCompatCheck 快照一致性复核时区分「并发自动绑定同步」与「管理员手工
// PATCH 改绑」——手工 PATCH（bindAccountProxy）与账号删除会清除记录，
// 管理员改绑到恰好等于溢出出口的代理时记录绝不命中，步骤 6 绝不对齐、
// 下轮保活原样 fail-closed。
type autoBoundRecord struct {
	proxyID  int64
	proxyURL string
}

// autoBoundProxyByAcct 由 claimMu 保护的 per-account 内存映射。
var autoBoundProxyByAcct = map[int64]autoBoundRecord{}

// isAutoBoundProxyForURL 只读查询：accountID 最近一次可核自动绑定是否恰好
// 是 (proxyID, proxyURL)（短持 claimMu，绝不持 m.mu 调用）。
func isAutoBoundProxyForURL(accountID, proxyID int64, proxyURL string) bool {
	if accountID <= 0 || proxyID <= 0 || proxyURL == "" {
		return false
	}
	claimMu.Lock()
	defer claimMu.Unlock()
	rec, ok := autoBoundProxyByAcct[accountID]
	return ok && rec.proxyID == proxyID && rec.proxyURL == proxyURL
}

// noteAutoBoundProxyLocked 登记一条可核自动绑定事实。调用方持 claimMu。
func noteAutoBoundProxyLocked(accountID, proxyID int64, proxyURL string) {
	if accountID <= 0 || proxyID <= 0 || proxyURL == "" {
		return
	}
	autoBoundProxyByAcct[accountID] = autoBoundRecord{proxyID: proxyID, proxyURL: proxyURL}
}

// clearAutoBoundProxy 清除该账号的自动绑定来源记录（管理员手工 PATCH/POST
// 改绑与账号删除调用）：之后的步骤 6 对齐与快照一致性复核绝不把它当作
// 自动绑定放行。内部自持 claimMu（调用方不得持有）。
func clearAutoBoundProxy(accountID int64) {
	if accountID <= 0 {
		return
	}
	claimMu.Lock()
	delete(autoBoundProxyByAcct, accountID)
	claimMu.Unlock()
}

// clearAutoBoundProxyLocked 是 clearAutoBoundProxy 的锁内版（调用方持 claimMu）。
func clearAutoBoundProxyLocked(accountID int64) {
	delete(autoBoundProxyByAcct, accountID)
}

// bindAccountProxy 是锁外入口（管理员 PATCH/POST 改绑、非严格模式的首绑/重绑落库）：
// 加 claimMu 后与 claimAccountProxy 完全互斥——手动改绑不能插在「核库→写库」之间。
// R8-3：手工改绑同时清除该账号的可核自动绑定来源记录——之后的冷建对齐与
// 快照一致性复核绝不把人工改绑当作自动绑定放行。
func bindAccountProxy(accountID, proxyID int64) {
	if accountID <= 0 {
		return
	}
	claimMu.Lock()
	defer claimMu.Unlock()
	clearAutoBoundProxyLocked(accountID)
	if bindAccountProxyLocked(accountID, proxyID) {
		// R12-2-M1：手工绑定写真实提交后递增驻留槽改绑代际——热注入
		// 授权复核与提交注入之间的本次改绑必然被检出（锁序 claimMu
		// -> m.mu 与 SyncAutoBoundProxy 一致）。
		browserMgr.NoteAccountRebindCommitted(accountID)
	}
}

// rebindAccountProxyAtomic 是管理员手工 PATCH 改绑的串行化原子版（R10-2，
// Reviewer M5/Oracle M5）：「读取实际旧绑定 + 写入新绑定 + 清除自动来源
// 记录」放进同一个 claimMu 临界区，返回该串行点上的**实际**旧绑定。
// 调用方按「实际旧值 != 新值」决定是否触发槽位失效摘除——修复前 PATCH
// 在进入 claimMu 之前提前读 before 快照算 proxyChanged：等待期间并发
// 配额自动重绑提交后，PATCH 把 DB 从 P2 实际改回 P1 却按早先的
// proxyChanged=false 跳过失效，旧 P2 槽及在途流继续存活。串行化后判定
// 永远基于真实发生的改写，同值 PATCH（old == new）依旧不拆槽。
//
// R11-6（Oracle A7/Reviewer #5）：严格提交门控——仅当 UPDATE 真实命中
// 目标行（Exec 无错且 RowsAffected > 0）才算改绑提交成功：先写库、确认
// 成功后才清除自动来源记录并返回 updated=true。行不存在（并发 DELETE）
// 或写库出错时返回 updated=false 与错误，调用方如实回面板错误且**绝不**
// 按「已改绑」拆槽/中断在途流——修复前丢弃 bindAccountProxyLocked 的
// false/错误且提前清来源：DELETE 抢先删行后绑定 UPDATE 命中 0 行，单字段
// PATCH 仍回 200 ok:true 并拆掉健康槽，DB 实际从未改绑。
func rebindAccountProxyAtomic(accountID, proxyID int64) (oldProxyID int64, updated bool, err error) {
	if accountID <= 0 {
		return 0, false, fmt.Errorf("account id required")
	}
	claimMu.Lock()
	defer claimMu.Unlock()
	// R12-5（Oracle S1/Reviewer #7）同值幂等：先在 claimMu 临界区内查询
	// 目标行是否存在与当前实际旧绑定，按三种事实分支裁决——
	//   - 行不存在（并发 DELETE）：明确 not found 错误，不清来源不拆槽；
	//   - 行存在且旧值==新值：合法同值幂等成功——不依赖 MySQL 默认
	//     clientFoundRows=false 下同值 UPDATE 的 RowsAffected()==0（它
	//     返回的是**实际改变**行数而非匹配行数，同值 UPDATE 合法却命中
	//     0 行被误当不存在回 400），直接清除残留自动来源记录返回成功；
	//   - 行存在且值变更：执行 UPDATE 并严格确认命中后才清来源返回成功。
	a := accountByID(accountID)
	if a == nil {
		return 0, false, fmt.Errorf(
			"账号 #%d 不存在（未执行任何改绑）", accountID)
	}
	oldProxyID = a.ProxyID
	if oldProxyID == proxyID {
		clearAutoBoundProxyLocked(accountID)
		return oldProxyID, true, nil
	}
	// 严格提交门控：写库未真实命中目标行（0 行：行已被并发删除；或
	// Exec 出错/行数不明）时改绑事实不成立——不清除自动来源记录、
	// 不向调用方谎报已改绑。
	if !bindAccountProxyLocked(accountID, proxyID) {
		return oldProxyID, false, fmt.Errorf(
			"账号 #%d 绑定出口 #%d 落库失败或账号行已不存在（未执行任何改绑）", accountID, proxyID)
	}
	clearAutoBoundProxyLocked(accountID)
	// R12-2-M1：改绑真实提交后递增驻留槽改绑代际（手工改绑必摘槽，
	// 代际递增使提交前热注入复核绑定事实必然过期——双保险）。
	browserMgr.NoteAccountRebindCommitted(accountID)
	return oldProxyID, true, nil
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
	// R7-3/R7-4：严格提交门控——只有真实写中目标行（Exec 无错且
	// RowsAffected > 0）才允许同步驻留槽内存事实；写库失败/0 行命中时
	// 绝不把未落库的绑定事实写进内存（否则 DB 与槽事实永久不一致）。
	if !bindAccountProxyLocked(accountID, proxyID) {
		return
	}
	// R12-2-M1：自动绑定真实提交后递增驻留槽改绑代际——驻留槽实际出口
	// 与新绑定不符时 SyncAutoBoundProxy 不动槽事实，热注入授权复核仅靠
	// (bcID, exitSyncVer) 对该交错不可见；rebindGen 使其必然被检出。
	browserMgr.NoteAccountRebindCommitted(accountID)
	// R8-3（Reviewer MF-3）：真实写中目标行后才登记可核的自动绑定来源——
	// 冷建槽步骤 6 对齐与 exitCompatCheck 快照一致性复核只认这条记录
	//（管理员手工 PATCH 会清掉它，绝不命中）。
	url := proxyURLByID(proxyID)
	noteAutoBoundProxyLocked(accountID, proxyID, url)
	// R6-4（Reviewer MF-4）：合法自动首绑（0 -> P）/合法自动重绑
	//（P旧 -> P新）落库后，把实际出口与新绑定一致的驻留槽绑定事实
	// 同步更新——下轮保活续期照常放行；管理员手工 PATCH 改绑不经此
	// 入口，驻留槽 orig 保持旧值仍严格拦截（负对照不变）。
	// 锁序：claimMu -> m.mu（m.mu 出度为 0，无任何反向获取路径）。
	if shouldUseBrowserTransport() && url != "" {
		browserMgr.SyncAutoBoundProxy(accountID, proxyID, url)
	}
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
// 返回 (winner, ok)：R8-5（Reviewer MF-5）严格提交门控贯穿仲裁调用方——
// 写库未真实命中目标行（0 行：账号在核库与 UPDATE 之间被并发删除等；或
// RowsAffected 出错的不明行数）时 ok=false，调用方绝不把候选出口当作
// 已赢得仲裁（严格仲裁须显式传播失败/释放槽位并拒绝继续外发，绝不能
// 落到匿名/直连）。ok=false 时 winner 无意义。
//
// 调用方在拿到 slot 后、发任何出站请求之前调；返回值 != 手上的 picked.ID 时
// 必须释放手上的 slot、按返回值重新 acquireSlot——胜者是别的并发请求绑的。
func claimAccountProxy(accountID, candidateProxyID int64) (int64, bool) {
	claimMu.Lock()
	defer claimMu.Unlock()
	cur := int64(0)
	if a := accountByID(accountID); a != nil {
		cur = a.ProxyID
	}
	if cur == proxyIDDirect {
		return proxyIDDirect, true // 锁定直连：任何候选出口都不得覆写
	}
	if cur > 0 && proxyEnabledByID(cur) {
		return cur, true
	}
	// R8-5：写库真实命中才算本次请求胜出——0 行/行数不明时不返回候选代理，
	// 严格仲裁调用方据此 fail-closed（释放槽位拒绝外发）。
	if !bindAccountProxyLocked(accountID, candidateProxyID) {
		return 0, false
	}
	// R8-3：严格模式首绑同样是自动绑定通道——登记可核来源（proxyID>0 且
	// URL 非空才登记；直连候选不产生可核代理绑定事实）。
	url := proxyURLByID(candidateProxyID)
	noteAutoBoundProxyLocked(accountID, candidateProxyID, url)
	// R12-2-M1：仲裁绑定真实提交后递增驻留槽改绑代际（热注入复核
	// 与提交之间的本次首绑必然被检出）。
	browserMgr.NoteAccountRebindCommitted(accountID)
	// R11-1（Oracle A1）：严格仲裁写库成功后必须同步推进驻留槽事实——
	// 此前严格首绑登记来源后直接返回、不调 SyncAutoBoundProxy，驻留槽的
	// exitSyncVer 不推进：复核已确认漂移、暂停在版本值守卫之前的请求，
	// 在严格仲裁把账号实际绑到本槽出口（合法恢复）之后，仍按旧版本提交
	// 摘除，误杀刚恢复的合法槽并终止在途流。与非严格 autoBindAccountProxy
	// 完全同构：仅当驻留槽实际出口正是候选出口时同步 orig/bound 并递增
	// exitSyncVer（版本失配使悬挂的摘除决定自动放弃）。锁序合法：
	// claimMu → m.mu（m.mu 出度为 0，与 autoBindAccountProxy 同序，无
	// m.mu → claimMu 反取路径）。
	if shouldUseBrowserTransport() && url != "" {
		browserMgr.SyncAutoBoundProxy(accountID, candidateProxyID, url)
	}
	return candidateProxyID, true
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

// cookieValuesEqual 按解析后的实际有序 (name, value) 列表逐项比对两份
// Cookie 串是否等价（R9-7 初版、R10-8 修正，Reviewer S2）：仅忽略纯
// 分隔符格式差异（"; " 与 ";" / 首尾空白），其余差异一律算变化：
//   - 空值键 k= 与缺失键严格区分（mergeSetCookie 会保留非过期的空值
//     键、cookieToCDP 也会播种空值——二者混同会让 Jar 新增的 k= 静默
//     丢失写库）；
//   - 同名多项全部保留逐项比对（裸串导入允许重名：DB k=x;k=y、Jar
//     k=x 合并后两项均为 x，只认首值会误判净零跳过写库，尾部旧票永
//     不更新）；
//   - 键序差异算变化：mergeSetCookie 合并本身保序（cur 顺序 + 新键
//     追加），纯重排在真实合并路径不存在，线性比较即充分（Reviewer
//     S2 判据）。
func cookieValuesEqual(a, b string) bool {
	if a == b {
		return true
	}
	pa, pb := splitCookiePairs(a), splitCookiePairs(b)
	if len(pa) != len(pb) {
		return false
	}
	for i := range pa {
		if pa[i] != pb[i] {
			return false
		}
	}
	return true
}

// cookieIdentity 取能代表"这是哪个账号"的那几项。
//
// SAPISID 用来算授权头、__Secure-1PSID 是会话主键，两者都不随刷新变化（2 小时
// 抓包里 0 次变动，变的只有 *SIDCC 和 *SIDTS 那两族）。所以它们变了就说明这份
// cookie 已经不是原来那个账号了。
//
// R11-8（Oracle B4）重复主键口径统一：重名多项取**首个非空值**，与
// cookieSubset（换发请求实际带出的子集——seen 去重后只有首个非空项被发出）
// 严格一致。此前 cookieIdentity 取末项而 cookieSubset 取首个非空项：输入
// `SAPISID=x; SAPISID=y` 时授权身份按 y 裁决，实际送出的却是 x——同一身份
// 键的共享缓存与热注入可能领取不对应自己会话的票。统一后同一份输入在
// 「身份裁决」与「实际换发请求」两侧永远指向同一个值；首项非空时口径与
// 末项无关，正常（无重复）输入行为完全不变。
func cookieIdentity(cookie string) string {
	var sapisid, psid string
	for _, kv := range splitCookiePairs(cookie) {
		switch kv[0] {
		case "SAPISID":
			if sapisid == "" {
				sapisid = kv[1]
			}
		case "__Secure-1PSID":
			if psid == "" {
				psid = kv[1]
			}
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

// mergeAccountSetCookie 把一次响应的增量 Set-Cookie 并发安全地合并进账号
// Cookie 并落库（替代调用方在锁外算好整串再 updateAccountCookie 的旧路径）。
//
// 为什么必须存在：旧路径在流开始前取快照、流结束锁外算 merged 再整串覆写——
// 流持续期间后台保活刚刷新过 DB 中的 __Secure-1PSIDTS 时，整串覆写会把它
// 倒退回旧值（丢失刚续期的短命票）。本函数在 L1 acctCookieLock 内重读 DB
// 最新 cur、在锁内基于最新 cur 增量合并，只更新本次 Set-Cookie 实际下发的键，
// 绝不触碰并发刷新的其它键。
//
// expectedIdentity 是**请求发出时**的 cookieIdentity（调用方在发出请求前从
// 当时的 Cookie 算出）：长流持续期间同 ID 账号可能已被 accountSync 换绑到
// 另一个会话——此时 cookieIdentity(cur) 已变，本次响应的增量（旧会话 A 的
// SIDCC）绝不能合并进已换绑为会话 B 的同 ID 账号，直接丢弃整个回写。
// 空串与 "|" 视为无可核身份（R7-2，Reviewer MF-2）：整个跳过写回——
// --cookie-file 等缺身份字段的合法旧配置在等待写回期间可能已被换绑到新
// 会话，无可核身份就没有任何手段分辨「旧会话派生票据」与「新会话票据」，
// fail-closed 绝不碰 DB（导入/检测旧配置的兼容读不受影响）。
//
// R6-2（Reviewer MF-2）：expectedAuthuser 是**请求发出时**的账号序号
// （调用方按 /u/N 发出，恒知该值）：纯 Go 长流在飞期间管理员 PATCH 只改
// 序号不改 cookie 时，/u/0 的 SIDCC 绝不能写进已变为 /u/1 的同一行——
// L1 锁内重读 cur.AuthUser 校验，且 SQL CAS 同样追加 authuser 谓词。
// 与 mergeAccountSetCookieIfNoActiveBrowser 对齐。
//
// 仅在 merged != cur 且 CAS 写库真实成功时 BumpAccountEpoch + 放锁后
// InvalidateAccount（同 updateAccountCookie 的既有语义）。
func mergeAccountSetCookie(id int64, setCookie []string, sourceURL, expectedIdentity string, expectedAuthuser int) {
	if id <= 0 || len(setCookie) == 0 {
		return
	}
	// R7-2：无可核身份一律跳过写回——旧配置缺身份串的票绝不能污染
	// 同 ID 换绑后的新会话。
	if expectedIdentity == "" || expectedIdentity == "|" {
		return
	}
	unlock, err := acquireAcctCookieLock(context.Background(), id)
	if err != nil {
		return
	}
	cur := accountByID(id)
	if cur == nil || cur.Status != "enabled" {
		unlock()
		return
	}
	// 请求身份守卫：发请求时的身份与 L1 内重读的最新身份不符——会话已换绑，
	// 本次旧会话派生票据绝不写回新会话所在行。
	if cookieIdentity(cur.Cookie) != expectedIdentity {
		unlock()
		return
	}
	// R6-2 请求序号守卫：持锁等待/长流期间管理员 PATCH 改了 authuser 时
	// 整个跳过（/u/0 的票据绝不写进已变为 /u/1 的账号）。
	if cur.AuthUser != expectedAuthuser {
		unlock()
		return
	}
	merged := mergeSetCookieForHost(cur.Cookie, setCookie, sourceURL, geminiCookieTargetHost)
	if merged == cur.Cookie {
		unlock()
		return
	}
	written := updateAccountCookieCASLockedAuthuser(id, cur.Cookie, merged, expectedAuthuser)
	bump := written
	if bump {
		browserMgr.BumpAccountEpoch(id)
	}
	unlock()
	if bump && shouldUseBrowserTransport() {
		browserMgr.InvalidateAccount(id, false, "cookie rotated")
	}
}

// mergeAccountSetCookieIfNoActiveBrowser 是辅助 /app 回收路径专用的严格守卫
// 增量写回（fix #4）：把「同账号/同 cookieIdentity 无任何活跃或 creating
// 浏览器槽」的原子检查与写库放进同一个 L1 临界区，消除锁外
// HasActiveAccountOrIdentity 检查与 L1 内写库之间的 TOCTOU 窗口——检查与
// 写库之间绝不可能插进一个已注册未播种的建槽。
//
// 守卫命中（存在活跃/creating 槽）时整个跳过：不写库、不 BumpAccountEpoch、
// 不 InvalidateAccount——该号 jar 由 SyncAccountCookiesBack 负责。守卫放行时
// 才走 mergeAccountSetCookie 同构的增量合并 + CAS 写库 + BumpAccountEpoch；
// 刻意**不** InvalidateAccount：此刻无槽可拆，而检查之后才新建的槽播种的
// 已是本次写库后的新 Cookie，绝不能按 ID 无条件拆掉它。锁序遵守 L1→L2：
// acctCookieLock 内短持 browserMgr.mu（m.mu 出度为 0，无反向获取）。
func mergeAccountSetCookieIfNoActiveBrowser(id int64, setCookie []string, sourceURL, expectedIdentity string, expectedAuthuser int) {
	if id <= 0 || len(setCookie) == 0 {
		return
	}
	// R7-2（Reviewer MF-2）：无可核身份（""/"|"）一律跳过写回——缺身份
	// 字段的旧配置（--cookie-file 合法接纳）在等待写回期间被换绑到新会话时，
	// 没有任何手段分辨新旧会话票据，fail-closed 绝不把旧票落进新会话行。
	if expectedIdentity == "" || expectedIdentity == "|" {
		return
	}
	unlock, err := acquireAcctCookieLock(context.Background(), id)
	if err != nil {
		return
	}
	cur := accountByID(id)
	if cur == nil || cur.Status != "enabled" {
		unlock()
		return
	}
	if cookieIdentity(cur.Cookie) != expectedIdentity {
		unlock()
		return
	}
	// R5-3（Reviewer MF-3）：持锁等待期间管理员可能已 PATCH 改了
	// authuser——锁内重读的 cur.AuthUser 与本次请求点名的序号不符时
	// 整个跳过：/u/0 的票据绝不能写进已变为 /u/1 的账号。
	if cur.AuthUser != expectedAuthuser {
		unlock()
		return
	}
	// 原子守卫：L1 临界区内检查**本账号自身**的活跃与 creating 槽——锁外检查
	// 与 L1 内写库之间的建槽注册窗口在此被彻底关闭。
	// Oracle MF-1：守卫粒度必须与写库粒度一致（都只针对本账号 id 自身）。
	// 同身份兄弟号有活跃槽绝不是本账号跳过写库的理由——本账号的 SIDCC 只能
	// 由它自己的回收路径落库，否则兄弟号驻留会让本账号的新票永久不进 DB
	// （静默失效）。
	if shouldUseBrowserTransport() && browserMgr.HasActiveOrCreatingAccount(id) {
		unlock()
		return
	}
	merged := mergeSetCookieForHost(cur.Cookie, setCookie, sourceURL, geminiCookieTargetHost)
	if merged == cur.Cookie {
		unlock()
		return
	}
	// R5-3：SQL CAS 同样带 authuser 谓词——上游 /app I/O 发生在持锁之前，
	// 等待 L1 期间管理员 PATCH 改了 authuser 的行 0 行命中，绝不落库。
	written := updateAccountCookieCASLockedAuthuser(id, cur.Cookie, merged, expectedAuthuser)
	if written {
		browserMgr.BumpAccountEpoch(id)
		invalidateXSRF(cur.Cookie)
	}
	unlock()
	// 绝不 InvalidateAccount：守卫已确认无活跃/creating 槽，检查之后新建的槽
	// 播种的是新 Cookie，按 ID 拆槽只会误杀它。
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
			// 首推建号一步带入 authuser：不再「先插 /u/0 再补序号」，
			// authuser!=0 的新号没有以 /u/0 短暂暴露给挑号器的窗口。
			id, err := accountInsertWithAuthUser(label, c, "由浏览器扩展同步", authuser)
			if err != nil {
				accountSyncCreateMu.Unlock()
				return accountSyncResult{}, err
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
	// reenable 快照判定只服务 unchanged 快路径：只对**自动停用**
	// （auto_disabled=1，fail_count 到阈值由 autoDisableIfDead 写入）的号
	// 才可能恢复——它停用的原因就是 cookie 失效；管理员手动停用
	// （auto_disabled=0，即使 fail_count>=3）绝不动。真正的恢复写不由这份
	// 快照决定，而是下推给下方条件 UPDATE 原子完成。
	reenable := cur.Status == "disabled" && cur.AutoDisabled
	// unchanged 快路径：cookie 完全未变、无失败计数、无需恢复——零写库零 Bump
	// 零失效，绝不空转误杀健康槽。
	if cur.Cookie == c && cur.FailCount == 0 && !reenable {
		unlock()
		return accountSyncResult{ID: hitID, Action: "unchanged"}, nil
	}
	// R2 丢失更新修复：状态恢复判定彻底下推数据库原子条件更新，不在 Go 内存
	// 拿 cur.Status 盲写回库——accountSetStatus 不经 L1，快照之后落进来的
	// 手动停用/启用绝不能被本快照覆盖。
	// R3：两条 UPDATE 装进一个事务，任一步失败整体回滚——不留「cookie 已写
	// 库却未 Bump 未失效」的部分写入态（它不自愈：重推同串会走 unchanged
	// 快路径，旧 Context 与旧代际永远留着）。事务内只放这两条 UPDATE，
	// 不放 SELECT，避免 SQLite SHARED→RESERVED 锁升级死锁。
	// 第一条只写凭据与错误计数，完全不碰 status/auto_disabled；
	// proxy_id 与 priority 同样保留不动。
	tx, err := getDB().Begin()
	if err != nil {
		unlock()
		return accountSyncResult{}, err
	}
	if _, err := tx.Exec(
		`UPDATE accounts SET cookie=?, fail_count=0, last_error='' WHERE id=?`,
		c, hitID); err != nil {
		_ = tx.Rollback()
		unlock()
		return accountSyncResult{}, err
	}
	// 第二条仅当该行此刻仍是自动停用（status='disabled' AND auto_disabled=1）
	// 才原子恢复 enabled 并清零标记：快照之后被管理员手动停用/启用的行
	// 0 行受影响，绝不越过手动决定（三方言通用、无 MySQL 赋值顺序歧义）。
	reenabled := false
	res, err := tx.Exec(
		`UPDATE accounts SET status='enabled', auto_disabled=0 WHERE id=? AND status='disabled' AND auto_disabled=1`,
		hitID)
	if err != nil {
		_ = tx.Rollback()
		unlock()
		return accountSyncResult{}, err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		reenabled = true
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		unlock()
		return accountSyncResult{}, err
	}
	// MF-6：真实写库成功后在 L1 内 BumpAccountEpoch（无条件推进，不门控
	// shouldUseBrowserTransport），杜绝新 cookie 与并发回写交错时代际漂移。
	newEpoch := browserMgr.BumpAccountEpoch(hitID)
	unlock()

	if reenabled {
		logf("[cookie] 账号 #%d 收到新 cookie，已从自动停用中恢复", hitID)
	}
	// 新旧两份的 XSRF 缓存都作废：新串等懒建后重抓，旧串绝不再信。
	// 顺手清掉 45s 防雪崩短冷却——换上新 cookie 的号不该再背着旧凭据的失败记录。
	invalidateXSRF(cur.Cookie)
	invalidateXSRF(c)
	recordAccountReqSuccess(hitID, nextClientReqID())
	if shouldUseBrowserTransport() {
		// R12-4（Oracle M7/Reviewer #5）代际条件失效：L1 释放后的锁外窗口
		// 内，新身份 Y 的前台请求可能已建好并发布健康 Y 槽（epoch 严格大于
		// 本次 Bump 值）并开始使用——按账号 ID 无条件摘除会把新 Y 槽连同
		// 在途流一起误杀。仅摘除旧代际（epoch < newEpoch）驻留槽：本就是
		// 换身份目标的旧 X 槽被清理，新 Y 槽及其在途流绝不受损。
		browserMgr.InvalidateAccountIfEpochBefore(hitID, newEpoch, "cookie synced")
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
