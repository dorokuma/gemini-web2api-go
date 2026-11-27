package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// keyCookieWants 是判断一条 cookie 是否"复制全了"要看的关键项。
//
// 抓包实测浏览器每次发往 gemini.google.com 的有 30 个 cookie，其中 16 个是
// 每请求必带的登录态项。这里只挑最能反映"复制全没全"的几个来展示，不是说
// 别的不重要 —— 我们把整串原样转发，少一个就是少发一个。
var keyCookieWants = []string{"SID", "HSID", "SSID", "APISID", "SAPISID",
	"__Secure-1PSID", "__Secure-1PSIDTS", "__Secure-1PAPISID", "__Secure-1PSIDCC", "OSID"}

// cookieAcctView 是账号的脱敏视图：**不含完整 cookie 值**，只回状态摘要。
// 凭证没必要从服务端再发回浏览器一次（跟旧 handleAdminCookie 同原则）。
func cookieAcctView(a CookieAccount) map[string]interface{} {
	names := cookieNames(a.Cookie)
	nameSet := map[string]bool{}
	for _, n := range names {
		nameSet[n] = true
	}
	var key []string
	for _, w := range keyCookieWants {
		if nameSet[w] {
			key = append(key, w)
		}
	}
	tail := ""
	if s := extractSAPISID(a.Cookie); len(s) >= 4 {
		tail = s[len(s)-4:]
	}
	// health 是给面板标红用的派生健康度（#18）。判据用 fail_count：markAccountResult
	// 成功会把它清零、失败才 +1，而且只有 401/403 才算失败（见 markCookieByStatus），
	// 所以 fail_count>0 就是「当前鉴权在失败」= 多半 cookie 过期了，直接标红提示重导。
	//   dead     鉴权失败中（红）—— 不管启用还是被自动停用，只要 fail_count>0
	//   disabled 用户手动停用且没有失败记录（灰）
	//   unknown  还没成功用过/检测过（待检测）
	//   ok       在用、没失败（绿）
	health := "ok"
	switch {
	case a.FailCount > 0:
		health = "dead"
	case a.Status != "enabled":
		health = "disabled"
	case a.LastOkAt == 0:
		health = "unknown"
	}
	return map[string]interface{}{
		"id":           a.ID,
		"label":        a.Label,
		"status":       a.Status,
		"health":       health,
		"note":         a.Note,
		"created_at":   a.CreatedAt,
		"last_used_at": a.LastUsedAt,
		"last_ok_at":   a.LastOkAt,
		"last_error":   a.LastError,
		"fail_count":   a.FailCount,
		"cookie_count": len(names),
		"key_cookies":  key,
		"sapisid_tail": tail,
		"has_1psidts":  nameSet["__Secure-1PSIDTS"],
		"has_osid":     nameSet["OSID"],
		"proxy_id":     a.ProxyID,
		"proxy_name":   proxyDisplayName(a.ProxyID),
		"authuser":     a.AuthUser,
		"priority":     a.Priority,
		// 45s 防雪崩短冷却剩余秒数（0 = 不在冷却）：面板状态列据此标黄，
		// 免得冷却中的主号被当成「坏了」去手动排障。
		"cooldown_left_sec": accountShortCooldownLeftSec(a.ID),
	}
}

// testHookAfterCookiePatchValidate 仅供测试注入（生产恒为 nil），在
// PATCH 快照读取之后、claimMu 串行化改绑之前触发。
var testHookAfterCookiePatchValidate func(id int64)

// testHookBeforeCookiePatchRebind 仅供测试注入（生产恒为 nil）：在
// PATCH 的非绑定字段（含停用）已提交之后、绑定写之前触发。
var testHookBeforeCookiePatchRebind func(id int64)

// handleAdminCookies — GET 列出池 / POST 新增一条。
func handleAdminCookies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		accts := accountList()
		items := make([]map[string]interface{}, 0, len(accts))
		for _, a := range accts {
			items = append(items, cookieAcctView(a))
		}
		total, enabled := accountCount()
		writeJSON(w, 200, map[string]interface{}{
			"items": items, "total": total, "enabled": enabled,
		})
	case http.MethodPost:
		var p struct {
			Label   string `json:"label"`
			Cookie  string `json:"cookie"`
			Note    string `json:"note"`
			ProxyID *int64 `json:"proxy_id"`
			// 浏览器多号登录时的账号序号（/u/N），不传 = 0
			AuthUser int `json:"authuser"`
			// Priority 主备优先级，不传 = 0（默认轮转）。
			Priority *int `json:"priority"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &p); err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad json"})
			return
		}
		// 先校验再插，免得插进去一条序号不合法的半成品
		if p.AuthUser < 0 || p.AuthUser > maxAuthUser {
			writeJSON(w, 400, map[string]string{"error": fmt.Sprintf("账号序号要在 0..%d 之间", maxAuthUser)})
			return
		}
		// ProxyID 手动指定绑定的出口（#80）：-1 = 锁定本机直连，>0 必须指向
		// 存在的代理，其它负值/不存在的代理直接 400——必须在 accountAdd 之前拦，
		// 否则 400 出去了库里却留下一条没绑定的号。
		if p.ProxyID != nil && (*p.ProxyID < proxyIDDirect ||
			(*p.ProxyID > 0 && !proxyExists(*p.ProxyID))) {
			writeJSON(w, 400, map[string]string{"error": "proxy not found"})
			return
		}
		if p.Priority != nil && (*p.Priority < minAccountPriority || *p.Priority > maxAccountPriority) {
			writeJSON(w, 400, map[string]string{"error": fmt.Sprintf("priority 要在 %d..%d 之间", minAccountPriority, maxAccountPriority)})
			return
		}
		id, err := accountAdd(p.Label, p.Cookie, p.Note)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if p.AuthUser != 0 {
			if err := accountSetAuthUser(id, p.AuthUser); err != nil {
				writeJSON(w, 500, map[string]string{"error": err.Error()})
				return
			}
		}
		if p.Priority != nil && *p.Priority != 0 {
			if err := accountSetPriority(id, *p.Priority); err != nil {
				writeJSON(w, 500, map[string]string{"error": err.Error()})
				return
			}
		}
		if p.ProxyID != nil {
			bindAccountProxy(id, *p.ProxyID)
		}
		logf("[cookies] 新增账号 #%d label=%q authuser=%d", id, strings.TrimSpace(p.Label), p.AuthUser)
		writeJSON(w, 200, map[string]interface{}{"id": id})
	default:
		writeJSON(w, 405, map[string]string{"error": "method not allowed"})
	}
}

// handleAdminCookieItem — /admin/api/cookies/{id}[/toggle]
//
//	DELETE          删除
//	POST .../toggle 翻转 enabled/disabled
//	PATCH           改 label / note / status
func handleAdminCookieItem(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/admin/api/cookies/")
	parts := strings.Split(rest, "/")
	if len(parts) < 1 || parts[0] == "" {
		writeJSON(w, 404, map[string]string{"error": "missing id"})
		return
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "bad id"})
		return
	}
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch r.Method {
	case http.MethodDelete:
		if err := accountDelete(id); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		if shouldUseBrowserTransport() {
			browserMgr.InvalidateAccount(id, false, "deleted")
		}
		logf("[cookies] 删除账号 #%d", id)
		writeJSON(w, 200, map[string]bool{"ok": true})
	case http.MethodPost:
		handleAdminCookiePost(w, id, action)
	case http.MethodPatch:
		handleAdminCookiePatch(w, r, id)
	default:
		writeJSON(w, 405, map[string]string{"error": "method not allowed"})
	}
}

// handleAdminCookiePost — POST /admin/api/cookies/{id}/{rotate,check,toggle}。
// 独立成函数控制 handleAdminCookieItem 的认知复杂度。
func handleAdminCookiePost(w http.ResponseWriter, id int64, action string) {
	switch action {
	case "rotate":
		for _, a := range accountList() {
			if a.ID == id {
				iv, names, err := rotateAccount(a)
				if err != nil {
					// 部分失败（如 1PSIDTS 401 但 SIDCC 已刷）与空跑都如实透出
					// refreshed——前端据 ok=false 显示错误详情，绝不谎报成功。
					writeJSON(w, 200, map[string]interface{}{"ok": false, "detail": err.Error(), "refreshed": names})
					return
				}
				detail := "保活成功"
				if len(names) > 0 {
					detail = "保活成功，刷新了 " + strings.Join(names, ", ")
				}
				writeJSON(w, 200, map[string]interface{}{
					"ok": true, "detail": detail, "refreshed": names, "next_sec": int(iv.Seconds())})
				return
			}
		}
		writeJSON(w, 404, map[string]string{"error": "account not found"})
	case "check":
		for _, a := range accountList() {
			if a.ID == id {
				writeJSON(w, 200, checkAccountCookie(a))
				return
			}
		}
		writeJSON(w, 404, map[string]string{"error": "account not found"})
	case "toggle":
		cur := ""
		for _, a := range accountList() {
			if a.ID == id {
				cur = a.Status
				break
			}
		}
		next := "enabled"
		if cur == "enabled" {
			next = "disabled"
		}
		if err := accountSetStatus(id, next); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		if next == "disabled" && shouldUseBrowserTransport() {
			browserMgr.InvalidateAccount(id, false, "disabled")
		}
		writeJSON(w, 200, map[string]interface{}{"status": next})
	default:
		writeJSON(w, 400, map[string]string{"error": "unknown action"})
	}
}

// cookiePatchBody 是 PATCH /admin/api/cookies/{id} 的全部可选字段。
type cookiePatchBody struct {
	Label  *string `json:"label"`
	Note   *string `json:"note"`
	Status *string `json:"status"`
	// ProxyID 手动指定绑定的出口（#80），0 = 清掉绑定，下次用到哪个出口再自动绑。
	ProxyID  *int64 `json:"proxy_id"`
	AuthUser *int   `json:"authuser"`
	// Priority 主备优先级（v6.6.0），范围 minAccountPriority..maxAccountPriority。
	Priority *int `json:"priority"`
}

// validateCookiePatch 全量校验 PATCH 字段，返回一段能直接写 400 的错误串
// （"" = 全部合法）。先全部校验再统一落库：任何一个字段非法都 400 零副作用——
// 以前 bindAccountProxy 抢在 authuser/status 校验前写库，带合法 proxy_id +
// 非法 authuser 的请求 400 了却已经改掉绑定。
func validateCookiePatch(p cookiePatchBody) string {
	if p.ProxyID != nil && (*p.ProxyID < proxyIDDirect ||
		(*p.ProxyID > 0 && !proxyExists(*p.ProxyID))) {
		return "proxy not found"
	}
	if p.AuthUser != nil && (*p.AuthUser < 0 || *p.AuthUser > maxAuthUser) {
		return fmt.Sprintf("账号序号要在 0..%d 之间", maxAuthUser)
	}
	if p.Status != nil && *p.Status != "enabled" && *p.Status != "disabled" {
		return fmt.Sprintf("非法状态 %q", *p.Status)
	}
	if p.Priority != nil && (*p.Priority < minAccountPriority || *p.Priority > maxAccountPriority) {
		return fmt.Sprintf("priority 要在 %d..%d 之间", minAccountPriority, maxAccountPriority)
	}
	return ""
}

// writeCookiePatchErr 把单字段落库错误回给面板（校验过仍可能撞并发删除等）。
func writeCookiePatchErr(w http.ResponseWriter, err error) {
	writeJSON(w, 400, map[string]string{"error": err.Error()})
}

// handleAdminCookiePatch — PATCH /admin/api/cookies/{id}：改 label / note /
// status / proxy_id / authuser / priority。独立成函数控制 handleAdminCookieItem
// 的认知复杂度（原 PATCH 分支占了大半）。
//
// 写库前先取 before := accountByID(id) 快照：Stage 2（多 BrowserContext）靠它
// 精准判断要不要失效在途浏览器上下文——只改 priority/label/note/authuser 绝不能
// 打断在途流，proxy_id 实际变化或 status 切 disabled 才失效。
func handleAdminCookiePatch(w http.ResponseWriter, r *http.Request, id int64) {
	var p cookiePatchBody
	body, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(body, &p); err != nil {
		writeJSON(w, 400, map[string]string{"error": "bad json"})
		return
	}
	if msg := validateCookiePatch(p); msg != "" {
		writeJSON(w, 400, map[string]string{"error": msg})
		return
	}
	// R10-2（Reviewer M5/Oracle M5）：改绑的「读实际旧值 + 写新绑定 +
	// 清自动来源」由 rebindAccountProxyAtomic 在同一个 claimMu 临界区
	// 原子完成，proxyChanged 按串行化后的**实际**旧值判定——修复前在
	// 锁外提前读 before 快照：等待 claimMu 期间并发配额自动重绑提交
	// 后，PATCH 把 DB 实际改回旧值却按早先的 false 跳过失效，旧出口
	// 槽及在途流继续存活。真正同值 PATCH（old == new）依旧不拆槽。
	before := accountByID(id)
	// 测试钩子（生产恒 nil）：仅在快照读取之后触发一次，供回归测试
	// 确定性构造「PATCH 已读快照、进入 claimMu 前的窗口内并发配额自
	// 动重绑已提交」的交错终态。生产路径绝不经由它做任何判断，绝不
	// 改变生产行为。
	if hook := testHookAfterCookiePatchValidate; hook != nil {
		hook(id)
	}
	proxyChanged := false
	disabling := p.Status != nil && *p.Status == "disabled" &&
		(before == nil || before.Status != "disabled")
	// R11-6（Oracle A7-1/Reviewer #5）字段顺序保证失败闭环：**先写全部非
	// 绑定字段，最后才提交绑定**——任一非绑定字段写失败时绑定尚未发生，
	// 不存在「P2 已提交但失效被跳过、DB=P2 而旧 P1 在途流继续存活」的半
	// 态（等效于绑定操作的原子回滚）；绑定自身写失败时后续字段未写，面板
	// 拿到如实错误。修复前绑定最先提交，后续字段写错直接返回跳过失效，
	// 重试同值 PATCH 又因实际同值不拆槽，旧出口流永久存活。
	if p.AuthUser != nil {
		if err := accountSetAuthUser(id, *p.AuthUser); err != nil {
			writeCookiePatchErr(w, err)
			return
		}
	}
	if p.Priority != nil {
		if err := accountSetPriority(id, *p.Priority); err != nil {
			writeCookiePatchErr(w, err)
			return
		}
	}
	// R12-5（Oracle M8/Reviewer #6）停用失效闭环：status 一旦成功提交为
	// disabled，无论后续字段（尤其是最后的 proxy_id 绑定写）是否失败，
	// 已提交的停用都必须完成 InvalidateAccount——否则错误返回后 DB 已
	// disabled 而旧槽在途流继续存活，且原样重试时 before.Status 已是
	// disabled，停用失效永远不会补做。未提交的改绑不作为拆槽理由（不变）。
	statusBecameDisabled := false
	if p.Status != nil {
		if err := accountSetStatus(id, *p.Status); err != nil {
			writeCookiePatchErr(w, err)
			return
		}
		statusBecameDisabled = disabling
	}
	// invalidateCommittedDisable 在任一后续失败返回路径上补做已提交停用
	// 的失效——只针对本 PATCH 已成功提交的停用，绝不为未提交的改绑拆槽。
	invalidateCommittedDisable := func() {
		if statusBecameDisabled && shouldUseBrowserTransport() {
			browserMgr.InvalidateAccount(id, false, "disabled")
		}
	}
	if p.Label != nil || p.Note != nil {
		// 只改传了的字段：没传的沿用当前值。
		var curLabel, curNote string
		for _, a := range accountList() {
			if a.ID == id {
				curLabel, curNote = a.Label, a.Note
				break
			}
		}
		if p.Label != nil {
			curLabel = *p.Label
		}
		if p.Note != nil {
			curNote = *p.Note
		}
		if err := accountUpdateMeta(id, curLabel, curNote); err != nil {
			invalidateCommittedDisable()
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
	}
	// 绑定最后提交：R11-6（Oracle A7-2）rebindAccountProxyAtomic 严格检
	// 查 UPDATE 真实命中目标行——行不存在/写库失败时如实返回面板错误，
	// 绝不按「已改绑」拆槽或中断在途流；仅确认提交成功后才按实际旧值
	// 判定失效。真正同值 PATCH（old == new）依旧不拆槽。
	if p.ProxyID != nil {
		// 测试钩子（生产恒 nil）：在停用/元数据已提交、绑定写之前的
		// 精确交错点执行一次——回归测试借此确定性构造「停用已提交但
		// 绑定写失败」的 Oracle M8 部分提交事故。生产路径绝不经由它
		// 做任何判断，绝不改变生产行为。
		if hook := testHookBeforeCookiePatchRebind; hook != nil {
			hook(id)
		}
		actualOld, updated, rerr := rebindAccountProxyAtomic(id, *p.ProxyID)
		if rerr != nil || !updated {
			if rerr == nil {
				rerr = fmt.Errorf("账号 #%d 的绑定未能落库（未执行任何改绑）", id)
			}
			// R12-5：绑定写失败不影响已提交停用的失效义务——先补做停用
			// 失效再如实返回错误，绝不留下已停用账号的在途流。
			invalidateCommittedDisable()
			writeCookiePatchErr(w, rerr)
			return
		}
		proxyChanged = before != nil && *p.ProxyID != actualOld
	}
	if shouldUseBrowserTransport() && (proxyChanged || statusBecameDisabled) {
		browserMgr.InvalidateAccount(id, !statusBecameDisabled, "patched")
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}

// handleAdminCookieSync — POST /admin/api/cookies/sync，给浏览器扩展定时推 cookie。
//
// body: {"label":"me@gmail.com","authuser":0,"cookie":<串或扩展导出的 JSON 数组>}
// cookie 字段可以是字符串，也可以直接是 JSON 数组/对象（扩展里省得再 stringify 一层）。
// 鉴权跟其它 admin 接口一样，扩展用 Authorization: Bearer <admin_token>。
func handleAdminCookieSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "method not allowed"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "bad body"})
		return
	}
	var p struct {
		Label    string          `json:"label"`
		AuthUser int             `json:"authuser"`
		Cookie   json.RawMessage `json:"cookie"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		writeJSON(w, 400, map[string]string{"error": "bad json"})
		return
	}
	cookie := strings.TrimSpace(string(p.Cookie))
	var s string
	if json.Unmarshal(p.Cookie, &s) == nil {
		cookie = s
	}
	if cookie == "" || cookie == "null" {
		writeJSON(w, 400, map[string]string{"error": "cookie 不能为空"})
		return
	}
	res, err := accountSync(p.Label, p.AuthUser, cookie)
	if err != nil {
		writeJSON(w, 400, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	msg := map[string]string{
		"added":     "已新建账号 #%d",
		"updated":   "已更新账号 #%d",
		"unchanged": "账号 #%d 的 cookie 没有变化",
	}[res.Action]
	if res.Action != "unchanged" {
		logf("[cookies] 扩展同步：账号 #%d %s label=%q authuser=%d", res.ID, res.Action, strings.TrimSpace(p.Label), p.AuthUser)
	}
	writeJSON(w, 200, map[string]interface{}{
		"success": true, "id": res.ID, "action": res.Action,
		"message": fmt.Sprintf(msg, res.ID),
	})
}
