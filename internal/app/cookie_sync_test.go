package app

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// 扩展定时推 cookie：同会话更新、换会话按标签更新、都对不上才新建，账号序号分开算。
// v6.7.0 适配后更新走 L1 串行锁 + BumpAccountEpoch，且绝不覆写 proxy_id/priority。
func TestAccountSync(t *testing.T) {
	resetSeedState(t)
	v1 := "SID=a; SAPISID=e; __Secure-1PSID=f; __Secure-1PSIDTS=t1; OSID=o"
	v2 := "SID=a; SAPISID=e; __Secure-1PSID=f; __Secure-1PSIDTS=t2; OSID=o"

	r, err := accountSync("Me@Gmail.com", 0, v1)
	if err != nil || r.Action != "added" {
		t.Fatalf("首次应新建：%+v %v", r, err)
	}
	id := r.ID

	// 更新绝不能丢我方 v6.6.0 的主备优先级与出口绑定（含 -1 锁定本机直连）。
	if err := accountSetPriority(id, 100); err != nil {
		t.Fatal(err)
	}
	bindAccountProxy(id, proxyIDDirect)

	if r, _ := accountSync("me@gmail.com", 0, v1); r.Action != "unchanged" || r.ID != id {
		t.Fatalf("内容没变应 unchanged：%+v", r)
	}
	// 同会话、1PSIDTS 换了：标签不同也照样按会话认出来
	if r, _ := accountSync("", 0, v2); r.Action != "updated" || r.ID != id {
		t.Fatalf("同会话应更新原行：%+v", r)
	}
	if a := accountByID(id); a.Cookie != v2 {
		t.Fatalf("没写进去：%q", a.Cookie)
	}
	if a := accountByID(id); a.Priority != 100 || a.ProxyID != proxyIDDirect {
		t.Fatalf("同步绝不能覆写 priority/proxy_id：%+v", a)
	}

	// 重新登录：会话全换了，靠标签（不分大小写）认回同一行
	v3 := "SID=b; SAPISID=x; __Secure-1PSID=y; OSID=o"
	if r, _ := accountSync("ME@gmail.com", 0, v3); r.Action != "updated" || r.ID != id {
		t.Fatalf("同标签应更新原行：%+v", r)
	}

	// 同一串 cookie 的 /u/1 是另一个号，不能写进 /u/0 那行
	r1, _ := accountSync("me@gmail.com", 1, v3)
	if r1.Action != "added" || r1.ID == id || accountByID(r1.ID).AuthUser != 1 {
		t.Fatalf("/u/1 应单独新建：%+v", r1)
	}

	// 自动停用（auto_disabled=1 由 autoDisableIfDead 写入）的号拿到新 cookie
	// 恢复；手动停用的不动
	_, _ = getDB().Exec(`UPDATE accounts SET status='disabled', fail_count=3, last_error='x', auto_disabled=1 WHERE id=?`, id)
	if r, _ := accountSync("", 0, v1); r.Action != "added" || r.ID == id {
		t.Fatalf("%+v", r)
	}
	// v1 的会话跟 v3 不同，但标签为空 → 新建，不该去碰 id
	if a := accountByID(id); a.Status != "disabled" {
		t.Fatalf("无标签、会话不同的推送不该动这一行")
	}
	if r, _ := accountSync("me@gmail.com", 0, v3); r.ID != id {
		t.Fatalf("%+v", r)
	}
	if a := accountByID(id); a.Status != "enabled" || a.FailCount != 0 || a.LastError != "" {
		t.Fatalf("自动停用的号应恢复：%+v", a)
	}
	_ = accountSetStatus(r1.ID, "disabled")
	_, _ = accountSync("me@gmail.com", 1, v2)
	if a := accountByID(r1.ID); a.Status != "disabled" {
		t.Fatalf("手动停用的号不该被重新启用")
	}

	if _, err := accountSync("x", 0, "SID=a; HSID=b"); err == nil {
		t.Fatal("没 SAPISID 应报错")
	}
	if _, err := accountSync("x", 12, v1); err == nil {
		t.Fatal("序号越界应报错")
	}
}

// unchanged 快路径零副作用：cookie 完全未变且健康的号不该推进 epoch、不该失效
// 健康 BrowserContext，也不该触发任何写库。
func TestAccountSyncUnchangedNoSideEffects(t *testing.T) {
	resetSeedState(t)
	v := "SID=a; SAPISID=e; __Secure-1PSID=f; OSID=o"
	r, err := accountSync("m", 0, v)
	if err != nil || r.Action != "added" {
		t.Fatalf("前置新建失败：%+v %v", r, err)
	}
	id := r.ID

	browserMgr.mu.Lock()
	epochBefore := browserMgr.maxSeededEpoch[id]
	nextBefore := browserMgr.nextCtxEpoch
	browserMgr.mu.Unlock()

	r2, err := accountSync("m", 0, v)
	if err != nil || r2.Action != "unchanged" || r2.ID != id {
		t.Fatalf("应 unchanged：%+v %v", r2, err)
	}

	browserMgr.mu.Lock()
	if browserMgr.maxSeededEpoch[id] != epochBefore || browserMgr.nextCtxEpoch != nextBefore {
		t.Fatalf("unchanged 快路径不该推进 epoch：maxSeeded=%d->%d next=%d->%d",
			epochBefore, browserMgr.maxSeededEpoch[id], nextBefore, browserMgr.nextCtxEpoch)
	}
	browserMgr.mu.Unlock()
}

// 真实更新（同会话换新值）必须推进 epoch：懒建槽靠 maxSeededEpoch 判定旧代际，
// 不 Bump 的话新 cookie 会被当成旧代际回写丢弃。
func TestAccountSyncUpdateBumpsEpoch(t *testing.T) {
	resetSeedState(t)
	v1 := "SID=a; SAPISID=e; __Secure-1PSID=f; __Secure-1PSIDTS=t1; OSID=o"
	v2 := "SID=a; SAPISID=e; __Secure-1PSID=f; __Secure-1PSIDTS=t2; OSID=o"
	r, err := accountSync("m", 0, v1)
	if err != nil || r.Action != "added" {
		t.Fatalf("前置新建失败：%+v %v", r, err)
	}
	id := r.ID

	browserMgr.mu.Lock()
	before := browserMgr.maxSeededEpoch[id]
	browserMgr.mu.Unlock()

	if r2, err := accountSync("m", 0, v2); err != nil || r2.Action != "updated" {
		t.Fatalf("应 updated：%+v %v", r2, err)
	}

	browserMgr.mu.Lock()
	if browserMgr.maxSeededEpoch[id] <= before {
		t.Fatalf("真实更新应推进 epoch：%d -> %d", before, browserMgr.maxSeededEpoch[id])
	}
	browserMgr.mu.Unlock()
	if a := accountByID(id); a.Cookie != v2 {
		t.Fatalf("cookie 没写库：%q", a.Cookie)
	}
}

// 接口层：cookie 字段既可以是字符串，也可以直接是扩展导出的数组。
func TestHandleAdminCookieSync(t *testing.T) {
	resetSeedState(t)
	body := `{"label":"me@gmail.com","authuser":0,"cookie":[
	  {"domain":".google.com","name":"SAPISID","value":"e"},
	  {"domain":".google.com","name":"__Secure-1PSID","value":"f"},
	  {"domain":".youtube.com","name":"YSC","value":"y"}]}`
	w := httptest.NewRecorder()
	handleAdminCookieSync(w, httptest.NewRequest("POST", "/admin/api/cookies/sync", strings.NewReader(body)))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"action":"added"`) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	accts := accountList()
	if len(accts) != 1 || accts[0].Cookie != "SAPISID=e; __Secure-1PSID=f" {
		t.Fatalf("%+v", accts)
	}

	w = httptest.NewRecorder()
	handleAdminCookieSync(w, httptest.NewRequest("POST", "/admin/api/cookies/sync",
		strings.NewReader(`{"label":"me@gmail.com","cookie":"SAPISID=e; __Secure-1PSID=f; NID=1"}`)))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"action":"updated"`) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	handleAdminCookieSync(w, httptest.NewRequest("POST", "/admin/api/cookies/sync", strings.NewReader(`{"label":"x"}`)))
	if w.Code != 400 {
		t.Fatalf("空 cookie 应 400：%d", w.Code)
	}
}

// 并发首推回归：N 个 goroutine 同一时刻对空池推同一账号，进程级互斥
// （accountSyncCreateMu + 锁内重查）必须保证恰好 1 次 added、其余全部落到
// 同一行的 unchanged，库内也只有 1 行。
// 复现的是「无锁查重 → 插入」竞态：没有互斥时两个并发同步同时查得 hitID==0
// 各自 accountInsert，库里就出了同 label/authuser/会话的重复 enabled 账号。
func TestAccountSyncConcurrentFirstPush(t *testing.T) {
	resetSeedState(t)
	v := "SID=a; SAPISID=e; __Secure-1PSID=f; OSID=o"

	const n = 16
	results := make(chan accountSyncResult, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, err := accountSync("me@gmail.com", 0, v)
			if err != nil {
				errs <- err
				return
			}
			results <- r
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("并发同步报错：%v", err)
	}
	var added, unchanged, updated int
	var id int64
	for r := range results {
		switch r.Action {
		case "added":
			added++
		case "unchanged":
			unchanged++
		case "updated":
			updated++
		}
		if id == 0 {
			id = r.ID
		} else if r.ID != id {
			t.Fatalf("并发同步落到了不同行：%d 与 %d", id, r.ID)
		}
	}
	if added != 1 || unchanged != n-1 || updated != 0 {
		t.Fatalf("应恰好 1 次 added + %d 次 unchanged，实际 added=%d unchanged=%d updated=%d",
			n-1, added, unchanged, updated)
	}
	accts := accountList()
	if len(accts) != 1 || accts[0].ID != id {
		t.Fatalf("库内应只有 1 行账号，实际 %+v", accts)
	}
}

// 手动停用与自动停用区分：自动停用 → 管理员启用 → 管理员再停用后，即使
// fail_count 仍 >=3，扩展推新 cookie 也绝不把它启用回来（v6.7.0 起靠
// auto_disabled 列区分，不能再拿 fail_count 推断停用来源）；
// 真自动停用的号收到新 cookie 仍照常恢复 enabled。
func TestAccountSyncManualDisableNotReenabled(t *testing.T) {
	resetSeedState(t)
	v1 := "SID=a; SAPISID=m1; __Secure-1PSID=p1; OSID=o"
	v2 := "SID=a; SAPISID=m1; __Secure-1PSID=p1; __Secure-1PSIDTS=t2; OSID=o"

	r, err := accountSync("manual@gmail.com", 0, v1)
	if err != nil || r.Action != "added" {
		t.Fatalf("前置新建失败：%+v %v", r, err)
	}
	id := r.ID

	// 1) 连续鉴权失败到阈值 → autoDisableIfDead 自动停用（auto_disabled=1）
	for i := 0; i < maxCookieAuthFailures; i++ {
		markAccountResult(id, false, "auth fail")
	}
	if a := accountByID(id); a.Status != "disabled" || !a.AutoDisabled {
		t.Fatalf("连续 %d 次鉴权失败应自动停用：%+v", maxCookieAuthFailures, a)
	}

	// 2) 管理员先启用再手动停用：toggle/PATCH 共用 accountSetStatus，一律清
	//    auto_disabled；fail_count 保持阈值以上不被清零。
	if err := accountSetStatus(id, "enabled"); err != nil {
		t.Fatal(err)
	}
	if err := accountSetStatus(id, "disabled"); err != nil {
		t.Fatal(err)
	}
	if a := accountByID(id); a.Status != "disabled" || a.AutoDisabled || a.FailCount < maxCookieAuthFailures {
		t.Fatalf("手动停用后应保留 fail_count 且 auto_disabled=0：%+v", a)
	}

	// 3) 扩展推新 cookie：手动停用的号绝不重新启用，cookie 照常写回
	if r2, err := accountSync("", 0, v2); err != nil || r2.Action != "updated" || r2.ID != id {
		t.Fatalf("同会话推送应仍写回 cookie：%+v %v", r2, err)
	}
	if a := accountByID(id); a.Status != "disabled" || a.AutoDisabled {
		t.Fatalf("手动停用的号不该被同步重新启用：%+v", a)
	}
	if a := accountByID(id); a.Cookie != v2 {
		t.Fatalf("cookie 应照常更新：%q", a.Cookie)
	}

	// 4) 对照：真自动停用的号收到新 cookie 仍自动恢复 enabled
	r3, err := accountSync("auto@gmail.com", 0, "SID=b; SAPISID=a1; __Secure-1PSID=q1; OSID=o")
	if err != nil || r3.Action != "added" {
		t.Fatalf("对照号新建失败：%+v %v", r3, err)
	}
	for i := 0; i < maxCookieAuthFailures; i++ {
		markAccountResult(r3.ID, false, "auth fail")
	}
	if a := accountByID(r3.ID); a.Status != "disabled" || !a.AutoDisabled {
		t.Fatalf("对照号应自动停用：%+v", a)
	}
	if r4, err := accountSync("", 0, "SID=b; SAPISID=a1; __Secure-1PSID=q1; __Secure-1PSIDTS=t9; OSID=o"); err != nil || r4.Action != "updated" {
		t.Fatalf("对照号同步应更新：%+v %v", r4, err)
	}
	if a := accountByID(r3.ID); a.Status != "enabled" || a.AutoDisabled {
		t.Fatalf("自动停用的号收到新 cookie 应恢复 enabled：%+v", a)
	}
}

// R2 丢失更新回归（并发探针）：accountSync 的状态恢复判定已下推数据库原子
// 条件 UPDATE（不再拿 L1 快照里的 cur.Status 盲写回库）。本用例多轮交错
// 「同步推新 cookie」与「accountSetStatus 手动停用/启用」，断言管理员的
// 手动决定绝不落在同步写的中间被覆盖：每轮结束后状态必须等于管理员最后
// 拍板的目标值。修复前（快照盲写 status）只要交错一次就会静默撤销手动
// 停用——本测试串行跑不保证必现，故用循环加压并在 -race -count=5 下验证。
func TestAccountSyncConcurrentManualDisableNotLost(t *testing.T) {
	resetSeedState(t)
	vBase := "SID=a; SAPISID=cd; __Secure-1PSID=cd; OSID=o"

	r, err := accountSync("cd@gmail.com", 0, vBase+"; __Secure-1PSIDTS=t0")
	if err != nil || r.Action != "added" {
		t.Fatalf("前置新建失败：%+v %v", r, err)
	}
	id := r.ID

	const rounds = 64
	// 两个方向交错：偶数轮管理员停用（同步绝不能恢复），奇数轮管理员启用。
	for round := 0; round < rounds; round++ {
		target := "disabled"
		if round%2 == 1 {
			target = "enabled"
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func(round int) {
			defer wg.Done()
			// 推不同 1PSIDTS 保证走 UPDATE 路径（同会话认回同一行）。
			v := fmt.Sprintf("%s; __Secure-1PSIDTS=t%d", vBase, round+1)
			if _, err := accountSync("cd@gmail.com", 0, v); err != nil {
				t.Errorf("accountSync 报错：%v", err)
			}
		}(round)
		go func(target string) {
			defer wg.Done()
			if err := accountSetStatus(id, target); err != nil {
				t.Errorf("accountSetStatus 报错：%v", err)
			}
		}(target)
		wg.Wait()

		a := accountByID(id)
		if a == nil {
			t.Fatalf("轮 %d：账号不见了", round)
		}
		if a.Status != target {
			t.Fatalf("轮 %d：管理员手动设为 %q 却被同步覆盖，现值 %q", round, target, a.Status)
		}
		// auto_disabled 恒应为 0：手动 setStatus 清标记；同步的条件 UPDATE
		// 只在 auto_disabled=1 时才动，而本用例从未产生自动停用。
		if a.AutoDisabled {
			t.Fatalf("轮 %d：本用例不该出现 auto_disabled=1：%+v", round, a)
		}
	}
}

// R2 丢失更新回归（反向，自动停用交错）：accountSync 恢复自动停用依赖
// 「此刻仍 status='disabled' AND auto_disabled=1」的原子条件。交错期间
// 管理员把自动停用改成手动停用（accountSetStatus 清 auto_disabled）时，
// 同步的条件 UPDATE 必须 0 行命中、绝不借自动停用名义把手动停用启用回来。
func TestAccountSyncConcurrentManualEnableNotLost(t *testing.T) {
	resetSeedState(t)
	vBase := "SID=a; SAPISID=ce; __Secure-1PSID=ce; OSID=o"

	r, err := accountSync("ce@gmail.com", 0, vBase+"; __Secure-1PSIDTS=t0")
	if err != nil || r.Action != "added" {
		t.Fatalf("前置新建失败：%+v %v", r, err)
	}
	id := r.ID

	// 先造成真自动停用（auto_disabled=1）。
	for i := 0; i < maxCookieAuthFailures; i++ {
		markAccountResult(id, false, "auth fail")
	}
	if a := accountByID(id); a.Status != "disabled" || !a.AutoDisabled {
		t.Fatalf("应自动停用：%+v", a)
	}

	// 交错：管理员把自动停用翻成手动停用（setStatus disabled → auto_disabled=0）
	// 与同步推新 cookie 竞争。无论谁先谁后，终态都必须是 disabled 且
	// auto_disabled=0——同步的条件 UPDATE 看到 auto_disabled=0 就必须放过。
	const rounds = 64
	for round := 0; round < rounds; round++ {
		// 每轮重置回「自动停用」起点，保证交错窗口每轮都成立。
		if _, err := getDB().Exec(
			`UPDATE accounts SET status='disabled', auto_disabled=1 WHERE id=?`, id); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func(round int) {
			defer wg.Done()
			v := fmt.Sprintf("%s; __Secure-1PSIDTS=t%d", vBase, round+1)
			if _, err := accountSync("ce@gmail.com", 0, v); err != nil {
				t.Errorf("accountSync 报错：%v", err)
			}
		}(round)
		go func() {
			defer wg.Done()
			if err := accountSetStatus(id, "disabled"); err != nil {
				t.Errorf("accountSetStatus 报错：%v", err)
			}
		}()
		wg.Wait()

		a := accountByID(id)
		if a == nil {
			t.Fatalf("轮 %d：账号不见了", round)
		}
		// 合法的终态只有两种：
		//   a) 管理员 setStatus 先到 → auto_disabled=0，同步条件 UPDATE 不命中
		//      → status='disabled', auto_disabled=0（手动停用被尊重）；
		//   b) 同步条件 UPDATE 先到 → status='enabled',auto_disabled=0 → 管理员
		//      setStatus('disabled') 再覆盖 → 同样是 disabled, auto_disabled=0。
		// 唯一非法终态是 enabled：意味着同步借「自动停用」名义越过了手动停用。
		if a.Status != "disabled" || a.AutoDisabled {
			t.Fatalf("轮 %d：手动停用被同步越权恢复：%+v", round, a)
		}
	}
}
