package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
)

// 服务端几乎每个响应都在刷新 SIDCC / __Secure-1PSIDCC / __Secure-3PSIDCC，
// 客户端要收下再带回去。一直发旧值会被判定为过期会话——实测号活一两小时就失效。
func TestMergeSetCookie(t *testing.T) {
	base := "SID=a; SAPISID=b; SIDCC=old1; __Secure-1PSIDCC=old2; __Secure-1PSID=c"

	got := mergeSetCookie(base, []string{
		"SIDCC=new1; expires=Sat, 07 Aug 2027 00:00:00 GMT; path=/; secure",
		"__Secure-1PSIDCC=new2; path=/; secure; httponly",
	})
	for _, want := range []string{"SIDCC=new1", "__Secure-1PSIDCC=new2"} {
		if !strings.Contains(got, want) {
			t.Errorf("没刷新 %q: %s", want, got)
		}
	}
	// 没被下发的项必须原样保留 —— 丢一个就等于把登录态削掉一块
	for _, want := range []string{"SID=a", "SAPISID=b", "__Secure-1PSID=c"} {
		if !strings.Contains(got, want) {
			t.Errorf("丢了 %q: %s", want, got)
		}
	}
	if strings.Contains(got, "old1") || strings.Contains(got, "old2") {
		t.Errorf("旧值没被替换掉: %s", got)
	}
}

// 响应里的新键要追加进来，不能丢。
func TestMergeSetCookieAddsNew(t *testing.T) {
	got := mergeSetCookie("SID=a", []string{"__Secure-3PSIDCC=fresh; path=/"})
	if !strings.Contains(got, "SID=a") || !strings.Contains(got, "__Secure-3PSIDCC=fresh") {
		t.Errorf("新键没追加: %s", got)
	}
}

// 删除指令（空值 + 1970 过期）不能当成新值写进去，否则会把好端端的 cookie 清空。
func TestMergeSetCookieIgnoresDeletion(t *testing.T) {
	got := mergeSetCookie("SID=a; SIDCC=keepme",
		[]string{"SIDCC=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/"})
	if !strings.Contains(got, "SIDCC=keepme") {
		t.Errorf("删除指令把值清掉了: %s", got)
	}
}

// 没有 Set-Cookie 时原样返回 —— 否则每次请求都会重排一遍 cookie 串，
// 让按内容比对的地方（poolHasCookie / --cookie-file 去重）误判成变了。
func TestMergeSetCookieNoop(t *testing.T) {
	base := "SID=a; SAPISID=b"
	if got := mergeSetCookie(base, nil); got != base {
		t.Errorf("空输入却改了串: %q", got)
	}
	if got := mergeSetCookie(base, []string{"garbage-without-equals"}); got != base {
		t.Errorf("无法解析的 Set-Cookie 却改了串: %q", got)
	}
}

// 值里带 '='（base64 补位）不能被切坏。
func TestMergeSetCookieKeepsEquals(t *testing.T) {
	got := mergeSetCookie("SID=a", []string{"SIDCC=AB==; path=/"})
	if !strings.Contains(got, "SIDCC=AB==") {
		t.Errorf("值里的等号被切坏: %s", got)
	}
}

// TestMergeAccountSetCookiePreservesConcurrent1PSIDTS 验证 mergeAccountSetCookie
// 的增量并发安全：锁内重读 DB 最新 cur 再合并——后台保活刚刷新的
// __Secure-1PSIDTS 绝不被长流结束时的旧快照整串覆写倒退。
func TestMergeAccountSetCookiePreservesConcurrent1PSIDTS(t *testing.T) {
	base := "SAPISID=mc; __Secure-1PSID=mp; SID=ms; __Secure-1PSIDTS=oldts"
	id, err := accountInsert("mergeincr", base, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)

	// 长流进行中（调用方已基于 base 快照算出 merged）：后台保活先一步
	// 在库中刷新了 1PSIDTS。
	rotateAccountTestOnly := func() {
		unlock, err := acquireAcctCookieLock(context.Background(), id)
		if err != nil {
			t.Fatalf("lock: %v", err)
		}
		cur := accountByID(id)
		rotated := strings.Replace(cur.Cookie, "__Secure-1PSIDTS=oldts", "__Secure-1PSIDTS=rotatedts", 1)
		if !updateAccountCookieCASLocked(id, cur.Cookie, rotated) {
			t.Fatal("模拟保活写库失败")
		}
		browserMgr.BumpAccountEpoch(id)
		unlock()
	}
	rotateAccountTestOnly()

	// 长流结束：只该把本次响应的 SIDCC 增量合进最新库值——rotatedts 必须保留。
	mergeAccountSetCookie(id, []string{
		"SIDCC=streamcc; Domain=.google.com; Path=/; Secure",
	}, "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate",
		cookieIdentity(base), 0)

	a := accountByID(id)
	if a == nil {
		t.Fatal("账号丢失")
	}
	if !strings.Contains(a.Cookie, "SIDCC=streamcc") {
		t.Errorf("流响应的 SIDCC 必须增量落库: %q", a.Cookie)
	}
	if !strings.Contains(a.Cookie, "__Secure-1PSIDTS=rotatedts") {
		t.Errorf("保活刚刷新的 1PSIDTS 绝不能被旧快照倒退覆盖: %q", a.Cookie)
	}
	if strings.Contains(a.Cookie, "oldts") {
		t.Errorf("旧 1PSIDTS 不应复活: %q", a.Cookie)
	}
}

// TestMultiTurnConvPersistsSetCookie 验证多轮续接对话收到 Set-Cookie 后：
// 会话内存态与当前重试头同步更新，且登录态账号持久化写回 accounts 表
// （此前多轮路径只更新内存，库里的票据停在旧值）。
func TestMultiTurnConvPersistsSetCookie(t *testing.T) {
	// 假上游：/app 出 XSRF；StreamGenerate 回带 cid/rid 的正常内容帧 + Set-Cookie。
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp := func(status int, body string) *http.Response {
			return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d", status),
				Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: req}
		}
		switch req.URL.Path {
		case "/app":
			return resp(200, fakeAppTokens), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			inner := make([]any, innerSlots)
			inner[1] = []any{"c_cookie", "r_1"}
			inner[4] = []any{[]any{"rc_x", []any{"answer-ok"}}}
			inner[39], inner[42] = hexFlash36, "3.6 Flash"
			ib, _ := json.Marshal(inner)
			line, _ := json.Marshal([]any{[]any{"wrb.fr", "rpc1", string(ib), nil}})
			r := resp(200, ")]}'\n"+string(line)+"\n")
			r.Header.Set("Set-Cookie", "SIDCC=convcc; Domain=.google.com; Path=/; Secure")
			return r, nil
		}
		return resp(404, ""), nil
	})
	convFakeUpstream(t, "http://fake-exit.invalid:3128", rt)
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{{ID: 997, Name: "conv-cookie-test", URL: "http://fake-exit.invalid:3128", Enabled: true}}
	proxyMu.Unlock()
	t.Cleanup(func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() })

	rtMu.Lock()
	oldCfg := rtVal
	rtVal.RetryAttempts, rtVal.RetryDelaySec = 1, 0
	rtMu.Unlock()
	t.Cleanup(func() { rtMu.Lock(); rtVal = oldCfg; rtMu.Unlock() })

	cookie := "SAPISID=mc2; SID=x; __Secure-1PSID=mc2"
	id, err := accountInsert("convcookie", cookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)
	bindAccountProxy(id, 997)

	conv := &convState{isLogin: true, accountID: id, proxyID: 997, cookie: cookie, turn: 0}
	mc := Models["gemini-3.6-flash"]
	res, err := streamGenerateConv(context.Background(), "hi", mc, conv, nil, nil)
	if err != nil {
		t.Fatalf("streamGenerateConv: %v", err)
	}
	if res == nil {
		t.Fatal("res 为空")
	}
	// 会话内存态已更新。
	if !strings.Contains(conv.cookie, "SIDCC=convcc") {
		t.Fatalf("会话内存 cookie 未合并 Set-Cookie: %q", conv.cookie)
	}
	// accounts 表持久化写回（此前多轮路径只更新内存不落库）。
	deadline := time.Now().Add(3 * time.Second)
	for {
		a := accountByID(id)
		if a != nil && strings.Contains(a.Cookie, "SIDCC=convcc") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("多轮会话的 Set-Cookie 必须持久化写回 accounts 表: %q", a.Cookie)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestFetchAppTokensRecyclesSetCookiePureGo 验证纯 Go 路径的 /app 请求回收
// Set-Cookie：无活跃浏览器槽时增量落库；BrowserTransport 下有同身份活跃槽
// 驻留时跳过（jar 由 SyncAccountCookiesBack 负责，绝不 Bump+Invalidate
// 误杀在途流）。
func TestFetchAppTokensRecyclesSetCookiePureGo(t *testing.T) {
	mkStub := func(t *testing.T) {
		t.Helper()
		fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
			h := fhttp.Header{}
			if req.URL.Path == "/app" {
				h.Add("Set-Cookie", "SIDCC=appcc; Domain=.google.com; Path=/; Secure")
				return &fhttp.Response{
					StatusCode: 200, Status: "200 OK",
					Header: h, Body: io.NopCloser(strings.NewReader(fakeAppTokens)),
				}, nil
			}
			return &fhttp.Response{
				StatusCode: 404, Status: "404",
				Header: fhttp.Header{},
				Body:   io.NopCloser(strings.NewReader("")),
			}, nil
		}}
		stubTLSRotateClient(t, fake)
	}

	t.Run("无活跃槽增量落库", func(t *testing.T) {
		mkStub(t)
		cookie := "SAPISID=fx; SID=x; __Secure-1PSID=fx"
		id, err := accountInsert("fetchrecycle", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		// BrowserTransport 关闭：纯 Go 路径必然回收。
		setRuntimeCfg(t, func(c *RuntimeConfig) { c.BrowserTransport = false })
		invalidateXSRF(cookie)
		e, err := fetchAppTokens(cookie, 0, "", false)
		if err != nil {
			t.Fatalf("fetchAppTokens: %v", err)
		}
		if e.token == "" {
			t.Fatal("应解析出 token")
		}
		a := accountByID(id)
		if a == nil || !strings.Contains(a.Cookie, "SIDCC=appcc") {
			t.Fatalf("/app 的 Set-Cookie 必须增量落库: %q", a.Cookie)
		}
		// 其余键不受影响。
		if !strings.Contains(a.Cookie, "SAPISID=fx") || !strings.Contains(a.Cookie, "__Secure-1PSID=fx") {
			t.Fatalf("增量合并绝不能丢身份键: %q", a.Cookie)
		}
	})

	t.Run("有活跃浏览器槽跳过不拆槽", func(t *testing.T) {
		srv := newFakeCDPServer(t)
		defer srv.close()
		cleanup := wireTestBrowser(t, srv)
		defer cleanup()
		m := browserMgr
		mkStub(t)
		cookie := "SAPISID=fx2; SID=x; __Secure-1PSID=fx2"
		id, err := accountInsert("fetchskip", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		acct := accountByID(id)
		setRuntimeCfg(t, func(c *RuntimeConfig) { c.BrowserTransport = true })

		// 注册活跃登录槽（同身份）。
		key := slotKeyForAccount(acct, "", true)
		ac := &accountBrowserCtx{
			key:              key,
			epoch:            1,
			cookieIdentity:   cookieIdentity(cookie),
			browserContextID: "bc-fetch-skip",
			targetID:         "t-fetch-skip",
			sessionID:        "sess-fetch-skip",
			allowDirect:      true,
			readyClosed:      true,
			inFlight:         1,
			lastUsedAt:       time.Now(),
		}
		ac.readyCh = make(chan struct{})
		close(ac.readyCh)
		m.mu.Lock()
		m.ctxs[key] = ac
		m.sessionIndex["sess-fetch-skip"] = ac
		m.targetIndex["t-fetch-skip"] = ac
		beforeEpoch := m.maxSeededEpoch[id]
		m.mu.Unlock()

		// 纯 Go 路径的 stdlib 出口由假 RoundTripper 接管：/app 回 200 +
		// SNlM0e + Set-Cookie，不打真网络。
		appRT := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/app" {
				r := respJSON(req, 200, fakeAppTokens)
				r.Header.Set("Set-Cookie", "SIDCC=appcc; Domain=.google.com; Path=/; Secure")
				return r, nil
			}
			return respJSON(req, 404, ""), nil
		})
		timeout := rtCfg().RequestTimeout
		for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
			stdlibClientCache.Store("http://127.0.0.1:1"+suf, &http.Client{Transport: appRT})
		}
		defer func() {
			for _, suf := range []string{"|0", "|" + fmt.Sprint(timeout)} {
				stdlibClientCache.Delete("http://127.0.0.1:1" + suf)
			}
		}()

		invalidateXSRF(cookie)
		// 辅助入口 proxyURL 传非空与槽位（direct）不匹配 → 回退纯 Go HTTP。
		// 纯 Go 路径拿到 /app 200 + Set-Cookie，但有活跃槽驻留 → 跳过落库。
		if _, err := fetchAppTokens(cookie, 0, "http://127.0.0.1:1", false); err != nil {
			t.Fatalf("fetchAppTokens: %v", err)
		}
		if a := accountByID(id); a != nil && strings.Contains(a.Cookie, "SIDCC=appcc") {
			t.Fatalf("有活跃槽驻留时纯 Go /app 的 Set-Cookie 绝不能落库: %q", a.Cookie)
		}
		m.mu.Lock()
		_, alive := m.ctxs[key]
		m.mu.Unlock()
		if !alive {
			t.Fatal("活跃槽绝不能因辅助入口落库被拆")
		}
		m.mu.Lock()
		afterEpoch := m.maxSeededEpoch[id]
		m.mu.Unlock()
		if afterEpoch != beforeEpoch {
			t.Fatal("跳过路径绝不许 BumpAccountEpoch（epoch 黑洞防线）")
		}
	})
}

// TestMergeAccountSetCookieRejectsCrossIdentityStaleStream 验证 fix #1 的请求
// 身份守卫：长流以身份 A 发出，流持续期间同 ID 账号被 accountSync 换绑到
// 身份 B——旧流结束时仅含 SIDCC 的 Set-Cookie 绝不能增量合并进已换绑为
// 会话 B 的同一行（旧实现会把会话 A 的派生票污染进新登录态）。
func TestMergeAccountSetCookieRejectsCrossIdentityStaleStream(t *testing.T) {
	cookieA := "SAPISID=sa; __Secure-1PSID=pa; SID=x"
	id, err := accountInsert("xident", cookieA, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)

	// 长流进行中：同 ID 账号被扩展推送换绑到会话 B（整串覆盖）。
	cookieB := "SAPISID=sb; __Secure-1PSID=pb; SID=y"
	unlock, err := acquireAcctCookieLock(context.Background(), id)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	cur := accountByID(id)
	res, derr := getDB().Exec(`UPDATE accounts SET cookie=? WHERE id=?`, cookieB, id)
	unlock()
	if derr != nil {
		t.Fatalf("模拟换绑写库: %v", derr)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("模拟换绑未命中（cur=%q）", cur.Cookie)
	}

	// 旧流结束：携带的是发请求时（身份 A）的 expectedIdentity——增量回写
	// 必须被整体丢弃，会话 B 的行绝不能混入会话 A 的 SIDCC。
	mergeAccountSetCookie(id, []string{
		"SIDCC=stale-a-cc; Domain=.google.com; Path=/; Secure",
	}, "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate",
		cookieIdentity(cookieA), 0)

	a := accountByID(id)
	if a == nil {
		t.Fatal("账号丢失")
	}
	if a.Cookie != cookieB {
		t.Fatalf("跨身份旧流的增量绝不能写回换绑后的账号: %q", a.Cookie)
	}
	if strings.Contains(a.Cookie, "stale-a-cc") {
		t.Fatalf("会话 A 的 SIDCC 污染进了会话 B: %q", a.Cookie)
	}

	// 对照：身份未变化（同身份并发刷新）时增量照常落库。
	mergeAccountSetCookie(id, []string{
		"SIDCC=fresh-b-cc; Domain=.google.com; Path=/; Secure",
	}, "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate",
		cookieIdentity(cookieB), 0)
	if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "SIDCC=fresh-b-cc") {
		t.Fatalf("同身份增量必须照常落库: %q", a.Cookie)
	}
}

// TestMergeAccountSetCookieIfNoActiveBrowser 验证 fix #4 的原子守卫写回：
//   - 存在 creating 中的槽时整个跳过（不写库、不 Bump、不 Invalidate）——
//     守卫在 L1 临界区内原子执行，锁外检查与写库之间的建槽窗口不复存在；
//   - 完全无槽时写库并 BumpAccountEpoch（拦截滞后回写），但绝不
//     InvalidateAccount——检查之后新建的槽播种的已是新 Cookie，不能按 ID
//     无条件拆掉。
func TestMergeAccountSetCookieIfNoActiveBrowser(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.BrowserTransport = true })

	t.Run("creating 中的槽跳过不拆", func(t *testing.T) {
		cookie := "SAPISID=cg; __Secure-1PSID=cp; SID=cs"
		id, err := accountInsert("guardcreate", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)
		acct := accountByID(id)

		// 注册一个仍在构建中的槽（creating=true，browserContextID 尚未发布）。
		key := slotKeyForAccount(acct, "", true)
		ac := &accountBrowserCtx{
			key:            key,
			epoch:          1,
			cookieIdentity: cookieIdentity(cookie),
			creating:       true,
			allowDirect:    true,
			readyCh:        make(chan struct{}),
		}
		m.mu.Lock()
		m.ctxs[key] = ac
		beforeEpoch := m.maxSeededEpoch[id]
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()

		mergeAccountSetCookieIfNoActiveBrowser(id, []string{
			"SIDCC=auxcc; Domain=.google.com; Path=/; Secure",
		}, "https://gemini.google.com/app", cookieIdentity(cookie), 0)

		if a := accountByID(id); a != nil && strings.Contains(a.Cookie, "SIDCC=auxcc") {
			t.Fatalf("creating 槽驻留时辅助写回必须整个跳过: %q", a.Cookie)
		}
		m.mu.Lock()
		afterEpoch := m.maxSeededEpoch[id]
		_, alive := m.ctxs[key]
		m.mu.Unlock()
		if afterEpoch != beforeEpoch {
			t.Fatal("跳过路径绝不许 BumpAccountEpoch")
		}
		if !alive {
			t.Fatal("creating 槽绝不能被辅助回收路径摘除")
		}
	})

	t.Run("完全无槽写库 Bump 但不 Invalidate", func(t *testing.T) {
		cookie := "SAPISID=ng; __Secure-1PSID=np; SID=ns"
		id, err := accountInsert("guardnone", cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		defer accountDelete(id)

		m.mu.Lock()
		beforeEpoch := m.maxSeededEpoch[id]
		m.mu.Unlock()

		mergeAccountSetCookieIfNoActiveBrowser(id, []string{
			"SIDCC=auxcc2; Domain=.google.com; Path=/; Secure",
		}, "https://gemini.google.com/app", cookieIdentity(cookie), 0)

		if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "SIDCC=auxcc2") {
			t.Fatalf("无槽时辅助增量必须写库: %q", a.Cookie)
		}
		m.mu.Lock()
		afterEpoch := m.maxSeededEpoch[id]
		m.mu.Unlock()
		if afterEpoch <= beforeEpoch {
			t.Fatal("无槽写库后必须 BumpAccountEpoch（拦截滞后回写）")
		}
		// 写库之后才注册的槽绝不能被拆：模拟紧跟着的新建槽注册。
		acct := accountByID(id)
		key := slotKeyForAccount(acct, "", true)
		late := &accountBrowserCtx{
			key:            key,
			epoch:          afterEpoch + 1,
			cookieIdentity: cookieIdentity(cookie),
			creating:       true,
			allowDirect:    true,
			readyCh:        make(chan struct{}),
		}
		m.mu.Lock()
		m.ctxs[key] = late
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.ctxs, key)
			m.mu.Unlock()
		}()
		// 再跑一次辅助写回：此时已有 creating 槽 → 跳过；而第一趟写库后
		// 绝没有任何 Invalidate 动作（late 仍在）。
		mergeAccountSetCookieIfNoActiveBrowser(id, []string{
			"SIDCC=auxcc3; Domain=.google.com; Path=/; Secure",
		}, "https://gemini.google.com/app", cookieIdentity(cookie), 0)
		m.mu.Lock()
		_, alive := m.ctxs[key]
		m.mu.Unlock()
		if !alive {
			t.Fatal("检查之后新建的槽绝不能被无条件拆掉")
		}
		if a := accountByID(id); a != nil && strings.Contains(a.Cookie, "SIDCC=auxcc3") {
			t.Fatalf("有 creating 槽后辅助写回必须跳过: %q", a.Cookie)
		}
	})
}

// TestMergeAccountSetCookieIfNoActiveBrowserSiblingMustNotBlockOwnWrite 复核
// R3-5（Oracle MF-1）：守卫粒度必须与写库粒度一致——同 cookieIdentity 的
// 兄弟号 A 有活跃槽绝不是本账号 B 跳过写库的理由。B 自己没有槽时其
// SIDCC 只能由它自己的回收路径落库；按身份守卫跳过会让 B 的新票永不进
// DB（静默失效）。同时对照：B 自己有活跃槽时守卫仍严格跳过。
// 复现失败特征（修复前）：HasActiveAccountOrIdentity 按 cookieIdentity
// 匹配到兄弟号 A 的槽 → B 的写库被整体跳过，DB 无 freshB。
func TestMergeAccountSetCookieIfNoActiveBrowserSiblingMustNotBlockOwnWrite(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.BrowserTransport = true })

	// A、B 同 cookieIdentity（SAPISID+__Secure-1PSID 完全相同）的兄弟号。
	sharedCookie := "SAPISID=sg5; __Secure-1PSID=sp5; SID=sx5"
	idA, err := accountInsert("sibA", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	defer accountDelete(idA)
	idB, err := accountInsert("sibB", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	acctA := accountByID(idA)

	// 只给 A 放一个已发布存活槽（B 无任何槽）。
	keyA := slotKeyForAccount(acctA, "", true)
	acA := &accountBrowserCtx{
		key:              keyA,
		epoch:            1,
		cookieIdentity:   cookieIdentity(sharedCookie),
		browserContextID: "bc-sib-a",
		targetID:         "t-sib-a",
		sessionID:        "sess-sib-a",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	acA.readyCh = make(chan struct{})
	close(acA.readyCh)
	m.mu.Lock()
	m.ctxs[keyA] = acA
	m.sessionIndex["sess-sib-a"] = acA
	m.targetIndex["t-sib-a"] = acA
	m.mu.Unlock()

	// B 走纯 Go /app 回收：兄弟号 A 的同身份活跃槽绝不能阻断 B 自己的写库。
	mergeAccountSetCookieIfNoActiveBrowser(idB, []string{
		"SIDCC=freshB; Domain=.google.com; Path=/; Secure",
	}, "https://gemini.google.com/app", cookieIdentity(sharedCookie), 0)

	b := accountByID(idB)
	if b == nil || !strings.Contains(b.Cookie, "SIDCC=freshB") {
		t.Fatalf("兄弟号有活跃槽绝不能阻断本账号写库（Oracle MF-1）: %q", b.Cookie)
	}

	// 对照：A 自己有活跃槽时守卫仍严格跳过（jar 归 SyncAccountCookiesBack 管）。
	mergeAccountSetCookieIfNoActiveBrowser(idA, []string{
		"SIDCC=mustSkip; Domain=.google.com; Path=/; Secure",
	}, "https://gemini.google.com/app", cookieIdentity(sharedCookie), 0)
	a := accountByID(idA)
	if a != nil && strings.Contains(a.Cookie, "SIDCC=mustSkip") {
		t.Fatalf("本账号有活跃槽时守卫必须严格跳过: %q", a.Cookie)
	}
}

// TestFetchAppTokensExactAuthuserWriteback 复核 R4-3（Reviewer MF-3）：
// 纯 Go /app 回收的有副作用写回必须精确匹配 authuser——目标账号 A
// （authuser=0）已停用、同 cookieIdentity 兄弟行 B（authuser=1）仍启用时，
// 请求 authuser=0 的 /app 绝不能把 Set-Cookie 写进 B（读路径
// browserAccountFromHeaders 的兼容 fallback 会命中 B，写回必须用
// enabledAccountByCookieAndAuthuser 精确匹配，无精确行就不写）。
// 复现失败特征（修复前）：回收路径走 browserAccountFromHeaders fallback
// 到 B 并调用 mergeAccountSetCookieIfNoActiveBrowser(B.ID, ...)——B 的 DB
// cookie 混入 xauthcc。
func TestFetchAppTokensExactAuthuserWriteback(t *testing.T) {
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		h := fhttp.Header{}
		if strings.HasSuffix(req.URL.Path, "/app") {
			// /u/1/app 与 /app 下发不同的 SIDCC 标记，精确区分两行的写回。
			cc := "xauthcc"
			if strings.Contains(req.URL.Path, "/u/1/") {
				cc = "xauthcc1"
			}
			h.Add("Set-Cookie", "SIDCC="+cc+"; Domain=.google.com; Path=/; Secure")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(fakeAppTokens)),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404",
			Header: fhttp.Header{},
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.BrowserTransport = false })

	// A（authuser=0）与 B（authuser=1）同 cookieIdentity；A 被管理员停用。
	sharedCookie := "SAPISID=xa1; __Secure-1PSID=xp1; SID=xs1"
	idA, err := accountInsert("xauthA", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	defer accountDelete(idA)
	idB, err := accountInsert("xauthB", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	if err := accountSetAuthUser(idB, 1); err != nil {
		t.Fatalf("set authuser B: %v", err)
	}
	if err := accountSetStatus(idA, "disabled"); err != nil {
		t.Fatalf("disable A: %v", err)
	}

	// 前置复核：读路径的兼容 fallback 确实会命中 B（验证场景搭建成立），
	// 精确匹配查找则绝不命中。
	fallback := browserAccountFromHeaders(map[string]string{
		"Cookie":          sharedCookie,
		"X-Goog-AuthUser": "0",
	})
	if fallback == nil || fallback.ID != idB {
		t.Fatalf("前置：读路径 fallback 应命中兄弟行 B（#%d），得到 %+v", idB, fallback)
	}
	if exact := enabledAccountByCookieAndAuthuser(sharedCookie, 0); exact != nil {
		t.Fatalf("前置：authuser=0 已停用，精确匹配必须落空，得到 %+v", exact)
	}

	// 经 fetchAppTokens 纯 Go 路径请求 authuser=0 的 /app：拿到 token 但
	// 写回必须整体跳过——B 的 DB cookie 绝不能混入 xauthcc。
	invalidateXSRF(sharedCookie)
	e, err := fetchAppTokens(sharedCookie, 0, "", false)
	if err != nil {
		t.Fatalf("fetchAppTokens: %v", err)
	}
	if e.token == "" {
		t.Fatal("应解析出 token")
	}
	if b := accountByID(idB); b == nil || strings.Contains(b.Cookie, "xauthcc") {
		t.Fatalf("A 停用后 /u/0 的 Set-Cookie 绝不能写进兄弟行 B: %q", b.Cookie)
	}
	if a := accountByID(idA); a != nil && strings.Contains(a.Cookie, "xauthcc") {
		t.Fatalf("停用行 A 同样绝不能被写回: %q", a.Cookie)
	}

	// 对照 1：A 恢复启用后同请求必须精确写回 A（authuser=0 行）。
	if err := accountSetStatus(idA, "enabled"); err != nil {
		t.Fatalf("re-enable A: %v", err)
	}
	invalidateXSRF(sharedCookie)
	if _, err := fetchAppTokens(sharedCookie, 0, "", false); err != nil {
		t.Fatalf("fetchAppTokens re-enabled: %v", err)
	}
	if a := accountByID(idA); a == nil || !strings.Contains(a.Cookie, "SIDCC=xauthcc") {
		t.Fatalf("A 启用后 authuser=0 的回收必须精确写回 A: %q", a.Cookie)
	}
	if b := accountByID(idB); b != nil && strings.Contains(b.Cookie, "xauthcc") {
		t.Fatalf("兄弟行 B 仍绝不能被 /u/0 的回收写入: %q", b.Cookie)
	}

	// 对照 2：authuser=1 的请求必须精确写回 B（合法兄弟号各写各行不受影响）。
	invalidateXSRF(sharedCookie)
	if _, err := fetchAppTokens(sharedCookie, 1, "", false); err != nil {
		t.Fatalf("fetchAppTokens authuser=1: %v", err)
	}
	if b := accountByID(idB); b == nil || !strings.Contains(b.Cookie, "SIDCC=xauthcc1") {
		t.Fatalf("authuser=1 的回收必须精确写回 B: %q", b.Cookie)
	}
	if a := accountByID(idA); a != nil && strings.Contains(a.Cookie, "xauthcc1") {
		t.Fatalf("/u/1 的票据绝不能写进 authuser=0 的 A 行: %q", a.Cookie)
	}

	// R5-3 场景一（Reviewer MF-3）：等待 L1 期间管理员 PATCH 将 A 从
	// /u/0 改为 /u/1——锁内重读 authuser 不符 + SQL CAS authuser 谓词
	// 双保险，/u/0 的票据绝不写进已变为 /u/1 的账号。
	// 确定性时序：主协程先持 L1，写回 goroutine 必然阻塞在锁等待上；
	// 主协程改绑后放锁——无论 goroutine 何时被调度，它拿到锁时看到的
	// 都已是 authuser=1（accountSetAuthUser 不经 L1，与实际管理员工具
	// 一致），无任何时序侥幸。
	holdUnlock, lerr := acquireAcctCookieLock(context.Background(), idA)
	if lerr != nil {
		t.Fatalf("pre-lock L1: %v", lerr)
	}
	wbDone := make(chan struct{})
	go func() {
		defer close(wbDone)
		mergeAccountSetCookieIfNoActiveBrowser(idA, []string{
			"SIDCC=patchrace; Domain=.google.com; Path=/; Secure",
		}, "https://gemini.google.com/app", cookieIdentity(sharedCookie), 0)
	}()
	// 管理员 PATCH（不拿 L1、不改 cookie）：A 从 /u/0 变为 /u/1。
	if err := accountSetAuthUser(idA, 1); err != nil {
		holdUnlock()
		t.Fatalf("模拟 PATCH authuser: %v", err)
	}
	holdUnlock()
	select {
	case <-wbDone:
	case <-time.After(3 * time.Second):
		t.Fatal("写回 goroutine 未在放锁后返回")
	}
	if a := accountByID(idA); a != nil && strings.Contains(a.Cookie, "patchrace") {
		t.Fatalf("等待 L1 期间 authuser 被改后绝不能把 /u/0 的票写入 /u/1: %q", a.Cookie)
	}
	// 对照：PATCH 后的新 authuser（1）发起的写回必须正常落库（身份/序号
	// 重新精确匹配，合法写回不受误伤）。
	mergeAccountSetCookieIfNoActiveBrowser(idA, []string{
		"SIDCC=postpatch; Domain=.google.com; Path=/; Secure",
	}, "https://gemini.google.com/u/1/app", cookieIdentity(sharedCookie), 1)
	if a := accountByID(idA); a == nil || !strings.Contains(a.Cookie, "SIDCC=postpatch") {
		t.Fatalf("PATCH 后按新 authuser 的合法写回必须落库: %q", a.Cookie)
	}

	// R5-3 场景二（Reviewer MF-3）：同身份同 authuser 两行并存——
	// 精确查找无法确定归属时必须返回 nil（绝不猜首行），写回整体跳过。
	dupCookie := "SAPISID=dp1; __Secure-1PSID=dq1; SID=ds1"
	idD1, err := accountInsert("dupA", dupCookie, "")
	if err != nil {
		t.Fatalf("insert dupA: %v", err)
	}
	defer accountDelete(idD1)
	idD2, err := accountInsert("dupB", dupCookie, "")
	if err != nil {
		t.Fatalf("insert dupB: %v", err)
	}
	defer accountDelete(idD2)
	if exact := enabledAccountByCookieAndAuthuser(dupCookie, 0); exact != nil {
		t.Fatalf("同身份同 authuser 两行并存时精确查找必须落空（不猜首行），得到 %+v", exact)
	}
	invalidateXSRF(dupCookie)
	if _, err := fetchAppTokens(dupCookie, 0, "", false); err != nil {
		t.Fatalf("fetchAppTokens dup: %v", err)
	}
	for _, idD := range []int64{idD1, idD2} {
		if a := accountByID(idD); a != nil && strings.Contains(a.Cookie, "xauthcc") {
			t.Fatalf("归属不明时绝不能猜首行写回（账号 #%d）: %q", idD, a.Cookie)
		}
	}
}

// TestCheckAccountCookieDisabledTargetNeverWritesSibling 复核 R6-1
// （Reviewer MF-1）：面板按 ID 检测 A（A 已停用、允许检测）时，入口已知
// 的 a.ID 必须透传到底——纯 Go /app 回收写回只校验 A 行，A 已停用直接
// 不写，**绝不**反查同身份同序号的启用兄弟行 B 把 A 的响应写进 B。
// 复现失败特征（修复前）：检测走 getXSRFSwitch 丢弃 a.ID，写回经
// enabledAccountByCookieAndAuthuser 唯一反查恰好命中启用兄弟行 B——
// B 的 DB cookie 混入 tidcc，A 的响应落进了 B。
func TestCheckAccountCookieDisabledTargetNeverWritesSibling(t *testing.T) {
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		h := fhttp.Header{}
		if strings.HasSuffix(req.URL.Path, "/app") {
			h.Add("Set-Cookie", "SIDCC=tidcc; Domain=.google.com; Path=/; Secure")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(fakeAppTokens)),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404",
			Header: fhttp.Header{},
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)
	// 清代理池 + 允许直连：检测必走直连 tls-client（打中上面的桩），
	// 出口选择不构成不确定因素。
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = false
		c.FallbackDirect = true
	})

	// A（停用）与 B（启用）同 cookieIdentity 同 authuser=0。
	sharedCookie := "SAPISID=ta1; __Secure-1PSID=tp1; SID=ts1"
	idA, err := accountInsert("tidA", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	defer accountDelete(idA)
	idB, err := accountInsert("tidB", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	if err := accountSetStatus(idA, "disabled"); err != nil {
		t.Fatalf("disable A: %v", err)
	}

	// 前置复核：唯一反查确实会命中启用兄弟行 B（验证场景搭建成立）。
	if exact := enabledAccountByCookieAndAuthuser(sharedCookie, 0); exact == nil || exact.ID != idB {
		t.Fatalf("前置：同身份同序号唯一反查必须命中 B（#%d），得到 %+v", idB, exact)
	}

	// 面板按 ID 检测停用的 A（纯 Go 模式）：写回目标只应是 A 行——
	// A 已停用直接不写，两行 DB cookie 均绝不能混入 tidcc。
	check := checkAccountCookie(*accountByID(idA))
	if !check.OK {
		t.Fatalf("cookie 本身有效的检测必须返回登录态有效（停用只是面板状态）: %+v", check)
	}
	if b := accountByID(idB); b == nil || strings.Contains(b.Cookie, "tidcc") {
		t.Fatalf("按 ID 检测停用 A 时绝不能把响应反查写进启用兄弟行 B: %q", b.Cookie)
	}
	if a := accountByID(idA); a == nil || strings.Contains(a.Cookie, "tidcc") {
		t.Fatalf("停用行 A 同样绝不能被写回: %q", a.Cookie)
	}
	// 健康度记账不受影响：A 的检测成功照常记 last_ok（与面板语义一致）。
	if a := accountByID(idA); a == nil || a.LastOkAt == 0 {
		t.Fatalf("检测成功必须照常记账健康度（写回守卫只关 cookie 落库）: %+v", a)
	}

	// 对照：A 重新启用后同一次检测必须把 tidcc 写回 A 行本身（透传
	// 目标不是「永不写」），而 B 行仍不受影响。
	if err := accountSetStatus(idA, "enabled"); err != nil {
		t.Fatalf("re-enable A: %v", err)
	}
	invalidateXSRF(sharedCookie)
	if check2 := checkAccountCookie(*accountByID(idA)); !check2.OK {
		t.Fatalf("A 启用后检测必须有效: %+v", check2)
	}
	if a := accountByID(idA); a == nil || !strings.Contains(a.Cookie, "SIDCC=tidcc") {
		t.Fatalf("A 启用后检测的回收必须精确写回 A 行: %q", a.Cookie)
	}
	if b := accountByID(idB); b != nil && strings.Contains(b.Cookie, "tidcc") {
		t.Fatalf("无论何种检测路径，兄弟行 B 都绝不能被写入: %q", b.Cookie)
	}
}

// TestStreamWritebackAuthuserGuardR6 复核 R6-2（Reviewer MF-2）：纯 Go 单轮
// 与多轮长流在飞期间，管理员 PATCH 只改 authuser 不改 cookie——旧 /u/0
// 的 SIDCC 绝不能写进已变为 /u/1 的同一行（此前 mergeAccountSetCookie 只
// 验身份不验序号，不带 authuser 的 CAS 会照常命中）。确定性时序：假上游
// RoundTripper 在 StreamGenerate 在飞时发出信号并扣住响应，主协程 PATCH
// 完成后才放行——写回评估必然落在 PATCH 之后，零时序侥幸。
// 复现失败特征（修复前）：DB 该行的 cookie 混入 in-flight 流的 SIDCC。
func TestStreamWritebackAuthuserGuardR6(t *testing.T) {
	setRuntimeCfg(t, func(c *RuntimeConfig) { c.BrowserTransport = false })
	rtMu.Lock()
	oldCfg := rtVal
	rtVal.RetryAttempts, rtVal.RetryDelaySec = 1, 0
	rtMu.Unlock()
	t.Cleanup(func() { rtMu.Lock(); rtVal = oldCfg; rtMu.Unlock() })

	// R7-7（Reviewer MF-7）：inflight 必须是容量 1 的缓冲通道——旧的无
	// 缓冲通道在子协程先于父协程开始接收执行非阻塞发送时走 default 丢
	// 唤醒，随后子协程阻塞于 <-release 而父协程 5s 后 Fatal 又不关闭
	// release，残留仍占租约的挂起请求影响后续用例。容量 1 后首次到达
	// 信号必落槽，父协程何时开始接收都能拿到。
	mkStreamRT := func(marker string, gate bool) (http.RoundTripper, chan struct{}, chan struct{}) {
		inflight := make(chan struct{}, 1)
		release := make(chan struct{})
		rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			resp := func(status int, body string) *http.Response {
				return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d", status),
					Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: req}
			}
			switch req.URL.Path {
			case "/app":
				return resp(200, fakeAppTokens), nil
			case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
				if gate {
					select {
					case inflight <- struct{}{}:
					default:
					}
					<-release
				}
				inner := make([]any, innerSlots)
				inner[1] = []any{"c_wb", "r_1"}
				inner[4] = []any{[]any{"rc_x", []any{"answer-ok"}}}
				inner[39], inner[42] = hexFlash36, "3.6 Flash"
				ib, _ := json.Marshal(inner)
				line, _ := json.Marshal([]any{[]any{"wrb.fr", "rpc1", string(ib), nil}})
				r := resp(200, ")]}'\n"+string(line)+"\n")
				r.Header.Set("Set-Cookie", "SIDCC="+marker+"; Domain=.google.com; Path=/; Secure")
				return r, nil
			}
			return resp(404, ""), nil
		})
		return rt, inflight, release
	}

	seedAcct := func(t *testing.T, label, tag string) int64 {
		t.Helper()
		cookie := "SAPISID=wb" + tag + "; SID=x; __Secure-1PSID=wp" + tag
		id, err := accountInsert(label, cookie, "")
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		t.Cleanup(func() { _ = accountDelete(id) })
		bindAccountProxy(id, 997)
		invalidateXSRF(cookie)
		xsrfMu.Lock()
		xsrfCache[xsrfKey(cookie, 0)] = xsrfEntry{token: "tok", fetched: time.Now()}
		xsrfMu.Unlock()
		return id
	}

	t.Run("单轮长流在飞 PATCH authuser 后 DB 不变", func(t *testing.T) {
		rt, inflight, release := mkStreamRT("sgu-cc", true)
		fakeUpstreamServer(t, rt)
		id := seedAcct(t, "wbsingle", "s1")
		cookieBefore := accountByID(id).Cookie

		done := make(chan error, 1)
		go func() {
			_, serr := streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
				Models["gemini-3.6-flash"], nil, nil, nil, nil, map[int64]bool{})
			done <- serr
		}()
		// R7-7：幂等关闭 release + 等子协程结束——即便上方断言失败
		// （Fatal 提前返回）也绝不残留仍占租约的挂起请求影响后续用例。
		var relOnce sync.Once
		closeRelease := func() { relOnce.Do(func() { close(release) }) }
		doneConsumed := false
		defer func() {
			closeRelease()
			if !doneConsumed {
				<-done
			}
		}()
		select {
		case <-inflight:
		case <-time.After(5 * time.Second):
			t.Fatal("StreamGenerate 未在飞")
		}
		// 管理员 PATCH：只改序号不改 cookie（在飞窗口内）。
		if err := accountSetAuthUser(id, 1); err != nil {
			t.Fatalf("PATCH authuser: %v", err)
		}
		closeRelease()
		serr := <-done
		doneConsumed = true
		if serr != nil {
			t.Fatalf("streamGenerateOnce: %v", serr)
		}
		// /u/0 的旧会话 SIDCC 绝不能写进已变为 /u/1 的行。
		if a := accountByID(id); a == nil || strings.Contains(a.Cookie, "sgu-cc") {
			t.Fatalf("在飞 PATCH authuser 后单轮写回必须被守卫丢弃: %q", a.Cookie)
		}
		if a := accountByID(id); a == nil || a.Cookie != cookieBefore {
			t.Fatalf("DB cookie 必须保持原样: %q vs %q", a.Cookie, cookieBefore)
		}

		// 对照：恢复 authuser=0 后重跑同一流（无 PATCH）——合法写回照常
		// 落库（守卫不误伤正常路径）。
		if err := accountSetAuthUser(id, 0); err != nil {
			t.Fatalf("restore authuser: %v", err)
		}
		invalidateXSRF(accountByID(id).Cookie)
		xsrfMu.Lock()
		xsrfCache[xsrfKey(accountByID(id).Cookie, 0)] = xsrfEntry{token: "tok", fetched: time.Now()}
		xsrfMu.Unlock()
		rt2, _, _ := mkStreamRT("sgu-cc", false)
		fakeUpstreamServer(t, rt2)
		if _, serr := streamGenerateOnce(context.Background(), "hi", promptCtx{Latest: "hi"},
			Models["gemini-3.6-flash"], nil, nil, nil, nil, map[int64]bool{}); serr != nil {
			t.Fatalf("对照 streamGenerateOnce: %v", serr)
		}
		if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "SIDCC=sgu-cc") {
			t.Fatalf("未 PATCH 的正常写回必须照常落库: %q", a.Cookie)
		}
	})

	t.Run("多轮长流在飞 PATCH authuser 后 DB 不变", func(t *testing.T) {
		rt, inflight, release := mkStreamRT("cvu-cc", true)
		convFakeUpstream(t, "http://fake-exit.invalid:3128", rt)
		proxyMu.Lock()
		oldProxies := proxyCache
		proxyCache = []Proxy{{ID: 997, Name: "conv-cookie-test", URL: "http://fake-exit.invalid:3128", Enabled: true}}
		proxyMu.Unlock()
		t.Cleanup(func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() })

		id := seedAcct(t, "wbconv", "c1")
		cookieBefore := accountByID(id).Cookie
		conv := &convState{isLogin: true, accountID: id, proxyID: 997, cookie: cookieBefore, turn: 0}
		mc := Models["gemini-3.6-flash"]

		done := make(chan error, 1)
		go func() {
			_, serr := streamGenerateConv(context.Background(), "hi", mc, conv, nil, nil)
			done <- serr
		}()
		// R7-7：幂等关闭 release + 等子协程结束——Fatal 提前返回同样
		// 绝不残留挂起请求。
		var relOnce sync.Once
		closeRelease := func() { relOnce.Do(func() { close(release) }) }
		doneConsumed := false
		defer func() {
			closeRelease()
			if !doneConsumed {
				<-done
			}
		}()
		select {
		case <-inflight:
		case <-time.After(5 * time.Second):
			t.Fatal("StreamGenerate 未在飞")
		}
		if err := accountSetAuthUser(id, 1); err != nil {
			t.Fatalf("PATCH authuser: %v", err)
		}
		closeRelease()
		serr := <-done
		doneConsumed = true
		if serr != nil {
			t.Fatalf("streamGenerateConv: %v", serr)
		}
		// 会话内存合并不受影响（本轮会话事实），但 DB 绝不能被旧序号票据写入。
		if !strings.Contains(conv.cookie, "SIDCC=cvu-cc") {
			t.Fatalf("会话内存合并不受守卫影响（本会话内事实）: %q", conv.cookie)
		}
		if a := accountByID(id); a == nil || strings.Contains(a.Cookie, "cvu-cc") {
			t.Fatalf("在飞 PATCH authuser 后多轮写回必须被守卫丢弃: %q", a.Cookie)
		}
		if a := accountByID(id); a == nil || a.Cookie != cookieBefore {
			t.Fatalf("DB cookie 必须保持原样: %q vs %q", a.Cookie, cookieBefore)
		}

		// 对照：恢复 authuser=0 后重跑（无 PATCH）——合法写回照常落库。
		if err := accountSetAuthUser(id, 0); err != nil {
			t.Fatalf("restore authuser: %v", err)
		}
		rt2, _, _ := mkStreamRT("cvu-cc", false)
		convFakeUpstream(t, "http://fake-exit.invalid:3128", rt2)
		conv2 := &convState{isLogin: true, accountID: id, proxyID: 997, cookie: accountByID(id).Cookie, turn: 0}
		if _, serr := streamGenerateConv(context.Background(), "hi", mc, conv2, nil, nil); serr != nil {
			t.Fatalf("对照 streamGenerateConv: %v", serr)
		}
		if a := accountByID(id); a == nil || !strings.Contains(a.Cookie, "SIDCC=cvu-cc") {
			t.Fatalf("未 PATCH 的正常多轮写回必须照常落库: %q", a.Cookie)
		}
	})
}

// TestXSRFBackgroundRefreshNeverWritesAccountDB 复核 R7-1（Reviewer MF-1
// 彻底闭合）：共享缓存后台刷新（refreshAppTokens 链）命中 5-20 分钟旧项时
// 用触发方 cookie 起后台 HTTP 抓 /app——共享键缓存项无单一账号归属，其
// Set-Cookie 回收**只更新 XSRF token 缓存、绝不写账号 DB**（哨兵
// xsrfNoWritebackAccountID）。A 请求在飞被停用、同身份同序号 B 成为唯一
// 启用行时，后台刷新的响应绝不能反查写进 B。
// 复现失败特征（修复前）：后台刷新走 fetchAppTokens（targetAccountID=0）
// 唯一反查命中启用兄弟行 B，B 的 DB cookie 混入 bgcc。
func TestXSRFBackgroundRefreshNeverWritesAccountDB(t *testing.T) {
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		h := fhttp.Header{}
		if strings.HasSuffix(req.URL.Path, "/app") {
			h.Add("Set-Cookie", "SIDCC=bgcc; Domain=.google.com; Path=/; Secure")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(fakeAppTokens)),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404",
			Header: fhttp.Header{},
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = false
		c.FallbackDirect = true
	})

	// 复用 A/B 停用/启用夹具：A（停用）与 B（启用）同 cookieIdentity 同
	// authuser=0——唯一反查恰好命中启用兄弟行 B，这是后台刷新跨行写回的
	// 真实危险场景（A 在飞被停用，B 是唯一启用行）。
	sharedCookie := "SAPISID=bga1; __Secure-1PSID=bgp1; SID=bgs1"
	idA, err := accountInsert("bgA", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	defer accountDelete(idA)
	idB, err := accountInsert("bgB", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	defer accountDelete(idB)
	if err := accountSetStatus(idA, "disabled"); err != nil {
		t.Fatalf("disable A: %v", err)
	}
	if exact := enabledAccountByCookieAndAuthuser(sharedCookie, 0); exact == nil || exact.ID != idB {
		t.Fatalf("前置：唯一反查必须命中启用兄弟行 B（#%d），得到 %+v", idB, exact)
	}

	// 后台共享缓存刷新路径（refreshAppTokens -> storeAppTokens）：回收的
	// SIDCC=bgcc 绝不能落任何账号行，缓存本身照常更新（只更新 token 缓存
	// 的语义不受影响）。
	key := xsrfKey(sharedCookie, 0)
	invalidateXSRF(sharedCookie)
	refreshAppTokens(key, sharedCookie, 0, "", false)
	if b := accountByID(idB); b == nil || strings.Contains(b.Cookie, "bgcc") {
		t.Fatalf("后台共享缓存刷新的回收绝不能写进启用兄弟行 B: %q", b.Cookie)
	}
	if a := accountByID(idA); a == nil || strings.Contains(a.Cookie, "bgcc") {
		t.Fatalf("后台共享缓存刷新的回收绝不能写进停用行 A: %q", a.Cookie)
	}
	xsrfMu.Lock()
	e, ok := xsrfCache[key]
	xsrfMu.Unlock()
	if !ok || e.token == "" {
		t.Fatalf("后台刷新必须照常更新 XSRF token 缓存（只更新缓存的语义不变）: %+v ok=%v", e, ok)
	}

	// 反向见证：真正无归属的旧包装路径（fetchAppTokens，targetAccountID=0）
	// 唯一反查仍会写进 B——证明上方零写入是哨兵透传的功劳而非环境干扰
	// （无归属兼容读规则不受影响，见 R6-1 既有契约）。
	invalidateXSRF(sharedCookie)
	if _, ferr := fetchAppTokens(sharedCookie, 0, "", false); ferr != nil {
		t.Fatalf("对照 fetchAppTokens: %v", ferr)
	}
	if b := accountByID(idB); b == nil || !strings.Contains(b.Cookie, "SIDCC=bgcc") {
		t.Fatalf("反向见证：无归属旧包装的唯一反查写回必须命中 B（证明上方零写入有意义）: %q", b.Cookie)
	}
}

// TestEmptyIdentitySkipsAuxiliaryWriteback 复核 R7-2（Reviewer MF-2）：
// --cookie-file 等缺身份字段的合法旧配置（cookieIdentity == "|"）发起
// 请求时，辅助 /app 回收与流写回一律跳过写库——无可核身份就没有任何
// 手段分辨「旧会话派生票据」与「同 ID 换绑后的新会话票据」，fail-closed
// 绝不碰 DB（导入/检测旧配置的兼容读不受影响）。
// 复现失败特征（修复前）：空身份 "|" 被当作「不拦截」放行，CAS 用重读的
// 新串作期望，旧票落到换绑后的新会话行。
func TestEmptyIdentitySkipsAuxiliaryWriteback(t *testing.T) {
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		h := fhttp.Header{}
		if strings.HasSuffix(req.URL.Path, "/app") {
			h.Add("Set-Cookie", "SIDCC=eicc; Domain=.google.com; Path=/; Secure")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(fakeAppTokens)),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404",
			Header: fhttp.Header{},
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = nil
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = false
		c.FallbackDirect = true
	})

	// 缺身份字段的合法旧配置（无 SAPISID / __Secure-1PSID → cookieIdentity == "|"）。
	legacyCookie := "SID=legacy-x"
	if id := cookieIdentity(legacyCookie); id != "|" {
		t.Fatalf("前置：旧配置身份必须为 |，得到 %q", id)
	}
	id, err := accountInsert("legacyempty", legacyCookie, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer accountDelete(id)

	// 已知 ID 路径透传到底后：取 token 成功（检测旧配置不受影响），但
	// 回收的 SIDCC=eicc 绝不能写库（空身份跳过辅助写回）。
	invalidateXSRF(legacyCookie)
	tok, err := getXSRFSwitchForAccount(legacyCookie, 0, "", false, id)
	if err != nil || tok == "" {
		t.Fatalf("旧配置取 token 必须照常成功（兼容读不受影响）: tok=%q err=%v", tok, err)
	}
	if a := accountByID(id); a == nil || strings.Contains(a.Cookie, "eicc") {
		t.Fatalf("空身份 \"|\" 的辅助回收绝不能写库（R7-2）: %q", a.Cookie)
	}

	// 直接调 mergeAccountSetCookieIfNoActiveBrowser 用空身份同样零写入。
	mergeAccountSetCookieIfNoActiveBrowser(id,
		[]string{"SIDCC=eicc2; Domain=.google.com; Path=/; Secure"},
		"https://gemini.google.com/app", "|", 0)
	if a := accountByID(id); a == nil || strings.Contains(a.Cookie, "eicc") {
		t.Fatalf("空身份直调辅助写回必须跳过: %q", a.Cookie)
	}
	mergeAccountSetCookie(id,
		[]string{"SIDCC=eicc3; Domain=.google.com; Path=/; Secure"},
		"https://gemini.google.com/x", "", 0)
	if a := accountByID(id); a == nil || strings.Contains(a.Cookie, "eicc") {
		t.Fatalf("空身份（\"\"）主流写回必须跳过: %q", a.Cookie)
	}

	// 对照：身份可核的账号（SAPISID+1PSID）走同一已知 ID 路径照常写回——
	// 跳过逻辑不误伤正常账号。
	normalCookie := "SAPISID=ei1; __Secure-1PSID=eip1; SID=eis1"
	idN, err := accountInsert("normalident", normalCookie, "")
	if err != nil {
		t.Fatalf("insert normal: %v", err)
	}
	defer accountDelete(idN)
	invalidateXSRF(normalCookie)
	if _, err := getXSRFSwitchForAccount(normalCookie, 0, "", false, idN); err != nil {
		t.Fatalf("对照取 token: %v", err)
	}
	if a := accountByID(idN); a == nil || !strings.Contains(a.Cookie, "SIDCC=eicc") {
		t.Fatalf("身份可核账号的已知 ID 回收必须照常写库: %q", a.Cookie)
	}
}

// TestXSRFBackgroundRefreshSkipsBrowserBranch 复核 R8-2（Reviewer MF-2）：
// BrowserTransport=true 且存在带增量的就绪浏览器槽时，共享缓存后台刷新
// （refreshAppTokens / xsrfNoWritebackAccountID 哨兵）必须**直接跳过整个
// 浏览器分支**——既不触碰任何驻留 Jar（服务端零 browser fetch），也不把
// Jar 增量写回账号 DB（后台刷新的响应无单一账号归属）；纯 Go HTTP 照常
// 抓 /app 并仅更新 token 缓存。
// 复现失败特征（修复前）：哨兵透传只挡住了纯 Go 写回，浏览器分支照旧
// 进入——fetchAppTokensViaBrowserCtx 借驻留槽页内抓 /app，rc.Close() 启动
// SyncAccountCookiesBack，驻留 Jar 里先于 DB 的增量被写进账号行，后台
// 刷新事实上携带了写回副作用。
func TestXSRFBackgroundRefreshSkipsBrowserBranch(t *testing.T) {
	srv := newFakeCDPServer(t)
	defer srv.close()
	cleanup := wireTestBrowser(t, srv)
	defer cleanup()
	m := browserMgr

	var tlsHits int
	fake := &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		h := fhttp.Header{}
		if strings.HasSuffix(req.URL.Path, "/app") {
			tlsHits++
			h.Add("Set-Cookie", "SIDCC=bgskipcc; Domain=.google.com; Path=/; Secure")
			return &fhttp.Response{
				StatusCode: 200, Status: "200 OK",
				Header: h, Body: io.NopCloser(strings.NewReader(fakeAppTokens)),
			}, nil
		}
		return &fhttp.Response{
			StatusCode: 404, Status: "404",
			Header: fhttp.Header{},
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}}
	stubTLSRotateClient(t, fake)
	setRuntimeCfg(t, func(c *RuntimeConfig) {
		c.BrowserTransport = true
		c.FallbackDirect = true
	})

	// 账号 A 启用入池，并注册其就绪浏览器槽——Jar 携带领先于 DB 的增量
	//（jarinc 在 Jar 中而 DB 中没有），这正是后台刷新绝不能触碰/回收的
	// 驻留 Jar 状态。
	sharedCookie := "SAPISID=bsA1; __Secure-1PSID=bsp1; SID=bss1"
	idA, err := accountInsert("bgskipA", sharedCookie, "")
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	defer accountDelete(idA)
	acct := accountByID(idA)
	key := slotKeyForAccount(acct, "", true)
	acSlot := &accountBrowserCtx{
		key:              key,
		epoch:            1,
		cookieIdentity:   cookieIdentity(sharedCookie),
		browserContextID: "bc-bgskip",
		targetID:         "t-bgskip",
		sessionID:        "sess-bgskip",
		allowDirect:      true,
		readyClosed:      true,
		lastUsedAt:       time.Now(),
	}
	acSlot.readyCh = make(chan struct{})
	close(acSlot.readyCh)
	m.mu.Lock()
	m.ctxs[key] = acSlot
	m.sessionIndex["sess-bgskip"] = acSlot
	m.targetIndex["t-bgskip"] = acSlot
	m.mu.Unlock()
	srv.bcMu.Lock()
	srv.sessToBC["sess-bgskip"] = "bc-bgskip"
	srv.bcStore["bc-bgskip"] = map[string]string{
		"SAPISID": "bsA1", "__Secure-1PSID": "bsp1", "SID": "bss1",
		"SIDCC": "jarinc", // 驻留 Jar 领先于 DB 的增量
	}
	srv.bcMu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.ctxs, key)
		delete(m.sessionIndex, "sess-bgskip")
		delete(m.targetIndex, "t-bgskip")
		m.mu.Unlock()
	}()
	if !browserMgr.HasActiveAccountOrIdentity(idA, cookieIdentity(sharedCookie)) {
		t.Fatal("前置：A 必须有就绪浏览器槽驻留")
	}

	// 后台共享缓存刷新（-1 哨兵链）：必须走纯 Go HTTP（TLS 桩被命中），
	// 浏览器侧零 fetch（驻留 Jar 不被触碰）。
	keyX := xsrfKey(sharedCookie, 0)
	invalidateXSRF(sharedCookie)
	refreshAppTokens(keyX, sharedCookie, 0, "", false)

	// 1) 纯 Go HTTP 路径被真实执行（token 缓存照常更新）。
	xsrfMu.Lock()
	e, ok := xsrfCache[keyX]
	xsrfMu.Unlock()
	if !ok || e.token == "" {
		t.Fatalf("后台刷新必须照常更新 XSRF token 缓存: %+v ok=%v", e, ok)
	}
	if tlsHits == 0 {
		t.Fatal("后台刷新必须走纯 Go HTTP 抓 /app（TLS 桩必须被命中）")
	}
	// 2) 浏览器分支被整个跳过：服务端绝不能收到任何页内 fetch
	//（驻留 Jar 不被触碰）。
	select {
	case spec := <-srv.fetchHit:
		t.Fatalf("后台共享缓存刷新绝不许触碰驻留浏览器 Jar（收到 fetch %s）", spec.url)
	case <-time.After(300 * time.Millisecond):
	}
	// 3) 无写回策略贯穿：驻留 Jar 的增量 jarinc 与响应下发的 bgskipcc
	//    都绝不能写进账号 DB（共享缓存后台刷新无单一账号归属）。
	if a := accountByID(idA); a == nil || a.Cookie != sharedCookie {
		t.Fatalf("后台共享缓存刷新的 DB 绝不能有任何写回: %q", a.Cookie)
	}

	// 对照：同一环境下主路径（getXSRFSwitchForAccount 已知账号 ID）命中
	// 浏览器分支——服务端收到页内 fetch，证明上方零 fetch 是哨兵跳过的
	// 功劳而非环境干扰（对照允许取 token 本身报错，只要求 fetch 到达）。
	invalidateXSRF(sharedCookie)
	browserHit := make(chan struct{}, 1)
	go func() {
		for spec := range srv.fetchHit {
			srv.setFetchScript(spec.reqID, fetchScript{status: 200, chunks: [][]byte{[]byte(fakeAppTokens)}})
			select {
			case browserHit <- struct{}{}:
			default:
			}
			return
		}
	}()
	if _, err := getXSRFSwitchForAccount(sharedCookie, 0, "", false, idA); err != nil {
		t.Logf("对照主路径取 token（允许错误，只要求 fetch 到达）: %v", err)
	}
	select {
	case <-browserHit:
	case <-time.After(3 * time.Second):
		t.Fatal("对照：主路径必须真实进入浏览器分支（服务端应已收到 fetch）")
	}
}
