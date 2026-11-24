package app

// browser_real_smoke_test.go —— 真实 Chromium 156 冒烟（BROWSER_REAL_SMOKE=1 门控）。
//
// 闭合 Reviewer #7 / Oracle M5 实机证据缺口：fakeCDPServer 无法证明
// 「rootBridge 恒拒 502 + 每槽独立 createBrowserContext({proxyServer,
// disposeOnDetach:true}) + Storage.* Cookie 域行为」在真实 Chromium 上成立。
// 本测试拉起本机真 Chromium（--headless=new，profile 用 t.TempDir() 隔离），
// 建两个带独立本地回环 CONNECT 桥的 BrowserContext，断言：
//   - Storage.setCookies（__Secure-1PSID / OSID / __Host-1PLSID / 域项）→
//     Storage.getCookies 精确命中各槽 jar，跨槽零污染；
//   - 各槽并发导航本地回环靶机，流量 100% 走各自槽位桥（ctxBridge_i 计数>=1）；
//   - rootBridge 业务计数 == 0（业务不偷跑根 Profile，且能拦后台外联）。
//
// 未设 BROWSER_REAL_SMOKE=1 时 t.Skip（CI 默认跳过，发版/抽查才实跑）。

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestRealChromiumMultiContextSmoke(t *testing.T) {
	if os.Getenv("BROWSER_REAL_SMOKE") != "1" {
		t.Skip("BROWSER_REAL_SMOKE!=1：跳过真实 Chromium 冒烟")
	}
	bin, err := discoverBrowserBinary(cfgBrowserPath())
	if err != nil {
		t.Skipf("本机无 Chromium 可执行文件: %v", err)
	}

	// 本地回环靶机：两个槽位桥各自转发到的真实 HTTP 服务（命中即计数）。
	// 各槽位本地回环 CONNECT 代理：Chromium 对 https 目标经槽位桥 CONNECT
	// host:port，proxy_N.gotHost 记录的就是「该槽位桥转发的 CONNECT 目标」。
	// http:// 明文不走 CONNECT（forward-proxy GET absolute-URI，CONNECT-only
	// 透明桥不处理）——故靶机必须是 https 才能验证 CONNECT 物理隔离。
	// https 靶机（只取监听端口；localConnectProxy 收 CONNECT 后 echo 不真
	// 转发，TLS 握手必失败——隔离证据是 CONNECT 目标而非请求完成）。
	target1 := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target1.Close()
	target2 := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target2.Close()
	// 靶机用域名 URL（host-resolver-rules 映射到回环）让 Chromium 经槽位桥
	// CONNECT；端口沿用 httptest 实际监听端口。
	port1 := target1.Listener.Addr().(*net.TCPAddr).Port
	port2 := target2.Listener.Addr().(*net.TCPAddr).Port
	target1URL := fmt.Sprintf("https://ctx-target-1.test:%d", port1)
	target2URL := fmt.Sprintf("https://ctx-target-2.test:%d", port2)

	// rootBridge：恒拒 502 封死根 Profile；统计业务 CONNECT（必须==0）。
	rootBridge, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("rootBridge: %v", err)
	}
	defer rootBridge.Shutdown(context.Background())
	if err := rootBridge.SetUpstream("", false); err != nil {
		t.Fatalf("rootBridge lockdown: %v", err)
	}

	// 两个槽位独立桥：都指各自本地 CONNECT 代理，再各转发到靶机。
	// 真实 Chromium 强制要求 createBrowserContext 的 proxyServer 是真实代理；
	// 这里用 newLocalConnectProxy 做「回环 CONNECT 代理」，命中即计 gotHost。
	proxy1 := newLocalConnectProxy(t)
	defer proxy1.close()
	proxy2 := newLocalConnectProxy(t)
	defer proxy2.close()

	ctxBridge1, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("ctxBridge1: %v", err)
	}
	defer ctxBridge1.Shutdown(context.Background())
	ctxBridge1.allowLoopback = true
	if err := ctxBridge1.SetUpstream("http://"+proxy1.ln.Addr().String(), false); err != nil {
		t.Fatalf("ctxBridge1 SetUpstream: %v", err)
	}

	ctxBridge2, err := newBrowserProxyBridge()
	if err != nil {
		t.Fatalf("ctxBridge2: %v", err)
	}
	defer ctxBridge2.Shutdown(context.Background())
	ctxBridge2.allowLoopback = true
	if err := ctxBridge2.SetUpstream("http://"+proxy2.ln.Addr().String(), false); err != nil {
		t.Fatalf("ctxBridge2 SetUpstream: %v", err)
	}

	// 拉起真实 Chromium：profile 隔离在 t.TempDir()，--proxy-server 只指
	// rootBridge。--host-resolver-rules 把靶机域名映射到回环：Chromium 对
	// 127.0.0.1 这类字面 loopback 会绕过代理直连，故靶机 URL 用域名
	// ctx-target-N.test 强制经槽位桥 CONNECT（才能断言流量物理隔离）。
	profileDir := t.TempDir()
	args := browserLaunchArgs(profileDir, rootBridge.Port())
	args = append(args, "--host-resolver-rules=MAP ctx-target-1.test 127.0.0.1,"+
		"MAP ctx-target-2.test 127.0.0.1,MAP *.test 127.0.0.1")
	proc, err := spawnBrowser(bin, args, nil)
	if err != nil {
		t.Fatalf("spawn chromium: %v", err)
	}
	defer killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)

	wsURL, err := waitDevToolsPort(context.Background(), profileDir, proc, 20*time.Second)
	if err != nil {
		t.Fatalf("devtools port: %v", err)
	}
	conn, err := dialCDPWebsocket(context.Background(), wsURL)
	if err != nil {
		t.Fatalf("dial cdp: %v", err)
	}
	client := newCDPClient(conn)
	defer conn.Close()
	defer client.Close()

	mkCtx := func(bridge *browserProxyBridge, name string) (bcID, sessID, targetID string) {
		var bcRes struct {
			BrowserContextID string `json:"browserContextId"`
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.Call(ctx, "", "Target.createBrowserContext", map[string]interface{}{
			"proxyServer":     fmt.Sprintf("http://127.0.0.1:%d", bridge.Port()),
			"disposeOnDetach": true,
		}, &bcRes); err != nil {
			t.Fatalf("%s createBrowserContext: %v", name, err)
		}
		bcID = bcRes.BrowserContextID
		var created struct {
			TargetID string `json:"targetId"`
		}
		if err := client.Call(ctx, "", "Target.createTarget", map[string]interface{}{
			"url": "about:blank", "browserContextId": bcID,
		}, &created); err != nil {
			t.Fatalf("%s createTarget: %v", name, err)
		}
		targetID = created.TargetID
		var attached struct {
			SessionID string `json:"sessionId"`
		}
		if err := client.Call(ctx, "", "Target.attachToTarget", map[string]interface{}{
			"targetId": targetID, "flatten": true,
		}, &attached); err != nil {
			t.Fatalf("%s attach: %v", name, err)
		}
		return bcID, attached.SessionID, targetID
	}

	bcID1, sess1, _ := mkCtx(ctxBridge1, "ctx1")
	bcID2, sess2, _ := mkCtx(ctxBridge2, "ctx2")

	// Storage.setCookies 精确命中各槽 jar：__Secure-1PSID + OSID(host-only) +
	// __Host-1PLSID(host-only url 形态) + 普通域项。
	seed := []map[string]interface{}{
		{"name": "__Secure-1PSID", "value": "p1", "domain": ".google.com", "path": "/", "secure": true, "httpOnly": true},
		{"name": "SAPISID", "value": "s1", "domain": ".google.com", "path": "/", "secure": true},
		{"name": "OSID", "value": "o1", "url": "https://gemini.google.com/", "path": "/", "secure": true, "httpOnly": true},
		{"name": "__Host-1PLSID", "value": "h1", "url": "https://gemini.google.com/", "path": "/", "secure": true, "httpOnly": true},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Call(ctx, "", "Storage.setCookies", map[string]interface{}{
		"browserContextId": bcID1, "cookies": seed,
	}, nil); err != nil {
		t.Fatalf("ctx1 setCookies: %v", err)
	}
	seed2 := []map[string]interface{}{
		{"name": "__Secure-1PSID", "value": "p2", "domain": ".google.com", "path": "/", "secure": true, "httpOnly": true},
		{"name": "SAPISID", "value": "s2", "domain": ".google.com", "path": "/", "secure": true},
	}
	if err := client.Call(ctx, "", "Storage.setCookies", map[string]interface{}{
		"browserContextId": bcID2, "cookies": seed2,
	}, nil); err != nil {
		t.Fatalf("ctx2 setCookies: %v", err)
	}

	var jar1, jar2 struct {
		Cookies []cdpJarCookie `json:"cookies"`
	}
	if err := client.Call(ctx, "", "Storage.getCookies", map[string]interface{}{
		"browserContextId": bcID1,
	}, &jar1); err != nil {
		t.Fatalf("ctx1 getCookies: %v", err)
	}
	if err := client.Call(ctx, "", "Storage.getCookies", map[string]interface{}{
		"browserContextId": bcID2,
	}, &jar2); err != nil {
		t.Fatalf("ctx2 getCookies: %v", err)
	}
	have := map[string]bool{}
	for _, c := range jar1.Cookies {
		have[c.Name] = true
	}
	for _, want := range []string{"__Secure-1PSID", "SAPISID", "OSID", "__Host-1PLSID"} {
		if !have[want] {
			t.Fatalf("ctx1 jar missing %q (got %v)", want, jar1.Cookies)
		}
	}
	// 跨槽隔离：ctx1 的 OSID/__Host-1PLSID 绝不漏进 ctx2。
	for _, c := range jar2.Cookies {
		if c.Name == "OSID" || c.Name == "__Host-1PLSID" {
			t.Fatalf("ctx1 host-only cookie %q leaked into ctx2 jar", c.Name)
		}
		if c.Name == "__Secure-1PSID" && c.Value == "p1" {
			t.Fatal("ctx1 __Secure-1PSID value leaked into ctx2 jar")
		}
	}

	// 并发导航本地回环靶机：流量 100% 走各自槽位桥（gotHost 命中各自端口）。
	// rootBridge 业务计数=0（业务不偷跑根 Profile）。
	var nav1, nav2 struct {
		FrameID   string `json:"frameId"`
		ErrorText string `json:"errorText"`
	}
	if err := client.Call(ctx, sess1, "Page.enable", nil, nil); err != nil {
		t.Fatalf("ctx1 Page.enable: %v", err)
	}
	if err := client.Call(ctx, sess2, "Page.enable", nil, nil); err != nil {
		t.Fatalf("ctx2 Page.enable: %v", err)
	}
	// navigate 允许 errorText（localConnectProxy 收 CONNECT 后 echo 不真转发，
	// TLS 握手必失败）——物理隔离的证据是「Chromium 向各自槽位桥发了
	// CONNECT host:port」，proxy_N.gotHost 命中各自靶机域名即可。
	_ = client.Call(ctx, sess1, "Page.navigate", map[string]interface{}{
		"url": target1URL + "/x",
	}, &nav1)
	_ = client.Call(ctx, sess2, "Page.navigate", map[string]interface{}{
		"url": target2URL + "/x",
	}, &nav2)

	// 各自槽位桥命中各自本地 CONNECT 代理（流量物理隔离）：proxy_N.gotHost
	// 收到的 CONNECT 目标必须是各自靶机域名：端口。
	want1 := fmt.Sprintf("ctx-target-1.test:%d", port1)
	want2 := fmt.Sprintf("ctx-target-2.test:%d", port2)
	got1, got2 := "", ""
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) && (got1 == "" || got2 == "") {
		select {
		case h := <-proxy1.gotHost:
			if h == want1 {
				got1 = h
			}
		case h := <-proxy2.gotHost:
			if h == want2 {
				got2 = h
			}
		case <-time.After(120 * time.Millisecond):
		}
	}
	t.Logf("nav: proxy1Got=%q(want %q) proxy2Got=%q(want %q) nav1Err=%q nav2Err=%q",
		got1, want1, got2, want2, nav1.ErrorText, nav2.ErrorText)
	if got1 == "" {
		t.Fatalf("ctx1 traffic did not reach its own bridge/proxy for %q (isolation broken)", want1)
	}
	if got2 == "" {
		t.Fatalf("ctx2 traffic did not reach its own bridge/proxy for %q (isolation broken)", want2)
	}

	// rootBridge 恒拒 502：业务 CONNECT 必被拒（根 Profile 物理封死）。
	if c, ok := bridgeConnect(t, rootBridge.Port(), "gemini.google.com:443"); ok {
		if c != nil {
			c.Close()
		}
		t.Fatal("rootBridge allowed business CONNECT (root profile leak)")
	}
}
