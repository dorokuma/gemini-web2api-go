package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/bandwidth"
	"golang.org/x/net/proxy"
)

// 同一个账号的请求不能因为走没走代理而呈现两种客户端：两条路径都要带齐浏览器头
// （writeBrowserHeaders / chromeClientHints 单源），调用方自己的 header 照样生效。
// 直连那条以前一个都不带，UA 是 Go-http-client/2.0。
//
// 直连走 tls-client，这里换成不校验证书的同款客户端，打本机的 HTTPS 测试服务器；
// 代理那条把 http:// 目标以绝对 URI 发给代理，测试服务器本身就能当代理（同 cancel_test）。
func TestDirectAndProxyPathsSendSameBrowserHeaders(t *testing.T) {
	old := rtCfg()
	defer func() { rtMu.Lock(); rtVal = old; rtMu.Unlock() }()
	rtMu.Lock()
	rtVal.Impersonate = "chrome_146"
	rtVal.FirefoxFingerprint = false
	rtMu.Unlock()

	got := map[string]http.Header{}
	record := func(name string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got[name] = r.Header.Clone()
			w.WriteHeader(200)
		})
	}
	direct := httptest.NewUnstartedServer(record("direct"))
	direct.EnableHTTP2 = true
	direct.StartTLS()
	defer direct.Close()
	proxy := httptest.NewServer(record("proxy"))
	defer proxy.Close()

	useInsecureTLSClients(t)

	own := map[string]string{"Accept-Language": "en-US,en;q=0.9", "X-Same-Domain": "1"}
	paths := []struct {
		name string
		send func(endpoint, proxyURL string)
	}{
		{"StreamGenerate", func(endpoint, proxyURL string) {
			doGeminiRequestStop(context.Background(), endpoint, "f.req=x", own, proxyURL, nil, nil)
		}},
		{"upload", func(endpoint, proxyURL string) {
			uploadPostCtx(context.Background(), endpoint, own, []byte("x"), proxyURL)
		}},
		{"media", func(endpoint, proxyURL string) {
			mediaGetOnce(endpoint, own, proxyURL)
		}},
		{"image", func(endpoint, proxyURL string) {
			fetchImage(endpoint, proxyURL, 0)
		}},
		// webchannel 直连分支以前漏掉整套浏览器头，必须纳入同一断言。
		{"webchannel", func(endpoint, proxyURL string) {
			r, err := wcSendWith(context.Background(), "GET", endpoint, "", "", proxyURL, false)
			if err == nil {
				r.body.Close()
			}
		}},
	}
	// 两条路径对两个目标都是 cross-site：direct 打 127.0.0.1，代理打 upstream.invalid，
	// 都不是 google.com 域。默认值必须是明确的非空期望，不只是两路相等。
	wantFetch := map[string]string{
		"Sec-Fetch-Dest": "empty",
		"Sec-Fetch-Mode": "cors",
		"Sec-Fetch-Site": "cross-site",
	}
	for _, p := range paths {
		clear(got)
		p.send(direct.URL+"/x", "")
		p.send("http://upstream.invalid/x", proxy.URL)
		d, pr := got["direct"], got["proxy"]
		if d == nil || pr == nil {
			t.Fatalf("%s: 没收到请求（direct=%v proxy=%v）", p.name, d != nil, pr != nil)
		}
		for name, h := range map[string]http.Header{"直连": d, "代理": pr} {
			if ua := h.Get("User-Agent"); ua != ChromeUA {
				t.Errorf("%s: %s UA = %q，应 %q", p.name, name, ua, ChromeUA)
			}
			for k, want := range wantFetch {
				if v := h.Get(k); v != want {
					t.Errorf("%s: %s %s = %q，应 %q", p.name, name, k, v, want)
				}
			}
		}
		for k, v := range chromeClientHints() {
			if d.Get(k) != v || pr.Get(k) != v {
				t.Errorf("%s: %s 直连 %q，代理 %q（应 %q）", p.name, k, d.Get(k), pr.Get(k), v)
			}
		}
		// webchannel 只发它内置的 header 集（不含 own），不验 own；image 自有 fetch 头。
		if p.name != "image" && p.name != "webchannel" {
			for k, v := range own {
				if d.Get(k) != v || pr.Get(k) != v {
					t.Errorf("%s: 调用方的 %s 没生效（直连 %q，代理 %q）", p.name, k, d.Get(k), pr.Get(k))
				}
			}
		}
	}
}

// ─── /app 导航头真实请求回归 ─────────────────────────────────────────────────
//
// GET /app 是打开网页（document/navigate），不是页面内 XHR（empty/cors）：
// fetchAppPageCtx 和 getAnonSession 都用 appPageHeaders，盖在 XHR 默认值上。
// 这里不再只测常量 map，而是截获两个入口 × 直连/代理两路真实发出的请求。

// captureTLSClient 是直连路径用的假 tls_client.HttpClient：Do 时把收到的 fhttp
// 请求交给测试回调（记头、回固定响应）。只用来截获导航请求，不模拟 TLS。
type captureTLSClient struct {
	do func(*fhttp.Request) (*fhttp.Response, error)
}

func (c *captureTLSClient) Do(req *fhttp.Request) (*fhttp.Response, error) { return c.do(req) }
func (c *captureTLSClient) GetCookies(*url.URL) []*fhttp.Cookie            { return nil }
func (c *captureTLSClient) SetCookies(*url.URL, []*fhttp.Cookie)           {}
func (c *captureTLSClient) SetCookieJar(fhttp.CookieJar)                   {}
func (c *captureTLSClient) GetCookieJar() fhttp.CookieJar                  { return nil }
func (c *captureTLSClient) SetProxy(string) error                          { return nil }
func (c *captureTLSClient) GetProxy() string                               { return "" }
func (c *captureTLSClient) SetFollowRedirect(bool)                         {}
func (c *captureTLSClient) GetFollowRedirect() bool                        { return false }
func (c *captureTLSClient) CloseIdleConnections()                          {}
func (c *captureTLSClient) GetBandwidthTracker() bandwidth.BandwidthTracker {
	return nil
}
func (c *captureTLSClient) GetDialer() proxy.ContextDialer         { return nil }
func (c *captureTLSClient) GetTLSDialer() tls_client.TLSDialerFunc { return nil }
func (c *captureTLSClient) Get(string) (*fhttp.Response, error) {
	return nil, errors.New("captureTLSClient: Get 未实现")
}
func (c *captureTLSClient) Head(string) (*fhttp.Response, error) {
	return nil, errors.New("captureTLSClient: Head 未实现")
}
func (c *captureTLSClient) Post(string, string, io.Reader) (*fhttp.Response, error) {
	return nil, errors.New("captureTLSClient: Post 未实现")
}

// swapTLSClient 把 getTLSClient 会取到的那份缓存换成 fake，返回 key 供清理恢复。
// key 的拼法跟 tlsClientWithin 完全一致。
func swapTLSClient(t *testing.T, fake tls_client.HttpClient) string {
	t.Helper()
	key := fmt.Sprintf("%s|%d", effectiveImpersonate(), rtCfg().RequestTimeout)
	clientCacheMu.Lock()
	prev, had := clientCache[key]
	clientCache[key] = fake
	clientCacheMu.Unlock()
	t.Cleanup(func() {
		clientCacheMu.Lock()
		defer clientCacheMu.Unlock()
		if had {
			clientCache[key] = prev
		} else {
			delete(clientCache, key)
		}
	})
	return key
}

func TestAppPageHeaders(t *testing.T) {
	old := rtCfg()
	defer func() { rtMu.Lock(); rtVal = old; rtMu.Unlock() }()
	rtMu.Lock()
	rtVal.Impersonate = "chrome_146"
	rtVal.FirefoxFingerprint = false
	rtMu.Unlock()

	// 浏览器地址栏打开 /app 该有的导航头（覆盖 writeBrowserHeaders 的 XHR 默认值）。
	want := map[string]string{
		"Sec-Fetch-Dest":            "document",
		"Sec-Fetch-Mode":            "navigate",
		"Sec-Fetch-Site":            "none",
		"Upgrade-Insecure-Requests": "1",
	}
	check := func(t *testing.T, name string, get func(string) string) {
		t.Helper()
		for k, v := range want {
			if got := get(k); got != v {
				t.Errorf("%s: %s = %q，应 %q", name, k, got, v)
			}
		}
		if a := get("Accept"); !strings.HasPrefix(a, "text/html,") {
			t.Errorf("%s: Accept = %q，应以 text/html 开头（导航 Accept）", name, a)
		}
	}

	// 直连：假 tls-client 记下 fhttp 请求头，回 200 + 一个 Set-Cookie
	// （getAnonSession 没有 Set-Cookie 会报错）。
	var directHdr fhttp.Header
	swapTLSClient(t, &captureTLSClient{do: func(req *fhttp.Request) (*fhttp.Response, error) {
		directHdr = req.Header.Clone()
		return &fhttp.Response{
			StatusCode: 200, Status: "200 OK",
			Header: fhttp.Header{"Set-Cookie": []string{"NID=test; Path=/"}},
			Body:   io.NopCloser(strings.NewReader(`"SNlM0e":"tok"`)),
		}, nil
	}})

	// 代理：fake-exit 的 stdlib 客户端换成记头的 RoundTripper。
	const fakeProxy = "http://fake-exit.invalid:3128"
	var proxyHdr http.Header
	stdlibClientCache.Store(fakeProxy+"|"+fmt.Sprint(rtCfg().RequestTimeout),
		&http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			proxyHdr = req.Header.Clone()
			return &http.Response{
				StatusCode: 200, Status: "200 OK",
				Header: http.Header{"Set-Cookie": []string{"NID=test; Path=/"}},
				Body:   io.NopCloser(strings.NewReader(`"SNlM0e":"tok"`)),
			}, nil
		})})
	defer stdlibClientCache.Delete(fakeProxy + "|" + fmt.Sprint(rtCfg().RequestTimeout))

	entries := []struct {
		name string
		call func(proxyURL string) error
	}{
		{"fetchAppPageCtx", func(proxyURL string) error {
			_, err := fetchAppPageCtx(context.Background(), "", 0, proxyURL)
			return err
		}},
		{"getAnonSession", func(proxyURL string) error {
			_, err := getAnonSession(proxyURL)
			return err
		}},
	}
	for _, e := range entries {
		directHdr, proxyHdr = nil, nil
		if err := e.call(""); err != nil {
			t.Fatalf("%s 直连失败: %v", e.name, err)
		}
		if directHdr == nil {
			t.Fatalf("%s: 直连没发出请求", e.name)
		}
		check(t, e.name+"/直连", directHdr.Get)

		if err := e.call(fakeProxy); err != nil {
			t.Fatalf("%s 代理失败: %v", e.name, err)
		}
		if proxyHdr == nil {
			t.Fatalf("%s: 代理没发出请求", e.name)
		}
		check(t, e.name+"/代理", proxyHdr.Get)
	}
}

// ─── 单次请求配置快照一致性回归（must fix） ─────────────────────────────────
//
// rtVal 可热更新：writeBrowserHeaders 旧实现分次读，并发 PUT /admin/api/config 改
// impersonate / firefox_fingerprint 时，单个请求可能混用两版（Chrome/133 UA +
// v=146 Sec-CH-UA，或 Firefox UA + Chrome hints）。这里边跑边翻转配置，要求
// 每次产出的 UA 大版本与 Sec-CH-UA 大版本自洽（Firefox 则必须无 hints）。
func TestBrowserHeaderSnapshotConsistency(t *testing.T) {
	old := rtCfg()
	defer func() { rtMu.Lock(); rtVal = old; rtMu.Unlock() }()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ff := false
		for {
			select {
			case <-stop:
				return
			default:
			}
			rtMu.Lock()
			ff = !ff
			rtVal.FirefoxFingerprint = ff
			if ff {
				rtVal.Impersonate = "chrome_133"
			} else {
				rtVal.Impersonate = "chrome_146"
			}
			rtMu.Unlock()
		}
	}()
	defer func() { close(stop); wg.Wait() }()

	uaRe := regexp.MustCompile(`Chrome/(\d+)\.`)
	hintRe := regexp.MustCompile(`"Google Chrome";v="(\d+)"`)
	for i := 0; i < 50000; i++ {
		h := http.Header{}
		writeBrowserHeaders("gemini.google.com", h.Set, h.Del)
		ua := h.Get("User-Agent")
		if strings.Contains(ua, "Firefox/") {
			for _, k := range []string{"Sec-CH-UA", "Sec-CH-UA-Mobile", "Sec-CH-UA-Platform"} {
				if v := h.Get(k); v != "" {
					t.Fatalf("迭代 %d: Firefox UA 却带了 %s=%q（UA=%q）", i, k, v, ua)
				}
			}
			continue
		}
		um := uaRe.FindStringSubmatch(ua)
		vm := hintRe.FindStringSubmatch(h.Get("Sec-CH-UA"))
		if um == nil || vm == nil {
			t.Fatalf("迭代 %d: 头形状不对 UA=%q Sec-CH-UA=%q", i, ua, h.Get("Sec-CH-UA"))
		}
		if um[1] != vm[1] {
			t.Fatalf("迭代 %d: 单请求混用了两版配置 UA=%q Sec-CH-UA=%q", i, ua, h.Get("Sec-CH-UA"))
		}
	}
}
