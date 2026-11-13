package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// 同一个账号的请求不能因为走没走代理而呈现两种客户端：两条路径都要带齐 chromeHeaders，
// 调用方自己的 header 照样生效。直连那条以前一个都不带，UA 是 Go-http-client/2.0。
//
// 直连走 tls-client，这里换成不校验证书的同款客户端，打本机的 HTTPS 测试服务器；
// 代理那条把 http:// 目标以绝对 URI 发给代理，测试服务器本身就能当代理（同 cancel_test）。
func TestDirectAndProxyPathsSendSameBrowserHeaders(t *testing.T) {
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
	}
	for _, p := range paths {
		clear(got)
		p.send(direct.URL+"/x", "")
		p.send("http://upstream.invalid/x", proxy.URL)
		d, pr := got["direct"], got["proxy"]
		if d == nil || pr == nil {
			t.Fatalf("%s: 没收到请求（direct=%v proxy=%v）", p.name, d != nil, pr != nil)
		}
		if ua := d.Get("User-Agent"); ua != ChromeUA {
			t.Errorf("%s: 直连 UA = %q", p.name, ua)
		}
		for _, kv := range chromeHeaders {
			if d.Get(kv[0]) != pr.Get(kv[0]) {
				t.Errorf("%s: %s 直连 %q，代理 %q", p.name, kv[0], d.Get(kv[0]), pr.Get(kv[0]))
			}
		}
		if p.name != "image" {
			for k, v := range own {
				if d.Get(k) != v || pr.Get(k) != v {
					t.Errorf("%s: 调用方的 %s 没生效（直连 %q，代理 %q）", p.name, k, d.Get(k), pr.Get(k))
				}
			}
		}
	}
}
