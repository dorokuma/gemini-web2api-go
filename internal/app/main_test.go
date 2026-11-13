package app

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	tls_client "github.com/bogdanfinn/tls-client"
)

// TestMain 把 DB 指向临时目录再跑测试。
//
// hasCookie() 现在只看 cookie 池，也就是要查 DB；不接管 cfg.DBPath 的话
// getDB() 会去开真实的 ./data/gemini.db，测试就会读写生产数据。
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "gw2a-test-")
	if err != nil {
		panic(err)
	}
	cfg.DBPath = filepath.Join(dir, "test.db")
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// withPoolCookie 往 cookie 池塞一条测试账号，用完自动删掉。
// 取代原来的 cookieRuntime.Store —— 那条单 cookie 路径已经取消。
func withPoolCookie(t *testing.T) {
	t.Helper()
	id, err := accountAdd("test", "SAPISID=dummy; SID=x", "")
	if err != nil {
		t.Fatalf("插测试 cookie 失败: %v", err)
	}
	t.Cleanup(func() { _ = accountDelete(id) })
}

// useInsecureTLSClients 把直连用的两个 tls-client 换成不校验证书的同款，好打本机的
// HTTPS 测试服务器。限时的那个给上传等请求，不限时的给 StreamGenerate（getTLSStreamClient）；
// 除了不校验证书，配置跟线上一样（newTLSClient）。
func useInsecureTLSClients(t *testing.T) {
	t.Helper()
	imp := effectiveImpersonate()
	for _, timeout := range []int{rtCfg().RequestTimeout, 0} {
		c, err := newTLSClient(imp, timeout, tls_client.WithInsecureSkipVerify())
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s|%d", imp, timeout)
		clientCacheMu.Lock()
		prev, had := clientCache[key]
		clientCache[key] = c
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
	}
}

// withRequestTimeout 把面板的 request_timeout_sec 临时改成 sec 秒。
func withRequestTimeout(t *testing.T, sec int) {
	t.Helper()
	rtMu.Lock()
	saved := rtVal.RequestTimeout
	rtVal.RequestTimeout = sec
	rtMu.Unlock()
	t.Cleanup(func() { rtMu.Lock(); rtVal.RequestTimeout = saved; rtMu.Unlock() })
}
