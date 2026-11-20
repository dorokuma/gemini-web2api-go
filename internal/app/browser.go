package app

// browser.go —— 常驻真实浏览器传输层（策略 M1：单浏览器主号粘性长驻）。
//
// 顶层结构：
//
//	browserManager（全局单例）
//	  ├─ spawn 出来的 Chromium（headless=new，profileDir 0700）
//	  ├─ browserProxyBridge（127.0.0.1:0 CONNECT 转发，--proxy-server 只指它）
//	  ├─ cdpClient（DevTools WS 多路复用）
//	  ├─ browserSession（Target.attachToTarget 拿到的 page session，flatten 模式）
//	  └─ streamRegistry（reqID → *browserStream 非阻塞队列 + io.Pipe 输出）
//
// 策略 M1 的关键约束：
//   - 单浏览器、单 Tab：同 cookieIdentity 下 authuser=0..9 的请求共用一条
//     page session；切号只发生在「驻留号被停用/删除、鉴权失效、或 strict_model
//     冷却进入 exclude」时，切号要走 30s 最小冷却 + 先写回旧号 Cookie 再清
//     浏览器 Cookie 再播种新号；
//   - Cookie 反向写回：上游几乎每个响应都在刷 SIDCC/*SIDCC，浏览器收下后要
//     节流回写 DB，否则重启后池里还是旧值——旧值发出去会被当成过期会话。
//   - 严禁 Network.enable：DevTools 协议一旦 enable，Chromium 会把所有响应体
//     通过 Network.dataReceived 推给调试端，常驻等于内存泄漏；我们只用
//     Network.setCookies / getCookies / clearBrowserCookies 这三个独立命令，
//     它们不打开网络抓包。

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// browserLocalError 是浏览器传输层自身的错（不是上游 HTTP 状态）：
// 没装 Chromium、启动失败、WS 断、CDP 调用错、流被 abort。
// isBrowserLocalError 判断它，请求路径据此决定要不要回 502 browser_unavailable
// 而不是降级走 HTTP。
type browserLocalError struct{ err error }

func (e *browserLocalError) Error() string { return "browser: " + e.err.Error() }
func (e *browserLocalError) Unwrap() error { return e.err }

func isBrowserLocalError(err error) bool {
	var be *browserLocalError
	return errors.As(err, &be)
}

func browserErr(format string, args ...interface{}) error {
	return &browserLocalError{err: fmt.Errorf(format, args...)}
}

// ─── 启动参数 ────────────────────────────────────────────────────────────────

// discoverBrowserBinary 找 Chromium 系可执行文件。explicitPath 非空时严格
// 使用它（不存在或不可执行直接报错，不回退探测——用户显式指了路径就该
// 立刻看到错误，而不是静默换成系统里另一个浏览器）。
func discoverBrowserBinary(explicitPath string) (string, error) {
	if explicitPath != "" {
		info, err := os.Stat(explicitPath)
		if err != nil {
			return "", browserErr("explicit browser path: %w", err)
		}
		if info.IsDir() || info.Mode()&0o111 == 0 {
			return "", browserErr("explicit browser path %q is not executable", explicitPath)
		}
		return explicitPath, nil
	}
	var candidates []string
	switch runtime.GOOS {
	case "linux":
		candidates = []string{
			"chromium", "chromium-browser", "google-chrome",
			"google-chrome-stable", "brave", "brave-browser",
			"microsoft-edge", "msedge",
		}
	case "darwin":
		candidates = []string{
			"/Applications/Chromium.app/Contents/MacOS/Chromium",
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
			"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
		}
	case "windows":
		// 注册表查询太重；常见安装路径覆盖绝大多数部署。
		pf := os.Getenv("ProgramFiles")
		pf86 := os.Getenv("ProgramFiles(x86)")
		la := os.Getenv("LOCALAPPDATA")
		for _, root := range []string{pf, pf86, la} {
			if root == "" {
				continue
			}
			for _, rel := range []string{
				`Google\Chrome\Application\chrome.exe`,
				`Chromium\Application\chrome.exe`,
				`BraveSoftware\Brave-Browser\Application\brave.exe`,
				`Microsoft\Edge\Application\msedge.exe`,
			} {
				candidates = append(candidates, filepath.Join(root, rel))
			}
		}
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	// PATH 兜底（linux 名字在上面，这里再查一次是因为 PATH 里的别名可能不同）
	for _, name := range []string{"chromium", "chromium-browser", "google-chrome", "brave", "msedge"} {
		if p, err := exec.LookPath(name); err == nil {
			return p, nil
		}
	}
	return "", browserErr("no Chromium-family browser found (install chromium/google-chrome/brave/edge, or set browser_path)")
}

// ─── 策略 M1 的状态 ──────────────────────────────────────────────────────────

// browserSeedState 是 profileDir/seed-state.json 里记的策略 M1 粘性状态：
// 当前浏览器里登着哪个账号。重启进程后靠它决定要不要重新播种 Cookie。
type browserSeedState struct {
	AccountID      int64  `json:"account_id"`
	CookieIdentity string `json:"cookie_identity"` // cookieIdentity(cookie)，不是整串
	SeededAt       int64  `json:"seeded_at"`
}

// browserManager 是全局单例：整个进程最多一个常驻 Chromium。
// 所有字段的访问规则：
//   - cfgOnce / readyCh：启动单飞；
//   - mu：保护 proc / client / session / seedState / shuttingDown；
//   - streamReg：自己的锁。
type browserManager struct {
	// 单飞启动：ensureRunning 只让第一个调用方真启动，其它等在 readyCh 上。
	startMu sync.Mutex
	readyCh chan struct{}
	ready   atomic.Bool
	started bool

	mu           sync.Mutex
	proc         *browserProc
	client       *cdpClient
	sessionID    string // 主 Tab 的 CDP sessionId（flatten 模式）
	targetID     string
	profileDir   string
	bridge       *browserProxyBridge
	seed         browserSeedState
	lastSwitchAt time.Time // 策略 M1 的 30s 切号冷却
	crashStreak  int
	stableSince  time.Time // 上次崩溃后稳定运行起点，>60s 清零 crashStreak
	shuttingDown bool

	// 流式管道注册表：CDP 读循环收到 Runtime.bindingCalled 后按 reqID
	// 找到对应 browserStream 推数据。
	streamRegMu sync.Mutex
	streams     map[string]*browserStream

	// Cookie 反向同步节流：同一账号两次写回至少隔这么久，
	// 上游刷 SIDCC 很勤，每次都 UPDATE 会变成 DB 噪音。
	lastCookieSync time.Time
}

// browser 全局单例。
var browserMgr = &browserManager{
	readyCh: make(chan struct{}),
	streams: map[string]*browserStream{},
}

// browserSwitchCooldown 是策略 M1 的最小切号冷却。低于这个间隔的换号请求
// 会让调用方等到冷却满——频繁换号本身就是异常信号，宁可拖慢也不让
// 浏览器 Cookie 在 1 秒内反复清种。
const browserSwitchCooldown = 30 * time.Second

// cookieSyncThrottle 是反向同步的最小间隔。
const cookieSyncThrottle = 30 * time.Second

// ─── 启动与就绪 ─────────────────────────────────────────────────────────────

// ensureRunning 是单飞入口：第一次调用真启动浏览器；并发调用方都等 readyCh
// 且各自响应 ctx.Done()（调用方取消不该拖住其它等待者）。
func (m *browserManager) ensureRunning(ctx context.Context) error {
	m.startMu.Lock()
	if !m.started {
		m.started = true
		go m.startOnce()
	}
	ready := m.readyCh
	m.startMu.Unlock()

	select {
	case <-ready:
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.proc == nil || m.client == nil {
			return browserErr("browser failed to start")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startOnce 真正把整条链路拉起来：探测二进制 → 建 profileDir → 起 proxy
// bridge → spawn Chromium → 读 DevToolsActivePort → 连 WS → attach 主 Tab
// → 注入 __gwBridge → 恢复 seed-state。
func (m *browserManager) startOnce() {
	err := m.doStart()
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		logf("[browser] 启动失败: %v", err)
		m.proc = nil
		m.client = nil
		m.sessionID = ""
		// 失败不 retry：调用方拿到 browser_unavailable，由上层决定是否
		// 走 browser_fallback_http。自动重试只会把启动成本摊进请求延迟。
	}
	close(m.readyCh)
	m.ready.Store(err == nil)
}

func (m *browserManager) doStart() error {
	bin, err := discoverBrowserBinary(cfgBrowserPath())
	if err != nil {
		return err
	}
	profileDir, err := browserProfileDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(profileDir, 0o700); err != nil {
		return browserErr("profile dir: %w", err)
	}
	// 启动前先回收上一轮的孤儿（进程被 SIGKILL 的话 browser-instance.json
	// 还在，Chromium 进程组可能还活着吃内存）。
	reclaimOrphanBrowser(profileDir)

	bridge, err := newBrowserProxyBridge()
	if err != nil {
		return err
	}
	m.bridge = bridge

	args := browserLaunchArgs(profileDir, bridge.Port())
	proc, err := spawnBrowser(bin, args, nil)
	if err != nil {
		bridge.Shutdown(context.Background())
		return browserErr("spawn: %w", err)
	}
	m.proc = proc
	if err := writeBrowserInstance(profileDir, proc); err != nil {
		logf("[browser] browser-instance.json 写失败（孤儿回收下次可能失效）: %v", err)
	}

	// DevToolsActivePort 是 Chromium 写进 profileDir 的「我监听了哪个端口」，
	// --remote-debugging-port=0 让它自己挑空闲端口，我们只认这个文件。
	wsURL, err := waitDevToolsPort(profileDir, proc, 15*time.Second)
	if err != nil {
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
		bridge.Shutdown(context.Background())
		return err
	}
	conn, err := dialCDPWebsocket(context.Background(), wsURL)
	if err != nil {
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
		bridge.Shutdown(context.Background())
		return browserErr("cdp dial: %w", err)
	}
	client := newCDPClient(conn)
	m.client = client
	m.registerCDPEvents(client)

	// attach 到 browser target 下创建的 page。主 Tab 必须停在
	// https://gemini.google.com/app：页内 fetch 是同源请求，没有 CORS 拦截
	// （about:blank origin 发 fetch 会被 Chromium 按跨域拒掉——Failed to fetch）。
	// flatten 模式让所有会话消息都走同一条 WS、用 sessionId 路由——
	// 不开 flatten 每条 session 要单独连接，不符合我们单 WS 的假设。
	var created struct {
		TargetID string `json:"targetId"`
	}
	if err := client.callCDPWithTimeout(context.Background(), 15*time.Second, "",
		"Target.createTarget",
		map[string]interface{}{"url": "https://gemini.google.com/app"}, &created); err != nil {
		client.Close()
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
		bridge.Shutdown(context.Background())
		return browserErr("create target: %w", err)
	}
	m.targetID = created.TargetID
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := client.callCDPWithTimeout(context.Background(), 10*time.Second, "",
		"Target.attachToTarget",
		map[string]interface{}{"targetId": m.targetID, "flatten": true},
		&attached); err != nil {
		client.Close()
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
		bridge.Shutdown(context.Background())
		return browserErr("attach target: %w", err)
	}
	m.sessionID = attached.SessionID

	if err := m.enablePageDomain(); err != nil {
		client.Close()
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
		bridge.Shutdown(context.Background())
		return err
	}
	if err := m.installBridge(); err != nil {
		client.Close()
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
		bridge.Shutdown(context.Background())
		return err
	}
	if err := m.restoreSeedState(); err != nil {
		logf("[browser] seed-state 恢复失败（将按未播种处理）: %v", err)
	}
	m.stableSince = time.Now()
	logf("[browser] 常驻浏览器已就绪 (pid=%d, port=%d)", proc.PID, bridge.Port())
	return nil
}

// browserLaunchArgs 拼启动参数。--no-sandbox 只在 Linux + euid==0 追加：
// 容器里 root 起 Chromium 必须带它才能起来，非 root 环境带上反而弱化沙箱。
func browserLaunchArgs(profileDir string, bridgePort int) []string {
	args := []string{
		"--headless=new",
		"--user-data-dir=" + profileDir,
		"--remote-debugging-port=0",
		"--remote-debugging-address=127.0.0.1",
		"--proxy-server=http://127.0.0.1:" + strconv.Itoa(bridgePort),
		"--no-first-run",
		"--no-default-browser-check",
		"--disable-extensions",
		"--disable-component-update",
		"--disable-sync",
		"--disable-default-apps",
		"--metrics-recording-only",
		"--mute-audio",
		"--no-pings",
		"--disable-dev-shm-usage",
		"--disable-gpu",
		"--disable-background-timer-throttling",
		"--disable-backgrounding-occluded-windows",
		"--disable-renderer-backgrounding",
		"about:blank",
	}
	if runtime.GOOS == "linux" && os.Geteuid() == 0 {
		args = append([]string{args[0], "--no-sandbox"}, args[1:]...)
	}
	return args
}

// browserProfileDir 决定 profile 落点。优先级：--browser-profile-dir /
// config.json browser_profile_dir > BROWSER_PROFILE_DIR 环境变量 >
// <db 目录>/browser-profile（跟 DBPath 同级放，升级/容器重建跟数据一起挂卷）。
func browserProfileDir() (string, error) {
	if cfg.BrowserProfileDir != "" {
		return cfg.BrowserProfileDir, nil
	}
	if env := os.Getenv("BROWSER_PROFILE_DIR"); env != "" {
		return env, nil
	}
	base := filepath.Dir(cfg.DBPath)
	if base == "" || base == "." {
		base = "./data"
	}
	return filepath.Join(base, "browser-profile"), nil
}

// cfgBrowserPath 取配置里的显式浏览器路径（--browser-path / config.json 的
// browser_path > BROWSER_PATH 环境变量；运行时不可改——改浏览器路径本来就要
// 重启）。RuntimeConfig 不存它，/admin 默认免鉴权，面板可写等于本地任意
// 可执行文件执行。
func cfgBrowserPath() string {
	if cfg.BrowserPath != "" {
		return cfg.BrowserPath
	}
	return os.Getenv("BROWSER_PATH")
}

// shouldUseBrowserTransport 是业务接线的总开关判断：browser_transport 开着才
// 走浏览器路径；关掉时一切照旧（默认就是关，零开销）。
func shouldUseBrowserTransport() bool {
	return rtCfg().BrowserTransport
}

// browserAccountFromHeaders 从请求头里的 Cookie 反查池子里的账号：
// doGeminiRequestStopInner 只拿到 headers，而策略 M1 切号/写回都需要 *CookieAccount。
// 按 cookieIdentity（SAPISID + __Secure-1PSID）匹配——账号的 *SIDCC 会被上游刷，
// 按整串对不上。匿名请求（无 Cookie 头）返回 nil，走浏览器的匿名会话。
func browserAccountFromHeaders(headers map[string]string) *CookieAccount {
	c := headers["Cookie"]
	if c == "" {
		return nil
	}
	id := cookieIdentity(c)
	if id == "|" {
		return nil
	}
	for _, a := range accountList() {
		if cookieIdentity(a.Cookie) == id {
			cp := a
			return &cp
		}
	}
	return nil
}

// stickyResidentAccount 返回策略 M1 的常驻粘性主号：浏览器已播种且该号仍然
// 启用、不在 exclude 里时，优先让它继续驻留（同 cookieIdentity 的 authuser=0..9
// 共用同一 /app Tab）。不在 exclude 中的驻留号优先于 pickAccountForModel 的
// 常规轮换——M1 的核心就是「能不动就不动」。
// 浏览器还没起过（seed.AccountID==0）或驻留号该换时返回 nil，由调用方走常规挑号。
func stickyResidentAccount(exclude map[int64]bool) *CookieAccount {
	browserMgr.mu.Lock()
	sid := browserMgr.seed.AccountID
	browserMgr.mu.Unlock()
	if sid == 0 || exclude[sid] {
		return nil
	}
	a := accountByID(sid)
	if a == nil || a.Status != "enabled" {
		return nil
	}
	return a
}

// syncResidentCookiesBack 是 rotateAllAccounts 在浏览器模式下的替代路径：
// 常驻浏览器自己就是 cookie 的权威 jar，上游刷的 SIDCC 它都收下了，
// 不该再拿 DB 里的旧串去打 /RotateCookies 双端抢刷（刷了也进不了浏览器 jar）。
// 这里把 jar 当前值立刻（绕过节流）写回 DB。
func syncResidentCookiesBack() {
	m := browserMgr
	m.mu.Lock()
	seed := m.seed
	m.mu.Unlock()
	if seed.AccountID == 0 {
		return
	}
	// 绕过节流写回一次：轮转周期（分钟级）远大于 cookieSyncThrottle，不冲突。
	m.mu.Lock()
	m.lastCookieSync = time.Time{}
	m.mu.Unlock()
	m.syncCookiesBack()
}

// reloadResidentPage 在 XSRF 400 这类「页面 token 失效」时让主 Tab 重新拿一份
// /app 上下文：fetch /app 页面本身就能让 WIZ_global_data 刷新（见
// fetchAppTokensViaBrowser 的注释——页内 fetch 拿到的 HTML 里就有新 token）。
// 调它等价于作废本地 XSRF 缓存后重取，调用方负责 invalidateXSRF。
func (m *browserManager) reloadResidentPage(ctx context.Context, account *CookieAccount) error {
	_, err := m.fetchAppTokensViaBrowser(ctx, account, nil)
	return err
}

// shutdownBrowser 回收常驻浏览器。saveRuntimeConfig 在 browser_transport 被
// 关掉时异步调用（关掉就该立刻释放 ~1.45GB 内存，不该等下次进程重启）。
// 幂等；浏览器没起过时直接返回。
func shutdownBrowser() {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	browserMgr.Shutdown(ctx)
}

// browserRunning 报告常驻浏览器当前是否活着（管理面板只读展示用）。
func browserRunning() bool {
	browserMgr.mu.Lock()
	defer browserMgr.mu.Unlock()
	return browserMgr.proc != nil && browserMgr.client != nil && pidAlive(browserMgr.proc.PID)
}

// browserDetected 探测本机可用的浏览器路径。管理面板只读展示用；
// 显式配置了 browser_path 时原样返回（探测失败会在拉起时才暴露）。
func browserDetected() string {
	if p := cfgBrowserPath(); p != "" {
		return p
	}
	p, err := discoverBrowserBinary("")
	if err != nil {
		return ""
	}
	return p
}

// waitDevToolsPort 等 Chromium 把 DevToolsActivePort 写进 profileDir。
// 文件第一行是端口，第二行是 WS 路径（/devtools/browser/<uuid>）。
func waitDevToolsPort(profileDir string, proc *browserProc, timeout time.Duration) (string, error) {
	path := filepath.Join(profileDir, "DevToolsActivePort")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !pidAlive(proc.PID) {
			return "", browserErr("browser exited during startup (check chromium install)")
		}
		raw, err := os.ReadFile(path)
		if err == nil {
			lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
			if len(lines) >= 2 {
				port := strings.TrimSpace(lines[0])
				wsPath := strings.TrimSpace(lines[1])
				return "ws://127.0.0.1:" + port + wsPath, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", browserErr("DevToolsActivePort not written within %s", timeout)
}

// ─── CDP 事件 ───────────────────────────────────────────────────────────────

func (m *browserManager) registerCDPEvents(c *cdpClient) {
	// 流式桥：页面 JS 把 fetch 的分片通过 Runtime.bindingCalled("__gwPush")
	// 推回来，params.payload 是 JSON 字符串。
	c.On("Runtime.bindingCalled", func(evt cdpEvent) {
		if evt.SessionID != "" && evt.SessionID != m.sessionID {
			return // 别的 target 上的绑定调用不管
		}
		var p struct {
			Name    string `json:"name"`
			Payload string `json:"payload"`
		}
		if json.Unmarshal(evt.Params, &p) != nil || p.Name != "__gwPush" {
			return
		}
		m.handleBridgePush(p.Payload)
	})
	// 渲染进程崩溃：Inspector.targetCrashed 在我们 attach 的 session 上来。
	c.On("Inspector.targetCrashed", func(evt cdpEvent) {
		if evt.SessionID != "" && evt.SessionID != m.sessionID {
			return
		}
		logf("[browser] target crashed, recreating tab")
		go m.recreateTab()
	})
	c.OnAny(func(evt cdpEvent) {
		// Target.targetDestroyed 里如果是我们的主 Tab 死了，也要重建。
		if evt.Method != "Target.targetDestroyed" {
			return
		}
		var p struct {
			TargetID string `json:"targetId"`
		}
		if json.Unmarshal(evt.Params, &p) != nil || p.TargetID == "" {
			return
		}
		m.mu.Lock()
		isMain := p.TargetID == m.targetID
		m.mu.Unlock()
		if isMain {
			logf("[browser] main tab destroyed, recreating")
			go m.recreateTab()
		}
	})
}

// recreateTab 是第一级自愈：渲染进程崩了但浏览器还活着时，只重建单 Tab
// 并重新 attach + 重新注入桥。浏览器/WS 断开那一级由 ensureRunning 之后的
// 调用失败路径触发 scheduleReconnect（见 doBrowserStreamRequest 的 err 分支）。
func (m *browserManager) recreateTab() {
	m.mu.Lock()
	if m.shuttingDown || m.client == nil {
		m.mu.Unlock()
		return
	}
	client := m.client
	oldTarget := m.targetID
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if oldTarget != "" {
		_ = client.Call(ctx, "", "Target.closeTarget",
			map[string]interface{}{"targetId": oldTarget}, nil)
	}
	var created struct {
		TargetID string `json:"targetId"`
	}
	// 同 doStart：主 Tab 停在 /app，页内 fetch 才能同源。
	if err := client.Call(ctx, "", "Target.createTarget",
		map[string]interface{}{"url": "https://gemini.google.com/app"}, &created); err != nil {
		logf("[browser] recreate target failed: %v", err)
		return
	}
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := client.Call(ctx, "", "Target.attachToTarget",
		map[string]interface{}{"targetId": created.TargetID, "flatten": true}, &attached); err != nil {
		logf("[browser] reattach failed: %v", err)
		return
	}
	m.mu.Lock()
	m.targetID = created.TargetID
	m.sessionID = attached.SessionID
	m.mu.Unlock()
	if err := m.enablePageDomain(); err != nil {
		logf("[browser] re-enable page: %v", err)
	}
	if err := m.installBridge(); err != nil {
		logf("[browser] re-install bridge: %v", err)
	}
	// 浏览器里旧 Cookie 还在（target 崩了不清 jar），不必重播种。
	m.failAllStreams(browserErr("tab recreated; previous streams aborted"))
}

// ─── Page 域与桥注入 ────────────────────────────────────────────────────────

func (m *browserManager) enablePageDomain() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return m.client.Call(ctx, m.sessionID, "Page.enable", nil, nil)
}

// installBridge 注入 window.__gwBridge：Runtime.addBinding("__gwPush") 让页面
// JS 能反向调我们；Page.addScriptToEvaluateOnNewDocument 让每次导航后桥还在。
//
// 桥做的事：
//   - __gwBridge.fetch(reqID, opts)：同源 fetch（redirect: "manual"），
//     响应头立刻通过 __gwPush({reqID, kind:"headers", ...}) 推回，
//     body 逐 chunk base64 推回（__gwPush 的 payload 长度限制约 100MB，
//     分 chunk 是安全值）；
//   - __gwBridge.abort(reqID)：AbortController.abort() 打断在飞 fetch；
//   - 结束时推 {kind:"end", status} 或 {kind:"error", message}。
//
// 为什么用 bindingCalled 而不是 console.log：binding 是双向命名通道，
// 参数直接序列化成字符串，不用劫持 console 也不用收日志噪声；而且
// Runtime.addBinding 是 CDP 专门为「页面→外部」设计的机制。
func (m *browserManager) installBridge() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// addBinding 只对未来/现存 context 生效；页面加载中的旧 context 销毁后
	// binding 随之消失，所以必须等页面就位（waitPageLoad）再注册——否则
	// __gwPush 在加载完的 document 里是 undefined，push 全部被 JS 里的
	// try/catch 静默吞掉，流永远等不到 headers（实测就是这么挂死的）。
	m.waitPageLoad(ctx)
	if err := m.client.Call(ctx, m.sessionID, "Runtime.addBinding",
		map[string]interface{}{"name": "__gwPush"}, nil); err != nil {
		return browserErr("addBinding: %w", err)
	}
	const src = `window.__gwBridge = (function() {
  var controllers = {};
  function push(obj) {
    try { __gwPush(JSON.stringify(obj)); } catch (e) {}
  }
  return {
    fetch: function(reqID, url, opts) {
      var ac = new AbortController();
      controllers[reqID] = ac;
      var headers = (opts && opts.headers) || {};
      return fetch(url, {
        method: (opts && opts.method) || "POST",
        headers: headers,
        body: (opts && opts.body) || undefined,
        redirect: "manual",
        credentials: "include",
        signal: ac.signal
      }).then(function(resp) {
        var hdrs = {};
        resp.headers.forEach(function(v, k) { hdrs[k] = v; });
        push({reqID: reqID, kind: "headers", status: resp.status,
              statusText: resp.statusText, url: resp.url, headers: hdrs});
        var reader = resp.body.getReader();
        function pump() {
          return reader.read().then(function(r) {
            if (r.done) {
              push({reqID: reqID, kind: "end", status: resp.status});
              delete controllers[reqID];
              return;
            }
            var bytes = r.value;
            var bin = "";
            for (var i = 0; i < bytes.length; i += 8192) {
              bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 8192));
            }
            push({reqID: reqID, kind: "chunk", data: btoa(bin)});
            return pump();
          });
        }
        return pump().catch(function(e) {
          push({reqID: reqID, kind: "error", message: String(e)});
          delete controllers[reqID];
        });
      }).catch(function(e) {
        push({reqID: reqID, kind: "error", message: String(e)});
        delete controllers[reqID];
      });
    },
    abort: function(reqID) {
      var c = controllers[reqID];
      if (c) { c.abort(); delete controllers[reqID]; }
    }
  };
})();`
	// 两条腿：addScriptToEvaluateOnNewDocument 保证**将来的导航**仍有桥；
	// 但当前页面的 document 可能已经在加载中（createTarget 带 url 就开了导航），
	// 钩子的时序不保证赶得上这第一页——所以等 load 后再就地 evaluate 注入一次，
	// 覆盖两种时序（重复注入幂等，window.__gwBridge 覆盖无副作用）。
	if err := m.client.Call(ctx, m.sessionID, "Page.addScriptToEvaluateOnNewDocument",
		map[string]interface{}{"source": src}, nil); err != nil {
		return err
	}
	return m.client.Call(ctx, m.sessionID, "Runtime.evaluate",
		map[string]interface{}{"expression": src, "awaitPromise": false}, nil)
}

// ensureBridge 确认当前页上下文里 __gwPush（binding）与 __gwBridge（fetch 桥）
// 都在；任一缺失就重跑 installBridge（幂等）。这是 doBrowserStreamRequest 的
// 每次前置检查——CDP addBinding/addScriptToEvaluateOnNewDocument 只对之后的
// context 生效，页面导航会重建 context 把注入丢掉，靠一次 install 的时序
// 永远有缝（实测 install 时页面还在 loading，注入落到旧 context）。
func (m *browserManager) ensureBridge(ctx context.Context) error {
	m.mu.Lock()
	client := m.client
	sess := m.sessionID
	m.mu.Unlock()
	if client == nil {
		return browserErr("cdp client gone")
	}
	var out struct {
		Result struct {
			Value string `json:"value"`
		} `json:"result"`
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := client.Call(cctx, sess, "Runtime.evaluate",
		map[string]interface{}{
			"expression":   `typeof __gwPush + "/" + typeof window.__gwBridge`,
			"awaitPromise": false,
		}, &out)
	cancel()
	if err == nil && out.Result.Value == "function/object" {
		return nil
	}
	logf("[browser] 桥不完整（%q），重新注入", out.Result.Value)
	return m.installBridge()
}

// waitPageLoad 等当前主 Tab 到可注入状态：document.readyState 已经
// interactive/complete 就直接返回；否则等 Page.loadEventFired（Once，触发即摘）。
// 超时只记日志不报错——注入失败会在后续 fetch 时以「无 push」暴露，
// 不该让整个启动失败。
func (m *browserManager) waitPageLoad(ctx context.Context) {
	m.mu.Lock()
	client := m.client
	sess := m.sessionID
	m.mu.Unlock()
	if client == nil {
		return
	}
	// 先查 readyState：页面可能已经加载完（loadEventFired 在我们注册前就发了，
	// 等一个已经发生的事件会空等超时）。
	var ready struct {
		Result struct {
			Value string `json:"value"`
		} `json:"result"`
	}
	rctx, rcancel := context.WithTimeout(ctx, 3*time.Second)
	err := client.Call(rctx, sess, "Runtime.evaluate",
		map[string]interface{}{"expression": "document.readyState", "awaitPromise": false}, &ready)
	rcancel()
	if err == nil && (ready.Result.Value == "interactive" || ready.Result.Value == "complete") {
		return
	}
	done := make(chan struct{}, 1)
	// Once：触发即摘除，不会随 installBridge 重入累积。
	client.Once("Page.loadEventFired", func(evt cdpEvent) {
		if evt.SessionID == "" || evt.SessionID == sess {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	lctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	select {
	case <-done:
	case <-lctx.Done():
		logf("[browser] 等页面 load 超时（继续，桥将在下次导航时注入）")
	}
}

// ─── Cookie 播种 / 反向同步 / 策略 M1 切号 ───────────────────────────────────

func (m *browserManager) restoreSeedState() error {
	raw, err := os.ReadFile(filepath.Join(m.profileDir, "seed-state.json"))
	if err != nil {
		return nil // 没播种过，正常
	}
	var s browserSeedState
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	m.seed = s
	return nil
}

func (m *browserManager) persistSeedState() {
	b, err := json.Marshal(m.seed)
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(m.profileDir, "seed-state.json"), b, 0o600)
}

// seedAccount 把账号 a 的 Cookie 播进浏览器 jar。
//
// 严格顺序（策略 M1）：先把旧驻留号当前 jar 写回 DB（可能已刷出新 SIDCC），
// 再 clearBrowserCookies，再 setCookies 新号。旧号不清就播种，新 cookie 会
// 叠在旧的上——Google 看到两套身份打架的 cookie 串，结果不可预期。
func (m *browserManager) seedAccount(ctx context.Context, a *CookieAccount) error {
	if a == nil || a.Cookie == "" {
		return nil
	}
	cookies := cookieToCDP(a.Cookie)
	if len(cookies) == 0 {
		return browserErr("account #%d cookie is empty after parse", a.ID)
	}
	if err := m.client.Call(ctx, m.sessionID, "Network.setCookies",
		map[string]interface{}{"cookies": cookies}, nil); err != nil {
		return browserErr("setCookies: %w", err)
	}
	m.mu.Lock()
	m.seed = browserSeedState{
		AccountID:      a.ID,
		CookieIdentity: cookieIdentity(a.Cookie),
		SeededAt:       time.Now().Unix(),
	}
	m.mu.Unlock()
	m.persistSeedState()
	return nil
}

// cookieToCDP 把 "k=v; k=v" 串转成 Network.setCookies 的 cookieParam 数组。
// domain 写死 .google.com：Gemini 的 cookie 都在这个域；path=/、secure、
// httpOnly 按 DevTools 最小必填集填。sameSite 不填（CDP 默认可放）。
func cookieToCDP(cookie string) []map[string]interface{} {
	var out []map[string]interface{}
	for _, kv := range splitCookiePairs(cookie) {
		name, val := kv[0], kv[1]
		if name == "" {
			continue
		}
		out = append(out, map[string]interface{}{
			"name":     name,
			"value":    val,
			"domain":   ".google.com",
			"path":     "/",
			"secure":   true,
			"httpOnly": strings.HasPrefix(name, "__Secure-") || strings.HasPrefix(name, "__Host-"),
		})
	}
	return out
}

// syncCookiesBack 从浏览器 jar 把 google.com 的 cookie 拉回 DB。
// 节流：同一账号两次写回至少隔 cookieSyncThrottle；只在 identity 一致时
// 用 mergeSetCookie 合并（identity 变了说明这个 jar 已经不是原来那个号）。
func (m *browserManager) syncCookiesBack() {
	m.mu.Lock()
	if time.Since(m.lastCookieSync) < cookieSyncThrottle {
		m.mu.Unlock()
		return
	}
	m.lastCookieSync = time.Now()
	seed := m.seed
	m.mu.Unlock()
	if seed.AccountID == 0 {
		return
	}
	a := accountByID(seed.AccountID)
	if a == nil {
		return
	}
	var res struct {
		Cookies []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"cookies"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.client.Call(ctx, m.sessionID, "Network.getCookies",
		map[string]interface{}{"urls": []string{"https://gemini.google.com", "https://google.com"}},
		&res); err != nil {
		return
	}
	var lines []string
	for _, c := range res.Cookies {
		lines = append(lines, c.Name+"="+c.Value)
	}
	if len(lines) == 0 {
		return
	}
	// mergeSetCookie 语义正好是「把新下发的 k=v 合并进既有串」：
	// lines 里每个 name=value 当一条单字段 Set-Cookie 处理。
	merged := mergeSetCookie(a.Cookie, lines)
	if cookieIdentity(merged) != cookieIdentity(a.Cookie) {
		logf("[browser] jar cookie identity changed for #%d, skip write-back", a.ID)
		return
	}
	updateAccountCookie(a.ID, merged)
}

// maybeSwitchAccount 是策略 M1 的切号决策。needsSwitch 为 true 时：
//  1. 把当前驻留号 jar 写回 DB；
//  2. 等 30s 最小冷却（不到就 sleep 到满）；
//  3. clearBrowserCookies + seedAccount(next)。
//
// 返回 nil = 现在浏览器里登的是 next；非 nil = 切不了（冷却没满会被等到满，
// 所以返回错只会是 CDP 或播种本身的失败）。
func (m *browserManager) maybeSwitchAccount(ctx context.Context, next *CookieAccount, reason string) error {
	m.mu.Lock()
	cur := m.seed.AccountID
	lastSwitch := m.lastSwitchAt
	m.mu.Unlock()

	if cur == next.ID {
		return nil // 已经在登这个号
	}
	logf("[browser] 策略 M1 切号 #%d -> #%d (%s)", cur, next.ID, reason)

	// 旧号先回写
	m.syncCookiesBack()

	// 冷却：上次切号距今 < 30s 就等满
	if dt := time.Since(lastSwitch); dt < browserSwitchCooldown {
		wait := browserSwitchCooldown - dt
		logf("[browser] 切号冷却再等 %s", wait.Round(time.Second))
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}

	if err := m.client.Call(ctx, m.sessionID, "Network.clearBrowserCookies", nil, nil); err != nil {
		return browserErr("clearBrowserCookies: %w", err)
	}
	if err := m.seedAccount(ctx, next); err != nil {
		return err
	}
	m.mu.Lock()
	m.lastSwitchAt = time.Now()
	m.mu.Unlock()
	return nil
}

// shouldSwitch 判断当前驻留号是不是必须换：
//   - 驻留号被停用/删除；
//   - next != cur 且 cur 不为 0（有主号时请求点名别的号）；
//   - cur 在 exclude（strict_model 冷却进入排除集合）。
//
// 返回 (needsSwitch, reason)。PinAccountID 的请求按 pinned 语义走，
// 不调这个函数。
func (m *browserManager) shouldSwitch(next *CookieAccount, exclude map[int64]bool) (bool, string) {
	m.mu.Lock()
	cur := m.seed.AccountID
	m.mu.Unlock()
	if cur == 0 {
		return true, "no resident account"
	}
	curAcc := accountByID(cur)
	if curAcc == nil || curAcc.Status != "enabled" {
		return true, "resident account disabled/deleted"
	}
	if exclude[cur] {
		return true, "resident in strict_model cooldown"
	}
	if next != nil && next.ID != cur {
		return true, "request targets another account"
	}
	return false, ""
}

// ─── 流式管道 ────────────────────────────────────────────────────────────────

// browserStream 是单条浏览器 fetch 的接收端：
// CDP 读循环（handleBridgePush）→ chunks 非阻塞队列 → pumpToPipe → io.PipeWriter。
//
// 关键设计（防死锁）：
//   - chunks 是带 sync.Cond 的有界切片队列，单流上限 maxStreamQueueBytes；
//   - CDP 读循环**永不阻塞**：push 时队列满就记 overflow 并把这条流判死
//     （不能让一条慢消费流堵住所有 CDP 事件）；
//   - pumpToPipe 是独立 goroutine，从队列取块往 io.PipeWriter 写；
//     PipeWriter.Write 会自然等 PipeReader 消费，天然背压。
type browserStream struct {
	reqID string

	mu        sync.Mutex
	cond      *sync.Cond
	queue     [][]byte
	queueByte int
	done      bool  // end/error 已收到
	fatalErr  error // overflow / abort / CDP 断
	closed    chan struct{}
	closeOnce sync.Once

	pipeR *io.PipeReader
	pipeW *io.PipeWriter
}

// maxStreamQueueBytes 是单条流在 CDP→pipe 之间允许积压的字节数。
// 超过即判流死亡：意味着消费端（HTTP handler）跟不上，继续堆积等于
// 让常驻浏览器给一条慢请求当缓冲区。
const maxStreamQueueBytes = 16 << 20

var browserStreamSeq atomic.Int64

func newBrowserStream() *browserStream {
	s := &browserStream{
		reqID:  "s" + strconv.FormatInt(browserStreamSeq.Add(1), 10),
		closed: make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	s.pipeR, s.pipeW = io.Pipe()
	go s.pumpToPipe()
	return s
}

// push 由 CDP 读循环调用。**绝不能阻塞**：队列满就把这条流判死。
func (s *browserStream) push(kind string, data []byte, status int, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return
	}
	switch kind {
	case "headers", "end":
		// headers 不算队列字节（元信息），end 关闭队列
		if kind == "end" {
			s.done = true
			s.cond.Broadcast()
			return
		}
		// headers 帧不排队；调用方直接看 status 字段
		s.queue = append(s.queue, encodeStreamHeader(status, msg))
		s.cond.Broadcast()
	case "chunk":
		if s.queueByte+len(data) > maxStreamQueueBytes {
			s.done = true
			s.fatalErr = browserErr("stream queue overflow (%d bytes)", maxStreamQueueBytes)
			s.cond.Broadcast()
			return
		}
		s.queue = append(s.queue, data)
		s.queueByte += len(data)
		s.cond.Broadcast()
	case "error":
		s.done = true
		s.fatalErr = browserErr("page fetch error: %s", msg)
		s.cond.Broadcast()
	}
}

// encodeStreamHeader 把响应头帧编成一个控制记录塞进队列首位。
// 格式：'\x00' + status(3B big-endian 没必要，直接一行文本) + '\n' + headers JSON。
// 消费端（doBrowserStreamRequest）在 pipe 里读到它时按前缀识别。
func encodeStreamHeader(status int, rawJSON string) []byte {
	return append([]byte(fmt.Sprintf("\x00H%d\n", status)), []byte(rawJSON)...)
}

// fail 是外部判死路径：CDP 断、Abort、Tab 重建。
func (s *browserStream) fail(err error) {
	s.mu.Lock()
	if !s.done {
		s.done = true
		if s.fatalErr == nil {
			s.fatalErr = err
		}
		s.cond.Broadcast()
	}
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.closed) })
}

// pumpToPipe 是每条流唯一的消费 goroutine：
//
//	queue → io.PipeWriter → 调用方 PipeReader → HTTP 响应
//
// PipeWriter.Write 在 PipeReader 不读时会阻塞，这没问题——它是这条流自己
// 的 goroutine，堵也只堵这条流；CDP 读循环由 push 的非阻塞保证不受影响。
func (s *browserStream) pumpToPipe() {
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.done {
			s.cond.Wait()
		}
		if len(s.queue) == 0 && s.done {
			err := s.fatalErr
			s.mu.Unlock()
			if err != nil {
				s.pipeW.CloseWithError(err)
			} else {
				s.pipeW.Close()
			}
			s.closeOnce.Do(func() { close(s.closed) })
			return
		}
		chunk := s.queue[0]
		s.queue = s.queue[1:]
		s.queueByte -= len(chunk)
		s.mu.Unlock()

		if _, err := s.pipeW.Write(chunk); err != nil {
			// 下游关了 pipe（HTTP handler 取消），把这条流标死
			s.fail(err)
			return
		}
	}
}

// Abort 异步发 window.__gwBridge.abort(reqID)。不返回错误：
// Abort 的目的是「别再推了」，CDP 失败本身就意味着流已经死。
func (s *browserStream) Abort(m *browserManager) {
	s.closeOnce.Do(func() { close(s.closed) })
	m.mu.Lock()
	client := m.client
	sess := m.sessionID
	m.mu.Unlock()
	if client == nil {
		return
	}
	expr := fmt.Sprintf("window.__gwBridge && window.__gwBridge.abort(%q)", s.reqID)
	_ = client.Call(context.Background(), sess, "Runtime.evaluate",
		map[string]interface{}{"expression": expr, "awaitPromise": false}, nil)
	s.fail(browserErr("aborted by caller"))
}

// handleBridgePush 解析 __gwPush 推上来的 JSON 并分发给对应流。
func (m *browserManager) handleBridgePush(payload string) {
	var msg struct {
		ReqID   string            `json:"reqID"`
		Kind    string            `json:"kind"`
		Status  int               `json:"status"`
		Message string            `json:"message"`
		URL     string            `json:"url"`
		Data    string            `json:"data"`
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal([]byte(payload), &msg); err != nil || msg.ReqID == "" {
		return
	}
	m.streamRegMu.Lock()
	s, ok := m.streams[msg.ReqID]
	m.streamRegMu.Unlock()
	if !ok {
		return
	}
	var raw []byte
	if msg.Kind == "chunk" && msg.Data != "" {
		b, err := base64.StdEncoding.DecodeString(msg.Data)
		if err != nil {
			s.fail(browserErr("chunk base64: %v", err))
			return
		}
		raw = b
	}
	if msg.Kind == "headers" {
		hdrJSON, _ := json.Marshal(map[string]interface{}{
			"status":  msg.Status,
			"url":     msg.URL,
			"headers": msg.Headers,
		})
		s.push("headers", raw, msg.Status, string(hdrJSON))
		return
	}
	s.push(msg.Kind, raw, msg.Status, msg.Message)
}

func (m *browserManager) registerStream(s *browserStream) {
	m.streamRegMu.Lock()
	m.streams[s.reqID] = s
	m.streamRegMu.Unlock()
}

func (m *browserManager) unregisterStream(s *browserStream) {
	m.streamRegMu.Lock()
	delete(m.streams, s.reqID)
	m.streamRegMu.Unlock()
}

// failAllStreams 在浏览器层出大事故（Tab 崩、WS 断）时把所有在飞流判死。
func (m *browserManager) failAllStreams(err error) {
	m.streamRegMu.Lock()
	list := make([]*browserStream, 0, len(m.streams))
	for _, s := range m.streams {
		list = append(list, s)
	}
	m.streamRegMu.Unlock()
	for _, s := range list {
		s.fail(err)
	}
}

// ─── 流式请求主入口 ─────────────────────────────────────────────────────────

// doBrowserStreamRequest 是 Phase 2 业务接线的调用面：
//
//	(ctx, account, url, method, headers, body) → (status, io.ReadCloser, err)
//
// 流程：
//  1. ensureRunning（单飞，并发安全）；
//  2. shouldSwitch + maybeSwitchAccount 处理策略 M1；
//  3. 新建 browserStream 注册，Runtime.evaluate 触发 __gwBridge.fetch；
//  4. 返回 pipeR：消费方按行/字节读，CDP 读循环在后台推；
//  5. ctx 取消 / pipe 关 → stream.Abort（异步 abort，调用方不等）。
func (m *browserManager) doBrowserStreamRequest(ctx context.Context,
	account *CookieAccount, fetchURL, method string, headers map[string]string, body []byte,
	exclude map[int64]bool) (int, io.ReadCloser, error) {

	if err := m.ensureRunning(ctx); err != nil {
		return 0, nil, err
	}

	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		return 0, nil, browserErr("browser is shutting down")
	}
	m.mu.Unlock()

	// 策略 M1：有驻留号时优先用驻留号；要换号走 maybeSwitchAccount。
	if account != nil {
		need, reason := m.shouldSwitch(account, exclude)
		if need {
			if err := m.maybeSwitchAccount(ctx, account, reason); err != nil {
				return 0, nil, err
			}
		}
	}

	// 桥健康检查：addBinding/addScript 都只对「之后的 context」生效，页面导航
	// 会重建 context 丢掉注入——每次 fetch 前确认 __gwPush/__gwBridge 都在，
	// 不在就重装桥（幂等，重复注入无副作用）。这一步扛住所有时序竞争。
	if err := m.ensureBridge(ctx); err != nil {
		return 0, nil, err
	}

	s := newBrowserStream()
	m.registerStream(s)

	// body base64 化再进 JS，避免二进制 / 引号转义问题。
	var bodyJS string
	if len(body) > 0 {
		bodyJS = ", body: Uint8Array.from(atob(\"" +
			base64.StdEncoding.EncodeToString(body) +
			"\"), c => c.charCodeAt(0))"
	}
	hdrJSON, _ := json.Marshal(headers)
	expr := fmt.Sprintf(
		`window.__gwBridge && window.__gwBridge.fetch(%q, %q, {method:%q, headers:%s%s})`,
		s.reqID, fetchURL, method, string(hdrJSON), bodyJS)

	m.mu.Lock()
	client := m.client
	sess := m.sessionID
	m.mu.Unlock()
	if client == nil {
		m.unregisterStream(s)
		s.fail(browserErr("cdp client gone"))
		return 0, nil, s.fatalErr
	}
	if err := client.Call(ctx, sess, "Runtime.evaluate",
		map[string]interface{}{"expression": expr, "awaitPromise": false}, nil); err != nil {
		m.unregisterStream(s)
		s.fail(err)
		return 0, nil, browserErr("evaluate fetch: %w", err)
	}

	// 等待 headers 帧（或 ctx 取消 / 流出错）。
	// pipeR 里第一条记录是 encodeStreamHeader 塞的 \x00H<status>\n<json>，
	// 之后才是正文 chunk。
	type headerResult struct {
		status int
		err    error
	}
	hdrCh := make(chan headerResult, 1)
	go func() {
		buf := make([]byte, 64*1024)
		var prefix []byte
		for len(prefix) < 2 {
			n, err := s.pipeR.Read(buf)
			if n > 0 {
				prefix = append(prefix, buf[:n]...)
			}
			if err != nil {
				hdrCh <- headerResult{err: err}
				return
			}
		}
		if len(prefix) >= 2 && prefix[0] == 0 && prefix[1] == 'H' {
			// \x00H<status>\n<json>
			nl := 2
			for nl < len(prefix) && prefix[nl] != '\n' {
				nl++
			}
			status, _ := strconv.Atoi(string(prefix[2:nl]))
			rest := prefix[nl+1:]
			// 把 header 帧之后的正文塞回一个 pre-loaded reader。
			hdrCh <- headerResult{status: status}
			if len(rest) > 0 {
				s.mu.Lock()
				s.queue = append([][]byte{rest}, s.queue...)
				s.queueByte += len(rest)
				s.cond.Broadcast()
				s.mu.Unlock()
			}
			return
		}
		// 不是 header 帧——协议异常，判死
		s.fail(browserErr("first stream frame is not a headers record"))
		hdrCh <- headerResult{err: s.fatalErr}
	}()

	select {
	case hr := <-hdrCh:
		if hr.err != nil {
			m.unregisterStream(s)
			return 0, nil, hr.err
		}
		return hr.status, &streamReadCloser{s: s, m: m}, nil
	case <-ctx.Done():
		m.unregisterStream(s)
		s.Abort(m)
		return 0, nil, ctx.Err()
	}
}

// streamReadCloser 把 PipeReader + Abort 绑成一个 io.ReadCloser。
// Close 时：PipeReader.Close → pumpToPipe 的 Write 报错 → fail → Abort。
type streamReadCloser struct {
	s *browserStream
	m *browserManager
	r *io.PipeReader
}

func (sc *streamReadCloser) Read(p []byte) (int, error) {
	if sc.r == nil {
		sc.r = sc.s.pipeR
	}
	return sc.r.Read(p)
}

func (sc *streamReadCloser) Close() error {
	sc.m.unregisterStream(sc.s)
	sc.s.Abort(sc.m)
	if sc.r != nil {
		return sc.r.Close()
	}
	return nil
}

// ─── 非流式辅助 ─────────────────────────────────────────────────────────────

// doBrowserFetch 是一次性非流式请求（取 XSRF、/app 页面、上传、删除会话等）。
// 跟 doBrowserStreamRequest 共用管道，但把 body 一次性读完——这些响应
// 都小（几 KB 到几百 KB），全读进内存比开流划算。method 可以是 GET/POST——
// /app 页面必须 GET（POST 实测回 405）。
func (m *browserManager) doBrowserFetch(ctx context.Context,
	account *CookieAccount, fetchURL, method string, headers map[string]string, body []byte,
	exclude map[int64]bool) (int, []byte, error) {
	status, rc, err := m.doBrowserStreamRequest(ctx, account, fetchURL, method, headers, body, exclude)
	if err != nil {
		return 0, nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, 16<<20))
	if err != nil {
		return status, nil, err
	}
	return status, data, nil
}

// doBrowserPost 是 doBrowserFetch 的 POST 薄包，保留原签名。
func (m *browserManager) doBrowserPost(ctx context.Context,
	account *CookieAccount, fetchURL string, headers map[string]string, body []byte,
	exclude map[int64]bool) (int, []byte, error) {
	return m.doBrowserFetch(ctx, account, fetchURL, "POST", headers, body, exclude)
}

// fetchAppTokensViaBrowser 用浏览器 fetch 拿 xsrfEntry：
// 读 window.WIZ_global_data（SNlM0e / qKIAYe / Ylro7b 都在里面），
// authuser>0 时在同 Tab 里轻量 fetch /u/N/app 提取该号的 token。
//
// 这一步替代 xsrf.go 的 fetchAppPage：浏览器路径下不用再走 stdlib/tls-client
// 单独发 HTTP——页面上下文里本来就有这些值。
func (m *browserManager) fetchAppTokensViaBrowser(ctx context.Context,
	account *CookieAccount, exclude map[int64]bool) (xsrfEntry, error) {
	if err := m.ensureRunning(ctx); err != nil {
		return xsrfEntry{}, err
	}
	if account != nil {
		need, reason := m.shouldSwitch(account, exclude)
		if need {
			if err := m.maybeSwitchAccount(ctx, account, reason); err != nil {
				return xsrfEntry{}, err
			}
		}
	}
	// authuser>0：让页面跳到 /u/N/app 再读（同 Tab 内 fetch，不导航）。
	path := "/app"
	if account != nil && account.AuthUser > 0 {
		path = "/u/" + strconv.Itoa(account.AuthUser) + "/app"
	}
	// /app 页面必须 GET（POST 实测回 405），token 就在响应 HTML 的 WIZ_global_data。
	status, body, err := m.doBrowserFetch(ctx, account,
		"https://gemini.google.com"+path, "GET",
		map[string]string{
			"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
			"Accept-Language": "en-US,en;q=0.9",
		}, nil, exclude)
	if err != nil {
		return xsrfEntry{}, err
	}
	if status != 200 {
		return xsrfEntry{}, newAppPageStatusError(status, "")
	}
	e := xsrfEntry{fetched: time.Now()}
	if m := snlm0eRe.FindSubmatch(body); m != nil {
		e.token = string(m[1])
	} else if account != nil {
		return xsrfEntry{}, fmt.Errorf("no SNlM0e in page (cookie expired or not signed in)")
	}
	if p := pushIDRe.FindSubmatch(body); p != nil {
		e.pushID = string(p[1])
	}
	if p := pctxRe.FindSubmatch(body); p != nil {
		e.pctx = string(p[1])
	}
	return e, nil
}

// ─── 崩溃自愈（第二级：进程/WS 断开） ───────────────────────────────────────

// scheduleReconnect 由调用路径在拿到「连接死了」的错时触发：
// 按 1s → 2s → 5s → 15s → 30s 指数退避，稳定 >60s 清零 crashStreak。
// 重连不在这个函数里做——它只负责记退避时长，真正重连发生在下一次
// ensureRunning 发现 started=true 但 proc 已死时（见 ensureAlive）。
func (m *browserManager) scheduleReconnect() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.proc != nil && pidAlive(m.proc.PID) {
		return // 进程还活着，只是 WS 断了——由 readLoop 的 failAll 处理
	}
	if time.Since(m.stableSince) > 60*time.Second {
		m.crashStreak = 0
	}
	m.crashStreak++
	streak := m.crashStreak
	m.proc = nil // 让下一次 ensureRunning 走重启
	m.started = false
	m.readyCh = make(chan struct{})
	m.ready.Store(false)
	// 退避时长只在日志里体现；锁由 defer 统一释放，别再手动 Unlock
	// （手动 + defer 是双重解锁，sync fatal）。
	backoffs := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second}
	idx := streak - 1
	if idx >= len(backoffs) {
		idx = len(backoffs) - 1
	}
	logf("[browser] 进程断开，%s 后由下次请求触发重启（crash streak=%d）",
		backoffs[idx], streak)
}

// ensureAlive 检查进程还活着、WS 还通；不活时按退避重连。
// 由 doBrowserStreamRequest / fetchAppTokensViaBrowser 的入口先调。
func (m *browserManager) ensureAlive(ctx context.Context) error {
	m.mu.Lock()
	alive := m.proc != nil && pidAlive(m.proc.PID) && m.client != nil
	m.mu.Unlock()
	if alive {
		return nil
	}
	m.scheduleReconnect()
	return m.ensureRunning(ctx)
}

// ─── Shutdown ───────────────────────────────────────────────────────────────

// Shutdown 关浏览器：先 CDP Browser.close 给 100ms 让 Chromium 自己清场，
// 然后 killBrowserProcessGroup -pgid 优雅 → 强杀，总计 <= 2.5s。
// 幂等。
func (m *browserManager) Shutdown(ctx context.Context) {
	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		return
	}
	m.shuttingDown = true
	proc := m.proc
	client := m.client
	bridge := m.bridge
	m.mu.Unlock()

	// 1. CDP Browser.close：Chromium 会自己写回 cookie jar 再退。
	if client != nil {
		cctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_ = client.Call(cctx, "", "Browser.close", nil, nil)
		cancel()
	}

	// 2. 进程组优雅 → 强杀
	if proc != nil {
		killBrowserProcessGroup(proc.PID, proc.PGID, 1800*time.Millisecond)
		clearBrowserInstance(m.profileDir)
	}

	// 3. 本地资源
	if client != nil {
		client.Close()
	}
	if bridge != nil {
		bridge.Shutdown(ctx)
	}
	m.failAllStreams(browserErr("browser shut down"))
	m.mu.Lock()
	m.proc = nil
	m.client = nil
	m.sessionID = ""
	m.mu.Unlock()
	logf("[browser] shutdown complete")
}

// 供 net/url 引用（browserProxyBridge 里用到 url.Parse 之外这里也留一个
// 对包级别的确认，避免误删 import）。
var _ = url.URL{}
