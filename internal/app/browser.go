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
//     浏览器 Cookie 再播种新号；切号全程持 switchMu 串行，绝不允许两条请求
//     并发 clearBrowserCookies 把两个号的 Cookie 混在一个 jar 里；
//   - Cookie 反向写回：上游几乎每个响应都在刷 SIDCC/*SIDCC，浏览器收下后要
//     节流回写 DB（30s/次），但切号与停机前必须 force 一次绕过节流，否则
//     新刷的 SIDCC 丢在 jar 里，重启后池里还是旧值；
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
// 没装 Chromium、启动失败、WS 断、CDP 调用错、流被本地 abort。
// isBrowserLocalError 判断它，请求路径据此决定要不要回 502 browser_unavailable
// 而不是降级走 HTTP。
//
// 与之相对的是**页内上游错误**：TypeError: Failed to fetch、代理断连、页内
// 超时——那是出口/上游的问题，跟 Go HTTP 路径的网络错同性质，要计入
// recordProxyResult 代理健康度、能换号重试，绝不能包成 browserLocalError
// 触发 browser_fallback_http 误降级。
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

// pageFetchErr 是页内 fetch 的上游网络错误（TypeError: Failed to fetch 等）。
// 它经 handleBridgePush 的 kind:"error" 到达，不带 browserLocalError 包装：
// 调用方拿到它跟拿到 Go 侧 client.Do 的网络错完全同义——计代理健康度、
// 正常走重试与换号，不回 browser_unavailable。
func pageFetchErr(msg string) error {
	return fmt.Errorf("page fetch error: %s", msg)
}

// isExecutableFile 是跨平台的「这路径能不能当可执行文件用」校验。
// Unix 要可执行位（!IsDir && Mode&0111 != 0）；Windows 的 .exe 没有
// Unix 可执行位概念，Mode()&0111 恒为 0，拿 Unix 判据会把合法 chrome.exe
// 全拒掉——Windows 只查 !IsDir。
func isExecutableFile(info os.FileInfo) bool {
	if info == nil || info.IsDir() {
		return false
	}
	if runtime.GOOS == "windows" {
		return true
	}
	return info.Mode()&0o111 != 0
}

// browserLookPathNames 是自动探测时按 PATH 找的裸命令名全集。
var browserLookPathNames = []string{
	"chromium", "chromium-browser",
	"google-chrome", "google-chrome-stable",
	"brave", "brave-browser",
	"msedge", "microsoft-edge",
}

// discoverBrowserBinary 找 Chromium 系可执行文件。explicitPath 非空时严格
// 使用它（不存在或不可执行直接报错，不回退探测——用户显式指了路径就该
// 立刻看到错误，而不是静默换成系统里另一个浏览器）。
//
// Linux/macOS 的裸命令名（无路径分隔符）**只走 exec.LookPath**：裸名进
// os.Stat 会在当前工作目录里找同名文件，cwd 里恰好有个叫 "chromium" 的
// 文件/目录时会被当成浏览器路径返回——CWD 随启动方式变化，这不是探测是踩雷。
// LookPath 走 PATH 语义且自带可执行校验。
func discoverBrowserBinary(explicitPath string) (string, error) {
	if explicitPath != "" {
		info, err := os.Stat(explicitPath)
		if err != nil {
			return "", browserErr("explicit browser path: %w", err)
		}
		if !isExecutableFile(info) {
			return "", browserErr("explicit browser path %q is not executable", explicitPath)
		}
		return explicitPath, nil
	}
	var pathCandidates []string // 带路径分隔符的绝对/相对候选
	switch runtime.GOOS {
	case "darwin":
		pathCandidates = []string{
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
				pathCandidates = append(pathCandidates, filepath.Join(root, rel))
			}
		}
	}
	for _, c := range pathCandidates {
		if info, err := os.Stat(c); err == nil && isExecutableFile(info) {
			return c, nil
		}
	}
	// 裸命令名一律走 LookPath（PATH 语义 + 可执行校验），绝不在 CWD 里 Stat。
	for _, name := range browserLookPathNames {
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

// 生命周期状态机（m.mu 保护，generation 单调递增）：
//
//	stopped ──ensureRunning──▶ starting ──doStart ok──▶ running
//	     ▲                       │                        │
//	     │                  doStart err              崩溃/Stop/Shutdown
//	     │                       ▼                        ▼
//	     └────────────  backoff(nextRetryAt)  ◀──  crashed/stopped
//
// Stop() 是运行时停用（面板关 browser_transport）：安全停浏览器、复位字段、
// 允许下次打开开关再拉起；Shutdown() 是进程终态关闭（SIGINT/SIGTERM），
// 之后 ensureRunning 一律拒绝。generation 让迟到的 goroutine（旧 readLoop、
// 迟到的 spawn）认出自己已经过时，不会把状态写回新实例。
type browserManager struct {
	mu sync.Mutex // 统一互斥锁：启动、就绪、停用、重启、终态全在这一把锁下
	// 状态字段（只在 mu 下读写）：
	started      bool          // 拉起流程已启动（含失败态，配合 readyCh）
	readyCh      chan struct{} // 本次启动完成信号（成功失败都 close）
	startErr     error         // 本次启动的错误（成功后 nil）
	shuttingDown bool          // 终态关闭（Shutdown 之后只读）
	stopping     bool          // Stop/Shutdown 正在执行
	starting     bool          // doStart 正在执行（含 startOnce 协程）
	proc         *browserProc
	client       *cdpClient
	sessionID    string // 主 Tab 的 CDP sessionId（flatten 模式）
	targetID     string
	profileDir   string // 归一化绝对路径；seed-state/browser-instance 都落这里
	bridge       *browserProxyBridge
	seed         browserSeedState
	generation   uint64      // 每次实例重建/清场自增，迟到回调靠它认出旧实例
	ready        atomic.Bool // running 态的只读快查（写仍在 mu 下）

	// 崩溃自愈：时间戳退避门控（不 sleep 占锁，退避期内直接回错）
	crashStreak int
	stableSince time.Time // 上次崩溃后稳定运行起点，>60s 清零 crashStreak
	nextRetryAt time.Time // 退避期内 ensureRunning 直接 browser_unavailable

	lastSwitchAt time.Time // 策略 M1 的 30s 切号冷却

	// 切号串行锁：maybeSwitchAccount 全段持有，清 Cookie/播种绝不可并发。
	// 独立字段（不并入 mu）：冷却等待要 sleep 几十秒，持 mu 睡等于冻结整个
	// 浏览器层的所有查询。
	switchMu sync.Mutex

	// recreateTab 单飞：targetDestroyed 事件（自己 closeTarget 也会触发它）
	// 与 Inspector.targetCrashed 可能同时到，单飞防连环重建。
	recreating bool

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

// browserCrashBackoffs 是崩溃自愈的重启退避序列（crashStreak 递增取）。
var browserCrashBackoffs = []time.Duration{
	time.Second, 2 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second,
}

// ─── 启动与就绪 ─────────────────────────────────────────────────────────────

// ensureRunning 是单飞入口：
//   - running：校验 proc.Alive() && client != nil && !client.Closed()，
//     死了当场清场进崩溃流程；
//   - stopped：退避期内直接 browser_unavailable（不空等锁）；
//     退避期到或首次启动则进 starting 起 startOnce；
//   - starting：并发调用方等 readyCh（各自响应 ctx.Done()）。
func (m *browserManager) ensureRunning(ctx context.Context) error {
	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		return browserErr("browser is shut down")
	}
	if m.started && !m.starting && m.readyCh != nil {
		select {
		case <-m.readyCh: // 已完成
		default:
		}
		// 启动已完成：校验活着
		if m.startErr == nil {
			alive := m.proc != nil && m.proc.Alive() && m.client != nil && !m.client.Closed()
			if alive {
				m.mu.Unlock()
				return nil
			}
			// 死了：进崩溃自愈（记退避、清场、下次重建）
			m.mu.Unlock()
			m.handleDeadLocked()
			m.mu.Lock()
		} else {
			err := m.startErr
			m.mu.Unlock()
			return err
		}
	}
	if !m.started {
		if !m.nextRetryAt.IsZero() && time.Now().Before(m.nextRetryAt) {
			wait := time.Until(m.nextRetryAt).Round(time.Second)
			m.mu.Unlock()
			return browserErr("browser unavailable (crash backoff, retry in ~%s)", wait)
		}
		m.started = true
		m.starting = true
		m.readyCh = make(chan struct{})
		go m.startOnce()
	}
	ready := m.readyCh
	m.mu.Unlock()

	select {
	case <-ready:
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.startErr != nil {
			return m.startErr
		}
		if m.proc == nil || m.client == nil {
			return browserErr("browser failed to start")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handleDeadLocked 处理「实例活着的检查没过」：记一次 crash、定退避、
// 清场（杀进程组、关 client/bridge、判死在飞流）、复位到可重建态。
// 调用方不持锁。
func (m *browserManager) handleDeadLocked() {
	m.mu.Lock()
	if time.Since(m.stableSince) > 60*time.Second {
		m.crashStreak = 0
	}
	m.crashStreak++
	streak := m.crashStreak
	proc := m.proc
	client := m.client
	bridge := m.bridge
	// 清场复位：下一次 ensureRunning 重新拉起
	m.proc = nil
	m.client = nil
	m.sessionID = ""
	m.targetID = ""
	m.bridge = nil
	m.started = false
	m.starting = false
	m.ready.Store(false)
	m.generation++
	idx := streak - 1
	if idx >= len(browserCrashBackoffs) {
		idx = len(browserCrashBackoffs) - 1
	}
	if idx < 0 {
		idx = 0
	}
	m.nextRetryAt = time.Now().Add(browserCrashBackoffs[idx])
	wait := browserCrashBackoffs[idx]
	m.mu.Unlock()

	logf("[browser] 实例失活（crash streak=%d），%s 后允许重建", streak, wait)
	// 锁外回收：杀进程组有 1.5s 等待，不占 mu。
	if client != nil {
		client.Close()
	}
	if proc != nil {
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
	}
	if bridge != nil {
		bridge.Shutdown(context.Background())
	}
	m.failAllStreams(browserErr("browser instance died; streams aborted"))
}

// startOnce 在协程里跑 doStart，结束时置 startErr 并 close readyCh。
// 启动中途收到 Stop()/Shutdown() 时，Stop 会等 readyCh 关闭后回收迟到的
// 进程/连接（见 Stop 的 generation 比对），不留孤儿。
func (m *browserManager) startOnce() {
	err := m.doStart()
	m.mu.Lock()
	m.startErr = err
	m.starting = false
	if err != nil {
		logf("[browser] 启动失败: %v", err)
		m.proc = nil
		m.client = nil
		m.sessionID = ""
		m.targetID = ""
		m.ready.Store(false)
		// 启动失败也进退避（discoverBinary 找不到这类错重试没意义，
		// 但 spawn 偶发失败值得等 1s 再让下一个请求拉起）。
		m.crashStreak++
		idx := m.crashStreak - 1
		if idx >= len(browserCrashBackoffs) {
			idx = len(browserCrashBackoffs) - 1
		}
		if idx < 0 {
			idx = 0
		}
		m.nextRetryAt = time.Now().Add(browserCrashBackoffs[idx])
	} else {
		m.ready.Store(true)
	}
	ready := m.readyCh
	m.mu.Unlock()
	if ready != nil {
		close(ready)
	}
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
	absProfileDir, err := filepath.Abs(profileDir)
	if err != nil {
		return browserErr("profile dir abs: %w", err)
	}
	// 启动前先回收上一轮的孤儿（进程被 SIGKILL 的话 browser-instance.json
	// 还在，Chromium 进程组可能还活着吃内存）。
	reclaimOrphanBrowser(absProfileDir)

	bridge, err := newBrowserProxyBridge()
	if err != nil {
		return err
	}

	// 首航代理接线：本次启动完成前业务请求还没机会同步出口，
	// 按当前代理池/fallback_direct 推一个保守值——池里任一可用代理 + 禁直连；
	// 之后每个业务请求都会 SetUpstream 校正到本次真正选的出口。
	initUpstream, initAllowDirect := initialBridgeUpstream()
	_ = bridge.SetUpstream(initUpstream, initAllowDirect)

	args := browserLaunchArgs(absProfileDir, bridge.Port())
	proc, err := spawnBrowser(bin, args, nil)
	if err != nil {
		bridge.Shutdown(context.Background())
		return browserErr("spawn: %w", err)
	}
	m.mu.Lock()
	m.proc = proc
	m.profileDir = absProfileDir
	m.bridge = bridge
	m.mu.Unlock()
	if err := writeBrowserInstance(absProfileDir, proc); err != nil {
		logf("[browser] browser-instance.json 写失败（孤儿回收下次可能失效）: %v", err)
	}

	cleanup := func() {
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
		bridge.Shutdown(context.Background())
		m.mu.Lock()
		if m.proc == proc {
			m.proc = nil
		}
		if m.bridge == bridge {
			m.bridge = nil
		}
		m.mu.Unlock()
	}

	// DevToolsActivePort 是 Chromium 写进 profileDir 的「我监听了哪个端口」，
	// --remote-debugging-port=0 让它自己挑空闲端口，我们只认这个文件。
	// 启动前必须删掉上一轮的残留：死端口文件会让这里连上一个没人听的地址。
	_ = os.Remove(filepath.Join(absProfileDir, "DevToolsActivePort"))
	wsURL, err := waitDevToolsPort(absProfileDir, proc, 15*time.Second)
	if err != nil {
		cleanup()
		return err
	}
	conn, err := dialCDPWebsocket(context.Background(), wsURL)
	if err != nil {
		cleanup()
		return browserErr("cdp dial: %w", err)
	}
	client := newCDPClient(conn)
	m.mu.Lock()
	m.client = client
	m.mu.Unlock()
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
		cleanup()
		return browserErr("create target: %w", err)
	}
	m.mu.Lock()
	m.targetID = created.TargetID
	m.mu.Unlock()
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := client.callCDPWithTimeout(context.Background(), 10*time.Second, "",
		"Target.attachToTarget",
		map[string]interface{}{"targetId": created.TargetID, "flatten": true},
		&attached); err != nil {
		client.Close()
		cleanup()
		return browserErr("attach target: %w", err)
	}
	m.mu.Lock()
	m.sessionID = attached.SessionID
	m.mu.Unlock()

	if err := m.enablePageDomain(); err != nil {
		client.Close()
		cleanup()
		return err
	}
	if err := m.installBridge(); err != nil {
		client.Close()
		cleanup()
		return err
	}
	if err := m.restoreSeedState(); err != nil {
		logf("[browser] seed-state 恢复失败（将按未播种处理）: %v", err)
	}
	m.mu.Lock()
	m.stableSince = time.Now()
	m.generation++
	m.mu.Unlock()
	logf("[browser] 常驻浏览器已就绪 (pid=%d, port=%d)", proc.PID, bridge.Port())
	return nil
}

// initialBridgeUpstream 为桥选首航出口与直连许可：
//   - 代理池里有可用代理 → 用第一个可用代理、禁直连（fallback_direct=false
//     时真实 IP 绝不出桥）；
//   - 池空或 fallback_direct=true → 直连、允许直连。
//
// 这只是「首个请求到来前」的保守默认；每个业务请求入口都会用本次真正
// acquireSlot 的结果 SetUpstream 校正。
func initialBridgeUpstream() (string, bool) {
	fallback := rtCfg().FallbackDirect
	for _, p := range listProxies() {
		if p.Enabled {
			return p.URL, fallback
		}
	}
	// 池空：直连是 acquireSlot 唯一会选的路（fallback_direct 语义只在有池时生效）。
	return "", true
}

// syncBridgeUpstream 是业务入口的全链路代理接线：把本次有效 proxyURL 与
// 「当前代理池配置下的允许直连语义」同步给桥。
//
// 允许直连的判据跟 acquireSlot 同构：
//   - 代理池为空：本来就只有直连可走（fallback_direct 不约束无池场景）；
//   - 池非空：仅 fallback_direct=true 才允许直连；fallback_direct=false
//     时即便本次 proxyURL 为空（比如 slot 兜底直连），桥也绝不能直连——
//     那是泄漏真实 IP，宁可让这次请求在桥上报 502。
func syncBridgeUpstream(proxyURL string) {
	bridge := currentBridge()
	if bridge == nil {
		return
	}
	allowDirect := true
	proxyMu.RLock()
	hasPool := len(proxyCache) > 0
	proxyMu.RUnlock()
	if hasPool && !rtCfg().FallbackDirect {
		allowDirect = false
	}
	if err := bridge.SetUpstream(proxyURL, allowDirect); err != nil {
		logf("[browser] bridge SetUpstream(%q) 失败: %v", proxyURL, err)
	}
}

func currentBridge() *browserProxyBridge {
	m := browserMgr
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bridge
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

// browserAccountFromHeaders 从请求头里的 Cookie 反查池子里的账号。
// 按 cookieIdentity（SAPISID + __Secure-1PSID）匹配，跳过 status != "enabled"
// 的记录；同 cookieIdentity 下多条记录（不同 authuser）时优先匹配请求里
// X-Goog-AuthUser 指定的那个 authuser，匹配不到退回第一条 enabled。
// 匿名请求（无 Cookie 头）返回 nil，走浏览器的匿名会话。
func browserAccountFromHeaders(headers map[string]string) *CookieAccount {
	c := headers["Cookie"]
	if c == "" {
		return nil
	}
	id := cookieIdentity(c)
	if id == "|" {
		return nil
	}
	wantAuth := -1
	if au := headers["X-Goog-AuthUser"]; au != "" {
		if n, err := strconv.Atoi(au); err == nil {
			wantAuth = n
		}
	}
	var fallback *CookieAccount
	for _, a := range accountList() {
		if a.Status != "enabled" || cookieIdentity(a.Cookie) != id {
			continue
		}
		cp := a
		if wantAuth >= 0 && a.AuthUser == wantAuth {
			return &cp
		}
		if fallback == nil {
			fallback = &cp
		}
	}
	return fallback
}

// stickyResidentAccount 返回策略 M1 的常驻粘性主号：浏览器已播种且该号仍然
// 启用、不在 exclude 里时，优先让它继续驻留。
//
// exclude 传 nil 时也要先合并**持久化的 strict_model 冷却集合**
// （coolingAccounts 是进程内存态，调用方只在 streamGenerate 路径传）——
// 否则辅助入口（视频额度、后台刷新、对话）会在驻留号正冷却时把它挑中，
// 等于绕过 strict_model 降级守卫。
func stickyResidentAccount(exclude map[int64]bool) *CookieAccount {
	return stickyResidentAccountFor(exclude, "")
}

// stickyResidentAccountFor 同上，可指定模型只查该模型的冷却。
func stickyResidentAccountFor(exclude map[int64]bool, upstream string) *CookieAccount {
	browserMgr.mu.Lock()
	sid := browserMgr.seed.AccountID
	browserMgr.mu.Unlock()
	if sid == 0 {
		return nil
	}
	merged := map[int64]bool{}
	for id := range exclude {
		merged[id] = true
	}
	if rtCfg().StrictModel {
		cooling, _ := coolingAccounts(ModelConfig{Upstream: upstream})
		for id := range cooling {
			merged[id] = true
		}
	}
	if merged[sid] {
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
	m.mu.Lock()
	m.lastCookieSync = time.Time{}
	m.mu.Unlock()
	m.syncCookiesBack(true)
}

// reloadResidentPage 在 XSRF 400 这类「页面 token 失效」时让主 Tab 重新拿一份
// /app 上下文。调它等价于作废本地 XSRF 缓存后重取，调用方负责 invalidateXSRF。
func (m *browserManager) reloadResidentPage(ctx context.Context, account *CookieAccount) error {
	_, err := m.fetchAppTokensViaBrowser(ctx, account, nil)
	return err
}

// shutdownBrowser 回收常驻浏览器。saveRuntimeConfig 在 browser_transport 被
// 关掉时异步调用（关掉就该立刻释放 ~1.45GB 内存，不该等下次进程重启）。
// 面板关开关是**运行时停用**（Stop），之后再次打开要能正常拉起；
// Shutdown 留给 SIGINT/SIGTERM 的进程终态。
func shutdownBrowser() {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	browserMgr.Stop(ctx)
}

// browserRunning 报告常驻浏览器当前是否活着（管理面板只读展示用）。
func browserRunning() bool {
	m := browserMgr
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.proc != nil && m.proc.Alive() && m.client != nil && !m.client.Closed()
}

// browserDetected 探测本机实际可用的浏览器路径（管理面板只读展示）。
//
// 配置了 browser_path 时**也走校验**：原样返回一个不存在/不可执行的路径
// 等于把「启动才会炸」的错误伪装成「已找到」——面板应该如实反映 deploy
// 配置值（admin 返回里有独立的 browser_path 字段）与实际探测结果：
//   - 显式配置了且可执行 → 返回该路径；
//   - 显式配置了但不可执行 → 返回 ""（面板据 browser_path + browser_detected
//     两字段显示「配置无效」）；
//   - 未配置 → 自动探测，找不到返回 ""。
func browserDetected() string {
	if p := cfgBrowserPath(); p != "" {
		if _, err := discoverBrowserBinary(p); err != nil {
			return ""
		}
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
		if !proc.Alive() {
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

// registerCDPEvents 把事件回调挂到 client 上。所有回调都先取 m.mu 快照
// 校验 generation/session，nil client / 过时实例的事件一律丢弃，
// 绝不在回调里裸读 m.client/m.sessionID（DATA RACE 与 nil deref 来源）。
func (m *browserManager) registerCDPEvents(c *cdpClient) {
	c.On("Runtime.bindingCalled", func(evt cdpEvent) {
		m.mu.Lock()
		cur := m.client
		sess := m.sessionID
		m.mu.Unlock()
		if cur != c { // 旧实例的事件：丢
			return
		}
		if evt.SessionID != "" && evt.SessionID != sess {
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
		m.mu.Lock()
		cur := m.client
		sess := m.sessionID
		m.mu.Unlock()
		if cur != c {
			return
		}
		if evt.SessionID != "" && evt.SessionID != sess {
			return
		}
		logf("[browser] target crashed, recreating tab")
		go m.recreateTab()
	})
	c.OnAny(func(evt cdpEvent) {
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
		isMain := p.TargetID == m.targetID && m.client == c && !m.shuttingDown
		m.mu.Unlock()
		if isMain {
			logf("[browser] main tab destroyed, recreating")
			go m.recreateTab()
		}
	})
}

// recreateTab 是第一级自愈：渲染进程崩了但浏览器还活着时，只重建单 Tab
// 并重新 attach + 重新注入桥。
//
// 两个防自激设计（targetDestroyed 风暴的根源）：
//  1. 单飞：recreating 标志下同一时刻只有一个重建在跑，后续事件直接丢；
//  2. 先在锁内清空 m.targetID 并递增 generation 再 closeTarget：
//     closeTarget 必然回推一个 targetDestroyed 事件，清掉 targetID 后
//     事件回调认不出它是「主 Tab」，不会再连环触发重建。
func (m *browserManager) recreateTab() {
	m.mu.Lock()
	if m.recreating || m.shuttingDown || m.stopping || m.client == nil {
		m.mu.Unlock()
		return
	}
	m.recreating = true
	client := m.client
	oldTarget := m.targetID
	m.targetID = "" // 先清：closeTarget 回推的事件据此被忽略
	m.generation++  // 旧会话期注册的回调全部失效
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.recreating = false
		m.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if oldTarget != "" {
		_ = client.Call(ctx, "", "Target.closeTarget",
			map[string]interface{}{"targetId": oldTarget}, nil)
	}
	var created struct {
		TargetID string `json:"targetId"`
	}
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
	// 重建期间实例可能已被换/停用：client 换了或 targetID 被别人填了都要放弃。
	if m.client != client || m.shuttingDown {
		m.mu.Unlock()
		return
	}
	m.targetID = created.TargetID
	m.sessionID = attached.SessionID
	m.mu.Unlock()
	if err := m.enablePageDomain(); err != nil {
		logf("[browser] re-enable page: %v", err)
	}
	if err := m.installBridge(); err != nil {
		logf("[browser] re-install bridge: %v", err)
	}
	m.failAllStreams(browserErr("tab recreated; previous streams aborted"))
}

// ─── Page 域与桥注入 ────────────────────────────────────────────────────────

func (m *browserManager) enablePageDomain() error {
	m.mu.Lock()
	client, sess := m.client, m.sessionID
	m.mu.Unlock()
	if client == nil || sess == "" {
		return browserErr("no page session for Page.enable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return client.Call(ctx, sess, "Page.enable", nil, nil)
}

// installBridge 注入 window.__gwBridge：Runtime.addBinding("__gwPush") 让页面
// JS 能反向调我们；Page.addScriptToEvaluateOnNewDocument 让每次导航后桥还在。
//
// 桥做的事：
//   - __gwBridge.fetch(reqID, url, opts)：同源 fetch（redirect: "follow"——
//     manual 时 resp.type=="opaqueredirect"、status=0、body=null，调
//     getReader() 直接抛 TypeError 让 302 永远走不到状态分支；follow 后
//     用 resp.redirected + resp.url 判终态）；
//   - 响应头元数据透传 {status, redirected, url, headers}；
//   - resp.body == null（204/HEAD/空体）直接推 end，绝不对 null getReader；
//   - __gwBridge.abort(reqID)：AbortController.abort() 打断在飞 fetch；
//   - opts.omitCredentials → credentials:"omit"（匿名请求不带 jar 里的登录
//     Cookie——浏览器驻留了主号时匿名请求必须真匿名，否则串号且可能 400）。
func (m *browserManager) installBridge() error {
	m.mu.Lock()
	client, sess := m.client, m.sessionID
	m.mu.Unlock()
	if client == nil || sess == "" {
		return browserErr("no page session for bridge install")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// addBinding 只对未来/现存 context 生效；页面加载中的旧 context 销毁后
	// binding 随之消失，所以必须等页面就位（waitPageLoad）再注册——否则
	// __gwPush 在加载完的 document 里是 undefined，push 全部被 JS 里的
	// try/catch 静默吞掉，流永远等不到 headers（实测就是这么挂死的）。
	m.waitPageLoad(ctx)
	if err := client.Call(ctx, sess, "Runtime.addBinding",
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
        redirect: "follow",
        credentials: (opts && opts.omitCredentials) ? "omit" : "include",
        signal: ac.signal
      }).then(function(resp) {
        var hdrs = {};
        resp.headers.forEach(function(v, k) { hdrs[k] = v; });
        push({reqID: reqID, kind: "headers", status: resp.status,
              statusText: resp.statusText, url: resp.url || "",
              redirected: !!resp.redirected, headers: hdrs});
        if (resp.body == null) {
          push({reqID: reqID, kind: "end", status: resp.status});
          delete controllers[reqID];
          return;
        }
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
	if err := client.Call(ctx, sess, "Page.addScriptToEvaluateOnNewDocument",
		map[string]interface{}{"source": src}, nil); err != nil {
		return err
	}
	return client.Call(ctx, sess, "Runtime.evaluate",
		map[string]interface{}{"expression": src, "awaitPromise": false}, nil)
}

// ensureBridge 确认当前页上下文里 __gwPush（binding）与 __gwBridge（fetch 桥）
// 都在；任一缺失就重跑 installBridge（幂等）。
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
	m.mu.Lock()
	dir := m.profileDir
	m.mu.Unlock()
	if dir == "" {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, "seed-state.json"))
	if err != nil {
		return nil // 没播种过，正常
	}
	var s browserSeedState
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	m.mu.Lock()
	m.seed = s
	m.mu.Unlock()
	return nil
}

func (m *browserManager) persistSeedState() {
	m.mu.Lock()
	dir := m.profileDir
	b, err := json.Marshal(m.seed)
	m.mu.Unlock()
	if err != nil || dir == "" {
		return
	}
	_ = os.WriteFile(filepath.Join(dir, "seed-state.json"), b, 0o600)
}

// seedAccount 把账号 a 的 Cookie 播进浏览器 jar。
//
// 严格顺序（策略 M1）：调用方先 clearBrowserCookies 再调本函数。
// 锁内取 client/session 快照，实例已清场直接报错。
func (m *browserManager) seedAccount(ctx context.Context, a *CookieAccount) error {
	if a == nil || a.Cookie == "" {
		return nil
	}
	cookies := cookieToCDP(a.Cookie)
	if len(cookies) == 0 {
		return browserErr("account #%d cookie is empty after parse", a.ID)
	}
	m.mu.Lock()
	client, sess := m.client, m.sessionID
	m.mu.Unlock()
	if client == nil {
		return browserErr("cdp client gone")
	}
	if err := client.Call(ctx, sess, "Network.setCookies",
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
// force=false 时受 30s 节流（常规请求路径）；force=true 绕过节流——
// 切号前与停机前必须 force：上游刷的 SIDCC 没写回就清 jar，
// 等于把刚续上的会话凭证丢在内存里，重启后池里那份是旧值。
func (m *browserManager) syncCookiesBack(force bool) {
	m.mu.Lock()
	if !force && time.Since(m.lastCookieSync) < cookieSyncThrottle {
		m.mu.Unlock()
		return
	}
	m.lastCookieSync = time.Now()
	seed := m.seed
	client, sess := m.client, m.sessionID
	m.mu.Unlock()
	if seed.AccountID == 0 || client == nil {
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
	if err := client.Call(ctx, sess, "Network.getCookies",
		map[string]interface{}{"urls": []string{"https://gemini.google.com", "https://google.com"}},
		&res); err != nil {
		if force {
			logf("[browser] 强制写回 Cookie 时 getCookies 失败（可能丢新刷 SIDCC）: %v", err)
		}
		return
	}
	var lines []string
	for _, c := range res.Cookies {
		lines = append(lines, c.Name+"="+c.Value)
	}
	if len(lines) == 0 {
		return
	}
	merged := mergeSetCookie(a.Cookie, lines)
	if cookieIdentity(merged) != cookieIdentity(a.Cookie) {
		logf("[browser] jar cookie identity changed for #%d, skip write-back", a.ID)
		return
	}
	updateAccountCookie(a.ID, merged)
}

// maybeSwitchAccount 是策略 M1 的切号决策执行。**全程持 switchMu 串行**：
// 两条请求并发切号会把 A 的 Cookie 清掉后种上 B 的，A 的请求就带着 B 的
// 会话发出——串号。冷却等待也在这个锁里（最长 30s），并发切号排队
// 本来就是正确行为（Cookie 清/种不能交错）。
//
// 冷却的时长预算：调用方 ctx（xsrf.go 的 30s）跟冷却同长，死睡到冷却满
// 必然触发 DeadlineExceeded；剩余冷却超过 ctx 剩余预算的 2/3 时直接快速
// 返回错，让调用方换号/重试，比死等超时强。
func (m *browserManager) maybeSwitchAccount(ctx context.Context, next *CookieAccount, reason string) error {
	m.switchMu.Lock()
	defer m.switchMu.Unlock()

	m.mu.Lock()
	cur := m.seed.AccountID
	curIdentity := m.seed.CookieIdentity
	lastSwitch := m.lastSwitchAt
	m.mu.Unlock()

	if cur == next.ID {
		return nil // 已经在登这个号
	}
	// 同 cookieIdentity 的不同 authuser（0..9）共用同一 Tab 的 jar：
	// Cookie 是同一串，切 authuser 只是改请求路径，**不需要** clearBrowserCookies
	// 也不需要 30s 冷却——清了反而把共用的 jar 打翻。
	if cur != 0 && curIdentity != "" && curIdentity == cookieIdentity(next.Cookie) {
		m.mu.Lock()
		m.seed.AccountID = next.ID
		m.seed.SeededAt = time.Now().Unix()
		m.mu.Unlock()
		m.persistSeedState()
		return nil
	}
	logf("[browser] 策略 M1 切号 #%d -> #%d (%s)", cur, next.ID, reason)

	// 旧号先回写（强制无节流，拿到最新 SIDCC 再清 jar）
	m.syncCookiesBack(true)

	// 冷却：上次切号距今 < 30s 就等满；但剩余等待超过 ctx 预算的大头时
	// 快速返回，别死睡到 DeadlineExceeded——这个错没法归「cookie 失效」
	// 也没法换号，只会整轮烧掉。
	if dt := time.Since(lastSwitch); dt < browserSwitchCooldown {
		wait := browserSwitchCooldown - dt
		if deadline, ok := ctx.Deadline(); ok && wait > time.Until(deadline)*2/3 {
			return fmt.Errorf("browser account switch cooldown needs %s beyond request budget", wait.Round(time.Second))
		}
		logf("[browser] 切号冷却再等 %s", wait.Round(time.Second))
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}

	m.mu.Lock()
	client, sess := m.client, m.sessionID
	m.mu.Unlock()
	if client == nil {
		return browserErr("cdp client gone")
	}
	if err := client.Call(ctx, sess, "Network.clearBrowserCookies", nil, nil); err != nil {
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

// shouldSwitch 判断当前驻留号是不是必须换。**只认驻留号自身坏掉**：
//   - 驻留号被停用/删除（accountByID 查不到或 status != enabled）；
//   - 驻留号在 exclude（strict_model 冷却进入排除集合）。
//
// 「请求点名别的号」不再触发切号：辅助/后台入口（Cookie 检测、视频额度、
// 后台 XSRF 刷新、异步删会话）会用各自的账号调进来，让它们换号等于把
// 正常驻留的主号偷换掉——那些入口拿到「驻留号还在」的结论后应自行决定
// 跳过浏览器路径或复用驻留号，见调用方的 shouldSwitch 结果使用。
//
// 同 cookieIdentity 的不同 authuser 不算切号（共用 jar，见 maybeSwitchAccount）。
func (m *browserManager) shouldSwitch(next *CookieAccount, exclude map[int64]bool) (bool, string) {
	m.mu.Lock()
	cur := m.seed.AccountID
	curIdentity := m.seed.CookieIdentity
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
	if next != nil && next.ID != cur && curIdentity != "" &&
		curIdentity != cookieIdentity(next.Cookie) {
		// 请求点名另一个身份的号时才换；同身份异 authuser 由
		// maybeSwitchAccount 的同身份快路径处理。
		return true, "request targets another account identity"
	}
	return false, ""
}

// ─── 流式管道 ────────────────────────────────────────────────────────────────

// browserStreamHeader 是响应头元数据，经独立 hdrCh 传递——
// 绝不塞进正文队列/pipe（旧实现把 \x00H<status>\n<json> 前缀编进正文，
// 正文首字节被 hdrJSON 污染、分片无 \n 时切片越界 panic）。
type browserStreamHeader struct {
	status     int
	statusText string
	url        string
	redirected bool
	headers    map[string]string
	err        error
}

// browserStream 是单条浏览器 fetch 的接收端：
// CDP 读循环（handleBridgePush）→ hdrCh（元数据，一次性）+ queue（纯正文）
// → pumpToPipe → io.PipeWriter。
//
// 关键设计（防死锁）：
//   - queue 只装正文 chunk，queueByte 只对它核算；
//   - CDP 读循环**永不阻塞**：push 时队列满就记 overflow 并把这条流判死；
//   - hdrCh 缓冲 1 + sync.Once，headers 帧最多发一次，后到的事件被丢；
//   - pumpToPipe 是独立 goroutine，PipeWriter.Write 自然等 PipeReader 消费。
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

	hdrCh   chan browserStreamHeader
	hdrOnce sync.Once

	pipeR *io.PipeReader
	pipeW *io.PipeWriter
}

// maxStreamQueueBytes 是单条流在 CDP→pipe 之间允许积压的字节数。
const maxStreamQueueBytes = 16 << 20

var browserStreamSeq atomic.Int64

func newBrowserStream() *browserStream {
	s := &browserStream{
		reqID:  "s" + strconv.FormatInt(browserStreamSeq.Add(1), 10),
		closed: make(chan struct{}),
		hdrCh:  make(chan browserStreamHeader, 1),
	}
	s.cond = sync.NewCond(&s.mu)
	s.pipeR, s.pipeW = io.Pipe()
	go s.pumpToPipe()
	return s
}

// push 由 CDP 读循环调用。**绝不能阻塞**：headers 走缓冲 1 的 hdrCh，
// chunk 满就把这条流判死。
func (s *browserStream) push(kind string, data []byte, hdr browserStreamHeader, msg string) {
	switch kind {
	case "headers":
		s.hdrOnce.Do(func() {
			s.hdrCh <- hdr
		})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return
	}
	switch kind {
	case "end":
		s.done = true
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
		// 页内 fetch 的上游错（TypeError: Failed to fetch 等）是普通上游错，
		// 不是浏览器本地故障——调用方拿它计代理健康度，不触发
		// browser_unavailable 误降级。
		s.fatalErr = pageFetchErr(msg)
		s.cond.Broadcast()
	}
}

// pushHeaderErr 把 headers 阶段的错误也经 hdrCh 送达（fetch 在 headers 前
// 就 error/done 时，调用方要等 hdrCh 拿到终态）。
func (s *browserStream) pushHeaderErr(err error) {
	s.hdrOnce.Do(func() {
		s.hdrCh <- browserStreamHeader{err: err}
	})
}

// fail 是外部判死路径：CDP 断、Abort、Tab 重建、ctx 取消、Close。
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
	// headers 还没来就死了：等待 hdrCh 的调用方也要被唤醒。
	s.pushHeaderErr(s.fatalErr)
	s.closeOnce.Do(func() { close(s.closed) })
}

// pumpToPipe 是每条流唯一的消费 goroutine：
//
//	queue → io.PipeWriter → 调用方 PipeReader → HTTP 响应
//
// 只搬运正文 chunk；headers 元数据不经这里。
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

// Abort 本地立即判死并注销，再**真正异步**给页面发 abort：
// window.__gwBridge.abort 走 Runtime.evaluate，渲染器卡死时这个 CDP 调用
// 可能永远不返回——同步调用等于让关闭路径被卡死的渲染器拖住，所以单独
// goroutine + 独立 1.5s 预算发它，调用方绝不阻塞等待。
func (s *browserStream) Abort(m *browserManager) {
	s.fail(browserErr("aborted by caller"))
	m.unregisterStream(s)
	m.mu.Lock()
	client := m.client
	sess := m.sessionID
	m.mu.Unlock()
	if client == nil {
		return
	}
	expr := fmt.Sprintf("window.__gwBridge && window.__gwBridge.abort(%q)", s.reqID)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		_ = client.Call(ctx, sess, "Runtime.evaluate",
			map[string]interface{}{"expression": expr, "awaitPromise": false}, nil)
	}()
}

// handleBridgePush 解析 __gwPush 推上来的 JSON 并分发给对应流。
func (m *browserManager) handleBridgePush(payload string) {
	var msg struct {
		ReqID      string            `json:"reqID"`
		Kind       string            `json:"kind"`
		Status     int               `json:"status"`
		StatusText string            `json:"statusText"`
		Redirected bool              `json:"redirected"`
		Message    string            `json:"message"`
		URL        string            `json:"url"`
		Data       string            `json:"data"`
		Headers    map[string]string `json:"headers"`
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
		s.push("headers", nil, browserStreamHeader{
			status:     msg.Status,
			statusText: msg.StatusText,
			url:        msg.URL,
			redirected: msg.Redirected,
			headers:    msg.Headers,
		}, "")
		return
	}
	s.push(msg.Kind, raw, browserStreamHeader{}, msg.Message)
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
//	(ctx, account, url, method, headers, body) → (status, io.ReadCloser, error)
//
// 流程：
//  1. ensureRunning（单飞 + 崩溃自愈校验）；
//  2. shouldSwitch + maybeSwitchAccount 处理策略 M1（串行）；
//  3. 新建 browserStream 注册，Runtime.evaluate 触发 __gwBridge.fetch；
//  4. hdrCh 拿响应头元数据；正文只经 io.Pipe；
//  5. ctx 取消全生命周期盯梢：取消即本地 fail + pipeR.Close，
//     瞬间唤醒阻塞在 Read/pumpToPipe 的协程（含 headers 已到、上游沉默的
//     死等场景——stallWatch 跟客户端断开都不能被页内沉默拖死）。
func (m *browserManager) doBrowserStreamRequest(ctx context.Context,
	account *CookieAccount, fetchURL, method string, headers map[string]string, body []byte,
	exclude map[int64]bool) (int, io.ReadCloser, error) {

	if err := m.ensureRunning(ctx); err != nil {
		return 0, nil, err
	}
	syncBridgeUpstream(pickCurrentProxyURL())

	m.mu.Lock()
	if m.shuttingDown || m.stopping {
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

	if err := m.ensureBridge(ctx); err != nil {
		return 0, nil, err
	}

	s := newBrowserStream()
	m.registerStream(s)

	// ctx 取消盯梢：从注册到 body 关闭全程有效，Close() 时停掉。
	// AfterFunc 在 ctx 取消时另起 goroutine 执行，不会阻塞取消方；
	// 本地 fail + pipeR.Close 是瞬间动作，唤醒所有读侧协程。
	stopWatch := context.AfterFunc(ctx, func() {
		s.fail(ctx.Err())
		_ = s.pipeR.Close()
	})

	// body base64 化再进 JS；匿名（account==nil）传 omitCredentials。
	var bodyJS string
	if len(body) > 0 {
		bodyJS = ", body: Uint8Array.from(atob(\"" +
			base64.StdEncoding.EncodeToString(body) +
			"\"), c => c.charCodeAt(0))"
	}
	omitJS := ""
	if account == nil {
		omitJS = ", omitCredentials: true"
	}
	hdrJSON, _ := json.Marshal(headers)
	expr := fmt.Sprintf(
		`window.__gwBridge && window.__gwBridge.fetch(%q, %q, {method:%q, headers:%s%s%s})`,
		s.reqID, fetchURL, method, string(hdrJSON), bodyJS, omitJS)

	m.mu.Lock()
	client := m.client
	sess := m.sessionID
	m.mu.Unlock()
	if client == nil {
		stopWatch()
		m.unregisterStream(s)
		s.fail(browserErr("cdp client gone"))
		return 0, nil, s.fatalErr
	}
	if err := client.Call(ctx, sess, "Runtime.evaluate",
		map[string]interface{}{"expression": expr, "awaitPromise": false}, nil); err != nil {
		stopWatch()
		m.unregisterStream(s)
		s.fail(err)
		return 0, nil, browserErr("evaluate fetch: %w", err)
	}

	// 响应头经独立 hdrCh：等它或 ctx 取消/流出错。
	var hdr browserStreamHeader
	select {
	case hdr = <-s.hdrCh:
		// got it
	case <-ctx.Done():
		stopWatch()
		m.unregisterStream(s)
		s.Abort(m)
		return 0, nil, ctx.Err()
	}
	if hdr.err != nil {
		stopWatch()
		m.unregisterStream(s)
		return 0, nil, hdr.err
	}
	// redirect:"follow" 后看终态：跳离 gemini.google.com（accounts.google.com
	// 登录页、/sorry 拦截页）按 appPageStatusError 返回，让既有
	// xsrfErrBlamesCookie 与失效 Cookie 换号/停用逻辑完整生效。
	if hdr.redirected || (hdr.url != "" && !strings.HasPrefix(hdr.url, "https://gemini.google.com")) {
		stopWatch()
		m.unregisterStream(s)
		s.Abort(m)
		loc := hdr.url
		if loc == "" {
			loc = fetchURL
		}
		return 0, nil, newAppPageStatusError(302, loc)
	}
	return hdr.status, &streamReadCloser{s: s, m: m, stopWatch: stopWatch}, nil
}

// pickCurrentProxyURL 取「当前代理池会选的出口」做桥同步的默认值。
// 调用方在 acquireSlot 之后已有确定 proxyURL 的用 syncBridgeUpstream(proxyURL)
// 直接传；这里给没拿到 slot 结果的入口（XSRF、/app、上传等）一个按池
// 现状推的保守值：池里第一个 enabled 代理优先，没有就空串（桥再按
// allowDirect 判能不能直连）。
func pickCurrentProxyURL() string {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.Enabled {
			return p.URL
		}
	}
	return ""
}

// streamReadCloser 把 PipeReader + Abort 绑成一个 io.ReadCloser。
// Close 无条件：关 pipeR（唤醒 pumpToPipe）、本地 fail、注销、停 ctx 盯梢、
// 异步 Abort——即使调用方一次 Read 都没调过也全部做到（旧实现在 r==nil
// 时漏掉 pipeR.Close，pumpToPipe goroutine 与 16MiB 缓冲会泄漏）。
type streamReadCloser struct {
	s         *browserStream
	m         *browserManager
	stopWatch func() bool // context.AfterFunc 的停止函数
	once      sync.Once
}

func (sc *streamReadCloser) Read(p []byte) (int, error) {
	return sc.s.pipeR.Read(p)
}

func (sc *streamReadCloser) Close() error {
	sc.once.Do(func() {
		if sc.stopWatch != nil {
			sc.stopWatch()
		}
		_ = sc.s.pipeR.Close() // 无条件：没 Read 过也要断 pumpToPipe
		sc.s.Abort(sc.m)       // 本地 fail + 注销 + 异步页内 abort
	})
	return nil
}

// ─── 非流式辅助 ─────────────────────────────────────────────────────────────

// doBrowserFetch 是一次性非流式请求（取 XSRF、/app 页面、上传、删除会话等）。
// 跟 doBrowserStreamRequest 共用管道，但把 body 一次性读完。
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

// fetchAppTokensViaBrowser 用浏览器 fetch 拿 xsrfEntry。
// 读 /app 的 WIZ_global_data；account.AuthUser>0 时在同 Tab 里 fetch /u/N/app。
//
// 独立预算：调用方 xsrf.go 给的是 30s，策略 M1 切号冷却也最长 30s——
// 让切号冷却吃光整个预算必然 DeadlineExceeded，所以本函数自己兜一个
// 20s 上限（页面 fetch 实测 1-3s），把冷却+抓页都控在预算内。
func (m *browserManager) fetchAppTokensViaBrowser(ctx context.Context,
	account *CookieAccount, exclude map[int64]bool) (xsrfEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := m.ensureRunning(ctx); err != nil {
		return xsrfEntry{}, err
	}
	syncBridgeUpstream(pickCurrentProxyURL())
	if account != nil {
		need, reason := m.shouldSwitch(account, exclude)
		if need {
			if err := m.maybeSwitchAccount(ctx, account, reason); err != nil {
				return xsrfEntry{}, err
			}
		}
	}
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

// ─── Stop / Shutdown ────────────────────────────────────────────────────────

// Stop 是**运行时停用**（面板关 browser_transport）：安全停当前浏览器、
// 复位全部状态、允许之后再开。与 Shutdown 的区别是它不进 shuttingDown
// 终态——下一次 ensureRunning 正常重新拉起。
//
// 启动中途收到 Stop：等 readyCh 关闭（doStart 自然收尾或失败），再按
// generation 回收迟到的进程/连接，不留孤儿。
func (m *browserManager) Stop(ctx context.Context) {
	m.stopImpl(ctx, false)
}

// Shutdown 是**进程终态关闭**（SIGINT/SIGTERM）：同 Stop 的回收动作，
// 另置 shuttingDown 让之后的 ensureRunning 一律拒绝。
func (m *browserManager) Shutdown(ctx context.Context) {
	m.stopImpl(ctx, true)
}

func (m *browserManager) stopImpl(ctx context.Context, final bool) {
	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		return
	}
	m.stopping = true
	if final {
		m.shuttingDown = true
	}
	// 若启动协程正在跑，等它 readyCh 关闭（启动有自己 15s 内超时，
	// doStart 各步也都有界；Stop 的 ctx 2.5s 超时就先回收快照）。
	var waitReady chan struct{}
	if m.starting && m.readyCh != nil {
		waitReady = m.readyCh
	}
	m.mu.Unlock()

	if waitReady != nil {
		select {
		case <-waitReady:
		case <-ctx.Done():
			// 启动协程还没完：不等了，先按快照回收；迟到的实例由
			// generation 比对在 stopImpl 末尾清掉。
		}
	}

	// 切号进行中等它落地（switchMu 拿到即说明没有切号在飞或已结束）。
	m.switchMu.Lock()
	m.switchMu.Unlock()

	// 停机前强制无节流写回 Cookie（新刷 SIDCC 不能丢在 jar 里）。
	m.syncCookiesBack(true)

	m.mu.Lock()
	proc := m.proc
	client := m.client
	bridge := m.bridge
	gen := m.generation
	m.proc = nil
	m.client = nil
	m.sessionID = ""
	m.targetID = ""
	m.bridge = nil
	m.started = false
	m.starting = false
	m.startErr = nil
	m.ready.Store(false)
	m.generation++
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
	dir := m.profileDir
	// 迟到的启动完成：doStart 在 Stop 清场后才把 proc/client 填上，
	// 它的 generation 比我们快照的旧——按「字段非空且 generation 变化」
	// 再回收一次，不留孤儿。
	lateProc, lateClient, lateBridge := m.proc, m.client, m.bridge
	if m.generation != gen {
		// 启动协程在我们清场后又写回了字段：再抓一次回收
		m.proc = nil
		m.client = nil
		m.bridge = nil
		m.generation++
	}
	m.stopping = false
	m.mu.Unlock()
	if lateProc != nil && lateProc != proc {
		killBrowserProcessGroup(lateProc.PID, lateProc.PGID, 1500*time.Millisecond)
	}
	if lateClient != nil && lateClient != client {
		lateClient.Close()
	}
	if lateBridge != nil && lateBridge != bridge {
		lateBridge.Shutdown(ctx)
	}
	if dir != "" {
		clearBrowserInstance(dir)
	}
	logf("[browser] stop complete")
}

// 供 net/url 引用（避免误删 import）。
var _ = url.URL{}
