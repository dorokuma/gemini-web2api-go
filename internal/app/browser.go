package app

// browser.go —— 常驻真实浏览器传输层（v6.6.0：单 Chromium 多 BrowserContext）。
//
// 顶层结构：
//
//	browserManager（全局单例：最多 1 个 Chromium 进程 + 1 条根 CDP WebSocket）
//	  ├─ proc (*browserProc) + profileDir（仅孤儿回收/展示用）
//	  ├─ rootBridge（SetUpstream("",false) 恒拒 502，封死根 Profile 后台外联）
//	  ├─ client (*cdpClient，flatten 多会话多路复用)
//	  ├─ ctxs: map[browserSlotKey]*accountBrowserCtx（每号/每匿名出口独立
//	  │      BrowserContext + 独立 browserProxyBridge + 独立 page session）
//	  ├─ sessionIndex/targetIndex: sessionID/targetID -> *accountBrowserCtx
//	  ├─ maxSeededEpoch: accountID -> 已播种最大 epoch（防旧槽滞后回写覆盖新令牌）
//	  └─ streams: map[string]*browserStream（每条流绑定 client+sessionID+ac）
//
// 锁纪律（v2.2 §2.9）：m.mu 出度为 0——纯内存短临界区，绝不持锁调
// cdpClient.Call/bridge.*/DB/acquireAcctCookieLock/proxyMu 或阻塞等 channel。

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

// errBrowserSkipAux 是辅助/管理入口（allowSwitch=false）的专用哨兵：
// 「这条请求不该借驻留槽，请回退纯 Go HTTP」。它**刻意不是**
// browserLocalError——浏览器本身没有坏，只是辅助入口点名了未建槽位/
// 槽位不可用，绝不能把它归 browser_unavailable 回 502，也绝不能受
// browser_fallback_http 开关控制（辅助入口无条件回退纯 Go HTTP）。
var errBrowserSkipAux = errors.New("browser: resident jar not applicable for auxiliary entry")

// isBrowserAuxSkip 判断错误是不是辅助入口哨兵。
func isBrowserAuxSkip(err error) bool {
	return errors.Is(err, errBrowserSkipAux)
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
	for _, name := range browserLookPathNames {
		if p, err := exec.LookPath(name); err == nil {
			return p, nil
		}
	}
	return "", browserErr("no Chromium-family browser found (install chromium/google-chrome/brave/edge, or set browser_path)")
}

// browserManager 生命周期状态机（m.mu 保护，generation 单调递增）：
//
//	stopped ──ensureRunning──▶ starting ──doStart ok──▶ running
//	     ▲                       │                        │
//	     │                  doStart err              崩溃/Stop/Shutdown
//	     │                       ▼                        ▼
//	     └────────────  backoff(nextRetryAt)  ◀──  crashed/stopped
type browserManager struct {
	mu sync.Mutex // 唯一协调锁：纯内存短临界区，出度 = 0
	// 状态字段（只在 mu 下读写）：
	started      bool          // 拉起流程已启动（含失败态，配合 readyCh）
	readyCh      chan struct{} // 本次启动完成信号（成功失败都 close）
	startErr     error         // 本次启动的错误（成功后 nil）
	shuttingDown bool          // 终态关闭（Shutdown 之后只读）
	stopping     bool          // Stop/Shutdown 正在执行
	starting     bool          // doStart 正在执行（含 startOnce 协程）
	proc         *browserProc
	client       *cdpClient
	profileDir   string              // 归一化绝对路径；browser-instance 孤儿回收落这里
	rootBridge   *browserProxyBridge // 恒 ("",false)：封死根 Profile 外联
	generation   uint64              // 每次实例重建/清场自增
	ready        atomic.Bool         // running 态的只读快查（写仍在 mu 下）

	// cleaning 是清场单飞标志：handleDeadLocked 认领清场时置位，期间
	// ensureRunning 一律拒绝新代际、第二个并发 handleDeadLocked 直接放弃。
	cleaning    bool
	startCancel context.CancelFunc // 取消本轮 doStart

	// 崩溃自愈：时间戳退避门控（不 sleep 占锁，退避期内直接回错）
	crashStreak int
	stableSince time.Time
	nextRetryAt time.Time

	// stopCh 是 stopImpl 开始时就关闭的「停机打断」信号。
	stopCh chan struct{}

	// 多 BrowserContext 槽位表（全部受 m.mu 保护）。
	nextCtxEpoch   uint64
	ctxs           map[browserSlotKey]*accountBrowserCtx
	sessionIndex   map[string]*accountBrowserCtx
	targetIndex    map[string]*accountBrowserCtx
	maxSeededEpoch map[int64]uint64

	// 流式管道注册表：CDP 读循环收到 Runtime.bindingCalled 后按 reqID
	// 找到对应 browserStream 推数据。
	streamRegMu sync.Mutex
	streams     map[string]*browserStream
}

// browser 全局单例。
var browserMgr = &browserManager{
	readyCh:        make(chan struct{}),
	streams:        map[string]*browserStream{},
	stopCh:         make(chan struct{}),
	ctxs:           map[browserSlotKey]*accountBrowserCtx{},
	sessionIndex:   map[string]*accountBrowserCtx{},
	targetIndex:    map[string]*accountBrowserCtx{},
	maxSeededEpoch: map[int64]uint64{},
}

// browserCrashBackoffs 是崩溃自愈的重启退避序列（crashStreak 递增取）。
var browserCrashBackoffs = []time.Duration{
	time.Second, 2 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second,
}

// doStartFn 是 doStart 的可注入实现：测试需要真实驱动 ensureRunning →
// startOnce 全生命周期（startCtx 创建登记、Stop 中断、代际校验）时桩住它，
// 不真 spawn Chromium。生产恒为 (*browserManager).doStart 本体。
var doStartFn = func(m *browserManager, ctx context.Context) startResult {
	return m.doStart(ctx)
}

// killProcGroupFn 是 killBrowserProcessGroup 的可注入实现：测试要精确控制
// handleDeadLocked 锁外清理的时序时桩住它。生产恒为 killBrowserProcessGroup。
var killProcGroupFn = killBrowserProcessGroup

// ─── 启动与就绪 ─────────────────────────────────────────────────────────────

// ensureRunning 是单飞入口：
//   - running：校验 proc.Alive() && client != nil && !client.Closed()，
//     死了当场清场进崩溃流程；
//   - stopped：退避期内直接 browser_unavailable；退避期到或首次启动则进
//     starting 起 startOnce（generation+1，局部 readyCh 显式传入）；
//   - starting：并发调用方等 readyCh（各自响应 ctx.Done()）。
func (m *browserManager) ensureRunning(ctx context.Context) error {
	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		return browserErr("browser is shut down")
	}
	if m.stopping || m.cleaning {
		m.mu.Unlock()
		return browserErr("browser stopping or cleaning")
	}
	if m.started && !m.starting && m.readyCh != nil {
		if m.startErr == nil {
			alive := m.proc != nil && m.proc.Alive() && m.client != nil && !m.client.Closed()
			if alive {
				m.mu.Unlock()
				return nil
			}
			deadClient := m.client
			deadGen := m.generation
			m.mu.Unlock()
			m.handleDeadLocked(deadClient, deadGen)
			m.mu.Lock()
			if m.shuttingDown || m.stopping || m.cleaning {
				m.mu.Unlock()
				return browserErr("browser is shutting down")
			}
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
		m.startErr = nil
		gen := m.generation + 1
		m.generation = gen
		ready := make(chan struct{})
		m.readyCh = ready
		startCtx, startCancel := context.WithCancel(context.Background())
		m.startCancel = startCancel
		go m.startOnce(gen, ready, startCtx, startCancel)
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

// handleDeadLocked 处理「实例活着的检查没过」。单飞清场：只有
// (m.client == expectedClient && m.generation == expectedGen) 且不在
// stopping/shuttingDown/cleaning 的调用方才认领清场——两条并发崩溃路径
// 同时清场会把对方刚发布的新代际踩坏，第二个调用方直接放弃返回。
//
// 认领后：锁内置 cleaning 单飞标志 + 代际+1 + 复位实例字段 + 全槽位
// detach（一次性取消全部 buildCancel 并关闭 readyCh/drainWait），放锁做
// 真正清理（杀进程组、关 client/rootBridge、判死在飞流、销毁各槽
// BrowserContext），清理完成回锁写 nextRetryAt 并复位 cleaning。
func (m *browserManager) handleDeadLocked(expectedClient *cdpClient, expectedGen uint64) {
	m.mu.Lock()
	if m.client != expectedClient || m.generation != expectedGen ||
		m.cleaning || m.stopping || m.shuttingDown {
		m.mu.Unlock()
		return
	}
	if time.Since(m.stableSince) > 60*time.Second {
		m.crashStreak = 0
	}
	m.crashStreak++
	streak := m.crashStreak
	idx := streak - 1
	if idx >= len(browserCrashBackoffs) {
		idx = len(browserCrashBackoffs) - 1
	}
	if idx < 0 {
		idx = 0
	}
	wait := browserCrashBackoffs[idx]
	proc := m.proc
	client := m.client
	bridge := m.rootBridge
	gen := m.generation
	var deadCtxs []detachedCtxSnap
	for _, ac := range m.ctxs {
		if snap := m.detachSnapLocked(ac, browserErr("browser instance died")); snap.ac != nil {
			deadCtxs = append(deadCtxs, snap)
		}
	}
	m.cleaning = true
	m.proc = nil
	m.client = nil
	m.rootBridge = nil
	m.started = false
	m.starting = false
	m.ready.Store(false)
	m.generation++
	m.mu.Unlock()

	logf("[browser] 实例失活（crash streak=%d），%s 后允许重建", streak, wait)
	if client != nil {
		client.Close()
	}
	if proc != nil {
		killProcGroupFn(proc.PID, proc.PGID, 1500*time.Millisecond)
	}
	for _, snap := range deadCtxs {
		m.failStreamsForCtx(snap.ac, browserErr("browser instance died; streams aborted"))
		if !snap.wasCreating {
			m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, false, client)
		}
	}
	if bridge != nil {
		bridge.Shutdown(context.Background())
	}
	m.failAllStreams(browserErr("browser instance died; streams aborted"))

	m.mu.Lock()
	if !m.stopping && !m.shuttingDown && m.generation == gen+1 {
		m.nextRetryAt = time.Now().Add(wait)
	}
	m.cleaning = false
	m.mu.Unlock()
}

// startOnce 是单轮启动协程：gen 是本轮代际、ready 是只归本轮的局部
// 就绪信号。doStart 全程在**局部变量**里构造 proc/client/rootBridge，
// 结束时回 m.mu 比对「m.generation == gen 且不在 stopping/shuttingDown」：
//   - 不匹配：本轮成果就地回收（杀进程、关 client/rootBridge），只关闭
//     自己的局部 ready——绝不读也绝不关 m.readyCh；
//   - 匹配且失败：复位 started/starting、记 startErr、按退避写 nextRetryAt；
//   - 匹配且成功：局部资源发布进 m.*，置 started/ready，关 ready。
func (m *browserManager) startOnce(gen uint64, ready chan struct{},
	startCtx context.Context, startCancel context.CancelFunc) {
	m.mu.Lock()
	if m.generation != gen || m.stopping || m.shuttingDown || startCtx.Err() != nil {
		if m.generation == gen {
			m.startCancel = nil
		}
		m.mu.Unlock()
		startCancel()
		close(ready)
		return
	}
	m.mu.Unlock()
	res := doStartFn(m, startCtx)
	startCancel()
	m.mu.Lock()
	if m.generation == gen {
		m.startCancel = nil
	}
	if m.generation != gen || m.stopping || m.shuttingDown {
		m.mu.Unlock()
		res.discard()
		close(ready)
		return
	}
	if res.err != nil {
		logf("[browser] 启动失败: %v", res.err)
		m.started = false
		m.starting = false
		m.startErr = res.err
		m.readyCh = nil
		m.proc = nil
		m.client = nil
		m.rootBridge = nil
		m.ready.Store(false)
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
		m.proc = res.proc
		m.client = res.client
		m.rootBridge = res.rootBridge
		m.profileDir = res.profileDir
		m.stableSince = time.Now()
		m.started = true
		m.starting = false
		m.startErr = nil
		m.ready.Store(true)
		logf("[browser] 常驻浏览器已就绪 (pid=%d, rootPort=%d)", res.proc.PID, res.rootBridge.Port())
	}
	m.mu.Unlock()
	close(ready)
}

// startResult 是 doStart 的局部成果（全程不落 m.*，startOnce 按代际决定
// 发布还是就地回收）。
type startResult struct {
	proc       *browserProc
	client     *cdpClient
	rootBridge *browserProxyBridge
	profileDir string
	err        error
}

// discard 就地回收本轮局部成果（本轮已过时，不许把资源发布进 m.*）。
func (r *startResult) discard() {
	if r.client != nil {
		r.client.Close()
	}
	if r.proc != nil {
		killBrowserProcessGroup(r.proc.PID, r.proc.PGID, 1500*time.Millisecond)
	}
	if r.rootBridge != nil {
		r.rootBridge.Shutdown(context.Background())
	}
}

// doStart 拉起根 Chromium：建 rootBridge（SetUpstream("",false) 恒拒 502
// 物理封死根 Profile 后台外联）→ spawn（--proxy-server 只指 rootBridge）
// → 等 DevToolsActivePort → 连 CDP → 注册事件 → 清 v6.5.0 升级遗留盘面
// （Storage.clearCookies + 删 seed-state.json）→ 停在 about:blank。
// 全部成果在 startResult 局部变量里，startOnce 按代际决定发布。
func (m *browserManager) doStart(ctx context.Context) (res startResult) {
	bin, err := discoverBrowserBinary(cfgBrowserPath())
	if err != nil {
		res.err = err
		return res
	}
	profileDir, err := browserProfileDir()
	if err != nil {
		res.err = err
		return res
	}
	if err := os.MkdirAll(profileDir, 0o700); err != nil {
		res.err = browserErr("profile dir: %w", err)
		return res
	}
	absProfileDir, err := filepath.Abs(profileDir)
	if err != nil {
		res.err = browserErr("profile dir abs: %w", err)
		return res
	}
	res.profileDir = absProfileDir
	reclaimOrphanBrowser(absProfileDir)

	rootBridge, err := newBrowserProxyBridge()
	if err != nil {
		res.err = err
		return res
	}
	res.rootBridge = rootBridge
	// 根 Profile 物理断网：任何后台自发外联（update/accounts.google.com 等）
	// 恒回 502，本机真实 IP 绝不从默认 Profile 漏出。
	_ = rootBridge.SetUpstream("", false)

	args := browserLaunchArgs(absProfileDir, rootBridge.Port())
	proc, err := spawnBrowser(bin, args, nil)
	if err != nil {
		rootBridge.Shutdown(context.Background())
		res.rootBridge = nil
		res.err = browserErr("spawn: %w", err)
		return res
	}
	res.proc = proc
	if err := writeBrowserInstance(absProfileDir, proc); err != nil {
		logf("[browser] browser-instance.json 写失败（孤儿回收下次可能失效）: %v", err)
	}

	cleanup := func() {
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
		rootBridge.Shutdown(context.Background())
		res.proc = nil
		res.rootBridge = nil
	}

	_ = os.Remove(filepath.Join(absProfileDir, "DevToolsActivePort"))
	wsURL, err := waitDevToolsPort(ctx, absProfileDir, proc, 15*time.Second)
	if err != nil {
		cleanup()
		res.err = err
		return res
	}
	conn, err := dialCDPWebsocket(ctx, wsURL)
	if err != nil {
		cleanup()
		res.err = browserErr("cdp dial: %w", err)
		return res
	}
	client := newCDPClient(conn)
	res.client = client
	m.registerCDPEvents(client)

	// v6.5.0 升级遗留清理：老 M1 持久 Jar 与 seed-state.json 只对新架构
	// 有害（陌生 Cookie 会从未经确认的出口发出）。5s 子超时 best-effort
	// 清空根 Profile jar，删 seed-state.json；失败仅打日志不阻断启动。
	clrCtx, clrCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := client.Call(clrCtx, "", "Storage.clearCookies", nil, nil); err != nil {
		logf("[browser] 升级遗留 Storage.clearCookies 失败（继续启动）: %v", err)
	}
	clrCancel()
	_ = os.Remove(filepath.Join(absProfileDir, "seed-state.json"))
	return res
}

// waitDevToolsPort 等 Chromium 把 DevToolsActivePort 写进 profileDir。
func waitDevToolsPort(ctx context.Context, profileDir string, proc *browserProc, timeout time.Duration) (string, error) {
	path := filepath.Join(profileDir, "DevToolsActivePort")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", browserErr("startup cancelled: %w", ctx.Err())
		default:
		}
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
		select {
		case <-ctx.Done():
			return "", browserErr("startup cancelled: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	return "", browserErr("DevToolsActivePort not written within %s", timeout)
}

// browserLaunchArgs 拼启动参数。--proxy-server 只指 rootBridge（恒拒 502）。
// --no-sandbox 只在 Linux + euid==0 追加。
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
// config.json browser_profile_dir > BROWSER_PROFILE_DIR > <db 目录>/browser-profile。
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

// cfgBrowserPath 取配置里的显式浏览器路径（运行时不可改）。RuntimeConfig
// 不存它，/admin 默认免鉴权，面板可写等于本地任意可执行文件执行。
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
// 按 cookieIdentity 匹配，跳过 status != "enabled" 的记录；同 cookieIdentity
// 多 authuser 时优先匹配 X-Goog-AuthUser 指定的 authuser。
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

// shutdownBrowser 回收常驻浏览器。saveRuntimeConfig 在 browser_transport 被
// 关掉时异步调用（关掉就该立刻释放内存，不该等下次进程重启）。
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

// ─── CDP 事件 ───────────────────────────────────────────────────────────────

// registerCDPEvents 把事件回调挂到 client 上。所有回调都先取 m.mu 快照
// 校验 generation/session，nil client / 过时实例的事件一律丢弃。
func (m *browserManager) registerCDPEvents(c *cdpClient) {
	c.On("Runtime.bindingCalled", func(evt cdpEvent) {
		m.mu.Lock()
		cur := m.client
		ac := m.sessionIndex[evt.SessionID]
		m.mu.Unlock()
		if cur != c || ac == nil || ac.disposed || ac.sessionID != evt.SessionID {
			return
		}
		var p struct {
			Name    string `json:"name"`
			Payload string `json:"payload"`
		}
		if json.Unmarshal(evt.Params, &p) != nil || p.Name != "__gwPush" {
			return
		}
		m.handleBridgePush(ac, p.Payload)
	})
	c.On("Inspector.targetCrashed", func(evt cdpEvent) {
		m.mu.Lock()
		cur := m.client
		ac := m.sessionIndex[evt.SessionID]
		gen := m.generation
		m.mu.Unlock()
		if cur != c || ac == nil || ac.disposed {
			return
		}
		logf("[browser] target crashed (slot %v), recreating tab", ac.key)
		go m.recreateAccountTab(c, ac, gen)
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
		ac := m.targetIndex[p.TargetID]
		cur := m.client
		gen := m.generation
		shut := m.shuttingDown
		m.mu.Unlock()
		if ac == nil || cur != c || ac.disposed || shut {
			return
		}
		logf("[browser] tab destroyed (slot %v), recreating", ac.key)
		go m.recreateAccountTab(c, ac, gen)
	})
	c.setOnClose(func(error) {
		m.mu.Lock()
		cur := m.client
		m.mu.Unlock()
		if cur != c {
			return
		}
		m.failAllStreams(browserErr("cdp disconnected"))
	})
}

// handleBridgePush 解析 __gwPush 推上来的 JSON 并分发给对应流。
// 仅当流绑定的 ac 与事件来源 ac 一致时才投递（跨槽隔离）。
func (m *browserManager) handleBridgePush(ac *accountBrowserCtx, payload string) {
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
	if !ok || s.ac != ac {
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

// enablePageDomainFor 对指定 session 开 Page 域（不依赖 m.*）。
func (m *browserManager) enablePageDomainFor(ctx context.Context, client *cdpClient, sess string) error {
	if client == nil || sess == "" {
		return browserErr("no page session for Page.enable")
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return client.Call(cctx, sess, "Page.enable", nil, nil)
}

// installBridgeFor 对指定 session 注入 window.__gwBridge 桥（不依赖 m.*）。
// addBinding 让页面 JS 反向调我们；addScriptToEvaluateOnNewDocument 让每次
// 导航后桥还在；当前已加载的 document 就地 evaluate 补注一次。
func (m *browserManager) installBridgeFor(ctx context.Context, client *cdpClient, sess string) error {
	if client == nil || sess == "" {
		return browserErr("no page session for bridge install")
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	m.waitPageLoad(cctx, client, sess)
	if err := client.Call(cctx, sess, "Runtime.addBinding",
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
	if err := client.Call(cctx, sess, "Page.addScriptToEvaluateOnNewDocument",
		map[string]interface{}{"source": src}, nil); err != nil {
		return err
	}
	return client.Call(cctx, sess, "Runtime.evaluate",
		map[string]interface{}{"expression": src, "awaitPromise": false}, nil)
}

// ensureBridgeFor 确认指定 session 的 __gwPush/__gwBridge 都在；缺失就重跑
// installBridgeFor（幂等）。
func (m *browserManager) ensureBridgeFor(ctx context.Context, client *cdpClient, sessID string) error {
	if client == nil {
		return browserErr("cdp client gone")
	}
	if sessID == "" {
		return browserErr("no active page session")
	}
	var out struct {
		Result struct {
			Value string `json:"value"`
		} `json:"result"`
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := client.Call(cctx, sessID, "Runtime.evaluate",
		map[string]interface{}{
			"expression":   `typeof __gwPush + "/" + typeof window.__gwBridge`,
			"awaitPromise": false,
		}, &out)
	cancel()
	if err == nil && out.Result.Value == "function/object" {
		return nil
	}
	logf("[browser] 桥不完整（%q），重新注入", out.Result.Value)
	return m.installBridgeFor(ctx, client, sessID)
}

// waitPageLoad 等当前 Tab 到可注入状态。
func (m *browserManager) waitPageLoad(ctx context.Context, client *cdpClient, sess string) {
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
	cancelOnce := client.OnceSession(sess, "Page.loadEventFired", func(evt cdpEvent) {
		select {
		case done <- struct{}{}:
		default:
		}
	})
	defer cancelOnce()
	lctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	select {
	case <-done:
	case <-lctx.Done():
		logf("[browser] 等页面 load 超时（继续，桥将在下次导航时注入）")
	case <-client.Done():
	}
}

// navigateAndWaitApp 导航到 https://gemini.google.com/app 并等它真正就绪，
// 严格 fail-closed 返回 error。
func (m *browserManager) navigateAndWaitApp(ctx context.Context, client *cdpClient, sess string) error {
	if client == nil {
		return browserErr("cdp client gone")
	}
	navCtx, navCancel := context.WithTimeout(ctx, 25*time.Second)
	defer navCancel()
	loadCh := make(chan struct{}, 1)
	cancelOnce := client.OnceSession(sess, "Page.loadEventFired", func(evt cdpEvent) {
		select {
		case loadCh <- struct{}{}:
		default:
		}
	})
	defer func() { cancelOnce() }()
	var navRes struct {
		ErrorText string `json:"errorText"`
	}
	if err := client.Call(navCtx, sess, "Page.navigate",
		map[string]interface{}{"url": "https://gemini.google.com/app"}, &navRes); err != nil {
		return browserErr("navigate /app: %w", err)
	}
	if navRes.ErrorText != "" {
		return browserErr("navigate /app: %s", navRes.ErrorText)
	}
	for {
		select {
		case <-client.Done():
			return browserErr("navigate /app: cdp client closed")
		case <-loadCh:
			if navCtx.Err() == nil {
				cancelOnce()
				cancelOnce = client.OnceSession(sess, "Page.loadEventFired", func(evt cdpEvent) {
					select {
					case loadCh <- struct{}{}:
					default:
					}
				})
			}
		case <-navCtx.Done():
		}
		var out struct {
			Result struct {
				Value string `json:"value"`
			} `json:"result"`
		}
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		err := client.Call(rctx, sess, "Runtime.evaluate",
			map[string]interface{}{
				"expression":   `location.origin + "/" + document.readyState`,
				"awaitPromise": false,
			}, &out)
		rcancel()
		if err == nil && out.Result.Value == "https://gemini.google.com/complete" {
			return nil
		}
		if navCtx.Err() != nil {
			got := out.Result.Value
			if err != nil {
				got = "origin probe failed"
			}
			return browserErr("navigate /app did not reach gemini.google.com origin (got %q)", got)
		}
	}
}

// ─── 流式管道 ────────────────────────────────────────────────────────────────

// browserStreamHeader 是响应头元数据，经独立 hdrCh 传递。
type browserStreamHeader struct {
	status     int
	statusText string
	url        string
	redirected bool
	headers    map[string]string
	err        error
}

// browserStream 是单条浏览器 fetch 的接收端。
type browserStream struct {
	reqID string

	mu        sync.Mutex
	cond      *sync.Cond
	queue     [][]byte
	queueByte int
	done      bool
	fatalErr  error
	closed    chan struct{}
	closeOnce sync.Once

	hdrCh   chan browserStreamHeader
	hdrOnce sync.Once

	pipeR *io.PipeReader
	pipeW *io.PipeWriter

	// 指针级绑定：创建时的 client/sessionID/ac，Abort 据此精确派发页内
	// abort（不再读全局会话，避免误发到别槽）。
	client    *cdpClient
	sessionID string
	ac        *accountBrowserCtx

	pumpDone chan struct{}
}

// maxStreamQueueBytes 是单条流在 CDP→pipe 之间允许积压的字节数。
const maxStreamQueueBytes = 16 << 20

var browserStreamSeq atomic.Int64

func newBrowserStream() *browserStream {
	s := &browserStream{
		reqID:    "s" + strconv.FormatInt(browserStreamSeq.Add(1), 10),
		closed:   make(chan struct{}),
		hdrCh:    make(chan browserStreamHeader, 1),
		pumpDone: make(chan struct{}),
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
		s.pushHeaderErr(browserErr("stream ended before headers"))
	case "chunk":
		if s.queueByte+len(data) > maxStreamQueueBytes {
			s.done = true
			s.fatalErr = browserErr("stream queue overflow (%d bytes)", maxStreamQueueBytes)
			s.cond.Broadcast()
			s.pushHeaderErr(s.fatalErr)
			return
		}
		s.queue = append(s.queue, data)
		s.queueByte += len(data)
		s.cond.Broadcast()
	case "error":
		s.done = true
		s.fatalErr = pageFetchErr(msg)
		s.cond.Broadcast()
		s.pushHeaderErr(s.fatalErr)
	}
}

// pushHeaderErr 把 headers 阶段的错误也经 hdrCh 送达。
func (s *browserStream) pushHeaderErr(err error) {
	s.hdrOnce.Do(func() {
		s.hdrCh <- browserStreamHeader{err: err}
	})
}

// fail 是外部判死路径。
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
	s.pushHeaderErr(s.fatalErr)
	s.closeOnce.Do(func() { close(s.closed) })
}

// pumpToPipe 是每条流唯一的消费 goroutine。
func (s *browserStream) pumpToPipe() {
	defer close(s.pumpDone)
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
			s.fail(err)
			return
		}
	}
}

// Abort 本地立即判死并注销，再异步给页面发 abort（按绑定的
// client+sessionID 精确派发，不再读全局会话）。
func (s *browserStream) Abort(m *browserManager) {
	s.fail(browserErr("aborted by caller"))
	m.unregisterStream(s)
	if s.client == nil || s.sessionID == "" {
		return
	}
	if s.client.Closed() {
		return
	}
	expr := fmt.Sprintf("window.__gwBridge && window.__gwBridge.abort(%q)", s.reqID)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		_ = s.client.Call(ctx, s.sessionID, "Runtime.evaluate",
			map[string]interface{}{"expression": expr, "awaitPromise": false}, nil)
	}()
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

// failStreamsForCtx 仅终止绑定指定 ac 的流（其余槽位零影响）。
func (m *browserManager) failStreamsForCtx(ac *accountBrowserCtx, err error) {
	m.streamRegMu.Lock()
	var list []*browserStream
	for _, s := range m.streams {
		if s.ac == ac {
			list = append(list, s)
		}
	}
	m.streamRegMu.Unlock()
	for _, s := range list {
		s.pushHeaderErr(err)
		s.fail(err)
		_ = s.pipeR.Close()
	}
}

// failAllStreams 在浏览器层出大事故（WS 断/整机清场）时把所有在飞流判死。
func (m *browserManager) failAllStreams(err error) {
	m.streamRegMu.Lock()
	list := make([]*browserStream, 0, len(m.streams))
	for _, s := range m.streams {
		list = append(list, s)
	}
	m.streamRegMu.Unlock()
	for _, s := range list {
		s.pushHeaderErr(err)
		s.fail(err)
		_ = s.pipeR.Close()
	}
}

// ─── 流式请求主入口 ─────────────────────────────────────────────────────────

// doBrowserStreamRequest 是业务接线的调用面：
//
//	(ctx, account, url, method, headers, body) → (status, io.ReadCloser, error)
func (m *browserManager) doBrowserStreamRequest(ctx context.Context,
	account *CookieAccount, fetchURL, method string, headers map[string]string, body []byte,
	allowSwitch bool, exclude map[int64]bool) (int, io.ReadCloser, error) {
	return m.doBrowserStreamRequestCtx(ctx, "", account, fetchURL, method, headers, body, allowSwitch, exclude)
}

// doBrowserStreamRequestCtx 比 doBrowserStreamRequest 多收一个 proxyURL：
// 它决定匿名槽的 exitKey 与登录槽的绑定出口核验。
func (m *browserManager) doBrowserStreamRequestCtx(ctx context.Context, proxyURL string,
	account *CookieAccount, fetchURL, method string, headers map[string]string, body []byte,
	allowSwitch bool, exclude map[int64]bool) (int, io.ReadCloser, error) {

	// 辅助入口（allowSwitch=false）前置守卫：槽位未就绪一律
	// errBrowserSkipAux 无条件回退纯 Go HTTP——既不冷启动 Chromium/Context，
	// 也不会在缺浏览器时把辅助请求误报成 browserLocalError/502。
	allowDirect := bridgeAllowDirect(account)
	if !allowSwitch {
		if err := m.auxSlotReady(account, proxyURL, allowDirect); err != nil {
			return 0, nil, err
		}
	}

	if err := m.ensureRunning(ctx); err != nil {
		return 0, nil, err
	}

	client, ac, sessID, release, err := m.acquireAccountCtx(ctx, account, proxyURL, allowDirect, allowSwitch)
	if err != nil {
		return 0, nil, err
	}
	// release 在流 Close 时归还（delivered 置 true 后移交 streamReadCloser）。
	releaseHeld := true
	defer func() {
		if releaseHeld {
			release()
		}
	}()

	if err := m.ensureBridgeFor(ctx, client, sessID); err != nil {
		return 0, nil, err
	}

	s := newBrowserStream()
	s.client = client
	s.sessionID = sessID
	s.ac = ac
	m.registerStream(s)

	delivered := false
	defer func() {
		if !delivered {
			m.unregisterStream(s)
			s.fail(browserErr("stream not delivered"))
			_ = s.pipeR.Close()
		}
	}()

	stopWatch := context.AfterFunc(ctx, func() {
		s.fail(ctx.Err())
		_ = s.pipeR.Close()
	})

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

	errEval := client.Call(ctx, sessID, "Runtime.evaluate",
		map[string]interface{}{"expression": expr, "awaitPromise": false}, nil)
	if errEval != nil {
		stopWatch()
		s.fail(errEval)
		return 0, nil, browserErr("evaluate fetch: %w", errEval)
	}

	var hdr browserStreamHeader
	select {
	case hdr = <-s.hdrCh:
	case <-ctx.Done():
		stopWatch()
		s.Abort(m)
		return 0, nil, ctx.Err()
	}
	if hdr.err != nil {
		stopWatch()
		return 0, nil, hdr.err
	}
	// 跨源重定向守卫：redirect:"follow" 后终态不在 gemini.google.com 按
	// appPageStatusError 返回（登录页/sorry 拦截页走完整失效 Cookie 逻辑）。
	if hdr.url != "" {
		u, perr := url.Parse(hdr.url)
		if perr != nil || !strings.EqualFold(u.Scheme, "https") ||
			!strings.EqualFold(u.Hostname(), "gemini.google.com") {
			stopWatch()
			s.Abort(m)
			return 0, nil, newAppPageStatusError(302, hdr.url)
		}
	}
	delivered = true
	releaseHeld = false
	return hdr.status, &streamReadCloser{s: s, m: m, stopWatch: stopWatch, release: release}, nil
}

// streamReadCloser 把 PipeReader + Abort + release 租约绑成一个 io.ReadCloser。
// Close 无条件：关 pipeR、本地 fail、注销、停 ctx 盯梢、异步 Abort、归还
// inFlight 租约。
type streamReadCloser struct {
	s         *browserStream
	m         *browserManager
	stopWatch func() bool
	release   func()
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
		_ = sc.s.pipeR.Close()
		sc.s.Abort(sc.m)
		if sc.release != nil {
			sc.release()
		}
		// SF-2：登录槽常规流结束后按 cookieSyncThrottle + syncingCookie
		// 单飞节流异步写回 Cookie（新刷 SIDCC 不丢 jar）；匿名槽跳过。
		if ac := sc.s.ac; ac != nil && ac.key.accountID > 0 {
			m := sc.m
			m.mu.Lock()
			single := !ac.syncingCookie
			if single {
				ac.syncingCookie = true
			}
			m.mu.Unlock()
			if single {
				go func(ac *accountBrowserCtx) {
					defer func() {
						m.mu.Lock()
						ac.syncingCookie = false
						m.mu.Unlock()
					}()
					m.SyncAccountCookiesBack(ac.key.accountID, false)
				}(ac)
			}
		}
	})
	return nil
}

// ─── 非流式辅助 ─────────────────────────────────────────────────────────────

// doBrowserFetch 是一次性非流式请求（取 XSRF、/app 页面、上传、删除会话等）。
func (m *browserManager) doBrowserFetch(ctx context.Context,
	account *CookieAccount, fetchURL, method string, headers map[string]string, body []byte,
	allowSwitch bool, exclude map[int64]bool) (int, []byte, error) {
	return m.doBrowserFetchCtx(ctx, "", account, fetchURL, method, headers, body, allowSwitch, exclude)
}

// doBrowserFetchCtx 同 doBrowserFetch，额外收 proxyURL 供槽位分键。
func (m *browserManager) doBrowserFetchCtx(ctx context.Context, proxyURL string,
	account *CookieAccount, fetchURL, method string, headers map[string]string, body []byte,
	allowSwitch bool, exclude map[int64]bool) (int, []byte, error) {
	status, rc, err := m.doBrowserStreamRequestCtx(ctx, proxyURL, account, fetchURL, method, headers, body, allowSwitch, exclude)
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
	allowSwitch bool, exclude map[int64]bool) (int, []byte, error) {
	return m.doBrowserFetch(ctx, account, fetchURL, "POST", headers, body, allowSwitch, exclude)
}

// doBrowserPostCtx 同 doBrowserPost，额外收 proxyURL 供槽位分键。
func (m *browserManager) doBrowserPostCtx(ctx context.Context, proxyURL string,
	account *CookieAccount, fetchURL string, headers map[string]string, body []byte,
	allowSwitch bool, exclude map[int64]bool) (int, []byte, error) {
	return m.doBrowserFetchCtx(ctx, proxyURL, account, fetchURL, "POST", headers, body, allowSwitch, exclude)
}

// fetchAppTokensViaBrowser 用浏览器 fetch 拿 xsrfEntry。
func (m *browserManager) fetchAppTokensViaBrowser(ctx context.Context,
	account *CookieAccount, allowSwitch bool, exclude map[int64]bool) (xsrfEntry, error) {
	return m.fetchAppTokensViaBrowserCtx(ctx, "", account, allowSwitch, exclude)
}

// fetchAppTokensViaBrowserCtx 同 fetchAppTokensViaBrowser，额外收 proxyURL
// 供槽位分键。
func (m *browserManager) fetchAppTokensViaBrowserCtx(ctx context.Context, proxyURL string,
	account *CookieAccount, allowSwitch bool, exclude map[int64]bool) (xsrfEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	// 辅助入口（allowSwitch=false）前置守卫：槽位未就绪一律
	// errBrowserSkipAux——辅助入口既无权拉起浏览器，也无权借用别的号的 jar。
	allowDirect := bridgeAllowDirect(account)
	if !allowSwitch {
		if err := m.auxSlotReady(account, proxyURL, allowDirect); err != nil {
			return xsrfEntry{}, err
		}
	}
	if err := m.ensureRunning(ctx); err != nil {
		return xsrfEntry{}, err
	}
	path := "/app"
	if account != nil && account.AuthUser > 0 {
		path = "/u/" + strconv.Itoa(account.AuthUser) + "/app"
	}
	status, body, err := m.doBrowserStreamRequestCtx(ctx, proxyURL, account,
		"https://gemini.google.com"+path, "GET",
		map[string]string{
			"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
			"Accept-Language": "en-US,en;q=0.9",
		}, nil, allowSwitch, exclude)
	if err != nil {
		return xsrfEntry{}, err
	}
	rc := body
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, 16<<20))
	if err != nil {
		return xsrfEntry{}, err
	}
	if status != 200 {
		return xsrfEntry{}, newAppPageStatusError(status, "")
	}
	e := xsrfEntry{fetched: time.Now()}
	if m := snlm0eRe.FindSubmatch(data); m != nil {
		e.token = string(m[1])
	} else if account != nil {
		return xsrfEntry{}, fmt.Errorf("no SNlM0e in page (cookie expired or not signed in)")
	}
	if p := pushIDRe.FindSubmatch(data); p != nil {
		e.pushID = string(p[1])
	}
	if p := pctxRe.FindSubmatch(data); p != nil {
		e.pctx = string(p[1])
	}
	return e, nil
}

// initialBridgeUpstream 为「首个请求到来前」的保守默认出口与直连许可：
//   - 代理池里有 enabled 代理 → 用第一个 enabled 代理、按 fallback_direct
//     决定直连许可；
//   - 池非空但全 disabled/熔断 + fallback_direct=false → upstream="" 且
//     allowDirect=false（配置了代理池的语义就是「绝不直连」）；
//   - 池空 → 直连、允许直连。
//
// 多 Context 架构下仅作测试/参考语义保留：实际业务槽位各自独立桥出口。
func initialBridgeUpstream() (string, bool) {
	fallback := rtCfg().FallbackDirect
	proxies := listProxies()
	if len(proxies) == 0 {
		return "", true
	}
	for _, p := range proxies {
		if p.Enabled {
			return p.URL, fallback
		}
	}
	return "", fallback
}

// bridgeAllowDirect 是请求级出口的 allowDirect 语义：
//   - 账号锁定直连（ProxyID == proxyIDDirect）：显式 true；
//   - 匿名/未点名账号：池空 → true；池非空 → 随 fallback_direct；
//   - 绑定代理的账号：proxyURL 非空走代理时 allowDirect 随池规则兜底。
func bridgeAllowDirect(account *CookieAccount) bool {
	if account != nil && account.ProxyID == proxyIDDirect {
		return true
	}
	if !hasProxyPool() {
		return true
	}
	return rtCfg().FallbackDirect
}

// ─── Stop / Shutdown ────────────────────────────────────────────────────────

// Stop 是**运行时停用**（面板关 browser_transport）：安全停当前浏览器、
// 复位全部状态、允许之后再开。
func (m *browserManager) Stop(ctx context.Context) {
	m.stopImpl(ctx, false)
}

// Shutdown 是**进程终态关闭**（SIGINT/SIGTERM）：同 Stop 的回收动作，
// 另置 shuttingDown 让之后的 ensureRunning 一律拒绝。
func (m *browserManager) Shutdown(ctx context.Context) {
	m.stopImpl(ctx, true)
}

// stopRemaining 返回调用方 ctx 的剩余时间；ctx 无 deadline 时按默认
// 停机总预算 2.5s 算。
func stopRemaining(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		rem := time.Until(deadline)
		if rem < 0 {
			return 0
		}
		return rem
	}
	return 2500 * time.Millisecond
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
	if m.startCancel != nil {
		m.startCancel()
	}
	if m.stopCh == nil {
		m.stopCh = make(chan struct{})
	}
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
	var waitReady chan struct{}
	if m.starting && m.readyCh != nil {
		waitReady = m.readyCh
	}
	m.mu.Unlock()

	if waitReady != nil {
		select {
		case <-waitReady:
		case <-ctx.Done():
		}
	}

	// 停机写回 + 全槽位摘除合并进同一个 m.mu 临界区：先在锁内快照全部
	// 已发布存活登录槽的写回目标（新刷 SIDCC 不能丢在 jar 里），再对
	// m.ctxs 逐槽 detachSnapLocked——创建/重建中槽位立即 buildCancel 0ms
	// 唤醒等待者，已发布槽此时尚未 disposeBrowserContext，锁外先用
	// client 对快照目标执行共享 800ms 预算写回再统一清理（Reviewer #5 & #8）。
	m.mu.Lock()
	client := m.client
	var syncTargets []cookieSyncTarget
	var deadSnaps []detachedCtxSnap
	for _, ac := range m.ctxs {
		if !ac.disposed && !ac.creating && ac.key.accountID > 0 &&
			ac.browserContextID != "" {
			syncTargets = append(syncTargets, cookieSyncTarget{
				bcID:  ac.browserContextID,
				accID: ac.key.accountID,
				epoch: ac.epoch,
				ident: ac.cookieIdentity,
			})
		}
		if snap := m.detachSnapLocked(ac, browserErr("browser shut down")); snap.ac != nil {
			deadSnaps = append(deadSnaps, snap)
		}
	}
	m.mu.Unlock()

	// 锁外共享预算写回（所有快照目标共用同一个绝对截止）。
	m.syncCookieTargetsBack(ctx, client, syncTargets)

	m.mu.Lock()
	proc := m.proc
	client = m.client
	bridge := m.rootBridge
	gen := m.generation
	m.proc = nil
	m.client = nil
	m.rootBridge = nil
	m.started = false
	m.starting = false
	m.startErr = nil
	m.ready.Store(false)
	m.generation++
	m.mu.Unlock()

	// 锁外统一清理全部已摘除槽位（failStreams→断桥→disposeBrowserContext；
	// 停机写回已在上方按快照完成，这里 syncCookie 恒 false）。
	for _, snap := range deadSnaps {
		m.failStreamsForCtx(snap.ac, snap.err)
		if !snap.wasCreating {
			m.disposeDetachedCtx(snap.ac, snap.bcID, snap.bridge, false, client)
		}
	}

	if client != nil {
		grace := stopRemaining(ctx)
		if grace > 100*time.Millisecond {
			grace = 100 * time.Millisecond
		}
		if grace > 0 {
			cctx, cancel := context.WithTimeout(context.Background(), grace)
			_ = client.Call(cctx, "", "Browser.close", nil, nil)
			cancel()
		}
	}
	if proc != nil {
		killBrowserProcessGroup(proc.PID, proc.PGID, stopRemaining(ctx))
	}
	if client != nil {
		client.Close()
	}
	if bridge != nil {
		bridge.Shutdown(ctx)
	}
	m.failAllStreams(browserErr("browser shut down"))

	m.stopReapLate(ctx, gen, proc, client, bridge)
}

// stopReapLate 是 stopImpl 的尾声：回锁清理「停机期间被并发重建」的
// 迟到实例字段（gen 已推进时把它们归零摘出），随后销毁旧/新实例资源
// 与 profile 记录、复位 stopCh/stopping。锁外调用。
func (m *browserManager) stopReapLate(ctx context.Context, gen uint64,
	proc *browserProc, client *cdpClient, bridge *browserProxyBridge) {
	m.mu.Lock()
	dir := m.profileDir
	lateProc, lateClient, lateBridge := m.proc, m.client, m.rootBridge
	if m.generation != gen {
		m.proc = nil
		m.client = nil
		m.rootBridge = nil
		m.generation++
	}
	m.stopCh = make(chan struct{})
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
