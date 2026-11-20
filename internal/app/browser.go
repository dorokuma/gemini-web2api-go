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

// errBrowserSkipAux 是辅助/管理入口（allowSwitch=false）的专用哨兵：
// 「这条请求不该借驻留 jar，请回退纯 Go HTTP」。它**刻意不是**
// browserLocalError——浏览器本身没有坏，只是辅助入口点名了非驻留号/
// 驻留号不可用，绝不能把它归 browser_unavailable 回 502，也绝不能受
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

	// cleaning 是清场单飞标志：handleDeadLocked 认领清场时置位，期间
	// ensureRunning 一律拒绝新代际、第二个并发 handleDeadLocked 直接放弃——
	// 两条并发崩溃路径同时清场会把刚发布的新代际踩坏。
	cleaning bool
	// startCancel 取消本轮 doStart（stopImpl 开头立即调用）：冷启动中途的
	// Stop 不再等 doStart 自然走完 15s+ 的 waitDevToolsPort/dial 超时，
	// 也不会在 Stop 超时后留下没人回收的启动中 Chromium。
	startCancel context.CancelFunc

	// 崩溃自愈：时间戳退避门控（不 sleep 占锁，退避期内直接回错）
	crashStreak int
	stableSince time.Time // 上次崩溃后稳定运行起点，>60s 清零 crashStreak
	nextRetryAt time.Time // 退避期内 ensureRunning 直接 browser_unavailable

	lastSwitchAt time.Time // 策略 M1 的 30s 切号冷却

	// 切号串行信号量（容量 1）：maybeSwitchAccount 全段持有，清 Cookie/播种
	// 绝不可并发。独立字段（不并入 mu）：冷却等待要 sleep 几十秒，持 mu 睡
	// 等于冻结整个浏览器层的所有查询。用 chan 不用 Mutex：stopImpl 必须
	// 能用剩余 ctx 预算抢占/打断持锁方——30s 冷却中途的 Stop 不该等到
	// 冷却自然睡完。
	switchSem chan struct{}

	// stopCh 是 stopImpl 开始时就关闭的「停机打断」信号：冷却等待里的
	// maybeSwitchAccount 监听它毫秒级醒来并释放 switchSem。
	// Stop() 后重建（stopCh == nil 表示不在停机中）。
	stopCh chan struct{}

	// jarRW 是主请求的 fetch 发出窗口与切号清种/播种的读写协调：
	// doBrowserStreamRequest 校验驻留身份并派发 Runtime.evaluate(fetch)
	// 期间持 RLock，maybeSwitchAccount 的 clearBrowserCookies+seedAccount
	// 期间持 Lock——彻底消除「请求 A 刚核完驻留身份、切号立刻清种换号、
	// 请求 A 才 evaluate 把 fetch 发进新 jar 导致 Cookie 与 Token 混配」
	// 的竞态。RLock 是短临界区（只盖校验+evaluate 派发），写锁方是低频
	// 切号，读侧并发互不影响。
	jarRW sync.RWMutex

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
	readyCh:   make(chan struct{}),
	streams:   map[string]*browserStream{},
	switchSem: make(chan struct{}, 1),
	stopCh:    make(chan struct{}), // 永不 nil：stopImpl 只 close 不重建，等冷却的切号拿到永远是有效 channel
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
//     退避期到或首次启动则进 starting 起 startOnce（generation+1，
//     局部 readyCh 显式传入）；
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
		// 启动已完成：校验活着
		if m.startErr == nil {
			alive := m.proc != nil && m.proc.Alive() && m.client != nil && !m.client.Closed()
			if alive {
				m.mu.Unlock()
				return nil
			}
			// 死了：进崩溃自愈（记退避、清场、下次重建）。handleDeadLocked
			// 只在当前 client/proc 仍是这条死实例时才认领清场。
			deadClient := m.client
			deadGen := m.generation
			m.mu.Unlock()
			m.handleDeadLocked(deadClient, deadGen)
			m.mu.Lock()
			// handleDeadLocked 清理期间可能进来过 Stop/Shutdown：重新判终态，
			// 不能拿着清场前的快照继续往启动分支走。
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
		go m.startOnce(gen, ready)
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

// handleDeadLocked 处理「实例活着的检查没过」。**单飞清场**：只有
// (m.client == expectedClient && m.generation == expectedGen) 且不在
// stopping/shuttingDown/cleaning 的调用方才认领清场——两条并发崩溃路径
// 同时清场会把对方刚发布的新代际踩坏，第二个调用方直接放弃返回。
//
// 认领后：锁内置 cleaning 单飞标志 + 代际+1 + 复位实例字段（proc/client/
// session/target/started/bridge），放锁做真正清理（杀进程组、关 client/
// bridge、判死在飞流），清理完成回锁写 nextRetryAt（退避从「清理完」起算）
// 并复位 cleaning。期间 ensureRunning 拒绝新启动，杜绝清理半途又并行拉起
// 一个 doStart。
// 调用方不持锁。
func (m *browserManager) handleDeadLocked(expectedClient *cdpClient, expectedGen uint64) {
	m.mu.Lock()
	if m.client != expectedClient || m.generation != expectedGen ||
		m.cleaning || m.stopping || m.shuttingDown {
		m.mu.Unlock()
		return // 已被别的路径认领/换过代际：不是我要清的场
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
	bridge := m.bridge
	gen := m.generation
	// 清场复位：cleaning 单飞标志挡新启动，下一次 ensureRunning 等 cleaning
	// 复位后重新拉起。
	m.cleaning = true
	m.proc = nil
	m.client = nil
	m.sessionID = ""
	m.targetID = ""
	m.bridge = nil
	m.started = false
	m.starting = false
	m.ready.Store(false)
	m.generation++
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

	// 清理完回锁写退避：期间若已被 Stop/Shutdown 或换了新代际，
	// 退避时间表不归这次清理管。cleaning 无条件复位放行下一轮启动。
	m.mu.Lock()
	if !m.stopping && !m.shuttingDown && m.generation == gen+1 {
		m.nextRetryAt = time.Now().Add(wait)
	}
	m.cleaning = false
	m.mu.Unlock()
}

// startOnce 是单轮启动协程：gen 是本轮代际、ready 是只归本轮的局部
// 就绪信号。doStart 全程在**局部变量**里构造 proc/client/bridge/session，
// 结束时回 m.mu 比对「m.generation == gen 且不在 stopping/shuttingDown」：
//   - 不匹配（期间发生了 Stop/Shutdown/新一轮启动）：本轮成果就地回收
//     （杀进程、关 client/bridge），只关闭自己的局部 ready——绝不读也
//     绝不关 m.readyCh（那是别轮的通道，关了就是 close of closed channel）；
//   - 匹配且 doStart 失败：复位 started/starting、记 startErr、清 m.readyCh
//     按指数退避写 nextRetryAt、关 ready——退避期到后 ensureRunning
//     重新拉起（彻底修掉失败永久锁死）；
//   - 匹配且成功：把局部资源发布进 m.*，置 started/ready，关 ready。
func (m *browserManager) startOnce(gen uint64, ready chan struct{}) {
	startCtx, startCancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.startCancel = startCancel
	m.mu.Unlock()
	res := m.doStart(startCtx)
	startCancel()
	m.mu.Lock()
	m.startCancel = nil
	if m.generation != gen || m.stopping || m.shuttingDown {
		// 本轮已过时：就地回收局部成果，绝不写 m.*、绝不碰 m.readyCh。
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
		m.sessionID = ""
		m.targetID = ""
		m.bridge = nil
		m.ready.Store(false)
		// 启动失败也进退避（discoverBinary 找不到这类错重试没意义，
		// 但 spawn 偶发失败值得等退避到了再让下一个请求拉起）。
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
		// 本轮成功：局部资源发布进 m.*（含 doStart 局部恢复的 seed-state——
		// doStart 全程不碰 m.seed，只有代际校验通过才统一发布）。
		m.proc = res.proc
		m.client = res.client
		m.bridge = res.bridge
		m.sessionID = res.sessionID
		m.targetID = res.targetID
		m.profileDir = res.profileDir
		m.seed = res.seed
		m.stableSince = time.Now()
		m.started = true
		m.starting = false
		m.startErr = nil
		m.ready.Store(true)
		logf("[browser] 常驻浏览器已就绪 (pid=%d, port=%d)", res.proc.PID, res.bridge.Port())
	}
	m.mu.Unlock()
	close(ready)
}

// startResult 是 doStart 的局部成果（全程不落 m.*，startOnce 按代际决定
// 发布还是就地回收）。
type startResult struct {
	proc       *browserProc
	client     *cdpClient
	bridge     *browserProxyBridge
	sessionID  string
	targetID   string
	profileDir string
	seed       browserSeedState
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
	if r.bridge != nil {
		r.bridge.Shutdown(context.Background())
	}
}

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
	// 启动前先回收上一轮的孤儿（进程被 SIGKILL 的话 browser-instance.json
	// 还在，Chromium 进程组可能还活着吃内存）。
	reclaimOrphanBrowser(absProfileDir)

	bridge, err := newBrowserProxyBridge()
	if err != nil {
		res.err = err
		return res
	}
	res.bridge = bridge

	// 首航代理接线：本次启动完成前业务请求还没机会同步出口，
	// 按当前代理池/fallback_direct 推一个保守值——池非空 + fallback_direct=false
	// 时禁直连（就算池内代理全熔断也绝不把真实 IP 漏出去）。
	initUpstream, initAllowDirect := initialBridgeUpstream()
	_ = bridge.SetUpstream(initUpstream, initAllowDirect)

	args := browserLaunchArgs(absProfileDir, bridge.Port())
	proc, err := spawnBrowser(bin, args, nil)
	if err != nil {
		bridge.Shutdown(context.Background())
		res.bridge = nil
		res.err = browserErr("spawn: %w", err)
		return res
	}
	res.proc = proc
	if err := writeBrowserInstance(absProfileDir, proc); err != nil {
		logf("[browser] browser-instance.json 写失败（孤儿回收下次可能失效）: %v", err)
	}

	cleanup := func() {
		killBrowserProcessGroup(proc.PID, proc.PGID, 1500*time.Millisecond)
		bridge.Shutdown(context.Background())
		res.proc = nil
		res.bridge = nil
	}

	// DevToolsActivePort 是 Chromium 写进 profileDir 的「我监听了哪个端口」，
	// --remote-debugging-port=0 让它自己挑空闲端口，我们只认这个文件。
	// 启动前必须删掉上一轮的残留：死端口文件会让这里连上一个没人听的地址。
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

	// attach 到 browser target 下创建的 page。主 Tab 必须停在
	// https://gemini.google.com/app：页内 fetch 是同源请求，没有 CORS 拦截
	// （about:blank origin 发 fetch 会被 Chromium 按跨域拒掉——Failed to fetch）。
	// flatten 模式让所有会话消息都走同一条 WS、用 sessionId 路由——
	// 不开 flatten 每条 session 要单独连接，不符合我们单 WS 的假设。
	var created struct {
		TargetID string `json:"targetId"`
	}
	if err := client.callCDPWithTimeout(ctx, 15*time.Second, "",
		"Target.createTarget",
		map[string]interface{}{"url": "https://gemini.google.com/app"}, &created); err != nil {
		client.Close()
		res.client = nil
		cleanup()
		res.err = browserErr("create target: %w", err)
		return res
	}
	res.targetID = created.TargetID
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := client.callCDPWithTimeout(ctx, 10*time.Second, "",
		"Target.attachToTarget",
		map[string]interface{}{"targetId": created.TargetID, "flatten": true},
		&attached); err != nil {
		client.Close()
		res.client = nil
		cleanup()
		res.err = browserErr("attach target: %w", err)
		return res
	}
	res.sessionID = attached.SessionID

	if err := m.enablePageDomainFor(ctx, client, res.sessionID); err != nil {
		client.Close()
		res.client = nil
		cleanup()
		res.err = err
		return res
	}
	if err := m.installBridgeFor(ctx, client, res.sessionID); err != nil {
		client.Close()
		res.client = nil
		cleanup()
		res.err = err
		return res
	}
	// seed-state 恢复到本轮局部成果：不直接写 m.seed——代际校验没过
	// （Stop/清场发生在启动中途）时绝不污染新代际的驻留状态。
	if seed, err := readSeedState(absProfileDir); err != nil {
		logf("[browser] seed-state 恢复失败（将按未播种处理）: %v", err)
	} else {
		res.seed = seed
	}
	return res
}

// initialBridgeUpstream 为桥选首航出口与直连许可：
//   - 代理池里有 enabled 代理 → 用第一个 enabled 代理、按 fallback_direct
//     决定直连许可；
//   - 池非空但全 disabled/熔断 + fallback_direct=false → upstream="" 且
//     **allowDirect=false**：配置了代理池的语义就是「绝不直连」，池内
//     代理全部暂时不可用也不能让首航把真实 IP 漏出去；
//   - 池空 → 直连、允许直连（fallback_direct 语义只在有池时生效）。
//
// allowDirect 的判据是「池里是否存在任何已配置代理」（len(proxyCache)>0），
// 不是「能否找到一个 enabled」——全部熔断只是出口暂时拿不到，不是允许直连。
// 这只是「首个请求到来前」的保守默认；每个业务请求入口都会用本次真正
// acquireSlot 的结果 SetUpstream 校正。
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
	// 池非空但无一 enabled：upstream 空、allowDirect 由 fallback_direct 定。
	return "", fallback
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
	_, err := m.fetchAppTokensViaBrowser(ctx, account, true, nil)
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
// ctx 取消（stopImpl 打断冷启动）立即返回，不再睡满 timeout。
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
	// CDP 断连终态传播：failAll 首次触发时异步到这条回调。若该 client
	// 仍是当前实例（m.client == c），把所有在途流判死——拿到 headers 后
	// 上游沉默的流不该再干等 stallLimit，已经死在 CDP 层的流更不该被
	// 当成「上游沉默」继续挂在重试上。failAllStreams 内部对每条流同时
	// 调 pushHeaderErr + fail + pipeR.Close，毫秒级全量唤醒。
	c.setOnClose(func(error) {
		m.mu.Lock()
		cur := m.client
		m.mu.Unlock()
		if cur != c {
			return // 旧实例的断连：不归本轮管
		}
		m.failAllStreams(browserErr("cdp disconnected"))
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
	return m.enablePageDomainFor(context.Background(), client, sess)
}

// enablePageDomainFor 是 enablePageDomain 的显式参数版：doStart 用它对
// 本轮局部 client/session 开 Page 域，不依赖 m.*。
func (m *browserManager) enablePageDomainFor(ctx context.Context, client *cdpClient, sess string) error {
	if client == nil || sess == "" {
		return browserErr("no page session for Page.enable")
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return client.Call(cctx, sess, "Page.enable", nil, nil)
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
	return m.installBridgeFor(context.Background(), client, sess)
}

// installBridgeFor 是 installBridge 的显式参数版：doStart / recreateTab
// 用它对局部 client/session 注入桥，不依赖 m.*。
func (m *browserManager) installBridgeFor(ctx context.Context, client *cdpClient, sess string) error {
	if client == nil || sess == "" {
		return browserErr("no page session for bridge install")
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	// addBinding 只对未来/现存 context 生效；页面加载中的旧 context 销毁后
	// binding 随之消失，所以必须等页面就位（waitPageLoad）再注册——否则
	// __gwPush 在加载完的 document 里是 undefined，push 全部被 JS 里的
	// try/catch 静默吞掉，流永远等不到 headers（实测就是这么挂死的）。
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
	// 两条腿：addScriptToEvaluateOnNewDocument 保证**将来的导航**仍有桥；
	// 但当前页面的 document 可能已经在加载中（createTarget 带 url 就开了导航），
	// 钩子的时序不保证赶得上这第一页——所以等 load 后再就地 evaluate 注入一次，
	// 覆盖两种时序（重复注入幂等，window.__gwBridge 覆盖无副作用）。
	if err := client.Call(cctx, sess, "Page.addScriptToEvaluateOnNewDocument",
		map[string]interface{}{"source": src}, nil); err != nil {
		return err
	}
	return client.Call(cctx, sess, "Runtime.evaluate",
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

// readSeedState 从指定 profile 目录读 seed-state 为**局部值**（doStart 用它，
// 全程不碰 m.seed——只在 startOnce 代际校验通过后才统一发布，启动中途的
// Stop/清场不会把旧驻留态写进新代际）。
func readSeedState(dir string) (browserSeedState, error) {
	var s browserSeedState
	if dir == "" {
		return s, nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, "seed-state.json"))
	if err != nil {
		return s, nil // 没播种过，正常
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return s, err
	}
	return s, nil
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
	// 常规路径（切号前/节流后）：用独立 5s 背景预算，跟停机路径分开。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.syncCookiesBackCtx(ctx, force)
}

// syncCookiesBackForce 是 stopImpl 的停机写回：透传剩余 ctx（封顶 800ms），
// 不再用独立 Background+5s 硬撑——停机预算本来就被编排卡死，多花的每一
// 毫秒都在吃后面的进程组回收窗口。
func (m *browserManager) syncCookiesBackForce(ctx context.Context) {
	budget := 800 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		if rem := time.Until(deadline); rem < budget {
			budget = rem
		}
	}
	if budget <= 0 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	m.syncCookiesBackCtx(cctx, true)
}

func (m *browserManager) syncCookiesBackCtx(ctx context.Context, force bool) {
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
	// switchSem 是容量 1 的可取消信号量：stopImpl 开始时会先关 stopCh
	// 打断冷却等待，再用剩余 ctx 抢占本信号量——30s 冷却中途的停机
	// 不会卡到冷却自然睡完。ctx 取消也能让排队者及时退出。
	select {
	case m.switchSem <- struct{}{}:
		defer func() { <-m.switchSem }()
	case <-ctx.Done():
		return ctx.Err()
	}

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
		m.mu.Lock()
		stopC := m.stopCh // 恒非 nil（初始化即有，stopImpl 只 close/重建）
		m.mu.Unlock()
		select {
		case <-timer.C:
		case <-stopC:
			// 停机打断：毫秒级醒来释放 switchSem，不等 30s 冷却睡完。
			timer.Stop()
			return browserErr("browser is stopping")
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}

	// jarRW 写锁段：清种+播种全程是 jar 的「写」临界区，与主请求
	// doBrowserStreamRequest 的 RLock（驻留身份校验 + evaluate 派发）互斥，
	// 消除「A 核完驻留身份、切号清种、A 才 evaluate 发进新 jar」的混配竞态。
	m.jarRW.Lock()
	defer m.jarRW.Unlock()
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

// shouldSwitch 忠实回答一个问题：**当前 jar 是否需要切号才能服务 next**。
// 它只看 jar 的客观状态，不看调用方有没有切号权限：
//   - 未播种（seed.AccountID == 0）；
//   - 驻留号被停用/删除（accountByID 查不到或 status != enabled）；
//   - 驻留号在 exclude（strict_model 冷却进入排除集合）；
//   - next 点名的 Cookie 身份与驻留 jar 不同（同 cookieIdentity 的不同
//     authuser 不算切号——共用同一 jar，/u/N 由请求路径区分）。
//
// allowSwitch 在这里**绝不参与**：true/false 拿到的是同一份事实。把
// 「不许切」折成「不需切」是 R3 查出的串号根源——辅助入口点名非驻留号
// 时会被假装的 need=false 放行，fetch 直接发进驻留 jar。正确的分工是：
// shouldSwitch 报事实，调用方按 need && !allowSwitch 返回 errBrowserSkipAux
// 回退纯 Go HTTP。
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
	if next != nil && next.ID != 0 && curIdentity != "" &&
		cookieIdentity(next.Cookie) != "" && curIdentity != cookieIdentity(next.Cookie) {
		// 请求点名另一个 Cookie 身份的号：jar 必须换（同身份异 authuser
		// 由 maybeSwitchAccount 的同身份快路径处理，不算切号）。
		return true, "request targets another account identity"
	}
	return false, ""
}

// auxJarBlocked 判断辅助入口（allowSwitch=false）能否安全使用当前驻留 jar：
// 未播种、驻留号已不可用、或点名账号的 Cookie 身份与驻留不一致，一律
// 返回 errBrowserSkipAux——辅助请求绝不借驻留 jar 发，也绝不包装成
// browserLocalError 触发 502，由调用方无条件回退纯 Go HTTP。
// 返回 nil 表示「驻留 jar 与请求身份一致 / 匿名请求」，可以安全使用。
func (m *browserManager) auxJarBlocked(account *CookieAccount) error {
	if account == nil {
		return nil // 匿名请求 omitCredentials，不依赖 jar 身份
	}
	m.mu.Lock()
	seedID := m.seed.AccountID
	seedIdentity := m.seed.CookieIdentity
	m.mu.Unlock()
	if seedID == 0 {
		return errBrowserSkipAux // 未播种：辅助入口不做首次播种（那是主请求的职责）
	}
	if acc := accountByID(seedID); acc == nil || acc.Status != "enabled" {
		return errBrowserSkipAux // 驻留号已不可用：辅助入口不负责切号
	}
	if seedIdentity == "" || cookieIdentity(account.Cookie) != seedIdentity {
		return errBrowserSkipAux // 非驻留身份：发进去就串号
	}
	return nil
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

	// pumpDone 精确反映 pumpToPipe 协程真实退出（close 发生在 pumpToPipe 的
	// defer 里）：断言「Close 后泵协程确实退了」就看它，不靠 stream.closed
	// 的近似信号。
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
		// headers 还没来就 end（上游提前收尾）：等待 hdrCh 的调用方
		// 也要被唤醒，不能干等超时。
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
		// 页内 fetch 的上游错（TypeError: Failed to fetch 等）是普通上游错，
		// 不是浏览器本地故障——调用方拿它计代理健康度，不触发
		// browser_unavailable 误降级。
		s.fatalErr = pageFetchErr(msg)
		s.cond.Broadcast()
		// pre-headers 就到错（Failed to fetch / 代理 502 / 断网）：
		// 毫秒级唤醒 <-s.hdrCh，不让调用方干等 180s 超时。
		s.pushHeaderErr(s.fatalErr)
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
// 只搬运正文 chunk；headers 元数据不经这里。pumpDone 在退出时关闭，
// 是「泵协程真退了」的权威信号（closed 是 fail 标记，不等价）。
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
// 每条流同时做三件事：
//   - pushHeaderErr：还在等 headers 的调用方毫秒级醒来；
//   - fail：标记终态 + 唤醒 queue 等待方 + close(closed)；
//   - pipeR.Close：headers 后已收 chunk、正阻塞在 pipeW.Write 的背压
//     pumpToPipe 立刻被唤醒退出——不关它的话泵协程带着队列与缓冲
//     一直挂到调用方自己 Close（可能永远不会来）。
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
	allowSwitch bool, exclude map[int64]bool) (int, io.ReadCloser, error) {
	return m.doBrowserStreamRequestCtx(ctx, "", account, fetchURL, method, headers, body, allowSwitch, exclude)
}

// doBrowserStreamRequestCtx 比 doBrowserStreamRequest 多收一个 proxyURL：
// ensureRunning 刚完成的冷启动桥用的是首航保守出口（initialBridgeUpstream），
// 本次请求真正选定的 proxyURL 必须在 fetch 派发前同步到桥——冷启动丢
// proxyURL 会让第一发请求走错出口/被禁直连的桥拒掉。
// proxyURL="" 时不动桥（调用方没给出口信息，维持首航/上次状态）。
func (m *browserManager) doBrowserStreamRequestCtx(ctx context.Context, proxyURL string,
	account *CookieAccount, fetchURL, method string, headers map[string]string, body []byte,
	allowSwitch bool, exclude map[int64]bool) (int, io.ReadCloser, error) {

	if err := m.ensureRunning(ctx); err != nil {
		return 0, nil, err
	}

	m.mu.Lock()
	if m.shuttingDown || m.stopping || m.cleaning {
		m.mu.Unlock()
		return 0, nil, browserErr("browser is shutting down")
	}
	m.mu.Unlock()

	// 策略 M1：有驻留号时优先用驻留号；要换号走 maybeSwitchAccount。
	// allowSwitch=false（辅助/管理入口）：jar 不能用 = errBrowserSkipAux
	// 无条件回退纯 Go HTTP——绝不借用驻留 jar，也绝不报 502。
	if account != nil {
		if !allowSwitch {
			if err := m.auxJarBlocked(account); err != nil {
				return 0, nil, err
			}
		} else {
			need, reason := m.shouldSwitch(account, exclude)
			if need {
				if err := m.maybeSwitchAccount(ctx, account, reason); err != nil {
					return 0, nil, err
				}
			}
		}
	}

	if err := m.ensureBridge(ctx); err != nil {
		return 0, nil, err
	}

	s := newBrowserStream()
	m.registerStream(s)

	// 交付前统一兜底：任何早退/错误/重定向路径都走同一个 defer 清理，
	// pipeR.Close 必须无条件执行——重定向响应若已推 chunk 阻塞在
	// pipeW.Write，pipeR.Close 瞬间唤醒 pumpToPipe 退出，不留泄漏。
	// delivered 置 true 后清理责任移交给 streamReadCloser。
	delivered := false
	defer func() {
		if !delivered {
			m.unregisterStream(s)
			s.fail(browserErr("stream not delivered"))
			_ = s.pipeR.Close()
		}
	}()

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

	// jarRW 读锁段：驻留身份校验 + evaluate 派发是一条「读」临界区，与
	// maybeSwitchAccount 的写锁（清种+播种）互斥。拿锁期间驻留身份
	// 不可能被偷换——A 核完身份后，切号的写锁要等到 A 的 fetch 已经
	// 派发进当前 jar 才放行，Cookie 与 Token 永不混配。
	// 冷启动后首次走这里时 proxyURL 同步也在这把锁里做：bridge 已存在
	// （ensureRunning 成功保证非 nil），仅在相对当前桥出口确实变更时
	// 才 SetUpstream——不扰动已稳定的隧道。
	m.jarRW.RLock()
	m.mu.Lock()
	client := m.client
	sess := m.sessionID
	residentSeed := m.seed.AccountID
	residentIdentity := m.seed.CookieIdentity
	m.mu.Unlock()
	if client == nil {
		m.jarRW.RUnlock()
		stopWatch()
		s.fail(browserErr("cdp client gone"))
		return 0, nil, s.fatalErr
	}
	if proxyURL != "" {
		syncBridgeUpstream(proxyURL)
	}
	// 最终身份守卫（无条件，不看 allowSwitch）：点名了账号的请求，其
	// Cookie 身份必须与驻留 jar 一致才允许发——异身份无论主请求还是
	// 辅助入口都绝不串进 jar。同 cookieIdentity 的不同 authuser（DB 里
	// ID 不同但 Cookie 同串）合法共用 jar，/u/N 由请求路径区分。
	if account != nil && account.ID != 0 &&
		(residentSeed == 0 || residentIdentity == "" ||
			residentIdentity != cookieIdentity(account.Cookie)) {
		m.jarRW.RUnlock()
		stopWatch()
		if !allowSwitch {
			s.fail(errBrowserSkipAux)
			return 0, nil, errBrowserSkipAux
		}
		s.fail(browserErr("jar identity mismatch: resident %q != request %q",
			residentIdentity, cookieIdentity(account.Cookie)))
		return 0, nil, s.fatalErr
	}
	errEval := client.Call(ctx, sess, "Runtime.evaluate",
		map[string]interface{}{"expression": expr, "awaitPromise": false}, nil)
	m.jarRW.RUnlock()
	if errEval != nil {
		stopWatch()
		s.fail(errEval)
		return 0, nil, browserErr("evaluate fetch: %w", errEval)
	}

	// 响应头经独立 hdrCh：等它或 ctx 取消/流出错。
	var hdr browserStreamHeader
	select {
	case hdr = <-s.hdrCh:
		// got it
	case <-ctx.Done():
		stopWatch()
		s.Abort(m)
		return 0, nil, ctx.Err()
	}
	if hdr.err != nil {
		stopWatch()
		return 0, nil, hdr.err
	}
	// redirect:"follow" 后看终态：跳离 gemini.google.com（accounts.google.com
	// 登录页、google.com/sorry 拦截页）按 appPageStatusError 返回，让既有
	// xsrfErrBlamesCookie 与失效 Cookie 换号/停用逻辑完整生效。
	// 用 url.Parse 精确校验 host：不再用 hdr.redirected 布尔 + HasPrefix
	// 字符串前缀——同源的合法 302（如 /app → /app 的斜杠/语言跟随、
	// 登录态内的内部跳转）不该被误杀；只有终态真落到别的 host 才算跨源。
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
	return hdr.status, &streamReadCloser{s: s, m: m, stopWatch: stopWatch}, nil
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
	allowSwitch bool, exclude map[int64]bool) (int, []byte, error) {
	return m.doBrowserFetchCtx(ctx, "", account, fetchURL, method, headers, body, allowSwitch, exclude)
}

// doBrowserFetchCtx 同 doBrowserFetch，额外收 proxyURL 供冷启动后同步桥出口。
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

// doBrowserPostCtx 同 doBrowserPost，额外收 proxyURL 供冷启动后同步桥出口。
func (m *browserManager) doBrowserPostCtx(ctx context.Context, proxyURL string,
	account *CookieAccount, fetchURL string, headers map[string]string, body []byte,
	allowSwitch bool, exclude map[int64]bool) (int, []byte, error) {
	return m.doBrowserFetchCtx(ctx, proxyURL, account, fetchURL, "POST", headers, body, allowSwitch, exclude)
}

// fetchAppTokensViaBrowser 用浏览器 fetch 拿 xsrfEntry。
// 读 /app 的 WIZ_global_data；account.AuthUser>0 时在同 Tab 里 fetch /u/N/app。
//
// 独立预算：调用方 xsrf.go 给的是 30s，策略 M1 切号冷却也最长 30s——
// 让切号冷却吃光整个预算必然 DeadlineExceeded，所以本函数自己兜一个
// 20s 上限（页面 fetch 实测 1-3s），把冷却+抓页都控在预算内。
func (m *browserManager) fetchAppTokensViaBrowser(ctx context.Context,
	account *CookieAccount, allowSwitch bool, exclude map[int64]bool) (xsrfEntry, error) {
	return m.fetchAppTokensViaBrowserCtx(ctx, "", account, allowSwitch, exclude)
}

// fetchAppTokensViaBrowserCtx 同 fetchAppTokensViaBrowser，额外收 proxyURL
// 用于冷启动后的桥出口同步（见 doBrowserStreamRequestCtx）。
func (m *browserManager) fetchAppTokensViaBrowserCtx(ctx context.Context, proxyURL string,
	account *CookieAccount, allowSwitch bool, exclude map[int64]bool) (xsrfEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	// 辅助入口（allowSwitch=false）先判 jar 适用性：未播种/驻留号不可用/
	// 非驻留身份一律 errBrowserSkipAux，连 ensureRunning 都不做——辅助入口
	// 既无权拉起浏览器，也无权借用别的号的 jar。
	if account != nil && !allowSwitch {
		if err := m.auxJarBlocked(account); err != nil {
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
	// /app 页面必须 GET（POST 实测回 405），token 就在响应 HTML 的 WIZ_global_data。
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

// stopRemaining 返回调用方 ctx 的剩余时间；ctx 无 deadline 时按默认
// 停机总预算 2.5s 算。所有停机步骤严格共享这条预算，绝不各自再要一份。
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
	// 立即中断冷启动：doStart 全程绑 m.startCancel 派生的 startCtx
	// （waitDevToolsPort/dialCDPWebsocket/waitPageLoad/installBridge 都在
	// 监听它）。不取消的话，Stop 超时后启动协程还会把局部 proc 一路
	// 推进到发布校验才回收——白白多占一个 Chromium 到自然超时。
	if m.startCancel != nil {
		m.startCancel()
	}
	// 立即打断正在冷却等待中的 maybeSwitchAccount：stopCh 一关，
	// 30s 冷却毫秒级醒来释放 switchSem，不等自然睡完。
	// stopCh 在 m.mu 下只关闭一次（重复 Stop 不能对已关闭的 channel 再 close）；
	// 独立构造的 manager 可能还没初始化 stopCh，nil 时先建再关。
	if m.stopCh == nil {
		m.stopCh = make(chan struct{})
	}
	select {
	case <-m.stopCh:
		// 已关闭：上次 Stop 已打断过，不重复 close。
	default:
		close(m.stopCh)
	}
	// 若启动协程正在跑，等它 readyCh 关闭（startCtx 已被取消，doStart
	// 各步会快速收尾；Stop 的 ctx 到点就先回收快照，不等它自然走完）。
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

	// 切号进行中用剩余 ctx 抢占 switchSem（可能正被冷却中的
	// maybeSwitchAccount 持着；stopCh 已先打断它，这里会很快拿到）。
	// ctx 超时拿不到也继续往下——不能为等一个信号量把整个停机拖死。
	select {
	case m.switchSem <- struct{}{}:
		<-m.switchSem
	case <-ctx.Done():
	}

	// 停机前强制无节流写回 Cookie（新刷 SIDCC 不能丢在 jar 里）；
	// 透传剩余 ctx 预算（封顶 800ms），不再用独立 Background+5s 硬撑。
	m.syncCookiesBackForce(ctx)

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

	// 回收链严格共享调用方剩余预算：Browser.close 封顶 min(remaining, 100ms)，
	// killBrowserProcessGroup 拿实际剩余（不额外叠加固定 1800ms）。
	// 1. CDP Browser.close：Chromium 会自己写回 cookie jar 再退。
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
	// 2. 进程组优雅 → 强杀（Windows 内部再拆优雅/强杀份额，见
	//    browser_proc_windows.go；剩余为 0 也要把 /F 那一下发出去）。
	if proc != nil {
		killBrowserProcessGroup(proc.PID, proc.PGID, stopRemaining(ctx))
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
	// stopCh 复位：停机清场完毕，重建一个新 channel 给下一轮生命周期——
	// maybeSwitchAccount 等待时拿到的必须是非 nil 且未关闭的 channel。
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

// 供 net/url 引用（避免误删 import）。
var _ = url.URL{}
