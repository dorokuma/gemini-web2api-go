//go:build !windows

package app

// browser_proc_unix.go —— Linux / macOS 上的常驻浏览器子进程管理。
//
// 三件事必须同时做对，少一件都会留僵尸或误杀：
//  1. 独立进程组（Setpgid）：Chromium 会 fork 出 renderer/GPU/utility 一串子进程，
//     只 kill 主 PID 会留下整组渲染进程吃内存；按 -pgid 发信号才能整组收走。
//     根 PID 先退、渲染子进程还活着时，必须按进程组存活（kill -pgid 0）判断，
//     只看根 PID 会把整组孤儿留成内存泄漏。
//  2. Pdeathsig（仅 Linux）：它是 per-thread 属性，绑定在哪个 OS 线程上，
//     那个线程死了子进程才收信号。Go 运行时会回收线程归池复用——如果设完
//     Pdeathsig 就 UnlockOSThread，这个线程可能接着去跑别的 goroutine，等于
//     把「我死了就杀 Chromium」挂到了无关任务头上（误杀）；而且 spawnBrowser
//     返回后没人 cmd.Wait()，子进程 100% 变 <defunct> 僵尸。所以 reaper
//     协程全程 LockOSThread 直到 cmd.Wait() 返回：Pdeathsig 的生命周期严格
//     跟随子进程，线程退出时才解锁归池。
//  3. 孤儿回收必须「精确核验」：browser-instance.json 记的是上次写的 PID，
//     重启后 PID 可能已经被系统分给了别的进程。不校验命令行直接 kill -pgid，
//     会把无辜进程整组带走。Linux 按 \x00 切 /proc/<pid>/cmdline 做参数级
//     精确比对（防 /tmp/p 误匹配 /tmp/p-other）；macOS 用 ps 处理带空格的
//     "Google Chrome" 路径。核验不过的活进程，Singleton* 一律不删——双实例
//     并发写同一 profile 的 SQLite 会写坏。

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// browserSysProcAttr 由平台文件提供：Linux 有 Pdeathsig，Darwin 只有 Setpgid。
func browserSysProcAttr() *syscall.SysProcAttr {
	return sysProcAttr()
}

// browserProc 记录一次 spawn 出来的浏览器进程与其进程组。
// exitedCh 由 reaper 协程在 cmd.Wait() 返回后关闭：进程真退出的权威信号，
// Alive() 靠它而不是只看 kill(pid,0)——僵尸进程信号 0 仍返回成功，
// 会被误判成「活着」。
type browserProc struct {
	Cmd      *exec.Cmd
	PID      int
	PGID     int
	exitedCh chan struct{} // reaper 在 cmd.Wait() 返回后关闭
}

// Alive 非阻塞检查进程是否还活着：
//   - exitedCh 已关 = reaper 亲眼看到 cmd.Wait() 返回，100% 死了；
//   - 否则再问内核 kill(pid,0)——信号 0 对僵尸仍返回成功，所以这只是
//     「可能活着」的佐证，exitedCh 才是权威。
func (p *browserProc) Alive() bool {
	if p == nil {
		return false
	}
	select {
	case <-p.exitedCh:
		return false
	default:
	}
	return pidAlive(p.PID)
}

// spawnBrowser 启动浏览器子进程，返回带 PGID 的句柄。
//
// Setpgid: true 让它脱离我们的进程组单独成组（PGID = 自己的 PID），
// 之后 killBrowserProcessGroup 可以用 -PGID 一次发整组。
// Linux 上额外套 Pdeathsig：父进程（也就是我们）死时内核给子进程发 SIGTERM，
// 兜底进程被强杀来不及 Shutdown 的场景。
//
// reaper 协程的生命周期 = 子进程的生命周期：
//   - LockOSThread() 后**不提前 Unlock**：Pdeathsig 绑定在这个线程上，
//     线程复用会误杀（见文件头注释 2）；
//   - cmd.Start() 之后立刻阻塞 cmd.Wait()：100% 回收子进程，零僵尸；
//   - Wait 返回才关 exitedCh（Alive() 的权威信号）并 UnlockOSThread 归池。
func spawnBrowser(name string, args []string, env []string) (*browserProc, error) {
	cmd := exec.Command(name, args...)
	if env != nil {
		cmd.Env = env
	}
	cmd.Stdout = nil // 不接 stdout：Chromium 自己会往 stderr 写 DevTools listening
	cmd.Stderr = nil // 日志走文件/系统，留给我们就是噪音

	startErr := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		cmd.SysProcAttr = browserSysProcAttr()
		if err := cmd.Start(); err != nil {
			startErr <- err
			close(exited)
			runtime.UnlockOSThread()
			return
		}
		startErr <- nil
		// 阻塞到子进程真退出：回收僵尸 + 释放 exitedCh + 线程归池，
		// 三件事同一条生命线上做，缺一不可。
		_ = cmd.Wait()
		close(exited)
		runtime.UnlockOSThread()
	}()
	if err := <-startErr; err != nil {
		return nil, err
	}
	return &browserProc{
		Cmd:      cmd,
		PID:      cmd.Process.Pid,
		PGID:     cmd.Process.Pid,
		exitedCh: exited,
	}, nil
}

// killBrowserProcessGroup 先向整个进程组发 SIGTERM，最多等 gracefulTimeout，
// 超时再 SIGKILL。pid 仅用于兜底（没拿到 pgid 的极端情况）。
//
// 等待判据是**进程组是否整体死透**（kill -pgid 0），不是根 PID：Chromium
// 主进程收到 TERM 会自己先退，渲染进程可能多活几百毫秒；只看根 PID 的话
// SIGKILL 那段就跳过了，整组渲染进程被留成孤儿。组信号对「组已不存在」
// 返回 ESRCH，对组里任一成员活着返回 nil——正好是我们要的语义。
//
// 严禁 pkill / killall：它们按名字模糊匹配，容器里同名的别的 Chromium
// （比如用户的调试实例）会被一起带走；进程组 + 已核验 PID 是唯一能精确
// 命中「我们 spawn 的那一棵」的办法。
func killBrowserProcessGroup(pid, pgid int, gracefulTimeout time.Duration) {
	groupAlive := func() bool {
		if pgid > 0 {
			err := syscall.Kill(-pgid, 0)
			return err == nil || errors.Is(err, syscall.EPERM)
		}
		return pidAlive(pid)
	}
	if pgid > 0 {
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	} else if pid > 0 {
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
	deadline := time.Now().Add(gracefulTimeout)
	for time.Now().Before(deadline) {
		if !groupAlive() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pgid > 0 {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	} else if pid > 0 {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// pidAlive 轻探活：信号 0 不真发，只问内核进程在不在。
// 僵尸进程返回 true（内核视角它还在），调用方要真「死没死」看 proc.Alive()。
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// browserInstanceFile 是 profile 目录里我们写的「这台浏览器是谁」记录。
// reclaimOrphanBrowser 靠它在进程重启后找到上次没退干净的 PID。
type browserInstanceFile struct {
	PID       int    `json:"pid"`
	PGID      int    `json:"pgid"`
	Profile   string `json:"profile"`
	StartedAt int64  `json:"started_at"`
}

// writeBrowserInstance 把当前 spawn 结果落盘，供下次启动回收孤儿。
// 文件权限 0600：里面只是 PID/路径，但要跟 profile 目录（0700）一个口径。
func writeBrowserInstance(profileDir string, p *browserProc) error {
	b, err := json.Marshal(browserInstanceFile{
		PID: p.PID, PGID: p.PGID, Profile: profileDir, StartedAt: time.Now().Unix(),
	})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(profileDir, "browser-instance.json"), b, 0o600)
}

// clearBrowserInstance 进程正常退出后清掉记录，避免下次回收假目标。
func clearBrowserInstance(profileDir string) {
	_ = os.Remove(filepath.Join(profileDir, "browser-instance.json"))
}

// reclaimOrphanBrowser 启动前清上一轮没退干净的浏览器。
//
// 步骤：
//  1. 读 browser-instance.json 拿上次的 PID/PGID；
//  2. 精确核验：可执行名属于 chromium/chrome/msedge/brave 一族，且参数里
//     有恰好等于 --user-data-dir=<本 profile 绝对路径> 的完整参数——两者
//     都中才说明这个 PID 现在仍是我们的浏览器；
//  3. 核验通过才 kill -pgid；核验不过但进程还活着时**绝不删 SingletonLock/
//     SingletonCookie/SingletonSocket**——那可能是另一个真在跑、共用此
//     profile 的 Chromium，删了它的 Singleton 文件再开新实例就是双实例
//     并发写同一 SQLite，profile 直接写坏；
//  4. 顺手删 DevToolsActivePort：文件里记的是上一轮的死端口，留着会让
//     waitDevToolsPort 连到一个已经没人听的端口上 ECONNREFUSED 白等。
func reclaimOrphanBrowser(profileDir string) {
	instPath := filepath.Join(profileDir, "browser-instance.json")
	raw, err := os.ReadFile(instPath)
	if err == nil {
		var inst browserInstanceFile
		if json.Unmarshal(raw, &inst) == nil && inst.PID > 0 {
			if processIsOurBrowser(inst.PID, profileDir) {
				logf("[browser] 回收上轮遗留的浏览器进程组 pid=%d pgid=%d", inst.PID, inst.PGID)
				pgid := inst.PGID
				if pgid <= 0 {
					pgid = inst.PID
				}
				killBrowserProcessGroup(inst.PID, pgid, 1500*time.Millisecond)
			} else if pidAlive(inst.PID) {
				// PID 被一个不是我们的浏览器的进程占着（PID 复用）：
				// Singleton* 可能是「另一个活着的实例」的，动不得；
				// instance 记录也留着——万一对方就是我们没核验出的浏览器。
				logf("[browser] browser-instance 的 pid=%d 活着但核验不过，"+
					"不动 Singleton* 也不杀（防误伤 PID 复用）", inst.PID)
				return
			}
		}
	}
	for _, name := range []string{"SingletonLock", "SingletonCookie", "SingletonSocket"} {
		_ = os.Remove(filepath.Join(profileDir, name))
	}
	_ = os.Remove(instPath)
	// DevToolsActivePort 记的是上一轮的调试端口，浏览器已死它只会误导
	// waitDevToolsPort 去连一个没人听的端口。
	_ = os.Remove(filepath.Join(profileDir, "DevToolsActivePort"))
}

// browserExeNames 是 processIsOurBrowser 认可的可执行名白名单（全小写基名
// 子串匹配）。chromium / chrome / brave / msedge / microsoft-edge 都在我们
// 的 spawn 候选名单里；Firefox 不在，天然排除。
var browserExeNames = []string{
	"chrom", // chromium / chrome / "Google Chrome" / "Chromium" 全覆盖
	"msedge",
	"microsoft-edge",
	"microsoft edge",
	"brave",
}

// processIsOurBrowser 精确核验 pid 当前是不是「我们这个 profile 的浏览器」。
//
// 两条件同时成立才返回 true：
//   - 可执行文件基名（或带空格路径的最后一个词，如 "Google Chrome"）命中
//     白名单；
//   - 完整参数列表里有一个**恰好等于** "--user-data-dir=<abs>" 的独立参数：
//     按 NUL（Linux）或 shell 词边界（macOS）切，绝不子串匹配——
//     "/tmp/p" 不能被 "/tmp/p-other" 骗过。
//
// 这条检查把「PID 复用」挡在 kill 之前：只看 PID 就杀，重启后可能把系统
// 新分给别人的进程整组带走。
//
// Linux 的 /proc/<pid>/cmdline 是 NUL 分隔，逐字段精确比对最干净；
// macOS 没有 /proc，用 ps -ww -o command= 拿整条命令行按空格切词——
// Chromium 的启动参数不含带空格的值（user-data-dir 是我们生成的固定路径），
// 空格切词在这个参数集合上是安全的。
func processIsOurBrowser(pid int, profileDir string) bool {
	abs, err := filepath.Abs(profileDir)
	if err != nil {
		abs = profileDir
	}
	want := "--user-data-dir=" + abs
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			return false
		}
		parts := strings.Split(string(b), "\x00")
		if len(parts) == 0 || parts[0] == "" {
			return false
		}
		if !exeNameOK(parts[0]) {
			return false
		}
		for _, a := range parts[1:] {
			if a == want {
				return true
			}
		}
		return false
	}
	// darwin：ps -ww -o command= 给整条命令行。带空格的可执行路径
	// （"Google Chrome.app/Contents/MacOS/Google Chrome"）按空格切词后
	// 可执行段在 "--user-data-dir=" 之前的整段，不能再按词逐个碰运气——
	// profile 路径本身可能含 "chrome"/"brave" 之类的词，误当可执行名
	// 会放过一个根本不是浏览器的进程。
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-ww", "-o", "command=").Output()
	if err != nil {
		return false
	}
	line := strings.TrimSpace(string(out))
	if line == "" {
		return false
	}
	return darwinCmdlineIsOurBrowser(line, want)
}

// darwinCmdlineIsOurBrowser 是 Darwin 命令行的纯解析判据（可单测）：
//   - 先定位 "--user-data-dir=" 首次出现之前的片段作为可执行路径段，
//     校验它的基名含合法浏览器名；
//   - 再按 " --" 边界切出每个选项，找恰好等于 want 的 --user-data-dir=<abs>
//     ——profile 路径可含空格，按 " --" 而不是空格切词才不会把一个带
//     空格的 profile 值切碎。
func darwinCmdlineIsOurBrowser(line, want string) bool {
	// 可执行路径段 = "--user-data-dir=" 之前的全部（含内部空格）。
	idx := strings.Index(line, "--user-data-dir=")
	if idx <= 0 {
		return false
	}
	exeSeg := strings.TrimSpace(line[:idx])
	if !exeNameOK(exeSeg) {
		return false
	}
	// 参数段按 " --" 边界切，逐项精确比对 --user-data-dir=<abs>。
	args := line[idx:]
	for _, a := range strings.Split(args, " --") {
		a = strings.TrimSpace(a)
		if a == want {
			return true
		}
	}
	return false
}

// exeNameOK 校验一个「可能是可执行路径」的词：取基名小写后命中白名单。
// "Google" 这类非路径词取基名后不匹配，天然被滤掉。
func exeNameOK(word string) bool {
	base := strings.ToLower(filepath.Base(word))
	// 可执行名里可能带版本/后缀（chrome.exe 的 "chrome"），也认 ".app" 路径
	// 切出的 "Google Chrome" 末词——白名单是子串匹配。
	for _, ok := range browserExeNames {
		if strings.Contains(base, ok) {
			return true
		}
	}
	return false
}
