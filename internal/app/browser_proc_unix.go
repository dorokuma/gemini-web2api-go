//go:build !windows

package app

// browser_proc_unix.go —— Linux / macOS 上的常驻浏览器子进程管理。
//
// 三件事必须同时做对，少一件都会留僵尸或误杀：
//  1. 独立进程组（Setpgid）：Chromium 会 fork 出 renderer/GPU/utility 一串子进程，
//     只 kill 主 PID 会留下整组渲染进程吃内存；按 -pgid 发信号才能整组收走。
//  2. Pdeathsig（仅 Linux）：我们进程被 SIGKILL 时 Go 来不及跑 defer，设了
//     Pdeathsig 的 Chromium 会收到 SIGTERM 跟着退，不至于变成孤儿。
//     macOS 没有等价物，靠 reclaimOrphanBrowser 用 profile 目录里的
//     browser-instance.json 在下次启动时回收。
//  3. 孤儿回收必须「精确核验」：browser-instance.json 记的是上次写的 PID，
//     重启后 PID 可能已经被系统分给了别的进程。不校验命令行直接 kill -pgid，
//     会把无辜进程整组带走。所以先对 /proc/<pid>/cmdline（Linux）或
//     ps -ww -o command=（macOS）做可执行名 + --user-data-dir 双重匹配。

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
type browserProc struct {
	Cmd  *exec.Cmd
	PID  int
	PGID int
}

// spawnBrowser 启动浏览器子进程，返回带 PGID 的句柄。
//
// Setpgid: true 让它脱离我们的进程组单独成组（PGID = 自己的 PID），
// 之后 killBrowserProcessGroup 可以用 -PGID 一次发整组。
// Linux 上额外套 Pdeathsig：父进程（也就是我们）死时内核给子进程发 SIGTERM，
// 兜底进程被强杀来不及 Shutdown 的场景。
func spawnBrowser(name string, args []string, env []string) (*browserProc, error) {
	cmd := exec.Command(name, args...)
	if env != nil {
		cmd.Env = env
	}
	cmd.Stdout = nil // 不接 stdout：Chromium 自己会往 stderr 写 DevTools listening
	cmd.Stderr = nil // 日志走文件/系统，留给我们就是噪音

	pdeathsig := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		// 必须在独立 OS 线程上设置 Pdeathsig——它是 per-thread 的进程属性，
		// runtime 可能把 goroutine 调度到别的线程上执行 cmd.Start。
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		cmd.SysProcAttr = browserSysProcAttr()
		close(pdeathsig)
		done <- cmd.Start()
	}()
	<-pdeathsig
	if err := <-done; err != nil {
		return nil, err
	}
	return &browserProc{Cmd: cmd, PID: cmd.Process.Pid, PGID: cmd.Process.Pid}, nil
}

// killBrowserProcessGroup 先向整个进程组发 SIGTERM，最多等 gracefulTimeout，
// 超时再 SIGKILL。pid 仅用于兜底（没拿到 pgid 的极端情况）。
//
// 严禁 pkill / killall：它们按名字模糊匹配，容器里同名的别的 Chromium
// （比如用户的调试实例）会被一起带走；进程组 + 已核验 PID 是唯一能精确
// 命中「我们 spawn 的那一棵」的办法。
func killBrowserProcessGroup(pid, pgid int, gracefulTimeout time.Duration) {
	if pgid > 0 {
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	} else if pid > 0 {
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
	deadline := time.Now().Add(gracefulTimeout)
	for time.Now().Before(deadline) {
		if !pidAlive(pid) {
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
//  2. 精确核验 cmdline：可执行文件基名要含 chrom（Chromium/Chrome/Edge 都含），
//     且参数里必须带 --user-data-dir=<本 profile 绝对路径>——两者都中才说明
//     这个 PID 现在仍是我们的浏览器，而不是 PID 被系统复用给了别的进程；
//  3. 核验通过才 kill -pgid；核验不过就当它已经死了，只清理现场；
//  4. 顺手删掉指向已死 PID 的 SingletonLock / SingletonCookie / SingletonSocket：
//     Chromium 启动时会检查 SingletonLock，里面记着一个已死 PID 时会判定
//     "另一个实例还活着"然后直接退出或开新 profile 失败。
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
			}
		}
	}
	// 陈旧 Singleton* 一律清：lock 里的 PID 若还活着且核验过是我们自己的，
	// 上面已经杀了；若不是我们的，这个 profile 目录本来就只能我们一家用。
	for _, name := range []string{"SingletonLock", "SingletonCookie", "SingletonSocket"} {
		_ = os.Remove(filepath.Join(profileDir, name))
	}
	_ = os.Remove(instPath)
}

// processIsOurBrowser 精确核验 pid 当前是不是「我们这个 profile 的浏览器」。
//
// 两条件同时成立才返回 true：
//   - 可执行文件基名含 "chrom"（覆盖 chromium / chrome / msedge / brave ——
//     它们文件名里都带这个子串；Firefox 不在我们的 spawn 名单里，天然排除）；
//   - 完整参数列表里带 --user-data-dir=<profileDir 的绝对路径>。
//
// 这条检查把「PID 复用」挡在 kill 之前：只看 PID 就杀，重启后可能把系统
// 新分给别人的进程整组带走。
//
// Linux 的 /proc/<pid>/cmdline 是 NUL 分隔，第一个字段就是可执行路径；
// macOS 没有 /proc，用 ps -ww -o command= 拿到整条命令行，按空格切第一词
// 做基名判断、再对整条做 --user-data-dir=<abs> 子串匹配——profileDir 是我们
// 自己生成的固定路径，不含空格，子串匹配不会误伤。
func processIsOurBrowser(pid int, profileDir string) bool {
	abs, err := filepath.Abs(profileDir)
	if err != nil {
		abs = profileDir
	}
	var exeBase, full string
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			return false
		}
		parts := strings.Split(string(b), "\x00")
		if len(parts) == 0 || parts[0] == "" {
			return false
		}
		exeBase = parts[0]
		full = string(b)
	} else {
		out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-ww", "-o", "command=").Output()
		if err != nil {
			return false
		}
		line := strings.TrimSpace(string(out))
		if line == "" {
			return false
		}
		exeBase = line
		if i := strings.IndexByte(line, ' '); i > 0 {
			exeBase = line[:i]
		}
		full = line
	}
	base := strings.ToLower(filepath.Base(exeBase))
	if !strings.Contains(base, "chrom") {
		return false
	}
	return strings.Contains(full, "--user-data-dir="+abs)
}
