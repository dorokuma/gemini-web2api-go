//go:build windows

package app

// browser_proc_windows.go —— Windows 上的常驻浏览器子进程管理。
//
// Windows 没有 Unix 进程组信号语义，但有一组等价物：
//   - CREATE_NEW_PROCESS_GROUP 让浏览器脱离我们的控制台 Ctrl 事件；
//   - taskkill /T 杀整棵进程树（等价于 Unix 的 kill -pgid）；
//   - taskkill /F 强杀（等价于 SIGKILL）。
//
// 孤儿回收不靠 cmdline（Windows 下 wmic 已废弃、PowerShell 冷启动慢且依赖
// 不一），改走更稳的路：browser-instance.json 里记的 PID 先跟 tasklist 对活，
// 再靠「profile 目录的 SingletonLock 已被 Chromium 自己删除」作为存活的间接
// 证据——这条在 Windows 上不如 Linux 的 /proc 硬，但 6 平台交叉编译零 CGO
// 是硬约束，WMI / NtQueryInformationProcess 都不在标准库里。宁可放过一个
// 极少数的 PID 复用（概率远低于 Unix，因为 PID 空间大得多），也不冒险杀错。

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// browserProc 与 unix 版同形，PGID 字段仅为对称；Windows 杀树靠 taskkill /T。
type browserProc struct {
	Cmd  *exec.Cmd
	PID  int
	PGID int
}

// spawnBrowser 启动浏览器子进程。CREATE_NEW_PROCESS_GROUP 让它不继承我们的
// 控制台 Ctrl+C 事件（否则我们收到 Ctrl+C 时浏览器也会被打断，Shutdown 就没机会
// 做优雅关闭）。
func spawnBrowser(name string, args []string, env []string) (*browserProc, error) {
	cmd := exec.Command(name, args...)
	if env != nil {
		cmd.Env = env
	}
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &browserProc{Cmd: cmd, PID: cmd.Process.Pid, PGID: cmd.Process.Pid}, nil
}

// killBrowserProcessGroup 用 taskkill 杀整棵树。/T = 树，/F = 强杀，/PID = 根。
// 先不带 /F 试一次优雅退出（Chromium 收到窗口消息会自己清理），超时后再补 /F。
//
// 严禁用 PowerShell Stop-Process -Recurse / WMI 这类更重的外部依赖：taskkill
// 是 Windows 自带的唯一「按 PID 精确指树」工具。
func killBrowserProcessGroup(pid, pgid int, gracefulTimeout time.Duration) {
	if pid <= 0 {
		return
	}
	_ = exec.Command("taskkill", "/T", "/PID", strconv.Itoa(pid)).Run()
	deadline := time.Now().Add(gracefulTimeout)
	for time.Now().Before(deadline) {
		if !pidAlive(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).Run()
}

// pidAlive 用 tasklist 查 PID 是否还在（不依赖 /proc，标准库能跑）。
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	out, err := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/NH").Output()
	if err != nil {
		return false
	}
	// 活进程输出带镜像名，死了输出 "INFO: No tasks are running..."
	return !strings.Contains(string(out), "No tasks")
}

// browserInstanceFile 与 unix 版同形。
type browserInstanceFile struct {
	PID       int    `json:"pid"`
	PGID      int    `json:"pgid"`
	Profile   string `json:"profile"`
	StartedAt int64  `json:"started_at"`
}

func writeBrowserInstance(profileDir string, p *browserProc) error {
	b, err := json.Marshal(browserInstanceFile{
		PID: p.PID, PGID: p.PGID, Profile: profileDir, StartedAt: time.Now().Unix(),
	})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(profileDir, "browser-instance.json"), b, 0o600)
}

func clearBrowserInstance(profileDir string) {
	_ = os.Remove(filepath.Join(profileDir, "browser-instance.json"))
}

// reclaimOrphanBrowser 见 unix 版注释。Windows 侧额外做一步防御：
// 记录里的 Profile 跟当前 profileDir 不一致时直接跳过——说明这个 json 是
// 别的 profile 目录的（或者被手改过），不能动它的 PID。
func reclaimOrphanBrowser(profileDir string) {
	instPath := filepath.Join(profileDir, "browser-instance.json")
	raw, err := os.ReadFile(instPath)
	if err == nil {
		var inst browserInstanceFile
		if json.Unmarshal(raw, &inst) == nil && inst.PID > 0 {
			abs, _ := filepath.Abs(profileDir)
			want, _ := filepath.Abs(inst.Profile)
			if strings.EqualFold(abs, want) && processIsOurBrowser(inst.PID, profileDir) {
				logf("[browser] 回收上轮遗留的浏览器进程树 pid=%d", inst.PID)
				killBrowserProcessGroup(inst.PID, inst.PGID, 1500*time.Millisecond)
			}
		}
	}
	for _, name := range []string{"SingletonLock", "SingletonCookie", "SingletonSocket"} {
		_ = os.Remove(filepath.Join(profileDir, name))
	}
	_ = os.Remove(instPath)
}

// processIsOurBrowser 的 Windows 版：活进程 + 记录的 profile 一致即认为命中。
//
// Windows 没法像 Linux 那样零成本读 cmdline（WMI 重、NtQueryInformationProcess
// 不在标准库），而 PID 复用概率远低于 Unix（PID 空间 2^22 且分配更稀疏），
// 用「还活着 + 同一个 profile 记录」做判据已经是这个平台上稳态可做的最强
// 校验；误杀场景只剩「浏览器死了、PID 恰好被分给另一个进程、且我们紧接着
// 启动」——即便撞上，taskkill 只杀那一棵树，不会波及整组无名进程。
func processIsOurBrowser(pid int, profileDir string) bool {
	return pidAlive(pid)
}
