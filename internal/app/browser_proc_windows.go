//go:build windows

package app

// browser_proc_windows.go —— Windows 上的常驻浏览器子进程管理。
//
// Windows 没有 Unix 进程组信号语义，但有一组等价物：
//   - CREATE_NEW_PROCESS_GROUP 让浏览器脱离我们的控制台 Ctrl 事件；
//   - taskkill /T 杀整棵进程树（等价于 Unix 的 kill -pgid）；
//   - taskkill /F 强杀（等价于 SIGKILL）。
//
// 孤儿回收的核验走 tasklist 的 CSV 输出（/FO CSV /NH）：文本模式的
// "No tasks are running..." 是按 UI 语言本地化的——中文 Windows 输出
// "信息: 没有运行的任务..."，英文串匹配不到会被误判成「进程还活着」
// 然后把无辜 PID 的进程树杀掉。CSV 第一列永远是镜像名，语言无关；
// 再比对镜像名属于 chrome/chromium/msedge/brave 才允许 taskkill，
// 核不准宁可不杀。
//
// 所有 taskkill / tasklist 调用都经 exec.CommandContext 绑定剩余超时，
// 绝不无界阻塞（外部命令卡住不能拖死调用方）。

import (
	"context"
	"encoding/csv"
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

// browserProc 与 unix 版同形，exitedCh 同样是 reaper 的权威退出信号；
// PGID 字段仅为对称（Windows 杀树靠 taskkill /T）。
type browserProc struct {
	Cmd      *exec.Cmd
	PID      int
	PGID     int
	exitedCh chan struct{}
}

// Alive 非阻塞检查：exitedCh 关了就是真死了；否则再问 tasklist。
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

// spawnBrowser 启动浏览器子进程。CREATE_NEW_PROCESS_GROUP 让它不继承我们的
// 控制台 Ctrl+C 事件（否则我们收到 Ctrl+C 时浏览器也会被打断，Shutdown 就没机会
// 做优雅关闭）。reaper 协程 cmd.Wait() 回收：Windows 下不 Wait 一样留僵尸句柄。
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
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	return &browserProc{
		Cmd:      cmd,
		PID:      cmd.Process.Pid,
		PGID:     cmd.Process.Pid,
		exitedCh: exited,
	}, nil
}

// killBrowserProcessGroup 用 taskkill 杀整棵树。/T = 树，/F = 强杀，/PID = 根。
// 先不带 /F 试一次优雅退出（Chromium 收到窗口消息会自己清理），超时后再补 /F。
// 两个 taskkill 都绑 ctx：外部命令可能被系统策略卡住，调用方的 gracefulTimeout
// 不能被一个卡住的 taskkill 吃掉。
func killBrowserProcessGroup(pid, pgid int, gracefulTimeout time.Duration) {
	if pid <= 0 {
		return
	}
	// 同一总 deadline：graceful taskkill、等活、强杀 taskkill 共享这一条
	// gracefulTimeout 预算，绝不各自叠加 5s+10s 超出总预算——停机编排
	// 给整个回收链的时间是有限的，每一步都不能自己再要一份。
	deadline := time.Now().Add(gracefulTimeout)
	graceCtx, graceCancel := context.WithDeadline(context.Background(), deadline)
	defer graceCancel()
	_ = exec.CommandContext(graceCtx, "taskkill", "/T", "/PID", strconv.Itoa(pid)).Run()
	for time.Now().Before(deadline) {
		if !pidAlive(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	forceCtx, forceCancel := context.WithDeadline(context.Background(), deadline)
	defer forceCancel()
	_ = exec.CommandContext(forceCtx, "taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).Run()
}

// tasklistImageName 查 PID 现在对应的镜像名（CSV 第一列）。
// 进程不存在时返回 ("", false)；查不动也返回 false——核不准宁可不杀。
func tasklistImageName(pid int) (string, bool) {
	if pid <= 0 {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "tasklist",
		"/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH").Output()
	if err != nil {
		return "", false
	}
	r := csv.NewReader(strings.NewReader(string(out)))
	rec, err := r.Read()
	if err != nil || len(rec) < 2 {
		return "", false
	}
	// CSV 第二列是 PID：对不上说明输出形态异常，宁信错不滥杀。
	if strings.TrimSpace(rec[1]) != strconv.Itoa(pid) {
		return "", false
	}
	return strings.TrimSpace(rec[0]), true
}

// pidAlive 用 tasklist CSV 查 PID 是否还在（语言无关，标准库能跑）。
func pidAlive(pid int) bool {
	_, ok := tasklistImageName(pid)
	return ok
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

// reclaimOrphanBrowser 见 unix 版注释。Windows 侧：
//  1. 记录里的 Profile 跟当前 profileDir 不一致时直接跳过——说明这个 json
//     是别的 profile 目录的（或者被手改过）；
//  2. tasklist 查活 + 镜像名白名单双重核验，核不准宁可不杀；
//  3. 核验不过但进程还活着时，Singleton* 一律不删（防双实例写坏 SQLite）。
func reclaimOrphanBrowser(profileDir string) {
	instPath := filepath.Join(profileDir, "browser-instance.json")
	raw, err := os.ReadFile(instPath)
	if err == nil {
		var inst browserInstanceFile
		if json.Unmarshal(raw, &inst) == nil && inst.PID > 0 {
			abs, _ := filepath.Abs(profileDir)
			want, _ := filepath.Abs(inst.Profile)
			if strings.EqualFold(abs, want) {
				if processIsOurBrowser(inst.PID, profileDir) {
					logf("[browser] 回收上轮遗留的浏览器进程树 pid=%d", inst.PID)
					killBrowserProcessGroup(inst.PID, inst.PGID, 1500*time.Millisecond)
				} else if pidAlive(inst.PID) {
					logf("[browser] browser-instance 的 pid=%d 活着但核验不过，"+
						"不动 Singleton* 也不杀（防误伤 PID 复用）", inst.PID)
					return
				}
			}
		}
	}
	for _, name := range []string{"SingletonLock", "SingletonCookie", "SingletonSocket"} {
		_ = os.Remove(filepath.Join(profileDir, name))
	}
	_ = os.Remove(instPath)
	// 同 unix：上一轮的死端口文件留着只会误导 waitDevToolsPort。
	_ = os.Remove(filepath.Join(profileDir, "DevToolsActivePort"))
}

// windowsBrowserImages 是允许 taskkill 的镜像名白名单（全小写、带 .exe）。
var windowsBrowserImages = map[string]bool{
	"chrome.exe":     true,
	"chromium.exe":   true,
	"msedge.exe":     true,
	"brave.exe":      true,
	"new_chrome.exe": true,
}

// processIsOurBrowser 的 Windows 版：活进程 + 镜像名命中浏览器白名单
// + 真实命令行核验 --user-data-dir=<abs>。
//
// Windows 没法像 Linux 那样零成本读 cmdline（WMI 重、NtQueryInformationProcess
// 不在标准库），但只靠 tasklist 镜像名太弱——镜像名对得上只能说明「这是台
// Chrome」，不能证明它用的是我们这个 profileDir：误杀一个挂在别的 profile 的
// Chrome 树，或误删活实例的 Singleton*，都是不可逆的。这里用 PowerShell 短超时
// （1.5s）拉取 Win32_Process.CommandLine 做精确核验：拿不到命令行、或命令行
// 不含目标 absProfileDir，一律保守返回 false。
func processIsOurBrowser(pid int, profileDir string) bool {
	image, ok := tasklistImageName(pid)
	if !ok {
		return false
	}
	if !windowsBrowserImages[strings.ToLower(image)] {
		return false
	}
	abs, err := filepath.Abs(profileDir)
	if err != nil {
		abs = profileDir
	}
	want := "--user-data-dir=" + abs
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	out, err := exec.CommandContext(ctx, "powershell", "-NoProfile", "-Command",
		fmt.Sprintf("(Get-CimInstance Win32_Process -Filter 'ProcessId=%d').CommandLine", pid)).Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), want)
}
