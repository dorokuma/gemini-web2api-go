//go:build linux

package app

import "syscall"

// sysProcAttr 返回 Linux 专属的 SysProcAttr：独立进程组 + Pdeathsig 兜底。
// Pdeathsig 是 per-thread 属性，必须在 LockOSThread 的 goroutine 里随 cmd.Start
// 一起设置，见 browser_proc_unix.go 的注释。
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGTERM}
}
