//go:build darwin

package app

import "syscall"

// sysProcAttr 返回 Darwin 的 SysProcAttr：只有独立进程组；macOS 没有
// Pdeathsig 等价物，进程被强杀后的孤儿靠 reclaimOrphanBrowser 回收。
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
