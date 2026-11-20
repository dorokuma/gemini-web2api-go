package app

// browser_proc_cmdline.go —— 孤儿回收用的「这是不是我们这个 profile 的浏览器」
// 纯解析判据，三平台共享且**无 build tag**：Linux CI 也能直接单测 Darwin 与
// Windows 的命令行解析，正/负前缀碰撞用例不再依赖真实进程。
//
// 共同语义（三平台一致）：
//   1. argv[0] / 可执行段的基名必须命中浏览器白名单（chrom* / msedge /
//      microsoft edge / brave），Firefox、/bin/sh、profile 路径里的 "chrome"
//      字样都不算；
//   2. 参数列表里必须有一个**恰好等于** "--user-data-dir=<abs>" 的独立参数：
//      逐字段精确比对（必要时统一分隔符、去尾部斜杠、EqualFold），绝不子串
//      匹配——"/tmp/p" 不能被 "/tmp/p-other" 骗过，"C:\data\p" 不能被
//      "C:\data\p-other" 骗过。

import (
	"path/filepath"
	"strings"
)

// browserExeNames 是认可的可执行名白名单（全小写基名子串匹配）。
// chromium / chrome / "Google Chrome" / "Chromium" / chrome.exe 全覆盖；
// msedge / microsoft-edge / "Microsoft Edge" / brave 同理。Firefox 不在。
var browserExeNames = []string{
	"chrom", // chromium / chrome / "Google Chrome" / "Chromium" / chrome.exe
	"msedge",
	"microsoft-edge",
	"microsoft edge",
	"brave",
}

// exeNameOK 校验一个「可能是可执行路径」的词：取基名小写后命中白名单。
// "Google" 这类非路径词取基名后不匹配，天然被滤掉。
func exeNameOK(word string) bool {
	base := strings.ToLower(filepath.Base(strings.TrimSpace(word)))
	for _, ok := range browserExeNames {
		if strings.Contains(base, ok) {
			return true
		}
	}
	return false
}

// normProfilePath 归一化 user-data-dir 的值用于精确比较：去首尾空白、
// 去尾部斜杠（"C:\p\" 与 "C:\p" 是同一个目录）。大小写是否敏感由调用方
// 的相等函数决定（Windows EqualFold / Unix 精确）。
func normProfilePath(p string) string {
	p = strings.TrimSpace(p)
	for len(p) > 1 && (strings.HasSuffix(p, "/") || strings.HasSuffix(p, `\`)) {
		p = p[:len(p)-1]
	}
	return p
}

// userDataDirValue 从一个参数 token 里提取 --user-data-dir 的值。
// 认两种形态："--user-data-dir=<v>" 与 "--user-data-dir <v>"（后者由
// 调用方在拆词后把下一个 token 传成 next）。
// 返回值 (值, 是否命中)。
func userDataDirValue(arg, next string) (string, bool) {
	if strings.HasPrefix(arg, "--") {
		arg = "--" + arg[2:] // 归一：调用方可能传去前缀片段
	}
	const k = "--user-data-dir="
	if strings.HasPrefix(arg, k) {
		return unquote(arg[len(k):]), true
	}
	if arg == "--user-data-dir" && next != "" {
		return unquote(next), true
	}
	return "", false
}

// unquote 去一层成对双引号或单引号（Windows 命令行里带空格路径必须带引号）。
func unquote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// linuxCmdlineIsOurBrowser 是 Linux /proc/<pid>/cmdline 的纯判据：
// rawNUL 是 NUL 分隔的原始字节（与 os.ReadFile("/proc/PID/cmdline") 一致）。
// argv[0] 命中白名单 + 某一项恰好等于 "--user-data-dir=<abs>"（路径逐字节
// 相等，Linux 文件系统大小写敏感）。
func linuxCmdlineIsOurBrowser(rawNUL []byte, absProfileDir string) bool {
	parts := strings.Split(string(rawNUL), "\x00")
	if len(parts) == 0 || parts[0] == "" {
		return false
	}
	if !exeNameOK(parts[0]) {
		return false
	}
	want := normProfilePath(absProfileDir)
	for i, a := range parts[1:] {
		var next string
		if i+1 < len(parts)-1 {
			next = parts[i+2]
		}
		if v, ok := userDataDirValue(a, next); ok && normProfilePath(v) == want {
			return true
		}
	}
	return false
}

// darwinCmdlineIsOurBrowser 是 Darwin `ps -ww -o command=` 输出的纯判据：
//   - "--user-data-dir=" 首次出现之前的整段是可执行路径段（"Google Chrome"
//     含空格，不能按词切——profile 路径里的 "chrome" 词不能冒充可执行名）；
//   - 参数段按 " --" 边界切出每个选项，找恰好等于目标的 --user-data-dir=<abs>
//     ——profile 路径可含空格，按 " --" 而不是空格切词才不会把值切碎。
//   - 也认 "--user-data-dir <abs>"（少见但合法的空格分隔形态）。
func darwinCmdlineIsOurBrowser(line, absProfileDir string) bool {
	idx := strings.Index(line, "--user-data-dir")
	if idx <= 0 {
		return false
	}
	exeSeg := strings.TrimSpace(line[:idx])
	if !exeNameOK(exeSeg) {
		return false
	}
	want := normProfilePath(absProfileDir)
	args := line[idx:]
	prev := ""
	for _, a := range strings.Split(args, " --") {
		a = strings.TrimSpace(a)
		if !strings.HasPrefix(a, "--") {
			a = "--" + a
		}
		if v, ok := userDataDirValue(a, ""); ok && normProfilePath(v) == want {
			return true
		}
		if prev == "--user-data-dir" && normProfilePath(unquote(a)) == want {
			return true
		}
		prev = a
	}
	return false
}

// windowsSplitCmdline 按 Windows CreateProcess 命令行规则拆词：
// 双引号内空格不分割，成对引号去掉，反斜杠在引号前的转义按 \" 处理
// （我们的参数不含复杂转义，处理到够用为止）。
func windowsSplitCmdline(cmdline string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	has := false
	for i := 0; i < len(cmdline); i++ {
		c := cmdline[i]
		switch {
		case c == '"':
			inQuote = !inQuote
			has = true
		case c == ' ' || c == '\t':
			if inQuote {
				cur.WriteByte(c)
			} else if has || cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
				has = false
			}
		default:
			cur.WriteByte(c)
		}
	}
	if has || cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// windowsCmdlineIsOurBrowser 是 Windows Win32_Process.CommandLine 的纯判据：
//   - argv[0] 基名（去引号、统一 \ 分隔符后取末段）命中白名单；
//   - --user-data-dir 参数值归一化（去引号、统一分隔符、去尾部斜杠）后与
//     absProfileDir 大小写不敏感精确相等（strings.EqualFold）——
//     "C:\data\p" 绝不会误匹配 "C:\data\p-other"。
func windowsCmdlineIsOurBrowser(cmdline, absProfileDir string) bool {
	args := windowsSplitCmdline(cmdline)
	if len(args) == 0 || args[0] == "" {
		return false
	}
	// argv[0] 可能是带引号的完整路径：统一分隔符取基名。
	exe := strings.ReplaceAll(args[0], "/", `\`)
	if i := strings.LastIndex(exe, `\`); i >= 0 {
		exe = exe[i+1:]
	}
	if !exeNameOK(exe) {
		return false
	}
	want := strings.ReplaceAll(normProfilePath(absProfileDir), "/", `\`)
	for i, a := range args[1:] {
		var next string
		if i+1 < len(args)-1 {
			next = args[i+2]
		}
		if v, ok := userDataDirValue(a, next); ok {
			got := strings.ReplaceAll(normProfilePath(v), "/", `\`)
			if strings.EqualFold(got, want) {
				return true
			}
		}
	}
	return false
}
