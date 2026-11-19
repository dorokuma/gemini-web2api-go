package app

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

type argonIdentityError struct {
	name string
	id   string
}

func (e *argonIdentityError) Error() string {
	return fmt.Sprintf("Argon model mismatch: upstream reported %q (%s)", e.name, e.id)
}

// argonDisplayNames 按请求的槽位（hex）和思考档位返回上游合法的自报显示名。
//
// Carbon 槽位（hexArgon4 / Mode=8，原 Argon 原位改名）：普通档 "4 Argon"/"4 Carbon"，
// 高档 "4 Argon High"/"4 Carbon High"（2026-11-13 起 High 档自报又脱了后缀、回落成
// 裸 "4 Carbon"，滚动期两套都收），中档裸名或 "* Medium"。
// Neon 槽位（hexNeon4 / Mode=3）：think=1 只认 "4 Neon"，think=2 只认 "4 Neon High"，
// think=4 只认 "4 Neon Medium"——Neon 的高/中档自报**带后缀**（与 Carbon High 档脱后缀
// 不同），且各档名字不互通：档位被静默换掉同样算身份不符。
// 两种槽位都附带 "Gemini " 前缀形态；按请求的档位只认对应名字。
func argonDisplayNames(hexID string, think int) []string {
	return argonDisplayNamesMode(hexID, think, false)
}

// argonDisplayNamesMode 同 argonDisplayNames，taskMode=true 时对 hexNeon4 放宽一档：
// 任务模式（gemini-4-neon-task）发 hexNeon4 + think=2 + inner[49]=40，上游实测自报
// 裸名 "4 Neon"（而非对话模式 think=2 的 "4 Neon High"）。裸名与未来可能恢复的
// "4 Neon High" 后缀都收，其它模型/档位仍严格拦截。
// taskMode=false 时逐字保留原规则——对话模式校验绝不放宽。
func argonDisplayNamesMode(hexID string, think int, taskMode bool) []string {
	if hexID == hexNeon4 {
		if taskMode {
			return []string{"4 Neon", "Gemini 4 Neon", "4 Neon High", "Gemini 4 Neon High"}
		}
		switch think {
		case thinkingExtended:
			return []string{"4 Neon High", "Gemini 4 Neon High"}
		case thinkingMedium:
			return []string{"4 Neon Medium", "Gemini 4 Neon Medium"}
		default:
			return []string{"4 Neon", "Gemini 4 Neon"}
		}
	}
	if think == thinkingExtended {
		// 2026-11-13 起上游对 High 档的自报又脱了 "High" 后缀、回落成裸 "4 Carbon"
		// （与 11 月把 "4 Argon"→"4 Carbon" 同一轮滚动改名）；滚动期两套都收。
		return []string{"4 Argon High", "Gemini 4 Argon High", "4 Carbon High", "Gemini 4 Carbon High",
			"4 Carbon", "Gemini 4 Carbon"}
	}
	if think == thinkingMedium {
		return []string{"4 Argon", "Gemini 4 Argon", "4 Argon Medium", "Gemini 4 Argon Medium",
			"4 Carbon", "Gemini 4 Carbon", "4 Carbon Medium", "Gemini 4 Carbon Medium"}
	}
	return []string{"4 Argon", "Gemini 4 Argon", "4 Carbon", "Gemini 4 Carbon"}
}

// argonThinkFromHeader 从模型 header 下标 15 取思考档位；最小形式没有这一槽，按普通档。
func argonThinkFromHeader(h []any) int {
	if len(h) > 15 {
		if f, ok := h[15].(float64); ok {
			return int(f)
		}
	}
	return thinkingNormal
}

// argonIdentityInLine 检查单行 wrb.fr 帧里的模型身份。
// 返回 found=true 表示这行带有 names 之一的身份；err 非 nil 表示这行报的是别的模型或档位。
// 注意：2026-10 起上游在响应帧的 hex 槽位可能固定回 cf41b0e0dd7d53e5（hexFlashLite 占位），
// 只有紧随其后的显示名槽位反映真实模型，因此 hex 槽位接受请求的 hexID 或 hexFlashLite 占位，
// 同时严格核对显示名。
func argonIdentityInLine(line string, hexID string, names []string) (found bool, err error) {
	var rows [][]json.RawMessage
	if json.Unmarshal([]byte(line), &rows) != nil {
		return false, nil
	}
	for _, row := range rows {
		if len(row) < 3 {
			continue
		}
		var kind, payload string
		if json.Unmarshal(row[0], &kind) != nil || kind != "wrb.fr" || json.Unmarshal(row[2], &payload) != nil {
			continue
		}
		var fields []any
		if json.Unmarshal([]byte(payload), &fields) != nil {
			continue
		}
		// Upstream reports model identity as consecutive top-level slots hex,null,null,name.
		for i := 0; i+3 < len(fields); i++ {
			id, ok := fields[i].(string)
			if !ok || len(id) != 16 || fields[i+1] != nil || fields[i+2] != nil {
				continue
			}
			validHex := true
			for _, c := range id {
				if !strings.ContainsRune("0123456789abcdef", c) {
					validHex = false
					break
				}
			}
			name, ok := fields[i+3].(string)
			if !validHex || !ok {
				continue
			}
			if (id != hexID && id != hexFlashLite) || !slices.Contains(names, name) {
				return false, &argonIdentityError{name: name, id: id}
			}
			found = true
		}
	}
	return found, nil
}

var errArgonIdentityMissing = fmt.Errorf("Argon identity missing in upstream response; refusing unverified model")

func validateArgonIdentity(raw []byte, hexID string, names []string) error {
	found := false
	for _, line := range strings.Split(string(raw), "\n") {
		ok, err := argonIdentityInLine(line, hexID, names)
		if err != nil {
			return err
		}
		found = found || ok
	}
	if !found {
		return errArgonIdentityMissing
	}
	return nil
}

// readModelCheckedBody 对身份校验模型（Carbon/Argon、Neon 等付费专属槽位）的请求
// 逐帧校验模型身份后再转发。请求的 hex 不在校验集合里时与原来一样直通。
//
// 身份确认之前的行先暂存、不回调；第一次在帧里确认是 Argon 时把暂存的行一次放行，
// 之后边读边转发，同时继续检查每一帧。上游对未授权账号会静默降级成别的模型，
// 所以：
//   - 身份始终没出现、或首次出现就不对：一个字都不放行，返回错误；
//   - 放行之后又报了别的模型：立刻停读并返回错误（调用方据此发流错误收尾）。
//     已经发出去的内容收不回来，这是增量转发换首字延迟必须接受的代价。
//
// 实测身份跟首段正文在同一帧（Argon 探针：首帧 1.9s、身份与首字 9.5s、结束 14.6s），
// 所以放行时机就是首字时机，不再要等上游整段生成完。
//
// onLine 为 nil（非流式）时没有转发，照旧读完整段再校验。
func readModelCheckedBody(r io.Reader, onLine func(string) error, start time.Time, status int, modelHeader string) ([]byte, int64, error) {
	return readModelCheckedBodyMode(r, onLine, start, status, modelHeader, false)
}

// readModelCheckedBodyMode 同 readModelCheckedBody，另带 taskMode：gemini-4-neon-task
// （hexNeon4 + 任务模式）上游自报裸名 "4 Neon"，合法名集合见 argonDisplayNamesMode。
func readModelCheckedBodyMode(r io.Reader, onLine func(string) error, start time.Time, status int, modelHeader string, taskMode bool) ([]byte, int64, error) {
	var h []any
	hexID := ""
	if json.Unmarshal([]byte(modelHeader), &h) == nil && len(h) > 4 {
		hexID, _ = h[4].(string)
	}
	if hexID != hexArgon4 && hexID != hexNeon4 {
		return readBody(r, onLine, start)
	}
	names := argonDisplayNamesMode(hexID, argonThinkFromHeader(h), taskMode)
	if status != 200 || onLine == nil {
		raw, ttfb, err := readBody(r, nil, start)
		if err != nil || status != 200 {
			return raw, ttfb, err
		}
		return raw, ttfb, validateArgonIdentity(raw, hexID, names)
	}

	var pending []string
	verified := false
	raw, ttfb, err := readBody(r, func(line string) error {
		ok, lerr := argonIdentityInLine(line, hexID, names)
		if lerr != nil {
			return lerr
		}
		if !verified {
			if !ok {
				if line != "" {
					pending = append(pending, line)
				}
				return nil
			}
			verified = true
			for _, p := range pending {
				if cerr := onLine(p); cerr != nil {
					return cerr
				}
			}
			pending = nil
		}
		if line != "" {
			if cerr := onLine(line); cerr != nil {
				return cerr
			}
		}
		return nil
	}, start)
	if err != nil {
		return raw, ttfb, err
	}
	if !verified {
		return raw, ttfb, errArgonIdentityMissing
	}
	return raw, ttfb, nil
}
