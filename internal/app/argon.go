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

// argonDisplayNames 是上游对 Argon 各思考档位自报的显示名：普通档 "4 Argon"，
// 扩展思考（think=2，上游叫 High）"4 Argon High"，中档（think=4）自报 "4 Argon" 或 "4 Argon Medium"。
// 2026-11 起上游把同一槽位的显示名原位改名成 "4 Carbon" 家族（hex 95a4c0abcba24364、Mode=8 不变），
// 改名滚动期两套名字都算同一模型的合法自报，按档位一起收。
// 按请求的档位只认对应名字，档位被静默换掉也算身份不符。
func argonDisplayNames(think int) []string {
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
// 返回 found=true 表示这行带有 names 之一的 Argon 身份；err 非 nil 表示这行报的是别的模型或档位。
// 注意：2026-10 起上游在响应帧的 hex 槽位可能固定回 cf41b0e0dd7d53e5（hexFlashLite 占位），
// 只有紧随其后的显示名槽位反映真实模型，因此 hex 槽位接受 hexArgon4 或 hexFlashLite 占位，同时严格核对显示名。
func argonIdentityInLine(line string, names []string) (found bool, err error) {
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
			if (id != hexArgon4 && id != hexFlashLite) || !slices.Contains(names, name) {
				return false, &argonIdentityError{name: name, id: id}
			}
			found = true
		}
	}
	return found, nil
}

var errArgonIdentityMissing = fmt.Errorf("Argon identity missing in upstream response; refusing unverified model")

func validateArgonIdentity(raw []byte, names []string) error {
	found := false
	for _, line := range strings.Split(string(raw), "\n") {
		ok, err := argonIdentityInLine(line, names)
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

// readModelCheckedBody 对 Argon 请求逐帧校验模型身份后再转发。
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
	var h []any
	isArgon := json.Unmarshal([]byte(modelHeader), &h) == nil && len(h) > 4 && h[4] == hexArgon4
	if !isArgon {
		return readBody(r, onLine, start)
	}
	names := argonDisplayNames(argonThinkFromHeader(h))
	if status != 200 || onLine == nil {
		raw, ttfb, err := readBody(r, nil, start)
		if err != nil || status != 200 {
			return raw, ttfb, err
		}
		return raw, ttfb, validateArgonIdentity(raw, names)
	}

	var pending []string
	verified := false
	raw, ttfb, err := readBody(r, func(line string) error {
		ok, lerr := argonIdentityInLine(line, names)
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
