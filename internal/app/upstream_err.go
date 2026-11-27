package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// ─── 502 结构化诊断 ─────────────────────────────────────────────────────────
//
// 线上症状：/v1/models 能列出模型，调用却全 502，客户端只拿到一句
// "upstream error: upstream returned no content frame (raw 216 bytes)"，
// 看不出是无权限、登录失效还是上游抖动 —— 工单里只剩「502」三个字。
//
// 这里把失败归一成一份结构化诊断：reason 是机读的原因码，upstream_model /
// frames 带现场数据，hint 是一句能直接贴给用户的人话。同一套字段贯穿所有
// 502 出口（chat/responses JSON、Google 原生形状、已开流后的 SSE 错误块、
// requests 表的 error 前缀），查任何一处都能对上号。

// reason 码（机读、落库前缀、面板归类共用）。
const (
	reasonNoContentFrame  = "no_content_frame" // 上游 200 但一个内容帧都没有（瞬时拒绝/无权限/登录失效）
	reasonFailureReply    = "failure_reply"    // 上游回了固定出错话术（inner[31]）
	reasonUpstreamTimeout = "upstream_timeout" // 等满 request_timeout_sec / StallError
	reasonEmptyContent    = "empty_content"    // 有内容帧但抽取后正文为空（整条回复只有无法解析的 tool_call 围栏等）
	reasonUpstreamHTTP    = "upstream_http"    // 上游回了非 200
	reasonUpstreamError   = "upstream_error"   // 其余上游失败兜底
	// browser_transport 开着但本机找不到/拉不起浏览器时的硬错：绝不静默降级
	// 回纯 Go HTTP（browser_fallback_http 显式开才回退），见 browser.go。
	reasonBrowserUnavailable = "browser_unavailable"
	// accounts_cooling_down：所有可挑登录账号都在 45s 防雪崩短冷却——
	// 不是上游故障也不是限流满，是「等几十秒再来」的软 429。
	reasonAccountsCoolingDown = "accounts_cooling_down"
)

// upstreamDiag 是 502 body 和 requests.error 前缀共用的一份诊断。
type upstreamDiag struct {
	Reason        string `json:"reason"`
	UpstreamModel string `json:"upstream_model"` // 上游自报显示名；解析不到给 "unknown"
	Frames        int    `json:"frames"`
	ContentFrame  bool   `json:"content_frame"`
	Hint          string `json:"hint"`
}

// upstreamFrameStats 从完整 raw 里真实解析 wrb.fr 帧统计：
// 一条行可能装多个数组条目（[["wrb.fr",…],["wrb.fr",…]]，抓包里就这么排），
// 按「含 wrb.fr 的行数」计会少算；逐条目解出来后，内容帧判据原样复用
// textsInLine / reasoningInLine（把条目重新包成单行喂给它们，不重复索引逻辑），
// 自报模型名走 extractUpstreamModel 的同一套正则（该正则认转义形态，必须喂帧串）。
// 只在失败路径即时调用：结果是一份结构化计数，不保留 raw 原文（原文含内部 token）。
func upstreamFrameStats(raw string) (frames int, content bool, model string) {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "[") || !strings.Contains(line, `"wrb.fr"`) {
			continue
		}
		var arr []interface{}
		if json.Unmarshal([]byte(line), &arr) != nil {
			continue
		}
		for _, it := range arr {
			row, ok := it.([]interface{})
			if !ok || len(row) < 3 || row[0] != "wrb.fr" {
				continue
			}
			frames++
			payload, ok := row[2].(string)
			if !ok || payload == "" {
				continue
			}
			frame := `[["wrb.fr","rpc",` + strconv.Quote(payload) + `,null]]`
			if !content && (len(textsInLine(frame)) > 0 || reasoningInLine(frame) != "") {
				content = true
			}
			if m := extractUpstreamModel(frame); m != "" {
				model = m // 取到最后一帧的自报名（跟 extractUpstreamModel 取最后一个一致）
			}
		}
	}
	return frames, content, model
}

// diagHint 按 reason 码给人话提示。err 可空：errStreamIdle 虽归
// upstream_timeout，但窗口是 stream_idle_timeout_sec（开流后块间沉默），
// hint 要指向它，不是总窗口 request_timeout_sec。
func diagHint(reason string, err error) string {
	if reason == reasonUpstreamTimeout && err != nil && errors.Is(err, errStreamIdle) {
		return "开流后块间沉默超过 stream_idle_timeout_sec：可开大该值或检查代理出口（上游排队/首帧未到的长沉默才归 request_timeout_sec）"
	}
	var ppe *poisonPayloadError
	if errors.As(err, &ppe) {
		return "主备两个账号都拒绝了这条载荷（毒载荷判定）：已豁免双端账号冷却与代理扣分，" +
			"通常是提示词/附件触发上游内容拒绝——请调整请求内容"
	}
	switch reason {
	case reasonNoContentFrame:
		return "上游没有返回正文内容：可能是该账号无此模型权限、登录已过期，或模型名/协议已变更"
	case reasonFailureReply:
		return "上游返回了固定的出错话术而不是回答：多为该账号被风控或临时故障，重试通常就好"
	case reasonUpstreamTimeout:
		return "上游在限定时长内没有回完：可能是排队过久或网络链路问题，可开大 request_timeout_sec 或换出口"
	case reasonEmptyContent:
		return "上游返回了内容帧但抽不出正文：可能是回复被清洗规则剥空或协议字段已变更"
	case reasonUpstreamHTTP:
		// 鉴权类跨源拦截（accounts.google.com / 缺 OSID）直接向客户端透出
		// 具体的登录态诊断指引；/sorry、429 等出口侧问题保持原风控提示。
		if err != nil && xsrfErrBlamesCookie(err) {
			return explainCookieFailure(err)
		}
		return "上游返回了非 200 状态码：可能是出口被风控（302/sorry）、限流（429）或上游临时故障"
	case reasonAccountsCoolingDown:
		return "所有登录账号刚连续被上游拒绝，正处 45s 防雪崩短冷却：按 Retry-After 稍等再试，" +
			"或检查这批账号 cookie 是否过期/被风控"
	case reasonBrowserUnavailable:
		return "常驻浏览器不可用：browser_transport 已开启但本机找不到 Chromium/Chrome/Edge，" +
			"或拉起/连接失败。请安装 chromium（如 apt install chromium），" +
			"或在 config.json / 启动参数里配置 browser_path 指向已装浏览器；" +
			"确需在浏览器缺席时继续跑，可显式打开 browser_fallback_http 回退纯 Go HTTP"
	default:
		return "上游调用失败：详见 message 字段"
	}
}

// upstreamReason 把一次调用失败归到上面的 reason 码。
//
// 先认按类型/哨兵错误能钉死的几种（超时、出错话术、空抽取），再认
// 「no content frame」这个错误串特征（单轮/多轮两条链、HTTP 层和解析层都产出
// 同一句），剩下按错误串里的上游状态码和兜底归。
func upstreamReason(err error) string {
	var se *StallError
	var ace *AccountCooldownError
	var ppe *poisonPayloadError
	var ape *appPageStatusError
	switch {
	case errors.As(err, &ace):
		return reasonAccountsCoolingDown
	case errors.As(err, &ppe):
		// 毒载荷保留内层 reason（no_content_frame 等）：外层只改了「免记账」语义。
		return upstreamReason(ppe.err)
	case isBrowserLocalError(err):
		return reasonBrowserUnavailable
	case errors.Is(err, errUpstreamFailureReply):
		return reasonFailureReply
	case errors.Is(err, errEmptyContent):
		return reasonEmptyContent
	case errors.As(err, &se), errors.Is(err, errStreamIdle), isUpstreamTimeout(err):
		// errStreamIdle 也是「等满配置窗口被掐断」的超时类，归 upstream_timeout
		//（hint 同样适用：可开大窗口或换出口），不落泛化 upstream_error。
		return reasonUpstreamTimeout
	case strings.Contains(err.Error(), "no content frame"):
		return reasonNoContentFrame
	case errors.As(err, &ape), strings.Contains(err.Error(), "upstream HTTP"):
		// *appPageStatusError（"fetch /app: HTTP 302 -> accounts.google.com/..."）
		// 错误串不含 "upstream HTTP"——按类型识别归 upstream_http，不落入兜底。
		return reasonUpstreamHTTP
	default:
		return reasonUpstreamError
	}
}

// errEmptyContent 标记「有内容帧但抽取后正文为空」的失败（reason=empty_content）。
// 跟「no content frame」分开：后者是上游根本没回内容，前者是回了但没法用，
// 排查方向完全不同。
var errEmptyContent = errors.New("upstream returned a content frame but extracted text is empty")

// upstreamDiagMeta 是失败路径上游响应的**结构化现场**：帧数 / 有没有内容帧 /
// 自报模型名。它由 gemini.go / conversation.go 在每次拿到完整 raw 时即时解算
// （upstreamFrameStats），失败 attrib 带出，替代「截断的 raw」——raw 原文含内部
// token，不该往 502 body 或 requests 表里放，这里只留数出来的字段。每轮重试
// 都先重置，防上一轮的部分帧污染最后一轮。
type upstreamDiagMeta struct {
	Frames       int
	ContentFrame bool
	Model        string
}

// diagErr 是带显式 reason + 现场 res 的错误：callGemini / callGeminiConv 里已经
// 算好诊断了，走到 HTTP 层时不必再从错误串里猜一遍。errors.Is/As 照常穿透到
// 被包的原始错误（重试判定、clientGone 等不受影响）。
type diagErr struct {
	err    error
	reason string
	res    *StreamResult
}

func (e *diagErr) Error() string { return e.err.Error() }
func (e *diagErr) Unwrap() error { return e.err }

// errDiagReason 取错误自带 reason，取不到再按错误内容归类。
func errDiagReason(err error) string {
	var de *diagErr
	if errors.As(err, &de) && de.reason != "" {
		return de.reason
	}
	return upstreamReason(err)
}

// upstreamErrorJSON 是 502 错误体的 error 字段。message 保留原来的
// "upstream error: <err>" 前缀（按这个串匹配的客户端不至于断），诊断字段全部机读。
//
// 帧信息来自失败路径即时解算的 res.Diag（完整 raw 就地解析成计数后丢弃，不在
// 失败结果里留存原文）。callGemini / callGeminiConv 空正文守卫走 diagErr 时，res
// 是成功形状、Diag 未填但完整 Raw 还在，这里就地对 Raw 解算同样一份统计——
// 只在内存里解析、不回传原文。都取不到时 frames=0、content_frame=false，
// upstream_model 显式 "unknown"，不瞎猜。
func upstreamErrorJSON(err error, res *StreamResult) map[string]interface{} {
	d := upstreamDiag{Reason: errDiagReason(err), UpstreamModel: "unknown"}
	if res != nil {
		d.Frames = res.Diag.Frames
		d.ContentFrame = res.Diag.ContentFrame
		d.UpstreamModel = res.UpstreamModel
		if d.Frames == 0 && res.Raw != "" {
			// 成功路径形状的 res：Diag 没填，完整 Raw 还在，就地补算。
			var m string
			d.Frames, d.ContentFrame, m = upstreamFrameStats(res.Raw)
			if d.UpstreamModel == "" {
				d.UpstreamModel = m
			}
		}
		if d.UpstreamModel == "" {
			d.UpstreamModel = res.Diag.Model
		}
		if d.UpstreamModel == "" {
			d.UpstreamModel = "unknown"
		}
	}
	d.Hint = diagHint(d.Reason, err)
	var ace *AccountCooldownError
	if errors.As(err, &ace) {
		// 短冷却按标准限流形状写：sse.Fail 与 response.failed 会带上
		// rate_limit_exceeded + accounts_cooling_down，客户端按「稍后重试」处理。
		return map[string]interface{}{
			"message":        "upstream error: " + err.Error(),
			"type":           "rate_limit_exceeded",
			"code":           reasonAccountsCoolingDown,
			"reason":         d.Reason,
			"upstream_model": d.UpstreamModel,
			"frames":         d.Frames,
			"content_frame":  d.ContentFrame,
			"hint":           d.Hint,
		}
	}
	return map[string]interface{}{
		"message":        "upstream error: " + err.Error(),
		"type":           "upstream_error",
		"reason":         d.Reason,
		"upstream_model": d.UpstreamModel,
		"frames":         d.Frames,
		"content_frame":  d.ContentFrame,
		"hint":           d.Hint,
	}
}

// writeUpstreamError 写结构化 502（OpenAI 两条路的未开流分支用）。
func writeUpstreamError(w http.ResponseWriter, err error, res *StreamResult) {
	writeJSON(w, 502, map[string]interface{}{"error": upstreamErrorJSON(err, res)})
}

// geminiErrorJSON 是 Google 原生形状的 502 错误体：code/message/status 之外挂
// details 诊断块（Google SDK 会原样透传 details 数组）。
// *AccountCooldownError（已开流由 geminiStream.fail 走到这里）映射 429 /
// RESOURCE_EXHAUSTED——跟未开流的 writeGeminiCallError 同一语义。
func geminiErrorJSON(err error, res *StreamResult) map[string]interface{} {
	code := 502
	var ace *AccountCooldownError
	if errors.As(err, &ace) {
		code = 429
	}
	e := upstreamErrorJSON(err, res)
	return map[string]interface{}{"error": map[string]interface{}{
		"code":    code,
		"message": e["message"],
		"status":  geminiStatus(code),
		"details": []interface{}{map[string]interface{}{
			"reason":         e["reason"],
			"upstream_model": e["upstream_model"],
			"frames":         e["frames"],
			"content_frame":  e["content_frame"],
			"hint":           e["hint"],
		}},
	}}
}

// recordUpstreamFailure 是失败请求落库的公共步：error 列写 [reason] 前缀 +
// 原始错误串，upstream_model 列由 recordRequest 从 res 取（失败路径上 res.Diag
// 里若解出了自报名，先回填 res.UpstreamModel，不碰 raw 原文）。
//
// requests 表是排查用户现场唯一的线索，所以落库**不受 log_requests 开关控制**
// （开关只管 stderr 日志，见 logf）；[reason] 前缀让面板按类别过滤可检索。
func recordUpstreamFailure(endpoint, model, prompt string, res *StreamResult, status int, err error, stream bool) {
	// upstream_model 列优先 res.UpstreamModel（attrib 已从 Diag 回填）；还没有再从
	// Diag.Model 补——两者都只是上游自报显示名，不含敏感原文。
	if res != nil && res.UpstreamModel == "" && res.Diag.Model != "" {
		res.UpstreamModel = res.Diag.Model
	}
	recordRequest(endpoint, model, prompt, "", res, status,
		fmt.Sprintf("[%s] %s", errDiagReason(err), err.Error()), stream)
}
