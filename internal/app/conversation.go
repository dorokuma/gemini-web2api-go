package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/google/uuid"
)

// convState 是一路 Gemini 原生会话的状态。整条会话绑定同一个 cookie / 出口，中途不换
// —— 换了 Gemini 就当成另一个会话，续接的 cid 失效。
type convState struct {
	cid, rid, rcid, tok26 string
	turn                  int    // 会话内轮次索引，填 inner[17]，逐轮 +1
	cookie                string // 会话绑定 cookie：登录号的 cookie，或匿名 /app 拿的 session
	sapisid               string // 登录态算 SAPISIDHASH 用；匿名为空
	isLogin               bool
	accountID             int64
	authuser              int // 登录号的账号序号（/u/N）；整条会话不变，匿名为 0
	proxyID               int64
	proxyURL              string
	updated               time.Time
	// fstate 是本请求跨号跳槽的统一准入状态（v6.6.0 防雪崩）：由
	// callGeminiConv 顶层建立、经 runConvWithFailover 挂到 conv 上透传——
	// runConversationTurn 签名保持原样，streamGenerateConv 在读 XSRF /
	// 发 StreamGenerate 前 conv.fstate.markTouched 登记。nil = 存量测试
	// 直接调 runConversationTurn，不参入准入（向后兼容）。
	fstate *reqFailoverState
}

const (
	// convTTL 跟导出 cookie ~2 小时寿命对齐，过期的会话状态没有复用价值。
	convTTL = 90 * time.Minute
	convMax = 4000
)

var (
	convMu    sync.Mutex
	convStore = map[string]*convState{}
)

// convTake atomically consumes the longest matching proper prefix. A state is
// never left under an old fingerprint after Google advances it: concurrent
// duplicates and forks start fresh instead of mutating the same upstream turn.
func convTake(keys []string) (*convState, int) {
	convMu.Lock()
	defer convMu.Unlock()
	for i := len(keys) - 2; i >= 0; i-- {
		st, ok := convStore[keys[i]]
		if !ok {
			continue
		}
		delete(convStore, keys[i])
		if time.Since(st.updated) > convTTL {
			continue
		}
		copy := *st
		return &copy, i + 1
	}
	return nil, 0
}

// convPutIfAbsent 只在 key 不存在时存一份副本（绝不覆盖已有 head）。
// 续接请求在发往上游之前就本地失败（Bound 429 / ctx 取消）时归还被 convTake
// 消费的健康 head 用：上游没被碰过，那轮没发生，head 仍然有效。
// 「不覆盖」是硬要求：归还失败期间另一路同前缀请求可能已经建了新会话落了新 head，
// 拿旧快照盖回去等于把刚建好的会话作废。
func convPutIfAbsent(key string, st *convState) {
	if key == "" || st == nil {
		return
	}
	convMu.Lock()
	defer convMu.Unlock()
	if _, ok := convStore[key]; ok {
		return
	}
	snapshot := *st
	convStore[key] = &snapshot
}

// convDelete 删一路会话状态（仅测试清理用）：生产路径已换成 convTake
// 原子消费——上游可能已推进，失败的续接不能把旧 head 放回去。
func convDelete(key string) {
	if key == "" {
		return
	}
	convMu.Lock()
	delete(convStore, key)
	convMu.Unlock()
}

// convPut 存一路会话，超量就按最旧的淘汰一批。
func convPut(key string, st *convState) {
	if key == "" {
		return
	}
	convMu.Lock()
	defer convMu.Unlock()
	snapshot := *st
	snapshot.updated = time.Now()
	convStore[key] = &snapshot
	if len(convStore) > convMax {
		// 简单淘汰：删掉过期的；还超就删最旧的一小批。
		var oldestK string
		var oldestT time.Time
		first := true
		for k, v := range convStore {
			if time.Since(v.updated) > convTTL {
				delete(convStore, k)
				continue
			}
			if first || v.updated.Before(oldestT) {
				oldestK, oldestT, first = k, v.updated, false
			}
		}
		if len(convStore) > convMax && oldestK != "" {
			delete(convStore, oldestK)
		}
	}
}

// canonicalMessages retains the semantic message structure, including tools.
// JSON escaping makes embedded control characters unambiguous.
func canonicalMessages(messages []map[string]interface{}) string {
	b, _ := json.Marshal(normalizeConversationMessages(messages))
	return string(b)
}

func hashStr(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

var rcidRe = regexp.MustCompile(`"(rc_[A-Za-z0-9_-]{6,})"`)

// parseConvIDs 从 StreamGenerate 响应里取续接要用的四样：cid=[1][0]、rid=[1][1]、
// tok26=帧[26]、rcid=正文里的 rc_xxx（登录态续接第 3 位要用它，匿名为空）。
func parseConvIDs(raw string) (cid, rid, rcid, tok26 string) {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "[[") {
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
			payload, ok := row[2].(string)
			if !ok {
				continue
			}
			var f []interface{}
			if json.Unmarshal([]byte(payload), &f) != nil {
				continue
			}
			if len(f) > 1 {
				if meta, ok := f[1].([]interface{}); ok && len(meta) >= 2 {
					if s, ok := meta[0].(string); ok && s != "" {
						cid = s
					}
					if s, ok := meta[1].(string); ok && s != "" {
						rid = s
					}
				}
			}
			if len(f) > 26 {
				if s, ok := f[26].(string); ok && s != "" {
					tok26 = s
				}
			}
		}
	}
	if m := rcidRe.FindStringSubmatch(raw); m != nil {
		rcid = m[1]
	}
	return
}

// getAnonSession 匿名 GET /app 拿一份 session cookie（NID/COMPASS 等），给匿名多轮当会话
// 载体。续接请求必须每轮带上它，否则被服务端当新会话（实测不带 0/4 通、带 4/4 通）。
// 必须走跟正式请求同一出口。
func getAnonSession(proxyURL string) (string, error) {
	// 浏览器传输模式下匿名会话**仍走纯 Go HTTP**：匿名多轮的会话载体是
	// /app 下发的一份 session cookie（NID/COMPASS 等），必须逐轮带上；
	// 常驻浏览器的页内 fetch 对匿名请求是 credentials:"omit"（JS fetch
	// 也禁止手工设置 Cookie 头），会话 cookie 根本发不出去——让匿名
	// 流量借浏览器 jar，不但丢续接，还会误带驻留主号的登录态。
	// 所以匿名会话始终持自己的 session cookie 走原有纯 Go HTTP 路径，
	// 仅登录态流量走常驻浏览器。
	req := func() (*http.Response, io.ReadCloser, []string, error) {
		if proxyURL != "" {
			r, err := http.NewRequest("GET", "https://gemini.google.com/app", nil)
			if err != nil {
				return nil, nil, nil, err
			}
			applyChromeHeaders(r)
			for k, v := range appPageHeaders() {
				r.Header.Set(k, v)
			}
			resp, err := getStdlibClient(proxyURL).Do(r)
			if err != nil {
				return nil, nil, nil, err
			}
			return resp, resp.Body, resp.Header.Values("Set-Cookie"), nil
		}
		r, err := fhttp.NewRequest("GET", "https://gemini.google.com/app", nil)
		if err != nil {
			return nil, nil, nil, err
		}
		applyBrowserFHeaders(r)
		for k, v := range appPageHeaders() {
			r.Header.Set(k, v)
		}
		resp, err := getTLSClient().Do(r)
		if err != nil {
			return nil, nil, nil, err
		}
		return nil, resp.Body, resp.Header.Values("Set-Cookie"), nil
	}
	_, body, setCookies, err := req()
	if err != nil {
		return "", err
	}
	if body != nil {
		io.Copy(io.Discard, body)
		body.Close()
	}
	var pairs []string
	seen := map[string]bool{}
	for _, sc := range setCookies {
		nv := strings.SplitN(sc, ";", 2)[0]
		name := strings.TrimSpace(strings.SplitN(nv, "=", 2)[0])
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		pairs = append(pairs, strings.TrimSpace(nv))
	}
	if len(pairs) == 0 {
		return "", fmt.Errorf("匿名 /app 没返回 Set-Cookie")
	}
	return strings.Join(pairs, "; "), nil
}

// streamGenerateConv 发一路会话的一轮。turn==0 是首轮（建会话），>0 是续接（只发新消息）。
// 整条会话绑定 conv.cookie / conv.proxyID，中途不换号不换出口。成功后回填 conv 的
// cid/rid/rcid/tok26 和 turn+1，供下一轮续接。
func streamGenerateConv(ctx context.Context, prompt string, mc ModelConfig, conv *convState,
	onDelta, onReasoning func(string)) (*StreamResult, error) {
	// CanRestoreConvHead 是 fail-closed：只在「还没碰上游」的确定性本地失败
	//（入口取消、拿不到出口/仲裁失败）显式置 true，head 才允许归还复用。
	if err := ctx.Err(); err != nil {
		return &StreamResult{CanRestoreConvHead: true}, err
	}
	p, release, prefer, proxyURL, err := acquireConvExit(conv)
	if err != nil {
		return &StreamResult{AccountID: conv.accountID, CanRestoreConvHead: true,
			ProxyID: p.ID, ProxyName: p.Name}, err
	}
	defer release()
	pickedOK := p.ID > 0

	if err := convFirstTurnSetup(conv, p, prefer, proxyURL); err != nil {
		return &StreamResult{ProxyID: p.ID, ProxyName: p.Name}, err
	}

	// 登录态每轮要带 at（XSRF）；匿名不要。
	xsrf := ""
	if conv.isLogin && conv.cookie != "" {
		// getXSRF 是会话账号对上游的第一发：记入 fstate.touched（多轮跟单轮
		// 共用统一准入；v2.2 §8——reqFailoverState 经 convState.fstate 透传，
		// runConversationTurn 签名保持原样不变）。
		if conv.fstate != nil {
			conv.fstate.markTouched(conv.accountID)
		}
		if tok, err := getXSRF(conv.cookie, conv.authuser, proxyURL); err == nil {
			xsrf = tok
		} else {
			return &StreamResult{ProxyID: p.ID, ProxyName: p.Name, AccountID: conv.accountID},
				fmt.Errorf("取 XSRF 失败: %w", err)
		}
	}

	reqUUID := uuid.NewString()
	inner := convInnerSlots(conv, mc, prompt, reqUUID)

	innerJSON, _ := json.Marshal(inner)
	outerJSON, _ := json.Marshal([]interface{}{nil, string(innerJSON)})

	buildBody := func(at string) string {
		form := url.Values{}
		form.Set("f.req", string(outerJSON))
		if at != "" {
			form.Set("at", at)
		}
		return form.Encode()
	}
	body := buildBody(xsrf)

	geminiHeaders := buildGeminiHeaders(conv.cookie, conv.sapisid, mc.HexID)
	setAuthUserHeaders(geminiHeaders, conv.authuser)
	geminiHeaders["x-goog-ext-525001261-jspb"] = buildModelHeader(mc.HexID, mc.Mode, mc.thinkWire(), uuid.NewString())
	geminiHeaders["x-goog-ext-525005358-jspb"] = fmt.Sprintf(`["%s",1]`, reqUUID)

	reqid := time.Now().UnixNano() % 1000000
	endpoint := fmt.Sprintf(
		"%s/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate?bl=%s&hl=en&_reqid=%d&rt=c",
		geminiBase(conv.authuser), currentBL(proxyURL), reqid)

	tracker := &deltaTracker{}
	rtracker := &deltaTracker{}
	strictCheck := strictLineCheck(mc, func() int64 { return conv.accountID })
	var lineCB func(string) error
	if onDelta != nil || onReasoning != nil || strictCheck != nil {
		lineCB = func(line string) error {
			if strictCheck != nil {
				if err := strictCheck(line); err != nil {
					return err
				}
			}
			// 出错话术在吐给客户端之前拦下，中断读取走重试（见 gemini.go 同一处）。
			if failureReplyInLine(line) {
				return errUpstreamFailureReply
			}
			if onReasoning != nil {
				if r := reasoningInLine(line); r != "" {
					if d := rtracker.Push(r); d != "" {
						onReasoning(d)
					}
				}
			}
			if onDelta == nil {
				return nil
			}
			for _, t := range renderedTextsInLine(line, false) {
				if d := tracker.Push(t); d != "" {
					onDelta(d)
				}
			}
			return nil
		}
	}

	t0 := time.Now()
	var lastErr error
	var lastDiag upstreamDiagMeta
	var lastTTFB int64
	// convHopRes 是本轮（会话账号+出口）的代理记账暂挂载体：多轮续接同样
	// 受 v2.2 §7「内层重试不提前写代理分」约束，失败归因在本轮结束后由
	// 调用方 commitPendingProxy 统一结算。
	convHopRes := &StreamResult{}
	noteConvProxy := func(success bool, errStr string) {
		noteProxyResult(convHopRes, p.ID, success, errStr, true)
	}
	// mergePendingProxy 把本轮暂挂迁到返回的 res 上（各 return 统一调）。
	mergePendingProxy := func(res *StreamResult) *StreamResult {
		if res != nil {
			res.PendingProxyFail, res.PendingProxyOK, res.proxyCommitted =
				convHopRes.PendingProxyFail, convHopRes.PendingProxyOK, convHopRes.proxyCommitted
		}
		return res
	}
	// failRes 是「循环启动后」各失败返回路径的统一回填：真实计时
	//（TTFBMs/TotalMs）、结构化现场（Diag/UpstreamModel）与已发增量。
	failRes := func() *StreamResult {
		return mergePendingProxy(&StreamResult{
			ProxyID: p.ID, ProxyName: p.Name, AccountID: conv.accountID,
			Diag: lastDiag, UpstreamModel: lastDiag.Model,
			TTFBMs: lastTTFB, TotalMs: time.Since(t0).Milliseconds(),
			Emitted: tracker.emitted, EmittedReasoning: rtracker.emitted,
		})
	}
	for attempt := 0; attempt < rtCfg().RetryAttempts; attempt++ {
		if cerr := ctx.Err(); cerr != nil {
			return failRes(), cerr
		}
		// 同 streamGenerateOnce：每轮先清上一轮现场，防帧数污染最后一轮。
		lastDiag = upstreamDiagMeta{}
		lastTTFB = 0
		// StreamGenerate 发出前登记 touched（会话账号的真实上游使用）。
		if conv.fstate != nil {
			conv.fstate.markTouched(conv.accountID)
		}
		statusCode, raw, ttfb, setCookie, err := doGeminiRequestStopMode(ctx, endpoint, body, geminiHeaders, proxyURL, lineCB, nil, mc.TaskMode)
		if len(raw) > 0 {
			lastDiag.Frames, lastDiag.ContentFrame, lastDiag.Model = upstreamFrameStats(string(raw))
		}
		if ttfb > 0 {
			lastTTFB = ttfb
		}
		if len(setCookie) > 0 && conv.cookie != "" {
			if merged := mergeSetCookieForHost(conv.cookie, setCookie, endpoint, geminiCookieTargetHost); merged != conv.cookie {
				conv.cookie = merged
			}
		}
		// Argon 身份缺失修正归类（同 streamGenerateOnce）：出错话术 →
		// errUpstreamFailureReply；真·空帧 → nil 落无内容帧分支；
		// 有正文/思考但无合法身份 → 原样拒绝未验证模型。
		if statusCode == 200 {
			err = argonMissingReclassify(err, string(raw), mc.Tool)
		}
		var aie *argonIdentityError
		if strictCheck != nil && errors.As(err, &aie) && modelDowngraded(mc, aie.name) {
			err = &ModelDowngradedError{Requested: mc.Upstream, Reported: aie.name, AccountID: conv.accountID}
		}
		// 降级：同一个号重试还是降级，直接交给上层（见 gemini.go 同一处）。
		var mde *ModelDowngradedError
		if errors.As(err, &mde) {
			res := failRes()
			res.UpstreamModel = mde.Reported
			return res, err
		}
		if err != nil || statusCode != 200 || !hasContentFrame(string(raw)) {
			if cerr := ctx.Err(); cerr != nil {
				return failRes(), cerr
			}
			if err == nil && statusCode == 200 {
				// 无帧在 conversation.go:412 用中文报错（续接作废提示），错误串里
				// 没有 "no content frame"，upstreamReason 按串匹配会漏归成兜底
				// upstream_error —— 显式带 diagErr reason，跟库/单轮同前缀。
				lastErr = &diagErr{err: fmt.Errorf("续接无内容帧（会话可能已失效，raw %d 字节）", len(raw)),
					reason: reasonNoContentFrame}
			} else if err != nil {
				lastErr = err
			} else {
				lastErr = fmt.Errorf("upstream HTTP %d: %s", statusCode, truncate(string(raw), 160))
			}
			// 出错话术跟出口无关，不记进代理健康度；浏览器自身错误
			// （browser_unavailable）同理——那是本地浏览器层的问题。
			// errStreamIdle 正常记：开流后断流/黑洞的坏代理该累计失败次数
			// 触发熔断与迁离（见 gemini.go）。
			if pickedOK && !errors.Is(err, errUpstreamFailureReply) && !isBrowserLocalError(err) {
				noteConvProxy(false, lastErr.Error())
			}
			if tracker.emitted != "" || rtracker.emitted != "" {
				break
			}
			if !retryAfterTimeout(err) {
				logf("上游超时，retry_on_timeout 关着，不再重试: %v", err)
				break
			}
			if attempt < rtCfg().RetryAttempts-1 {
				if errors.Is(err, errUpstreamFailureReply) {
					logf("retry %d/%d: %v", attempt+1, rtCfg().RetryAttempts, err)
				}
				if werr := sleepCtx(ctx, retryBackoffDelay(rtCfg().RetryDelaySec, attempt, lastErr)); werr != nil {
					return failRes(), werr
				}
			}
			continue
		}
		// 非流式没有 lineCB，在这里补判出错话术。续接状态还没回填，重发仍接在上一轮后面。
		if hasFailureReply(string(raw)) {
			lastErr = errUpstreamFailureReply
			if tracker.emitted != "" || rtracker.emitted != "" {
				break
			}
			if attempt < rtCfg().RetryAttempts-1 {
				logf("retry %d/%d: %v", attempt+1, rtCfg().RetryAttempts, lastErr)
				if werr := sleepCtx(ctx, retryBackoffDelay(rtCfg().RetryDelaySec, attempt, lastErr)); werr != nil {
					return failRes(), werr
				}
			}
			continue
		}
		// 成功：回填续接状态。
		cid, rid, rcid, tok26 := parseConvIDs(string(raw))
		if cid != "" {
			conv.cid = cid
		}
		if rid != "" {
			conv.rid = rid
		}
		if rcid != "" {
			conv.rcid = rcid
		}
		conv.tok26 = tok26 // 每轮更新（可能为空）
		conv.turn++
		if pickedOK {
			noteConvProxy(true, "")
		}
		return mergePendingProxy(&StreamResult{
			Emitted:          tracker.emitted,
			EmittedReasoning: rtracker.emitted,
			Raw:              string(raw),
			Reasoning:        extractReasoning(string(raw)),
			UpstreamModel:    extractUpstreamModel(string(raw)),
			ProxyID:          p.ID,
			ProxyName:        p.Name,
			AccountID:        conv.accountID,
			TTFBMs:           ttfb,
			TotalMs:          time.Since(t0).Milliseconds(),
		}), nil
	}
	return failRes(), lastErr
}

// acquireConvExit 给本轮会话拿出口 slot：按库中实锤的当前绑定仲裁
// （内存快照可能陈旧），挑到后同步绑定持久化（非严格模式才写库——严格
// 模式的落库已在 claimMu 仲裁里完成）。返回 (picked, release, proxyURL, err)。
func acquireConvExit(conv *convState) (Proxy, func(), int64, string, error) {
	prefer := conv.proxyID
	if conv.isLogin && conv.accountID > 0 {
		// 会话内存快照可能陈旧：管理员改绑或并发请求已写入新绑定后，
		// acquireSlot 必须按库里的当前值走（严格绑定的承诺按库兑现）。
		if a := accountByID(conv.accountID); a != nil && a.ProxyID != conv.proxyID {
			prefer = a.ProxyID
		}
	}
	acctID := int64(0)
	if conv.isLogin {
		acctID = conv.accountID
	}
	pp, release, slotErr := acquireClaimedSlot(acctID, prefer)
	if pp == nil {
		// AccountID 带上会话绑的号：strict_proxy_binding 的 Bound 错误要靠它
		// 把这个号并进外层 exclude 换号（见 retryOnAnotherAccount）。
		return Proxy{}, func() {}, 0, "", slotErr
	}
	p := *pp
	// 绑定持久化：挑到的出口跟记的不一样就写库同步——含首次绑定（旧值 0）、
	// 绑定出口被删/禁用后的重绑、以及管理员在面板上改绑后被本轮仲裁纠正回
	// 胜者的场景。匿名会话（accountID==0）不写：它没有账号行可绑。
	// 严格模式下绝不锁外写库：绑定仲裁已在 acquireClaimedSlot 的 claimMu 里完成，
	// 这里再写会覆盖管理员并发改绑（R3-S2）；内存快照 conv.proxyID 仍就地同步。
	// conv.proxyID == proxyIDDirect（锁定直连）绝不覆写：直连是它的合法承诺。
	if conv.isLogin && conv.accountID > 0 && p.ID > 0 && p.ID != conv.proxyID && conv.proxyID != proxyIDDirect {
		if !rtCfg().StrictProxyBinding {
			autoBindAccountProxy(conv.accountID, p.ID)
		}
		conv.proxyID = p.ID
	}
	return p, release, prefer, p.URL, nil
}

// convFirstTurnSetup 只在首轮（turn==0）定出口并给匿名会话建载体 cookie：
// 首轮定出口；续接沿用会话原出口（acquireSlot 已优先，但拿不到原出口时只能换，
// 换了续接大概率失败，靠调用方回退到全量重发兜底）。
// 账号锁定 proxyIDDirect 时不许用 p.ID（0=未绑语义的直连槽值）覆写绑定记忆——
// -1 是「永不走代理」的显式承诺，落了 0 下一轮就丢了这个承诺。
// p.ID == 0（直连槽）且库中实为锁定直连时同样核库保持 -1，不用 0 误落。
func convFirstTurnSetup(conv *convState, p Proxy, prefer int64, proxyURL string) error {
	if conv.turn != 0 {
		return nil
	}
	conv.proxyID = resolveFirstTurnProxyID(conv, prefer, p.ID)
	conv.proxyURL = proxyURL
	// 匿名首轮：没有登录 cookie，就地拿一份 session cookie 当会话载体。
	if conv.cookie == "" && !conv.isLogin {
		c, err := getAnonSession(proxyURL)
		if err != nil {
			return fmt.Errorf("建匿名会话失败: %w", err)
		}
		conv.cookie = c
	}
	return nil
}

// convInnerSlots 拼一路会话一轮的 inner 槽位数组（首轮空接/续接回填三件套）。
func convInnerSlots(conv *convState, mc ModelConfig, prompt, reqUUID string) []interface{} {
	inner := make([]interface{}, innerSlots)
	inner[0] = []interface{}{prompt, 0, nil, nil, nil, nil, 0}
	inner[1] = []interface{}{"en"}
	if conv.turn == 0 {
		inner[2] = []interface{}{"", "", "", nil, nil, nil, nil, nil, nil, ""}
	} else {
		// 续接：[cid, rid, rcid(登录)/""(匿名), null×6, tok26]
		inner[2] = []interface{}{conv.cid, conv.rid, conv.rcid,
			nil, nil, nil, nil, nil, nil, conv.tok26}
	}
	inner[6] = []interface{}{0}
	inner[7] = 1
	inner[10] = 1
	inner[11] = 0
	inner[17] = []interface{}{[]interface{}{conv.turn}}
	inner[18] = 0
	inner[27] = 1
	inner[30] = []interface{}{4}
	inner[41] = []interface{}{1}
	inner[53] = 0
	inner[59] = reqUUID
	inner[61] = []interface{}{}
	inner[68] = 1
	inner[79] = mc.Mode
	tw := mc.thinkWire()
	inner[80] = tw
	inner[91] = 0
	inner[96] = 0
	if tw != thinkingNormal {
		inner[96] = 1
	}
	// 任务模式槽位：仅 mc.TaskMode 触发，与 streamGenerateOnce 同一套路由——
	// Mode==8 走 argonMode8Slots，Mode!=8 走 taskModeSlots；对话模型发纯普通 inner。
	if mc.TaskMode {
		if mc.Mode == 8 {
			argonMode8Slots(inner)
		} else {
			taskModeSlots(inner)
		}
	}
	return inner
}

// resolveFirstTurnProxyID 决定首轮会话记哪个出口（turn==0 时调一次）：
// 登录账号以库中实锤为准——锁定直连的号即便手上的槽是 0 也记 -1（-1 是
// 「永不走代理」的显式承诺，落了 0 下一轮就丢了）；库中值已被改回代理或
// prefer 已核成 -1 之外的值时按 prefer/picked 记。
func resolveFirstTurnProxyID(conv *convState, prefer, pickedID int64) int64 {
	// pickedID>0 一律返回 pickedID：会话实际建在这个出口上，只有
	// pickedID==0（直连槽）时才可能记 proxyIDDirect——并发改绑已把会话
	// 落到真代理时记 -1 会让下一轮误判无需冷启动而串出口。
	if pickedID > 0 {
		return pickedID
	}
	if conv.isLogin && conv.accountID > 0 {
		if a := accountByID(conv.accountID); a != nil && a.ProxyID == proxyIDDirect {
			return proxyIDDirect
		}
		if prefer == proxyIDDirect {
			return proxyIDDirect
		}
	}
	return pickedID
}

// convMustColdStart 判断登录会话是否必须作废回冷启动：
//   - 会话绑死的号被禁用/删除：再往上发要么失败，更糟的是在
//     BrowserTransport=true && BrowserFallbackHTTP=false 下绕过常驻浏览器
//     漏出 Go HTTP 指纹——强制冷启动让挑号逻辑重新选健康账号；
//   - 锁定直连参与改绑（任一侧是 -1）：无论 strict_proxy_binding 开不开都
//     必须冷启动——-1 的「绝不走代理/绝不直连」承诺不靠开关兑现，带着旧
//     出口的 cid/rid/rcid 续接等于从错误出口把会话推下去；
//   - 严格绑定下绑定出口变了：新绑定的出口才是这个号的合法出口，会话
//     记忆里那个旧出口（含直连 0）绝不能再外发。
func convMustColdStart(conv *convState) bool {
	a := accountByID(conv.accountID)
	switch {
	case a == nil || a.Status != "enabled":
		logf("[conv] 会话绑定账号 #%d 已禁用或删除，回退冷启动重挑账号", conv.accountID)
		return true
	case (a.ProxyID == proxyIDDirect || conv.proxyID == proxyIDDirect) &&
		a.ProxyID != conv.proxyID:
		logf("[conv] 账号 #%d 锁定直连绑定出口已从 #%d 改为 #%d，回退冷启动",
			conv.accountID, conv.proxyID, a.ProxyID)
		return true
	case rtCfg().StrictProxyBinding && a.ProxyID != conv.proxyID &&
		(a.ProxyID > 0 || a.ProxyID == proxyIDDirect || conv.proxyID == proxyIDDirect):
		logf("[conv] 账号 #%d 绑定出口已从 #%d 改为 #%d，回退冷启动",
			conv.accountID, conv.proxyID, a.ProxyID)
		return true
	}
	return false
}

// strictOrLockedPool 决定 Bound 换号预算要不要放宽到全池：严格绑定开着，
// 或池里存在启用中的锁定直连（-1）账号（直连槽满时它们彼此互堵，第 4+
// 个锁定直连号也该被试到）。
func strictOrLockedPool() bool {
	if rtCfg().StrictProxyBinding {
		return true
	}
	for _, a := range accountList() {
		if a.ProxyID == proxyIDDirect && a.Status == "enabled" {
			return true
		}
	}
	return false
}

// callGeminiConv 是多轮开启时的入口：按历史前缀识别续接。
//   - 命中已知历史前缀 → 发全部新增消息（user 或 tool 结果），历史留服务端（绕开字节墙）。
//   - 没命中 → 新建会话，首轮发全量拼接的历史（含 tools 指令，把当前上下文交给服务端）。
//
// 支持 tools（续接里也解析 ```tool_call``` 围栏）；图片走原单轮路径（callGemini）。
// ctx 全程透传：客户端取消/超时不能丢（本仓跟上游的分歧点，上游没有 ctx 参数）。
func callGeminiConv(ctx context.Context, messages []map[string]interface{}, mc ModelConfig,
	tools []map[string]interface{}, toolChoice interface{},
	onDelta, onReasoning func(string), options ...conversationOptions) (string, []ToolCall, *StreamResult, error) {
	// 已取消的请求不许动会话存储和账号池：convTake 会销毁健康 head，
	// pickAccountForModel 会白占一轮轮询。
	if err := ctx.Err(); err != nil {
		return "", nil, &StreamResult{}, err
	}
	messages = normalizeConversationMessages(messages)
	var opt conversationOptions
	if len(options) > 0 {
		opt = options[0]
	}
	scope := convScopeKey(mc, tools, toolChoice, opt.requestScope)
	keys := convPrefixKeys(messages, scope, opt.normalizeToolIDs)
	conv, prefixLen, takenKey, savedConv := convTakeForTurn(keys)
	fresh := conv == nil
	if !fresh && conv.isLogin {
		if convMustColdStart(conv) {
			fresh = true
			conv = nil
		}
	}
	prompt, fresh := convTurnPrompt(messages, prefixLen, fresh, conv, mc, tools, toolChoice)

	// strict_model 换号只对新会话做：续接的会话绑死在原来的号上，降级了只记冷却、
	// 报错作废这路会话，客户端重试时当新会话全量重发，自然挑到别的号。
	// Bound 换号不受 maxCookieTries 截断：绑定出口不可用不是模型降级，池里第 4+
	// 个绑在空闲出口上的号也该被试到。上限取「全池账号数 + maxCookieTries」，
	// 足够覆盖任意大小池子的 Bound 换号 + 原有的降级换号预算。
	// 防雪崩顶层共享跳槽准入状态：多轮换号跟单轮共用一套预算
	//（v2.2 §8：runConversationTurn 签名不变，经 convState/闭包透传 fstate）。
	fstate := newReqFailoverState(false)
	defer releaseReqProbes(fstate.reqID)
	exclude := map[int64]bool{}
	var res *StreamResult
	var err error
	var prevRes *StreamResult
	var prevErr error
	var pendingHop0 *pendingHopFailure // fresh 第 0 跳 C1/C1b 的延迟暂挂（未结算）
	var lastBoundErr *RateLimitError
	// 两条预算独立：上游模型降级换号严格保持 maxCookieTries=3（连着 3 个号
	// 都降级说明池子整体见底）；Bound 受阻换号只在严格绑定下放宽到账号池
	// 大小（绑定出口不可用不是降级，第 N 个号绑在空闲出口上就该被试到）。
	// 分开计数互不放大：Bound 次数不能用来延长降级预算，反之亦然。
	boundBudget := boundHopBudget()
	boundTries := 0
	firstAttempt := true
	// 物理上限防死循环：Bound 换号最多走遍全池 + 降级预算；预算内的独立上限
	// 由 fstate.tryAdvanceHop 统一判定（与单轮同一套准入不变量）。
	hardCap := maxCookieTries + boundBudget
	// 退出统一兜底：暂挂的 pendingHop0 按「无法双号验证」单次因果提交，
	// 本跳代理记账照常落库（毒载荷路径 settleHop 已把暂挂作废，幂等安全）。
	settleExit := func() {
		settlePendingHop(pendingHop0, fstate, true)
		commitPendingProxy(res)
	}
	for tries := 0; tries < hardCap; tries++ {
		if fresh {
			conv = &convState{}
			var perr error
			perr, conv = prepareFreshConvHop(mc, prompt, exclude, fstate)
			if perr != nil {
				// 挑号失败：优先报上一跳真实错误（降级/Bound），没有才按
				// 冷却/降级/perr 报——prevErr 不在这里被 perr 覆盖。
				res, err = resolveConvPickErr(perr, prevRes, prevErr, lastBoundErr, firstAttempt, mc)
				break
			}
		}
		res, err = runConvWithFailover(ctx, prompt, mc, conv, onDelta, onReasoning, fstate)
		if !fresh {
			settleConvContinuation(res, err, takenKey, &savedConv, fstate, mc)
			// !fresh 不换号的红线必须守在打换号日志之前：Bound/降级续接只返回错误，
			// 不能打「换下一个号」又把请求钉在原号上重发。
			break
		}
		firstAttempt = false
		var done bool
		res, err, pendingHop0, done = settleFreshConvHop(res, err, tools, fstate, mc, exclude, &pendingHop0)
		if done {
			break
		}
		var rle *RateLimitError
		if errors.As(err, &rle) && rle.Bound {
			lastBoundErr = rle
		}
		// 与单轮同一套准入（advanceOuterHop）：C4b 仅未碰上游时豁免
		//（受 boundBudget 截断），C3 走降级预算至多 3 号，故障类至多 1 跳。
		// 多轮无 FallbackAnon 外层兜底（fresh 才走，C4a 由调用方按现有
		// 语义结算）——anonAllowed=false。
		adv, _, _ := advanceOuterHop(res, err, fstate, mc, exclude,
			boundBudget, &boundTries, pendingHop0, false, nil)
		if !adv {
			break
		}
		prevRes, prevErr = res, err
	}
	settleExit()
	if err != nil {
		// 旧 head 已在生成前被 convTake 消费：失败请求上游可能已推进，
		// 不能把旧状态放回去让重试撞同一路会话——客户端重发全历史时会冷启动。
		return "", nil, res, err
	}
	text, toolCalls, verr := validateConvTurnText(res, tools)
	if verr != nil {
		return "", nil, res, verr
	}
	// 存续接状态：按完整逻辑回复（含 tool_calls）发布新 head——客户端下一轮把
	// 这段回复原样带回来时，convTake 会命中它的前缀键。只发新 head，旧键已消费，
	// 纯工具回复（text 为空、只有 tool_calls）也能被区分开。
	headKeys := convPrefixKeys(conversationMessagesWithReply(messages, text, toolCalls), scope, opt.normalizeToolIDs)
	// 没拿到 cid 的会话不能落库：上游没给 cid 就永远续接不上，存了只会让
	// 下一路同前缀请求撞进一条死会话（发的续接请求带空 cid）。
	if len(headKeys) > 0 && conv.cid != "" {
		convPut(headKeys[len(headKeys)-1], conv)
	}
	return text, toolCalls, res, nil
}

// validateConvTurnText 抽成功轮的最终正文：抽空时按「有帧抽不出
// （empty_content）/ 无帧（no_content_frame）」归一报错（同 callGemini）。
func validateConvTurnText(res *StreamResult, tools []map[string]interface{}) (string, []ToolCall, error) {
	text := extractResponseText(res.Raw)
	var toolCalls []ToolCall
	if len(tools) > 0 {
		text, toolCalls = parseToolCalls(text)
	}
	if text != "" || len(toolCalls) > 0 {
		return text, toolCalls, nil
	}
	if hasContentFrame(res.Raw) {
		return "", nil, &diagErr{
			err:    fmt.Errorf("%w (raw %d bytes)", errEmptyContent, len(res.Raw)),
			reason: reasonEmptyContent, res: res}
	}
	return "", nil, &diagErr{
		err:    fmt.Errorf("upstream returned no content frame (raw %d bytes)", len(res.Raw)),
		reason: reasonNoContentFrame, res: res}
}

// runConversationTurn is the upstream seam used by continuation regression tests.
var runConversationTurn = streamGenerateConv

// runConvWithFailover 包一层上游轮次：把顶层共享 fstate 挂到 conv 上供
// streamGenerateConv 内部 markTouched，并在返回后把实际用了哪个号登记
// 进 touched。
//
// MF-2（后果 D）：成功提交（commitPendingProxy + recordAccountReqSuccess）
// 绝不在这里做——fresh 首跳须先过 validateConvTurnOutput（C1b 空输出校验）
// 与整跳延迟结算后才记成功；!fresh 续接由调用方在合适时机显式结算。
func runConvWithFailover(ctx context.Context, prompt string, mc ModelConfig, conv *convState,
	onDelta, onReasoning func(string), fstate *reqFailoverState) (*StreamResult, error) {
	prev := conv.fstate
	conv.fstate = fstate
	defer func() { conv.fstate = prev }()
	res, err := runConversationTurn(ctx, prompt, mc, conv, onDelta, onReasoning)
	// 只在真正碰过上游时记 touched：C4b 门前 Bound（CanRestoreConvHead=true，
	// 本地拿 slot 就失败，没发过一个字节）不算「试过一个号」——多轮的 Bound
	// 换号预算豁免全靠这个判定，记了会把每个 Bound 号误记成已碰上游，
	// 让 advanceOuterHop 把它改判成故障跳槽消耗 faultHops（至多 1 跳就截断，
	// 池里第 4+ 个绑空闲出口的号永远试不到）。
	if res != nil && res.AccountID > 0 && !res.CanRestoreConvHead && fstate != nil {
		fstate.markTouched(res.AccountID)
	}
	return res, err
}

// convTakeForTurn 消费一条续接 head：返回 (conv, prefixLen, takenKey, savedConv)。
// takenKey 记 convTake 消费的 head 键、savedConv 是它的初始快照：续接在发往
// 上游之前就本地失败（Bound 429 / ctx 取消）时按原样归还（convPutIfAbsent）。
func convTakeForTurn(keys []string) (*convState, int, string, convState) {
	conv, prefixLen := convTake(keys)
	takenKey := ""
	if conv != nil && prefixLen > 0 {
		takenKey = keys[prefixLen-1]
	}
	savedConv := convState{}
	if conv != nil {
		savedConv = *conv
	}
	return conv, prefixLen, takenKey, savedConv
}

// boundHopBudget 是 Bound 受阻换号的预算：严格绑定或池里有锁定直连（-1）
// 号时放宽到全池账号数（直连槽满时第 4+ 个锁定直连号也该被试到，与
// streamGenerateOnce 扫全池一致），否则保持 maxCookieTries。
func boundHopBudget() int {
	budget := maxCookieTries
	if strictOrLockedPool() {
		if total, _ := accountCount(); total > budget {
			budget = total
		}
	}
	return budget
}

// prepareFreshConvHop 为 fresh 首轮挑号并初始化 convState：
// 匿名优先（模型不需要登录态时不占号）时直接返回空 conv；挑到登录号时
// 回填 cookie/sapisid/accountID/authuser/proxyID。
// 返回 (pickErr, conv)：pickErr 非 nil 表示挑号失败（nee/冷却/降级），
// 由调用方按 resolveConvPickErr 收尾。
func prepareFreshConvHop(mc ModelConfig, prompt string, exclude map[int64]bool,
	fstate *reqFailoverState) (error, *convState) {
	conv := &convState{}
	// #20 匿名优先：模型不需要登录态时不占号（多轮已被 gate 限成无图无工具，附件
	// 恒无，所以第二个参数传 false）。必须在调用前短路：挑号会更新
	// last_used_at，调了不用等于白占一轮。
	if anonFirstEligible(mc, false, len(prompt)) {
		return nil, conv
	}
	a, perr := pickFreshConvAccount(mc, exclude, fstate)
	if perr != nil {
		return perr, conv
	}
	if a != nil {
		conv.cookie = a.Cookie
		conv.sapisid = extractSAPISID(a.Cookie)
		conv.isLogin = true
		conv.accountID = a.ID
		conv.authuser = a.AuthUser
		conv.proxyID = a.ProxyID
	}
	return nil, conv
}

// convTurnPrompt 决定本轮发给上游的 prompt 与是否按新会话发：
// fresh（或增量拼不出正文的续接回退）发全量历史；续接只发新增段。
// 返回（prompt, fresh）：fresh 可能在增量为空时被改判回 true。
func convTurnPrompt(messages []map[string]interface{}, prefixLen int, fresh bool,
	conv *convState, mc ModelConfig, tools []map[string]interface{},
	toolChoice interface{}) (string, bool) {
	if fresh {
		// 首轮带上 tools 指令：模型要靠它知道怎么吐 tool_call 围栏，续接轮不再重发
		//（指令已在服务端首轮历史里）。
		prompt, _ := messagesToPrompt(messages, tools, toolChoice, mc.TaskMode)
		logf("[conv] 新会话，首轮发 %d 字节（tools=%d）", len(prompt), len(tools))
		return prompt, true
	}
	// 续接一次性发 messages[prefixLen:] 全部新增消息，跟首轮同一个格式化
	// 通道（tool 结果带 [Tool result for …] 前缀）；mc.TaskMode 必须透传——
	// Task 模式的工具锚点不收平台「0 remaining」免疫词。
	prompt := buildPrompt(messages[prefixLen:], nil, nil, mc.TaskMode)
	if strings.TrimSpace(prompt) == "" {
		// 增量拼不出正文（新增消息全空）：发 0 字节 prompt 上游只会报
		// 一个看不懂的错，回退全量历史冷启动。
		p, _ := messagesToPrompt(messages, tools, toolChoice, mc.TaskMode)
		logf("[conv] 续接增量为空，回退新会话全量发 %d 字节", len(p))
		return p, true
	}
	logf("[conv] 续接命中 turn=%d，只发 %d 字节（新增 %d 条消息，历史留服务端）",
		conv.turn, len(prompt), len(messages)-prefixLen)
	return prompt, false
}

// settleConvContinuation 是 !fresh 续接轮返回后的统一收尾（MF-2）：
// 确定性本地失败归还被 convTake 消费的 head；代理成败显式 commit；
// 成功记账号成功；降级记该号该模型冷却（续接绝不换号）。
func settleConvContinuation(res *StreamResult, err error, takenKey string,
	savedConv *convState, fstate *reqFailoverState, mc ModelConfig) {
	// 续接在发往上游之前就确定性本地失败（CanRestoreConvHead 为 true）：
	// 被消费的 head 仍然有效，按原键归还，让客户端重试能接着 turn 续接。
	if err != nil && res != nil && res.CanRestoreConvHead && takenKey != "" {
		convPutIfAbsent(takenKey, savedConv)
	}
	// v2.2 §3.4：!fresh 续接返回路径必须显式 commitPendingProxy——
	// 会话绑原号不换，代理成败照常记（续接不走延迟毒载荷结算）。
	commitPendingProxy(res)
	// 续接成功记账号成功（半开探测成功会清零并放探测位）。
	if err == nil && res != nil && res.AccountID > 0 {
		recordAccountReqSuccess(res.AccountID, fstate.reqID)
	}
	// 续接降级：把降级登记进该号该模型的冷却（下次冷启动自动跳过它），
	// 但绝不换号——续接绑死在原号上，换了续接就是另一条会话。
	var mde *ModelDowngradedError
	if errors.As(err, &mde) && mde.AccountID > 0 {
		markDowngraded(mde.AccountID, mc)
	}
}

// settleFreshConvHop 是 fresh 首轮每跳返回后的统一结算（MF-2 后果 D）：
//   - err==nil 时先做 C1b 空输出校验（成功提交之前），通过后才
//     commitPendingProxy + recordAccountReqSuccess；
//   - 失败走与单轮同构的 settleHop 延迟结算（pendingHop0 + isPoisonPair）。
//
// 返回 (res, err, pendingHop0, done)：done=true 表示本跳已定局（成功、
// 毒载荷实锤或不再跳槽前的最后状态），调用方据此 break。
func settleFreshConvHop(res *StreamResult, err error, tools []map[string]interface{},
	fstate *reqFailoverState, mc ModelConfig, exclude map[int64]bool,
	pendingHop0 **pendingHopFailure) (*StreamResult, error, *pendingHopFailure, bool) {
	// MF-2（后果 D）：C1b 空输出校验必须在成功提交**之前**——
	// validateConvTurnOutput 检出的 errEmptyContent 参与 isPoisonPair
	// 与跨号跳槽，通过校验、整跳落定后才调 commitPendingProxy 与
	// recordAccountReqSuccess（先记成功再校验会把失败错记成成功）。
	if err == nil && res != nil && res.Emitted == "" && res.EmittedReasoning == "" {
		if verr := validateConvTurnOutput(res, tools); verr != nil {
			err = verr
		}
	}
	if err == nil {
		// 本跳成功：第 0 跳暂挂非毒按因果提交，代理记账 + 记账号成功。
		settlePendingHop(*pendingHop0, fstate, true)
		commitPendingProxy(res)
		if res != nil && res.AccountID > 0 {
			recordAccountReqSuccess(res.AccountID, fstate.reqID)
		}
		return res, nil, *pendingHop0, true
	}
	// fresh 失败：与单轮同构的延迟结算——C1/C1b 可跳槽时暂挂 pendingHop0
	//（不写全局），落定后按 isPoisonPair 判毒载荷双端免责或单次提交。
	var p0 *pendingHopFailure
	res, err, p0 = settleHop(res, err, *pendingHop0, fstate, mc, exclude)
	*pendingHop0 = p0
	if err == nil {
		return res, nil, p0, true
	}
	var ppe *poisonPayloadError
	if errors.As(err, &ppe) {
		return res, err, p0, true // 毒载荷实锤：双端零记账，直接定局
	}
	return res, err, p0, false
}

// resolveConvPickErr 决定 fresh 跳槽选号失败（perr）时的最终上报：
// 上一跳真实错误（降级/Bound）永远优先——「没号可挑」不该覆盖现场。
// 返回（最终 res, 最终 err）。
func resolveConvPickErr(perr error, prevRes *StreamResult, prevErr, lastBoundErr error,
	firstAttempt bool, mc ModelConfig) (*StreamResult, error) {
	var last *ModelDowngradedError
	var nee *noEligibleAccountError
	switch {
	case !firstAttempt && errors.As(prevErr, &last) && errors.As(perr, &nee):
		// 换号时已经没号可挑：报上一个号真实的降级，补上解冻时间
		last.RetryAfter = nee.retryAfter()
		return prevRes, last
	case errors.As(perr, &nee):
		if lastBoundErr != nil {
			// 排除集里是 Bound 受阻的号而不是降级号：全池 Bound 不可用时
			// 报真实的 Bound 429，不谎报模型降级。
			return prevRes, lastBoundErr
		}
		if prevErr != nil {
			return prevRes, prevErr // 上一跳真实失败比「没号可挑」更有信息量
		}
		if nee.shortRetryAfter > 0 {
			return prevRes, &AccountCooldownError{RetryAfter: nee.shortRetryAfter}
		}
		return prevRes, &ModelDowngradedError{Requested: mc.Upstream, RetryAfter: nee.modelRetryAfter}
	case lastBoundErr != nil && errors.As(perr, &last):
		return prevRes, lastBoundErr
	default:
		return &StreamResult{}, perr
	}
}

// pickFreshConvAccount 从 callGeminiConv 抽出的首轮挑号：统一走
// pickAccountForModel（带 fstate 半开探测认领；v6.6.0 多 Context 后
// 不再有 M1 驻留粘性优先）。返回 (nil, nil) 表示合法空池走匿名。
func pickFreshConvAccount(mc ModelConfig, exclude map[int64]bool, fstate *reqFailoverState) (*CookieAccount, error) {
	a, _, err := pickAccountForModel(mc, exclude, fstate)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// validateConvTurnOutput 是 fresh 首跳的循环内输出校验（C1b 前移）：
// 抽不出正文且无 tool_call 时返回 diagErr——hasContentFrame 为真记
// empty_content（C1b），无帧记 no_content_frame（C1）。
// 续接（!fresh）不调它：抽空在循环外按作废会话处理。
func validateConvTurnOutput(res *StreamResult, tools []map[string]interface{}) error {
	text := extractResponseText(res.Raw)
	var toolCalls []ToolCall
	if len(tools) > 0 {
		text, toolCalls = parseToolCalls(text)
	}
	if text != "" || len(toolCalls) > 0 {
		return nil
	}
	if hasContentFrame(res.Raw) {
		return &diagErr{err: fmt.Errorf("%w (raw %d bytes)", errEmptyContent, len(res.Raw)),
			reason: reasonEmptyContent, res: res}
	}
	return &diagErr{err: fmt.Errorf("upstream returned no content frame (raw %d bytes)", len(res.Raw)),
		reason: reasonNoContentFrame, res: res}
}
