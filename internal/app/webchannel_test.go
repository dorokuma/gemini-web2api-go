package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// 下行内容消息：X 是老响应的 inner 数组。正文要够长，textsInLine 对太短的行直接跳过。
const wcTestText = "伦敦是英国的首都，一座将千年历史与现代文化融合在一起的国际大都市，泰晤士河穿城而过。"

func wcContentPayload(flag string) string {
	x := `[null,["c_1","r_2"],null,null,[["rc_3",["` + wcTestText + `"],null,null,null,null,null,null,[1],"zh"]],` +
		`null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,` +
		`"fbb127bbb056c959",null,null,"3.6 Flash",true]`
	return `[[null,["5",null,null,null,null,null,[[` + flag + `,["r_2"],null,[null,null,null,` + x + `]]]]]]`
}

func TestWCReadFrames(t *testing.T) {
	// 长度按 JS 字符串计，跟 Go 字节数对不上（中文）；解析不能依赖它。
	content := wcContentPayload("0")
	body := "11\n[[1,[\"noop\"]]]" + "999\n[[2," + content + "]]" + "40\n[[3,[[null,null,null,[]]]]]"
	var ids []int64
	var evs []wcEvent
	if err := wcReadFrames(strings.NewReader(body), func(it wcItem) bool {
		ids = append(ids, it.ID)
		evs = append(evs, wcClassify(it.Payload))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || ids[2] != 3 {
		t.Fatalf("ids = %v", ids)
	}
	if evs[0] != (wcEvent{}) {
		t.Errorf("noop 应该什么都不是: %+v", evs[0])
	}
	if !evs[2].End {
		t.Errorf("[null,null,null,[]] 应该是结束: %+v", evs[2])
	}
	line := evs[1].Line
	if got := textsInLine(line); len(got) != 1 || got[0] != wcTestText {
		t.Errorf("包成老格式后取不出正文: %q", got)
	}
	if m := extractUpstreamModel(line); m != "3.6 Flash" {
		t.Errorf("取不出模型名: %q", m)
	}
}

func TestWCClassify(t *testing.T) {
	if ev := wcClassify([]byte(wcContentPayload("1"))); !ev.Done || ev.Line == "" {
		t.Errorf("flag=1 应该是生成完毕且带正文: %+v", ev)
	}
	if ev := wcClassify([]byte(wcContentPayload("0"))); ev.Done {
		t.Errorf("flag=0 不该是生成完毕")
	}
	if ev := wcClassify([]byte(`[[[]]]`)); !ev.Ack {
		t.Errorf("[[]] 应该是确认: %+v", ev)
	}
	if ev := wcClassify([]byte(`["close"]`)); !ev.End {
		t.Errorf("close 应该是结束")
	}

	cases := []struct {
		payload string
		code    int
		status  int
	}{
		{`{"__sm__":{"status":[[[16,"Request is missing required authentication credential."]]]}}`, 16, 401},
		{`{"__sm__":{"status":[[[7,"Method doesn't allow unregistered callers"]]]}}`, 7, 502},
		{`{"__sm__":{"status":[[{"error":{"code":400,"message":"Invalid JSON payload received."}}]]}}`, 400, 400},
	}
	for _, c := range cases {
		ev := wcClassify([]byte(c.payload))
		if ev.Err == nil || ev.Err.Code != c.code || ev.Err.httpStatus() != c.status {
			t.Errorf("%s → %+v", c.payload, ev.Err)
		}
	}
}

func TestWCModelHeaderBin(t *testing.T) {
	if got := wcModelHeaderBin("fbb127bbb056c959", 1, 0); got != "CAEqEGZiYjEyN2JiYjA1NmM5NTl4AQ==" {
		t.Errorf("3.6 Flash: %s", got)
	}
	if got := wcModelHeaderBin("9d8ca3786ebdfbea", 3, 2); got != "CAEqEDlkOGNhMzc4NmViZGZiZWF4A4ABAg==" {
		t.Errorf("3.1 Pro think=2: %s", got)
	}
}

// 带授权头不带 cookie 服务端判缺凭证，所以没 cookie 时一定不能加授权头。
func TestWCHTTPHeadersAuth(t *testing.T) {
	anon := wcHTTPHeaders("AIzaKEY", wcRequest{HexID: "fbb127bbb056c959", Mode: 1, SAPISID: "x"})
	if strings.Contains(anon, "Authorization") {
		t.Errorf("没 cookie 却带了授权头: %q", anon)
	}
	for _, want := range []string{"X-Goog-Api-Key:AIzaKEY\r\n", "x-goog-ext-525001261-bin:", "X-WebChannel-Content-Type:application/json+protobuf\r\n"} {
		if !strings.Contains(anon, want) {
			t.Errorf("缺 %q: %q", want, anon)
		}
	}
	login := wcHTTPHeaders("AIzaKEY", wcRequest{Cookie: "SAPISID=x", SAPISID: "x", AuthUser: 1})
	if !strings.Contains(login, "Authorization:SAPISIDHASH ") || !strings.Contains(login, "X-Goog-AuthUser:1\r\n") {
		t.Errorf("登录态缺授权头或账号序号: %q", login)
	}
}

func TestWCInnerWithoutPrompt(t *testing.T) {
	inner := []interface{}{[]interface{}{"hello", 0}, "x"}
	out := wcInnerWithoutPrompt(inner)
	if out[0].([]interface{})[0] != "" {
		t.Errorf("正文槽没清空")
	}
	if inner[0].([]interface{})[0] != "hello" {
		t.Errorf("改到了原数组")
	}
}

// 握手 URL 的 $httpHeaders 塞着 API key 和 SAPISIDHASH：错误往外带 URL 时必须脱敏，
// 但 host、VER= 等非敏感参数要留着便于定位；实际发送的 URL 不能动。
func TestWCRedactURL(t *testing.T) {
	const apiKey = "AIzaTESTKEY00000000000000000000000aa"
	r := wcRequest{HexID: "fbb127bbb056c959", Mode: 1, Cookie: "SAPISID=x", SAPISID: "x"}
	raw := fmt.Sprintf("%s?VER=8&RID=10001&CVER=22&X-HTTP-Session-Id=gsessionid&$httpHeaders=%s&zx=abc123&t=1",
		wcEndpoint, url.QueryEscape(wcHTTPHeaders(apiKey, r)))

	red := wcRedactURL(raw)
	for _, leak := range []string{"SAPISIDHASH", apiKey, url.QueryEscape(apiKey)} {
		if strings.Contains(red, leak) {
			t.Errorf("脱敏后的 URL 还含 %q: %s", leak, red)
		}
	}
	for _, keep := range []string{"VER=8", "RID=10001", "zx=abc123", "geminiweb-pa.clients6.google.com", "$httpHeaders=%3Credacted%3E"} {
		if !strings.Contains(red, keep) {
			t.Errorf("脱敏后的 URL 丢了 %q: %s", keep, red)
		}
	}
	// $httpHeaders 不在或落在末尾也要安全。
	if got := wcRedactURL(raw + "&extra=1"); !strings.HasSuffix(got, "&extra=1") || strings.Contains(got, "SAPISIDHASH") {
		t.Errorf("末尾还有参数时脱敏不对: %s", got)
	}
	if got := wcRedactURL("https://x/?VER=8"); got != "https://x/?VER=8" {
		t.Errorf("没 $httpHeaders 的 URL 不该动: %s", got)
	}
	// 实际发送的 URL 原样不动：raw 本身必须还含完整 headers（脱敏只作用于错误呈现）。
	if !strings.Contains(raw, url.QueryEscape("X-Goog-Api-Key:"+apiKey)) {
		t.Fatal("前提不成立：构造的握手 URL 没带 API key")
	}

	// 会话值 SID / gsessionid 同样要脱敏：下行/上行 URL 把它们放在裸 query 里。
	sessURL := wcEndpoint + "?gsessionid=GSESSION_PLACEHOLDER&SID=SID_PLACEHOLDER&VER=8&RID=rpc&AID=3&CI=0"
	redSess := wcRedactURL(sessURL)
	for _, leak := range []string{"GSESSION_PLACEHOLDER", "SID_PLACEHOLDER"} {
		if strings.Contains(redSess, leak) {
			t.Errorf("脱敏后的 URL 还含会话值 %q: %s", leak, redSess)
		}
	}
	for _, keep := range []string{"gsessionid=%3Credacted%3E", "SID=%3Credacted%3E", "VER=8", "RID=rpc", "AID=3"} {
		if !strings.Contains(redSess, keep) {
			t.Errorf("脱敏后的 URL 丢了 %q: %s", keep, redSess)
		}
	}
	// 参数名按 name= 精确匹配：X-HTTP-Session-Id=gsessionid 的值恰好等于字符串
	// "gsessionid"，但它不是 gsessionid 参数，不能被误伤。
	if !strings.Contains(wcRedactURL(raw), "X-HTTP-Session-Id=gsessionid") {
		t.Errorf("X-HTTP-Session-Id=gsessionid 被误脱敏: %s", wcRedactURL(raw))
	}
	// 同名参数出现多次时全部替换。
	dup := wcEndpoint + "?SID=ONE_PLACEHOLDER&zx=abc&SID=TWO_PLACEHOLDER&t=1"
	redDup := wcRedactURL(dup)
	if strings.Contains(redDup, "ONE_PLACEHOLDER") || strings.Contains(redDup, "TWO_PLACEHOLDER") {
		t.Errorf("重复的 SID 没全部脱敏: %s", redDup)
	}
	if strings.Count(redDup, "SID=%3Credacted%3E") != 2 {
		t.Errorf("期望两个 SID=<redacted>: %s", redDup)
	}
}

// 传输层失败走代理分支（stdlib）时，*url.Error 里不能再见到凭据；
// Op 和底层错误要保留，Timeout() 之类行为不变。
func TestWCSendRedactsURLError(t *testing.T) {
	const apiKey = "AIzaTESTKEY00000000000000000000000aa"
	r := wcRequest{HexID: "fbb127bbb056c959", Mode: 1, Cookie: "SAPISID=x", SAPISID: "x"}
	hsURL := fmt.Sprintf("%s?VER=8&RID=10001&CVER=22&X-HTTP-Session-Id=gsessionid&$httpHeaders=%s&zx=abc123&t=1",
		wcEndpoint, url.QueryEscape(wcHTTPHeaders(apiKey, r)))

	fail := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: connection refused")
	})
	const fakeProxy = "http://wcredact.invalid:3128"
	stdlibClientCache.Store(fakeProxy+"|"+fmt.Sprint(rtCfg().RequestTimeout), &http.Client{Transport: fail})
	defer stdlibClientCache.Delete(fakeProxy + "|" + fmt.Sprint(rtCfg().RequestTimeout))

	sess := "gsessionid=GSESSION_PLACEHOLDER&SID=SID_PLACEHOLDER"
	getURL := fmt.Sprintf("%s?%s&VER=8&RID=rpc&AID=0&CI=0&TYPE=xmlhttp&zx=abc123&t=1", wcEndpoint, sess)

	_, err := wcSend(context.Background(), "POST", hsURL, "count=0", "SAPISID=x", fakeProxy)
	if err == nil {
		t.Fatal("传输层失败应报错")
	}
	msg := err.Error()
	for _, leak := range []string{"SAPISIDHASH", apiKey, url.QueryEscape(apiKey), "$httpHeaders=" + url.QueryEscape(wcHTTPHeaders(apiKey, r))} {
		if strings.Contains(msg, leak) {
			t.Errorf("错误信息泄露 %q: %s", leak, msg)
		}
	}
	// 会话值也不能出现在错误里：下行 GET 的 URL 带着 SID/gsessionid。
	_, err = wcSend(context.Background(), "GET", getURL, "", "SAPISID=x", fakeProxy)
	if err == nil {
		t.Fatal("传输层失败应报错")
	}
	sessMsg := err.Error()
	if strings.Contains(sessMsg, "SID_PLACEHOLDER") || strings.Contains(sessMsg, "GSESSION_PLACEHOLDER") {
		t.Errorf("错误信息泄露会话值: %s", sessMsg)
	}
	for _, keep := range []string{"SID=%3Credacted%3E", "gsessionid=%3Credacted%3E", "RID=rpc"} {
		if !strings.Contains(sessMsg, keep) {
			t.Errorf("错误信息里 %q 应保留参数名并脱敏值: %s", keep, sessMsg)
		}
	}
	for _, keep := range []string{"geminiweb-pa.clients6.google.com", "VER=8", "connection refused"} {
		if !strings.Contains(msg, keep) {
			t.Errorf("错误信息丢了非敏感定位信息 %q: %s", keep, msg)
		}
	}
	var ue *url.Error
	if !errors.As(err, &ue) {
		t.Fatalf("应保留 *url.Error 类型便于调用方判断: %T", err)
	}
	if strings.Contains(ue.URL, "SAPISIDHASH") || !strings.Contains(ue.URL, "%3Credacted%3E") {
		t.Errorf("url.Error.URL 没脱敏: %s", ue.URL)
	}

	// 非 *url.Error 的错误按字符串脱敏。
	nonURL := wcRedactErr(fmt.Errorf("POST %s failed", hsURL))
	if strings.Contains(nonURL.Error(), "SAPISIDHASH") || strings.Contains(nonURL.Error(), apiKey) ||
		!strings.Contains(nonURL.Error(), "%3Credacted%3E") || !strings.Contains(nonURL.Error(), "VER=8") {
		t.Errorf("非 *url.Error 脱敏不对: %s", nonURL)
	}
	// 底层 error 里带 URL 原文的场景（tls-client 不包 *url.Error 时）；
	// 脱敏后错误链仍要保留，上层靠 errors.Is 判 context.Canceled 之类。
	wrapped := wcRedactErr(fmt.Errorf("request failed: %w", io.ErrUnexpectedEOF))
	if !strings.Contains(wrapped.Error(), "unexpected EOF") {
		t.Errorf("底层错误信息不该丢: %s", wrapped)
	}
	if !errors.Is(wrapped, io.ErrUnexpectedEOF) {
		t.Errorf("脱敏包装丢了错误链: %T", wrapped)
	}
	canceled := wcRedactErr(fmt.Errorf("get %s?SID=SID_PLACEHOLDER&gsessionid=GSESSION_PLACEHOLDER: %w", wcEndpoint, context.Canceled))
	if !errors.Is(canceled, context.Canceled) {
		t.Errorf("脱敏包装后 errors.Is(context.Canceled) 失效: %T", canceled)
	}
	if strings.Contains(canceled.Error(), "SID_PLACEHOLDER") || strings.Contains(canceled.Error(), "GSESSION_PLACEHOLDER") {
		t.Errorf("非 *url.Error 的错误消息泄露会话值: %s", canceled)
	}
}
