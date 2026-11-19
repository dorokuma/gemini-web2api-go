package app

import (
	"encoding/json"
	"fmt"
	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/google/uuid"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Explicitly opted-in local protocol probe. Does not log cookies, tokens, or raw account responses.
func TestArgonTimingProbe(t *testing.T) {
	cookie := os.Getenv("ARGON_PROBE_COOKIE")
	if cookie == "" {
		t.Skip("local authenticated probe not enabled")
	}
	dbPath := cfg.DBPath
	cfg = defaultConfig()
	cfg.DBPath = dbPath
	cfg.RequestTimeout = 65
	initRuntimeConfig()
	page, err := fetchAppPage(cookie, 0, "")
	if err != nil {
		t.Fatal("page fetch failed")
	}
	tok := snlm0eRe.FindSubmatch(page)
	if len(tok) < 2 {
		t.Fatal("login token unavailable")
	}
	bl := regexp.MustCompile(`"cfb2h":"([^"]+)"`).FindSubmatch(page)
	if len(bl) < 2 {
		t.Fatal("build version unavailable")
	}
	headers := buildGeminiHeaders(cookie, extractSAPISID(cookie), "")
	delete(headers, "x-goog-ext-525001261-jspb")
	freq, _ := json.Marshal([]any{[]any{[]any{"otAQ7b", "[]", nil, "generic"}}})
	form := url.Values{"f.req": {string(freq)}, "at": {string(tok[1])}}
	endpoint := "https://gemini.google.com/_/BardChatUi/data/batchexecute?rpcids=otAQ7b&hl=en&rt=c&bl=" + url.QueryEscape(string(bl[1]))
	status, _, catalog, err := uploadPost(endpoint, headers, []byte(form.Encode()), "")
	if err != nil {
		t.Fatal("catalog request failed")
	}
	t.Logf("catalog_status=%d argon_hex_present=%v argon_name_present=%v", status, strings.Contains(string(catalog), "95a4c0abcba24364"), strings.Contains(string(catalog), "Argon"))
	if !strings.Contains(string(catalog), "95a4c0abcba24364") {
		t.Fatal("Argon not confirmed in account catalog")
	}
	if os.Getenv("ARGON_PROBE_CATALOG_ONLY") == "1" {
		return
	}
	// 与 streamGenerateOnce/streamGenerateConv 同一套拼法：gemini-4-carbon 是对话模式
	// （TaskMode=false），走**纯普通 Chat inner**（不补 argonMode8Slots，实测三档均
	// HTTP 200 且准确自报）；inner[45] 由 temporary_chat 开关决定，探针默认配置开着
	// 临时对话，保持 inner[45]=1 不变。任务槽位由 gemini-4-carbon-task 承接。
	carbon := Models["gemini-4-carbon"]
	inner := make([]any, innerSlots)
	inner[0] = []any{"Explain how rain forms in 12 short numbered sentences. Begin directly with sentence 1.", 0, nil, nil, nil, nil, 0}
	inner[1] = []any{"en"}
	inner[2] = []any{"", "", "", nil, nil, nil, nil, nil, nil, ""}
	inner[6] = []any{0}
	inner[7] = 1
	inner[10] = 1
	inner[11] = 0
	inner[17] = []any{[]any{0}}
	inner[18] = 0
	inner[27] = 1
	inner[30] = []any{4}
	inner[41] = []any{1}
	inner[45] = 1
	inner[53] = 0
	inner[59] = uuid.NewString()
	inner[61] = []any{}
	inner[68] = 1
	inner[79] = carbon.Mode
	inner[80] = carbon.thinkWire()
	inner[91] = 0
	inner[96] = 0
	if carbon.TaskMode {
		if carbon.Mode == 8 {
			argonMode8Slots(inner)
		} else {
			taskModeSlots(inner)
		}
	}
	ib, _ := json.Marshal(inner)
	outer, _ := json.Marshal([]any{nil, string(ib)})
	form = url.Values{"f.req": {string(outer)}, "at": {string(tok[1])}}
	headers = buildGeminiHeaders(cookie, extractSAPISID(cookie), hexArgon4)
	headers["x-goog-ext-525001261-jspb"] = buildModelHeader(hexArgon4, carbon.Mode, carbon.thinkWire(), uuid.NewString())
	headers["x-goog-ext-525005358-jspb"] = fmt.Sprintf(`["%s",1]`, inner[59])
	endpoint = "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate?hl=en&rt=c&bl=" + url.QueryEscape(string(bl[1]))
	start := time.Now()
	req, e := fhttp.NewRequest("POST", endpoint, strings.NewReader(form.Encode()))
	if e != nil {
		t.Fatal("request construction failed")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, e := getTLSClient().Do(req)
	if e != nil {
		t.Fatalf("transport failed (%T)", e)
	}
	defer resp.Body.Close()
	status = resp.StatusCode
	firstIdentity, firstText, firstReasoning, lastText := int64(-1), int64(-1), int64(-1), int64(-1)
	lineCount, textFrames := 0, 0
	lastContent := ""
	raw, firstLine, err := readBody(resp.Body, func(line string) error {
		lineCount++
		ms := time.Since(start).Milliseconds()
		if firstIdentity < 0 && validateArgonIdentity([]byte(line), hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)) == nil {
			firstIdentity = ms
		}
		if firstReasoning < 0 && reasoningInLine(line) != "" {
			firstReasoning = ms
		}
		texts := textsInLine(line)
		if len(texts) > 0 && strings.Join(texts, "\n") != lastContent {
			lastContent = strings.Join(texts, "\n")
			textFrames++
			lastText = ms
			if firstText < 0 {
				firstText = ms
			}
		}
		return nil
	}, start)
	t.Logf("TIMING first_line_ms=%d first_identity_ms=%d first_text_ms=%d first_reasoning_ms=%d last_text_ms=%d complete_ms=%d lines=%d text_updates=%d", firstLine, firstIdentity, firstText, firstReasoning, lastText, time.Since(start).Milliseconds(), lineCount, textFrames)
	if err != nil {
		t.Fatalf("generation transport failed (%T)", err)
	}
	actual := extractUpstreamModel(string(raw))
	answer := extractResponseText(string(raw))
	// 身份断言与运行时同槽白名单对齐：普通档 Carbon 收 "4 Argon"/"4 Carbon"（原位改名
	// 滚动期两套都收），不再硬判名字含 "argon"。
	names := argonDisplayNames(hexArgon4, carbon.thinkWire())
	nameOK := false
	for _, n := range names {
		if actual == n {
			nameOK = true
		}
	}
	t.Logf("generation_status=%d mode=8 think=1 actual_model=%q answer_bytes=%d elapsed_ms=%d raw_bytes=%d", status, actual, len(answer), time.Since(start).Milliseconds(), len(raw))
	if status != 200 || !nameOK || strings.TrimSpace(answer) == "" {
		t.Fatalf("Carbon generation not verified: status=%d actual=%q want one of %v", status, actual, names)
	}
}
