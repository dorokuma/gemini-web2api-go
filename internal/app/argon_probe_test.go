package app

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Explicitly opted-in local protocol probe. Does not log cookies, tokens, or raw account responses.
func TestArgonProtocolProbe(t *testing.T) {
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
	inner := make([]any, 97)
	inner[0] = []any{"Reply with exactly ARGON_OK.", 0, nil, nil, nil, nil, 0}
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
	inner[79] = 8
	inner[80] = 1
	inner[91] = 0
	inner[96] = 0
	ib, _ := json.Marshal(inner)
	outer, _ := json.Marshal([]any{nil, string(ib)})
	form = url.Values{"f.req": {string(outer)}, "at": {string(tok[1])}}
	headers = buildGeminiHeaders(cookie, extractSAPISID(cookie), "95a4c0abcba24364")
	headers["x-goog-ext-525001261-jspb"] = buildModelHeader("95a4c0abcba24364", 8, 1, uuid.NewString())
	headers["x-goog-ext-525005358-jspb"] = fmt.Sprintf(`["%s",1]`, inner[59])
	endpoint = "https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate?hl=en&rt=c&bl=" + url.QueryEscape(string(bl[1]))
	start := time.Now()
	status, raw, _, _, err := doGeminiRequest(endpoint, form.Encode(), headers, "", nil)
	if err != nil {
		t.Fatalf("generation transport failed (%T)", err)
	}
	actual := extractUpstreamModel(string(raw))
	answer := extractResponseText(string(raw))
	t.Logf("generation_status=%d mode=8 think=1 actual_model=%q answer=%q elapsed_ms=%d raw_bytes=%d", status, actual, answer, time.Since(start).Milliseconds(), len(raw))
	if status != 200 || !strings.Contains(strings.ToLower(actual), "argon") || strings.TrimSpace(answer) != "ARGON_OK" {
		t.Fatal("Argon generation not verified")
	}
}
