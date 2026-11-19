package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func makeSyntheticArgonFrame(hexID string, modelName string, answer string) []byte {
	fields := make([]any, 43)
	if answer != "" {
		fields[4] = answer
	}
	if hexID != "" {
		fields[39] = hexID
		fields[40] = nil
		fields[41] = nil
		fields[42] = modelName
	}
	payloadBytes, _ := json.Marshal(fields)
	row := []any{"wrb.fr", "rpc1", string(payloadBytes), nil}
	rows := [][]any{row}
	lineBytes, _ := json.Marshal(rows)
	return append(lineBytes, '\n')
}

func TestArgonValidateIdentity(t *testing.T) {
	// 1. Correct hex and name passes (including upstream's hexFlashLite placeholder slot with Argon name)
	validFrame1 := makeSyntheticArgonFrame(hexArgon4, "4 Argon", "ARGON_OK")
	if err := validateArgonIdentity(validFrame1, hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)); err != nil {
		t.Fatalf("expected valid Argon frame to pass, got: %v", err)
	}

	validFrame2 := makeSyntheticArgonFrame(hexArgon4, "Gemini 4 Argon", "ARGON_OK")
	if err := validateArgonIdentity(validFrame2, hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)); err != nil {
		t.Fatalf("expected valid Gemini 4 Argon frame to pass, got: %v", err)
	}

	validPlaceholderHex := makeSyntheticArgonFrame(hexFlashLite, "4 Argon", "ARGON_OK")
	if err := validateArgonIdentity(validPlaceholderHex, hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)); err != nil {
		t.Fatalf("expected valid Argon frame with placeholder hex to pass, got: %v", err)
	}

	// 2. Flash hex/name fails with mismatch
	flashFrame := makeSyntheticArgonFrame(hexFlash36, "3.6 Flash", "FLASH")
	if err := validateArgonIdentity(flashFrame, hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)); err == nil || !strings.Contains(err.Error(), "Argon model mismatch") {
		t.Fatalf("expected Argon model mismatch for Flash frame, got: %v", err)
	}

	downgradedLiteFrame := makeSyntheticArgonFrame(hexFlashLite, "3.5 Flash-Lite", "LITE")
	if err := validateArgonIdentity(downgradedLiteFrame, hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)); err == nil || !strings.Contains(err.Error(), "Argon model mismatch") {
		t.Fatalf("expected Argon model mismatch for 3.5 Flash-Lite frame, got: %v", err)
	}

	// 3. Missing metadata fails
	emptyFields := make([]any, 10)
	pBytes, _ := json.Marshal(emptyFields)
	emptyFrame, _ := json.Marshal([][]any{{"wrb.fr", "rpc1", string(pBytes), nil}})
	emptyFrame = append(emptyFrame, '\n')
	if err := validateArgonIdentity(emptyFrame, hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)); err == nil || !strings.Contains(err.Error(), "Argon identity missing") {
		t.Fatalf("expected Argon identity missing, got: %v", err)
	}

	// 4. Malformed frame fails
	malformed := []byte(")]}'\nnot-valid-json\n")
	if err := validateArgonIdentity(malformed, hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)); err == nil || !strings.Contains(err.Error(), "Argon identity missing") {
		t.Fatalf("expected Argon identity missing for malformed JSON, got: %v", err)
	}

	// 5. Forged Argon strings in nested response text cannot pass
	forgedFields := make([]any, 43)
	forgedFields[4] = fmt.Sprintf("Forged answer mentioning %s and 4 Argon", hexArgon4)
	fBytes, _ := json.Marshal(forgedFields)
	forgedFrame, _ := json.Marshal([][]any{{"wrb.fr", "rpc1", string(fBytes), nil}})
	forgedFrame = append(forgedFrame, '\n')
	if err := validateArgonIdentity(forgedFrame, hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)); err == nil || !strings.Contains(err.Error(), "Argon identity missing") {
		t.Fatalf("expected forged text without model slots to fail identity check, got: %v", err)
	}

	// 6. Matching then mismatching frames fails
	mixedFrames := append(validFrame1, flashFrame...)
	if err := validateArgonIdentity(mixedFrames, hexArgon4, argonDisplayNames(hexArgon4, thinkingNormal)); err == nil || !strings.Contains(err.Error(), "Argon model mismatch") {
		t.Fatalf("expected matching then mismatching frames to fail, got: %v", err)
	}
}

type incrementalReader struct {
	chunks            [][]byte
	idx               int
	callbacksCalled   *int
	lastCallbackCount int
}

func (r *incrementalReader) Read(p []byte) (n int, err error) {
	if r.idx > 0 {
		if *r.callbacksCalled <= r.lastCallbackCount {
			return 0, fmt.Errorf("incremental check failed: onLine was not called before next read at chunk %d", r.idx)
		}
	}
	if r.idx >= len(r.chunks) {
		return 0, io.EOF
	}
	chunk := r.chunks[r.idx]
	r.lastCallbackCount = *r.callbacksCalled
	r.idx++
	copy(p, chunk)
	return len(chunk), nil
}

type truncatedReader struct {
	delivered bool
}

func (r *truncatedReader) Read(p []byte) (n int, err error) {
	if !r.delivered {
		r.delivered = true
		copy(p, []byte("data: partial\n"))
		return 14, nil
	}
	return 0, io.ErrUnexpectedEOF
}

func TestReadModelCheckedBody(t *testing.T) {
	argonHeader := fmt.Sprintf(`[1,null,null,null,"%s"]`, hexArgon4)
	flashHeader := fmt.Sprintf(`[1,null,null,null,"%s"]`, hexFlash36)

	// 1. Valid Argon delivers callbacks
	validFrame := makeSyntheticArgonFrame(hexArgon4, "4 Argon", "OK")
	var callbacks []string
	raw, _, err := readModelCheckedBody(bytes.NewReader(validFrame), func(line string) error {
		callbacks = append(callbacks, line)
		return nil
	}, time.Now(), 200, argonHeader)

	if err != nil {
		t.Fatalf("unexpected error for valid Argon body: %v", err)
	}
	if len(callbacks) == 0 {
		t.Fatal("expected callbacks for valid Argon body")
	}
	if !bytes.Equal(raw, validFrame) {
		t.Fatal("raw body does not match")
	}

	// 2. Mismatch/missing -> err and zero callbacks
	flashFrame := makeSyntheticArgonFrame(hexFlash36, "3.6 Flash", "OK")
	callbacks = nil
	_, _, err = readModelCheckedBody(bytes.NewReader(flashFrame), func(line string) error {
		callbacks = append(callbacks, line)
		return nil
	}, time.Now(), 200, argonHeader)

	if err == nil {
		t.Fatal("expected error on mismatched model for Argon header")
	}
	if len(callbacks) != 0 {
		t.Fatalf("expected 0 callbacks on mismatch, got %d", len(callbacks))
	}

	// 3. Non-200 Argon preserves status-body read with zero callbacks, no identity error
	errorBody := []byte(`{"error":"bad request"}` + "\n")
	callbacks = nil
	rawErr, _, err := readModelCheckedBody(bytes.NewReader(errorBody), func(line string) error {
		callbacks = append(callbacks, line)
		return nil
	}, time.Now(), 400, argonHeader)

	if err != nil {
		t.Fatalf("expected no error from readModelCheckedBody on non-200 status, got: %v", err)
	}
	if len(callbacks) != 0 {
		t.Fatalf("expected zero callbacks on non-200 status, got %d", len(callbacks))
	}
	if !bytes.Equal(rawErr, errorBody) {
		t.Fatal("expected status body to be preserved")
	}

	// 4. Ordinary model remains incremental
	callbackCount := 0
	incReader := &incrementalReader{
		chunks: [][]byte{
			[]byte("line 1\n"),
			[]byte("line 2\n"),
			[]byte("line 3\n"),
		},
		callbacksCalled: &callbackCount,
	}

	rawOrd, _, err := readModelCheckedBody(incReader, func(line string) error {
		callbackCount++
		return nil
	}, time.Now(), 200, flashHeader)

	if err != nil {
		t.Fatalf("incremental ordinary model read failed: %v", err)
	}
	if callbackCount != 3 {
		t.Fatalf("expected 3 incremental callbacks, got %d", callbackCount)
	}
	if !bytes.Equal(rawOrd, []byte("line 1\nline 2\nline 3\n")) {
		t.Fatal("raw body mismatch for ordinary model")
	}

	// 5. Truncated reader errors, no Argon callbacks
	callbacks = nil
	trunc := &truncatedReader{}
	_, _, err = readModelCheckedBody(trunc, func(line string) error {
		callbacks = append(callbacks, line)
		return nil
	}, time.Now(), 200, argonHeader)

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF, got: %v", err)
	}
	if len(callbacks) != 0 {
		t.Fatalf("expected zero callbacks on truncated read, got %d", len(callbacks))
	}
}

func TestArgonModelNeedsLoginAndAvailable(t *testing.T) {
	for _, name := range []string{"gemini-4-carbon", "gemini-4-neon"} {
		mc, ok := Models[name]
		if !ok {
			t.Fatalf("%s missing in Models map", name)
		}
		if !modelNeedsLogin(mc) {
			t.Fatalf("%s should require login", name)
		}
		// Without cookie: hidden
		if _, ok := availableModels()[name]; ok {
			t.Fatalf("%s should not be available without cookie", name)
		}
	}

	// With cookie: available
	withPoolCookie(t)
	for _, name := range []string{"gemini-4-carbon", "gemini-4-neon"} {
		if _, ok := availableModels()[name]; !ok {
			t.Fatalf("%s should be available when pool cookie exists", name)
		}
	}
}

// -thinking 版上游自报 "4 Carbon High"（2026-11-13 起又回裸 "4 Carbon"，滚动期两套都收）：
// 按请求 header 的思考位只认对应档位的名字，档位被静默换掉（要 High 回了普通档，或反过来）
// 同样报身份不符。
func TestArgonIdentityByThinkTier(t *testing.T) {
	low := Models["gemini-4-carbon"]
	med := Models["gemini-4-carbon-thinking-medium"]
	high := Models["gemini-4-carbon-thinking"]
	if low.Thinking || !high.Thinking || high.HexID != hexArgon4 || high.Mode != 8 || med.ThinkWire != thinkingMedium {
		t.Fatalf("unexpected Carbon model table: low=%+v med=%+v high=%+v", low, med, high)
	}
	lowHeader := buildModelHeader(low.HexID, low.Mode, thinkingNormal, "uuid-x")
	medHeader := buildModelHeader(med.HexID, med.Mode, thinkingMedium, "uuid-x")
	highHeader := buildModelHeader(high.HexID, high.Mode, thinkingExtended, "uuid-x")

	cases := []struct {
		header, name string
		ok           bool
	}{
		{lowHeader, "4 Argon", true},
		{lowHeader, "4 Carbon", true},
		{medHeader, "4 Argon", true},
		{medHeader, "4 Argon Medium", true},
		{medHeader, "4 Carbon", true},
		{medHeader, "4 Carbon Medium", true},
		{highHeader, "4 Argon High", true},
		{highHeader, "Gemini 4 Argon High", true},
		{highHeader, "4 Carbon High", true},
		{highHeader, "Gemini 4 Carbon High", true},
		// 2026-11-13 起 High 档自报脱后缀回裸 "4 Carbon"；放行。
		{highHeader, "4 Carbon", true},
		{lowHeader, "4 Argon High", false},
		{lowHeader, "4 Carbon High", false},
		{medHeader, "4 Argon High", false},
		{medHeader, "4 Carbon High", false},
		{highHeader, "4 Argon", false},
		{highHeader, "4 Argon Medium", false},
		{lowHeader, "4 Neon", false},
		{highHeader, "4 Neon High", false},
	}
	for _, c := range cases {
		frame := makeSyntheticArgonFrame(hexArgon4, c.name, "ok")
		var got []string
		_, _, err := readModelCheckedBody(bytes.NewReader(frame), func(l string) error {
			got = append(got, l)
			return nil
		}, time.Now(), 200, c.header)
		if c.ok && (err != nil || len(got) == 0) {
			t.Errorf("%q with header %s: want pass, err=%v lines=%d", c.name, c.header, err, len(got))
		}
		if !c.ok && (err == nil || !strings.Contains(err.Error(), "Argon model mismatch") || len(got) != 0) {
			t.Errorf("%q with header %s: want mismatch and no output, err=%v lines=%d", c.name, c.header, err, len(got))
		}
		if _, _, err := readModelCheckedBody(bytes.NewReader(frame), nil, time.Now(), 200, c.header); (err == nil) != c.ok {
			t.Errorf("%q non-stream with header %s: ok=%v err=%v", c.name, c.header, c.ok, err)
		}
	}
}

func TestArgonIdentityWithStrictModel(t *testing.T) {
	setStrictModel(t, true)
	low := Models["gemini-4-carbon"]
	med := Models["gemini-4-carbon-thinking-medium"]
	high := Models["gemini-4-carbon-thinking"]

	type tc struct {
		mc             ModelConfig
		name           string
		wantPass       bool
		wantDowngraded bool
	}
	cases := []tc{
		{low, "4 Argon", true, false},
		{low, "Gemini 4 Argon", true, false},
		{low, "4 Carbon", true, false},
		{low, "Gemini 4 Carbon", true, false},
		{med, "4 Argon", true, false},
		{med, "Gemini 4 Argon", true, false},
		{med, "4 Argon Medium", true, false},
		{med, "Gemini 4 Argon Medium", true, false},
		{med, "4 Carbon", true, false},
		{med, "4 Carbon Medium", true, false},
		{high, "4 Argon High", true, false},
		{high, "Gemini 4 Argon High", true, false},
		{high, "4 Carbon High", true, false},
		{high, "Gemini 4 Carbon High", true, false},
		{high, "4 Carbon", true, false},
		// Tier mismatch within Argon/Carbon: rejected by identity check, NOT classified as model downgrade cooldown.
		{low, "4 Argon High", false, false},
		{low, "Gemini 4 Argon High", false, false},
		{low, "4 Carbon High", false, false},
		{med, "4 Argon High", false, false},
		{med, "Gemini 4 Argon High", false, false},
		{med, "4 Carbon High", false, false},
		{high, "4 Argon", false, false},
		{high, "Gemini 4 Argon", false, false},
		{high, "4 Argon Medium", false, false},
		{high, "4 Carbon Medium", false, false},
		// Other model family: identity check rejects; under strict_model a true
		// downgrade (different model self-report) becomes ModelDowngradedError.
		{low, "4 Neon", false, true},
		{low, "4 Neon High", false, true},
		// Silent downgrade to another model family: rejected and converted to ModelDowngradedError under strict_model.
		{low, "3.5 Flash-Lite", false, true},
		{low, "Gemini 3.5 Flash-Lite", false, true},
		{med, "3.5 Flash-Lite", false, true},
		{high, "3.6 Flash", false, true},
	}

	for _, c := range cases {
		header := buildModelHeader(c.mc.HexID, c.mc.Mode, c.mc.thinkWire(), "uuid-x")
		frame := makeSyntheticArgonFrame(hexFlashLite, c.name, "ok")

		// Check both the streamGenerateOnce (newLineHandler) and streamGenerateConv (lineCB) pipelines.
		for _, mode := range []string{"once", "conv"} {
			strictCheck := strictLineCheck(c.mc, func() int64 { return 42 })
			var cb func(string) error
			if mode == "once" {
				lh := newLineHandler(nil, nil, nil, strictCheck)
				cb = lh.cb
			} else {
				cb = func(line string) error {
					if strictCheck != nil {
						if err := strictCheck(line); err != nil {
							return err
						}
					}
					return nil
				}
			}

			_, _, err := readModelCheckedBody(bytes.NewReader(frame), cb, time.Now(), 200, header)
			var aie *argonIdentityError
			if strictCheck != nil && errors.As(err, &aie) && modelDowngraded(c.mc, aie.name) {
				err = &ModelDowngradedError{Requested: c.mc.Upstream, Reported: aie.name, AccountID: 42}
			}

			if c.wantPass {
				if err != nil {
					t.Errorf("[%s] %q (think=%d): expected pass under strict_model, got %v", mode, c.name, c.mc.thinkWire(), err)
				}
				continue
			}
			if err == nil {
				t.Errorf("[%s] %q (think=%d): expected error under strict_model, got nil", mode, c.name, c.mc.thinkWire())
				continue
			}
			var mde *ModelDowngradedError
			isDowngraded := errors.As(err, &mde)
			if isDowngraded != c.wantDowngraded {
				t.Errorf("[%s] %q (think=%d): wantDowngraded=%v, got err=%T (%v)", mode, c.name, c.mc.thinkWire(), c.wantDowngraded, err, err)
			}
		}
	}
}

// 上游 2026-11 原位改名 "4 Argon" → "4 Carbon"（同 hex、同 Mode=8）：同档名字放行，
// 跨档仍拒，降级到 Flash 族仍拒；argon 请求收到 carbon 自报不算降级（改名滚动期）。
func TestArgonCarbonIdentity(t *testing.T) {
	low := Models["gemini-4-carbon"]
	med := Models["gemini-4-carbon-thinking-medium"]
	high := Models["gemini-4-carbon-thinking"]
	if low.HexID != hexArgon4 || low.Mode != 8 || low.Thinking || low.ThinkWire != 0 ||
		med.HexID != hexArgon4 || med.Mode != 8 || med.ThinkWire != thinkingMedium || med.Thinking ||
		high.HexID != hexArgon4 || high.Mode != 8 || !high.Thinking {
		t.Fatalf("unexpected Carbon model table: low=%+v med=%+v high=%+v", low, med, high)
	}
	if !modelNeedsLogin(low) || !modelNeedsLogin(med) || !modelNeedsLogin(high) {
		t.Fatal("carbon models should require login (same hexArgon4 slot)")
	}

	lowHeader := buildModelHeader(low.HexID, low.Mode, thinkingNormal, "uuid-x")
	medHeader := buildModelHeader(med.HexID, med.Mode, thinkingMedium, "uuid-x")
	highHeader := buildModelHeader(high.HexID, high.Mode, thinkingExtended, "uuid-x")
	cases := []struct {
		header, name string
		ok           bool
	}{
		{lowHeader, "4 Carbon", true},
		{lowHeader, "Gemini 4 Carbon", true},
		{medHeader, "4 Carbon", true},
		{medHeader, "4 Carbon Medium", true},
		{medHeader, "Gemini 4 Carbon Medium", true},
		{highHeader, "4 Carbon High", true},
		{highHeader, "Gemini 4 Carbon High", true},
		// 2026-11-13 起上游对 High 档自报脱了 "High" 后缀、回裸 "4 Carbon"；放行。
		{highHeader, "4 Carbon", true},
		{highHeader, "Gemini 4 Carbon", true},
		{medHeader, "4 Carbon High", false},
		{lowHeader, "4 Carbon High", false},
		{lowHeader, "4 Carbon Medium", false},
		{lowHeader, "3.5 Flash-Lite", false},
		{lowHeader, "3.6 Flash", false},
	}
	for _, c := range cases {
		frame := makeSyntheticArgonFrame(hexArgon4, c.name, "ok")
		var got []string
		_, _, err := readModelCheckedBody(bytes.NewReader(frame), func(l string) error {
			got = append(got, l)
			return nil
		}, time.Now(), 200, c.header)
		if c.ok && (err != nil || len(got) == 0) {
			t.Errorf("%q: want pass, err=%v lines=%d", c.name, err, len(got))
		}
		if !c.ok && (err == nil || !strings.Contains(err.Error(), "Argon model mismatch") || len(got) != 0) {
			t.Errorf("%q: want mismatch and no output, err=%v lines=%d", c.name, err, len(got))
		}
	}

	// strict_model 判降级：argon/carbon 是同一个槽位的两个名字，互报不算降级；
	// 掉到 Flash 族仍算降级。
	for _, c := range []struct {
		model, reported string
		want            bool
	}{
		{"gemini-4-carbon", "4 Argon", false},
		{"gemini-4-carbon", "4 Argon High", false},
		{"gemini-4-carbon-thinking", "4 Argon", false},
		{"gemini-4-carbon-thinking", "4 Argon High", false},
		{"gemini-4-carbon", "4 Carbon", false},
		{"gemini-4-carbon", "4 Carbon High", false},
		{"gemini-4-carbon-thinking", "4 Carbon High", false},
		{"gemini-4-carbon-thinking-medium", "4 Carbon Medium", false},
		{"gemini-4-carbon", "3.5 Flash-Lite", true},
		{"gemini-4-carbon-thinking", "3.6 Flash", true},
		{"gemini-4-carbon", "4 Neon", true},
		{"gemini-4-carbon", "4 Hydrogen", true},
	} {
		if got := modelDowngraded(Models[c.model], c.reported); got != c.want {
			t.Errorf("modelDowngraded(%s, %q)=%v want %v", c.model, c.reported, got, c.want)
		}
	}
}

// 4 Neon（hexNeon4 / Mode=3）：上游实测 think=1/2/4 自报 "4 Neon" / "4 Neon High" /
// "4 Neon Medium"（高/中档**带后缀**，与 Carbon High 档脱后缀相反），三档都不吐思考链。
// 身份核验必须：同一 hex 槽位只认对应档名的显示名、收 hexFlashLite 占位 hex、
// Carbon/Flash 等异族或错档自报一律拒。
func TestNeonIdentityByThinkTier(t *testing.T) {
	low := Models["gemini-4-neon"]
	med := Models["gemini-4-neon-thinking-medium"]
	high := Models["gemini-4-neon-thinking"]
	if low.HexID != hexNeon4 || low.Mode != 3 || low.Thinking || low.ThinkWire != 0 ||
		med.HexID != hexNeon4 || med.Mode != 3 || med.ThinkWire != thinkingMedium || med.Thinking ||
		high.HexID != hexNeon4 || high.Mode != 3 || !high.Thinking || high.thinkWire() != thinkingExtended {
		t.Fatalf("unexpected Neon model table: low=%+v med=%+v high=%+v", low, med, high)
	}
	if !modelNeedsLogin(low) || !modelNeedsLogin(med) || !modelNeedsLogin(high) {
		t.Fatal("neon models should require login (hexNeon4 slot)")
	}

	lowHeader := buildModelHeader(low.HexID, low.Mode, thinkingNormal, "uuid-x")
	medHeader := buildModelHeader(med.HexID, med.Mode, thinkingMedium, "uuid-x")
	highHeader := buildModelHeader(high.HexID, high.Mode, thinkingExtended, "uuid-x")
	cases := []struct {
		header, name string
		ok           bool
	}{
		{lowHeader, "4 Neon", true},
		{lowHeader, "Gemini 4 Neon", true},
		{medHeader, "4 Neon Medium", true},
		{medHeader, "Gemini 4 Neon Medium", true},
		{highHeader, "4 Neon High", true},
		{highHeader, "Gemini 4 Neon High", true},
		// 档位被静默换掉同样算身份不符（Neon 三档名字不互通）。
		{lowHeader, "4 Neon High", false},
		{lowHeader, "4 Neon Medium", false},
		{medHeader, "4 Neon", false},
		{medHeader, "4 Neon High", false},
		{highHeader, "4 Neon", false},
		{highHeader, "4 Neon Medium", false},
		// 异族自报一律拒。
		{lowHeader, "4 Carbon", false},
		{lowHeader, "4 Argon", false},
		{medHeader, "4 Carbon Medium", false},
		{highHeader, "4 Carbon High", false},
		{lowHeader, "4 Hydrogen", false},
		{lowHeader, "3.5 Flash-Lite", false},
		{lowHeader, "3.6 Flash", false},
	}
	for _, c := range cases {
		// 响应帧 hex 槽位允许 hexNeon4 本身或 hexFlashLite 占位。
		for _, id := range []string{hexNeon4, hexFlashLite} {
			frame := makeSyntheticArgonFrame(id, c.name, "ok")
			var got []string
			_, _, err := readModelCheckedBody(bytes.NewReader(frame), func(l string) error {
				got = append(got, l)
				return nil
			}, time.Now(), 200, c.header)
			if c.ok && (err != nil || len(got) == 0) {
				t.Errorf("%q id=%s: want pass, err=%v lines=%d", c.name, id, err, len(got))
			}
			if !c.ok && (err == nil || !strings.Contains(err.Error(), "Argon model mismatch") || len(got) != 0) {
				t.Errorf("%q id=%s: want mismatch and no output, err=%v lines=%d", c.name, id, err, len(got))
			}
			if _, _, err := readModelCheckedBody(bytes.NewReader(frame), nil, time.Now(), 200, c.header); (err == nil) != c.ok {
				t.Errorf("%q id=%s non-stream: ok=%v err=%v", c.name, id, c.ok, err)
			}
		}
	}

	// 异族 hex（Carbon）就算名字是 Neon 也得拒：hex 槽与显示名都核对。
	foreignHex := makeSyntheticArgonFrame(hexArgon4, "4 Neon", "ok")
	if _, _, err := readModelCheckedBody(bytes.NewReader(foreignHex), nil, time.Now(), 200, lowHeader); err == nil {
		t.Error("foreign hexArgon4 id with '4 Neon' name must be rejected for a neon header")
	}

	// strict_model 判降级：Neon 三档自报互认档位名（前缀规则），掉到异族/Flash 判降级。
	setStrictModel(t, true)
	for _, c := range []struct {
		model, reported string
		want            bool
	}{
		{"gemini-4-neon", "4 Neon", false},
		{"gemini-4-neon", "Gemini 4 Neon", false},
		{"gemini-4-neon-thinking", "4 Neon High", false},
		{"gemini-4-neon-thinking", "Gemini 4 Neon High", false},
		{"gemini-4-neon-thinking-medium", "4 Neon Medium", false},
		{"gemini-4-neon", "4 Neon High", false},
		{"gemini-4-neon-thinking-medium", "4 Neon High", false}, // 中→高同槽升级不算降级（strict_model 不记冷却）
		{"gemini-4-neon-thinking", "4 Neon Medium", true},       // 高→中仍算降级
		{"gemini-4-neon-thinking", "4 Neon", true},              // High 档收到裸名算降级（少后缀）
		{"gemini-4-neon", "4 Carbon", true},
		{"gemini-4-neon", "4 Argon", true},
		{"gemini-4-neon", "4 Hydrogen", true},
		{"gemini-4-neon", "3.5 Flash-Lite", true},
		{"gemini-4-neon-thinking", "3.6 Flash", true},
		{"gemini-4-neon-thinking-medium", "3.5 Flash-Lite", true},
	} {
		if got := modelDowngraded(Models[c.model], c.reported); got != c.want {
			t.Errorf("modelDowngraded(%s, %q)=%v want %v", c.model, c.reported, got, c.want)
		}
	}
}

func TestTokenizerLoaderTimeout(t *testing.T) {
	t.Setenv("TIKTOKEN_CACHE_DIR", t.TempDir())
	prevTimeout := tokenizerHTTPTimeout
	tokenizerHTTPTimeout = 80 * time.Millisecond
	defer func() { tokenizerHTTPTimeout = prevTimeout }()

	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-unblock
	}))
	defer func() {
		close(unblock)
		srv.Close()
	}()

	loader := &boundedBpeLoader{}
	start := time.Now()
	_, err := loader.LoadTiktokenBpe(srv.URL + "/cl100k_base.tiktoken")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error from hanging tokenizer download")
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("tokenizer download took %v, expected bounded timeout", elapsed)
	}
}

func TestAdminCookiesBindProxyBeforeCheck(t *testing.T) {
	getDB()
	_, _ = getDB().Exec(`DELETE FROM proxies`)
	_, _ = getDB().Exec(`DELETE FROM accounts`)
	t.Cleanup(func() {
		_, _ = getDB().Exec(`DELETE FROM proxies`)
		_, _ = getDB().Exec(`DELETE FROM accounts`)
		loadProxies()
	})

	idA, err := proxyCreate("proxy-a", "http://127.0.0.1:18080", 1)
	if err != nil {
		t.Fatalf("proxyCreate A: %v", err)
	}
	idB, err := proxyCreate("proxy-b", "http://127.0.0.1:18081", 1)
	if err != nil {
		t.Fatalf("proxyCreate B: %v", err)
	}

	reqBody, _ := json.Marshal(map[string]any{
		"label":    "Magpie",
		"cookie":   "SID=s; HSID=h; SSID=ss; APISID=a; SAPISID=sap; __Secure-1PSID=p",
		"note":     "Added by Magpie",
		"proxy_id": idB,
	})
	req := httptest.NewRequest(http.MethodPost, "/admin/api/cookies", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	handleAdminCookies(rec, req)
	if rec.Code != 200 {
		t.Fatalf("POST /admin/api/cookies status=%d body=%s", rec.Code, rec.Body.String())
	}
	var added struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &added); err != nil || added.ID <= 0 {
		t.Fatalf("unexpected response: %s", rec.Body.String())
	}

	accts := accountList()
	if len(accts) != 1 || accts[0].ProxyID != idB {
		t.Fatalf("expected account bound to proxy B (%d), got %+v (proxy A=%d)", idB, accts, idA)
	}

	picked, ok, err := acquireSlot(accts[0].ProxyID)
	if !ok || err != nil {
		t.Fatalf("acquireSlot failed: ok=%v err=%v", ok, err)
	}
	slotRelease(picked.ID)
	if picked.ID != idB {
		t.Fatalf("first check slot picked proxy %d (%s), want proxy B %d", picked.ID, picked.Name, idB)
	}
}

// PR #71：mode-8（4 Argon/4 Carbon）inner 槽位补齐的防回归。
// 单轮（streamGenerateOnce）与多轮（streamGenerateConv）共用 argonMode8Slots，
// 这里直接断言该块的槽位值，以及它对非 mode-8、mode-8 各档的作用范围。
func TestArgonMode8Slots(t *testing.T) {
	// 非 mode-8 的 inner 不含补齐槽位：按 streamGenerateOnce/streamGenerateConv 的
	// 默认值拼一个 3.6-flash 的 inner，逐个核对 PR #71 动的槽位都停在原值。
	if innerSlots != 99 {
		t.Fatalf("innerSlots = %d, want 99（PR #71 为 inner[98] 扩容）", innerSlots)
	}
	flash := Models["gemini-3.6-flash"]
	inner := make([]any, innerSlots)
	inner[6] = []any{0}
	inner[30] = []any{4}
	inner[68] = 1
	inner[79] = flash.Mode
	inner[80] = flash.thinkWire()
	inner[91] = 0
	inner[96] = 0
	if flash.Mode == 8 {
		argonMode8Slots(inner)
	}
	if inner[49] != nil || inner[83] != nil || inner[98] != nil {
		t.Errorf("non-mode-8 inner 不应含补齐槽位: [49]=%v [83]=%v [98]=%v", inner[49], inner[83], inner[98])
	}
	if got := fmt.Sprint(inner[6]); got != "[0]" {
		t.Errorf("non-mode-8 inner[6]=%s want [0]", got)
	}
	if inner[68] != 1 || inner[91] != 0 {
		t.Errorf("non-mode-8 inner[68]=%v inner[91]=%v want 1/0", inner[68], inner[91])
	}

	// mode-8 补齐块按 PR #71 抓包逐槽核对；三档（普通/中/高）都走同一块。
	for _, name := range []string{"gemini-4-carbon", "gemini-4-carbon-thinking-medium", "gemini-4-carbon-thinking"} {
		mc := Models[name]
		inner := make([]any, innerSlots)
		inner[6] = []any{0}
		inner[30] = []any{4}
		inner[68] = 1
		inner[79] = mc.Mode
		inner[80] = mc.thinkWire()
		inner[91] = 0
		inner[96] = 0
		if mc.Mode == 8 {
			argonMode8Slots(inner)
		}
		if got := fmt.Sprint(inner[6]); got != "[1]" {
			t.Errorf("%s inner[6]=%s want [1]", name, got)
		}
		if got := fmt.Sprint(inner[30]); got != "[]" {
			t.Errorf("%s inner[30]=%s want []", name, got)
		}
		for i, want := range map[int]int{49: 40, 68: 2, 83: 1, 91: 1, 96: 1, 98: 1} {
			if inner[i] != want {
				t.Errorf("%s inner[%d]=%v want %d", name, i, inner[i], want)
			}
		}
		// 补齐块不改思考槽：inner[80] 仍是 thinkWire，header 档位不受影响。
		if inner[80] != mc.thinkWire() || inner[79] != 8 {
			t.Errorf("%s inner[79]=%v inner[80]=%v want 8/%d", name, inner[79], inner[80], mc.thinkWire())
		}
	}
}

// 4 Hydrogen：3.8 Flash 槽位（hexFlash38 / Mode=1）2026-11 原位升级。三档表项、登录门控、
// 以及 strict_model 降级判定（明确请求 Hydrogen 收到 Flash 族自报一律算降级；
// 旧配置请求 3.8 Flash 收到 4 Hydrogen 算同槽升级）。
func TestHydrogenModelsAndDowngrade(t *testing.T) {
	low := Models["gemini-4-hydrogen"]
	med := Models["gemini-4-hydrogen-thinking-medium"]
	high := Models["gemini-4-hydrogen-thinking"]
	if low.HexID != hexFlash38 || low.Mode != 1 || low.Upstream != "4 Hydrogen" || low.Thinking || low.ThinkWire != 0 ||
		med.HexID != hexFlash38 || med.Mode != 1 || med.ThinkWire != thinkingMedium || med.Thinking ||
		high.HexID != hexFlash38 || high.Mode != 1 || !high.Thinking ||
		Models["gemini-3.8-flash-thinking-medium"].ThinkWire != thinkingMedium {
		t.Fatalf("unexpected Hydrogen model table: low=%+v med=%+v high=%+v", low, med, high)
	}
	if !modelNeedsLogin(low) || !modelNeedsLogin(med) || !modelNeedsLogin(high) {
		t.Fatal("hydrogen models should require login (hexFlash38 slot)")
	}
	for _, name := range []string{"gemini-4-hydrogen", "gemini-4-hydrogen-thinking-medium", "gemini-4-hydrogen-thinking"} {
		if _, ok := availableModels()[name]; ok {
			t.Fatalf("%s should not be available without cookie", name)
		}
	}
	withPoolCookie(t)
	for _, name := range []string{"gemini-4-hydrogen", "gemini-4-hydrogen-thinking-medium", "gemini-4-hydrogen-thinking"} {
		if _, ok := availableModels()[name]; !ok {
			t.Fatalf("%s should be available when pool cookie exists", name)
		}
	}

	for _, c := range []struct {
		model, reported string
		want            bool
	}{
		{"gemini-4-hydrogen", "4 Hydrogen", false},
		{"gemini-4-hydrogen", "4 Hydrogen Medium", false},
		{"gemini-4-hydrogen", "4 Hydrogen High", false},
		{"gemini-4-hydrogen-thinking-medium", "4 Hydrogen Medium", false},
		{"gemini-4-hydrogen-thinking", "4 Hydrogen High", false},
		{"gemini-4-hydrogen-thinking", "Gemini 4 Hydrogen High", false},
		{"gemini-4-hydrogen", "3.8 Flash", true},
		{"gemini-4-hydrogen", "3.7 Flash", true},
		{"gemini-4-hydrogen", "3.6 Flash", true},
		{"gemini-4-hydrogen", "3.5 Flash-Lite", true},
		{"gemini-4-hydrogen-thinking", "3.8 Flash", true},
		// 旧配置仍请求 3.8 Flash，上游自报新名 = 同槽升级，不算降级；掉到其它 Flash 仍算。
		{"gemini-3.8-flash", "4 Hydrogen", false},
		{"gemini-3.8-flash", "4 Hydrogen High", false},
		{"gemini-3.8-flash", "Gemini 4 Hydrogen", false},
		{"gemini-3.8-flash-thinking", "4 Hydrogen High", false},
		{"gemini-3.8-flash", "3.6 Flash", true},
		{"gemini-3.8-flash", "3.5 Flash-Lite", true},
		// 反向：请求 Hydrogen 收到 3.8 Flash 不是升级。
		{"gemini-4-hydrogen", "Gemini 3.8 Flash", true},
	} {
		if got := modelDowngraded(Models[c.model], c.reported); got != c.want {
			t.Errorf("modelDowngraded(%s, %q)=%v want %v", c.model, c.reported, got, c.want)
		}
	}
}

// phantomCleanup（#phantom 幽灵会话根治）：带附件/Mode-8 时服务端无视 inner[45]=1
// 照样在侧栏落「打不开」的索引残片，旧版只在 AutoDeleteConversation && 200 成功时删，
// 取消/超时/出错的路径全漏。这里断言三点：
//  1. feed 能从 raw 帧里提取 cid（含「只有 cid 没有正文」的幽灵帧）；
//  2. 条件不满足（无 cookie / multi_turn）时 enabled=false，整体 no-op；
//  3. enabled 时 flush 会把收集到的 cid 各发一次 GzXR5e（用打桩的 deleteConversation 计）。
func TestPhantomCleanupGateAndCollect(t *testing.T) {
	frame := `[["wrb.fr","A","[null,[\"c_deadbeef1234abcd\"],null,4]",null,"generic"]]`
	// 纯文本（无附件）temporary_chat 下，服务端也可能回 cid —— 一并删无害。
	if got := convIDInLine(frame); got != "c_deadbeef1234abcd" {
		t.Fatalf("convIDInLine=%q want c_deadbeef1234abcd", got)
	}
	if got := extractConversationID(frame); got != "c_deadbeef1234abcd" {
		t.Fatalf("extractConversationID=%q", got)
	}

	// 条件关闭：multi_turn 开着时必须不动（会话留给续接）。
	saveRt := rtVal
	defer func() { rtMu.Lock(); rtVal = saveRt; rtMu.Unlock() }()
	setRT := func(temp, autodel, multi bool) {
		rtMu.Lock()
		rtVal.TemporaryChat, rtVal.AutoDeleteConversation, rtVal.MultiTurn = temp, autodel, multi
		rtMu.Unlock()
	}
	credsOf := func(cookie, xsrf string) func() (string, string, string, int) {
		return func() (string, string, string, int) { return cookie, "sa", xsrf, 0 }
	}
	setRT(true, false, true) // temporary+multi → multi 优先，关
	if p := phantomCleanupFunc(credsOf("ck", "xs"), ""); p.enabled {
		t.Fatal("multi_turn on should disable phantom cleanup")
	}
	setRT(true, false, false) // temporary only → 开
	p := phantomCleanupFunc(credsOf("ck", "xs"), "")
	if !p.enabled {
		t.Fatal("temporary_chat should enable phantom cleanup")
	}
	p.feed(frame)
	p.feed(`[["wrb.fr","A","[null,[\"c_deadbeef1234abcd\"],null]",null,"generic"]]` + "\n" + frame) // dup
	if len(p.cids) != 1 {
		t.Fatalf("dedup failed, cids=%v", p.cids)
	}
	p.feed("not a frame\nplain text")
	if len(p.cids) != 1 {
		t.Fatal("non-frame lines must not add cids")
	}
	// xsrf 缺失 → no-op（删不了，不浪费 RPC）。
	setRT(false, true, false)
	if p2 := phantomCleanupFunc(credsOf("ck", ""), ""); p2.enabled {
		t.Fatal("missing xsrf should disable")
	}

	// 真实 HTTP 回归（must-fix）：上游已写出带 c_xxx 的首帧，客户端随后取消。
	// doGeminiRequestStop 在 err != nil 时必须仍带回已读到的 raw，否则首帧里的
	// c_xxx 被丢、幽灵会话漏删。这里走代理分支（stdlib，httptest 服务器当上游）。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		io.WriteString(w, frame+"\n")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	status, raw, _, _, err := doGeminiRequestStop(ctx, "http://upstream.invalid/StreamGenerate",
		"f.req=x", map[string]string{}, srv.URL, func(string) error { return nil }, nil)
	if err == nil {
		t.Fatal("客户端取消应报错")
	}
	if status != 200 {
		t.Fatalf("首帧已响应，status=%d want 200", status)
	}
	// 回归断言：错误路径不能把已读到的 raw 丢成 nil。
	if got := extractConversationID(string(raw)); got != "c_deadbeef1234abcd" {
		t.Fatalf("错误路径丢了已读 raw：raw=%q cid=%q", truncate(string(raw), 120), got)
	}

	// feed + flush 走通到底：打桩 deleteConversationFn 确认 GzXR5e 会发出。
	setRT(true, false, false)
	deleted := make(chan string, 1)
	prevDel := deleteConversationFn
	deleteConversationFn = func(cid, cookie, sapisid, xsrf, proxy string, authuser int) {
		deleted <- cid
	}
	defer func() { deleteConversationFn = prevDel }()
	pc := phantomCleanupFunc(credsOf("ck", "xs"), "")
	pc.feed(string(raw))
	pc.flush()
	select {
	case cid := <-deleted:
		if cid != "c_deadbeef1234abcd" {
			t.Fatalf("GzXR5e 删的 cid=%q want c_deadbeef1234abcd", cid)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("flush 没有发出 GzXR5e 删除请求")
	}
}

// must-fix 回归：400+XSRF 自愈换 token 之后，phantomCleanup 收尾必须带新 token ——
// 旧版拿的是创建时快照的旧 token，自愈后取消/超时会让 GzXR5e 带着已被拒的旧 token
// 去删，幽灵照样残留。成功路径不再直调 deleteConversation：同一 cid 由
// defer phantomCleanup.flush() 删一次。
//
// 「恰好一次」计数必须下沉到假传输层：只数 deleteConversationFn 桩点的话，
// 成功分支若被改回直调 go deleteConversation(...)（不经过桩点），请求照样发出、
// 测试却假绿。所以在 RoundTripper 里按 rpcids=GzXR5e 拦截真实出站请求计数，
// 并校验它带的 XSRF（at=）与 Authorization（SAPISIDHASH 认证）—— 后者能直接
// 检出 cleanup 凭据闭包里 sapisid 被遮蔽成零值的 bug。桩点只作旁证保留。
//
// 出口侧不打真网络：streamGenerateOnce 走代理分支（stdlib 客户端），测试把该
// proxyURL 的 stdlibClientCache 换成走假 RoundTripper 的客户端，handler 按 URL
// 分派 —— /app 取 XSRF、StreamGenerate 先 400 再 200、/gg-dl 出 PNG。
func TestXSRFSelfHealPhantomCleanupAndSingleDelete(t *testing.T) {
	cid := "c_f00dd00df00dd00d"
	// 首帧只带 cid（幽灵帧）、次帧带图片 CDN 链让 collectImageURLs 命中，末帧出正文。
	frame := fmt.Sprintf(`[["wrb.fr","A","[null,[\"%s\"],null,4]",null,"generic"]]`, cid)
	imgLine := `[["wrb.fr","B","[null,null,null,[null,null,null,\"https://lh3.googleusercontent.com/gg-dl/abc\"]]",null,"generic"]]`

	// 正文帧：textsInLine 取 inner[4][i][1][j] 的字符串，构造同形状的候选回复块。
	// inner[1] 放 [cid, rid] 供 convIDInLine / parseConvIDs 提取（同 frame 行的 [1][0]）。
	contentFields := make([]any, 43)
	contentFields[1] = []any{cid, "r_1"}
	// 答案给长一点：textsInLine 对 <200 字节的行直接返回 nil，padding 凑够长度。
	contentFields[4] = []any{[]any{"rc_x", []any{"ok - a deliberately long answer body to pad the wrb.fr line beyond the 200-byte textsInLine minimum length gate"}}}
	cpayload, _ := json.Marshal(contentFields)
	crow, _ := json.Marshal([]any{[]any{"wrb.fr", "rpc1", string(cpayload), nil}})
	contentLine := string(crow)

	// 传输层证据：GzXR5e（删会话）出站请求的认证信息与 cid，桩点之外的第二见证。
	type delReq struct {
		at   string // form at=，flush 用的 XSRF
		auth string // Authorization 头，sapisid 为空时 buildGeminiHeaders 不会写它
		cid  string
	}
	delReqs := make(chan delReq, 4)

	var first atomic.Bool
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		var body []byte
		if req.Body != nil {
			body, _ = io.ReadAll(req.Body)
			_ = req.Body.Close()
		}
		resp := func(status int, body string) *http.Response {
			return &http.Response{
				StatusCode: status, Status: fmt.Sprintf("%d", status),
				Header:  http.Header{},
				Body:    io.NopCloser(strings.NewReader(body)),
				Request: req,
			}
		}
		if req.URL.Query().Get("rpcids") == "GzXR5e" || strings.Contains(string(body), `"GzXR5e"`) {
			d := delReq{auth: req.Header.Get("Authorization")}
			if vals, err := url.ParseQuery(string(body)); err == nil {
				d.at = vals.Get("at")
				// f.req 形状：[[["GzXR5e","[\"c_xxx\"]",null,"generic"]]]，防御式逐层取。
				var outer [][][]any
				if json.Unmarshal([]byte(vals.Get("f.req")), &outer) == nil &&
					len(outer) > 0 && len(outer[0]) > 0 && len(outer[0][0]) > 1 {
					var inner []string
					if s, _ := outer[0][0][1].(string); json.Unmarshal([]byte(s), &inner) == nil && len(inner) > 0 {
						d.cid = inner[0]
					}
				}
			}
			delReqs <- d
			return resp(200, `")']'\n[[\"wrb.fr\",\"GzXR5e\",null]]"`), nil
		}
		switch req.URL.Path {
		case "/app":
			// SNlM0e 形状是 <token>:<毫秒时间戳>，正则要求整段 ≥10 字符。
			return resp(200, `"SNlM0e":"tok-new-xyz:1700000000000","cfb2h":"boq_x_20260101.01_p1","qKIAYe":"pid","Ylro7b":"pctx"`), nil
		case "/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate":
			if !first.Swap(true) {
				return resp(400, `[["er",null,400,null,null,[["48448350",["xsrf","tok-new"]]]]]`), nil
			}
			return resp(200, ")]}'\n"+frame+"\n"+imgLine+"\n"+contentLine+"\n"), nil
		case "/gg-dl/abc=s0":
			r := resp(200, "PNGDATA")
			r.Header.Set("Content-Type", "image/png")
			return r, nil
		}
		t.Logf("未命中: %s %s body=%.200s", req.Method, req.URL.String(), body)
		return resp(404, ""), nil
	})
	const fakeProxy = "http://fake-exit.invalid:3128"
	stdlibClientCache.Store(fakeProxy+"|0", &http.Client{Transport: rt})
	stdlibClientCache.Store(fakeProxy+"|"+fmt.Sprint(rtCfg().RequestTimeout), &http.Client{Transport: rt})
	defer func() {
		stdlibClientCache.Delete(fakeProxy + "|0")
		stdlibClientCache.Delete(fakeProxy + "|" + fmt.Sprint(rtCfg().RequestTimeout))
	}()

	// 把池子里塞一条测试号，绑定走我们的"代理"。
	withPoolCookie(t)
	acct, ok := pickCookieAccount()
	if !ok {
		t.Fatal("池里没有号")
	}
	proxyMu.Lock()
	oldProxies := proxyCache
	proxyCache = []Proxy{{ID: 999, Name: "test", URL: fakeProxy, Enabled: true}}
	proxyMu.Unlock()
	defer func() { proxyMu.Lock(); proxyCache = oldProxies; proxyMu.Unlock() }()

	// token 桩：getXSRF 走缓存路径返回，不发真 HTTP。tok-old 是被上游拒的那个。
	cookie := acct.Cookie
	invalidateXSRF(cookie)
	xsrfMu.Lock()
	xsrfCache[xsrfKey(cookie, acct.AuthUser)] = xsrfEntry{token: "tok-old", fetched: time.Now()}
	xsrfMu.Unlock()

	// rtVal：测试进程里 initRuntimeConfig 没跑过，rtVal 是零值（RetryAttempts=0 会
	// 让整个重试循环一次都不跑直接空返回）。按默认值补重试预算，再开本用例要的开关。
	saveRt := rtVal
	defer func() { rtMu.Lock(); rtVal = saveRt; rtMu.Unlock() }()
	rtMu.Lock()
	rtVal.RetryAttempts, rtVal.RetryDelaySec = 3, 1
	rtVal.TemporaryChat, rtVal.AutoDeleteConversation, rtVal.MultiTurn = true, false, false
	rtMu.Unlock()

	// 打桩只作旁证（防绕过计数主要靠上面的传输层 delReqs）：透传调用真实
	// deleteConversation，让 GzXR5e 真打到假 RoundTripper 被计数；若有人把删除
	// 改成绕开 deleteConversationFn 的写法，桩点静默而传输层仍会计数。
	deleted := make(chan string, 4)
	prevDel := deleteConversationFn
	deleteConversationFn = func(cid, cookie, sapisid, xsrf, proxy string, authuser int) {
		deleted <- xsrf + "|" + cid
		prevDel(cid, cookie, sapisid, xsrf, proxy, authuser)
	}
	defer func() { deleteConversationFn = prevDel }()

	// 媒体工具（toolImage）走 fetchMediaArtifacts 的 /gg-dl 分支，把 PNG 捞回来。
	mc := ModelConfig{HexID: hexFlash36, Mode: 1, Tool: toolImage}
	res, err := streamGenerateOnce(context.Background(), "draw", promptCtx{Latest: "draw"}, mc, nil, nil, nil, nil, map[int64]bool{})
	if err != nil {
		t.Fatalf("streamGenerateOnce err=%v res=%+v", err, res)
	}
	if res == nil || len(res.Artifacts) == 0 {
		t.Fatalf("没取回产物: artifacts=%d mediaErr=%v raw=%.400s", len(res.Artifacts), res.MediaErr, res.Raw)
	}

	// 传输层证据是主判据：同一 cid 的 GzXR5e 出站请求恰好一次，且带自愈后的
	// 新 XSRF 与非空认证（sapisid 被遮蔽成零值时这里没有 Authorization）。
	select {
	case d := <-delReqs:
		if d.cid != cid {
			t.Fatalf("删的 cid=%q want %s", d.cid, cid)
		}
		if d.at != "tok-new-xyz:1700000000000" {
			t.Fatalf("用的 XSRF=%q want tok-new-xyz:1700000000000（旧 token 已被拒，flush 必须用新的）", d.at)
		}
		if !strings.HasPrefix(d.auth, "SAPISIDHASH ") {
			t.Fatalf("删除请求缺 Authorization（sapisid 没传下去？）: auth=%q", d.auth)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("flush 没有发出 GzXR5e 删除请求")
	}
	select {
	case extra := <-delReqs:
		t.Fatalf("同一 cid 的 GzXR5e 发了第二次: %+v", extra)
	case <-time.After(300 * time.Millisecond):
	}

	// 桩点旁证：现有实现下也应被命中一次、带新 token。
	select {
	case got := <-deleted:
		parts := strings.SplitN(got, "|", 2)
		if parts[1] != cid || parts[0] != "tok-new-xyz:1700000000000" {
			t.Fatalf("桩点记的删除=%q want %s|%s", got, "tok-new-xyz:1700000000000", cid)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("deleteConversationFn 桩点未命中（传输层已见请求，路径变了？）")
	}
}

// 任务模式（Task Mode）槽位防回归：taskModeSlots 按浏览器 Task Mode 抓包逐槽核对；
// 普通对话模型不触发、Mode=8（Carbon）仍只走 argonMode8Slots、两块不混用。
func TestTaskModeSlots(t *testing.T) {
	neonTask := Models["gemini-4-neon-task"]
	hydrogenTask := Models["gemini-4-hydrogen-task"]
	if neonTask.HexID != hexNeon4 || neonTask.Mode != 3 || !neonTask.TaskMode ||
		neonTask.thinkWire() != thinkingExtended || neonTask.Upstream != "4 Neon" {
		t.Fatalf("unexpected neon-task entry: %+v", neonTask)
	}
	if hydrogenTask.HexID != hexFlash38 || hydrogenTask.Mode != 1 || !hydrogenTask.TaskMode ||
		hydrogenTask.thinkWire() != thinkingExtended || hydrogenTask.Upstream != "4 Hydrogen" {
		t.Fatalf("unexpected hydrogen-task entry: %+v", hydrogenTask)
	}
	// 不注册 -task-thinking 变体。
	if _, ok := Models["gemini-4-neon-task-thinking"]; ok {
		t.Fatal("gemini-4-neon-task-thinking must not exist")
	}
	if _, ok := Models["gemini-4-hydrogen-task-thinking"]; ok {
		t.Fatal("gemini-4-hydrogen-task-thinking must not exist")
	}

	build := func(mc ModelConfig) []any {
		inner := make([]any, innerSlots)
		inner[6] = []any{0}
		inner[30] = []any{4}
		inner[68] = 1
		inner[79] = mc.Mode
		inner[80] = mc.thinkWire()
		inner[91] = 0
		inner[96] = 0
		if tw := mc.thinkWire(); tw != thinkingNormal {
			inner[96] = 1
		}
		if mc.Mode == 8 {
			argonMode8Slots(inner)
		}
		if mc.TaskMode && mc.Mode != 8 {
			taskModeSlots(inner)
		}
		if mc.Tool > 0 {
			inner[49] = mc.Tool
		}
		return inner
	}

	// TaskMode=true：逐槽核对抓包值，inner[80] 保持 think 线值不被覆盖。
	for _, name := range []string{"gemini-4-neon-task", "gemini-4-hydrogen-task"} {
		inner := build(Models[name])
		if got := fmt.Sprint(inner[6]); got != "[0]" {
			t.Errorf("%s inner[6]=%s want [0]", name, got)
		}
		if got := fmt.Sprint(inner[30]); got != "[4 27 28 16 19 12]" {
			t.Errorf("%s inner[30]=%s want [4 27 28 16 19 12]", name, got)
		}
		for i, want := range map[int]int{49: 40, 68: 1, 83: 1, 91: 1, 96: 1, 98: 1} {
			if inner[i] != want {
				t.Errorf("%s inner[%d]=%v want %d", name, i, inner[i], want)
			}
		}
		if inner[80] != thinkingExtended {
			t.Errorf("%s inner[80]=%v want %d（taskModeSlots 不得改思考槽）", name, inner[80], thinkingExtended)
		}
	}

	// TaskMode=false 的普通对话模型：补齐槽位一律不落。
	for _, name := range []string{"gemini-4-neon", "gemini-4-neon-thinking", "gemini-4-hydrogen"} {
		inner := build(Models[name])
		if inner[49] != nil || inner[83] != nil || inner[98] != nil {
			t.Errorf("%s 非任务模型不应含任务槽位: [49]=%v [83]=%v [98]=%v", name, inner[49], inner[83], inner[98])
		}
	}

	// Mode=8 仍只走 argonMode8Slots：inner[68]=2（任务模式为 1），两块不混用。
	carbon := Models["gemini-4-carbon"]
	inner := build(carbon)
	if inner[68] != 2 || inner[49] != 40 {
		t.Errorf("carbon inner[68]=%v inner[49]=%v want 2/40", inner[68], inner[49])
	}
	// TaskMode 与 Mode=8 同设（防御）：Mode=8 优先，不叠任务槽。
	weird := ModelConfig{HexID: hexArgon4, Mode: 8, TaskMode: true}
	inner = build(weird)
	if inner[68] != 2 {
		t.Errorf("Mode=8+TaskMode inner[68]=%v want 2（argonMode8Slots 优先）", inner[68])
	}
}

// gemini-4-neon-task 身份核验：taskMode=true + hexNeon4 + think=2 收裸名 "4 Neon"
// 与 "4 Neon High"（未来上游恢复后缀仍兼容），拒异族/错档自报；
// taskMode=false（对话模式）同 hex + think=2 仍只认 High 名，绝不放宽。
func TestNeonTaskModeIdentity(t *testing.T) {
	neonTask := Models["gemini-4-neon-task"]
	header := buildModelHeader(neonTask.HexID, neonTask.Mode, neonTask.thinkWire(), "uuid-x")
	cases := []struct {
		name string
		ok   bool
	}{
		{"4 Neon", true},
		{"Gemini 4 Neon", true},
		{"4 Neon High", true},
		{"Gemini 4 Neon High", true},
		{"4 Neon Medium", false},
		{"4 Carbon", false},
		{"4 Argon", false},
		{"4 Hydrogen", false},
		{"3.5 Flash-Lite", false},
		{"3.6 Flash", false},
	}
	for _, c := range cases {
		for _, id := range []string{hexNeon4, hexFlashLite} {
			frame := makeSyntheticArgonFrame(id, c.name, "ok")
			var got []string
			_, _, err := readModelCheckedBodyMode(bytes.NewReader(frame), func(l string) error {
				got = append(got, l)
				return nil
			}, time.Now(), 200, header, true)
			if c.ok && (err != nil || len(got) == 0) {
				t.Errorf("%q id=%s: want pass, err=%v lines=%d", c.name, id, err, len(got))
			}
			if !c.ok && (err == nil || !strings.Contains(err.Error(), "Argon model mismatch") || len(got) != 0) {
				t.Errorf("%q id=%s: want mismatch and no output, err=%v lines=%d", c.name, id, err, len(got))
			}
			if _, _, err := readModelCheckedBodyMode(bytes.NewReader(frame), nil, time.Now(), 200, header, true); (err == nil) != c.ok {
				t.Errorf("%q id=%s non-stream: ok=%v err=%v", c.name, id, c.ok, err)
			}
		}
	}

	// taskMode=false + hexNeon4 + think=2：仍拒裸 "4 Neon"（对话模式规则不回归）。
	neonHigh := Models["gemini-4-neon-thinking"]
	highHeader := buildModelHeader(neonHigh.HexID, neonHigh.Mode, neonHigh.thinkWire(), "uuid-x")
	bareFrame := makeSyntheticArgonFrame(hexNeon4, "4 Neon", "ok")
	var got []string
	_, _, err := readModelCheckedBodyMode(bytes.NewReader(bareFrame), func(l string) error {
		got = append(got, l)
		return nil
	}, time.Now(), 200, highHeader, false)
	if err == nil || !strings.Contains(err.Error(), "Argon model mismatch") || len(got) != 0 {
		t.Errorf("taskMode=false think=2 + 裸 4 Neon: want mismatch and no output, err=%v lines=%d", err, len(got))
	}
	// 原入口 readModelCheckedBody（taskMode=false 薄封装）同样拒。
	if _, _, err := readModelCheckedBody(bytes.NewReader(bareFrame), nil, time.Now(), 200, highHeader); err == nil {
		t.Error("readModelCheckedBody(think=2 + 裸 4 Neon) must reject")
	}

	// 两个任务模型都要登录态。
	for _, name := range []string{"gemini-4-neon-task", "gemini-4-hydrogen-task"} {
		if !modelNeedsLogin(Models[name]) {
			t.Errorf("%s should require login", name)
		}
		if _, ok := availableModels()[name]; ok {
			t.Errorf("%s should not be available without cookie", name)
		}
	}
	withPoolCookie(t)
	for _, name := range []string{"gemini-4-neon-task", "gemini-4-hydrogen-task"} {
		if _, ok := availableModels()[name]; !ok {
			t.Errorf("%s should be available when pool cookie exists", name)
		}
	}

	// strict_model：任务模型按登记的 Upstream 判降级——自报 "4 Neon"/"4 Hydrogen"
	// 通过，掉到 Flash 族/异族正确识别为降级。
	setStrictModel(t, true)
	for _, c := range []struct {
		model, reported string
		want            bool
	}{
		{"gemini-4-neon-task", "4 Neon", false},
		{"gemini-4-neon-task", "Gemini 4 Neon", false},
		{"gemini-4-neon-task", "4 Neon High", false},
		{"gemini-4-neon-task", "3.5 Flash-Lite", true},
		{"gemini-4-neon-task", "3.6 Flash", true},
		{"gemini-4-neon-task", "4 Carbon", true},
		{"gemini-4-neon-task", "4 Hydrogen", true},
		{"gemini-4-hydrogen-task", "4 Hydrogen", false},
		{"gemini-4-hydrogen-task", "Gemini 4 Hydrogen", false},
		{"gemini-4-hydrogen-task", "3.5 Flash-Lite", true},
		{"gemini-4-hydrogen-task", "3.6 Flash", true},
		{"gemini-4-hydrogen-task", "4 Neon", true},
		{"gemini-4-hydrogen-task", "4 Carbon", true},
	} {
		if got := modelDowngraded(Models[c.model], c.reported); got != c.want {
			t.Errorf("modelDowngraded(%s, %q)=%v want %v", c.model, c.reported, got, c.want)
		}
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// strict_model 端到端：请求 Neon 中档、上游升成高档（自报 "4 Neon High"）时，
// 身份核验按错档拒绝（Neon 三档名字不互通），但不得转成 ModelDowngradedError、
// 也不得把账号写进 300 分钟降级冷却——那是同槽升级，不是降级。
func TestNeonMediumUpgradedToHighNotDowngraded(t *testing.T) {
	setStrictModel(t, true)
	rtMu.Lock()
	rtVal.StrictModelCooldownMin = 300
	rtMu.Unlock()
	resetDowngrades(t)
	med := Models["gemini-4-neon-thinking-medium"]
	header := buildModelHeader(med.HexID, med.Mode, med.thinkWire(), "uuid-x")
	frame := makeSyntheticArgonFrame(hexNeon4, "4 Neon High", "ok")

	// 与 streamGenerateOnce / streamGenerateConv 同一条链路：strictCheck 逐行判降级，
	// 身份核验抛 argonIdentityError 后按 modelDowngraded 决定要不要转 ModelDowngradedError。
	strictCheck := strictLineCheck(med, func() int64 { return 42 })
	cb := func(line string) error {
		if strictCheck != nil {
			if err := strictCheck(line); err != nil {
				return err
			}
		}
		return nil
	}
	_, _, err := readModelCheckedBody(bytes.NewReader(frame), cb, time.Now(), 200, header)
	var aie *argonIdentityError
	if strictCheck != nil && errors.As(err, &aie) && modelDowngraded(med, aie.name) {
		err = &ModelDowngradedError{Requested: med.Upstream, Reported: aie.name, AccountID: 42}
	}
	if err == nil {
		t.Fatal("错档响应（中档请求收到 High 自报）应被身份核验拒绝")
	}
	var mde *ModelDowngradedError
	if errors.As(err, &mde) {
		t.Fatalf("中档→高档是同槽升级，不应转成 ModelDowngradedError: %v", err)
	}
	// 真实链路把转换后的 err 交给 retryOnAnotherAccount（本请求账号 #42）：升级错档
	// 不是 ModelDowngradedError，不换号、不进 exclude、不写 300 分钟冷却。
	exclude := map[int64]bool{}
	if retryOnAnotherAccount(&StreamResult{}, err, med, exclude) {
		t.Fatal("同槽升级错档不应触发换号重试")
	}
	if len(exclude) != 0 {
		t.Fatalf("同槽升级不应把账号加入排除表: %v", exclude)
	}
	if cooling, _ := coolingAccounts(med); len(cooling) != 0 {
		t.Fatalf("同槽升级不应写入降级冷却: %v", cooling)
	}
}
