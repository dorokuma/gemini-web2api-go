package app

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func turnBody(turns ...[]any) string {
	var ts []any
	for _, t := range turns {
		ts = append(ts, t)
	}
	inner, _ := json.Marshal([]any{ts})
	env, _ := json.Marshal([]any{[]any{"wrb.fr", "hNvQHb", string(inner)}})
	return ")]}'\n\n1\n" + string(env) + "\n"
}

func TestClassifyVideoTurns(t *testing.T) {
	refused := []any{[]any{"c_1", "r_2"}, nil, []any{[]any{"p"}}, []any{[]any{"rc_22222222",
		[]any{"I can create more videos as soon as your limit resets. Check your usage in Settings."},
		map[string]any{"60": []any{"video_generation", []any{1792119910, 281161000}, "User is out of video generation quota (Aureus)"}}}},
		[]any{1792103308, 60831000}}
	text := []any{[]any{"c_1", "r_3"}, nil, []any{[]any{"hi"}}, []any{[]any{"rc_33333333", []any{"hello"}}}, []any{1792103000, 0}}
	evs := classifyVideoTurns(turnBody(refused, text))
	if len(evs) != 1 {
		t.Fatalf("want 1 video turn, got %d: %+v", len(evs), evs)
	}
	if evs[0].Kind != "quota_refused" || evs[0].At != 1792103308 || evs[0].ResetAt != 1792119910 {
		t.Fatalf("bad refusal event: %+v", evs[0])
	}

	// 成品视频：带 response_data 下载链的那一轮（c 参数是含该标记的 base64）。
	c := base64.RawURLEncoding.EncodeToString([]byte("response_data:clip"))
	ready := []any{[]any{"c_1", "r_4"}, nil, []any{[]any{"a fox"}}, []any{[]any{"rc_44444444",
		[]any{"Here is your video"},
		map[string]any{"60": []any{"video_generation", []any{"https://contribution.usercontent.google.com/download?c=" + c}}}}},
		[]any{1792104000, 0}}
	evs = classifyVideoTurns(turnBody(ready))
	if len(evs) != 1 || evs[0].Kind != "video" || evs[0].At != 1792104000 {
		t.Fatalf("ready video not classified: %+v", evs)
	}
}

func TestSummarizeQuota(t *testing.T) {
	now := time.Unix(1792110000, 0)
	h := int64(3600)
	// 3 videos, then a refusal that carries Google's reset time.
	q := accountQuota{Events: []quotaEvent{
		{At: now.Unix() - 3*h, Kind: "video"},
		{At: now.Unix() - 2*h, Kind: "video"},
		{At: now.Unix() - 1*h, Kind: "video"},
		{At: now.Unix() - 30*60, Kind: "quota_refused", ResetAt: now.Unix() + 2*h},
	}}
	summarizeQuota(&q, now)
	if q.Status != "exhausted" || q.ResetAt != now.Unix()+2*h || q.ResetSource != "google" || q.VideosInWin != 3 {
		t.Fatalf("google reset: %+v", q)
	}
	// Refusal without Google's time -> first video in window + 5 h.
	q = accountQuota{Events: []quotaEvent{
		{At: now.Unix() - 4*h, Kind: "video"},
		{At: now.Unix() - 10*60, Kind: "quota_refused"},
	}}
	summarizeQuota(&q, now)
	if q.Status != "exhausted" || q.ResetAt != now.Unix()+h || q.ResetSource != "estimated" {
		t.Fatalf("estimated reset: %+v", q)
	}
	// Reset time already passed -> available again.
	q = accountQuota{Events: []quotaEvent{
		{At: now.Unix() - 7*h, Kind: "video"},
		{At: now.Unix() - 6*h, Kind: "quota_refused", ResetAt: now.Unix() - h},
	}}
	summarizeQuota(&q, now)
	if q.Status != "available" || q.VideosInWin != 0 {
		t.Fatalf("after reset: %+v", q)
	}
	// A successful video after a refusal means the account recovered.
	q = accountQuota{Events: []quotaEvent{
		{At: now.Unix() - 2*h, Kind: "quota_refused", ResetAt: now.Unix() + h},
		{At: now.Unix() - h, Kind: "video"},
	}}
	summarizeQuota(&q, now)
	if q.Status != "available" || !strings.Contains(q.ResetSource, "estimated") {
		t.Fatalf("recovered: %+v", q)
	}
}

// R1 修复 4 回归：viaSlot 在挑到的出口跟账号记的不一样时持久化 bindAccountProxy
// 并同步 acct.ProxyID——同一次额度查询的后续 RPC 沿用新出口，不跨 IP 漂移。
func TestViaSlotPersistsRebind(t *testing.T) {
	_, b := withTwoProxies(t, true) // A 被 withTwoProxies 占满
	acct := addTestAccount(t, "viaslot")
	// 未绑定的账号（ProxyID=0）：acquireSlot(0) 轮询到 B（A 满）。应写库 B。
	var used string
	ca := CookieAccount{ID: acct}
	if err := viaSlot(&ca, func(url string) error { used = url; return nil }); err != nil {
		t.Fatalf("viaSlot 应拿到空闲出口：%v", err)
	}
	if ca.ProxyID != b || used == "" {
		t.Fatalf("首次绑定应就地同步 ProxyID=%d，得到 %d", b, ca.ProxyID)
	}
	if acc := accountByID(acct); acc == nil || acc.ProxyID != b {
		t.Fatalf("首次绑定必须落库：%+v", acc)
	}

	// 已绑 B 再调：绑定有效就不重写（B 仍空闲，acquireSlot(B) 命中 B）。
	ca2 := CookieAccount{ID: acct, ProxyID: b}
	if err := viaSlot(&ca2, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if ca2.ProxyID != b {
		t.Fatalf("绑定出口可用时不该换：%d", ca2.ProxyID)
	}
}
