package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 客户端断开后必须真的掐掉上游连接：上游那头要能看到连接断了，而不是一直生成到结束。
//
// 假上游挂在代理那条路上（stdlib 客户端 + 代理），http:// 目标会以绝对 URI 发给代理，
// 所以 httptest 服务器本身就能当上游，不用碰真网络。
func TestCancelAbortsUpstreamStream(t *testing.T) {
	upstreamGone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(")]}'\nfirst frame\n"))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(upstreamGone)
		case <-time.After(10 * time.Second): // 模拟一个还要生成很久的上游
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	_, _, _, _, err := doGeminiRequestStop(ctx, "http://upstream.invalid/StreamGenerate", "f.req=x",
		map[string]string{}, srv.URL, func(line string) error {
			if line == "first frame" {
				cancel() // 客户端在收到首帧后走了
			}
			return nil
		}, nil)
	if err == nil {
		t.Fatal("取消后应返回错误")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("取消后仍等了 %v 才返回", took)
	}
	select {
	case <-upstreamGone:
	case <-time.After(3 * time.Second):
		t.Fatal("上游连接没有被关闭")
	}
}

func TestSleepCtxCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	if err := sleepCtx(ctx, 10*time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleepCtx = %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("重试等待没有随取消提前结束")
	}
	if err := sleepCtx(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

// 已经断开的请求不能再去占限流 slot、挑号、发上游。
func TestStreamGenerateSkipsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := streamGenerateWithFiles(ctx, "hi", promptCtx{Latest: "hi"}, Models["gemini-3.5-flash-lite"], nil, nil, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

// 客户端断开：不写响应（没人收），requests 表记 499 而不是 502。
func TestChatCompletionsClientGoneRecords499(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	body := `{"model":"gemini-3.5-flash-lite","messages":[{"role":"user","content":"cancel-probe-499"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body)).WithContext(ctx)
	rec := httptest.NewRecorder()
	handleChatCompletions(rec, req)
	if rec.Body.Len() != 0 {
		t.Fatalf("客户端已断开仍写了响应: %q", rec.Body.String())
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var status int
		var errStr string
		row := getDB().QueryRow(`SELECT status, error FROM requests WHERE prompt_chars=? ORDER BY id DESC LIMIT 1`,
			len("cancel-probe-499"))
		if row.Scan(&status, &errStr) == nil {
			if status != statusClientClosed || errStr != "client disconnected" {
				t.Fatalf("记录 = %d %q，应为 499 client disconnected", status, errStr)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("没有记录这次请求")
}
