package app

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkoukk/tiktoken-go"
)

// tiktoken cl100k_base — GPT-4 BPE。Gemini 真 tokenizer Google 没公开，
// 但 BPE 层面规则相近，对中英混合都比 chars/4 准确得多（实测中文偏差 ±20% 内）。
//
// 加载一次到内存（~5MB），后续 Encode 每次 ~微秒级。
var (
	tokenizer            *tiktoken.Tiktoken
	tokenizerOnce        sync.Once
	tokenizerOK          bool
	tokenizerHTTPTimeout = 2 * time.Second
)

type boundedBpeLoader struct{}

func tokenizerCacheDir() string {
	if d := strings.TrimSpace(os.Getenv("TIKTOKEN_CACHE_DIR")); d != "" {
		return d
	}
	if d := strings.TrimSpace(os.Getenv("DATA_GYM_CACHE_DIR")); d != "" {
		return d
	}
	return filepath.Join(os.TempDir(), "data-gym-cache")
}

func parseTiktokenBpe(contents []byte) (map[string]int, error) {
	bpeRanks := make(map[string]int)
	for _, line := range strings.Split(string(contents), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, " ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid tiktoken bpe line")
		}
		token, err := base64.StdEncoding.DecodeString(parts[0])
		if err != nil {
			return nil, err
		}
		rank, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		}
		bpeRanks[string(token)] = rank
	}
	if len(bpeRanks) == 0 {
		return nil, fmt.Errorf("empty tiktoken bpe file")
	}
	return bpeRanks, nil
}

func (l *boundedBpeLoader) LoadTiktokenBpe(tiktokenBpeFile string) (map[string]int, error) {
	if tiktokenBpeFile == "" {
		return nil, fmt.Errorf("blobpath cannot be empty")
	}
	if !strings.HasPrefix(tiktokenBpeFile, "http://") && !strings.HasPrefix(tiktokenBpeFile, "https://") {
		contents, err := os.ReadFile(tiktokenBpeFile)
		if err != nil {
			return nil, err
		}
		return parseTiktokenBpe(contents)
	}

	cacheDir := tokenizerCacheDir()
	cacheKey := fmt.Sprintf("%x", sha1.Sum([]byte(tiktokenBpeFile)))
	cachePath := filepath.Join(cacheDir, cacheKey)
	if contents, err := os.ReadFile(cachePath); err == nil {
		if ranks, err := parseTiktokenBpe(contents); err == nil {
			return ranks, nil
		}
	}

	client := &http.Client{Timeout: tokenizerHTTPTimeout}
	resp, err := client.Get(tiktokenBpeFile)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP %d fetching tokenizer", resp.StatusCode)
	}
	contents, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	ranks, err := parseTiktokenBpe(contents)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(cacheDir, 0o755); err == nil {
		tmp := fmt.Sprintf("%s.%d.tmp", cachePath, time.Now().UnixNano())
		if err := os.WriteFile(tmp, contents, 0o644); err == nil {
			if err := os.Rename(tmp, cachePath); err != nil {
				_ = os.Remove(tmp)
			}
		}
	}
	return ranks, nil
}

func initTokenizer() {
	tokenizerOnce.Do(func() {
		// tiktoken-go 第一次加载会从远端下 cl100k_base.tiktoken (~3MB)，
		// 缓存到 ${TIKTOKEN_CACHE_DIR} 或 ${HOME}/.cache/tiktoken。
		// 默认 loader 使用无超时的 http.DefaultClient，黑洞代理下会卡住启动；
		// 这里改用带超时的 boundedBpeLoader，超时后立即回退到 chars/4。
		tiktoken.SetBpeLoader(&boundedBpeLoader{})
		t, err := tiktoken.GetEncoding("cl100k_base")
		if err != nil {
			fmt.Fprintf(os.Stderr, "[tokenizer] init failed (will fall back to chars/4): %v\n", err)
			return
		}
		tokenizer = t
		tokenizerOK = true
	})
}

// countTokens 返回文本的近似 token 数。
// tokenizer 加载失败时退回到 chars/4 估算。
func countTokens(s string) int {
	if !tokenizerOK || tokenizer == nil {
		return len(s) / 4
	}
	return len(tokenizer.Encode(s, nil, nil))
}
