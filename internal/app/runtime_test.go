package app

import "testing"

// RuntimeConfig 范围校验：request_timeout_sec 上限放宽到 1200（长思考 + 长输出能跑过
// 600 秒，借鉴上游 #83）；stream_idle_timeout_sec 接受 [0,600]（0 = 关，上游 #81 思路）。
func TestValidateRuntimeConfigTimeoutBounds(t *testing.T) {
	saved := cfg
	t.Cleanup(func() { cfg = saved; initRuntimeConfig() })
	cfg = defaultConfig()
	cfg.DBPath = saved.DBPath // 别让 kvGet 去开真实的 ./data 库
	initRuntimeConfig()

	for _, tc := range []struct {
		name   string
		mutate func(*RuntimeConfig)
		ok     bool
	}{
		{"request_timeout_sec=4", func(c *RuntimeConfig) { c.RequestTimeout = 4 }, false},
		{"request_timeout_sec=5", func(c *RuntimeConfig) { c.RequestTimeout = 5 }, true},
		{"request_timeout_sec=600", func(c *RuntimeConfig) { c.RequestTimeout = 600 }, true},
		{"request_timeout_sec=1200", func(c *RuntimeConfig) { c.RequestTimeout = 1200 }, true},
		{"request_timeout_sec=1201", func(c *RuntimeConfig) { c.RequestTimeout = 1201 }, false},
		{"stream_idle_timeout_sec=-1", func(c *RuntimeConfig) { c.StreamIdleTimeoutSec = -1 }, false},
		{"stream_idle_timeout_sec=0", func(c *RuntimeConfig) { c.StreamIdleTimeoutSec = 0 }, true},
		{"stream_idle_timeout_sec=600", func(c *RuntimeConfig) { c.StreamIdleTimeoutSec = 600 }, true},
		{"stream_idle_timeout_sec=601", func(c *RuntimeConfig) { c.StreamIdleTimeoutSec = 601 }, false},
	} {
		c := rtCfg()
		tc.mutate(&c)
		if err := validateRuntimeConfig(c); (err == nil) != tc.ok {
			t.Errorf("%s: err=%v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}
