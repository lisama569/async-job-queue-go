package queue

import "time"

// backoff 计算第 attempt 次失败（attempt 从 1 开始）后应等待的时长。
//
// 采用指数退避：base * 2^(attempt-1)。对任意 attempt 有
// backoff(attempt+1) >= backoff(attempt)，且结果被截断到 [0, MaxBackoff]。
// 使用先翻倍再截断的顺序可避免中间乘法溢出。
func backoff(cfg Config, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := cfg.BaseBackoff
	for i := 1; i < attempt; i++ {
		if d >= cfg.MaxBackoff {
			return cfg.MaxBackoff
		}
		d *= 2
		if d > cfg.MaxBackoff || d < 0 {
			return cfg.MaxBackoff
		}
	}
	return d
}
