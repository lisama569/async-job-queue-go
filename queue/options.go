package queue

import "time"

// Config 为队列的可调参数。
type Config struct {
	// Workers 为并发消费者数量，必须 >= 1。
	Workers int
	// Capacity 为待处理通道容量，必须 >= 1。
	Capacity int
	// MaxAttempts 为最大尝试次数（含首次），必须 >= 1。
	MaxAttempts int
	// BaseBackoff 为第一次失败后的退避基准间隔。
	BaseBackoff time.Duration
	// MaxBackoff 为退避间隔的上限。
	MaxBackoff time.Duration
	// logger 为内部日志器，通过 WithLogger 注入，默认丢弃日志。
	logger Logger
}

// Option 以函数式选项方式覆盖 Config。
type Option func(*Config)

// DefaultConfig 返回一份可用的默认配置。
func DefaultConfig() Config {
	return Config{
		Workers:     4,
		Capacity:    128,
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  time.Second,
	}
}

// WithWorkers 设置并发消费者数量。
func WithWorkers(n int) Option { return func(c *Config) { c.Workers = n } }

// WithCapacity 设置待处理通道容量。
func WithCapacity(n int) Option { return func(c *Config) { c.Capacity = n } }

// WithMaxAttempts 设置最大尝试次数。
func WithMaxAttempts(n int) Option { return func(c *Config) { c.MaxAttempts = n } }

// WithBackoff 设置退避基准间隔与上限。
func WithBackoff(base, max time.Duration) Option {
	return func(c *Config) {
		c.BaseBackoff = base
		c.MaxBackoff = max
	}
}

// WithLogger 注入自定义日志器（测试中用于收集状态迁移日志）。
func WithLogger(l Logger) Option {
	return func(c *Config) {
		if l != nil {
			c.logger = l
		}
	}
}

// normalize 对配置做下限保护，保证任何非法输入都不会破坏运行不变量。
func normalize(c *Config) {
	if c.Workers < 1 {
		c.Workers = 1
	}
	if c.Capacity < 1 {
		c.Capacity = 1
	}
	if c.MaxAttempts < 1 {
		c.MaxAttempts = 1
	}
	if c.BaseBackoff < 0 {
		c.BaseBackoff = 0
	}
	if c.MaxBackoff < c.BaseBackoff {
		c.MaxBackoff = c.BaseBackoff
	}
}
