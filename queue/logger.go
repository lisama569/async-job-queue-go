package queue

// Logger 为队列内部使用的最小日志接口，便于测试中替换。
type Logger interface {
	Printf(format string, args ...any)
}

// noopLogger 默认不输出任何日志。
type noopLogger struct{}

func (noopLogger) Printf(format string, args ...any) {}
