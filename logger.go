package housegate

import "sentioxyz/sentio-core/common/log"

// Logger is the structured-logging contract the proxy uses for its own
// lifecycle messages. Library hosts may inject their own implementation
// via Options.Logger; when nil, New defaults to a sentio-core adapter so
// existing standalone behaviour is preserved.
//
// The signature mirrors zap's sugared *w family: alternating key-value
// pairs after the message. Implementations should treat odd-length kv
// slices the way zap does (emit a logger-level error or a synthetic
// dangling-key entry) — the proxy will never produce one intentionally.
type Logger interface {
	Debugw(msg string, keysAndValues ...any)
	Infow(msg string, keysAndValues ...any)
	Warnw(msg string, keysAndValues ...any)
	Errorw(msg string, keysAndValues ...any)
}

type sentioCoreLogger struct{}

func (sentioCoreLogger) Debugw(msg string, kv ...any) { log.Debugw(msg, kv...) }
func (sentioCoreLogger) Infow(msg string, kv ...any)  { log.Infow(msg, kv...) }
func (sentioCoreLogger) Warnw(msg string, kv ...any)  { log.Warnw(msg, kv...) }
func (sentioCoreLogger) Errorw(msg string, kv ...any) { log.Errorw(msg, kv...) }

func defaultLogger() Logger { return sentioCoreLogger{} }
