package log

import "context"

type ctxKey struct{}

// From returns the Logger bound to ctx, or the package default when none is
// attached. Always returns a non-nil Logger.
func From(ctx context.Context) *Logger {
	if ctx != nil {
		if v := ctx.Value(ctxKey{}); v != nil {
			if l, ok := v.(*Logger); ok && l != nil {
				return l
			}
		}
	}
	return Default()
}

// WithContext returns a ctx whose Logger is l. Passing a nil Logger returns
// ctx unchanged.
func WithContext(ctx context.Context, l *Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext returns the Logger bound to ctx, optionally enriched with kv.
// When kv is non-empty the enriched Logger is also rebound to the returned
// ctx so downstream callers that pull it out via From see the same fields.
//
// Mirrors sentioxyz/sentio-core/common/log.FromContext semantics:
//
//	ctx, logger := log.FromContext(ctx, "session", id, "remote_addr", addr)
//	logger.Infow("hello")
func FromContext(ctx context.Context, kv ...any) (context.Context, *Logger) {
	l := From(ctx).With(kv...)
	if len(kv) > 0 {
		ctx = WithContext(ctx, l)
	}
	return ctx, l
}
