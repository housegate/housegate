// Package log is a thin convenience wrapper around log/slog.
//
// External users inject any *slog.Logger (or slog.Handler) via housegate.Options;
// internal code uses the convenience surface here (Info/Infof/Infow/Infoe/Infofe
// plus InfoIf/InfoEveryN/InfoEvery and the same family for Debug/Warn/Error/Fatal).
//
// Lazy arguments. Anywhere an argument or value would be evaluated, a
//
//	func() string  or  func() any
//
// is accepted and is only invoked when the level is enabled. This lets you
// pass expensive formatters to a Debug call without paying the cost at Info
// level:
//
//	log.Debugw("plan", "explain", func() string { return formatPlan(p) })
//
// Values implementing slog.LogValuer remain lazy via slog's own machinery.
//
// The default logger uses slog.Default() until SetDefault is called.
package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

// LevelFatal is above slog.LevelError; Fatal* methods log at this level and
// then call osExit. Handlers that don't understand it render as a numeric
// level — install a ReplaceAttr or a custom handler to map it to "FATAL".
const LevelFatal slog.Level = slog.LevelError + 4

// osExit is indirected so tests can swap it out without terminating the
// test binary.
var osExit = os.Exit

// Logger wraps *slog.Logger with the convenience surface used inside housegate.
// The zero value is unusable; construct via New, NewFromSlog, or Default.
type Logger struct {
	sl *slog.Logger
}

// New returns a Logger backed by a freshly constructed *slog.Logger over h.
func New(h slog.Handler) *Logger {
	if h == nil {
		h = slog.Default().Handler()
	}
	return &Logger{sl: slog.New(h)}
}

// NewFromSlog wraps an existing *slog.Logger. Passing nil yields a Logger
// backed by slog.Default().
func NewFromSlog(s *slog.Logger) *Logger {
	if s == nil {
		s = slog.Default()
	}
	return &Logger{sl: s}
}

// Slog exposes the underlying *slog.Logger for callers that need slog-native
// APIs (LogAttrs, Enabled, etc.) without losing field accumulation.
func (l *Logger) Slog() *slog.Logger { return l.sl }

// Enabled reports whether the handler accepts records at level. Cheap; safe
// to call as a manual gate around expensive code paths that can't easily be
// expressed as a func() string.
func (l *Logger) Enabled(level slog.Level) bool {
	return l.sl.Enabled(context.Background(), level)
}

// With returns a Logger whose records inherit the given key-value pairs.
// Returns the receiver unchanged when kv is empty. Lazy values are NOT
// resolved here; slog.LogValuer / lazy funcs are resolved at emit time.
func (l *Logger) With(kv ...any) *Logger {
	if len(kv) == 0 {
		return l
	}
	return &Logger{sl: l.sl.With(kv...)}
}

// fmtMode selects how emit builds the record message.
type fmtMode uint8

const (
	fmtRaw      fmtMode = iota // msg = msgArg (Infow / Infoe family)
	fmtSprint                  // msg = fmt.Sprint(args...) (Info family)
	fmtSprintf                 // msg = fmt.Sprintf(msgArg, args...) (Infof / Infofe family)
)

// emit is the single funnel that constructs and dispatches a slog.Record.
//
// The level gate runs BEFORE args and kv are touched, so lazy values are
// never invoked at a filtered level. After the gate passes, lazy values
// (func() string / func() any) are resolved in place, then args is fed to
// fmt and kv is added to the record.
//
// skip = 3 is correct when called from a public method or top-level package
// function: [runtime.Callers, emit, caller, user].
func (l *Logger) emit(skip int, level slog.Level, mode fmtMode, msgArg string, args, kv []any) {
	ctx := context.Background()
	if !l.sl.Enabled(ctx, level) {
		return
	}
	var msg string
	switch mode {
	case fmtRaw:
		msg = msgArg
	case fmtSprint:
		if len(args) > 0 {
			resolveLazyInPlace(args)
		}
		msg = fmt.Sprint(args...)
	case fmtSprintf:
		if len(args) > 0 {
			resolveLazyInPlace(args)
		}
		msg = fmt.Sprintf(msgArg, args...)
	}
	if len(kv) > 0 {
		resolveLazyInPlace(kv)
	}
	var pcs [1]uintptr
	runtime.Callers(skip, pcs[:])
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.Add(kv...)
	_ = l.sl.Handler().Handle(ctx, r)
}

// --- Debug ---------------------------------------------------------------

func (l *Logger) Debug(args ...any) {
	l.emit(3, slog.LevelDebug, fmtSprint, "", args, nil)
}
func (l *Logger) Debugf(format string, args ...any) {
	l.emit(3, slog.LevelDebug, fmtSprintf, format, args, nil)
}
func (l *Logger) Debugw(msg string, kv ...any) {
	l.emit(3, slog.LevelDebug, fmtRaw, msg, nil, kv)
}
func (l *Logger) Debuge(err error, msg string) {
	l.emit(3, slog.LevelDebug, fmtRaw, msg, nil, []any{"error", err})
}
func (l *Logger) Debugfe(err error, format string, args ...any) {
	l.emit(3, slog.LevelDebug, fmtSprintf, format, args, []any{"error", err})
}
func (l *Logger) DebugIf(cond bool, msg string, kv ...any) {
	if cond {
		l.emit(3, slog.LevelDebug, fmtRaw, msg, nil, kv)
	}
}
func (l *Logger) DebugEveryN(id string, n int64, msg string, kv ...any) {
	if l.Enabled(slog.LevelDebug) && everyN(id, n) {
		l.emit(3, slog.LevelDebug, fmtRaw, msg, nil, kv)
	}
}
func (l *Logger) DebugEvery(id string, d time.Duration, msg string, kv ...any) {
	if l.Enabled(slog.LevelDebug) && every(id, d) {
		l.emit(3, slog.LevelDebug, fmtRaw, msg, nil, kv)
	}
}

// --- Info ----------------------------------------------------------------

func (l *Logger) Info(args ...any) {
	l.emit(3, slog.LevelInfo, fmtSprint, "", args, nil)
}
func (l *Logger) Infof(format string, args ...any) {
	l.emit(3, slog.LevelInfo, fmtSprintf, format, args, nil)
}
func (l *Logger) Infow(msg string, kv ...any) {
	l.emit(3, slog.LevelInfo, fmtRaw, msg, nil, kv)
}
func (l *Logger) Infoe(err error, msg string) {
	l.emit(3, slog.LevelInfo, fmtRaw, msg, nil, []any{"error", err})
}
func (l *Logger) Infofe(err error, format string, args ...any) {
	l.emit(3, slog.LevelInfo, fmtSprintf, format, args, []any{"error", err})
}
func (l *Logger) InfoIf(cond bool, msg string, kv ...any) {
	if cond {
		l.emit(3, slog.LevelInfo, fmtRaw, msg, nil, kv)
	}
}
func (l *Logger) InfoEveryN(id string, n int64, msg string, kv ...any) {
	if l.Enabled(slog.LevelInfo) && everyN(id, n) {
		l.emit(3, slog.LevelInfo, fmtRaw, msg, nil, kv)
	}
}
func (l *Logger) InfoEvery(id string, d time.Duration, msg string, kv ...any) {
	if l.Enabled(slog.LevelInfo) && every(id, d) {
		l.emit(3, slog.LevelInfo, fmtRaw, msg, nil, kv)
	}
}

// --- Warn ----------------------------------------------------------------

func (l *Logger) Warn(args ...any) {
	l.emit(3, slog.LevelWarn, fmtSprint, "", args, nil)
}
func (l *Logger) Warnf(format string, args ...any) {
	l.emit(3, slog.LevelWarn, fmtSprintf, format, args, nil)
}
func (l *Logger) Warnw(msg string, kv ...any) {
	l.emit(3, slog.LevelWarn, fmtRaw, msg, nil, kv)
}
func (l *Logger) Warne(err error, msg string) {
	l.emit(3, slog.LevelWarn, fmtRaw, msg, nil, []any{"error", err})
}
func (l *Logger) Warnfe(err error, format string, args ...any) {
	l.emit(3, slog.LevelWarn, fmtSprintf, format, args, []any{"error", err})
}
func (l *Logger) WarnIf(cond bool, msg string, kv ...any) {
	if cond {
		l.emit(3, slog.LevelWarn, fmtRaw, msg, nil, kv)
	}
}
func (l *Logger) WarnEveryN(id string, n int64, msg string, kv ...any) {
	if l.Enabled(slog.LevelWarn) && everyN(id, n) {
		l.emit(3, slog.LevelWarn, fmtRaw, msg, nil, kv)
	}
}
func (l *Logger) WarnEvery(id string, d time.Duration, msg string, kv ...any) {
	if l.Enabled(slog.LevelWarn) && every(id, d) {
		l.emit(3, slog.LevelWarn, fmtRaw, msg, nil, kv)
	}
}

// --- Error ---------------------------------------------------------------

func (l *Logger) Error(args ...any) {
	l.emit(3, slog.LevelError, fmtSprint, "", args, nil)
}
func (l *Logger) Errorf(format string, args ...any) {
	l.emit(3, slog.LevelError, fmtSprintf, format, args, nil)
}
func (l *Logger) Errorw(msg string, kv ...any) {
	l.emit(3, slog.LevelError, fmtRaw, msg, nil, kv)
}
func (l *Logger) Errore(err error, msg string) {
	l.emit(3, slog.LevelError, fmtRaw, msg, nil, []any{"error", err})
}
func (l *Logger) Errorfe(err error, format string, args ...any) {
	l.emit(3, slog.LevelError, fmtSprintf, format, args, []any{"error", err})
}
func (l *Logger) ErrorIf(cond bool, msg string, kv ...any) {
	if cond {
		l.emit(3, slog.LevelError, fmtRaw, msg, nil, kv)
	}
}
func (l *Logger) ErrorEveryN(id string, n int64, msg string, kv ...any) {
	if l.Enabled(slog.LevelError) && everyN(id, n) {
		l.emit(3, slog.LevelError, fmtRaw, msg, nil, kv)
	}
}
func (l *Logger) ErrorEvery(id string, d time.Duration, msg string, kv ...any) {
	if l.Enabled(slog.LevelError) && every(id, d) {
		l.emit(3, slog.LevelError, fmtRaw, msg, nil, kv)
	}
}

// --- Fatal ---------------------------------------------------------------
//
// Fatal* always calls osExit, even if the level is filtered out.

func (l *Logger) Fatal(args ...any) {
	l.emit(3, LevelFatal, fmtSprint, "", args, nil)
	osExit(1)
}
func (l *Logger) Fatalf(format string, args ...any) {
	l.emit(3, LevelFatal, fmtSprintf, format, args, nil)
	osExit(1)
}
func (l *Logger) Fatalw(msg string, kv ...any) {
	l.emit(3, LevelFatal, fmtRaw, msg, nil, kv)
	osExit(1)
}
func (l *Logger) Fatale(err error, msg string) {
	l.emit(3, LevelFatal, fmtRaw, msg, nil, []any{"error", err})
	osExit(1)
}
func (l *Logger) Fatalfe(err error, format string, args ...any) {
	l.emit(3, LevelFatal, fmtSprintf, format, args, []any{"error", err})
	osExit(1)
}

// --- Package-level default logger ----------------------------------------

var (
	defaultLogger atomic.Pointer[Logger]

	// defaultLevel is the Leveler installed in the package-default handler.
	// SetLevel mutates it atomically so log.SetLevel applies to every record
	// emitted via the default logger going forward — no handler rebuild.
	defaultLevel = new(slog.LevelVar) // zero value = LevelInfo
)

func init() {
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:       defaultLevel,
		ReplaceAttr: replaceLevelFatal,
	})
	defaultLogger.Store(New(h))
}

// replaceLevelFatal renames the synthetic LevelFatal so handlers print
// "level=FATAL" instead of "level=ERROR+4".
func replaceLevelFatal(_ []string, a slog.Attr) slog.Attr {
	if a.Key != slog.LevelKey {
		return a
	}
	if lv, ok := a.Value.Any().(slog.Level); ok && lv == LevelFatal {
		a.Value = slog.StringValue("FATAL")
	}
	return a
}

// Default returns the package-level Logger.
func Default() *Logger { return defaultLogger.Load() }

// SetDefault replaces the package-level Logger. Safe for concurrent use.
// A nil argument is ignored.
//
// Note: SetLevel only controls the level of the Logger constructed by
// pkg/log's init. If you SetDefault to a Logger wrapping a host-supplied
// handler, manage that handler's level via your own Leveler.
func SetDefault(l *Logger) {
	if l == nil {
		return
	}
	defaultLogger.Store(l)
}

// Enabled reports whether the package-default Logger accepts records at level.
func Enabled(level slog.Level) bool { return Default().Enabled(level) }

// SetLevel sets the level of the package-default Logger's handler. Effective
// for every subsequent emission via Default(). Has no effect on Loggers
// constructed by callers via New / NewFromSlog with a different handler.
func SetLevel(level slog.Level) { defaultLevel.Set(level) }

// GetLevel returns the current level of the package-default Logger's handler.
func GetLevel() slog.Level { return defaultLevel.Level() }

// DefaultLevelVar exposes the *slog.LevelVar that backs SetLevel/GetLevel,
// for callers that build their own handler and want to share level state
// with the package default — e.g. the binary's console handler in cmd.
func DefaultLevelVar() *slog.LevelVar { return defaultLevel }

// ParseLevel parses a level string (case-insensitive). Accepts the standard
// names ("debug", "info", "warn"/"warning", "error", "fatal") as well as
// slog's own text encoding ("DEBUG+1", "INFO-2", numeric "-4", ...).
//
// Empty string is treated as LevelInfo so an unset config field gets a sane
// default rather than an error.
func ParseLevel(s string) (slog.Level, error) {
	if s == "" {
		return slog.LevelInfo, nil
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error", "err":
		return slog.LevelError, nil
	case "fatal":
		return LevelFatal, nil
	}
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo, fmt.Errorf("invalid log level %q: %w", s, err)
	}
	return lv, nil
}

// --- Package-level convenience pass-throughs -----------------------------
//
// These mirror the methods 1:1 so callers can write `log.Infow(...)` instead
// of `log.Default().Infow(...)`. The skip value stays at 3 because each
// package-level function calls emit directly (it does not delegate to the
// matching method).

func Debug(args ...any) {
	Default().emit(3, slog.LevelDebug, fmtSprint, "", args, nil)
}
func Debugf(format string, args ...any) {
	Default().emit(3, slog.LevelDebug, fmtSprintf, format, args, nil)
}
func Debugw(msg string, kv ...any) {
	Default().emit(3, slog.LevelDebug, fmtRaw, msg, nil, kv)
}
func Debuge(err error, msg string) {
	Default().emit(3, slog.LevelDebug, fmtRaw, msg, nil, []any{"error", err})
}
func Debugfe(err error, format string, args ...any) {
	Default().emit(3, slog.LevelDebug, fmtSprintf, format, args, []any{"error", err})
}
func DebugIf(cond bool, msg string, kv ...any) {
	if cond {
		Default().emit(3, slog.LevelDebug, fmtRaw, msg, nil, kv)
	}
}
func DebugEveryN(id string, n int64, msg string, kv ...any) {
	l := Default()
	if l.Enabled(slog.LevelDebug) && everyN(id, n) {
		l.emit(3, slog.LevelDebug, fmtRaw, msg, nil, kv)
	}
}
func DebugEvery(id string, d time.Duration, msg string, kv ...any) {
	l := Default()
	if l.Enabled(slog.LevelDebug) && every(id, d) {
		l.emit(3, slog.LevelDebug, fmtRaw, msg, nil, kv)
	}
}

func Info(args ...any) {
	Default().emit(3, slog.LevelInfo, fmtSprint, "", args, nil)
}
func Infof(format string, args ...any) {
	Default().emit(3, slog.LevelInfo, fmtSprintf, format, args, nil)
}
func Infow(msg string, kv ...any) {
	Default().emit(3, slog.LevelInfo, fmtRaw, msg, nil, kv)
}
func Infoe(err error, msg string) {
	Default().emit(3, slog.LevelInfo, fmtRaw, msg, nil, []any{"error", err})
}
func Infofe(err error, format string, args ...any) {
	Default().emit(3, slog.LevelInfo, fmtSprintf, format, args, []any{"error", err})
}
func InfoIf(cond bool, msg string, kv ...any) {
	if cond {
		Default().emit(3, slog.LevelInfo, fmtRaw, msg, nil, kv)
	}
}
func InfoEveryN(id string, n int64, msg string, kv ...any) {
	l := Default()
	if l.Enabled(slog.LevelInfo) && everyN(id, n) {
		l.emit(3, slog.LevelInfo, fmtRaw, msg, nil, kv)
	}
}
func InfoEvery(id string, d time.Duration, msg string, kv ...any) {
	l := Default()
	if l.Enabled(slog.LevelInfo) && every(id, d) {
		l.emit(3, slog.LevelInfo, fmtRaw, msg, nil, kv)
	}
}

func Warn(args ...any) {
	Default().emit(3, slog.LevelWarn, fmtSprint, "", args, nil)
}
func Warnf(format string, args ...any) {
	Default().emit(3, slog.LevelWarn, fmtSprintf, format, args, nil)
}
func Warnw(msg string, kv ...any) {
	Default().emit(3, slog.LevelWarn, fmtRaw, msg, nil, kv)
}
func Warne(err error, msg string) {
	Default().emit(3, slog.LevelWarn, fmtRaw, msg, nil, []any{"error", err})
}
func Warnfe(err error, format string, args ...any) {
	Default().emit(3, slog.LevelWarn, fmtSprintf, format, args, []any{"error", err})
}
func WarnIf(cond bool, msg string, kv ...any) {
	if cond {
		Default().emit(3, slog.LevelWarn, fmtRaw, msg, nil, kv)
	}
}
func WarnEveryN(id string, n int64, msg string, kv ...any) {
	l := Default()
	if l.Enabled(slog.LevelWarn) && everyN(id, n) {
		l.emit(3, slog.LevelWarn, fmtRaw, msg, nil, kv)
	}
}
func WarnEvery(id string, d time.Duration, msg string, kv ...any) {
	l := Default()
	if l.Enabled(slog.LevelWarn) && every(id, d) {
		l.emit(3, slog.LevelWarn, fmtRaw, msg, nil, kv)
	}
}

func Error(args ...any) {
	Default().emit(3, slog.LevelError, fmtSprint, "", args, nil)
}
func Errorf(format string, args ...any) {
	Default().emit(3, slog.LevelError, fmtSprintf, format, args, nil)
}
func Errorw(msg string, kv ...any) {
	Default().emit(3, slog.LevelError, fmtRaw, msg, nil, kv)
}
func Errore(err error, msg string) {
	Default().emit(3, slog.LevelError, fmtRaw, msg, nil, []any{"error", err})
}
func Errorfe(err error, format string, args ...any) {
	Default().emit(3, slog.LevelError, fmtSprintf, format, args, []any{"error", err})
}
func ErrorIf(cond bool, msg string, kv ...any) {
	if cond {
		Default().emit(3, slog.LevelError, fmtRaw, msg, nil, kv)
	}
}
func ErrorEveryN(id string, n int64, msg string, kv ...any) {
	l := Default()
	if l.Enabled(slog.LevelError) && everyN(id, n) {
		l.emit(3, slog.LevelError, fmtRaw, msg, nil, kv)
	}
}
func ErrorEvery(id string, d time.Duration, msg string, kv ...any) {
	l := Default()
	if l.Enabled(slog.LevelError) && every(id, d) {
		l.emit(3, slog.LevelError, fmtRaw, msg, nil, kv)
	}
}

func Fatal(args ...any) {
	Default().emit(3, LevelFatal, fmtSprint, "", args, nil)
	osExit(1)
}
func Fatalf(format string, args ...any) {
	Default().emit(3, LevelFatal, fmtSprintf, format, args, nil)
	osExit(1)
}
func Fatalw(msg string, kv ...any) {
	Default().emit(3, LevelFatal, fmtRaw, msg, nil, kv)
	osExit(1)
}
func Fatale(err error, msg string) {
	Default().emit(3, LevelFatal, fmtRaw, msg, nil, []any{"error", err})
	osExit(1)
}
func Fatalfe(err error, format string, args ...any) {
	Default().emit(3, LevelFatal, fmtSprintf, format, args, []any{"error", err})
	osExit(1)
}
