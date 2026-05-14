package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"

	"housegate/housegate/pkg/log"
)

// consoleHandler is a slog.Handler whose output visually matches a
// zap "development" encoder, so housegate's own logs and any transitive
// deps' logs share one pattern.
//
// Format (TAB-separated columns):
//
//	<ts> <level> <caller> <msg> [json-fields]
//
// Example:
//
//	2026-05-11T15:53:50.825+0800<TAB>info<TAB>pkg/proxy/relay.go:42<TAB>handshake done<TAB>{"conn":1}
//
// Caller comes from r.PC (set by pkg/log via runtime.Callers), so attribution
// stays accurate regardless of whether inner methods are inlined.
type consoleHandler struct {
	w      io.Writer
	level  slog.Leveler
	attrs  []slog.Attr
	groups []string
	mu     *sync.Mutex
	color  bool // ANSI level coloring; off for file output
}

func newConsoleHandler(w io.Writer, level slog.Leveler, color bool) *consoleHandler {
	return &consoleHandler{w: w, level: level, mu: &sync.Mutex{}, color: color}
}

func (h *consoleHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *consoleHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	// zap's ISO8601 millisecond layout with numeric timezone offset.
	b.WriteString(r.Time.Format("2006-01-02T15:04:05.000-0700"))
	b.WriteByte('\t')
	b.WriteString(levelLowercase(r.Level, h.color))
	b.WriteByte('\t')
	b.WriteString(shortCaller(r.PC))
	b.WriteByte('\t')
	b.WriteString(r.Message)
	if r.NumAttrs() > 0 || len(h.attrs) > 0 {
		fields := make(map[string]any, r.NumAttrs()+len(h.attrs))
		for _, a := range h.attrs {
			fields[a.Key] = attrJSONValue(a.Value)
		}
		r.Attrs(func(a slog.Attr) bool {
			fields[a.Key] = attrJSONValue(a.Value)
			return true
		})
		if len(fields) > 0 {
			if data, err := json.Marshal(fields); err == nil {
				b.WriteByte('\t')
				b.Write(data)
			}
		}
	}
	b.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, b.String())
	return err
}

// Matches zap's LowercaseColorLevelEncoder palette.
const (
	ansiCyan      = "\x1b[36m"
	ansiBlue      = "\x1b[34m"
	ansiYellow    = "\x1b[33m"
	ansiRed       = "\x1b[31m"
	ansiRedBright = "\x1b[31;1m"
	ansiReset     = "\x1b[0m"
)

func levelLowercase(l slog.Level, color bool) string {
	switch {
	case l < slog.LevelInfo:
		if color {
			return ansiCyan + "debug" + ansiReset
		}
		return "debug"
	case l < slog.LevelWarn:
		if color {
			return ansiBlue + "info" + ansiReset
		}
		return "info"
	case l < slog.LevelError:
		if color {
			return ansiYellow + "warn" + ansiReset
		}
		return "warn"
	case l < log.LevelFatal:
		if color {
			return ansiRed + "error" + ansiReset
		}
		return "error"
	default:
		if color {
			return ansiRedBright + "fatal" + ansiReset
		}
		return "fatal"
	}
}

// attrJSONValue prepares an Attr.Value for json.Marshal. Errors are rendered
// via .Error() (matching zap's behavior) instead of marshaling the underlying
// struct — otherwise a *net.OpError would dump every field.
func attrJSONValue(v slog.Value) any {
	a := v.Resolve().Any()
	if err, ok := a.(error); ok {
		return err.Error()
	}
	return a
}

// shortCaller renders r.PC as "pkg/file.go:line", matching zap's
// ShortCallerEncoder.
func shortCaller(pc uintptr) string {
	if pc == 0 {
		return "?"
	}
	frames := runtime.CallersFrames([]uintptr{pc})
	frame, _ := frames.Next()
	file := frame.File
	if i := strings.LastIndex(file, "/"); i > 0 {
		if j := strings.LastIndex(file[:i], "/"); j >= 0 {
			file = file[j+1:]
		}
	}
	return fmt.Sprintf("%s:%d", file, frame.Line)
}

func (h *consoleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	nh := *h
	nh.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &nh
}

func (h *consoleHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	nh := *h
	nh.groups = append(append([]string{}, h.groups...), name)
	return &nh
}
