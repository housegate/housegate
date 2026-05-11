package log

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// newTestLogger returns a Logger writing to buf with a text handler that
// includes source attribution so tests can assert call-site correctness.
func newTestLogger(buf *bytes.Buffer, level slog.Level) *Logger {
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
		// Stable timestamps so substring checks don't have to special-case time=.
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
	return New(h)
}

func TestLogger_LevelMethods(t *testing.T) {
	tests := []struct {
		name      string
		emit      func(l *Logger)
		wantLevel string
		wantMsg   string
		wantKV    []string
	}{
		{"Info", func(l *Logger) { l.Info("hello", " ", "world") }, "INFO", "hello world", nil},
		{"Infof", func(l *Logger) { l.Infof("rows=%d", 42) }, "INFO", "rows=42", nil},
		{"Infow", func(l *Logger) { l.Infow("done", "rows", 7) }, "INFO", "done", []string{"rows=7"}},
		{"Infoe", func(l *Logger) { l.Infoe(errors.New("boom"), "ctx") }, "INFO", "ctx", []string{"error=boom"}},
		{"Infofe", func(l *Logger) { l.Infofe(errors.New("boom"), "dial %s", "127.0.0.1") }, "INFO", "dial 127.0.0.1", []string{"error=boom"}},
		{"Debug", func(l *Logger) { l.Debugw("d", "k", "v") }, "DEBUG", "d", []string{"k=v"}},
		{"Warn", func(l *Logger) { l.Warnfe(errors.New("x"), "warn %d", 3) }, "WARN", "warn 3", []string{"error=x"}},
		{"Error", func(l *Logger) { l.Errorw("oops", "code", 500) }, "ERROR", "oops", []string{"code=500"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := newTestLogger(&buf, slog.LevelDebug)
			tt.emit(l)
			out := buf.String()
			if !strings.Contains(out, "level="+tt.wantLevel) {
				t.Errorf("missing level=%s in %q", tt.wantLevel, out)
			}
			if !strings.Contains(out, `msg="`+tt.wantMsg+`"`) && !strings.Contains(out, "msg="+tt.wantMsg) {
				t.Errorf("missing msg=%q in %q", tt.wantMsg, out)
			}
			for _, kv := range tt.wantKV {
				if !strings.Contains(out, kv) {
					t.Errorf("missing %q in %q", kv, out)
				}
			}
		})
	}
}

func TestLogger_WithFieldsAccumulate(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelDebug).With("conn", 5)
	l.Infow("hi", "user", "alice")
	out := buf.String()
	if !strings.Contains(out, "conn=5") || !strings.Contains(out, "user=alice") {
		t.Fatalf("expected both fields in %q", out)
	}
}

func TestLogger_LevelFilter(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelWarn)
	l.Infow("filtered")
	l.Warnw("kept")
	out := buf.String()
	if strings.Contains(out, "filtered") {
		t.Errorf("info should be filtered: %q", out)
	}
	if !strings.Contains(out, "kept") {
		t.Errorf("warn should be kept: %q", out)
	}
}

func TestLogger_SourceAttribution(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelDebug)
	l.Infow("trace me")
	out := buf.String()
	// source=foo/log_test.go:LINE — we just check the test file shows up.
	if !strings.Contains(out, "log_test.go") {
		t.Errorf("expected source=log_test.go in %q", out)
	}
}

func TestInfoIf(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelDebug)
	l.InfoIf(false, "skipped")
	l.InfoIf(true, "kept")
	out := buf.String()
	if strings.Contains(out, "skipped") {
		t.Errorf("false branch logged: %q", out)
	}
	if !strings.Contains(out, "kept") {
		t.Errorf("true branch missing: %q", out)
	}
}

func TestInfoEveryN(t *testing.T) {
	resetSamplersForTest()
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelDebug)
	for i := 0; i < 10; i++ {
		l.InfoEveryN("test-everyn", 3, "msg", "i", i)
	}
	// Hits at i=0,3,6,9 => 4 lines.
	got := strings.Count(buf.String(), "msg=msg")
	if got != 4 {
		t.Errorf("expected 4 emissions for everyN=3 over 10 calls, got %d (%q)", got, buf.String())
	}
}

func TestInfoEvery(t *testing.T) {
	resetSamplersForTest()
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelDebug)
	l.InfoEvery("test-every", 10*time.Millisecond, "first")
	l.InfoEvery("test-every", 10*time.Millisecond, "second-suppressed")
	time.Sleep(15 * time.Millisecond)
	l.InfoEvery("test-every", 10*time.Millisecond, "third")
	out := buf.String()
	if !strings.Contains(out, "first") {
		t.Errorf("first missing: %q", out)
	}
	if strings.Contains(out, "second-suppressed") {
		t.Errorf("second should be rate-limited: %q", out)
	}
	if !strings.Contains(out, "third") {
		t.Errorf("third missing after sleep: %q", out)
	}
}

func TestFromContext_DefaultWhenAbsent(t *testing.T) {
	l := From(context.Background())
	if l == nil {
		t.Fatal("From returned nil")
	}
	if l != Default() {
		t.Fatal("expected default logger when ctx has no bound logger")
	}
}

func TestFromContext_EnrichmentRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	base := newTestLogger(&buf, slog.LevelDebug)
	ctx := WithContext(context.Background(), base)

	ctx2, l := FromContext(ctx, "session", 42)
	l.Infow("first")
	if From(ctx2) != l {
		t.Fatal("FromContext should rebind enriched logger to ctx")
	}
	From(ctx2).Infow("second")

	out := buf.String()
	if strings.Count(out, "session=42") != 2 {
		t.Errorf("expected session=42 on both lines, got %q", out)
	}
}

func TestFromContext_NoKVDoesNotRebind(t *testing.T) {
	var buf bytes.Buffer
	base := newTestLogger(&buf, slog.LevelDebug)
	ctx := WithContext(context.Background(), base)

	ctx2, l := FromContext(ctx)
	if ctx2 != ctx {
		t.Fatal("FromContext with no kv should return the same ctx")
	}
	if l != base {
		t.Fatal("FromContext with no kv should return the bound logger as-is")
	}
}

func TestFatal_CallsExit(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelDebug)
	var code int
	origExit := osExit
	osExit = func(c int) { code = c }
	defer func() { osExit = origExit }()

	l.Fatale(errors.New("boom"), "die")
	if code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(buf.String(), "die") {
		t.Errorf("fatal message missing: %q", buf.String())
	}
}

func TestPackageLevel_UsesDefault(t *testing.T) {
	var buf bytes.Buffer
	prev := Default()
	SetDefault(newTestLogger(&buf, slog.LevelDebug))
	defer SetDefault(prev)

	Infow("pkg-level", "k", "v")
	out := buf.String()
	if !strings.Contains(out, "pkg-level") || !strings.Contains(out, "k=v") {
		t.Errorf("package-level Infow not routed to Default: %q", out)
	}
	if !strings.Contains(out, "log_test.go") {
		t.Errorf("source attribution lost on package-level call: %q", out)
	}
}

func TestSetDefault_NilIgnored(t *testing.T) {
	prev := Default()
	SetDefault(nil)
	if Default() != prev {
		t.Fatal("SetDefault(nil) clobbered default")
	}
}

// TestLazy_FuncResolvedWhenEnabled verifies that func() string / func() any
// values are invoked and their result rendered when the level is enabled.
func TestLazy_FuncResolvedWhenEnabled(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelDebug)
	calls := 0
	l.Infow("hello",
		"str", func() string { calls++; return "S" },
		"any", func() any { calls++; return 42 },
	)
	out := buf.String()
	if calls != 2 {
		t.Errorf("expected both lazies invoked, got calls=%d", calls)
	}
	if !strings.Contains(out, "str=S") {
		t.Errorf("func() string result missing: %q", out)
	}
	if !strings.Contains(out, "any=42") {
		t.Errorf("func() any result missing: %q", out)
	}
}

// TestLazy_FuncSkippedWhenFiltered is the headline guarantee: at INFO level,
// a Debug call must NOT invoke the lazy function.
func TestLazy_FuncSkippedWhenFiltered(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelInfo) // Debug filtered out
	calls := 0
	l.Debugw("expensive",
		"plan", func() string { calls++; return "should not run" },
	)
	if calls != 0 {
		t.Fatalf("lazy func invoked while Debug is filtered (calls=%d)", calls)
	}
	if buf.Len() != 0 {
		t.Fatalf("filtered Debug emitted output: %q", buf.String())
	}
}

// TestLazy_AcrossAllVariants covers the format-style and Sprint-style paths
// (Info / Infof) to confirm lazy args feed fmt correctly.
func TestLazy_AcrossAllVariants(t *testing.T) {
	t.Run("Info_Sprint", func(t *testing.T) {
		var buf bytes.Buffer
		l := newTestLogger(&buf, slog.LevelDebug)
		l.Info("rows=", func() string { return "7" })
		if !strings.Contains(buf.String(), "rows=7") {
			t.Errorf("Info did not resolve lazy: %q", buf.String())
		}
	})
	t.Run("Infof_Sprintf", func(t *testing.T) {
		var buf bytes.Buffer
		l := newTestLogger(&buf, slog.LevelDebug)
		l.Infof("plan: %s", func() string { return "scan(t)" })
		if !strings.Contains(buf.String(), "plan: scan(t)") {
			t.Errorf("Infof did not resolve lazy: %q", buf.String())
		}
	})
	t.Run("Infofe_Sprintf", func(t *testing.T) {
		var buf bytes.Buffer
		l := newTestLogger(&buf, slog.LevelDebug)
		l.Infofe(errors.New("e"), "got %d", func() any { return 9 })
		out := buf.String()
		if !strings.Contains(out, "got 9") || !strings.Contains(out, "error=e") {
			t.Errorf("Infofe lazy mismatch: %q", out)
		}
	})
}

// TestLazy_EveryNGateBeforeFunc verifies that *EveryN consults the level gate
// before incrementing the counter or invoking lazies.
func TestLazy_EveryNGateBeforeFunc(t *testing.T) {
	resetSamplersForTest()
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelInfo) // Debug filtered
	calls := 0
	for i := 0; i < 5; i++ {
		l.DebugEveryN("lazy-test", 2, "msg",
			"k", func() string { calls++; return "v" })
	}
	if calls != 0 {
		t.Errorf("EveryN invoked lazy while filtered (calls=%d)", calls)
	}
}

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
		ok   bool
	}{
		{"", slog.LevelInfo, true},
		{"debug", slog.LevelDebug, true},
		{"DEBUG", slog.LevelDebug, true},
		{"  Info  ", slog.LevelInfo, true},
		{"warn", slog.LevelWarn, true},
		{"warning", slog.LevelWarn, true},
		{"error", slog.LevelError, true},
		{"err", slog.LevelError, true},
		{"fatal", LevelFatal, true},
		{"DEBUG+1", slog.LevelDebug + 1, true},
		{"junk", slog.LevelInfo, false},
	}
	for _, c := range cases {
		got, err := ParseLevel(c.in)
		if c.ok && err != nil {
			t.Errorf("ParseLevel(%q) err=%v, want ok", c.in, err)
		}
		if !c.ok && err == nil {
			t.Errorf("ParseLevel(%q) ok, want err", c.in)
		}
		if c.ok && got != c.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestSetLevel_AffectsDefault(t *testing.T) {
	prev := GetLevel()
	defer SetLevel(prev)

	SetLevel(slog.LevelWarn)
	if Enabled(slog.LevelInfo) {
		t.Fatal("Info should be filtered at Warn level")
	}
	if !Enabled(slog.LevelError) {
		t.Fatal("Error should pass at Warn level")
	}

	SetLevel(slog.LevelDebug)
	if !Enabled(slog.LevelDebug) {
		t.Fatal("Debug should pass at Debug level after SetLevel")
	}
}

func TestSetLevel_DoesNotAffectInjectedLogger(t *testing.T) {
	// A Logger built by the caller with a fixed-level handler must NOT
	// be subject to log.SetLevel — SetLevel only governs the default.
	var buf bytes.Buffer
	fixed := New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))

	prev := GetLevel()
	defer SetLevel(prev)
	SetLevel(slog.LevelDebug) // would turn on Debug globally — but fixed should ignore

	fixed.Debugw("filtered")
	if buf.Len() != 0 {
		t.Fatalf("injected logger's handler honored package-level SetLevel: %q", buf.String())
	}
}

func TestDefaultHandler_RenamesFatalLevel(t *testing.T) {
	// The default handler's ReplaceAttr should turn LevelFatal into "FATAL"
	// rather than the numeric "ERROR+4". Swap stderr by replacing the default
	// logger with one writing to buf, configured the same way.
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level:       defaultLevel,
		ReplaceAttr: replaceLevelFatal,
	})
	l := New(h)

	origExit := osExit
	osExit = func(int) {}
	defer func() { osExit = origExit }()

	l.Fatalw("boom")
	out := buf.String()
	if !strings.Contains(out, "level=FATAL") {
		t.Fatalf("expected level=FATAL in %q", out)
	}
}

// TestLazy_LogValuerStillWorks confirms slog's own lazy mechanism (LogValuer)
// continues to work alongside func()-style lazies.
type lazyValuer struct{ called *int }

func (lv lazyValuer) LogValue() slog.Value {
	*lv.called++
	return slog.StringValue("LV")
}

func TestLazy_LogValuerStillWorks(t *testing.T) {
	var buf bytes.Buffer
	l := newTestLogger(&buf, slog.LevelDebug)
	c := 0
	l.Infow("hi", "v", lazyValuer{called: &c})
	if c != 1 {
		t.Errorf("LogValuer called=%d, want 1", c)
	}
	if !strings.Contains(buf.String(), "v=LV") {
		t.Errorf("LogValuer result missing: %q", buf.String())
	}
}
