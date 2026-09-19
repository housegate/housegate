package plugin

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
)

type testSnapshotQueryHostSource struct{ admits atomic.Int32 }

func (s *testSnapshotQueryHostSource) IsSnapshotQuery(*chproto.Query) (bool, error) { return true, nil }
func (s *testSnapshotQueryHostSource) AdmitSnapshotQueryAtHost(context.Context, *chproto.Query) (SnapshotQueryHostAdmission, error) {
	s.admits.Add(1)
	return SnapshotQueryHostAdmission{Run: func(context.Context) error { return nil }, CancelClient: func() {}, MaxControlBytes: 64}, nil
}

type cancelOnceSnapshotQueryHostSource struct {
	started  chan struct{}
	canceled atomic.Int32
}

func (*cancelOnceSnapshotQueryHostSource) IsSnapshotQuery(*chproto.Query) (bool, error) { return true, nil }
func (s *cancelOnceSnapshotQueryHostSource) AdmitSnapshotQueryAtHost(context.Context, *chproto.Query) (SnapshotQueryHostAdmission, error) {
	return SnapshotQueryHostAdmission{
		Run: func(ctx context.Context) error {
			close(s.started)
			<-ctx.Done()
			return ctx.Err()
		},
		CancelClient:    func() { s.canceled.Add(1) },
		MaxControlBytes: 64,
	}, nil
}

func TestSnapshotQueryHostPlugin_DefersAdmissionUntilQueryOnlyRun(t *testing.T) {
	source := &testSnapshotQueryHostSource{}
	host, err := NewSnapshotQueryHostPlugin(source)
	if err != nil {
		t.Fatalf("NewSnapshotQueryHostPlugin: %v", err)
	}
	qctx := &QueryContext{Session: newFakeSession(), Query: &chproto.Query{Body: "INSERT INTO t SELECT 1"}, Values: map[string]any{}}
	if err := host.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.QueryOnly == nil || !qctx.QueryOnly.ValidHostPlan() {
		t.Fatal("host did not install private query-only plan")
	}
	if source.admits.Load() != 0 {
		t.Fatalf("OnQuery performed blocking admission %d times", source.admits.Load())
	}
	if err := qctx.QueryOnly.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if source.admits.Load() != 1 {
		t.Fatalf("Run admissions=%d, want 1", source.admits.Load())
	}
}

func TestSnapshotQueryHostPlugin_CancelClientAndContextCancelInvokeAdmissionCancelOnce(t *testing.T) {
	source := &cancelOnceSnapshotQueryHostSource{started: make(chan struct{})}
	host, err := NewSnapshotQueryHostPlugin(source)
	if err != nil {
		t.Fatalf("NewSnapshotQueryHostPlugin: %v", err)
	}
	qctx := &QueryContext{Session: newFakeSession(), Query: &chproto.Query{Body: "INSERT INTO t SELECT 1"}, Values: map[string]any{}}
	if err := host.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- qctx.QueryOnly.Run(ctx) }()
	<-source.started
	cancel()
	qctx.QueryOnly.CancelClient()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v, want context.Canceled", err)
	}
	if got := source.canceled.Load(); got != 1 {
		t.Fatalf("CancelClient calls=%d, want 1", got)
	}
}
