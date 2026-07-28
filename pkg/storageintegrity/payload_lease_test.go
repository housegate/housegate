package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type sequencePayloadWriter struct {
	mu      sync.Mutex
	results []PayloadPutResult
	calls   int
}

func (w *sequencePayloadWriter) PutPayload(context.Context, []byte, string, uint64) (PayloadPutResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if len(w.results) == 0 {
		return PayloadPutResult{}, errors.New("no payload result configured")
	}
	idx := w.calls - 1
	if idx >= len(w.results) {
		idx = len(w.results) - 1
	}
	return w.results[idx], nil
}

func (w *sequencePayloadWriter) callCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

func newLeaseSupervisorFixture(t *testing.T, remote PayloadWriter) *PayloadLeaseSupervisor {
	t.Helper()
	spool, err := NewFilePayloadSpool(t.TempDir())
	if err != nil {
		t.Fatalf("NewFilePayloadSpool: %v", err)
	}
	writer := NewSpoolingPayloadWriterWithLeasePolicy(spool, remote, time.Minute)
	return NewPayloadLeaseSupervisor(writer, time.Second)
}

func TestPayloadLeaseSupervisorRefreshesTrackedPayload(t *testing.T) {
	remote := &sequencePayloadWriter{results: []PayloadPutResult{{
		PayloadRef:         "payload://store/ref-1",
		State:              PayloadStateAvailable,
		LeaseExpiresUnixMS: uint64(time.Now().Add(5 * time.Second).UnixMilli()),
	}}}
	supervisor := newLeaseSupervisorFixture(t, remote)
	adm := admissionFixture()
	adm.PayloadRef = "payload://store/ref-1"

	if err := supervisor.EnsurePayloadLease(context.Background(), adm, adm.PayloadRef); err != nil {
		t.Fatalf("EnsurePayloadLease: %v", err)
	}
	if err := supervisor.refreshTracked(context.Background()); err != nil {
		t.Fatalf("refreshTracked: %v", err)
	}
	if got := remote.callCount(); got != 2 {
		t.Fatalf("remote calls = %d, want 2", got)
	}
}

func TestPayloadLeaseSupervisorRejectsChangedPayloadRef(t *testing.T) {
	remote := &sequencePayloadWriter{results: []PayloadPutResult{
		{
			PayloadRef:         "payload://store/ref-1",
			State:              PayloadStateAvailable,
			LeaseExpiresUnixMS: uint64(time.Now().Add(5 * time.Second).UnixMilli()),
		},
		{
			PayloadRef:         "payload://store/ref-2",
			State:              PayloadStateAvailable,
			LeaseExpiresUnixMS: uint64(time.Now().Add(time.Hour).UnixMilli()),
		},
	}}
	supervisor := newLeaseSupervisorFixture(t, remote)
	adm := admissionFixture()
	adm.PayloadRef = "payload://store/ref-1"

	if err := supervisor.EnsurePayloadLease(context.Background(), adm, adm.PayloadRef); err != nil {
		t.Fatalf("EnsurePayloadLease: %v", err)
	}
	err := supervisor.refreshTracked(context.Background())
	if err == nil || !strings.Contains(err.Error(), "payload_ref changed") {
		t.Fatalf("refreshTracked err = %v, want payload_ref change", err)
	}
}

func TestPayloadLeaseSupervisorReleaseStopsRefresh(t *testing.T) {
	remote := &sequencePayloadWriter{results: []PayloadPutResult{{
		PayloadRef:         "payload://store/ref-1",
		State:              PayloadStateAvailable,
		LeaseExpiresUnixMS: uint64(time.Now().Add(5 * time.Second).UnixMilli()),
	}}}
	supervisor := newLeaseSupervisorFixture(t, remote)
	adm := admissionFixture()
	adm.PayloadRef = "payload://store/ref-1"

	if err := supervisor.EnsurePayloadLease(context.Background(), adm, adm.PayloadRef); err != nil {
		t.Fatalf("EnsurePayloadLease: %v", err)
	}
	supervisor.ReleasePayloadLease(adm.PayloadHash)
	if err := supervisor.refreshTracked(context.Background()); err != nil {
		t.Fatalf("refreshTracked after release: %v", err)
	}
	if got := remote.callCount(); got != 1 {
		t.Fatalf("remote calls = %d, want 1", got)
	}
}
