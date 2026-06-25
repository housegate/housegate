package storageintegrity

import (
	"context"
	"testing"
	"time"
)

func TestMockFinalityWatcherEmitsImmediateFinality(t *testing.T) {
	ctx := context.Background()
	sink := &recordingFinalitySink{}
	watcher := MockFinalityWatcher{
		Delay: 0,
		Now:   func() time.Time { return time.Unix(100, 0).UTC() },
		Sink:  sink,
	}

	rec, err := watcher.Finalize(ctx, FinalityRequest{
		BatchID:     "batch-1",
		StatementID: "stmt-1",
		PayloadRef:  "mockda://table/stmt/hash",
		PayloadHash: "0xabc",
	})
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if rec.Kind != "mock" || !rec.Finalized {
		t.Fatalf("record = %#v, want finalized mock record", rec)
	}
	if rec.FinalizedAt != time.Unix(100, 0).UTC() {
		t.Fatalf("finalized_at = %s", rec.FinalizedAt)
	}
	if len(sink.records) != 1 || sink.records[0].BatchID != "batch-1" {
		t.Fatalf("sink records = %#v", sink.records)
	}
}

func TestMockFinalityWatcherHonorsContextDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	watcher := MockFinalityWatcher{
		Delay: time.Hour,
		Sink:  &recordingFinalitySink{},
	}

	if _, err := watcher.Finalize(ctx, FinalityRequest{BatchID: "batch-1"}); err == nil {
		t.Fatal("expected canceled context error")
	}
}

type recordingFinalitySink struct {
	records []FinalityRecord
}

func (s *recordingFinalitySink) SubmitFinality(_ context.Context, rec FinalityRecord) error {
	s.records = append(s.records, rec)
	return nil
}
