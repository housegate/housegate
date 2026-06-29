package storageintegrity

import (
	"context"
	"testing"
	"time"
)

func TestMockExternalFinalityServiceSubmitsImmediateEvent(t *testing.T) {
	ctx := context.Background()
	sink := &recordingFinalitySink{}
	service := MockExternalFinalityService{
		Delay: 0,
		Now:   func() time.Time { return time.Unix(100, 0).UTC() },
		Sink:  sink,
	}

	rec, err := service.Submit(ctx, FinalityEvent{
		Kind:        "mock",
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

func TestMockExternalFinalityServiceHonorsContextDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	service := MockExternalFinalityService{
		Delay: time.Hour,
		Sink:  &recordingFinalitySink{},
	}

	if _, err := service.Submit(ctx, FinalityEvent{BatchID: "batch-1"}); err == nil {
		t.Fatal("expected canceled context error")
	}
}

func TestMockExternalRollbackServiceSubmitsRollbackEvent(t *testing.T) {
	ctx := context.Background()
	sink := &recordingRollbackSink{}
	service := MockExternalRollbackService{
		Now:  func() time.Time { return time.Unix(200, 0).UTC() },
		Sink: sink,
	}

	event, err := service.Submit(ctx, RollbackEvent{
		Kind:        "mock",
		BatchID:     "batch-1",
		StatementID: "stmt-1",
		Reason:      "dispute",
	})
	if err != nil {
		t.Fatalf("Submit rollback: %v", err)
	}
	if event.Kind != "mock" || event.ReceivedAt != time.Unix(200, 0).UTC() {
		t.Fatalf("rollback event = %#v", event)
	}
	if len(sink.events) != 1 || sink.events[0].Reason != "dispute" {
		t.Fatalf("rollback events = %#v", sink.events)
	}
}

type recordingFinalitySink struct {
	records []FinalityRecord
}

func (s *recordingFinalitySink) SubmitFinality(_ context.Context, rec FinalityRecord) error {
	s.records = append(s.records, rec)
	return nil
}

type recordingRollbackSink struct {
	events []RollbackEvent
}

func (s *recordingRollbackSink) SubmitRollback(_ context.Context, event RollbackEvent) error {
	s.events = append(s.events, event)
	return nil
}

func (s *recordingRollbackSink) FinishRollback(context.Context, RollbackResult) error {
	return nil
}

func (s *recordingRollbackSink) FailRollback(context.Context, RollbackFailure) error {
	return nil
}
