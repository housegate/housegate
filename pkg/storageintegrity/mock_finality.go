package storageintegrity

import (
	"context"
	"fmt"
	"time"
)

// MockExternalFinalityService is the P0 stand-in for an external DA/L2 watcher.
// It submits a mock event into HouseKeeper; it does not consume HouseKeeper
// finality tasks and does not call HouseGate workers directly.
type MockExternalFinalityService struct {
	Delay time.Duration
	Now   func() time.Time
	Sink  FinalitySink
}

func (s MockExternalFinalityService) Submit(ctx context.Context, event FinalityEvent) (FinalityRecord, error) {
	if s.Delay < 0 {
		return FinalityRecord{}, fmt.Errorf("mock finality delay must be >= 0")
	}
	if s.Delay > 0 {
		timer := time.NewTimer(s.Delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return FinalityRecord{}, ctx.Err()
		case <-timer.C:
		}
	} else if err := ctx.Err(); err != nil {
		return FinalityRecord{}, err
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now()
	}
	if event.Kind == "" {
		event.Kind = "mock"
	}
	rec := FinalityRecord{
		Kind:        event.Kind,
		BatchID:     event.BatchID,
		StatementID: event.StatementID,
		PayloadRef:  event.PayloadRef,
		PayloadHash: event.PayloadHash,
		Finalized:   true,
		FinalizedAt: now.UTC(),
	}
	if s.Sink != nil {
		if err := s.Sink.SubmitFinality(ctx, rec); err != nil {
			return FinalityRecord{}, fmt.Errorf("submit mock finality: %w", err)
		}
	}
	return rec, nil
}

// MockExternalRollbackService is the P0 stand-in for an external dispute or
// rollback watcher. It submits rollback events into HouseKeeper only.
type MockExternalRollbackService struct {
	Delay time.Duration
	Now   func() time.Time
	Sink  RollbackSink
}

func (s MockExternalRollbackService) Submit(ctx context.Context, event RollbackEvent) (RollbackEvent, error) {
	if s.Delay < 0 {
		return RollbackEvent{}, fmt.Errorf("mock rollback delay must be >= 0")
	}
	if s.Delay > 0 {
		timer := time.NewTimer(s.Delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return RollbackEvent{}, ctx.Err()
		case <-timer.C:
		}
	} else if err := ctx.Err(); err != nil {
		return RollbackEvent{}, err
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now()
	}
	if event.Kind == "" {
		event.Kind = "mock"
	}
	event.ReceivedAt = now.UTC()
	if s.Sink != nil {
		if err := s.Sink.SubmitRollback(ctx, event); err != nil {
			return RollbackEvent{}, fmt.Errorf("submit mock rollback: %w", err)
		}
	}
	return event, nil
}
