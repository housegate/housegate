package storageintegrity

import (
	"context"
	"fmt"
	"time"
)

// MockFinalityWatcher turns a verified batch into a mock finalized marker. It
// deliberately does not talk to DA/L2; the record is tagged kind=mock so callers
// cannot confuse it with a real anchor.
type MockFinalityWatcher struct {
	Delay time.Duration
	Now   func() time.Time
	Sink  FinalitySink
}

func (w MockFinalityWatcher) Finalize(ctx context.Context, req FinalityRequest) (FinalityRecord, error) {
	if w.Delay < 0 {
		return FinalityRecord{}, fmt.Errorf("mock finality delay must be >= 0")
	}
	if w.Delay > 0 {
		timer := time.NewTimer(w.Delay)
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
	if w.Now != nil {
		now = w.Now()
	}
	rec := FinalityRecord{
		Kind:        "mock",
		BatchID:     req.BatchID,
		StatementID: req.StatementID,
		PayloadRef:  req.PayloadRef,
		PayloadHash: req.PayloadHash,
		Finalized:   true,
		FinalizedAt: now.UTC(),
	}
	if w.Sink != nil {
		if err := w.Sink.SubmitFinality(ctx, rec); err != nil {
			return FinalityRecord{}, fmt.Errorf("submit mock finality: %w", err)
		}
	}
	return rec, nil
}
