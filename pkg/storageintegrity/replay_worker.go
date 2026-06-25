package storageintegrity

import (
	"context"
	"fmt"

	"housegate/housegate/pkg/replay"
)

// ReplayWorker is the HouseGate-side worker that performs replay verification
// and submits the signed attestation back to HouseKeeper.
type ReplayWorker struct {
	Verifier ReplayVerifier
	Sink     ReplaySink
}

func (w ReplayWorker) VerifyAndSubmit(ctx context.Context, job replay.ReplayJob) error {
	if w.Verifier == nil {
		return fmt.Errorf("replay verifier is required")
	}
	if w.Sink == nil {
		return fmt.Errorf("replay sink is required")
	}
	att, err := w.Verifier.Verify(ctx, job)
	if err != nil {
		failure := ReplayFailure{BlockSeq: job.BlockSeq, Error: err.Error()}
		if sinkErr := w.Sink.SubmitReplayFailure(ctx, failure); sinkErr != nil {
			return fmt.Errorf("replay failed: %v; submit failure: %w", err, sinkErr)
		}
		return err
	}
	if err := w.Sink.SubmitReplayAttestation(ctx, att); err != nil {
		return fmt.Errorf("submit replay attestation: %w", err)
	}
	return nil
}
