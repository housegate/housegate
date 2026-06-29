package storageintegrity

import (
	"context"
	"fmt"
)

type UnsafeValidationWorker struct {
	Verifier UnsafeTableVerifier
	Sink     UnsafeValidationSink
}

func (w UnsafeValidationWorker) VerifyAndSubmit(ctx context.Context, task UnsafeValidationTask) error {
	if w.Verifier == nil {
		return fmt.Errorf("unsafe validation verifier is required")
	}
	if w.Sink == nil {
		return fmt.Errorf("unsafe validation sink is required")
	}
	result, err := w.Verifier.VerifyUnsafe(ctx, task)
	if err != nil {
		failure := UnsafeValidationFailure{
			ValidationID: task.ValidationID,
			StatementID:  task.StatementID,
			Error:        err.Error(),
		}
		if sinkErr := w.Sink.SubmitUnsafeValidationFailure(ctx, failure); sinkErr != nil {
			return fmt.Errorf("unsafe validation failed: %v; submit failure: %w", err, sinkErr)
		}
		return nil
	}
	if err := w.Sink.SubmitUnsafeValidation(ctx, result); err != nil {
		return fmt.Errorf("submit unsafe validation result: %w", err)
	}
	return nil
}
