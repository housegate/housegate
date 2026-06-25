package storageintegrity

import (
	"context"
	"errors"

	"housegate/housegate/pkg/replay"
)

var errReplayForTest = errors.New("replay failed")

type verifierFunc func(context.Context, replay.ReplayJob) (replay.ReplayAttestation, error)

func (f verifierFunc) Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error) {
	return f(ctx, job)
}
