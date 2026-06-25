package storageintegrity

import (
	"context"
	"testing"
	"time"

	"housegate/housegate/pkg/replay"
)

func TestRuntimePollsReplayJobsUntilContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &singleReplayJobSource{job: replay.ReplayJob{BlockSeq: 99}}
	sink := &cancelingReplaySink{cancel: cancel}
	runtime := Runtime{
		PollInterval: time.Millisecond,
		ReplayJobs:   source,
		Replay: &ReplayWorker{
			Verifier: verifierFunc(func(context.Context, replay.ReplayJob) (replay.ReplayAttestation, error) {
				return replay.ReplayAttestation{ReplicaID: "replica-a", ReceiptHash: "0xhash", Signature: "sig"}, nil
			}),
			Sink: sink,
		},
	}

	if err := runtime.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if source.claims != 1 {
		t.Fatalf("claims = %d, want 1", source.claims)
	}
	if len(sink.attestations) != 1 {
		t.Fatalf("attestations = %#v", sink.attestations)
	}
}

type singleReplayJobSource struct {
	job    replay.ReplayJob
	claims int
}

func (s *singleReplayJobSource) ClaimReplayJob(context.Context) (replay.ReplayJob, bool, error) {
	if s.claims > 0 {
		return replay.ReplayJob{}, false, nil
	}
	s.claims++
	return s.job, true, nil
}

type cancelingReplaySink struct {
	cancel       context.CancelFunc
	attestations []replay.ReplayAttestation
	failures     []ReplayFailure
}

func (s *cancelingReplaySink) SubmitReplayAttestation(_ context.Context, att replay.ReplayAttestation) error {
	s.attestations = append(s.attestations, att)
	s.cancel()
	return nil
}

func (s *cancelingReplaySink) SubmitReplayFailure(_ context.Context, failure ReplayFailure) error {
	s.failures = append(s.failures, failure)
	s.cancel()
	return nil
}
