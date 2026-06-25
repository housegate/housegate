package storageintegrity

import (
	"context"
	"testing"

	"housegate/housegate/pkg/replay"
)

func TestReplayWorkerSubmitsSignedAttestation(t *testing.T) {
	ctx := context.Background()
	att := replay.ReplayAttestation{ReplicaID: "replica-a", ReceiptHash: "0xreceipt", Signature: "sig"}
	sink := &recordingReplaySink{}
	worker := ReplayWorker{
		Verifier: verifierFunc(func(context.Context, replay.ReplayJob) (replay.ReplayAttestation, error) {
			return att, nil
		}),
		Sink: sink,
	}

	if err := worker.VerifyAndSubmit(ctx, replay.ReplayJob{BlockSeq: 42}); err != nil {
		t.Fatalf("VerifyAndSubmit: %v", err)
	}
	if len(sink.attestations) != 1 || sink.attestations[0].ReceiptHash != "0xreceipt" {
		t.Fatalf("attestations = %#v", sink.attestations)
	}
	if len(sink.failures) != 0 {
		t.Fatalf("unexpected failures: %#v", sink.failures)
	}
}

func TestReplayWorkerReportsVerifierFailure(t *testing.T) {
	ctx := context.Background()
	sink := &recordingReplaySink{}
	worker := ReplayWorker{
		Verifier: verifierFunc(func(context.Context, replay.ReplayJob) (replay.ReplayAttestation, error) {
			return replay.ReplayAttestation{}, errReplayForTest
		}),
		Sink: sink,
	}

	if err := worker.VerifyAndSubmit(ctx, replay.ReplayJob{BlockSeq: 7}); err == nil {
		t.Fatal("expected verifier error")
	}
	if len(sink.failures) != 1 || sink.failures[0].BlockSeq != 7 {
		t.Fatalf("failures = %#v", sink.failures)
	}
}

type recordingReplaySink struct {
	attestations []replay.ReplayAttestation
	failures     []ReplayFailure
}

func (s *recordingReplaySink) SubmitReplayAttestation(_ context.Context, att replay.ReplayAttestation) error {
	s.attestations = append(s.attestations, att)
	return nil
}

func (s *recordingReplaySink) SubmitReplayFailure(_ context.Context, failure ReplayFailure) error {
	s.failures = append(s.failures, failure)
	return nil
}
