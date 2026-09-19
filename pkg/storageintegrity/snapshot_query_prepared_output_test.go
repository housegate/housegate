package storageintegrity

import (
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

func TestPreparedOutputProjectionCannotBeClaim(t *testing.T) {
	env := snapshotQueryEnvelopeFixture(t)
	accepted := replay.SnapshotQuerySubmitResult{AdmissionCode: 0, StatementSeq: 1, BlockSeq: 9, SourceNode: "source", InputRoot: env.InputRoot, Reservation: replay.SnapshotQueryReservation{ReservationID: env.Input.Binding.ReservationID, FencingGeneration: env.Input.Binding.FencingGeneration, ClientAccount: env.Input.Binding.ClientAccount, StatementID: env.Input.Binding.StatementID, ReadSnapshot: env.Input.Binding.ReadSnapshot, ExecutorProfileID: env.Input.Binding.ExecutorProfileID, QueryProfileID: env.Input.Binding.QueryProfileID, ActivationID: "active"}}
	base := SnapshotQueryPrepared{StatementID: env.Input.Binding.StatementID, InputRoot: env.InputRoot, BlockSeq: 9, FencingGeneration: env.Input.Binding.FencingGeneration, OutputRowsRoot: replay.DigestString("output"), ComputedStateRoot: replay.DigestString("state"), Status: "PendingUnsubmitted", Candidates: []replay.SnapshotReadPart{}, Stage: string(SnapshotQueryStagePreparedOutput), CachePath: "cache", CacheDigest: replay.DigestString("cache")}
	if err := validateSnapshotQueryPrepared(env, accepted, base); err != nil {
		t.Fatalf("valid pending projection: %v", err)
	}
	for name, alter := range map[string]func(*SnapshotQueryPrepared){
		"claim root": func(p *SnapshotQueryPrepared) { p.SourceClaimRoot = replay.DigestString("claim") },
		"candidate":  func(p *SnapshotQueryPrepared) { p.Candidates = []replay.SnapshotReadPart{{TableID: "t"}} },
		"capacity":   func(p *SnapshotQueryPrepared) { p.CapacityReservationID = "capacity" },
		"status":     func(p *SnapshotQueryPrepared) { p.Status = "Committed" },
	} {
		t.Run(name, func(t *testing.T) {
			p := base
			alter(&p)
			if err := validateSnapshotQueryPrepared(env, accepted, p); err == nil {
				t.Fatal("claim-like prepared projection was accepted")
			}
		})
	}
}
