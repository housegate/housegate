package snapshotquery

import (
	"context"
	"errors"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

// historicalSourceFake models the source as a completed authentication
// operation: every gate failure is an error, so the adapter cannot mistake a
// source self-report for endpoint authentication or proof verification.
type historicalSourceFake struct {
	records HistoricalDecisionRecords
	err     error
	seen    []historicalDecisionRequest
	hook    func(historicalDecisionRequest)
}

func (s *historicalSourceFake) loadAuthenticatedHistoricalDecision(_ context.Context, request historicalDecisionRequest) (HistoricalDecisionRecords, error) {
	s.seen = append(s.seen, request)
	if s.hook != nil {
		s.hook(request)
	}
	return s.records, s.err
}

func TestAuthenticatedHistoricalPolicyAcceptsOnlyCompletedSourceRecord(t *testing.T) {
	job, records := historicalFixture(t)
	source := &historicalSourceFake{records: records}
	policy, err := newAuthenticatedHistoricalPolicy(source)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := policy.VerifyReservation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.CheckJob(job); err != nil {
		t.Fatal(err)
	}
	if len(source.seen) != 1 {
		t.Fatalf("source calls = %d, want 1", len(source.seen))
	}
	request := source.seen[0]
	if request.NetworkID() != job.Reservation.ReadSnapshot.NetworkID || request.KeeperShardID() != job.Reservation.ReadSnapshot.KeeperShardID || request.ExecutorProfileID() != job.ExecutorProfileID || request.QueryProfileID() != job.QueryProfileID {
		t.Fatal("source scope did not bind exact job semantics")
	}
	got := request.Job()
	gotRoot, err := replay.SnapshotQueryStatementRoot(got.Statement)
	wantRoot, err := replay.SnapshotQueryStatementRoot(job.Statement)
	if err != nil {
		t.Fatal(err)
	}
	if gotRoot != wantRoot || got.Reservation != job.Reservation || got.BlockSeq != job.BlockSeq || got.PrevSafeSnapshotID != job.PrevSafeSnapshotID || got.PrevStateRoot != job.PrevStateRoot || got.SchemaSnapshotID != job.SchemaSnapshotID || got.ExecutorProfileID != job.ExecutorProfileID || got.QueryProfileID != job.QueryProfileID {
		t.Fatal("source identity did not bind exact job")
	}
}

func TestAuthenticatedHistoricalPolicyRejectsEverySourceGateFailure(t *testing.T) {
	job, _ := historicalFixture(t)
	for _, gate := range []string{"endpoint authentication", "authorized endpoint role", "leader barrier", "committed proof", "not found"} {
		t.Run(gate, func(t *testing.T) {
			policy, err := newAuthenticatedHistoricalPolicy(&historicalSourceFake{err: errors.New(gate)})
			if err != nil {
				t.Fatal(err)
			}
			if decision, err := policy.VerifyReservation(context.Background(), job); err == nil || decision != (HistoricalDecision{}) {
				t.Fatalf("failed source gate accepted: %+v", decision)
			}
		})
	}
}

func TestAuthenticatedHistoricalPolicyRejectsInvalidRecordAfterSourceCompletion(t *testing.T) {
	job, _ := historicalFixture(t)
	policy, err := newAuthenticatedHistoricalPolicy(&historicalSourceFake{records: HistoricalDecisionRecords{}})
	if err != nil {
		t.Fatal(err)
	}
	if decision, err := policy.VerifyReservation(context.Background(), job); err == nil || decision != (HistoricalDecision{}) {
		t.Fatalf("bad source records accepted: %+v", decision)
	}
	if policy, err := newAuthenticatedHistoricalPolicy(nil); err == nil || policy != nil {
		t.Fatal("nil source accepted")
	}
}

func TestAuthenticatedHistoricalPolicyCopiesJobAndRecordsAtSourceBoundary(t *testing.T) {
	job, records := historicalFixture(t)
	source := &historicalSourceFake{records: records}
	source.hook = func(request historicalDecisionRequest) {
		// Mutating the source's request must not alter the verifier-owned job.
		request.job.Statement.Envelope.Input.ReadSet.Tables = append(request.job.Statement.Envelope.Input.ReadSet.Tables, replay.SnapshotReadTable{TableID: "source-only"})
		request.job.SourceClaim = &replay.SnapshotQueryClaim{SourceNode: "source-only"}
	}
	policy, err := newAuthenticatedHistoricalPolicy(source)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := policy.VerifyReservation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.CheckJob(job); err != nil {
		t.Fatal("source mutation altered decision", err)
	}
	source.records.Reservation.ActivationID = "mutated-after-return"
	source.records.Artifacts.Parts = append(source.records.Artifacts.Parts, replay.SnapshotArtifactEntry{PartPhysHash: replay.DigestString("source-only"), ObjectDigest: replay.DigestString("source-object")})
	if err := decision.CheckJob(job); err != nil {
		t.Fatal("source response escaped decision boundary", err)
	}
	if len(job.Statement.Envelope.Input.ReadSet.Tables) != 0 || job.SourceClaim != nil {
		t.Fatal("source mutation escaped request copy")
	}
}

func TestAuthenticatedHistoricalPolicyRejectsCanceledContextBeforeAndAfterSource(t *testing.T) {
	job, records := historicalFixture(t)
	source := &historicalSourceFake{records: records}
	policy, err := newAuthenticatedHistoricalPolicy(source)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = policy.VerifyReservation(ctx, job); !errors.Is(err, context.Canceled) || len(source.seen) != 0 {
		t.Fatalf("pre-cancel: err=%v calls=%d", err, len(source.seen))
	}

	ctx, cancel = context.WithCancel(context.Background())
	source.hook = func(historicalDecisionRequest) { cancel() }
	if _, err = policy.VerifyReservation(ctx, job); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-cancel: %v", err)
	}
}
