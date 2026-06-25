package storageintegrity

import (
	"context"
	"testing"
)

func TestSafeAuditWorkerComputesStableBatchHashAndSubmitsVote(t *testing.T) {
	ctx := context.Background()
	reader := &recordingSafeAuditReader{
		rows: []SafeRow{
			{RowID: "row-2", Values: []any{"bob", uint64(20)}},
			{RowID: "row-1", Values: []any{"alice", uint64(10)}},
		},
	}
	sink := &recordingSafeAuditSink{}
	signer := &recordingSafeAuditSigner{workerID: "worker-a", signature: "sig-a"}
	worker := SafeAuditWorker{Reader: reader, Signer: signer, Sink: sink}
	task := SafeAuditTask{
		AuditID:    "audit-1",
		ReplicaID:  "replica-a",
		NetworkID:  "net-1",
		TableID:    "Transfer",
		SchemaHash: "0xschema",
		SnapshotID: "snapshot-1",
		Range:      "partition=202606",
	}

	vote, err := worker.Audit(ctx, task)
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if vote.WorkerID != "worker-a" || vote.Signature != "sig-a" {
		t.Fatalf("vote signature fields = %#v", vote)
	}
	if vote.RowCount != 2 {
		t.Fatalf("row count = %d, want 2", vote.RowCount)
	}
	if vote.BatchHash == "" || signer.seenHash != vote.VoteHash {
		t.Fatalf("hashes not populated: vote=%#v signer_hash=%q", vote, signer.seenHash)
	}
	if len(sink.votes) != 1 || sink.votes[0].BatchHash != vote.BatchHash {
		t.Fatalf("sink votes = %#v", sink.votes)
	}

	reversed := []SafeRow{reader.rows[1], reader.rows[0]}
	reader.rows = reversed
	vote2, err := worker.Audit(ctx, task)
	if err != nil {
		t.Fatalf("Audit reversed: %v", err)
	}
	if vote2.BatchHash != vote.BatchHash {
		t.Fatalf("batch hash changed after row reorder: %s vs %s", vote.BatchHash, vote2.BatchHash)
	}
}

type recordingSafeAuditReader struct {
	rows []SafeRow
}

func (r *recordingSafeAuditReader) ReadSafeRows(_ context.Context, task SafeAuditTask) ([]SafeRow, error) {
	out := append([]SafeRow(nil), r.rows...)
	return out, nil
}

type recordingSafeAuditSink struct {
	votes []SafeAuditVote
}

func (s *recordingSafeAuditSink) SubmitSafeAuditVote(_ context.Context, vote SafeAuditVote) error {
	s.votes = append(s.votes, vote)
	return nil
}

type recordingSafeAuditSigner struct {
	workerID  string
	signature string
	seenHash  string
}

func (s *recordingSafeAuditSigner) SignSafeAuditVote(_ context.Context, voteHash string) (workerID, signature string, err error) {
	s.seenHash = voteHash
	return s.workerID, s.signature, nil
}
