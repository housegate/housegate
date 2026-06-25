package storageintegrity

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"housegate/housegate/pkg/replay"
)

// SafeAuditWorker reads safe rows from ClickHouse replicas and computes the
// audit vote hash. SafeAuditCoordinator in HouseKeeper only coordinates votes.
type SafeAuditWorker struct {
	Reader SafeAuditReader
	Signer SafeAuditSigner
	Sink   SafeAuditSink
}

func (w SafeAuditWorker) Audit(ctx context.Context, task SafeAuditTask) (SafeAuditVote, error) {
	if w.Reader == nil {
		return SafeAuditVote{}, fmt.Errorf("safe audit reader is required")
	}
	if w.Signer == nil {
		return SafeAuditVote{}, fmt.Errorf("safe audit signer is required")
	}
	if w.Sink == nil {
		return SafeAuditVote{}, fmt.Errorf("safe audit sink is required")
	}
	rows, err := w.Reader.ReadSafeRows(ctx, task)
	if err != nil {
		return SafeAuditVote{}, fmt.Errorf("read safe rows: %w", err)
	}
	batchHash, err := SafeBatchHash(task.NetworkID, task.TableID, task.SchemaHash, rows)
	if err != nil {
		return SafeAuditVote{}, err
	}
	vote := SafeAuditVote{
		AuditID:    task.AuditID,
		ReplicaID:  task.ReplicaID,
		SnapshotID: task.SnapshotID,
		Range:      task.Range,
		BatchHash:  batchHash,
		RowCount:   uint64(len(rows)),
	}
	voteHash, err := hashDomain("housegate-safe-audit-vote-v1", struct {
		AuditID    string `json:"audit_id"`
		ReplicaID  string `json:"replica_id"`
		SnapshotID string `json:"snapshot_id"`
		Range      string `json:"range"`
		BatchHash  string `json:"batch_hash"`
		RowCount   uint64 `json:"row_count"`
	}{
		AuditID:    vote.AuditID,
		ReplicaID:  vote.ReplicaID,
		SnapshotID: vote.SnapshotID,
		Range:      vote.Range,
		BatchHash:  vote.BatchHash,
		RowCount:   vote.RowCount,
	})
	if err != nil {
		return SafeAuditVote{}, err
	}
	workerID, sig, err := w.Signer.SignSafeAuditVote(ctx, voteHash)
	if err != nil {
		return SafeAuditVote{}, fmt.Errorf("sign safe audit vote: %w", err)
	}
	if workerID == "" {
		return SafeAuditVote{}, fmt.Errorf("safe audit signer returned empty worker id")
	}
	if sig == "" {
		return SafeAuditVote{}, fmt.Errorf("safe audit signer returned empty signature")
	}
	vote.WorkerID = workerID
	vote.VoteHash = voteHash
	vote.Signature = sig
	if err := w.Sink.SubmitSafeAuditVote(ctx, vote); err != nil {
		return SafeAuditVote{}, fmt.Errorf("submit safe audit vote: %w", err)
	}
	return vote, nil
}

func SafeBatchHash(networkID, tableID, schemaHash string, rows []SafeRow) (string, error) {
	rowHashes := make([]string, 0, len(rows))
	for _, row := range rows {
		h, err := SafeRowHash(networkID, tableID, schemaHash, row)
		if err != nil {
			return "", err
		}
		rowHashes = append(rowHashes, h)
	}
	sort.Strings(rowHashes)
	return hashDomain("housegate-safe-batch-v1", struct {
		NetworkID  string   `json:"network_id"`
		TableID    string   `json:"table_id"`
		SchemaHash string   `json:"schema_hash"`
		Rows       []string `json:"rows"`
	}{
		NetworkID:  networkID,
		TableID:    tableID,
		SchemaHash: schemaHash,
		Rows:       rowHashes,
	})
}

func SafeRowHash(networkID, tableID, schemaHash string, row SafeRow) (string, error) {
	if row.RowID == "" {
		return "", fmt.Errorf("safe row id is required")
	}
	return hashDomain("housegate-safe-row-v1", struct {
		NetworkID  string `json:"network_id"`
		TableID    string `json:"table_id"`
		SchemaHash string `json:"schema_hash"`
		RowID      string `json:"row_id"`
		Values     []any  `json:"values"`
	}{
		NetworkID:  networkID,
		TableID:    tableID,
		SchemaHash: schemaHash,
		RowID:      row.RowID,
		Values:     row.Values,
	})
}

func hashDomain(domain string, v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("canonicalize %s: %w", domain, err)
	}
	payload := make([]byte, 0, len(domain)+1+len(b))
	payload = append(payload, domain...)
	payload = append(payload, 0)
	payload = append(payload, b...)
	return replay.DigestBytes(payload), nil
}
