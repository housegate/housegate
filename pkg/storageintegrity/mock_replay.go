package storageintegrity

import (
	"context"
	"encoding/json"
	"fmt"

	"housegate/housegate/pkg/replay"
)

type MockReplayVerifier struct {
	ReplicaID string
}

func (v MockReplayVerifier) Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error) {
	if err := ctx.Err(); err != nil {
		return replay.ReplayAttestation{}, err
	}
	replicaID := v.ReplicaID
	if replicaID == "" {
		replicaID = "mock-replay"
	}
	statementRoot := digestJSON("mock-replay-statements", job.Statements)
	payloadRoot := digestJSON("mock-replay-payloads", payloadClaims(job.Statements))
	receipt := replay.ExecutionReceipt{
		BlockSeq:           job.BlockSeq,
		PrevSafeSnapshotID: job.PrevSafeSnapshotID,
		PrevStateRoot:      job.PrevStateRoot,
		SchemaSnapshotID:   job.SchemaSnapshotID,
		ExecutorProfileID:  job.ExecutorProfileID,
		StatementRoot:      statementRoot,
		PayloadRoot:        payloadRoot,
		SourceClaimRoot:    job.SourceClaimRoot,
		ComputedStateRoot:  replay.DigestBytes([]byte(fmt.Sprintf("%s:%s", job.SourceClaimRoot, payloadRoot))),
		MatchSourceRoot:    true,
		ReplayLogHash:      replay.DigestBytes([]byte(fmt.Sprintf("mock-replay:%d", job.BlockSeq))),
	}
	hash, err := receipt.Hash()
	if err != nil {
		return replay.ReplayAttestation{}, err
	}
	return replay.ReplayAttestation{
		ReplicaID:       replicaID,
		Receipt:         receipt,
		ReceiptHash:     hash,
		Signature:       "mock:" + hash,
		MatchSourceRoot: true,
	}, nil
}

type payloadClaim struct {
	StatementID   string `json:"statement_id"`
	PayloadRef    string `json:"payload_ref,omitempty"`
	PayloadHash   string `json:"payload_hash,omitempty"`
	PayloadLength uint64 `json:"payload_length,omitempty"`
}

func payloadClaims(stmts []replay.Statement) []payloadClaim {
	out := make([]payloadClaim, 0, len(stmts))
	for _, stmt := range stmts {
		out = append(out, payloadClaim{
			StatementID:   stmt.StatementID,
			PayloadRef:    stmt.PayloadRef,
			PayloadHash:   stmt.PayloadHash,
			PayloadLength: stmt.PayloadLength,
		})
	}
	return out
}

func digestJSON(domain string, v any) string {
	body, _ := json.Marshal(struct {
		Domain string `json:"domain"`
		Value  any    `json:"value"`
	}{Domain: domain, Value: v})
	return replay.DigestBytes(body)
}
