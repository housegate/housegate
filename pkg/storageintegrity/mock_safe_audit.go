package storageintegrity

import "context"

type MockSafeAuditReader struct{}

func (MockSafeAuditReader) ReadSafeRows(ctx context.Context, _ SafeAuditTask) ([]SafeRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

type MockSafeAuditSigner struct {
	WorkerID string
}

func (s MockSafeAuditSigner) SignSafeAuditVote(ctx context.Context, voteHash string) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	workerID := s.WorkerID
	if workerID == "" {
		workerID = "mock-safe-audit"
	}
	return workerID, "mock:" + voteHash, nil
}
