package storageintegrity

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"housegate/housegate/pkg/replay"
)

const mockPayloadScheme = "mockda"

// MockPayloadStore is a durable local stand-in for the real Payload / DA store.
// It stores bytes by digest and returns refs that carry table and statement
// identity for debuggability; integrity still comes from the digest.
type MockPayloadStore struct {
	root string
}

func NewMockPayloadStore(root string) (*MockPayloadStore, error) {
	if root == "" {
		return nil, fmt.Errorf("mock payload store root is required")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create mock payload store %q: %w", root, err)
	}
	return &MockPayloadStore{root: root}, nil
}

func (s *MockPayloadStore) PutPayload(ctx context.Context, req PutPayloadRequest) (PayloadCommitment, error) {
	if s == nil {
		return PayloadCommitment{}, fmt.Errorf("mock payload store is nil")
	}
	if err := ctx.Err(); err != nil {
		return PayloadCommitment{}, err
	}
	if req.TableID == "" {
		return PayloadCommitment{}, fmt.Errorf("table_id is required")
	}
	if req.StatementID == "" {
		return PayloadCommitment{}, fmt.Errorf("statement_id is required")
	}
	hash := replay.DigestBytes(req.Payload)
	path := s.pathForHash(hash)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return PayloadCommitment{}, fmt.Errorf("create payload shard dir: %w", err)
	}
	if err := os.WriteFile(path, req.Payload, 0o600); err != nil {
		return PayloadCommitment{}, fmt.Errorf("write payload %s: %w", hash, err)
	}
	ref := (&url.URL{
		Scheme: mockPayloadScheme,
		Host:   url.PathEscape(req.TableID),
		Path:   "/" + url.PathEscape(req.StatementID) + "/" + strings.TrimPrefix(hash, "0x"),
	}).String()
	return PayloadCommitment{Ref: ref, Hash: hash, Length: uint64(len(req.Payload))}, nil
}

func (s *MockPayloadStore) GetPayload(ctx context.Context, ref string) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("mock payload store is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u, err := url.Parse(ref)
	if err != nil {
		return nil, fmt.Errorf("parse payload ref %q: %w", ref, err)
	}
	if u.Scheme != mockPayloadScheme {
		return nil, fmt.Errorf("payload ref %q has scheme %q, want %q", ref, u.Scheme, mockPayloadScheme)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 || parts[1] == "" {
		return nil, fmt.Errorf("payload ref %q is malformed", ref)
	}
	hash := "0x" + parts[1]
	payload, err := os.ReadFile(s.pathForHash(hash))
	if err != nil {
		return nil, fmt.Errorf("read payload %s: %w", hash, err)
	}
	if got := replay.DigestBytes(payload); got != hash {
		return nil, fmt.Errorf("payload digest mismatch for %q: got %s want %s", ref, got, hash)
	}
	return payload, nil
}

func (s *MockPayloadStore) pathForHash(hash string) string {
	trimmed := strings.TrimPrefix(hash, "0x")
	shard := "xx"
	if len(trimmed) >= 2 {
		shard = trimmed[:2]
	}
	return filepath.Join(s.root, shard, trimmed)
}
