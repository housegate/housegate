package payloadexec

import (
	"context"
	"fmt"

	"housegate/housegate/pkg/replay"
)

// MemSnapshotStore is an in-memory replay.SnapshotStore for the MVP and tests.
type MemSnapshotStore struct {
	m map[string]replay.SafeSnapshotManifest
}

// NewMemSnapshotStore returns an empty snapshot store.
func NewMemSnapshotStore() *MemSnapshotStore {
	return &MemSnapshotStore{m: map[string]replay.SafeSnapshotManifest{}}
}

// Put indexes a snapshot by its SnapshotID.
func (s *MemSnapshotStore) Put(snap replay.SafeSnapshotManifest) {
	s.m[snap.SnapshotID] = snap
}

// GetSafeSnapshot returns the snapshot for the given id.
func (s *MemSnapshotStore) GetSafeSnapshot(_ context.Context, snapshotID string) (replay.SafeSnapshotManifest, error) {
	snap, ok := s.m[snapshotID]
	if !ok {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("snapshot %q not found", snapshotID)
	}
	return snap, nil
}

// MemPayloadStore is an in-memory replay.PayloadStore for the MVP and tests.
type MemPayloadStore struct {
	m map[string][]byte
}

// NewMemPayloadStore returns an empty payload store.
func NewMemPayloadStore() *MemPayloadStore {
	return &MemPayloadStore{m: map[string][]byte{}}
}

// Put stores payload bytes under a reference (copied defensively).
func (s *MemPayloadStore) Put(ref string, payload []byte) {
	s.m[ref] = append([]byte(nil), payload...)
}

// GetPayload returns a copy of the payload bytes for the given reference.
func (s *MemPayloadStore) GetPayload(_ context.Context, ref string) ([]byte, error) {
	payload, ok := s.m[ref]
	if !ok {
		return nil, fmt.Errorf("payload %q not found", ref)
	}
	return append([]byte(nil), payload...), nil
}
