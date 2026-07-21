package storageintegrity

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// PublicationAckKey identifies one durable per-worker publication ack.
type PublicationAckKey struct {
	MutationID     string
	WorkerID       string
	PublicationSeq uint64
}

func (k PublicationAckKey) String() string {
	return fmt.Sprintf("%s/%s/%d", k.MutationID, k.WorkerID, k.PublicationSeq)
}

// PublicationAckStore durably persists a per-worker PublicationAck keyed by
// (mutation_id, worker_id, publication_seq). Put is idempotent: a duplicate key
// returns the already-stored ack and never re-verifies or re-executes, so a
// worker that durably saved its ack before sending it recovers by returning the
// same ack (design section 4.8). The first Put verifies the ack's readback
// equals the canonical inventory before persisting.
type PublicationAckStore interface {
	Put(ctx context.Context, ack PublicationAck, canonical CanonicalPublicationSet) (PublicationAck, error)
	Get(ctx context.Context, key PublicationAckKey) (PublicationAck, bool, error)
}

// MemPublicationAckStore is an in-memory PublicationAckStore (green today; a
// real durable store lands with the runtime wiring).
type MemPublicationAckStore struct {
	mu   sync.Mutex
	acks map[string]PublicationAck
}

// NewMemPublicationAckStore constructs an empty in-memory ack store.
func NewMemPublicationAckStore() *MemPublicationAckStore {
	return &MemPublicationAckStore{acks: map[string]PublicationAck{}}
}

// Put verifies (on first insert) that the ack is valid and its readback equals
// the canonical inventory, then durably stores a copy and returns it. A repeated
// Put for the same key returns the already-stored ack unchanged, without
// re-verifying or overwriting — the idempotent recovery path.
func (s *MemPublicationAckStore) Put(_ context.Context, ack PublicationAck, canonical CanonicalPublicationSet) (PublicationAck, error) {
	if ack.MutationID == "" || ack.WorkerID == "" || ack.PublicationSeq == 0 {
		return PublicationAck{}, fmt.Errorf("publication ack store: blank key (%s/%s/%d)", ack.MutationID, ack.WorkerID, ack.PublicationSeq)
	}
	key := PublicationAckKey{MutationID: ack.MutationID, WorkerID: ack.WorkerID, PublicationSeq: ack.PublicationSeq}.String()

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.acks[key]; ok {
		return clonePublicationAck(existing), nil
	}
	if err := ack.Valid(); err != nil {
		return PublicationAck{}, err
	}
	if err := verifyReadbackEqualsCanonical(ack, canonical); err != nil {
		return PublicationAck{}, err
	}
	stored := clonePublicationAck(ack)
	s.acks[key] = stored
	return clonePublicationAck(stored), nil
}

// Get returns the stored ack for a key, if present.
func (s *MemPublicationAckStore) Get(_ context.Context, key PublicationAckKey) (PublicationAck, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ack, ok := s.acks[key.String()]
	if !ok {
		return PublicationAck{}, false, nil
	}
	return clonePublicationAck(ack), true, nil
}

// verifyReadbackEqualsCanonical checks the ack's exact active-parts readback
// equals the canonical publication set's parts (design section 4.8: all retained
// readbacks == canonical manifest input). The readback is []CandidatePart (the
// frozen ack type), so the comparison projects the canonical replay.PartManifestEntry
// down to the CandidatePart fields (table/partition/part name/row-lthash/rows/
// bytes) — the physical hash and storage refs are not on CandidatePart. It also
// binds the readback to the same mutation and publication seq. Order-insensitive;
// fail closed on any count or field mismatch.
func verifyReadbackEqualsCanonical(ack PublicationAck, canonical CanonicalPublicationSet) error {
	if ack.MutationID != canonical.MutationID {
		return fmt.Errorf("publication ack: mutation %s != canonical %s", ack.MutationID, canonical.MutationID)
	}
	if ack.PublicationSeq != canonical.PublicationSeq {
		return fmt.Errorf("publication ack: seq %d != canonical %d", ack.PublicationSeq, canonical.PublicationSeq)
	}
	// Expected parts: flatten the canonical REPLACE plans' parts, projected to the
	// CandidatePart fields.
	var expected []CandidatePart
	for _, plan := range canonical.Plans {
		for _, p := range plan.CanonicalParts {
			expected = append(expected, CandidatePart{
				TableID:       p.TableID,
				PartitionID:   p.PartitionID,
				PartName:      p.PartName,
				PartRowLtHash: p.PartRowLtHash,
				RowCount:      p.RowCount,
				Bytes:         p.Bytes,
			})
		}
	}
	got := append([]CandidatePart(nil), ack.ExactActivePartsReadback...)
	if len(got) != len(expected) {
		return fmt.Errorf("publication ack: readback has %d parts, canonical has %d", len(got), len(expected))
	}
	sortCandidateParts(expected)
	sortCandidateParts(got)
	for i := range expected {
		if got[i] != expected[i] {
			return fmt.Errorf("publication ack: readback part %s/%s/%s does not match canonical", got[i].TableID, got[i].PartitionID, got[i].PartName)
		}
	}
	return nil
}

func sortCandidateParts(parts []CandidatePart) {
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].TableID != parts[j].TableID {
			return parts[i].TableID < parts[j].TableID
		}
		if parts[i].PartitionID != parts[j].PartitionID {
			return parts[i].PartitionID < parts[j].PartitionID
		}
		return parts[i].PartName < parts[j].PartName
	})
}

func clonePublicationAck(a PublicationAck) PublicationAck {
	out := a
	out.BasePartitionRoots = append([]PartitionCommitment(nil), a.BasePartitionRoots...)
	out.PostPartitionCommitments = append([]PartitionCommitment(nil), a.PostPartitionCommitments...)
	out.ExactActivePartsReadback = append([]CandidatePart(nil), a.ExactActivePartsReadback...)
	return out
}
