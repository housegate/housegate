package replay

import (
	"fmt"
	"sort"
)

// SafeSnapshotManifest is the replay verifier's immutable view of a previous
// safe state. It is a manifest of stable table/part commitments, not a
// ClickHouse filesystem backup.
type SafeSnapshotManifest struct {
	SnapshotID        string          `json:"snapshot_id"`
	ParentSnapshotID  string          `json:"parent_snapshot_id,omitempty"`
	SafeL3BlockSeq      uint64          `json:"safe_l3_block_seq"`
	StateRoot         string          `json:"state_root"`
	SchemaSnapshotID  string          `json:"schema_snapshot_id"`
	SchemaRoot        string          `json:"schema_root"`
	ExecutorProfileID string          `json:"executor_profile_id"`
	DataRoot          string          `json:"data_root"`
	ManifestRoot      string          `json:"manifest_root"`
	Tables            []TableManifest `json:"tables"`
}

// TableManifest records the data root for one verified table.
type TableManifest struct {
	TableID        string                `json:"table_id"`
	SchemaHash     string                `json:"schema_hash"`
	PartitionRoots []PartitionCommitment `json:"partition_roots"`
	ActiveParts    []PartManifestEntry   `json:"active_parts"`
}

// PartitionCommitment is the post-replay commitment for one table partition.
type PartitionCommitment struct {
	TableID     string `json:"table_id"`
	PartitionID string `json:"partition_id"`
	Root        string `json:"root"`
}

// PartManifestEntry identifies an immutable safe part by content, not by a
// trusted storage location. StorageRefs are download hints only.
type PartManifestEntry struct {
	TableID       string   `json:"table_id"`
	PartitionID   string   `json:"partition_id"`
	PartName      string   `json:"part_name"`
	PartPhysHash  string   `json:"part_phys_hash"`
	PartRowLtHash string   `json:"part_row_lthash"`
	RowCount      uint64   `json:"row_count"`
	Bytes         uint64   `json:"bytes"`
	StorageRefs   []string `json:"storage_refs,omitempty"`
}

// Seal fills DataRoot, StateRoot, ManifestRoot, and SnapshotID using the MVP
// canonical hashing profile. Callers should populate schema/executor fields
// before sealing.
func (m SafeSnapshotManifest) Seal() (SafeSnapshotManifest, error) {
	m = m.normalized()
	dataRoot, err := m.ComputeDataRoot()
	if err != nil {
		return SafeSnapshotManifest{}, err
	}
	m.DataRoot = dataRoot
	stateRoot, err := m.ComputeStateRoot()
	if err != nil {
		return SafeSnapshotManifest{}, err
	}
	m.StateRoot = stateRoot
	manifestRoot, err := m.ComputeManifestRoot()
	if err != nil {
		return SafeSnapshotManifest{}, err
	}
	m.ManifestRoot = manifestRoot
	if m.SnapshotID == "" {
		m.SnapshotID = manifestRoot
	}
	return m, nil
}

// Validate checks that a manifest is internally self-consistent.
func (m SafeSnapshotManifest) Validate() error {
	if m.SnapshotID == "" {
		return fmt.Errorf("snapshot_id is required")
	}
	if m.StateRoot == "" {
		return fmt.Errorf("state_root is required")
	}
	if m.SchemaSnapshotID == "" {
		return fmt.Errorf("schema_snapshot_id is required")
	}
	if m.SchemaRoot == "" {
		return fmt.Errorf("schema_root is required")
	}
	if m.ExecutorProfileID == "" {
		return fmt.Errorf("executor_profile_id is required")
	}
	if m.ManifestRoot == "" {
		return fmt.Errorf("manifest_root is required")
	}

	n := m.normalized()
	dataRoot, err := n.ComputeDataRoot()
	if err != nil {
		return err
	}
	if m.DataRoot != dataRoot {
		return fmt.Errorf("data_root mismatch: got %s want %s", m.DataRoot, dataRoot)
	}
	stateRoot, err := n.ComputeStateRoot()
	if err != nil {
		return err
	}
	if m.StateRoot != stateRoot {
		return fmt.Errorf("state_root mismatch: got %s want %s", m.StateRoot, stateRoot)
	}
	manifestRoot, err := n.ComputeManifestRoot()
	if err != nil {
		return err
	}
	if m.ManifestRoot != manifestRoot {
		return fmt.Errorf("manifest_root mismatch: got %s want %s", m.ManifestRoot, manifestRoot)
	}
	if err := n.validateSemantics(); err != nil {
		return err
	}
	return nil
}

// validateSemantics checks the structural invariants a hash-consistent manifest
// must also satisfy to be an executable authoritative safe state (HG-P1-04):
// unique table/partition/part keys, required part fields present, active parts
// filed under a real partition of their table, and per-partition row counts
// consistent with the parts. It deliberately does NOT recompute the additive
// partition_root == Σ part_row_lthash equation — that needs the lthash library
// and lives in the storageintegrity layer's semantic validator, keeping
// pkg/replay dependency-free. Operates on a normalized (sorted) manifest.
func (m SafeSnapshotManifest) validateSemantics() error {
	seenTable := map[string]struct{}{}
	for ti := range m.Tables {
		t := m.Tables[ti]
		if t.TableID == "" {
			return fmt.Errorf("table[%d] has empty table_id", ti)
		}
		if _, dup := seenTable[t.TableID]; dup {
			return fmt.Errorf("duplicate table_id %q in manifest", t.TableID)
		}
		seenTable[t.TableID] = struct{}{}

		// Partition roots: unique per (table, partition), non-empty root.
		seenPartition := map[string]struct{}{}
		for _, pr := range t.PartitionRoots {
			if pr.PartitionID == "" {
				return fmt.Errorf("table %q has a partition commitment with empty partition_id", t.TableID)
			}
			if pr.Root == "" {
				return fmt.Errorf("table %q partition %q has empty root", t.TableID, pr.PartitionID)
			}
			if pr.TableID != "" && pr.TableID != t.TableID {
				return fmt.Errorf("table %q partition %q commitment mis-filed under table_id %q", t.TableID, pr.PartitionID, pr.TableID)
			}
			if _, dup := seenPartition[pr.PartitionID]; dup {
				return fmt.Errorf("table %q has duplicate partition commitment for %q", t.TableID, pr.PartitionID)
			}
			seenPartition[pr.PartitionID] = struct{}{}
		}

		// Active parts: unique names, required fields present, filed under a
		// declared partition. (Per-partition row-count totals are not checkable
		// here because PartitionCommitment carries only a root, not a row count;
		// the storageintegrity semantic validator cross-checks the additive
		// root, which subsumes the row content.)
		seenPart := map[string]struct{}{}
		for _, p := range t.ActiveParts {
			if p.PartName == "" {
				return fmt.Errorf("table %q has an active part with empty part_name", t.TableID)
			}
			if p.PartitionID == "" {
				return fmt.Errorf("table %q part %q has empty partition_id", t.TableID, p.PartName)
			}
			if p.PartRowLtHash == "" {
				return fmt.Errorf("table %q part %q has empty part_row_lthash", t.TableID, p.PartName)
			}
			if p.TableID != "" && p.TableID != t.TableID {
				return fmt.Errorf("table %q part %q mis-filed under table_id %q", t.TableID, p.PartName, p.TableID)
			}
			if _, dup := seenPart[p.PartName]; dup {
				return fmt.Errorf("table %q has duplicate active part %q", t.TableID, p.PartName)
			}
			seenPart[p.PartName] = struct{}{}
			// An active part must belong to a declared partition (when partition
			// roots are present at all).
			if len(t.PartitionRoots) > 0 {
				if _, ok := seenPartition[p.PartitionID]; !ok {
					return fmt.Errorf("table %q part %q is in partition %q with no partition commitment", t.TableID, p.PartName, p.PartitionID)
				}
			}
		}
	}
	return nil
}

func (m SafeSnapshotManifest) normalized() SafeSnapshotManifest {
	n := m
	n.Tables = append([]TableManifest(nil), m.Tables...)
	for i := range n.Tables {
		t := &n.Tables[i]
		t.PartitionRoots = append([]PartitionCommitment(nil), t.PartitionRoots...)
		t.ActiveParts = append([]PartManifestEntry(nil), t.ActiveParts...)
		sort.Slice(t.PartitionRoots, func(a, b int) bool {
			if t.PartitionRoots[a].TableID != t.PartitionRoots[b].TableID {
				return t.PartitionRoots[a].TableID < t.PartitionRoots[b].TableID
			}
			return t.PartitionRoots[a].PartitionID < t.PartitionRoots[b].PartitionID
		})
		sort.Slice(t.ActiveParts, func(a, b int) bool {
			if t.ActiveParts[a].TableID != t.ActiveParts[b].TableID {
				return t.ActiveParts[a].TableID < t.ActiveParts[b].TableID
			}
			if t.ActiveParts[a].PartitionID != t.ActiveParts[b].PartitionID {
				return t.ActiveParts[a].PartitionID < t.ActiveParts[b].PartitionID
			}
			return t.ActiveParts[a].PartName < t.ActiveParts[b].PartName
		})
	}
	sort.Slice(n.Tables, func(a, b int) bool { return n.Tables[a].TableID < n.Tables[b].TableID })
	return n
}

// ReplayJob is the verifier input for one sequenced block.
type ReplayJob struct {
	BlockSeq           uint64      `json:"block_seq"`
	PrevSafeSnapshotID string      `json:"prev_safe_snapshot_id"`
	PrevStateRoot      string      `json:"prev_state_root"`
	SchemaSnapshotID   string      `json:"schema_snapshot_id"`
	ExecutorProfileID  string      `json:"executor_profile_id"`
	SourceClaimRoot    string      `json:"source_claim_root"`
	Statements         []Statement `json:"statements"`
}

// Statement is the replay-relevant projection of a signed statement envelope.
type Statement struct {
	StatementID   string `json:"statement_id"`
	StatementSeq  uint64 `json:"statement_seq"`
	SQL           string `json:"sql"`
	SQLHash       string `json:"sql_hash"`
	SettingsHash  string `json:"settings_hash"`
	PayloadRef    string `json:"payload_ref,omitempty"`
	PayloadHash   string `json:"payload_hash,omitempty"`
	PayloadLength uint64 `json:"payload_length,omitempty"`
	TargetTableID string `json:"target_table_id"`
	UserJWS       string `json:"user_jws,omitempty"`
}

// PreparedStatement is passed to the executor after payload hash/length
// validation succeeds.
type PreparedStatement struct {
	Statement
	Payload []byte `json:"-"`
}

// ExecutionRequest is the only input a pinned executor may use.
type ExecutionRequest struct {
	Job        ReplayJob
	Snapshot   SafeSnapshotManifest
	Statements []PreparedStatement
}

// ExecutionResult is the pinned executor's result over the scratch state.
type ExecutionResult struct {
	BlockSeq                  uint64                `json:"block_seq"`
	PrevSafeSnapshotID        string                `json:"prev_safe_snapshot_id"`
	PrevStateRoot             string                `json:"prev_state_root"`
	SchemaSnapshotID          string                `json:"schema_snapshot_id"`
	ExecutorProfileID         string                `json:"executor_profile_id"`
	ComputedStateRoot         string                `json:"computed_state_root"`
	PartitionCommitmentsAfter []PartitionCommitment `json:"partition_commitments_after,omitempty"`
	AffectedParts             []PartManifestEntry   `json:"affected_parts,omitempty"`
	ReplayLogHash             string                `json:"replay_log_hash,omitempty"`
}

// ExecutionReceipt is what a verifier signs. Mismatches are signed too, so
// Keeper can open a challenge with non-repudiable evidence.
type ExecutionReceipt struct {
	BlockSeq                  uint64                `json:"block_seq"`
	PrevSafeSnapshotID        string                `json:"prev_safe_snapshot_id"`
	PrevStateRoot             string                `json:"prev_state_root"`
	SchemaSnapshotID          string                `json:"schema_snapshot_id"`
	ExecutorProfileID         string                `json:"executor_profile_id"`
	StatementRoot             string                `json:"statement_root"`
	PayloadRoot               string                `json:"payload_root"`
	SourceClaimRoot           string                `json:"source_claim_root"`
	ComputedStateRoot         string                `json:"computed_state_root"`
	MatchSourceRoot           bool                  `json:"match_source_root"`
	PartitionCommitmentsAfter []PartitionCommitment `json:"partition_commitments_after,omitempty"`
	AffectedParts             []PartManifestEntry   `json:"affected_parts,omitempty"`
	ReplayLogHash             string                `json:"replay_log_hash,omitempty"`
}

func (r ExecutionReceipt) Hash() (string, error) {
	return canonicalDigest("replay-execution-receipt", r)
}

// ReplayAttestation is the verifier output Keeper stores.
type ReplayAttestation struct {
	ReplicaID       string           `json:"replica_id"`
	Receipt         ExecutionReceipt `json:"receipt"`
	ReceiptHash     string           `json:"receipt_hash"`
	Signature       string           `json:"signature,omitempty"`
	MatchSourceRoot bool             `json:"match_source_root"`
}
