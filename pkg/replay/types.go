package replay

import (
	"fmt"
	"sort"
)

// PayloadFormatClickHouseNativeData is the only production payload format
// accepted by the envelope-v2 verifier. Legacy/test decoders live below that
// verifier boundary and must not be able to produce signed receipts.
const PayloadFormatClickHouseNativeData = "clickhouse-native-data-v1"

// SafeSnapshotManifest is the replay verifier's immutable view of a previous
// safe state. It is a manifest of stable table/part commitments, not a
// ClickHouse filesystem backup.
type SafeSnapshotManifest struct {
	SnapshotID        string          `json:"snapshot_id"`
	ParentSnapshotID  string          `json:"parent_snapshot_id,omitempty"`
	SafeBlockSeq      uint64          `json:"safe_block_seq"`
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
	// TableSetTransition is set only for a table-set transition block (dynamic
	// SI table set, spec D7). Such a job carries no statements and no
	// source_claim_root. JSON tags are frozen against arbiter-proto
	// replay.proto ReplayJob fields 8-9; omitempty keeps every job without a
	// transition byte-identical to its pre-transition encoding.
	TableSetTransition *ReplayTableSetTransition `json:"table_set_transition,omitempty"`
	// TableSchemas carries the schemas of the chain-origin tables the job's
	// statements target, so a verifier needs no registry access. Sorted by
	// table id; tables configured locally (genesis) are not repeated here.
	TableSchemas []ReplayTableSchema `json:"table_schemas,omitempty"`
}

// ReplayTableSchema is one table's declared schema as the arbiter registry
// committed it: the payloadexec.TableSchema JSON, verbatim. The verifier
// decodes it and recomputes the schema hash itself; it never trusts a hash
// carried next to the JSON.
type ReplayTableSchema struct {
	TableID    string `json:"table_id"`
	SchemaJSON string `json:"schema_json"`
}

// ReplayTableSetTransition is the table-set change of a zero-statement
// transition block: Retires leave the state root, Adds enter it with no data,
// and NewSchemaRoot is the schema root of the resulting table set. Adds are
// sorted by table id, Retires ascending, and the two are disjoint.
type ReplayTableSetTransition struct {
	Adds          []ReplayTableSchema `json:"adds,omitempty"`
	Retires       []string            `json:"retires,omitempty"`
	NewSchemaRoot string              `json:"new_schema_root"`
}

// Validate checks the transition's canonical shape. It does not decode the
// added schemas or check them against a base; the executor does that.
func (t ReplayTableSetTransition) Validate() error {
	if t.NewSchemaRoot == "" {
		return fmt.Errorf("table_set_transition.new_schema_root is required")
	}
	if len(t.Adds) == 0 && len(t.Retires) == 0 {
		return fmt.Errorf("table_set_transition must add or retire at least one table")
	}
	if err := ValidateReplayTableSchemas("table_set_transition.adds", t.Adds); err != nil {
		return err
	}
	added := make(map[string]bool, len(t.Adds))
	for _, add := range t.Adds {
		added[add.TableID] = true
	}
	previous := ""
	for i, id := range t.Retires {
		if id == "" || id <= previous {
			return fmt.Errorf("table_set_transition.retires[%d]: table ids must be sorted, unique and non-empty", i)
		}
		if added[id] {
			return fmt.Errorf("table_set_transition: table %q is both added and retired", id)
		}
		previous = id
	}
	return nil
}

// ValidateReplayTableSchemas checks that schemas are sorted by table id,
// unique, and each carries its JSON.
func ValidateReplayTableSchemas(field string, schemas []ReplayTableSchema) error {
	previous := ""
	for i, s := range schemas {
		if s.TableID == "" || s.TableID <= previous {
			return fmt.Errorf("%s[%d]: table ids must be sorted, unique and non-empty", field, i)
		}
		if s.SchemaJSON == "" {
			return fmt.Errorf("%s[%d] (%s): schema_json is required", field, i, s.TableID)
		}
		previous = s.TableID
	}
	return nil
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
	// v2 additions (envelope v2): how to decode the payload and which
	// declared schema the signer encoded against. JSON tags are frozen
	// against arbiter-proto replay.proto Statement fields 11-13.
	PayloadFormat  string `json:"payload_format,omitempty"`
	ClientRevision uint32 `json:"client_revision,omitempty"`
	SchemaHash     string `json:"schema_hash,omitempty"`
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
	// Snapshot query extensions are in-process only; never frozen wire/hash fields.
	SnapshotQuery            *SnapshotQueryJob `json:"-"`
	SnapshotQueryReferenceID string            `json:"-"`
}

// ExecutionResult is the pinned executor's result over the scratch state.
type ExecutionResult struct {
	SnapshotQuery             *SnapshotQueryEvidence `json:"-"`
	BlockSeq                  uint64                 `json:"block_seq"`
	PrevSafeSnapshotID        string                 `json:"prev_safe_snapshot_id"`
	PrevStateRoot             string                 `json:"prev_state_root"`
	SchemaSnapshotID          string                 `json:"schema_snapshot_id"`
	ExecutorProfileID         string                 `json:"executor_profile_id"`
	ComputedStateRoot         string                 `json:"computed_state_root"`
	PartitionCommitmentsAfter []PartitionCommitment  `json:"partition_commitments_after,omitempty"`
	AffectedParts             []PartManifestEntry    `json:"affected_parts,omitempty"`
	ReplayLogHash             string                 `json:"replay_log_hash,omitempty"`
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
