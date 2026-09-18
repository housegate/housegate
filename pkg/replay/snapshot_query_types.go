package replay

// SnapshotQueryEnvelopeVersion and SnapshotQueryStatementKind identify the
// separate signed query lane. Legacy payload records and domains are unchanged.
const (
	SnapshotQueryEnvelopeVersion uint32 = 3
	SnapshotQueryStatementKind   uint32 = 2
	SnapshotQueryInputKind              = "snapshot_query"
)

// SnapshotPin is the frozen snapshot-query contract.
type SnapshotPin struct {
	NetworkID        string `json:"network_id"`
	KeeperShardID    uint32 `json:"keeper_shard_id"`
	SnapshotID       string `json:"snapshot_id"`
	SafeBlockSeq     uint64 `json:"safe_block_seq"`
	ManifestRoot     string `json:"manifest_root"`
	StateRoot        string `json:"state_root"`
	SchemaSnapshotID string `json:"schema_snapshot_id"`
	SchemaRoot       string `json:"schema_root"`
}

// SnapshotReadPart is the frozen snapshot-query contract.
type SnapshotReadPart struct {
	TableID       string `json:"table_id"`
	PartitionID   string `json:"partition_id"`
	PartName      string `json:"part_name"`
	PartPhysHash  string `json:"part_phys_hash"`
	PartRowLtHash string `json:"part_row_lthash"`
	RowCount      uint64 `json:"row_count"`
	Bytes         uint64 `json:"bytes"`
}

// SnapshotReadTable is the frozen snapshot-query contract.
type SnapshotReadTable struct {
	Database       string                `json:"database"`
	Table          string                `json:"table"`
	TableID        string                `json:"table_id"`
	SchemaHash     string                `json:"schema_hash"`
	PartitionRoots []PartitionCommitment `json:"partition_roots"`
	ActiveParts    []SnapshotReadPart    `json:"active_parts"`
}

// SnapshotReadSet is the frozen snapshot-query contract.
type SnapshotReadSet struct {
	ReadSnapshot SnapshotPin         `json:"read_snapshot"`
	Tables       []SnapshotReadTable `json:"tables"`
}

// SnapshotQueryBinding is the frozen snapshot-query contract.
type SnapshotQueryBinding struct {
	EnvelopeVersion   uint32      `json:"envelope_version"`
	InputKind         string      `json:"input_kind"`
	ClientAccount     string      `json:"client_account"`
	StatementID       string      `json:"statement_id"`
	StatementKind     uint32      `json:"statement_kind"`
	NetworkID         string      `json:"network_id"`
	KeeperShardID     uint32      `json:"keeper_shard_id"`
	SQLHash           string      `json:"sql_hash"`
	SettingsHash      string      `json:"settings_hash"`
	TargetTableID     string      `json:"target_table_id"`
	SchemaHash        string      `json:"schema_hash"`
	RowIDProfileID    string      `json:"row_id_profile_id"`
	ClientRevision    uint32      `json:"client_revision"`
	ReadSnapshot      SnapshotPin `json:"read_snapshot"`
	ReadSetRoot       string      `json:"read_set_root"`
	SchemaSnapshotID  string      `json:"schema_snapshot_id"`
	SchemaRoot        string      `json:"schema_root"`
	LogicalDatabase   string      `json:"logical_database"`
	QueryProfileID    string      `json:"query_profile_id"`
	ExecutorProfileID string      `json:"executor_profile_id"`
	ReservationID     string      `json:"reservation_id"`
	FencingGeneration uint64      `json:"fencing_generation"`
}

// SnapshotQueryInput is the frozen snapshot-query contract.
type SnapshotQueryInput struct {
	Binding SnapshotQueryBinding `json:"binding"`
	SQL     string               `json:"sql"`
	ReadSet SnapshotReadSet      `json:"read_set"`
}

// SnapshotQueryEnvelope is the frozen snapshot-query contract.
type SnapshotQueryEnvelope struct {
	Input     SnapshotQueryInput `json:"input"`
	InputRoot string             `json:"input_root"`
	UserJWS   string             `json:"user_jws"`
}

// SnapshotQueryReservation is the frozen snapshot-query contract.
type SnapshotQueryReservation struct {
	ReservationID     string      `json:"reservation_id"`
	FencingGeneration uint64      `json:"fencing_generation"`
	ClientAccount     string      `json:"client_account"`
	StatementID       string      `json:"statement_id"`
	ReadSnapshot      SnapshotPin `json:"read_snapshot"`
	ExecutorProfileID string      `json:"executor_profile_id"`
	QueryProfileID    string      `json:"query_profile_id"`
	ActivationID      string      `json:"activation_id"`
}

// SnapshotQueryReservationStatus is the frozen snapshot-query contract.
type SnapshotQueryReservationStatus struct {
	Version           uint32                    `json:"version"`
	Found             bool                      `json:"found"`
	State             string                    `json:"state"`
	RequestID         string                    `json:"request_id"`
	ClientAccount     string                    `json:"client_account"`
	StatementID       string                    `json:"statement_id"`
	FencingGeneration uint64                    `json:"fencing_generation"`
	Reservation       *SnapshotQueryReservation `json:"reservation"`
	BlockSeq          uint64                    `json:"block_seq"`
	TerminalProof     []byte                    `json:"terminal_proof"`
}

// SnapshotQueryStatement is the frozen snapshot-query contract.
type SnapshotQueryStatement struct {
	StatementSeq uint64                `json:"statement_seq"`
	Envelope     SnapshotQueryEnvelope `json:"envelope"`
}

// SnapshotQueryJob is the frozen snapshot-query contract.
type SnapshotQueryJob struct {
	BlockSeq           uint64                   `json:"block_seq"`
	PrevSafeSnapshotID string                   `json:"prev_safe_snapshot_id"`
	PrevStateRoot      string                   `json:"prev_state_root"`
	SchemaSnapshotID   string                   `json:"schema_snapshot_id"`
	ExecutorProfileID  string                   `json:"executor_profile_id"`
	QueryProfileID     string                   `json:"query_profile_id"`
	Reservation        SnapshotQueryReservation `json:"reservation"`
	Statement          SnapshotQueryStatement   `json:"statement"`
	SourceClaimRoot    string                   `json:"source_claim_root"`
	SourceClaim        *SnapshotQueryClaim      `json:"source_claim"`
}

// SnapshotQueryEvidence is the frozen snapshot-query contract.
type SnapshotQueryEvidence struct {
	ExecutionOutcome string `json:"execution_outcome"`
	OutputRowCount   uint64 `json:"output_row_count"`
	OutputRowsRoot   string `json:"output_rows_root"`
}

// SnapshotQueryReceipt is the frozen snapshot-query contract.
type SnapshotQueryReceipt struct {
	BlockSeq                  uint64                `json:"block_seq"`
	StatementRoot             string                `json:"statement_root"`
	InputRoot                 string                `json:"input_root"`
	ReadSetRoot               string                `json:"read_set_root"`
	ReadSnapshot              SnapshotPin           `json:"read_snapshot"`
	SchemaSnapshotID          string                `json:"schema_snapshot_id"`
	ExecutorProfileID         string                `json:"executor_profile_id"`
	QueryProfileID            string                `json:"query_profile_id"`
	ReservationID             string                `json:"reservation_id"`
	FencingGeneration         uint64                `json:"fencing_generation"`
	ExecutionOutcome          string                `json:"execution_outcome"`
	AbortRecordRoot           string                `json:"abort_record_root"`
	OutputRowCount            uint64                `json:"output_row_count"`
	OutputRowsRoot            string                `json:"output_rows_root"`
	SourceClaimRoot           string                `json:"source_claim_root"`
	ComputedStateRoot         string                `json:"computed_state_root"`
	MatchSourceRoot           bool                  `json:"match_source_root"`
	PartitionCommitmentsAfter []PartitionCommitment `json:"partition_commitments_after"`
	AffectedParts             []PartManifestEntry   `json:"affected_parts"`
	ReplayLogHash             string                `json:"replay_log_hash"`
}

// SnapshotQueryAttestation is the frozen snapshot-query contract.
type SnapshotQueryAttestation struct {
	ReplicaID   string               `json:"replica_id"`
	Receipt     SnapshotQueryReceipt `json:"receipt"`
	ReceiptHash string               `json:"receipt_hash"`
	Signature   string               `json:"signature"`
}

// SnapshotQuerySubmitResult is the frozen snapshot-query contract.
type SnapshotQuerySubmitResult struct {
	AdmissionCode uint32                   `json:"admission_code"`
	Message       string                   `json:"message"`
	StatementSeq  uint64                   `json:"statement_seq"`
	BlockSeq      uint64                   `json:"block_seq"`
	SourceNode    string                   `json:"source_node"`
	InputRoot     string                   `json:"input_root"`
	Reservation   SnapshotQueryReservation `json:"reservation"`
}

// SnapshotQueryStatus is the frozen snapshot-query contract.
type SnapshotQueryStatus struct {
	Version          uint32                    `json:"version"`
	Found            bool                      `json:"found"`
	Accepted         SnapshotQuerySubmitResult `json:"accepted"`
	Lifecycle        string                    `json:"lifecycle"`
	ExecutionOutcome string                    `json:"execution_outcome"`
	TerminalProof    []byte                    `json:"terminal_proof"`
}

// ActiveQueryPolicy is the frozen snapshot-query contract.
type ActiveQueryPolicy struct {
	ActivationID       string `json:"activation_id"`
	NetworkID          string `json:"network_id"`
	KeeperShardID      uint32 `json:"keeper_shard_id"`
	ActivationBlockSeq uint64 `json:"activation_block_seq"`
	ExecutorProfileID  string `json:"executor_profile_id"`
	QueryProfileID     string `json:"query_profile_id"`
	Enabled            bool   `json:"enabled"`
}

// ExecutorProfileTransition is the frozen snapshot-query contract.
type ExecutorProfileTransition struct {
	NetworkID            string            `json:"network_id"`
	KeeperShardID        uint32            `json:"keeper_shard_id"`
	PrevSnapshotID       string            `json:"prev_snapshot_id"`
	PrevStateRoot        string            `json:"prev_state_root"`
	OldExecutorProfileID string            `json:"old_executor_profile_id"`
	NewExecutorProfileID string            `json:"new_executor_profile_id"`
	SchemaSnapshotID     string            `json:"schema_snapshot_id"`
	SchemaRoot           string            `json:"schema_root"`
	DataRoot             string            `json:"data_root"`
	NextSnapshotID       string            `json:"next_snapshot_id"`
	NextStateRoot        string            `json:"next_state_root"`
	NextManifestRoot     string            `json:"next_manifest_root"`
	Activation           ActiveQueryPolicy `json:"activation"`
}

// ExecutorProfileTransitionReceipt is the frozen snapshot-query contract.
type ExecutorProfileTransitionReceipt struct {
	TransitionRoot string `json:"transition_root"`
	ReplicaID      string `json:"replica_id"`
	Signature      string `json:"signature"`
}

// SnapshotArtifactReady is the frozen snapshot-query contract.
type SnapshotArtifactReady struct {
	SnapshotID        string `json:"snapshot_id"`
	ManifestRoot      string `json:"manifest_root"`
	SchemaRoot        string `json:"schema_root"`
	ArtifactSetRoot   string `json:"artifact_set_root"`
	PublisherID       string `json:"publisher_id"`
	RetentionPolicyID string `json:"retention_policy_id"`
}

// SnapshotQueryAbortRecord is the frozen snapshot-query contract.
type SnapshotQueryAbortRecord struct {
	BlockSeq                 uint64 `json:"block_seq"`
	StatementID              string `json:"statement_id"`
	InputRoot                string `json:"input_root"`
	ReservationID            string `json:"reservation_id"`
	FencingGeneration        uint64 `json:"fencing_generation"`
	ReasonCode               string `json:"reason_code"`
	CleanupAuthorizationRoot string `json:"cleanup_authorization_root"`
	PrevSnapshotID           string `json:"prev_snapshot_id"`
	NextSnapshotID           string `json:"next_snapshot_id"`
}

// SnapshotQueryClaim is the frozen snapshot-query contract.
type SnapshotQueryClaim struct {
	SourceNode                string                `json:"source_node"`
	StatementID               string                `json:"statement_id"`
	StatementSeq              uint64                `json:"statement_seq"`
	BlockSeq                  uint64                `json:"block_seq"`
	InputRoot                 string                `json:"input_root"`
	ReservationID             string                `json:"reservation_id"`
	FencingGeneration         uint64                `json:"fencing_generation"`
	ExecutionOutcome          string                `json:"execution_outcome"`
	OutputRowCount            uint64                `json:"output_row_count"`
	OutputRowsRoot            string                `json:"output_rows_root"`
	ComputedStateRoot         string                `json:"computed_state_root"`
	PartitionDeltas           []PartitionCommitment `json:"partition_deltas"`
	PartitionCommitmentsAfter []PartitionCommitment `json:"partition_commitments_after"`
	CandidateParts            []SnapshotReadPart    `json:"candidate_parts"`
}

// ProfileSetting is the frozen snapshot-query contract.
type ProfileSetting struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// QueryLimits is the frozen snapshot-query contract.
type QueryLimits struct {
	MaxSQLBytes        uint64 `json:"max_sql_bytes"`
	MaxDescriptorBytes uint64 `json:"max_descriptor_bytes"`
	MaxOutputRows      uint64 `json:"max_output_rows"`
	MaxOutputBytes     uint64 `json:"max_output_bytes"`
	MaxRestoreBytes    uint64 `json:"max_restore_bytes"`
	MaxSortMemoryBytes uint64 `json:"max_sort_memory_bytes"`
	MaxSpillBytes      uint64 `json:"max_spill_bytes"`
	MaxExecutionMS     uint64 `json:"max_execution_ms"`
}

// QueryProfileRecord is the frozen snapshot-query contract.
type QueryProfileRecord struct {
	Version                   uint32           `json:"version"`
	ClickHouseBuildDigest     string           `json:"clickhouse_build_digest"`
	Platform                  string           `json:"platform"`
	NativeAnalyzerBuildDigest string           `json:"native_analyzer_build_digest"`
	GRPCAnalyzerBuildDigest   string           `json:"grpc_analyzer_build_digest"`
	TZDataDigest              string           `json:"tzdata_digest"`
	Settings                  []ProfileSetting `json:"settings"`
	ScalarOperators           []string         `json:"scalar_operators"`
	ColumnProfileID           string           `json:"column_profile_id"`
	OutputOrderID             string           `json:"output_order_id"`
	Limits                    QueryLimits      `json:"limits"`
}

// SnapshotArtifactEntry is the frozen snapshot-query contract.
type SnapshotArtifactEntry struct {
	TableID      string `json:"table_id"`
	PartitionID  string `json:"partition_id"`
	PartName     string `json:"part_name"`
	PartPhysHash string `json:"part_phys_hash"`
	ObjectDigest string `json:"object_digest"`
	Bytes        uint64 `json:"bytes"`
}

// SnapshotArtifactSet is the frozen snapshot-query contract.
type SnapshotArtifactSet struct {
	Parts                []SnapshotArtifactEntry `json:"parts"`
	SchemaArtifactDigest string                  `json:"schema_artifact_digest"`
}

// SnapshotArtifactReadySubmission is the frozen snapshot-query contract.
type SnapshotArtifactReadySubmission struct {
	Record    SnapshotArtifactReady `json:"record"`
	Signature string                `json:"signature"`
}

// SnapshotQueryOutputCommitment binds hashes of canonical user-row bytes in
// their full-byte-sorted order. Equal rows remain repeated; hashes are not sorted.
// Only its root travels on the wire, so no protobuf mirror is needed.
type SnapshotQueryOutputCommitment struct {
	TargetTableID string   `json:"target_table_id"`
	SchemaHash    string   `json:"schema_hash"`
	RowCount      uint64   `json:"row_count"`
	RowHashes     []string `json:"row_hashes"`
}
