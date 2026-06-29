package storageintegrity

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"housegate/housegate/pkg/replay"
)

type LocalCoordinatorConfig struct {
	RequireFinality         bool
	RequireReplay           bool
	RequireUnsafeValidation bool
	UnsafeReplicas          []UnsafeReplica
	SafeAuditReplicas       []SafeAuditReplica
	SafeAuditNetworkID      string
	SafeAuditSchemaHash     string
	UnsafeDatabase          string
	SafeDatabase            string
	UnsafeTableSuffix       string
}

// LocalCoordinator is the in-process P0 stand-in for HouseKeeper's
// SafeAuditCoordinator state machine. It is deliberately small: statements are
// kept in memory and workers communicate through the same source/sink
// interfaces a real HouseKeeper client will implement.
type LocalCoordinator struct {
	mu sync.Mutex

	cfg    LocalCoordinatorConfig
	layout TableLayout

	nextBlockSeq uint64
	records      map[string]InsertRecord
	blockToStmt  map[uint64]string

	replayQueue           []replay.ReplayJob
	unsafeValidationQueue []UnsafeValidationTask
	promotionQueue        []PromotionTask
	rollbackQueue         []RollbackTask
	safeAuditQueue        []SafeAuditTask

	finalized       map[string]bool
	replayed        map[string]bool
	unsafeValidated map[string]bool
	unsafeFailed    map[string]UnsafeValidationFailure
	promotionQueued map[string]bool
	promoted        map[string]bool
	rollbackQueued  map[string]bool
	rolledBack      map[string]bool
	safeAuditQueued map[string]bool

	safeAuditExpected  map[string]map[string]bool
	safeAuditVotes     map[string]map[string]SafeAuditVote
	safeAuditDecisions map[string]SafeAuditDecision
}

func NewLocalCoordinator(cfg LocalCoordinatorConfig) *LocalCoordinator {
	return &LocalCoordinator{
		cfg: cfg,
		layout: NewTableLayout(TableLayoutConfig{
			UnsafeDatabase:    cfg.UnsafeDatabase,
			SafeDatabase:      cfg.SafeDatabase,
			UnsafeTableSuffix: cfg.UnsafeTableSuffix,
		}),
		records:         map[string]InsertRecord{},
		blockToStmt:     map[uint64]string{},
		finalized:       map[string]bool{},
		replayed:        map[string]bool{},
		unsafeValidated: map[string]bool{},
		unsafeFailed:    map[string]UnsafeValidationFailure{},
		promotionQueued: map[string]bool{},
		promoted:        map[string]bool{},
		rollbackQueued:  map[string]bool{},
		rolledBack:      map[string]bool{},
		safeAuditQueued: map[string]bool{},

		safeAuditExpected:  map[string]map[string]bool{},
		safeAuditVotes:     map[string]map[string]SafeAuditVote{},
		safeAuditDecisions: map[string]SafeAuditDecision{},
	}
}

func (c *LocalCoordinator) SubmitInsert(ctx context.Context, rec InsertRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rec.TableID == "" {
		return fmt.Errorf("table_id is required")
	}
	if rec.StatementID == "" {
		return fmt.Errorf("statement_id is required")
	}
	if rec.Payload.Ref == "" || rec.Payload.Hash == "" {
		return fmt.Errorf("payload commitment is required")
	}
	if rec.UnsafeTable == "" {
		rec.UnsafeTable = c.layout.UnsafeTable(rec.TableID)
	}
	if rec.SafeTable == "" {
		rec.SafeTable = c.layout.SafeTable(rec.TableID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.records[rec.StatementID]; exists {
		return fmt.Errorf("statement %q already exists", rec.StatementID)
	}
	c.nextBlockSeq++
	blockSeq := c.nextBlockSeq
	c.records[rec.StatementID] = rec
	c.blockToStmt[blockSeq] = rec.StatementID
	if !c.cfg.RequireFinality {
		c.finalized[rec.StatementID] = true
	}
	if c.cfg.RequireReplay {
		c.replayQueue = append(c.replayQueue, c.buildReplayJobLocked(blockSeq, rec))
	} else {
		c.replayed[rec.StatementID] = true
	}
	if c.cfg.RequireUnsafeValidation {
		c.unsafeValidationQueue = append(c.unsafeValidationQueue, UnsafeValidationTask{
			ValidationID: "unsafe-" + rec.StatementID,
			StatementID:  rec.StatementID,
			TableID:      rec.TableID,
			UnsafeTable:  rec.UnsafeTable,
			Replicas:     append([]UnsafeReplica(nil), c.cfg.UnsafeReplicas...),
		})
	} else {
		c.unsafeValidated[rec.StatementID] = true
	}
	c.queuePromotionIfReadyLocked(rec.StatementID)
	return nil
}

func (c *LocalCoordinator) SubmitFinality(ctx context.Context, rec FinalityRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rec.StatementID == "" {
		return fmt.Errorf("statement_id is required")
	}
	if !rec.Finalized {
		return fmt.Errorf("finality record for %q is not finalized", rec.StatementID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.records[rec.StatementID]; !ok {
		return fmt.Errorf("unknown statement_id %q", rec.StatementID)
	}
	c.finalized[rec.StatementID] = true
	c.queuePromotionIfReadyLocked(rec.StatementID)
	return nil
}

func (c *LocalCoordinator) ClaimReplayJob(ctx context.Context) (replay.ReplayJob, bool, error) {
	if err := ctx.Err(); err != nil {
		return replay.ReplayJob{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.replayQueue) == 0 {
		return replay.ReplayJob{}, false, nil
	}
	job := c.replayQueue[0]
	c.replayQueue = c.replayQueue[1:]
	return job, true, nil
}

func (c *LocalCoordinator) SubmitReplayAttestation(ctx context.Context, att replay.ReplayAttestation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !att.MatchSourceRoot || !att.Receipt.MatchSourceRoot {
		return fmt.Errorf("replay attestation for block %d does not match source root", att.Receipt.BlockSeq)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stmtID := c.blockToStmt[att.Receipt.BlockSeq]
	if stmtID == "" {
		return fmt.Errorf("unknown replay block_seq %d", att.Receipt.BlockSeq)
	}
	c.replayed[stmtID] = true
	c.queuePromotionIfReadyLocked(stmtID)
	return nil
}

func (c *LocalCoordinator) SubmitReplayFailure(ctx context.Context, failure ReplayFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("replay failed for block %d: %s", failure.BlockSeq, failure.Error)
}

func (c *LocalCoordinator) ClaimUnsafeValidation(ctx context.Context) (UnsafeValidationTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return UnsafeValidationTask{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.unsafeValidationQueue) == 0 {
		return UnsafeValidationTask{}, false, nil
	}
	task := c.unsafeValidationQueue[0]
	c.unsafeValidationQueue = c.unsafeValidationQueue[1:]
	return task, true, nil
}

func (c *LocalCoordinator) SubmitUnsafeValidation(ctx context.Context, result UnsafeValidationResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if result.ValidationID == "" {
		return fmt.Errorf("validation_id is required")
	}
	if result.StatementID == "" {
		return fmt.Errorf("statement_id is required")
	}
	if result.RowsHash == "" {
		return fmt.Errorf("rows_hash is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.records[result.StatementID]
	if !ok {
		return fmt.Errorf("unknown statement_id %q", result.StatementID)
	}
	if result.TableID != "" && result.TableID != rec.TableID {
		return fmt.Errorf("table_id mismatch for %q: got %s want %s", result.StatementID, result.TableID, rec.TableID)
	}
	if result.UnsafeTable != "" && result.UnsafeTable != rec.UnsafeTable {
		return fmt.Errorf("unsafe_table mismatch for %q: got %s want %s", result.StatementID, result.UnsafeTable, rec.UnsafeTable)
	}
	c.unsafeValidated[result.StatementID] = true
	c.queuePromotionIfReadyLocked(result.StatementID)
	return nil
}

func (c *LocalCoordinator) SubmitUnsafeValidationFailure(ctx context.Context, failure UnsafeValidationFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if failure.StatementID == "" {
		return fmt.Errorf("statement_id is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unsafeFailed[failure.StatementID] = failure
	return nil
}

func (c *LocalCoordinator) ClaimPromotion(ctx context.Context) (PromotionTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return PromotionTask{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.promotionQueue) == 0 {
		return PromotionTask{}, false, nil
	}
	task := c.promotionQueue[0]
	c.promotionQueue = c.promotionQueue[1:]
	return task, true, nil
}

func (c *LocalCoordinator) SubmitRollback(ctx context.Context, event RollbackEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.StatementID == "" {
		event.StatementID = event.BatchID
	}
	if event.StatementID == "" {
		return fmt.Errorf("statement_id is required")
	}
	if event.Reason == "" {
		return fmt.Errorf("rollback reason is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.records[event.StatementID]
	if !ok {
		return fmt.Errorf("unknown statement_id %q", event.StatementID)
	}
	c.rolledBack[event.StatementID] = true
	c.removePromotionLocked(event.StatementID)
	c.queueRollbackLocked(rec, event)
	return nil
}

func (c *LocalCoordinator) ClaimRollback(ctx context.Context) (RollbackTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return RollbackTask{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.rollbackQueue) == 0 {
		return RollbackTask{}, false, nil
	}
	task := c.rollbackQueue[0]
	c.rollbackQueue = c.rollbackQueue[1:]
	return task, true, nil
}

func (c *LocalCoordinator) FinishRollback(ctx context.Context, result RollbackResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (c *LocalCoordinator) FailRollback(ctx context.Context, failure RollbackFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("rollback %s failed: %s", failure.RollbackID, failure.Error)
}

func (c *LocalCoordinator) FinishPromotion(ctx context.Context, result PromotionResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for stmtID, rec := range c.records {
		if "promotion-"+rec.StatementID == result.PromotionID {
			c.promoted[stmtID] = true
			c.queueSafeAuditLocked(stmtID, rec)
			return nil
		}
	}
	return nil
}

func (c *LocalCoordinator) FailPromotion(ctx context.Context, failure PromotionFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("promotion %s failed: %s", failure.PromotionID, failure.Error)
}

func (c *LocalCoordinator) ClaimSafeAudit(ctx context.Context) (SafeAuditTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return SafeAuditTask{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.safeAuditQueue) == 0 {
		return SafeAuditTask{}, false, nil
	}
	task := c.safeAuditQueue[0]
	c.safeAuditQueue = c.safeAuditQueue[1:]
	return task, true, nil
}

func (c *LocalCoordinator) SubmitSafeAuditVote(ctx context.Context, vote SafeAuditVote) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if vote.AuditID == "" {
		return fmt.Errorf("audit_id is required")
	}
	if vote.ReplicaID == "" {
		return fmt.Errorf("replica_id is required")
	}
	if vote.BatchHash == "" {
		return fmt.Errorf("batch_hash is required")
	}
	if vote.VoteHash == "" {
		return fmt.Errorf("vote_hash is required")
	}
	if vote.Signature == "" {
		return fmt.Errorf("signature is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	expected, ok := c.safeAuditExpected[vote.AuditID]
	if !ok {
		return fmt.Errorf("unknown audit_id %q", vote.AuditID)
	}
	if !expected[vote.ReplicaID] {
		return fmt.Errorf("replica %q is not expected for audit %q", vote.ReplicaID, vote.AuditID)
	}
	votes := c.safeAuditVotes[vote.AuditID]
	if votes == nil {
		votes = map[string]SafeAuditVote{}
		c.safeAuditVotes[vote.AuditID] = votes
	}
	if _, exists := votes[vote.ReplicaID]; exists {
		return fmt.Errorf("duplicate vote from replica %q for audit %q", vote.ReplicaID, vote.AuditID)
	}
	votes[vote.ReplicaID] = vote
	c.evaluateSafeAuditLocked(vote.AuditID)
	return nil
}

func (c *LocalCoordinator) SafeAuditDecision(ctx context.Context, auditID string) (SafeAuditDecision, bool) {
	if err := ctx.Err(); err != nil {
		return SafeAuditDecision{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	decision, ok := c.safeAuditDecisions[auditID]
	return decision, ok
}

func (c *LocalCoordinator) buildReplayJobLocked(blockSeq uint64, rec InsertRecord) replay.ReplayJob {
	stmt := replay.Statement{
		StatementID:   rec.StatementID,
		StatementSeq:  1,
		SQL:           rec.OriginalSQL,
		SQLHash:       replay.DigestBytes([]byte(rec.OriginalSQL)),
		SettingsHash:  replay.DigestBytes([]byte("{}")),
		PayloadRef:    rec.Payload.Ref,
		PayloadHash:   rec.Payload.Hash,
		PayloadLength: rec.Payload.Length,
		TargetTableID: rec.TableID,
	}
	return replay.ReplayJob{
		BlockSeq:           blockSeq,
		PrevSafeSnapshotID: "mock-genesis",
		PrevStateRoot:      replay.DigestBytes([]byte("mock-genesis-state")),
		SchemaSnapshotID:   "mock-schema",
		ExecutorProfileID:  "mock-replay",
		SourceClaimRoot:    sourceClaimRoot(rec),
		Statements:         []replay.Statement{stmt},
	}
}

func (c *LocalCoordinator) queuePromotionIfReadyLocked(stmtID string) {
	if c.promotionQueued[stmtID] || c.rolledBack[stmtID] || !c.finalized[stmtID] || !c.replayed[stmtID] || !c.unsafeValidated[stmtID] {
		return
	}
	rec, ok := c.records[stmtID]
	if !ok {
		return
	}
	c.promotionQueued[stmtID] = true
	c.promotionQueue = append(c.promotionQueue, PromotionTask{
		PromotionID:  "promotion-" + rec.StatementID,
		LeaseID:      "lease-" + rec.StatementID,
		UnsafeTable:  rec.UnsafeTable,
		SafeTable:    rec.SafeTable,
		PartitionIDs: append([]string(nil), rec.PartitionIDs...),
		Readback:     PromotionReadbackSpec{Table: rec.SafeTable},
	})
}

func (c *LocalCoordinator) queueRollbackLocked(rec InsertRecord, event RollbackEvent) {
	if c.rollbackQueued[rec.StatementID] || c.promoted[rec.StatementID] {
		return
	}
	c.rollbackQueued[rec.StatementID] = true
	c.rollbackQueue = append(c.rollbackQueue, RollbackTask{
		RollbackID:  "rollback-" + rec.StatementID,
		LeaseID:     "rollback-lease-" + rec.StatementID,
		BatchID:     event.BatchID,
		StatementID: rec.StatementID,
		Reason:      event.Reason,
		Statements: []string{
			"TRUNCATE TABLE " + rec.UnsafeTable,
		},
	})
}

func (c *LocalCoordinator) queueSafeAuditLocked(stmtID string, rec InsertRecord) {
	if c.safeAuditQueued[stmtID] || len(c.cfg.SafeAuditReplicas) == 0 {
		return
	}
	c.safeAuditQueued[stmtID] = true
	auditID := "audit-" + stmtID
	networkID := c.cfg.SafeAuditNetworkID
	if networkID == "" {
		networkID = "mock-network"
	}
	schemaHash := c.cfg.SafeAuditSchemaHash
	if schemaHash == "" {
		schemaHash = "mock-schema"
	}
	expected := map[string]bool{}
	for _, replica := range c.cfg.SafeAuditReplicas {
		if replica.ReplicaID == "" || expected[replica.ReplicaID] {
			continue
		}
		expected[replica.ReplicaID] = true
		c.safeAuditQueue = append(c.safeAuditQueue, SafeAuditTask{
			AuditID:    auditID,
			ReplicaID:  replica.ReplicaID,
			NetworkID:  networkID,
			TableID:    rec.TableID,
			SchemaHash: schemaHash,
			SnapshotID: "snapshot-" + stmtID,
			Range:      "safe=" + rec.SafeTable,
		})
	}
	if len(expected) == 0 {
		return
	}
	c.safeAuditExpected[auditID] = expected
	c.safeAuditDecisions[auditID] = SafeAuditDecision{
		AuditID:       auditID,
		Status:        SafeAuditStatusPending,
		ExpectedVotes: len(expected),
	}
}

func (c *LocalCoordinator) evaluateSafeAuditLocked(auditID string) {
	expected := c.safeAuditExpected[auditID]
	votes := c.safeAuditVotes[auditID]
	if len(expected) == 0 {
		return
	}
	threshold := len(expected)/2 + 1
	byHash := map[string]int{}
	for _, vote := range votes {
		byHash[vote.BatchHash]++
	}
	decision := SafeAuditDecision{
		AuditID:       auditID,
		Status:        SafeAuditStatusPending,
		TotalVotes:    len(votes),
		ExpectedVotes: len(expected),
	}
	for hash, count := range byHash {
		if count >= threshold && count > decision.MajorityCount {
			decision.Status = SafeAuditStatusMajority
			decision.MajorityHash = hash
			decision.MajorityCount = count
		}
	}
	if decision.Status == SafeAuditStatusMajority {
		for replicaID, vote := range votes {
			if vote.BatchHash != decision.MajorityHash {
				decision.MinorityReplicas = append(decision.MinorityReplicas, replicaID)
			}
		}
		sort.Strings(decision.MinorityReplicas)
		c.safeAuditDecisions[auditID] = decision
		return
	}
	if len(votes) >= len(expected) {
		decision.Status = SafeAuditStatusDispute
	}
	c.safeAuditDecisions[auditID] = decision
}

func (c *LocalCoordinator) removePromotionLocked(stmtID string) {
	if !c.promotionQueued[stmtID] {
		return
	}
	filtered := c.promotionQueue[:0]
	for _, task := range c.promotionQueue {
		if task.PromotionID != "promotion-"+stmtID {
			filtered = append(filtered, task)
		}
	}
	c.promotionQueue = filtered
	c.promotionQueued[stmtID] = false
}

func sourceClaimRoot(rec InsertRecord) string {
	body, _ := json.Marshal(struct {
		TableID     string            `json:"table_id"`
		StatementID string            `json:"statement_id"`
		OriginalSQL string            `json:"original_sql"`
		Payload     PayloadCommitment `json:"payload"`
	}{
		TableID:     rec.TableID,
		StatementID: rec.StatementID,
		OriginalSQL: rec.OriginalSQL,
		Payload:     rec.Payload,
	})
	return replay.DigestBytes(body)
}
