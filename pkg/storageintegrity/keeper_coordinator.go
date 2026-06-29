package storageintegrity

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"

	"housegate/housegate/pkg/replay"
)

var (
	errKeeperNodeExists = errors.New("keeper node exists")
	errKeeperNoNode     = errors.New("keeper node does not exist")
)

type keeperStore interface {
	EnsurePath(ctx context.Context, p string) error
	Create(ctx context.Context, p string, data []byte) error
	Set(ctx context.Context, p string, data []byte) error
	Get(ctx context.Context, p string) ([]byte, bool, error)
	Children(ctx context.Context, p string) ([]string, error)
	Close()
}

type KeeperCoordinatorConfig struct {
	Endpoints      []string
	SessionTimeout time.Duration
	Root           string
	WorkerID       string
	LocalAddr      string
	ReplayQuorum   int

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

// KeeperCoordinator persists the HouseGate worker control plane in
// ClickHouse Keeper znodes. It implements the same source/sink interfaces as
// LocalCoordinator so runtime workers can switch from mock to HouseKeeper
// without changing worker code.
type KeeperCoordinator struct {
	cfg    KeeperCoordinatorConfig
	store  keeperStore
	layout TableLayout
}

func NewKeeperCoordinator(ctx context.Context, cfg KeeperCoordinatorConfig) (*KeeperCoordinator, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("housekeeper endpoints are required")
	}
	timeout := cfg.SessionTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	conn, events, err := zk.Connect(cfg.Endpoints, timeout)
	if err != nil {
		return nil, fmt.Errorf("connect housekeeper: %w", err)
	}
	if err := waitForZKSession(ctx, events, timeout); err != nil {
		conn.Close()
		return nil, err
	}
	return NewKeeperCoordinatorWithStore(cfg, &zkKeeperStore{conn: conn})
}

func waitForZKSession(ctx context.Context, events <-chan zk.Event, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return fmt.Errorf("connect housekeeper: event channel closed before session")
			}
			switch ev.State {
			case zk.StateConnected, zk.StateHasSession:
				return nil
			case zk.StateAuthFailed:
				if ev.Err != nil {
					return fmt.Errorf("connect housekeeper: auth failed: %w", ev.Err)
				}
				return fmt.Errorf("connect housekeeper: auth failed")
			default:
				continue
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("connect housekeeper: timed out after %s", timeout)
		}
	}
}

func NewKeeperCoordinatorWithStore(cfg KeeperCoordinatorConfig, store keeperStore) (*KeeperCoordinator, error) {
	if store == nil {
		return nil, fmt.Errorf("keeper store is required")
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = "housegate-local"
	}
	if cfg.Root == "" {
		cfg.Root = "/housekeeper/v1/storage_integrity"
	}
	cfg.Root = cleanKeeperPath(cfg.Root)
	if cfg.ReplayQuorum <= 0 {
		cfg.ReplayQuorum = 1
	}
	c := &KeeperCoordinator{
		cfg:   cfg,
		store: store,
		layout: NewTableLayout(TableLayoutConfig{
			UnsafeDatabase:    cfg.UnsafeDatabase,
			SafeDatabase:      cfg.SafeDatabase,
			UnsafeTableSuffix: cfg.UnsafeTableSuffix,
		}),
	}
	if err := c.ensureRoots(context.Background()); err != nil {
		store.Close()
		return nil, err
	}
	return c, nil
}

func (c *KeeperCoordinator) Close() {
	if c.store != nil {
		c.store.Close()
	}
}

func (c *KeeperCoordinator) SubmitInsert(ctx context.Context, rec InsertRecord) error {
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
	stmtPath := c.statementPath(rec.StatementID)
	participants := c.configuredParticipants()
	if len(participants) == 0 {
		return fmt.Errorf("housekeeper participants are required")
	}
	if err := c.createKV(ctx, stmtPath, c.encodeStatement(rec, participants)); err != nil {
		if errors.Is(err, errKeeperNodeExists) {
			return fmt.Errorf("statement %q already exists", rec.StatementID)
		}
		return err
	}
	return nil
}

func (c *KeeperCoordinator) SubmitFinality(ctx context.Context, rec FinalityRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rec.StatementID == "" {
		rec.StatementID = rec.BatchID
	}
	if rec.StatementID == "" {
		return fmt.Errorf("statement_id is required")
	}
	if !rec.Finalized {
		return fmt.Errorf("finality record for %q is not finalized", rec.StatementID)
	}
	if rec.FinalizedAt.IsZero() {
		rec.FinalizedAt = time.Now().UTC()
	}
	fields := []keeperKVPair{
		{Key: "kind", Value: rec.Kind},
		{Key: "batch_id", Value: rec.BatchID},
		{Key: "payload_ref", Value: rec.PayloadRef},
		{Key: "payload_hash", Value: rec.PayloadHash},
		{Key: "finalized", Value: strconv.FormatBool(rec.Finalized)},
		{Key: "finalized_at", Value: rec.FinalizedAt.UTC().Format(time.RFC3339Nano)},
	}
	if err := c.createKV(ctx, c.finalityPath(rec.StatementID), encodeKeeperKV(fields...)); err != nil {
		if !errors.Is(err, errKeeperNodeExists) {
			return err
		}
		if err := c.setKV(ctx, c.finalityPath(rec.StatementID), encodeKeeperKV(fields...)); err != nil {
			return err
		}
	}
	return nil
}

func (c *KeeperCoordinator) ClaimReplayJob(ctx context.Context) (replay.ReplayJob, bool, error) {
	if err := ctx.Err(); err != nil {
		return replay.ReplayJob{}, false, err
	}
	if quarantined, err := c.workerReplayQuarantined(ctx); err != nil || quarantined {
		return replay.ReplayJob{}, false, err
	}
	children, err := c.children(ctx, c.path("replay_jobs"))
	if err != nil {
		return replay.ReplayJob{}, false, err
	}
	for _, child := range children {
		stmtID := unescapeSegment(child)
		if exists, err := c.exists(ctx, c.attestationPath(stmtID, c.cfg.WorkerID)); err != nil {
			return replay.ReplayJob{}, false, err
		} else if exists {
			continue
		}
		if exists, err := c.exists(ctx, c.replayJobPath(stmtID)); err != nil || !exists {
			return replay.ReplayJob{}, false, err
		}
		ledger, ok, err := c.readStatement(ctx, stmtID)
		if err != nil || !ok {
			return replay.ReplayJob{}, false, err
		}
		job := c.buildReplayJob(ledger.BlockSeq, ledger.InsertRecord)
		return job, true, nil
	}
	return replay.ReplayJob{}, false, nil
}

func (c *KeeperCoordinator) SubmitReplayAttestation(ctx context.Context, att replay.ReplayAttestation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if att.ReplicaID == "" {
		att.ReplicaID = c.cfg.WorkerID
	}
	if att.ReplicaID == "" {
		return fmt.Errorf("replica_id is required")
	}
	if !att.MatchSourceRoot || !att.Receipt.MatchSourceRoot {
		return fmt.Errorf("replay attestation for block %d does not match source root", att.Receipt.BlockSeq)
	}
	stmtID, ok, err := c.statementIDByBlockSeq(ctx, att.Receipt.BlockSeq)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unknown replay block_seq %d", att.Receipt.BlockSeq)
	}
	fields := []keeperKVPair{
		{Key: "computed_state_root", Value: att.Receipt.ComputedStateRoot},
		{Key: "receipt_hash", Value: att.ReceiptHash},
		{Key: "match_source_root", Value: strconv.FormatBool(att.MatchSourceRoot && att.Receipt.MatchSourceRoot)},
		{Key: "signature", Value: att.Signature},
	}
	if err := c.createKV(ctx, c.attestationPath(stmtID, att.ReplicaID), encodeKeeperKV(fields...)); err != nil {
		if errors.Is(err, errKeeperNodeExists) {
			return fmt.Errorf("duplicate replay attestation from %q for %q", att.ReplicaID, stmtID)
		}
		return err
	}
	return nil
}

func (c *KeeperCoordinator) SubmitReplayFailure(ctx context.Context, failure ReplayFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if failure.BlockSeq == 0 {
		return fmt.Errorf("block_seq is required")
	}
	return c.createJSON(ctx, c.replayFailurePath(failure.BlockSeq, c.cfg.WorkerID), failure)
}

func (c *KeeperCoordinator) ClaimUnsafeValidation(ctx context.Context) (UnsafeValidationTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return UnsafeValidationTask{}, false, err
	}
	if quarantined, err := c.workerReplayQuarantined(ctx); err != nil || quarantined {
		return UnsafeValidationTask{}, false, err
	}
	children, err := c.children(ctx, c.path("unsafe_tasks"))
	if err != nil {
		return UnsafeValidationTask{}, false, err
	}
	for _, child := range children {
		stmtID := unescapeSegment(child)
		if exists, err := c.exists(ctx, c.unsafeParticipantResultPath(stmtID, c.cfg.WorkerID)); err != nil {
			return UnsafeValidationTask{}, false, err
		} else if exists {
			continue
		}
		if data, ok, err := c.store.Get(ctx, c.unsafeResultPath(stmtID)); err != nil {
			return UnsafeValidationTask{}, false, err
		} else if ok && len(data) > 0 {
			continue
		}
		if exists, err := c.exists(ctx, c.unsafeFailurePath(stmtID)); err != nil {
			return UnsafeValidationTask{}, false, err
		} else if exists {
			continue
		}
		task, ok, err := c.getUnsafeTask(ctx, stmtID)
		if err != nil || !ok {
			return UnsafeValidationTask{}, false, err
		}
		return task, true, nil
	}
	return UnsafeValidationTask{}, false, nil
}

func (c *KeeperCoordinator) SubmitUnsafeValidation(ctx context.Context, result UnsafeValidationResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.validateUnsafeResult(ctx, result); err != nil {
		return err
	}
	resultPath := c.unsafeResultPath(result.StatementID)
	if len(result.Replicas) == 1 && result.Replicas[0].ReplicaID == c.cfg.WorkerID {
		resultPath = c.unsafeParticipantResultPath(result.StatementID, c.cfg.WorkerID)
	}
	resultData := encodeUnsafeResultKV(result)
	if err := c.store.EnsurePath(ctx, parentPath(resultPath)); err != nil {
		return err
	}
	if err := c.createKV(ctx, resultPath, resultData); err != nil {
		if errors.Is(err, errKeeperNodeExists) {
			existing, ok, getErr := c.store.Get(ctx, resultPath)
			if getErr != nil {
				return getErr
			}
			if ok && bytes.Equal(existing, resultData) {
				return nil
			}
			return fmt.Errorf("unsafe validation for %q already exists", result.StatementID)
		}
		return err
	}
	return nil
}

func (c *KeeperCoordinator) SubmitUnsafeValidationFailure(ctx context.Context, failure UnsafeValidationFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if failure.StatementID == "" {
		return fmt.Errorf("statement_id is required")
	}
	if err := c.createJSON(ctx, c.unsafeFailurePath(failure.StatementID), failure); err != nil {
		if errors.Is(err, errKeeperNodeExists) {
			return nil
		}
		return err
	}
	return nil
}

func (c *KeeperCoordinator) ClaimPromotion(ctx context.Context) (PromotionTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return PromotionTask{}, false, err
	}
	if quarantined, err := c.workerReplayQuarantined(ctx); err != nil || quarantined {
		return PromotionTask{}, false, err
	}
	children, err := c.children(ctx, c.path("promotions"))
	if err != nil {
		return PromotionTask{}, false, err
	}
	for _, child := range children {
		stmtID := unescapeSegment(child)
		if exists, err := c.exists(ctx, c.promotionParticipantResultPath(stmtID, c.cfg.WorkerID)); err != nil {
			return PromotionTask{}, false, err
		} else if exists {
			continue
		}
		if exists, err := c.exists(ctx, c.promotionParticipantFailurePath(stmtID, c.cfg.WorkerID)); err != nil {
			return PromotionTask{}, false, err
		} else if exists {
			continue
		}
		if exists, err := c.exists(ctx, c.rollbackEventPath(stmtID)); err != nil {
			return PromotionTask{}, false, err
		} else if exists {
			continue
		}
		task, ok, err := c.getPromotionTask(ctx, stmtID)
		if err != nil || !ok {
			return PromotionTask{}, false, err
		}
		if ok, err := c.tryClaim(ctx, c.promotionParticipantLeasePath(stmtID, c.cfg.WorkerID), task.LeaseID); err != nil || !ok {
			return PromotionTask{}, false, err
		}
		return task, true, nil
	}
	return PromotionTask{}, false, nil
}

func (c *KeeperCoordinator) FinishPromotion(ctx context.Context, result PromotionResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stmtID := strings.TrimPrefix(result.PromotionID, "promotion-")
	if stmtID == result.PromotionID || stmtID == "" {
		return fmt.Errorf("promotion_id %q does not encode statement id", result.PromotionID)
	}
	if err := c.createJSON(ctx, c.promotionParticipantResultPath(stmtID, c.cfg.WorkerID), result); err != nil && !errors.Is(err, errKeeperNodeExists) {
		return err
	}
	complete, err := c.promotionResultsComplete(ctx, stmtID)
	if err != nil || !complete {
		return err
	}
	return c.queueSafeAudit(ctx, stmtID)
}

func (c *KeeperCoordinator) FailPromotion(ctx context.Context, failure PromotionFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stmtID := strings.TrimPrefix(failure.PromotionID, "promotion-")
	if stmtID == failure.PromotionID || stmtID == "" {
		return fmt.Errorf("promotion_id %q does not encode statement id", failure.PromotionID)
	}
	if err := c.createJSON(ctx, c.promotionParticipantFailurePath(stmtID, c.cfg.WorkerID), failure); err != nil && !errors.Is(err, errKeeperNodeExists) {
		return err
	}
	return nil
}

func (c *KeeperCoordinator) SubmitRollback(ctx context.Context, event RollbackEvent) error {
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
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = time.Now().UTC()
	}
	fields := []keeperKVPair{
		{Key: "kind", Value: event.Kind},
		{Key: "batch_id", Value: event.BatchID},
		{Key: "reason", Value: event.Reason},
		{Key: "received_at", Value: event.ReceivedAt.UTC().Format(time.RFC3339Nano)},
	}
	if err := c.createKV(ctx, c.rollbackEventPath(event.StatementID), encodeKeeperKV(fields...)); err != nil && !errors.Is(err, errKeeperNodeExists) {
		return err
	}
	return nil
}

func (c *KeeperCoordinator) ClaimRollback(ctx context.Context) (RollbackTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return RollbackTask{}, false, err
	}
	if quarantined, err := c.workerReplayQuarantined(ctx); err != nil || quarantined {
		return RollbackTask{}, false, err
	}
	children, err := c.children(ctx, c.path("rollback_tasks"))
	if err != nil {
		return RollbackTask{}, false, err
	}
	for _, child := range children {
		stmtID := unescapeSegment(child)
		if exists, err := c.exists(ctx, c.rollbackResultPath(stmtID)); err != nil {
			return RollbackTask{}, false, err
		} else if exists {
			continue
		}
		task, ok, err := c.getRollbackTask(ctx, stmtID)
		if err != nil || !ok {
			return RollbackTask{}, false, err
		}
		if ok, err := c.tryClaim(ctx, c.rollbackLeasePath(stmtID), task.LeaseID); err != nil || !ok {
			return RollbackTask{}, false, err
		}
		return task, true, nil
	}
	return RollbackTask{}, false, nil
}

func (c *KeeperCoordinator) FinishRollback(ctx context.Context, result RollbackResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stmtID := strings.TrimPrefix(result.RollbackID, "rollback-")
	if stmtID == result.RollbackID || stmtID == "" {
		return fmt.Errorf("rollback_id %q does not encode statement id", result.RollbackID)
	}
	return c.createJSON(ctx, c.rollbackResultPath(stmtID), result)
}

func (c *KeeperCoordinator) FailRollback(ctx context.Context, failure RollbackFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stmtID := strings.TrimPrefix(failure.RollbackID, "rollback-")
	if stmtID == failure.RollbackID || stmtID == "" {
		return fmt.Errorf("rollback_id %q does not encode statement id", failure.RollbackID)
	}
	return c.createJSON(ctx, c.rollbackFailurePath(stmtID), failure)
}

func (c *KeeperCoordinator) ClaimSafeAudit(ctx context.Context) (SafeAuditTask, bool, error) {
	if err := ctx.Err(); err != nil {
		return SafeAuditTask{}, false, err
	}
	if quarantined, err := c.workerReplayQuarantined(ctx); err != nil || quarantined {
		return SafeAuditTask{}, false, err
	}
	children, err := c.children(ctx, c.path("safe_audit_tasks"))
	if err != nil {
		return SafeAuditTask{}, false, err
	}
	for _, child := range children {
		key := unescapeSegment(child)
		if exists, err := c.exists(ctx, c.safeAuditVotePath(key)); err != nil {
			return SafeAuditTask{}, false, err
		} else if exists {
			continue
		}
		var task SafeAuditTask
		ok, err := c.getJSON(ctx, c.safeAuditTaskPath(key), &task)
		if err != nil || !ok {
			return SafeAuditTask{}, false, err
		}
		if task.ReplicaID != "" && task.ReplicaID != c.cfg.WorkerID {
			continue
		}
		return task, true, nil
	}
	return SafeAuditTask{}, false, nil
}

func (c *KeeperCoordinator) SubmitSafeAuditVote(ctx context.Context, vote SafeAuditVote) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if vote.AuditID == "" || vote.ReplicaID == "" || vote.BatchHash == "" || vote.VoteHash == "" || vote.Signature == "" {
		return fmt.Errorf("safe audit vote is incomplete")
	}
	key := vote.AuditID + "/" + vote.ReplicaID
	return c.createJSON(ctx, c.safeAuditVotePath(key), vote)
}

func (c *KeeperCoordinator) validateUnsafeResult(ctx context.Context, result UnsafeValidationResult) error {
	if result.ValidationID == "" || result.StatementID == "" || result.RowsHash == "" {
		return fmt.Errorf("unsafe validation result is incomplete")
	}
	task, ok, err := c.getUnsafeTask(ctx, result.StatementID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unknown unsafe validation task for %q", result.StatementID)
	}
	if result.TableID != "" && result.TableID != task.TableID {
		return fmt.Errorf("table_id mismatch for %q: got %s want %s", result.StatementID, result.TableID, task.TableID)
	}
	if result.UnsafeTable != "" && result.UnsafeTable != task.UnsafeTable {
		return fmt.Errorf("unsafe_table mismatch for %q: got %s want %s", result.StatementID, result.UnsafeTable, task.UnsafeTable)
	}
	expected := map[string]struct{}{}
	for _, replica := range task.Replicas {
		expected[replica.ReplicaID] = struct{}{}
	}
	seen := map[string]struct{}{}
	for _, replica := range result.Replicas {
		if _, ok := expected[replica.ReplicaID]; !ok {
			return fmt.Errorf("unexpected unsafe replica %q", replica.ReplicaID)
		}
		if _, ok := seen[replica.ReplicaID]; ok {
			return fmt.Errorf("duplicate unsafe replica %q", replica.ReplicaID)
		}
		if replica.RowCount != result.RowCount || replica.RowsHash != result.RowsHash {
			return fmt.Errorf("unsafe replica %q mismatch", replica.ReplicaID)
		}
		seen[replica.ReplicaID] = struct{}{}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("unsafe validation replicas = %d, want %d", len(seen), len(expected))
	}
	return nil
}

func (c *KeeperCoordinator) queueSafeAudit(ctx context.Context, stmtID string) error {
	ledger, ok, err := c.readStatement(ctx, stmtID)
	if err != nil || !ok {
		return err
	}
	participants := append([]string(nil), ledger.Participants...)
	if len(participants) == 0 {
		participants = c.configuredParticipants()
	}
	if len(participants) == 0 {
		return nil
	}
	networkID := c.cfg.SafeAuditNetworkID
	if networkID == "" {
		networkID = "mock-network"
	}
	schemaHash := c.cfg.SafeAuditSchemaHash
	if schemaHash == "" {
		schemaHash = "mock-schema"
	}
	seen := map[string]struct{}{}
	for _, participant := range participants {
		if participant == "" {
			continue
		}
		if _, ok := seen[participant]; ok {
			continue
		}
		seen[participant] = struct{}{}
		task := SafeAuditTask{
			AuditID:    "audit-" + stmtID,
			ReplicaID:  participant,
			NetworkID:  networkID,
			TableID:    ledger.InsertRecord.TableID,
			SchemaHash: schemaHash,
			SnapshotID: "snapshot-" + stmtID,
			Range:      "safe=" + ledger.InsertRecord.SafeTable,
		}
		key := task.AuditID + "/" + task.ReplicaID
		if err := c.createJSON(ctx, c.safeAuditTaskPath(key), task); err != nil && !errors.Is(err, errKeeperNodeExists) {
			return err
		}
	}
	return nil
}

func (c *KeeperCoordinator) configuredParticipants() []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(c.cfg.UnsafeReplicas))
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, replica := range c.cfg.UnsafeReplicas {
		add(replica.ReplicaID)
	}
	if len(out) == 0 {
		for _, replica := range c.cfg.SafeAuditReplicas {
			add(replica.ReplicaID)
		}
	}
	if len(out) == 0 {
		add(c.cfg.WorkerID)
	}
	sort.Strings(out)
	return out
}

func (c *KeeperCoordinator) workerReplayQuarantined(ctx context.Context) (bool, error) {
	if c.cfg.WorkerID == "" {
		return false, nil
	}
	data, ok, err := c.store.Get(ctx, c.replayQuarantinePath(c.cfg.WorkerID))
	if err != nil || !ok {
		return ok, err
	}
	fields, err := parseKeeperKV(data)
	if err != nil {
		return false, err
	}
	status := strings.TrimSpace(fields["status"])
	return status == "" || status == "active", nil
}

func (c *KeeperCoordinator) buildReplayJob(blockSeq uint64, rec InsertRecord) replay.ReplayJob {
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
		PrevSafeSnapshotID: "housekeeper-genesis",
		PrevStateRoot:      replay.DigestBytes([]byte("housekeeper-genesis-state")),
		SchemaSnapshotID:   "housekeeper-schema",
		ExecutorProfileID:  "housekeeper-replay",
		SourceClaimRoot:    sourceClaimRoot(rec),
		Statements:         []replay.Statement{stmt},
	}
}

func (c *KeeperCoordinator) readStatement(ctx context.Context, stmtID string) (keeperStatementRecord, bool, error) {
	data, ok, err := c.store.Get(ctx, c.statementPath(stmtID))
	if err != nil || !ok {
		return keeperStatementRecord{}, ok, err
	}
	fields, err := parseKeeperKV(data)
	if err != nil {
		return keeperStatementRecord{}, false, err
	}
	rec, err := c.decodeStatement(stmtID, fields)
	if err != nil {
		return keeperStatementRecord{}, false, err
	}
	return keeperStatementRecord{InsertRecord: rec, BlockSeq: replayBlockSeq(stmtID), Participants: splitKeeperCSV(fields["participants"])}, true, nil
}

func (c *KeeperCoordinator) statementIDByBlockSeq(ctx context.Context, blockSeq uint64) (string, bool, error) {
	children, err := c.children(ctx, c.path("statements"))
	if err != nil {
		return "", false, err
	}
	for _, child := range children {
		stmtID := unescapeSegment(child)
		if replayBlockSeq(stmtID) == blockSeq {
			return stmtID, true, nil
		}
	}
	return "", false, nil
}

func (c *KeeperCoordinator) encodeStatement(rec InsertRecord, participants []string) []byte {
	fields := []keeperKVPair{
		keeperKVPair{Key: "table_id", Value: rec.TableID},
		keeperKVPair{Key: "unsafe_table", Value: rec.UnsafeTable},
		keeperKVPair{Key: "safe_table", Value: rec.SafeTable},
		keeperKVPair{Key: "payload_ref", Value: rec.Payload.Ref},
		keeperKVPair{Key: "payload_hash", Value: rec.Payload.Hash},
		keeperKVPair{Key: "payload_length", Value: strconv.FormatUint(rec.Payload.Length, 10)},
		keeperKVPair{Key: "replay_quorum", Value: strconv.Itoa(c.cfg.ReplayQuorum)},
		keeperKVPair{Key: "participants", Value: strings.Join(participants, ",")},
		keeperKVPair{Key: "original_sql_b64", Value: base64.StdEncoding.EncodeToString([]byte(rec.OriginalSQL))},
		keeperKVPair{Key: "unsafe_sql_b64", Value: base64.StdEncoding.EncodeToString([]byte(rec.UnsafeSQL))},
	}
	if len(rec.PartitionIDs) > 0 {
		fields = append(fields, keeperKVPair{Key: "partition_ids", Value: strings.Join(normalizeKeeperCSVValues(rec.PartitionIDs), ",")})
	}
	if len(c.cfg.UnsafeReplicas) > 0 {
		fields = append(fields, keeperKVPair{Key: "unsafe_replicas", Value: c.unsafeReplicaIDsCSV()})
	}
	return encodeKeeperKV(fields...)
}

func (c *KeeperCoordinator) decodeStatement(stmtID string, fields map[string]string) (InsertRecord, error) {
	payloadLength, err := parseOptionalUint(fields["payload_length"])
	if err != nil {
		return InsertRecord{}, fmt.Errorf("payload_length for %q: %w", stmtID, err)
	}
	originalSQL, err := decodeOptionalBase64(fields["original_sql_b64"])
	if err != nil {
		return InsertRecord{}, fmt.Errorf("original_sql_b64 for %q: %w", stmtID, err)
	}
	unsafeSQL, err := decodeOptionalBase64(fields["unsafe_sql_b64"])
	if err != nil {
		return InsertRecord{}, fmt.Errorf("unsafe_sql_b64 for %q: %w", stmtID, err)
	}
	rec := InsertRecord{
		TableID:      fields["table_id"],
		StatementID:  stmtID,
		OriginalSQL:  originalSQL,
		UnsafeSQL:    unsafeSQL,
		UnsafeTable:  fields["unsafe_table"],
		SafeTable:    fields["safe_table"],
		PartitionIDs: splitKeeperCSV(fields["partition_ids"]),
		Payload: PayloadCommitment{
			Ref:    fields["payload_ref"],
			Hash:   fields["payload_hash"],
			Length: payloadLength,
		},
	}
	if rec.TableID == "" || rec.UnsafeTable == "" || rec.SafeTable == "" || rec.Payload.Ref == "" || rec.Payload.Hash == "" {
		return InsertRecord{}, fmt.Errorf("statement %q is incomplete", stmtID)
	}
	return rec, nil
}

func (c *KeeperCoordinator) getUnsafeTask(ctx context.Context, stmtID string) (UnsafeValidationTask, bool, error) {
	data, ok, err := c.store.Get(ctx, c.unsafeTaskPath(stmtID))
	if err != nil || !ok {
		return UnsafeValidationTask{}, ok, err
	}
	fields, err := parseKeeperKV(data)
	if err != nil {
		return UnsafeValidationTask{}, false, err
	}
	task := UnsafeValidationTask{
		ValidationID: "unsafe-" + stmtID,
		StatementID:  firstNonEmpty(fields["statement_id"], stmtID),
		TableID:      fields["table_id"],
		UnsafeTable:  fields["unsafe_table"],
	}
	if participants := splitKeeperCSV(fields["participants"]); len(participants) > 0 {
		for _, participant := range participants {
			if participant == c.cfg.WorkerID {
				task.Replicas = []UnsafeReplica{{ReplicaID: c.cfg.WorkerID, Addr: c.cfg.LocalAddr}}
				break
			}
		}
	} else {
		task.Replicas = c.unsafeReplicasByID(splitKeeperCSV(fields["replicas"]))
	}
	if len(task.Replicas) == 0 && len(c.cfg.UnsafeReplicas) > 0 {
		task.Replicas = append([]UnsafeReplica(nil), c.cfg.UnsafeReplicas...)
	}
	if task.StatementID == "" || task.TableID == "" || task.UnsafeTable == "" || len(task.Replicas) == 0 {
		return UnsafeValidationTask{}, false, fmt.Errorf("unsafe task %q is incomplete", stmtID)
	}
	return task, true, nil
}

func (c *KeeperCoordinator) getPromotionTask(ctx context.Context, stmtID string) (PromotionTask, bool, error) {
	data, ok, err := c.store.Get(ctx, c.promotionPath(stmtID))
	if err != nil || !ok {
		return PromotionTask{}, ok, err
	}
	fields, err := parseKeeperKV(data)
	if err != nil {
		return PromotionTask{}, false, err
	}
	unsafeTable := fields["unsafe_table"]
	safeTable := fields["safe_table"]
	partitionIDs := splitKeeperCSV(fields["partition_ids"])
	if unsafeTable == "" || safeTable == "" {
		if statement, ok, err := c.readStatement(ctx, stmtID); err != nil {
			return PromotionTask{}, false, err
		} else if ok {
			unsafeTable = firstNonEmpty(unsafeTable, statement.UnsafeTable)
			safeTable = firstNonEmpty(safeTable, statement.SafeTable)
			if len(partitionIDs) == 0 {
				partitionIDs = append([]string(nil), statement.PartitionIDs...)
			}
		}
	} else if len(partitionIDs) == 0 {
		if statement, ok, err := c.readStatement(ctx, stmtID); err != nil {
			return PromotionTask{}, false, err
		} else if ok {
			partitionIDs = append([]string(nil), statement.PartitionIDs...)
		}
	}
	task := PromotionTask{
		PromotionID:  firstNonEmpty(fields["promotion_id"], "promotion-"+stmtID),
		LeaseID:      participantPromotionLeaseID(firstNonEmpty(fields["lease_id"], "lease-"+stmtID), c.cfg.WorkerID),
		UnsafeTable:  unsafeTable,
		SafeTable:    safeTable,
		PartitionIDs: partitionIDs,
		Readback:     PromotionReadbackSpec{Table: safeTable},
	}
	if unsafeResult, ok, err := c.readUnsafeResult(ctx, stmtID); err != nil {
		return PromotionTask{}, false, err
	} else if ok {
		task.Readback.ExpectedRows = unsafeResult.RowCount
		task.Readback.ExpectedHash = unsafeResult.RowsHash
	}
	if task.PromotionID == "" || task.LeaseID == "" || unsafeTable == "" || safeTable == "" {
		return PromotionTask{}, false, fmt.Errorf("promotion task %q is incomplete", stmtID)
	}
	return task, true, nil
}

func participantPromotionLeaseID(baseLeaseID, workerID string) string {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return baseLeaseID
	}
	suffix := "-" + workerID
	if strings.HasSuffix(baseLeaseID, suffix) {
		return baseLeaseID
	}
	return baseLeaseID + suffix
}

func (c *KeeperCoordinator) promotionResultsComplete(ctx context.Context, stmtID string) (bool, error) {
	ledger, ok, err := c.readStatement(ctx, stmtID)
	if err != nil || !ok {
		return false, err
	}
	participants := append([]string(nil), ledger.Participants...)
	if len(participants) == 0 {
		participants = c.configuredParticipants()
	}
	if len(participants) == 0 {
		return true, nil
	}
	for _, participant := range participants {
		participant = strings.TrimSpace(participant)
		if participant == "" {
			continue
		}
		exists, err := c.exists(ctx, c.promotionParticipantResultPath(stmtID, participant))
		if err != nil || !exists {
			return false, err
		}
	}
	return true, nil
}

func (c *KeeperCoordinator) readUnsafeResult(ctx context.Context, stmtID string) (UnsafeValidationResult, bool, error) {
	resultPath := c.unsafeResultPath(stmtID)
	data, ok, err := c.store.Get(ctx, resultPath)
	if err != nil || !ok {
		return UnsafeValidationResult{}, ok, err
	}
	if strings.TrimSpace(string(data)) != "" {
		fields, err := parseKeeperKV(data)
		if err != nil {
			return UnsafeValidationResult{}, false, err
		}
		result, err := decodeUnsafeResultKV(stmtID, fields)
		return result, err == nil, err
	}

	children, err := c.children(ctx, resultPath)
	if err != nil || len(children) == 0 {
		return UnsafeValidationResult{}, false, err
	}
	var aggregate UnsafeValidationResult
	for _, child := range children {
		participantID := unescapeSegment(child)
		childData, childOK, childErr := c.store.Get(ctx, c.unsafeParticipantResultPath(stmtID, participantID))
		if childErr != nil || !childOK {
			return UnsafeValidationResult{}, false, childErr
		}
		fields, err := parseKeeperKV(childData)
		if err != nil {
			return UnsafeValidationResult{}, false, err
		}
		result, err := decodeUnsafeResultKV(stmtID, fields)
		if err != nil {
			return UnsafeValidationResult{}, false, err
		}
		if aggregate.StatementID == "" {
			aggregate = result
			continue
		}
		if aggregate.RowCount != result.RowCount || aggregate.RowsHash != result.RowsHash {
			return UnsafeValidationResult{}, false, fmt.Errorf("unsafe result %q has inconsistent participant digest from %q", stmtID, participantID)
		}
		aggregate.Replicas = append(aggregate.Replicas, result.Replicas...)
	}
	return aggregate, true, nil
}

func decodeUnsafeResultKV(stmtID string, fields map[string]string) (UnsafeValidationResult, error) {
	rowCount, err := strconv.ParseUint(fields["row_count"], 10, 64)
	if err != nil {
		return UnsafeValidationResult{}, fmt.Errorf("unsafe result %q row_count: %w", stmtID, err)
	}
	result := UnsafeValidationResult{
		ValidationID: fields["validation_id"],
		StatementID:  firstNonEmpty(fields["statement_id"], stmtID),
		TableID:      fields["table_id"],
		UnsafeTable:  fields["unsafe_table"],
		RowCount:     rowCount,
		RowsHash:     fields["rows_hash"],
	}
	for _, digest := range splitKeeperCSV(fields["replica_digests"]) {
		parts := strings.SplitN(digest, ":", 3)
		if len(parts) != 3 {
			return UnsafeValidationResult{}, fmt.Errorf("unsafe result %q invalid replica digest %q", stmtID, digest)
		}
		replicaRows, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return UnsafeValidationResult{}, fmt.Errorf("unsafe result %q replica %q row_count: %w", stmtID, parts[0], err)
		}
		result.Replicas = append(result.Replicas, UnsafeReplicaDigest{ReplicaID: parts[0], RowCount: replicaRows, RowsHash: parts[2]})
	}
	if result.RowsHash == "" || len(result.Replicas) == 0 {
		return UnsafeValidationResult{}, fmt.Errorf("unsafe result %q is incomplete", stmtID)
	}
	return result, nil
}

func (c *KeeperCoordinator) getRollbackTask(ctx context.Context, stmtID string) (RollbackTask, bool, error) {
	data, ok, err := c.store.Get(ctx, c.rollbackTaskPath(stmtID))
	if err != nil || !ok {
		return RollbackTask{}, ok, err
	}
	fields, err := parseKeeperKV(data)
	if err != nil {
		return RollbackTask{}, false, err
	}
	unsafeTable := fields["unsafe_table"]
	if unsafeTable == "" {
		if statement, ok, err := c.readStatement(ctx, stmtID); err != nil {
			return RollbackTask{}, false, err
		} else if ok {
			unsafeTable = statement.UnsafeTable
		}
	}
	task := RollbackTask{
		RollbackID:  firstNonEmpty(fields["rollback_id"], "rollback-"+stmtID),
		LeaseID:     firstNonEmpty(fields["lease_id"], "rollback-lease-"+stmtID),
		BatchID:     fields["batch_id"],
		StatementID: firstNonEmpty(fields["statement_id"], stmtID),
		Reason:      fields["reason"],
		Statements:  []string{"TRUNCATE TABLE " + unsafeTable},
	}
	if task.RollbackID == "" || task.LeaseID == "" || task.StatementID == "" || unsafeTable == "" {
		return RollbackTask{}, false, fmt.Errorf("rollback task %q is incomplete", stmtID)
	}
	return task, true, nil
}

func (c *KeeperCoordinator) unsafeReplicaIDsCSV() string {
	ids := make([]string, 0, len(c.cfg.UnsafeReplicas))
	for _, replica := range c.cfg.UnsafeReplicas {
		if replica.ReplicaID != "" {
			ids = append(ids, replica.ReplicaID)
		}
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

func (c *KeeperCoordinator) unsafeReplicasByID(ids []string) []UnsafeReplica {
	byID := make(map[string]UnsafeReplica, len(c.cfg.UnsafeReplicas))
	for _, replica := range c.cfg.UnsafeReplicas {
		byID[replica.ReplicaID] = replica
	}
	out := make([]UnsafeReplica, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if replica, ok := byID[id]; ok {
			out = append(out, replica)
		} else {
			out = append(out, UnsafeReplica{ReplicaID: id})
		}
	}
	return out
}

func encodeUnsafeResultKV(result UnsafeValidationResult) []byte {
	digests := make([]string, 0, len(result.Replicas))
	for _, replica := range result.Replicas {
		digests = append(digests, replica.ReplicaID+":"+strconv.FormatUint(replica.RowCount, 10)+":"+replica.RowsHash)
	}
	sort.Strings(digests)
	return encodeKeeperKV(
		keeperKVPair{Key: "validation_id", Value: result.ValidationID},
		keeperKVPair{Key: "statement_id", Value: result.StatementID},
		keeperKVPair{Key: "table_id", Value: result.TableID},
		keeperKVPair{Key: "unsafe_table", Value: result.UnsafeTable},
		keeperKVPair{Key: "row_count", Value: strconv.FormatUint(result.RowCount, 10)},
		keeperKVPair{Key: "rows_hash", Value: result.RowsHash},
		keeperKVPair{Key: "replica_digests", Value: strings.Join(digests, ",")},
	)
}

func (c *KeeperCoordinator) tryClaim(ctx context.Context, p string, leaseID string) (bool, error) {
	claim := keeperLease{WorkerID: c.cfg.WorkerID, LeaseID: leaseID, ClaimedAt: time.Now().UTC()}
	if err := c.createJSON(ctx, p, claim); err != nil {
		if errors.Is(err, errKeeperNodeExists) {
			var existing keeperLease
			ok, getErr := c.getJSON(ctx, p, &existing)
			if getErr != nil || !ok {
				return false, getErr
			}
			return existing.WorkerID == c.cfg.WorkerID, nil
		}
		return false, err
	}
	return true, nil
}

func (c *KeeperCoordinator) ensureRoots(ctx context.Context) error {
	for _, p := range []string{
		c.cfg.Root,
		c.path("statements"),
		c.path("blocks"),
		c.path("replay_jobs"),
		c.path("attestations"),
		c.path("replay_failures"),
		c.path("unsafe_tasks"),
		c.path("unsafe_results"),
		c.path("unsafe_failures"),
		c.path("finality"),
		c.path("rollbacks"),
		c.path("rollback_tasks"),
		c.path("rollback_leases"),
		c.path("rollback_results"),
		c.path("rollback_failures"),
		c.path("promotions"),
		c.path("promotion_leases"),
		c.path("promotion_results"),
		c.path("promotion_failures"),
		c.path("safe_audit_tasks"),
		c.path("safe_audit_votes"),
		c.path("decisions"),
	} {
		if err := c.store.EnsurePath(ctx, p); err != nil {
			return fmt.Errorf("ensure housekeeper path %s: %w", p, err)
		}
	}
	return nil
}

func (c *KeeperCoordinator) createKV(ctx context.Context, p string, data []byte) error {
	return c.store.Create(ctx, p, data)
}

func (c *KeeperCoordinator) setKV(ctx context.Context, p string, data []byte) error {
	return c.store.Set(ctx, p, data)
}

func (c *KeeperCoordinator) createJSON(ctx context.Context, p string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", p, err)
	}
	if err := c.store.EnsurePath(ctx, parentPath(p)); err != nil {
		return err
	}
	return c.store.Create(ctx, p, data)
}

func (c *KeeperCoordinator) setJSON(ctx context.Context, p string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", p, err)
	}
	return c.store.Set(ctx, p, data)
}

func (c *KeeperCoordinator) getJSON(ctx context.Context, p string, v any) (bool, error) {
	data, ok, err := c.store.Get(ctx, p)
	if err != nil || !ok {
		return ok, err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return false, fmt.Errorf("unmarshal %s: %w", p, err)
	}
	return true, nil
}

func (c *KeeperCoordinator) exists(ctx context.Context, p string) (bool, error) {
	_, ok, err := c.store.Get(ctx, p)
	return ok, err
}

func (c *KeeperCoordinator) children(ctx context.Context, p string) ([]string, error) {
	children, err := c.store.Children(ctx, p)
	if errors.Is(err, errKeeperNoNode) {
		return nil, nil
	}
	sort.Strings(children)
	return children, err
}

func (c *KeeperCoordinator) path(parts ...string) string {
	all := append([]string{c.cfg.Root}, parts...)
	return path.Join(all...)
}

func (c *KeeperCoordinator) statementPath(stmtID string) string {
	return c.path("statements", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) blockPath(blockSeq uint64) string {
	return c.path("blocks", fmt.Sprintf("%020d", blockSeq))
}
func (c *KeeperCoordinator) replayJobPath(stmtID string) string {
	return c.path("replay_jobs", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) attestationsPath(stmtID string) string {
	return c.path("attestations", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) attestationPath(stmtID, replicaID string) string {
	return c.path("attestations", escapeSegment(stmtID), escapeSegment(replicaID))
}
func (c *KeeperCoordinator) replayFailurePath(blockSeq uint64, workerID string) string {
	return c.path("replay_failures", fmt.Sprintf("%020d", blockSeq), escapeSegment(workerID))
}
func (c *KeeperCoordinator) unsafeTaskPath(stmtID string) string {
	return c.path("unsafe_tasks", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) unsafeResultPath(stmtID string) string {
	return c.path("unsafe_results", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) unsafeParticipantResultPath(stmtID, participantID string) string {
	return c.path("unsafe_results", escapeSegment(stmtID), escapeSegment(participantID))
}
func (c *KeeperCoordinator) unsafeFailurePath(stmtID string) string {
	return c.path("unsafe_failures", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) finalityPath(stmtID string) string {
	return c.path("finality", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) rollbackEventPath(stmtID string) string {
	return c.path("rollbacks", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) rollbackTaskPath(stmtID string) string {
	return c.path("rollback_tasks", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) replayQuarantinePath(workerID string) string {
	return c.path("replay_quarantine", escapeSegment(workerID))
}
func (c *KeeperCoordinator) rollbackLeasePath(stmtID string) string {
	return c.path("rollback_leases", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) rollbackResultPath(stmtID string) string {
	return c.path("rollback_results", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) rollbackFailurePath(stmtID string) string {
	return c.path("rollback_failures", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) promotionPath(stmtID string) string {
	return c.path("promotions", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) promotionLeasePath(stmtID string) string {
	return c.path("promotion_leases", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) promotionParticipantLeasePath(stmtID, workerID string) string {
	return c.path("promotion_leases", escapeSegment(stmtID), escapeSegment(workerID))
}
func (c *KeeperCoordinator) promotionResultPath(stmtID string) string {
	return c.path("promotion_results", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) promotionParticipantResultPath(stmtID, workerID string) string {
	return c.path("promotion_results", escapeSegment(stmtID), escapeSegment(workerID))
}
func (c *KeeperCoordinator) promotionFailurePath(stmtID string) string {
	return c.path("promotion_failures", escapeSegment(stmtID))
}
func (c *KeeperCoordinator) promotionParticipantFailurePath(stmtID, workerID string) string {
	return c.path("promotion_failures", escapeSegment(stmtID), escapeSegment(workerID))
}
func (c *KeeperCoordinator) safeAuditTaskPath(key string) string {
	return c.path("safe_audit_tasks", escapeSegment(key))
}
func (c *KeeperCoordinator) safeAuditVotePath(key string) string {
	return c.path("safe_audit_votes", escapeSegment(key))
}

type keeperStatementRecord struct {
	InsertRecord
	BlockSeq     uint64   `json:"block_seq"`
	Participants []string `json:"participants,omitempty"`
}

type keeperKVPair struct {
	Key   string
	Value string
}

func encodeKeeperKV(pairs ...keeperKVPair) []byte {
	var b strings.Builder
	for _, pair := range pairs {
		if pair.Key == "" {
			continue
		}
		b.WriteString(pair.Key)
		b.WriteByte('=')
		b.WriteString(pair.Value)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

func parseKeeperKV(data []byte) (map[string]string, error) {
	fields := map[string]string{}
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		equals := strings.IndexByte(line, '=')
		if equals < 0 {
			return nil, fmt.Errorf("invalid key/value line %q", line)
		}
		key := strings.TrimSpace(line[:equals])
		if key == "" {
			return nil, fmt.Errorf("empty key in key/value line %q", line)
		}
		fields[key] = strings.TrimSpace(line[equals+1:])
	}
	return fields, nil
}

func splitKeeperCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	items := strings.Split(value, ",")
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func normalizeKeeperCSVValues(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func parseOptionalUint(value string) (uint64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func decodeOptionalBase64(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

type keeperBlockRecord struct {
	BlockSeq    uint64 `json:"block_seq"`
	StatementID string `json:"statement_id"`
}

type keeperLease struct {
	WorkerID  string    `json:"worker_id"`
	LeaseID   string    `json:"lease_id"`
	ClaimedAt time.Time `json:"claimed_at"`
}

func replayBlockSeq(statementID string) uint64 {
	sum := replay.DigestBytes([]byte(statementID))
	var out uint64
	for _, ch := range sum {
		out = out*131 + uint64(ch)
	}
	if out == 0 {
		return 1
	}
	return out
}

func escapeSegment(s string) string {
	return url.PathEscape(s)
}

func unescapeSegment(s string) string {
	out, err := url.PathUnescape(s)
	if err != nil {
		return s
	}
	return out
}

func parentPath(p string) string {
	parent := path.Dir(cleanKeeperPath(p))
	if parent == "." {
		return "/"
	}
	return parent
}

func cleanKeeperPath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

type zkKeeperStore struct {
	conn *zk.Conn
}

func (s *zkKeeperStore) EnsurePath(ctx context.Context, p string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p = cleanKeeperPath(p)
	if p == "/" {
		return nil
	}
	cur := ""
	for _, part := range strings.Split(strings.Trim(p, "/"), "/") {
		cur += "/" + part
		exists, _, err := s.conn.Exists(cur)
		if err != nil {
			return fmt.Errorf("exists %s: %w", cur, mapZKErr(err))
		}
		if exists {
			continue
		}
		_, err = s.conn.Create(cur, nil, 0, zk.WorldACL(zk.PermAll))
		mapped := mapZKErr(err)
		if mapped != nil && !errors.Is(mapped, errKeeperNodeExists) {
			return fmt.Errorf("create %s: %w", cur, mapped)
		}
	}
	return nil
}

func (s *zkKeeperStore) Create(ctx context.Context, p string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clean := cleanKeeperPath(p)
	_, err := s.conn.Create(clean, data, 0, zk.WorldACL(zk.PermAll))
	if mapped := mapZKErr(err); mapped != nil {
		return fmt.Errorf("create %s: %w", clean, mapped)
	}
	return nil
}

func (s *zkKeeperStore) Set(ctx context.Context, p string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clean := cleanKeeperPath(p)
	_, err := s.conn.Set(clean, data, -1)
	if mapped := mapZKErr(err); mapped != nil {
		return fmt.Errorf("set %s: %w", clean, mapped)
	}
	return nil
}

func (s *zkKeeperStore) Get(ctx context.Context, p string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	clean := cleanKeeperPath(p)
	data, _, err := s.conn.Get(clean)
	mapped := mapZKErr(err)
	if errors.Is(mapped, errKeeperNoNode) {
		return nil, false, nil
	}
	if mapped != nil {
		return nil, false, fmt.Errorf("get %s: %w", clean, mapped)
	}
	return data, true, nil
}

func (s *zkKeeperStore) Children(ctx context.Context, p string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	clean := cleanKeeperPath(p)
	children, _, err := s.conn.Children(clean)
	if mapped := mapZKErr(err); mapped != nil {
		return nil, fmt.Errorf("children %s: %w", clean, mapped)
	}
	return children, nil
}

func (s *zkKeeperStore) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

func mapZKErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, zk.ErrNodeExists) {
		return errKeeperNodeExists
	}
	if errors.Is(err, zk.ErrNoNode) {
		return errKeeperNoNode
	}
	return err
}

var _ IngressSink = (*KeeperCoordinator)(nil)
var _ FinalitySink = (*KeeperCoordinator)(nil)
var _ ReplayJobSource = (*KeeperCoordinator)(nil)
var _ ReplaySink = (*KeeperCoordinator)(nil)
var _ UnsafeValidationSource = (*KeeperCoordinator)(nil)
var _ UnsafeValidationSink = (*KeeperCoordinator)(nil)
var _ PromotionSource = (*KeeperCoordinator)(nil)
var _ PromotionSink = (*KeeperCoordinator)(nil)
var _ RollbackSource = (*KeeperCoordinator)(nil)
var _ RollbackSink = (*KeeperCoordinator)(nil)
var _ SafeAuditSource = (*KeeperCoordinator)(nil)
var _ SafeAuditSink = (*KeeperCoordinator)(nil)
