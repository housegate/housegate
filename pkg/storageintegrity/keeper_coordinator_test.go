package storageintegrity

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"

	"housegate/housegate/pkg/replay"
)

func TestWaitForZKSessionIgnoresTransientStates(t *testing.T) {
	events := make(chan zk.Event, 2)
	events <- zk.Event{State: zk.StateConnecting}
	events <- zk.Event{State: zk.StateHasSession}

	if err := waitForZKSession(context.Background(), events, time.Second); err != nil {
		t.Fatalf("waitForZKSession: %v", err)
	}
}

func TestWaitForZKSessionReturnsAuthFailure(t *testing.T) {
	events := make(chan zk.Event, 1)
	events <- zk.Event{State: zk.StateAuthFailed}

	err := waitForZKSession(context.Background(), events, time.Second)
	if err == nil || !strings.Contains(err.Error(), "auth failed") {
		t.Fatalf("waitForZKSession err = %v, want auth failed", err)
	}
}

func TestKeeperCoordinatorEnsuresDecisionRootForKeeperSideEffects(t *testing.T) {
	store := newMemoryKeeperStore()
	c := newTestKeeperCoordinator(t, store, "hg-1")

	if exists := testKeeperNodeExists(t, store, c.path("decisions")); !exists {
		t.Fatalf("decisions root was not created")
	}
}

func TestKeeperCoordinatorRequiresReplayQuorumAndUnsafeAllReplicasBeforePromotion(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "hg-1")
	hg2 := newTestKeeperCoordinator(t, store, "hg-2")
	hg3 := newTestKeeperCoordinator(t, store, "hg-3")

	rec := testKeeperInsertRecord("stmt-keeper-1")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	if exists := testKeeperNodeExists(t, store, hg1.replayJobPath(rec.StatementID)); exists {
		t.Fatalf("SubmitInsert created replay job; Keeper C++ should own managed task creation")
	}
	if exists := testKeeperNodeExists(t, store, hg1.unsafeTaskPath(rec.StatementID)); exists {
		t.Fatalf("SubmitInsert created unsafe task; Keeper C++ should own managed task creation")
	}
	if statementData := testKeeperNodeData(t, store, hg1.statementPath(rec.StatementID)); strings.HasPrefix(strings.TrimSpace(statementData), "{") {
		t.Fatalf("statement znode is JSON, want Keeper C++ key/value protocol: %s", statementData)
	}
	testKeeperMaterializeStatementTasks(t, store, hg1, rec)

	if _, ok, err := hg1.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion before quorum ok=%v err=%v, want no task", ok, err)
	}

	job1, ok, err := hg1.ClaimReplayJob(ctx)
	if err != nil || !ok {
		t.Fatalf("hg1 ClaimReplayJob ok=%v err=%v", ok, err)
	}
	job2, ok, err := hg2.ClaimReplayJob(ctx)
	if err != nil || !ok {
		t.Fatalf("hg2 ClaimReplayJob ok=%v err=%v", ok, err)
	}
	if _, ok, err := hg3.ClaimReplayJob(ctx); err != nil || !ok {
		t.Fatalf("hg3 ClaimReplayJob before quorum ok=%v err=%v, want visible job", ok, err)
	}
	if job1.BlockSeq != job2.BlockSeq || job1.Statements[0].StatementID != rec.StatementID {
		t.Fatalf("replay jobs = %+v / %+v", job1, job2)
	}

	if err := hg1.SubmitReplayAttestation(ctx, testReplayAttestation(job1, "hg-1", "0xstate-a")); err != nil {
		t.Fatalf("hg1 SubmitReplayAttestation: %v", err)
	}
	if err := hg1.SubmitFinality(ctx, FinalityRecord{
		Kind:        "mock",
		BatchID:     rec.StatementID,
		StatementID: rec.StatementID,
		PayloadRef:  rec.Payload.Ref,
		PayloadHash: rec.Payload.Hash,
		Finalized:   true,
	}); err != nil {
		t.Fatalf("SubmitFinality: %v", err)
	}
	unsafeTask, ok, err := hg1.ClaimUnsafeValidation(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimUnsafeValidation ok=%v err=%v", ok, err)
	}
	if err := hg1.SubmitUnsafeValidation(ctx, testUnsafeResult(unsafeTask, "0xrows", "r1", "r2", "r3")); err != nil {
		t.Fatalf("SubmitUnsafeValidation: %v", err)
	}
	if unsafeData := testKeeperNodeData(t, store, hg1.unsafeResultPath(rec.StatementID)); !strings.Contains(unsafeData, "replica_digests=r1:1:0xrows,r2:1:0xrows,r3:1:0xrows\n") {
		t.Fatalf("unsafe result znode = %q, want Keeper C++ key/value replica_digests", unsafeData)
	}
	if _, ok, err := hg1.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion with one replay vote ok=%v err=%v, want no task", ok, err)
	}

	if err := hg2.SubmitReplayAttestation(ctx, testReplayAttestation(job2, "hg-2", "0xstate-a")); err != nil {
		t.Fatalf("hg2 SubmitReplayAttestation: %v", err)
	}
	testKeeperMaterializePromotion(t, store, hg1, rec)
	task, ok, err := hg1.ClaimPromotion(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimPromotion after 2/3 replay + 3/3 unsafe + finality ok=%v err=%v", ok, err)
	}
	if task.PromotionID != "promotion-"+rec.StatementID || task.LeaseID == "" {
		t.Fatalf("promotion task ids = %+v", task)
	}
	if got, want := task.Statements[0], "INSERT INTO `hg_safe`.`dual_hg_auth.t` SELECT * FROM `hg_unsafe`.`dual_hg_auth.t_a`"; got != want {
		t.Fatalf("promotion INSERT = %q, want %q", got, want)
	}
	if got, want := task.Statements[1], "TRUNCATE TABLE `hg_unsafe`.`dual_hg_auth.t_a`"; got != want {
		t.Fatalf("promotion TRUNCATE = %q, want %q", got, want)
	}
}

func TestKeeperCoordinatorRequiresUnsafeValidationFromEveryConfiguredReplica(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "hg-1")
	hg2 := newTestKeeperCoordinator(t, store, "hg-2")

	rec := testKeeperInsertRecord("stmt-keeper-unsafe")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	testKeeperMaterializeStatementTasks(t, store, hg1, rec)
	job1, _, _ := hg1.ClaimReplayJob(ctx)
	job2, _, _ := hg2.ClaimReplayJob(ctx)
	if err := hg1.SubmitReplayAttestation(ctx, testReplayAttestation(job1, "hg-1", "0xstate-a")); err != nil {
		t.Fatalf("hg1 SubmitReplayAttestation: %v", err)
	}
	if err := hg2.SubmitReplayAttestation(ctx, testReplayAttestation(job2, "hg-2", "0xstate-a")); err != nil {
		t.Fatalf("hg2 SubmitReplayAttestation: %v", err)
	}
	if err := hg1.SubmitFinality(ctx, FinalityRecord{Kind: "mock", BatchID: rec.StatementID, StatementID: rec.StatementID, Finalized: true}); err != nil {
		t.Fatalf("SubmitFinality: %v", err)
	}
	unsafeTask, ok, err := hg1.ClaimUnsafeValidation(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimUnsafeValidation ok=%v err=%v", ok, err)
	}
	if err := hg1.SubmitUnsafeValidation(ctx, testUnsafeResult(unsafeTask, "0xrows", "r1", "r2")); err == nil {
		t.Fatalf("SubmitUnsafeValidation with missing replica succeeded, want error")
	}
	if _, ok, err := hg1.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion after incomplete unsafe ok=%v err=%v, want no task", ok, err)
	}
}

func TestKeeperCoordinatorRollbackBlocksPromotionAndQueuesRollbackTask(t *testing.T) {
	ctx := context.Background()
	store := newMemoryKeeperStore()
	hg1 := newTestKeeperCoordinator(t, store, "hg-1")
	hg2 := newTestKeeperCoordinator(t, store, "hg-2")

	rec := testKeeperInsertRecord("stmt-keeper-rollback")
	if err := hg1.SubmitInsert(ctx, rec); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	testKeeperMaterializeStatementTasks(t, store, hg1, rec)
	job1, _, _ := hg1.ClaimReplayJob(ctx)
	job2, _, _ := hg2.ClaimReplayJob(ctx)
	unsafeTask, _, _ := hg1.ClaimUnsafeValidation(ctx)
	if err := hg1.SubmitReplayAttestation(ctx, testReplayAttestation(job1, "hg-1", "0xstate-a")); err != nil {
		t.Fatalf("hg1 SubmitReplayAttestation: %v", err)
	}
	if err := hg2.SubmitReplayAttestation(ctx, testReplayAttestation(job2, "hg-2", "0xstate-a")); err != nil {
		t.Fatalf("hg2 SubmitReplayAttestation: %v", err)
	}
	if err := hg1.SubmitUnsafeValidation(ctx, testUnsafeResult(unsafeTask, "0xrows", "r1", "r2", "r3")); err != nil {
		t.Fatalf("SubmitUnsafeValidation: %v", err)
	}
	if err := hg1.SubmitRollback(ctx, RollbackEvent{Kind: "mock", BatchID: rec.StatementID, StatementID: rec.StatementID, Reason: "dispute"}); err != nil {
		t.Fatalf("SubmitRollback: %v", err)
	}
	if exists := testKeeperNodeExists(t, store, hg1.rollbackTaskPath(rec.StatementID)); exists {
		t.Fatalf("SubmitRollback created rollback task; Keeper C++ should own managed task creation")
	}
	if err := hg1.SubmitFinality(ctx, FinalityRecord{Kind: "mock", BatchID: rec.StatementID, StatementID: rec.StatementID, Finalized: true}); err != nil {
		t.Fatalf("SubmitFinality: %v", err)
	}
	if _, ok, err := hg1.ClaimPromotion(ctx); err != nil || ok {
		t.Fatalf("ClaimPromotion after rollback ok=%v err=%v, want no task", ok, err)
	}
	testKeeperMaterializeRollback(t, store, hg1, rec, "dispute")
	task, ok, err := hg1.ClaimRollback(ctx)
	if err != nil || !ok {
		t.Fatalf("ClaimRollback ok=%v err=%v, want task", ok, err)
	}
	if task.RollbackID != "rollback-"+rec.StatementID || task.LeaseID == "" || len(task.Statements) != 1 {
		t.Fatalf("rollback task = %+v", task)
	}
}

func newTestKeeperCoordinator(t *testing.T, store keeperStore, workerID string) *KeeperCoordinator {
	t.Helper()
	c, err := NewKeeperCoordinatorWithStore(KeeperCoordinatorConfig{
		Root:                    "/housekeeper/v1/storage_integrity_test",
		WorkerID:                workerID,
		ReplayQuorum:            2,
		RequireFinality:         true,
		RequireReplay:           true,
		RequireUnsafeValidation: true,
		UnsafeReplicas: []UnsafeReplica{
			{ReplicaID: "r1", Addr: "127.0.0.1:9001"},
			{ReplicaID: "r2", Addr: "127.0.0.1:9002"},
			{ReplicaID: "r3", Addr: "127.0.0.1:9003"},
		},
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
	}, store)
	if err != nil {
		t.Fatalf("NewKeeperCoordinatorWithStore: %v", err)
	}
	return c
}

func testKeeperInsertRecord(statementID string) InsertRecord {
	return InsertRecord{
		TableID:     "dual_hg_auth.t",
		StatementID: statementID,
		OriginalSQL: "INSERT INTO dual_hg_auth.t VALUES (1)",
		UnsafeSQL:   "INSERT INTO `hg_unsafe`.`dual_hg_auth.t_a` VALUES (1)",
		UnsafeTable: "`hg_unsafe`.`dual_hg_auth.t_a`",
		SafeTable:   "`hg_safe`.`dual_hg_auth.t`",
		Payload: PayloadCommitment{
			Ref:    "mockda://dual_hg_auth.t/" + statementID + "/hash",
			Hash:   "0xpayload",
			Length: 11,
		},
	}
}

func testReplayAttestation(job replay.ReplayJob, replicaID, stateRoot string) replay.ReplayAttestation {
	receipt := replay.ExecutionReceipt{
		BlockSeq:          job.BlockSeq,
		SourceClaimRoot:   job.SourceClaimRoot,
		ComputedStateRoot: stateRoot,
		MatchSourceRoot:   true,
	}
	return replay.ReplayAttestation{
		ReplicaID:       replicaID,
		Receipt:         receipt,
		ReceiptHash:     "receipt-" + stateRoot,
		Signature:       "sig-" + replicaID,
		MatchSourceRoot: true,
	}
}

func testUnsafeResult(task UnsafeValidationTask, rowsHash string, replicas ...string) UnsafeValidationResult {
	result := UnsafeValidationResult{
		ValidationID: task.ValidationID,
		StatementID:  task.StatementID,
		TableID:      task.TableID,
		UnsafeTable:  task.UnsafeTable,
		RowCount:     1,
		RowsHash:     rowsHash,
	}
	for _, replicaID := range replicas {
		result.Replicas = append(result.Replicas, UnsafeReplicaDigest{ReplicaID: replicaID, RowCount: 1, RowsHash: rowsHash})
	}
	return result
}

func testKeeperMaterializeStatementTasks(t *testing.T, store keeperStore, c *KeeperCoordinator, rec InsertRecord) {
	t.Helper()
	testKeeperEnsurePath(t, store, c.attestationsPath(rec.StatementID))
	testKeeperCreateNode(t, store, c.replayJobPath(rec.StatementID),
		"statement_id="+rec.StatementID+"\n"+
			"table_id="+rec.TableID+"\n"+
			"payload_ref="+rec.Payload.Ref+"\n"+
			"payload_hash="+rec.Payload.Hash+"\n")
	testKeeperCreateNode(t, store, c.unsafeTaskPath(rec.StatementID),
		"statement_id="+rec.StatementID+"\n"+
			"table_id="+rec.TableID+"\n"+
			"unsafe_table="+rec.UnsafeTable+"\n"+
			"replicas=r1,r2,r3\n")
	testKeeperCreateNode(t, store, c.path("decisions", escapeSegment(rec.StatementID)),
		"statement_id="+rec.StatementID+"\n"+
			"replay_quorum_met=false\n"+
			"unsafe_validated=false\n"+
			"finalized=false\n"+
			"rollback_requested=false\n"+
			"promotion_ready=false\n"+
			"rollback_ready=false\n"+
			"replay_result_hash=\n"+
			"replay_tally=\n")
}

func testKeeperMaterializePromotion(t *testing.T, store keeperStore, c *KeeperCoordinator, rec InsertRecord) {
	t.Helper()
	testKeeperCreateNode(t, store, c.promotionPath(rec.StatementID),
		"promotion_id=promotion-"+rec.StatementID+"\n"+
			"lease_id=lease-"+rec.StatementID+"\n"+
			"unsafe_table="+rec.UnsafeTable+"\n"+
			"safe_table="+rec.SafeTable+"\n")
}

func testKeeperMaterializeRollback(t *testing.T, store keeperStore, c *KeeperCoordinator, rec InsertRecord, reason string) {
	t.Helper()
	testKeeperCreateNode(t, store, c.rollbackTaskPath(rec.StatementID),
		"rollback_id=rollback-"+rec.StatementID+"\n"+
			"lease_id=rollback-lease-"+rec.StatementID+"\n"+
			"statement_id="+rec.StatementID+"\n"+
			"batch_id="+rec.StatementID+"\n"+
			"reason="+reason+"\n"+
			"unsafe_table="+rec.UnsafeTable+"\n")
}

func testKeeperEnsurePath(t *testing.T, store keeperStore, p string) {
	t.Helper()
	if err := store.EnsurePath(context.Background(), p); err != nil {
		t.Fatalf("EnsurePath(%s): %v", p, err)
	}
}

func testKeeperCreateNode(t *testing.T, store keeperStore, p string, data string) {
	t.Helper()
	testKeeperEnsurePath(t, store, parentPath(p))
	if err := store.Create(context.Background(), p, []byte(data)); err != nil {
		t.Fatalf("Create(%s): %v", p, err)
	}
}

func testKeeperNodeExists(t *testing.T, store keeperStore, p string) bool {
	t.Helper()
	_, ok, err := store.Get(context.Background(), p)
	if err != nil {
		t.Fatalf("Get(%s): %v", p, err)
	}
	return ok
}

func testKeeperNodeData(t *testing.T, store keeperStore, p string) string {
	t.Helper()
	data, ok, err := store.Get(context.Background(), p)
	if err != nil {
		t.Fatalf("Get(%s): %v", p, err)
	}
	if !ok {
		t.Fatalf("Get(%s): missing node", p)
	}
	return string(data)
}

type memoryKeeperStore struct {
	mu    sync.Mutex
	nodes map[string][]byte
}

func newMemoryKeeperStore() *memoryKeeperStore {
	return &memoryKeeperStore{nodes: map[string][]byte{"/": nil}}
}

func (s *memoryKeeperStore) EnsurePath(_ context.Context, p string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := strings.Split(strings.Trim(p, "/"), "/")
	cur := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		cur += "/" + part
		if _, ok := s.nodes[cur]; !ok {
			s.nodes[cur] = nil
		}
	}
	return nil
}

func (s *memoryKeeperStore) Create(_ context.Context, p string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[p]; ok {
		return errKeeperNodeExists
	}
	parent := parentPath(p)
	if _, ok := s.nodes[parent]; !ok {
		return errKeeperNoNode
	}
	s.nodes[p] = append([]byte(nil), data...)
	return nil
}

func (s *memoryKeeperStore) Set(_ context.Context, p string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[p]; !ok {
		return errKeeperNoNode
	}
	s.nodes[p] = append([]byte(nil), data...)
	return nil
}

func (s *memoryKeeperStore) Get(_ context.Context, p string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.nodes[p]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), data...), true, nil
}

func (s *memoryKeeperStore) Children(_ context.Context, p string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[p]; !ok {
		return nil, errKeeperNoNode
	}
	seen := map[string]struct{}{}
	prefix := strings.TrimRight(p, "/") + "/"
	for node := range s.nodes {
		if !strings.HasPrefix(node, prefix) {
			continue
		}
		rest := strings.TrimPrefix(node, prefix)
		if rest == "" || strings.Contains(rest, "/") {
			continue
		}
		seen[rest] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for child := range seen {
		out = append(out, child)
	}
	sort.Strings(out)
	return out, nil
}

func (s *memoryKeeperStore) Close() {}

var _ keeperStore = (*memoryKeeperStore)(nil)

func TestMemoryKeeperStoreErrorsMatchSentinels(t *testing.T) {
	store := newMemoryKeeperStore()
	if err := store.Create(context.Background(), "/missing/child", nil); !errors.Is(err, errKeeperNoNode) {
		t.Fatalf("Create without parent err=%v, want errKeeperNoNode", err)
	}
}
