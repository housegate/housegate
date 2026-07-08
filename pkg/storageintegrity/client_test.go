package storageintegrity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"housegate/housegate/pkg/replay"
)

func TestHTTPDAClientPutsPayload(t *testing.T) {
	var got struct {
		TableID     string `json:"table_id"`
		StatementID string `json:"statement_id"`
		Payload     string `json:"payload_base64"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/da/payloads" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(PayloadCommitment{Ref: "mockda://accounts.events/stmt-1/hash", Hash: "0xhash", Length: 3})
	}))
	defer srv.Close()

	client := NewHTTPDAClient(srv.URL)
	commit, err := client.PutPayload(context.Background(), PutPayloadRequest{
		TableID:     "accounts.events",
		StatementID: "stmt-1",
		Payload:     []byte("abc"),
	})
	if err != nil {
		t.Fatalf("PutPayload: %v", err)
	}
	if commit.Ref == "" || commit.Hash != "0xhash" || commit.Length != 3 {
		t.Fatalf("commit = %+v", commit)
	}
	if got.Payload != base64.StdEncoding.EncodeToString([]byte("abc")) {
		t.Fatalf("payload = %q", got.Payload)
	}
}

func TestHTTPSequencerClientSubmitsInsert(t *testing.T) {
	var got InsertRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/sequencer/inserts" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(Ack{OK: true})
	}))
	defer srv.Close()

	client := NewHTTPSequencerClient(srv.URL)
	err := client.SubmitInsert(context.Background(), InsertRecord{
		TableID:     "accounts.events",
		StatementID: "stmt-1",
		Payload: PayloadCommitment{
			Ref:    "mockda://accounts.events/stmt-1/hash",
			Hash:   "0xhash",
			Length: 3,
		},
	})
	if err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	if got.StatementID != "stmt-1" || got.Payload.Ref == "" {
		t.Fatalf("insert record = %+v", got)
	}
}

func TestHTTPArbiterClientSubmitsInsertToArbiterEndpoint(t *testing.T) {
	var got InsertRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/arbiter/inserts" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(Ack{OK: true})
	}))
	defer srv.Close()

	client := NewHTTPArbiterClient(srv.URL)
	err := client.SubmitInsert(context.Background(), InsertRecord{
		TableID:     "accounts.events",
		StatementID: "stmt-1",
		Payload: PayloadCommitment{
			Ref:    "mockda://accounts.events/stmt-1/hash",
			Hash:   "0xhash",
			Length: 3,
		},
	})
	if err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	if got.StatementID != "stmt-1" || got.Payload.Ref == "" {
		t.Fatalf("insert record = %+v", got)
	}
}

func TestHTTPArbiterClientFallsBackToLegacySequencerEndpoint(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case "/v1/arbiter/inserts":
			http.NotFound(w, r)
		case "/v1/sequencer/inserts":
			_ = json.NewEncoder(w).Encode(Ack{OK: true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewHTTPArbiterClient(srv.URL)
	if err := client.SubmitInsert(context.Background(), InsertRecord{TableID: "accounts.events", StatementID: "stmt-1"}); err != nil {
		t.Fatalf("SubmitInsert: %v", err)
	}
	if len(paths) != 2 || paths[0] != "/v1/arbiter/inserts" || paths[1] != "/v1/sequencer/inserts" {
		t.Fatalf("paths = %v, want arbiter then legacy sequencer fallback", paths)
	}
}

func TestHTTPClientsSupportWorkerEndpoints(t *testing.T) {
	var replaySubmitted replay.ReplayAttestation
	var byteSubmitted ByteSideScanResult
	var promotionSubmitted PromotionResult
	var mutationClaimSubmitted MutationClaim
	var mutationReplaySubmitted MutationReplayResult
	var safeAuditSubmitted SafeAuditVote
	var rollbackSubmitted RollbackResult
	var repairSyncSubmitted RepairSyncResult
	var compactionSubmitted CompactionResult
	var readSetReq SafeReadRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/da/get-payload":
			var req struct {
				Ref string `json:"ref"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode get payload: %v", err)
			}
			if req.Ref != "mockda://payload" {
				t.Fatalf("payload ref = %q", req.Ref)
			}
			_ = json.NewEncoder(w).Encode(struct {
				Payload string `json:"payload_base64"`
			}{Payload: base64.StdEncoding.EncodeToString([]byte("payload"))})
		case "/v1/sequencer/replay-jobs/claim":
			_ = json.NewEncoder(w).Encode(struct {
				OK  bool             `json:"ok"`
				Job replay.ReplayJob `json:"job"`
			}{OK: true, Job: replay.ReplayJob{BlockSeq: 9, SourceClaimRoot: "root"}})
		case "/v1/sequencer/replay-attestations":
			if err := json.NewDecoder(r.Body).Decode(&replaySubmitted); err != nil {
				t.Fatalf("decode replay attestation: %v", err)
			}
			_ = json.NewEncoder(w).Encode(Ack{OK: true})
		case "/v1/sequencer/byte-side-scans/claim":
			_ = json.NewEncoder(w).Encode(struct {
				OK   bool             `json:"ok"`
				Task ByteSideScanTask `json:"task"`
			}{OK: true, Task: ByteSideScanTask{ScanID: "scan-1", StatementID: "stmt-1"}})
		case "/v1/sequencer/byte-side-scans":
			if err := json.NewDecoder(r.Body).Decode(&byteSubmitted); err != nil {
				t.Fatalf("decode byte scan: %v", err)
			}
			_ = json.NewEncoder(w).Encode(Ack{OK: true})
		case "/v1/sequencer/promotions/claim":
			_ = json.NewEncoder(w).Encode(struct {
				OK   bool          `json:"ok"`
				Task PromotionTask `json:"task"`
			}{OK: true, Task: PromotionTask{PromotionID: "promotion-1", TableID: "tenant.events", SafeTable: "`hg_safe`.`events`"}})
		case "/v1/sequencer/promotions":
			if err := json.NewDecoder(r.Body).Decode(&promotionSubmitted); err != nil {
				t.Fatalf("decode promotion result: %v", err)
			}
			_ = json.NewEncoder(w).Encode(PromotionReceipt{
				OK: true,
				Watermark: SafeWatermark{
					SnapshotID:   "snap-2",
					SafeL3BlockSeq: 10,
					StateRoot:    "state-root-2",
					ManifestRoot: "manifest-root-2",
				},
			})
		case "/v1/sequencer/safe-watermark":
			_ = json.NewEncoder(w).Encode(struct {
				OK        bool          `json:"ok"`
				Watermark SafeWatermark `json:"watermark"`
			}{OK: true, Watermark: SafeWatermark{SnapshotID: "snap-2", SafeL3BlockSeq: 10}})
		case "/v1/sequencer/safe-snapshots/get":
			var req struct {
				SnapshotID string `json:"snapshot_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode safe snapshot request: %v", err)
			}
			if req.SnapshotID != "snap-2" {
				t.Fatalf("snapshot request = %q", req.SnapshotID)
			}
			manifest := sealedClientTestManifest(t)
			_ = json.NewEncoder(w).Encode(struct {
				OK       bool                        `json:"ok"`
				Manifest replay.SafeSnapshotManifest `json:"manifest"`
			}{OK: true, Manifest: manifest})
		case "/v1/sequencer/read-set/check":
			if err := json.NewDecoder(r.Body).Decode(&readSetReq); err != nil {
				t.Fatalf("decode read-set request: %v", err)
			}
			_ = json.NewEncoder(w).Encode(struct {
				OK       bool             `json:"ok"`
				Decision SafeReadDecision `json:"decision"`
			}{OK: true, Decision: SafeReadDecision{Active: false, Reason: "node quarantined", SnapshotID: "snap-2", SafeL3BlockSeq: 10}})
		case "/v1/sequencer/mutation-tasks/claim":
			_ = json.NewEncoder(w).Encode(struct {
				OK   bool         `json:"ok"`
				Task MutationTask `json:"task"`
			}{OK: true, Task: MutationTask{StatementID: "stmt-mut", MutationType: MutationTypeUpdate}})
		case "/v1/sequencer/mutation-claims":
			if err := json.NewDecoder(r.Body).Decode(&mutationClaimSubmitted); err != nil {
				t.Fatalf("decode mutation claim: %v", err)
			}
			_ = json.NewEncoder(w).Encode(Ack{OK: true})
		case "/v1/sequencer/mutation-replays/claim":
			_ = json.NewEncoder(w).Encode(struct {
				OK   bool         `json:"ok"`
				Task MutationTask `json:"task"`
			}{OK: true, Task: MutationTask{StatementID: "stmt-mut", MutationType: MutationTypeUpdate}})
		case "/v1/sequencer/mutation-replays":
			if err := json.NewDecoder(r.Body).Decode(&mutationReplaySubmitted); err != nil {
				t.Fatalf("decode mutation replay: %v", err)
			}
			_ = json.NewEncoder(w).Encode(Ack{OK: true})
		case "/v1/sequencer/safe-audits/claim":
			_ = json.NewEncoder(w).Encode(struct {
				OK   bool          `json:"ok"`
				Task SafeAuditTask `json:"task"`
			}{OK: true, Task: SafeAuditTask{AuditID: "audit-1", SnapshotID: "snap"}})
		case "/v1/sequencer/safe-audits":
			if err := json.NewDecoder(r.Body).Decode(&safeAuditSubmitted); err != nil {
				t.Fatalf("decode safe audit: %v", err)
			}
			_ = json.NewEncoder(w).Encode(Ack{OK: true})
		case "/v1/sequencer/rollbacks/claim":
			_ = json.NewEncoder(w).Encode(struct {
				OK   bool         `json:"ok"`
				Task RollbackTask `json:"task"`
			}{OK: true, Task: RollbackTask{RollbackID: "rollback-1", StatementID: "stmt-1", UnsafeTable: "`hg_unsafe`.`events`"}})
		case "/v1/sequencer/rollbacks":
			if err := json.NewDecoder(r.Body).Decode(&rollbackSubmitted); err != nil {
				t.Fatalf("decode rollback: %v", err)
			}
			_ = json.NewEncoder(w).Encode(Ack{OK: true})
		case "/v1/sequencer/repair-sync/claim":
			_ = json.NewEncoder(w).Encode(struct {
				OK   bool           `json:"ok"`
				Task RepairSyncTask `json:"task"`
			}{OK: true, Task: RepairSyncTask{RepairID: "repair-1", SnapshotID: "snap-2", SafeTable: "`hg_safe`.`events`"}})
		case "/v1/sequencer/repair-sync":
			if err := json.NewDecoder(r.Body).Decode(&repairSyncSubmitted); err != nil {
				t.Fatalf("decode repair sync: %v", err)
			}
			_ = json.NewEncoder(w).Encode(Ack{OK: true})
		case "/v1/sequencer/compactions/claim":
			_ = json.NewEncoder(w).Encode(struct {
				OK   bool           `json:"ok"`
				Task CompactionTask `json:"task"`
			}{OK: true, Task: CompactionTask{CompactionID: "compact-1", SafeTable: "`hg_safe`.`events`", PartitionIDs: []string{"all"}}})
		case "/v1/sequencer/compactions":
			if err := json.NewDecoder(r.Body).Decode(&compactionSubmitted); err != nil {
				t.Fatalf("decode compaction: %v", err)
			}
			_ = json.NewEncoder(w).Encode(Ack{OK: true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	da := NewHTTPDAClient(srv.URL)
	payload, err := da.GetPayload(context.Background(), "mockda://payload")
	if err != nil {
		t.Fatalf("GetPayload: %v", err)
	}
	if string(payload) != "payload" {
		t.Fatalf("payload = %q", payload)
	}

	seq := NewHTTPSequencerClient(srv.URL)
	job, ok, err := seq.ClaimReplayJob(context.Background())
	if err != nil || !ok || job.BlockSeq != 9 {
		t.Fatalf("ClaimReplayJob = job %+v ok %v err %v", job, ok, err)
	}
	if err := seq.SubmitReplayAttestation(context.Background(), replay.ReplayAttestation{ReplicaID: "replica-a"}); err != nil {
		t.Fatalf("SubmitReplayAttestation: %v", err)
	}
	if replaySubmitted.ReplicaID != "replica-a" {
		t.Fatalf("submitted replay = %+v", replaySubmitted)
	}
	byteTask, ok, err := seq.ClaimByteSideScan(context.Background())
	if err != nil || !ok || byteTask.ScanID != "scan-1" {
		t.Fatalf("ClaimByteSideScan = task %+v ok %v err %v", byteTask, ok, err)
	}
	if err := seq.SubmitByteSideScan(context.Background(), ByteSideScanResult{ScanID: "scan-1", WorkerID: "worker-a"}); err != nil {
		t.Fatalf("SubmitByteSideScan: %v", err)
	}
	if byteSubmitted.WorkerID != "worker-a" {
		t.Fatalf("submitted byte scan = %+v", byteSubmitted)
	}
	promotion, ok, err := seq.ClaimPromotion(context.Background())
	if err != nil || !ok || promotion.PromotionID != "promotion-1" {
		t.Fatalf("ClaimPromotion = task %+v ok %v err %v", promotion, ok, err)
	}
	receipt, err := seq.SubmitPromotionResult(context.Background(), PromotionResult{
		PromotionID: "promotion-1",
		WorkerID:    "worker-a",
		ActiveParts: []replay.PartManifestEntry{{TableID: "tenant.events", PartitionID: "all", PartName: "p1"}},
	})
	if err != nil {
		t.Fatalf("SubmitPromotionResult: %v", err)
	}
	if promotionSubmitted.WorkerID != "worker-a" || receipt.Watermark.SnapshotID != "snap-2" {
		t.Fatalf("promotion submitted=%+v receipt=%+v", promotionSubmitted, receipt)
	}
	watermark, err := seq.GetSafeWatermark(context.Background())
	if err != nil {
		t.Fatalf("GetSafeWatermark: %v", err)
	}
	if watermark.SnapshotID != "snap-2" || watermark.SafeL3BlockSeq != 10 {
		t.Fatalf("watermark = %+v", watermark)
	}
	manifest, ok, err := seq.GetSafeSnapshot(context.Background(), "snap-2")
	if err != nil || !ok || manifest.SnapshotID == "" {
		t.Fatalf("GetSafeSnapshot = manifest %+v ok %v err %v", manifest, ok, err)
	}
	decision, err := seq.CheckSafeRead(context.Background(), SafeReadRequest{NodeID: "node-a", TableIDs: []string{"tenant.events"}})
	if err != nil {
		t.Fatalf("CheckSafeRead: %v", err)
	}
	if decision.Active || decision.Reason != "node quarantined" || readSetReq.NodeID != "node-a" {
		t.Fatalf("decision=%+v request=%+v", decision, readSetReq)
	}
	mutationTask, ok, err := seq.ClaimMutationTask(context.Background())
	if err != nil || !ok || mutationTask.StatementID != "stmt-mut" {
		t.Fatalf("ClaimMutationTask = task %+v ok %v err %v", mutationTask, ok, err)
	}
	if err := seq.SubmitMutationClaim(context.Background(), MutationClaim{StatementID: "stmt-mut", WorkerID: "worker-a", ScratchTable: "scratch", PostStateRoot: "root"}); err != nil {
		t.Fatalf("SubmitMutationClaim: %v", err)
	}
	if mutationClaimSubmitted.WorkerID != "worker-a" {
		t.Fatalf("submitted mutation claim = %+v", mutationClaimSubmitted)
	}
	mutationReplayTask, ok, err := seq.ClaimMutationReplayTask(context.Background())
	if err != nil || !ok || mutationReplayTask.StatementID != "stmt-mut" {
		t.Fatalf("ClaimMutationReplayTask = task %+v ok %v err %v", mutationReplayTask, ok, err)
	}
	if err := seq.SubmitMutationReplay(context.Background(), MutationReplayResult{StatementID: "stmt-mut", WorkerID: "worker-a", PostStateRoot: "root"}); err != nil {
		t.Fatalf("SubmitMutationReplay: %v", err)
	}
	if mutationReplaySubmitted.WorkerID != "worker-a" {
		t.Fatalf("submitted mutation replay = %+v", mutationReplaySubmitted)
	}
	auditTask, ok, err := seq.ClaimSafeAudit(context.Background())
	if err != nil || !ok || auditTask.AuditID != "audit-1" {
		t.Fatalf("ClaimSafeAudit = task %+v ok %v err %v", auditTask, ok, err)
	}
	if err := seq.SubmitSafeAudit(context.Background(), SafeAuditVote{AuditID: "audit-1", WorkerID: "worker-a", Match: true}); err != nil {
		t.Fatalf("SubmitSafeAudit: %v", err)
	}
	if safeAuditSubmitted.WorkerID != "worker-a" {
		t.Fatalf("submitted safe audit = %+v", safeAuditSubmitted)
	}
	rollbackTask, ok, err := seq.ClaimRollback(context.Background())
	if err != nil || !ok || rollbackTask.RollbackID != "rollback-1" {
		t.Fatalf("ClaimRollback = task %+v ok %v err %v", rollbackTask, ok, err)
	}
	if err := seq.SubmitRollback(context.Background(), RollbackResult{RollbackID: "rollback-1", WorkerID: "worker-a", DroppedScratch: true}); err != nil {
		t.Fatalf("SubmitRollback: %v", err)
	}
	if rollbackSubmitted.WorkerID != "worker-a" || !rollbackSubmitted.DroppedScratch {
		t.Fatalf("submitted rollback = %+v", rollbackSubmitted)
	}
	repairTask, ok, err := seq.ClaimRepairSync(context.Background())
	if err != nil || !ok || repairTask.RepairID != "repair-1" {
		t.Fatalf("ClaimRepairSync = task %+v ok %v err %v", repairTask, ok, err)
	}
	if err := seq.SubmitRepairSync(context.Background(), RepairSyncResult{RepairID: "repair-1", WorkerID: "worker-a", InSync: true}); err != nil {
		t.Fatalf("SubmitRepairSync: %v", err)
	}
	if repairSyncSubmitted.WorkerID != "worker-a" || !repairSyncSubmitted.InSync {
		t.Fatalf("submitted repair sync = %+v", repairSyncSubmitted)
	}
	compactionTask, ok, err := seq.ClaimCompaction(context.Background())
	if err != nil || !ok || compactionTask.CompactionID != "compact-1" {
		t.Fatalf("ClaimCompaction = task %+v ok %v err %v", compactionTask, ok, err)
	}
	if err := seq.SubmitCompaction(context.Background(), CompactionResult{CompactionID: "compact-1", WorkerID: "worker-a", CompactTable: "`hg_compact`.`events_compact_1`"}); err != nil {
		t.Fatalf("SubmitCompaction: %v", err)
	}
	if compactionSubmitted.WorkerID != "worker-a" || compactionSubmitted.CompactTable == "" {
		t.Fatalf("submitted compaction = %+v", compactionSubmitted)
	}
}

func sealedClientTestManifest(t *testing.T) replay.SafeSnapshotManifest {
	t.Helper()
	manifest, err := (replay.SafeSnapshotManifest{
		SafeL3BlockSeq:      10,
		SchemaSnapshotID:  "schema",
		SchemaRoot:        "schema-root",
		ExecutorProfileID: "exec",
		Tables: []replay.TableManifest{{
			TableID:    "tenant.events",
			SchemaHash: "schema-hash",
			PartitionRoots: []replay.PartitionCommitment{{
				TableID:     "tenant.events",
				PartitionID: "all",
				Root:        "part-root",
			}},
			ActiveParts: []replay.PartManifestEntry{{
				TableID:       "tenant.events",
				PartitionID:   "all",
				PartName:      "p1",
				PartRowLtHash: "part-root",
				RowCount:      1,
			}},
		}},
	}).Seal()
	if err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	return manifest
}
