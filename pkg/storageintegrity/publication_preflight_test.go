package storageintegrity

import (
	"context"
	"strings"
	"testing"

	"housegate/housegate/pkg/replay"
)

func TestPromotionPreflightRejectsMalformedInsertBeforeSQL(t *testing.T) {
	task := PromotionTask{
		PromotionID:         "promotion-bad-insert",
		PromotionSeq:        10,
		Kind:                "insert",
		TableID:             "tenant.events",
		SafeTable:           "`hg_safe`.`events`",
		PartitionIDs:        []string{"202607"},
		ExpectedPostRoots:   []replay.PartitionCommitment{{TableID: "tenant.events", PartitionID: "202607", Root: "0xaa"}},
		RequirePostRootCAS:  true,
		RequirePromotionSeq: true,
	}
	if err := ValidatePromotionPreflight(task, true); err == nil || !strings.Contains(err.Error(), "candidate parts") {
		t.Fatalf("preflight error = %v, want candidate parts rejection", err)
	}
}

func TestPromotionPreflightRejectsScalarRootForMultiPartition(t *testing.T) {
	task := PromotionTask{
		PromotionID:         "promotion-multi",
		PromotionSeq:        11,
		Kind:                "insert",
		TableID:             "tenant.events",
		SafeTable:           "`hg_safe`.`events`",
		PartitionIDs:        []string{"202607", "202608"},
		CandidateParts:      []ByteSidePart{{PartitionID: "202607", PartRowLtHash: "0xaa", RowCount: 1}},
		ExpectedPostRoot:    "0xaa",
		RequirePostRootCAS:  true,
		RequirePromotionSeq: true,
	}
	if err := ValidatePromotionPreflight(task, true); err == nil || !strings.Contains(err.Error(), "per-partition expected post roots") {
		t.Fatalf("preflight error = %v, want multi-partition expected roots rejection", err)
	}
}

func TestCompactionPreflightRejectsMissingInputParts(t *testing.T) {
	task := CompactionTask{
		CompactionID:       "compact-bad",
		PromotionSeq:       12,
		TableID:            "tenant.events",
		SafeTable:          "`hg_safe`.`events`",
		CompactDatabase:    "hg_compact",
		CompactTable:       "`hg_compact`.`events_202607`",
		BaseSafeSnapshotID: "snap-1",
		BasePartitionRoot:  "0xaa",
		ExpectedPostRoot:   "0xbb",
		PartitionIDs:       []string{"202607"},
		RequireBaseRootCAS: true,
		RequirePostRootCAS: true,
		DropCompactTable:   true,
		LeaderSignature:    "dummy-signature",
	}
	if err := ValidateCompactionPreflight(task, true); err == nil || !strings.Contains(err.Error(), "input parts") {
		t.Fatalf("preflight error = %v, want input parts rejection", err)
	}
}

func TestPromotionWorkerRunsPreflightBeforePromoter(t *testing.T) {
	seq := &fakeWorkerSequencer{
		promotionTask: PromotionTask{
			PromotionID:         "promotion-bad-insert",
			PromotionSeq:        13,
			Kind:                "insert",
			TableID:             "tenant.events",
			SafeTable:           "`hg_safe`.`events`",
			PartitionIDs:        []string{"202607"},
			ExpectedPostRoots:   []replay.PartitionCommitment{{TableID: "tenant.events", PartitionID: "202607", Root: "0xaa"}},
			RequirePostRootCAS:  true,
			RequirePromotionSeq: true,
		},
		promotionOK: true,
	}
	promoter := &fakePromoter{}
	worker := PromotionWorker{WorkerID: "promoter-a", Sequencer: seq, Promoter: promoter}

	didWork, err := worker.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "candidate parts") {
		t.Fatalf("RunOnce error = %v, want candidate parts preflight rejection", err)
	}
	if didWork {
		t.Fatal("RunOnce didWork=true after preflight rejection")
	}
	if promoter.task.PromotionID != "" {
		t.Fatalf("promoter executed before preflight rejection: %+v", promoter.task)
	}
}

func TestPromotionPreflightRejectsNamelessInsertPhysicalParts(t *testing.T) {
	task := PromotionTask{
		PromotionID:        "promotion-logical",
		PromotionSeq:       14,
		Kind:               "insert",
		TableID:            "tenant.events",
		SafeTable:          "`hg_safe`.`events`",
		PartitionIDs:       []string{"202607"},
		CandidateParts:     []ByteSidePart{{PartitionID: "all", PartRowLtHash: "0xaa", RowCount: 1}},
		ExpectedPostRoots:  []replay.PartitionCommitment{{TableID: "tenant.events", PartitionID: "202607", Root: "0xaa"}},
		RequirePostRootCAS: true,
	}
	if err := ValidatePromotionPreflight(task, false); err == nil || !strings.Contains(err.Error(), "physical candidate parts") {
		t.Fatalf("preflight error = %v, want physical candidate rejection", err)
	}

	task.CandidateParts = []ByteSidePart{{PartitionID: "202607", PartName: "p_1_1_0", PartRowLtHash: "0xaa", RowCount: 1}}
	task.CleanupUnsafeParts = []ByteSidePart{{PartitionID: "all", PartRowLtHash: "0xaa", RowCount: 1}}
	if err := ValidatePromotionPreflight(task, false); err == nil || !strings.Contains(err.Error(), "physical cleanup parts") {
		t.Fatalf("preflight error = %v, want physical cleanup rejection", err)
	}
}
