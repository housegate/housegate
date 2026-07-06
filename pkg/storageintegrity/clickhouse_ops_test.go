package storageintegrity

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"housegate/housegate/pkg/replay"
)

func TestClickHousePromoterUsesReplacePartitionForMutationPromotion(t *testing.T) {
	conn := &fakeSQLConn{}
	promoter := ClickHousePromoter{Conn: conn}

	result, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:      "promotion-mut",
		SafeTable:        "`hg_safe`.`events`",
		SourceTable:      "`hg_mutation`.`events_stmt_worker`",
		ReplacePartition: true,
		PartitionIDs:     []string{"202607"},
	})
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	want := "ALTER TABLE `hg_safe`.`events` REPLACE PARTITION ID '202607' FROM `hg_mutation`.`events_stmt_worker`"
	if len(conn.execs) != 1 || conn.execs[0] != want {
		t.Fatalf("execs = %v, want %q", conn.execs, want)
	}
	if result.PromotionID != "promotion-mut" || result.SafeTable != "`hg_safe`.`events`" {
		t.Fatalf("result = %+v", result)
	}
}

func TestClickHousePromoterUsesPromoteShadowCASSeqAndUnsafeCleanup(t *testing.T) {
	conn := &fakeSQLConn{}
	seqStore := newFakePromotionSeqStore()
	rootReader := &fakePartitionRootReader{roots: map[string]string{"`hg_safe`.`events`\x00202607": "base-root"}}
	active := &fakeActivePartReader{parts: []replay.PartManifestEntry{{
		TableID:       "tenant.events",
		PartitionID:   "202607",
		PartName:      "safe_p1",
		PartRowLtHash: "post-root",
		RowCount:      2,
	}}}
	promoter := ClickHousePromoter{
		Conn:             conn,
		ActiveParts:      active,
		PartitionRoots:   rootReader,
		PromotionSeqs:    seqStore,
		PromoteDatabase:  "hg_promote",
		CleanupUnsafe:    true,
		DropPromoteTable: true,
	}

	result, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:         "promotion-stmt-1",
		PromotionSeq:        42,
		BaseSafeSnapshotID:  "snap-1",
		BasePartitionRoot:   "base-root",
		TableID:             "tenant.events",
		UnsafeTable:         "`hg_unsafe`.`events`",
		SafeTable:           "`hg_safe`.`events`",
		PromoteDatabase:     "hg_promote",
		PartitionIDs:        []string{"202607"},
		StatementIDs:        []string{"stmt-1"},
		CandidateParts:      []ByteSidePart{{PartitionID: "202607", PartName: "p_1_1_0"}, {PartitionID: "202607", PartName: "p_1_2_0"}},
		CleanupUnsafeParts:  []ByteSidePart{{PartitionID: "202607", PartName: "p_1_1_0"}, {PartitionID: "202607", PartName: "p_1_2_0"}},
		ExpectedPostRoot:    "post-root",
		RequirePostRootCAS:  true,
		RequireBaseRootCAS:  true,
		RequirePromotionSeq: true,
	})
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}

	shadow := "`hg_promote`.`events_promotion_stmt_1_42`"
	want := []string{
		"CREATE DATABASE IF NOT EXISTS `hg_promote`",
		"DROP TABLE IF EXISTS " + shadow,
		"CREATE TABLE " + shadow + " AS `hg_safe`.`events`",
		"ALTER TABLE " + shadow + " ATTACH PARTITION ID '202607' FROM `hg_safe`.`events`",
		"INSERT INTO " + shadow + " SELECT * FROM `hg_unsafe`.`events` WHERE _partition_id = '202607' AND _part IN ('p_1_1_0','p_1_2_0')",
		"ALTER TABLE `hg_safe`.`events` REPLACE PARTITION ID '202607' FROM " + shadow,
		"ALTER TABLE `hg_unsafe`.`events` DROP PART 'p_1_1_0'",
		"ALTER TABLE `hg_unsafe`.`events` DROP PART 'p_1_2_0'",
		"DROP TABLE IF EXISTS " + shadow,
	}
	if strings.Join(conn.execs, "\n") != strings.Join(want, "\n") {
		t.Fatalf("execs:\n%s\nwant:\n%s", strings.Join(conn.execs, "\n"), strings.Join(want, "\n"))
	}
	if got := seqStore.last["`hg_safe`.`events`\x00202607"]; got != 42 {
		t.Fatalf("promotion seq watermark = %d, want 42", got)
	}
	if result.PromotionSeq != 42 || result.BaseSafeSnapshotID != "snap-1" ||
		result.BasePartitionRoot != "base-root" || len(result.CleanupUnsafeParts) != 2 ||
		len(result.ActiveParts) != 1 || result.ActiveParts[0].PartName != "safe_p1" {
		t.Fatalf("result = %+v", result)
	}
}

func TestClickHousePromoterTreatsMutationShadowSourceAsFullPostState(t *testing.T) {
	conn := &fakeSQLConn{}
	promoter := ClickHousePromoter{
		Conn:            conn,
		PromoteDatabase: "hg_promote",
	}

	_, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:      "promotion-mut",
		Kind:             "mutation",
		SafeTable:        "`hg_safe`.`events`",
		SourceTable:      "`hg_mutation`.`events_stmt_worker`",
		ReplacePartition: true,
		PartitionIDs:     []string{"202607"},
		CandidateParts:   []ByteSidePart{{PartitionID: "202607", PartName: "hash-scan-202607", RowCount: 1, PartRowLtHash: "post-root"}},
	})
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}

	joined := strings.Join(conn.execs, "\n")
	if strings.Contains(joined, "ATTACH PARTITION ID '202607' FROM `hg_safe`.`events`") {
		t.Fatalf("mutation full post-state promotion attached base partition:\n%s", joined)
	}
	if strings.Contains(joined, "_part IN ('hash-scan-202607')") {
		t.Fatalf("mutation shadow promotion used logical hash-scan part as physical part:\n%s", joined)
	}
	if !strings.Contains(joined, "ALTER TABLE `hg_promote`.`events_promotion_mut` ATTACH PARTITION ID '202607' FROM `hg_mutation`.`events_stmt_worker`") {
		t.Fatalf("mutation shadow promotion did not attach candidate partition from source:\n%s", joined)
	}
	if !strings.Contains(joined, "ALTER TABLE `hg_safe`.`events` REPLACE PARTITION ID '202607' FROM `hg_promote`.`events_promotion_mut`") {
		t.Fatalf("mutation shadow promotion did not replace safe partition:\n%s", joined)
	}
}

func TestClickHousePromoterDropsEmptyMutationPartition(t *testing.T) {
	conn := &fakeSQLConn{}
	seqStore := newFakePromotionSeqStore()
	promoter := ClickHousePromoter{Conn: conn, PromotionSeqs: seqStore}

	result, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:             "promotion-empty-delete",
		PromotionSeq:            88,
		Kind:                    "mutation",
		TableID:                 "tenant.events",
		SafeTable:               "`hg_safe`.`events`",
		InternalDropPartition:   true,
		DropPartitionIDs:        []string{"202607"},
		RequirePromotionSeq:     true,
		SkipBasePartitionAttach: true,
	})
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	want := "ALTER TABLE `hg_safe`.`events` DROP PARTITION ID '202607'"
	if len(conn.execs) != 1 || conn.execs[0] != want {
		t.Fatalf("execs = %v, want %q", conn.execs, want)
	}
	if got := seqStore.last["`hg_safe`.`events`\x00202607"]; got != 88 {
		t.Fatalf("promotion seq watermark = %d, want 88", got)
	}
	if result.PromotionID != "promotion-empty-delete" ||
		result.PromotionSeq != 88 ||
		len(result.PartitionIDs) != 1 ||
		result.PartitionIDs[0] != "202607" {
		t.Fatalf("result = %+v", result)
	}
}

func TestClickHousePromoterRejectsStalePromotionSeq(t *testing.T) {
	conn := &fakeSQLConn{}
	seqStore := newFakePromotionSeqStore()
	seqStore.last["`hg_safe`.`events`\x00202607"] = 42
	promoter := ClickHousePromoter{Conn: conn, PromotionSeqs: seqStore}

	_, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:         "promotion-stale",
		PromotionSeq:        42,
		SafeTable:           "`hg_safe`.`events`",
		UnsafeTable:         "`hg_unsafe`.`events`",
		PartitionIDs:        []string{"202607"},
		RequirePromotionSeq: true,
	})
	if err == nil || !strings.Contains(err.Error(), "stale promotion_seq") {
		t.Fatalf("err = %v, want stale promotion_seq", err)
	}
	if len(conn.execs) != 0 {
		t.Fatalf("execs = %v, want none before stale seq rejection", conn.execs)
	}
}

func TestClickHousePromoterRejectsBaseRootMismatch(t *testing.T) {
	conn := &fakeSQLConn{}
	promoter := ClickHousePromoter{
		Conn:           conn,
		PartitionRoots: &fakePartitionRootReader{roots: map[string]string{"`hg_safe`.`events`\x00202607": "new-root"}},
	}

	_, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:        "promotion-base-mismatch",
		BasePartitionRoot:  "old-root",
		SafeTable:          "`hg_safe`.`events`",
		UnsafeTable:        "`hg_unsafe`.`events`",
		PartitionIDs:       []string{"202607"},
		RequireBaseRootCAS: true,
	})
	if err == nil || !strings.Contains(err.Error(), "base partition root mismatch") {
		t.Fatalf("err = %v, want base partition root mismatch", err)
	}
	if len(conn.execs) != 0 {
		t.Fatalf("execs = %v, want none before base CAS rejection", conn.execs)
	}
}

func TestClickHousePromoterRejectsPostRootMismatchBeforeReplace(t *testing.T) {
	conn := &fakeSQLConn{}
	shadow := "`hg_promote`.`events_promotion_stmt_1_7`"
	promoter := ClickHousePromoter{
		Conn: conn,
		ActiveParts: &fakeActivePartReader{partsByTable: map[string][]replay.PartManifestEntry{
			shadow: {{
				TableID:       "tenant.events",
				PartitionID:   "202607",
				PartName:      "shadow_p1",
				PartRowLtHash: "unexpected-post-root",
				RowCount:      1,
			}},
		}},
		PromoteDatabase: "hg_promote",
	}

	_, err := promoter.Promote(context.Background(), PromotionTask{
		PromotionID:             "promotion-stmt-1",
		PromotionSeq:            7,
		UnsafeTable:             "`hg_unsafe`.`events`",
		SafeTable:               "`hg_safe`.`events`",
		PartitionIDs:            []string{"202607"},
		CandidateParts:          []ByteSidePart{{PartitionID: "202607", PartName: "p_1_1_0"}},
		ExpectedPostRoot:        "expected-post-root",
		RequirePostRootCAS:      true,
		SkipBasePartitionAttach: true,
	})
	if err == nil || !strings.Contains(err.Error(), "post partition root mismatch") {
		t.Fatalf("err = %v, want post partition root mismatch", err)
	}
	joined := strings.Join(conn.execs, "\n")
	if strings.Contains(joined, "REPLACE PARTITION") || strings.Contains(joined, "DROP PART ") {
		t.Fatalf("post-root mismatch modified safe or unsafe tables:\n%s", joined)
	}
	wantBeforeFailure := []string{
		"CREATE DATABASE IF NOT EXISTS `hg_promote`",
		"DROP TABLE IF EXISTS " + shadow,
		"CREATE TABLE " + shadow + " AS `hg_safe`.`events`",
		"INSERT INTO " + shadow + " SELECT * FROM `hg_unsafe`.`events` WHERE _partition_id = '202607' AND _part IN ('p_1_1_0')",
	}
	if joined != strings.Join(wantBeforeFailure, "\n") {
		t.Fatalf("execs before mismatch:\n%s\nwant:\n%s", joined, strings.Join(wantBeforeFailure, "\n"))
	}
}

func TestClickHouseMutationExecutorCreatesScratchRunsMutationAndHashesPostState(t *testing.T) {
	conn := &fakeSQLConn{}
	hasher := &fakeTableHasher{root: "post-root", parts: []ByteSidePart{{
		PartitionID:   "202607",
		PartName:      "p1",
		RowCount:      2,
		PartRowLtHash: "0xpart",
	}}}
	active := &fakeActivePartReader{parts: []replay.PartManifestEntry{{
		PartitionID:   "202607",
		PartName:      "real_part_1",
		PartRowLtHash: "0xactive",
		RowCount:      2,
	}}}
	executor := ClickHouseMutationExecutor{
		Conn:            conn,
		Hasher:          hasher,
		ActiveParts:     active,
		ClaimSigner:     &fakeMutationClaimSigner{},
		WorkerID:        "worker-a",
		ScratchDatabase: "hg_mutation",
	}

	claim, err := executor.ExecuteMutation(context.Background(), MutationTask{
		StatementID:  "stmt-mut",
		MutationType: MutationTypeUpdate,
		MutationSQL:  "ALTER TABLE `hg_safe`.`events` UPDATE label = 'b' WHERE day = '2026-07-03'",
		SafeTable:    "`hg_safe`.`events`",
		PartitionIDs: []string{"202607"},
	})
	if err != nil {
		t.Fatalf("ExecuteMutation: %v", err)
	}
	if claim.WorkerID != "worker-a" || claim.PostStateRoot != "post-root" || claim.ScratchTable == "" {
		t.Fatalf("claim = %+v", claim)
	}
	if hasher.table != claim.ScratchTable {
		t.Fatalf("hasher table = %q, want scratch table %q", hasher.table, claim.ScratchTable)
	}
	if len(claim.Parts) != 1 || claim.Parts[0].PartName != "real_part_1" || claim.Parts[0].PartRowLtHash != "0xactive" {
		t.Fatalf("claim parts = %+v, want physical active part evidence", claim.Parts)
	}
	joined := strings.Join(conn.execs, "\n")
	for _, want := range []string{
		"CREATE DATABASE IF NOT EXISTS `hg_mutation`",
		"DROP TABLE IF EXISTS " + claim.ScratchTable,
		"CREATE TABLE " + claim.ScratchTable + " AS `hg_safe`.`events`",
		"INSERT INTO " + claim.ScratchTable + " SELECT * FROM `hg_safe`.`events`",
		"ALTER TABLE " + claim.ScratchTable + " UPDATE label = 'b' WHERE day = '2026-07-03' SETTINGS mutations_sync = 2",
		"OPTIMIZE TABLE " + claim.ScratchTable + " FINAL",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("execs missing %q in:\n%s", want, joined)
		}
	}
}

func TestClickHouseMutationExecutorUsesInternalDropPartitionForEmptyDelete(t *testing.T) {
	conn := &fakeSQLConn{}
	hasher := &fakeTableHasher{root: "post-root"}
	executor := ClickHouseMutationExecutor{
		Conn:            conn,
		Hasher:          hasher,
		ClaimSigner:     &fakeMutationClaimSigner{},
		WorkerID:        "worker-a",
		ScratchDatabase: "hg_mutation",
	}

	claim, err := executor.ExecuteMutation(context.Background(), MutationTask{
		StatementID:           "stmt-empty-delete",
		MutationType:          MutationTypeDelete,
		MutationSQL:           "ALTER TABLE `hg_safe`.`events` DELETE WHERE day = '2026-07-03'",
		SafeTable:             "`hg_safe`.`events`",
		PartitionIDs:          []string{"202607"},
		InternalDropPartition: true,
		DropPartitionIDs:      []string{"202607"},
		BaseSafeSnapshotID:    "snap-1",
		BasePartitionRoot:     "base-root",
		PromotionSeq:          77,
	})
	if err != nil {
		t.Fatalf("ExecuteMutation: %v", err)
	}
	if claim.BaseSafeSnapshotID != "snap-1" || claim.BasePartitionRoot != "base-root" || claim.PromotionSeq != 77 {
		t.Fatalf("claim metadata = %+v", claim)
	}
	joined := strings.Join(conn.execs, "\n")
	want := "ALTER TABLE " + claim.ScratchTable + " DROP PARTITION ID '202607'"
	if !strings.Contains(joined, want) {
		t.Fatalf("execs missing internal drop partition %q in:\n%s", want, joined)
	}
	if strings.Contains(joined, " DELETE WHERE ") {
		t.Fatalf("execs used user DELETE instead of internal DROP PARTITION:\n%s", joined)
	}
}

func TestClickHouseMutationExecutorIncludesClaimEvidence(t *testing.T) {
	conn := &fakeSQLConn{}
	hasher := &fakeTableHasher{hashes: []TableHash{
		{
			StateRoot: "base-root",
			Parts: []ByteSidePart{{
				PartitionID:   "202607",
				PartName:      "safe_p1",
				RowCount:      5,
				PartRowLtHash: "base-root",
			}},
		},
		{
			StateRoot: "post-root",
			Parts: []ByteSidePart{{
				PartitionID:   "202607",
				PartName:      "scratch_p1",
				RowCount:      3,
				PartRowLtHash: "post-root",
			}},
		},
	}}
	executor := ClickHouseMutationExecutor{
		Conn:            conn,
		Hasher:          hasher,
		ClaimSigner:     &fakeMutationClaimSigner{},
		WorkerID:        "worker-a",
		ScratchDatabase: "hg_mutation",
	}

	claim, err := executor.ExecuteMutation(context.Background(), MutationTask{
		StatementID:        "stmt-mut",
		TableID:            "tenant.events",
		MutationType:       MutationTypeDelete,
		MutationSQL:        "ALTER TABLE `hg_safe`.`events` DELETE WHERE day = '2026-07-03'",
		SafeTable:          "`hg_safe`.`events`",
		PartitionIDs:       []string{"202607"},
		BaseSafeSnapshotID: "snap-1",
		BasePartitionRoot:  "base-root",
		SchemaSnapshotID:   "schema-1",
		PromotionSeq:       12,
	})
	if err != nil {
		t.Fatalf("ExecuteMutation: %v", err)
	}
	if claim.ClaimHash == "" {
		t.Fatalf("claim hash missing: %+v", claim)
	}
	if claim.Signature != "sig:"+claim.ClaimHash {
		t.Fatalf("claim signature = %q, want signature over claim hash %q", claim.Signature, claim.ClaimHash)
	}
	if claim.SchemaSnapshotID != "schema-1" || claim.RowsBefore != 5 || claim.RowsAfter != 3 || claim.RowsDeleted != 2 {
		t.Fatalf("claim aggregate evidence = %+v", claim)
	}
	if len(claim.BasePartitionRoots) != 1 || claim.BasePartitionRoots[0].Root != "base-root" {
		t.Fatalf("base partition roots = %+v", claim.BasePartitionRoots)
	}
	if len(claim.PostPartitionCommitments) != 1 || claim.PostPartitionCommitments[0].Root != "post-root" {
		t.Fatalf("post commitments = %+v", claim.PostPartitionCommitments)
	}
	if len(claim.PartitionDeltas) != 1 ||
		claim.PartitionDeltas[0].BaseRoot != "base-root" ||
		claim.PartitionDeltas[0].PostRoot != "post-root" ||
		claim.PartitionDeltas[0].RowsBefore != 5 ||
		claim.PartitionDeltas[0].RowsAfter != 3 ||
		claim.PartitionDeltas[0].RowsDeleted != 2 ||
		claim.PartitionDeltas[0].DeltaRoot == "" {
		t.Fatalf("partition deltas = %+v", claim.PartitionDeltas)
	}
	if len(hasher.tables) != 2 || hasher.tables[0] != "`hg_safe`.`events`" || hasher.tables[1] != claim.ScratchTable {
		t.Fatalf("hasher tables = %+v, scratch=%s", hasher.tables, claim.ScratchTable)
	}
}

func TestClickHouseRollbackExecutorCleansUnsafeScratchAndPromoteState(t *testing.T) {
	conn := &fakeSQLConn{}
	executor := ClickHouseRollbackExecutor{Conn: conn}

	result, err := executor.Rollback(context.Background(), RollbackTask{
		RollbackID:   "rollback-stmt-1",
		StatementID:  "stmt-1",
		TableID:      "tenant.events",
		UnsafeTable:  "`hg_unsafe`.`events`",
		ScratchTable: "`hg_mutation`.`events_stmt_1_worker_a`",
		PromoteTable: "`hg_promote`.`events_stmt_1`",
		PartitionIDs: []string{"202607"},
		UnsafeParts: []ByteSidePart{
			{PartitionID: "202607", PartName: "p_1_1_0"},
			{PartitionID: "202607", PartName: "p_1_2_0"},
		},
	})
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	want := []string{
		"ALTER TABLE `hg_unsafe`.`events` DROP PART 'p_1_1_0'",
		"ALTER TABLE `hg_unsafe`.`events` DROP PART 'p_1_2_0'",
		"DROP TABLE IF EXISTS `hg_mutation`.`events_stmt_1_worker_a`",
		"DROP TABLE IF EXISTS `hg_promote`.`events_stmt_1`",
	}
	if strings.Join(conn.execs, "\n") != strings.Join(want, "\n") {
		t.Fatalf("execs:\n%s\nwant:\n%s", strings.Join(conn.execs, "\n"), strings.Join(want, "\n"))
	}
	if result.RollbackID != "rollback-stmt-1" ||
		len(result.CleanedUnsafeParts) != 2 ||
		!result.DroppedScratch ||
		!result.DroppedPromote {
		t.Fatalf("result = %+v", result)
	}
}

func TestClickHouseRepairSyncExecutorReplacesPartitionsAndVerifiesManifest(t *testing.T) {
	manifest := sealedOpsTestManifest(t, []replay.PartManifestEntry{{
		TableID:       "tenant.events",
		PartitionID:   "202607",
		PartName:      "safe_p1",
		PartRowLtHash: "safe-root",
		RowCount:      3,
	}})
	conn := &fakeSQLConn{}
	hasher := &fakeTableHasher{root: manifest.StateRoot}
	active := &fakeActivePartReader{parts: []replay.PartManifestEntry{{
		TableID:       "tenant.events",
		PartitionID:   "202607",
		PartName:      "safe_p1",
		PartRowLtHash: "safe-root",
		RowCount:      3,
	}}}
	executor := ClickHouseRepairSyncExecutor{Conn: conn, Hasher: hasher, ActiveParts: active}

	result, err := executor.RepairSync(context.Background(), RepairSyncTask{
		RepairID:             "repair-node-b",
		SnapshotID:           manifest.SnapshotID,
		TableID:              "tenant.events",
		SafeTable:            "`hg_safe`.`events`",
		SourceTable:          "`hg_repair`.`events_latest`",
		Manifest:             manifest,
		PartitionIDs:         []string{"202607"},
		ExpectedStateRoot:    manifest.StateRoot,
		RequireManifestMatch: true,
	})
	if err != nil {
		t.Fatalf("RepairSync: %v", err)
	}
	want := "ALTER TABLE `hg_safe`.`events` REPLACE PARTITION ID '202607' FROM `hg_repair`.`events_latest`"
	if len(conn.execs) != 1 || conn.execs[0] != want {
		t.Fatalf("execs = %v, want %q", conn.execs, want)
	}
	if !result.Repaired || !result.InSync || !result.ActivePartsMatch ||
		result.ManifestRoot != manifest.ManifestRoot ||
		result.StateRoot != manifest.StateRoot ||
		len(result.ActiveParts) != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestClickHouseCompactorUsesShadowTableCASAndReplacePartition(t *testing.T) {
	conn := &fakeSQLConn{}
	compactTable := "`hg_compact`.`events_compact_202607_101`"
	active := &fakeActivePartReader{partsByTable: map[string][]replay.PartManifestEntry{
		compactTable: {{
			TableID:       "tenant.events",
			PartitionID:   "202607",
			PartName:      "compact_p1",
			PartRowLtHash: "base-root",
			RowCount:      5,
		}},
		"`hg_safe`.`events`": {{
			TableID:       "tenant.events",
			PartitionID:   "202607",
			PartName:      "safe_compact_p1",
			PartRowLtHash: "base-root",
			RowCount:      5,
		}},
	}}
	compactor := ClickHouseCompactor{
		Conn:             conn,
		ActiveParts:      active,
		PartitionRoots:   &fakePartitionRootReader{roots: map[string]string{"`hg_safe`.`events`\x00202607": "base-root"}},
		CompactDatabase:  "hg_compact",
		DropCompactTable: true,
	}

	result, err := compactor.Compact(context.Background(), CompactionTask{
		CompactionID:       "compact-202607",
		PromotionSeq:       101,
		TableID:            "tenant.events",
		SafeTable:          "`hg_safe`.`events`",
		BaseSafeSnapshotID: "snap-1",
		BasePartitionRoot:  "base-root",
		ExpectedPostRoot:   "base-root",
		PartitionIDs:       []string{"202607"},
		RequireBaseRootCAS: true,
		RequirePostRootCAS: true,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	want := []string{
		"CREATE DATABASE IF NOT EXISTS `hg_compact`",
		"DROP TABLE IF EXISTS " + compactTable,
		"CREATE TABLE " + compactTable + " AS `hg_safe`.`events`",
		"ALTER TABLE " + compactTable + " ATTACH PARTITION ID '202607' FROM `hg_safe`.`events`",
		"OPTIMIZE TABLE " + compactTable + " FINAL",
		"ALTER TABLE `hg_safe`.`events` REPLACE PARTITION ID '202607' FROM " + compactTable,
		"DROP TABLE IF EXISTS " + compactTable,
	}
	if strings.Join(conn.execs, "\n") != strings.Join(want, "\n") {
		t.Fatalf("execs:\n%s\nwant:\n%s", strings.Join(conn.execs, "\n"), strings.Join(want, "\n"))
	}
	if result.CompactionID != "compact-202607" ||
		result.CompactTable != compactTable ||
		result.BasePartitionRoot != "base-root" ||
		len(result.ActiveParts) != 1 ||
		result.ActiveParts[0].PartName != "safe_compact_p1" {
		t.Fatalf("result = %+v", result)
	}
}

func TestRewriteAlterTableTargetOnlyRewritesTargetTable(t *testing.T) {
	rewritten, err := rewriteAlterTableTarget(
		"ALTER TABLE `hg_safe`.`events` UPDATE note = '`hg_safe`.`events`' WHERE day = '2026-07-03'",
		"`hg_safe`.`events`",
		"`hg_mutation`.`events_stmt_worker`",
	)
	if err != nil {
		t.Fatalf("rewriteAlterTableTarget: %v", err)
	}
	want := "ALTER TABLE `hg_mutation`.`events_stmt_worker` UPDATE note = '`hg_safe`.`events`' WHERE day = '2026-07-03'"
	if rewritten != want {
		t.Fatalf("rewritten = %q, want %q", rewritten, want)
	}
}

func TestClickHouseSafeAuditorHashesSafeTable(t *testing.T) {
	hasher := &fakeTableHasher{root: "safe-root"}
	auditor := ClickHouseSafeAuditor{Hasher: hasher, WorkerID: "auditor-a"}
	vote, err := auditor.AuditSafe(context.Background(), SafeAuditTask{
		AuditID:      "audit-1",
		SnapshotID:   "snap",
		SafeTable:    "`hg_safe`.`events`",
		StateRoot:    "safe-root",
		PartitionIDs: []string{"202607"},
	})
	if err != nil {
		t.Fatalf("AuditSafe: %v", err)
	}
	if !vote.Match || vote.WorkerID != "auditor-a" {
		t.Fatalf("vote = %+v", vote)
	}
	if hasher.table != "`hg_safe`.`events`" {
		t.Fatalf("hasher table = %q", hasher.table)
	}
}

func TestClickHouseSafeAuditorRejectsManifestActivePartMismatch(t *testing.T) {
	manifest := sealedOpsTestManifest(t, []replay.PartManifestEntry{{
		TableID:       "tenant.events",
		PartitionID:   "202607",
		PartName:      "p_good",
		PartRowLtHash: "expected-root",
		RowCount:      2,
	}})
	auditor := ClickHouseSafeAuditor{
		Hasher: &fakeTableHasher{root: manifest.StateRoot},
		ActiveParts: &fakeActivePartReader{parts: []replay.PartManifestEntry{{
			TableID:       "tenant.events",
			PartitionID:   "202607",
			PartName:      "p_evil",
			PartRowLtHash: "evil-root",
			RowCount:      2,
		}}},
		WorkerID: "auditor-a",
	}

	vote, err := auditor.AuditSafe(context.Background(), SafeAuditTask{
		AuditID:      "audit-1",
		SnapshotID:   manifest.SnapshotID,
		TableID:      "tenant.events",
		SafeTable:    "`hg_safe`.`events`",
		StateRoot:    manifest.StateRoot,
		Manifest:     manifest,
		PartitionIDs: []string{"202607"},
	})
	if err != nil {
		t.Fatalf("AuditSafe: %v", err)
	}
	if vote.Match || vote.ActivePartsMatch || !strings.Contains(vote.Error, "active parts do not match manifest") {
		t.Fatalf("vote = %+v, want manifest active-part mismatch", vote)
	}
	if len(vote.ActiveParts) != 1 || vote.ActiveParts[0].PartName != "p_evil" {
		t.Fatalf("vote active parts = %+v", vote.ActiveParts)
	}
}

func TestClickHousePromotionSeqStorePersistsAndRejectsStaleSeq(t *testing.T) {
	conn := &fakeSQLConn{}
	query := &fakeHashQueryConn{rows: &fakeHashRows{values: [][]any{{uint64(41)}}}}
	store := ClickHousePromotionSeqStore{Exec: conn, Query: query, MetadataDatabase: "hg_meta"}

	last, err := store.LastPromotionSeq(context.Background(), "hg_safe.events", "202607")
	if err != nil {
		t.Fatalf("LastPromotionSeq: %v", err)
	}
	if last != 41 {
		t.Fatalf("LastPromotionSeq = %d, want 41", last)
	}
	if !strings.Contains(query.query, "SELECT max(seq)") || !strings.Contains(query.query, "`hg_meta`.`hg_promotion_seq`") {
		t.Fatalf("query = %q", query.query)
	}

	query.rows = &fakeHashRows{values: [][]any{{uint64(41)}}}
	if err := store.RecordPromotionSeq(context.Background(), "hg_safe.events", "202607", 42); err != nil {
		t.Fatalf("RecordPromotionSeq: %v", err)
	}
	if !containsExec(conn.execs, "INSERT INTO `hg_meta`.`hg_promotion_seq`") {
		t.Fatalf("execs = %#v, want promotion_seq insert", conn.execs)
	}

	query.rows = &fakeHashRows{values: [][]any{{uint64(42)}}}
	if err := store.RecordPromotionSeq(context.Background(), "hg_safe.events", "202607", 42); err == nil {
		t.Fatalf("RecordPromotionSeq accepted stale seq")
	}
}

func TestClickHouseTableControllerStopsMergesAndSettings(t *testing.T) {
	conn := &fakeSQLConn{}
	controller := ClickHouseTableController{
		Conn:                   conn,
		StopMerges:             true,
		EnforceNoMergeSettings: true,
	}

	if err := controller.PrepareTable(context.Background(), "hg_safe.events"); err != nil {
		t.Fatalf("PrepareTable: %v", err)
	}
	if !containsExec(conn.execs, "SYSTEM STOP MERGES hg_safe.events") {
		t.Fatalf("execs = %#v, want SYSTEM STOP MERGES", conn.execs)
	}
	if !containsExec(conn.execs, "ALTER TABLE hg_safe.events MODIFY SETTING") {
		t.Fatalf("execs = %#v, want no-merge settings", conn.execs)
	}
}

func TestClickHouseTableControllerPreparesDatabaseTables(t *testing.T) {
	conn := &fakeSQLConn{}
	query := &fakeHashQueryConn{rows: &fakeHashRows{values: [][]any{{"events"}, {"accounts"}}}}
	controller := ClickHouseTableController{
		Conn:                   conn,
		Query:                  query,
		StopMerges:             true,
		EnforceNoMergeSettings: true,
	}

	if err := controller.PrepareDatabase(context.Background(), "hg_safe"); err != nil {
		t.Fatalf("PrepareDatabase: %v", err)
	}
	if !strings.Contains(query.query, "FROM system.tables") || !strings.Contains(query.query, "database = 'hg_safe'") {
		t.Fatalf("query = %q, want system.tables lookup for hg_safe", query.query)
	}
	for _, table := range []string{"`hg_safe`.`accounts`", "`hg_safe`.`events`"} {
		if !containsExec(conn.execs, "SYSTEM STOP MERGES "+table) {
			t.Fatalf("execs = %#v, want stop merges for %s", conn.execs, table)
		}
		if !containsExec(conn.execs, "ALTER TABLE "+table+" MODIFY SETTING") {
			t.Fatalf("execs = %#v, want no-merge settings for %s", conn.execs, table)
		}
	}
}

func TestClickHouseActivePartReaderReadsVisiblePartsAndHashesEachPart(t *testing.T) {
	rows := &fakeHashRows{
		columns: []string{"_partition_id", "_part", "_hg_row_id", "id", "label"},
		types:   []string{"String", "String", "FixedString(32)", "UInt64", "String"},
		values: [][]any{
			{"202607", "p1", []byte("row-a-000000000000000000000000000"), uint64(1), "alpha"},
			{"202607", "p2", []byte("row-b-000000000000000000000000000"), uint64(2), "beta"},
		},
	}
	conn := &fakeHashQueryConn{rows: rows}
	reader := ClickHouseActivePartReader{Conn: conn, TableID: "tenant.events"}

	parts, err := reader.ReadActiveParts(context.Background(), "`hg_safe`.`events`", []string{"202607"})
	if err != nil {
		t.Fatalf("ReadActiveParts: %v", err)
	}
	if len(parts) != 2 || parts[0].PartName != "p1" || parts[1].PartName != "p2" {
		t.Fatalf("parts = %+v", parts)
	}
	if parts[0].PartRowLtHash == "" || parts[1].PartRowLtHash == "" {
		t.Fatalf("part row LtHash missing: %+v", parts)
	}
	if !strings.Contains(conn.query, "_partition_id IN ('202607')") || !strings.Contains(conn.query, "_part") {
		t.Fatalf("query = %q, want active part scan with partition filter", conn.query)
	}
}

func TestClickHouseTableHasherHashesRowsByPartition(t *testing.T) {
	rows := &fakeHashRows{
		columns: []string{"_partition_id", "_hg_row_id", "id", "label"},
		types:   []string{"String", "FixedString(32)", "UInt64", "String"},
		values: [][]any{
			{"202607", []byte("row-a-000000000000000000000000000"), uint64(1), "alpha"},
			{"202607", []byte("row-b-000000000000000000000000000"), uint64(2), "beta"},
		},
	}
	conn := &fakeHashQueryConn{rows: rows}
	hasher := ClickHouseTableHasher{Conn: conn, TableID: "tenant.events"}

	hash, err := hasher.HashTable(context.Background(), "`hg_safe`.`events`", []string{"202607"})
	if err != nil {
		t.Fatalf("HashTable: %v", err)
	}
	if hash.StateRoot == "" {
		t.Fatal("state root missing")
	}
	if len(hash.Parts) != 1 || hash.Parts[0].PartitionID != "202607" || hash.Parts[0].RowCount != 2 || hash.Parts[0].PartRowLtHash == "" {
		t.Fatalf("parts = %+v", hash.Parts)
	}
	if !strings.Contains(conn.query, "WHERE _partition_id IN ('202607')") {
		t.Fatalf("query = %q, want partition filter", conn.query)
	}
}

type fakeSQLConn struct {
	execs []string
}

func (f *fakeSQLConn) Exec(_ context.Context, query string, _ ...any) error {
	f.execs = append(f.execs, query)
	return nil
}

func containsExec(execs []string, needle string) bool {
	for _, exec := range execs {
		if strings.Contains(exec, needle) {
			return true
		}
	}
	return false
}

type fakeTableHasher struct {
	table      string
	tables     []string
	partitions []string
	root       string
	parts      []ByteSidePart
	hashes     []TableHash
	calls      int
}

type fakeMutationClaimSigner struct{}

func (fakeMutationClaimSigner) SignClaim(claimHash string) (string, error) {
	return "sig:" + claimHash, nil
}

func (f *fakeTableHasher) HashTable(_ context.Context, table string, partitions []string) (TableHash, error) {
	f.table = table
	f.tables = append(f.tables, table)
	f.partitions = append([]string(nil), partitions...)
	if len(f.hashes) > 0 {
		idx := f.calls
		f.calls++
		if idx >= len(f.hashes) {
			idx = len(f.hashes) - 1
		}
		return f.hashes[idx], nil
	}
	f.calls++
	return TableHash{StateRoot: f.root, Parts: f.parts}, nil
}

type fakeActivePartReader struct {
	table        string
	partitions   []string
	parts        []replay.PartManifestEntry
	partsByTable map[string][]replay.PartManifestEntry
}

func (f *fakeActivePartReader) ReadActiveParts(_ context.Context, table string, partitions []string) ([]replay.PartManifestEntry, error) {
	f.table = table
	f.partitions = append([]string(nil), partitions...)
	if f.partsByTable != nil {
		return append([]replay.PartManifestEntry(nil), f.partsByTable[table]...), nil
	}
	return append([]replay.PartManifestEntry(nil), f.parts...), nil
}

type fakePartitionRootReader struct {
	roots map[string]string
}

func (f *fakePartitionRootReader) CurrentPartitionRoot(_ context.Context, table, partitionID string) (string, error) {
	return f.roots[table+"\x00"+partitionID], nil
}

type fakePromotionSeqStore struct {
	last map[string]uint64
}

func newFakePromotionSeqStore() *fakePromotionSeqStore {
	return &fakePromotionSeqStore{last: map[string]uint64{}}
}

func (f *fakePromotionSeqStore) LastPromotionSeq(_ context.Context, table, partitionID string) (uint64, error) {
	return f.last[table+"\x00"+partitionID], nil
}

func (f *fakePromotionSeqStore) RecordPromotionSeq(_ context.Context, table, partitionID string, seq uint64) error {
	f.last[table+"\x00"+partitionID] = seq
	return nil
}

func sealedOpsTestManifest(t *testing.T, parts []replay.PartManifestEntry) replay.SafeSnapshotManifest {
	t.Helper()
	roots := make([]replay.PartitionCommitment, 0, len(parts))
	for _, part := range parts {
		roots = append(roots, replay.PartitionCommitment{
			TableID:     part.TableID,
			PartitionID: part.PartitionID,
			Root:        part.PartRowLtHash,
		})
	}
	manifest, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq:      11,
		SchemaSnapshotID:  "schema",
		SchemaRoot:        "schema-root",
		ExecutorProfileID: "exec",
		Tables: []replay.TableManifest{{
			TableID:        "tenant.events",
			SchemaHash:     "schema-hash",
			PartitionRoots: roots,
			ActiveParts:    parts,
		}},
	}).Seal()
	if err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	return manifest
}

type fakeHashQueryConn struct {
	query string
	rows  *fakeHashRows
}

func (f *fakeHashQueryConn) Query(_ context.Context, query string, _ ...any) (HashRows, error) {
	f.query = query
	return f.rows, nil
}

type fakeHashRows struct {
	columns []string
	types   []string
	values  [][]any
	pos     int
}

func (r *fakeHashRows) Next() bool {
	return r.pos < len(r.values)
}

func (r *fakeHashRows) Scan(dest ...any) error {
	row := r.values[r.pos]
	r.pos++
	for i := range dest {
		if anyDest, ok := dest[i].(*any); ok {
			*anyDest = row[i]
			continue
		}
		target := reflect.ValueOf(dest[i])
		if target.Kind() != reflect.Pointer || target.IsNil() {
			continue
		}
		value := reflect.ValueOf(row[i])
		elem := target.Elem()
		if value.IsValid() && value.Type().AssignableTo(elem.Type()) {
			elem.Set(value)
			continue
		}
		if value.IsValid() && value.Type().ConvertibleTo(elem.Type()) {
			elem.Set(value.Convert(elem.Type()))
		}
	}
	return nil
}

func (r *fakeHashRows) Columns() []string { return append([]string(nil), r.columns...) }

func (r *fakeHashRows) ColumnTypes() []HashColumnType {
	out := make([]HashColumnType, 0, len(r.types))
	for _, typ := range r.types {
		out = append(out, fakeHashColumnType(typ))
	}
	return out
}

func (r *fakeHashRows) Close() error { return nil }
func (r *fakeHashRows) Err() error   { return nil }

type fakeHashColumnType string

func (t fakeHashColumnType) DatabaseTypeName() string { return string(t) }
