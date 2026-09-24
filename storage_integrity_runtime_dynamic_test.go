package housegate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/sitable"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

func dynamicRuntimePorts() StorageIntegrityRuntimeOptions {
	return StorageIntegrityRuntimeOptions{
		StatementSubmitter: &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
		SourcePreparer:     &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}},
		StatusQuerier:      rootRecordingStatusQuerier{},
		PayloadWriter:      &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}},
		MergeGuard:         &recordingBuildMergeGuard{},
	}
}

// TestBuildStorageIntegrityRuntimeDynamicSkipsTheStaticSchemaChecks is spec
// 2026-09-24 §9.3: a dynamic host has no startup schema set and no
// config-to-schema bijection; the static set keeps both.
func TestBuildStorageIntegrityRuntimeDynamicSkipsTheStaticSchemaChecks(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)
	ingress, guard, err := buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, sitable.NewFake(sitable.Pending), nil, dynamicRuntimePorts())
	if err != nil {
		t.Fatalf("a dynamic runtime needs no startup schema set: %v", err)
	}
	defer ingress.Close()
	if guard == nil {
		t.Fatal("the runtime must return its merge supervisor")
	}
	if _, _, err := buildStaticRuntimeConsumer(cfg.StorageIntegrity.Runtime, []string{"net1.events"}, dynamicRuntimePorts()); err == nil || !strings.Contains(err.Error(), "authoritative table schema set") {
		t.Fatalf("err = %v, want the static set to keep requiring its schemas", err)
	}
}

// TestStartStorageIntegrityRuntimeFailsFastOnlyForTheStaticSet is spec
// 2026-09-24 §9.4: a dynamic host starts with the unready table's latch
// closed; the static set keeps its fail-fast startup.
func TestStartStorageIntegrityRuntimeFailsFastOnlyForTheStaticSet(t *testing.T) {
	guard := &controllableMergeGuard{}
	guard.set("db1.bad", sicore.ErrMergeGuardTableMissing)
	supervisor := NewStorageIntegrityMergeSupervisor(guard, sitable.NewFake(sitable.Ordinary, active("db1.good", "db1.bad")...), time.Second)
	if err := startStorageIntegrityRuntime(context.Background(), nil, supervisor, false); err != nil {
		t.Fatalf("a dynamic host must start with one unready table: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.good"); err != nil {
		t.Fatalf("the ready table must admit: %v", err)
	}
	if err := supervisor.CheckMergeHealth("db1.bad"); err == nil {
		t.Fatal("the unready table must stay closed")
	}
	if err := startStorageIntegrityRuntime(context.Background(), nil, supervisor, true); err == nil || !strings.Contains(err.Error(), "storage_integrity.merge_guard") {
		t.Fatalf("err = %v, want the static set to fail startup", err)
	}
}
