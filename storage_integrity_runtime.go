package housegate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/config"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// StorageIntegrityMergeGuard is the startup fail-closed guard for storage
// integrity tables. *storageintegrity.MergeGuard satisfies it; tests and hosts may
// inject narrower implementations at the library boundary.
type StorageIntegrityMergeGuard interface {
	AssertStopMerges(context.Context) error
}

// StorageIntegrityRuntimeOptions supplies the host-owned C1/P1e runtime ports.
// HouseGate can adapt arbiter-proto clients into its core ports and can build
// its durable local journal/spool/merge-guard helpers from config, but the
// selected-SNode SourcePreparer remains host/companion-owned until that seam
// exists in arbiter/arbiter-proto.
type StorageIntegrityRuntimeOptions struct {
	ArbiterIngressClient pb.ArbiterIngressClient
	PayloadStoreClient   pb.PayloadStoreClient

	StatementSubmitter sicore.StatementSubmitter
	SourcePreparer     sicore.SourcePreparer
	StatusQuerier      sicore.IntakeStatusQuerier
	PayloadWriter      sicore.PayloadWriter
	Journal            sicore.IntakeJournal
	PayloadSpool       *sicore.FilePayloadSpool
	MergeConn          sicore.MergeConn
	MergeGuard         StorageIntegrityMergeGuard
}

func buildStorageIntegrityRuntimeConsumer(runtimeCfg config.StorageIntegrityRuntimeConfig, opts StorageIntegrityRuntimeOptions) (*StorageIntegrityIngress, StorageIntegrityMergeGuard, error) {
	expectedSource := strings.TrimSpace(runtimeCfg.ExpectedSource)

	submitter := opts.StatementSubmitter
	if submitter == nil && opts.ArbiterIngressClient != nil {
		submitter = sicore.NewArbiterStatementSubmitter(opts.ArbiterIngressClient)
	}
	payloadWriter := opts.PayloadWriter
	if payloadWriter == nil && opts.PayloadStoreClient != nil {
		payloadWriter = sicore.NewArbiterPayloadStoreWriter(opts.PayloadStoreClient)
	}

	journal := opts.Journal
	if journal == nil && strings.TrimSpace(runtimeCfg.JournalDir) != "" {
		var err error
		journal, err = sicore.NewFileIntakeJournal(strings.TrimSpace(runtimeCfg.JournalDir))
		if err != nil {
			return nil, nil, fmt.Errorf("storage_integrity.runtime.journal: %w", err)
		}
	}

	spool := opts.PayloadSpool
	if spool == nil && strings.TrimSpace(runtimeCfg.PayloadSpoolDir) != "" {
		var err error
		spool, err = sicore.NewFilePayloadSpool(strings.TrimSpace(runtimeCfg.PayloadSpoolDir))
		if err != nil {
			return nil, nil, fmt.Errorf("storage_integrity.runtime.payload_spool: %w", err)
		}
	}
	var leaseManager sicore.PayloadLeaseManager
	if payloadWriter != nil && spool != nil {
		spoolingWriter := sicore.NewSpoolingPayloadWriterWithLeasePolicy(
			spool,
			payloadWriter,
			runtimeCfg.PayloadLease.RefreshBefore.Duration,
		)
		payloadWriter = spoolingWriter
		leaseManager = sicore.NewPayloadLeaseSupervisor(
			spoolingWriter,
			runtimeCfg.PayloadLease.RefreshInterval.Duration,
		)
	}

	rawMergeGuard, err := buildStorageIntegrityMergeGuard(runtimeCfg.MergeGuard, opts)
	if err != nil {
		return nil, nil, err
	}

	var errs []error
	if submitter == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.statement_submitter is required"))
	}
	if opts.SourcePreparer == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.source_preparer is required"))
	}
	if payloadWriter == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.payload_writer is required"))
	}
	if journal == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.journal or journal_dir is required"))
	}
	if spool == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.payload_spool or payload_spool_dir is required"))
	}
	if rawMergeGuard == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.merge_guard or merge_conn is required"))
	}
	if joined := errors.Join(errs...); joined != nil {
		return nil, nil, joined
	}
	mergeGuard := NewStorageIntegrityMergeSupervisor(
		rawMergeGuard,
		runtimeCfg.MergeGuard.ReassertInterval.Duration,
	)

	var orch *sicore.Orchestrator
	orchCfg := sicore.OrchestratorConfig{
		ExpectedSource:      expectedSource,
		Journal:             journal,
		PayloadLeaseManager: leaseManager,
	}
	if opts.StatusQuerier != nil {
		orch = sicore.NewOrchestratorWithQuerier(submitter, opts.SourcePreparer, opts.StatusQuerier, orchCfg)
	} else {
		orch = sicore.NewOrchestrator(submitter, opts.SourcePreparer, orchCfg)
	}
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, mergeGuard, sicore.MaterializerNative, payloadWriter)
	if err != nil {
		return nil, nil, fmt.Errorf("storage_integrity.runtime: %w", err)
	}
	ingress.leaseManager = leaseManager
	ingress.mergeRunner = mergeGuard
	return ingress, mergeGuard, nil
}

func startStorageIntegrityRuntime(ctx context.Context, runtime *StorageIntegrityIngress, guard StorageIntegrityMergeGuard) error {
	if guard != nil {
		if err := guard.AssertStopMerges(ctx); err != nil {
			return fmt.Errorf("storage_integrity.merge_guard: %w", err)
		}
	}
	if runtime == nil {
		return nil
	}
	runtime.StartBackground(ctx)
	if err := runtime.RecoverPending(ctx); err != nil {
		return fmt.Errorf("storage_integrity.recovery: %w", err)
	}
	return nil
}

func buildStorageIntegrityMergeGuard(cfg config.StorageIntegrityRuntimeMergeGuardConfig, opts StorageIntegrityRuntimeOptions) (StorageIntegrityMergeGuard, error) {
	if opts.MergeGuard != nil {
		return opts.MergeGuard, nil
	}
	if opts.MergeConn == nil {
		return nil, nil
	}
	tables, err := storageIntegrityMergeTables(cfg.Tables)
	if err != nil {
		return nil, err
	}
	return sicore.NewMergeGuard(opts.MergeConn, tables), nil
}

func storageIntegrityMergeTables(cfgTables []config.StorageIntegrityRuntimeMergeTableConfig) ([]sicore.MergeTable, error) {
	if len(cfgTables) == 0 {
		return nil, errors.New("storage_integrity.runtime.merge_guard.tables is required when using merge_conn")
	}
	tables := make([]sicore.MergeTable, 0, len(cfgTables))
	for i, table := range cfgTables {
		db := strings.TrimSpace(table.Database)
		tbl := strings.TrimSpace(table.Table)
		if db == "" {
			return nil, fmt.Errorf("storage_integrity.runtime.merge_guard.tables[%d].database is required", i)
		}
		if tbl == "" {
			return nil, fmt.Errorf("storage_integrity.runtime.merge_guard.tables[%d].table is required", i)
		}
		tables = append(tables, sicore.MergeTable{Database: db, Table: tbl})
	}
	return tables, nil
}
