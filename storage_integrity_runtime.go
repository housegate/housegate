package housegate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
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
// selected-SNode SourcePreparer remains host-owned because HouseGate does not
// import arbiter-core. Production construction requires that adapter to also
// implement PreparedStatementLookup.
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
	SchemaResolver     sicore.TableSchemaResolver
	// TableSchemas is the complete authoritative startup schema set. Runtime
	// construction validates its frozen physical outputs globally before any
	// listener, DDL, or Keeper-backed role can mix distinct logical tables.
	TableSchemas  []payloadexec.TableSchema
	PartsPressure StorageIntegrityPartsPressure
}

func buildStorageIntegrityRuntimeConsumer(runtimeCfg config.StorageIntegrityRuntimeConfig, opts StorageIntegrityRuntimeOptions) (*StorageIntegrityIngress, StorageIntegrityMergeGuard, error) {
	expectedSource := strings.TrimSpace(runtimeCfg.ExpectedSource)

	submitter := opts.StatementSubmitter
	if submitter == nil && opts.ArbiterIngressClient != nil {
		submitter = sicore.NewArbiterStatementSubmitter(opts.ArbiterIngressClient)
	}
	statusQuerier := opts.StatusQuerier
	if statusQuerier == nil && opts.ArbiterIngressClient != nil {
		statusQuerier = sicore.NewArbiterIntakeStatusQuerier(opts.ArbiterIngressClient)
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
	} else if _, ok := opts.SourcePreparer.(sicore.PreparedStatementLookup); !ok {
		errs = append(errs, errors.New("storage_integrity.runtime.source_preparer must implement prepared statement lookup"))
	}
	if statusQuerier == nil {
		errs = append(errs, errors.New("storage_integrity.runtime.status_querier is required"))
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
	if len(opts.TableSchemas) == 0 {
		errs = append(errs, errors.New("storage_integrity.runtime requires the authoritative table schema set (StorageIntegrityRuntimeOptions.TableSchemas)"))
	} else if err := sicore.ValidatePhysicalTableNames(opts.TableSchemas); err != nil {
		errs = append(errs, fmt.Errorf("storage_integrity.runtime schema set: %w", err))
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
	orch = sicore.NewOrchestratorWithQuerier(submitter, opts.SourcePreparer, statusQuerier, orchCfg)
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, mergeGuard, sicore.MaterializerNative, payloadWriter)
	if err != nil {
		return nil, nil, fmt.Errorf("storage_integrity.runtime: %w", err)
	}
	ingress.leaseManager = leaseManager
	ingress.mergeRunner = mergeGuard
	ingress.WithTableSchemas(runtimeTableSchemaResolver(opts.TableSchemas))
	if backpressure := runtimeCfg.Backpressure; backpressure.Enabled {
		unsafeDatabase := strings.TrimSpace(backpressure.UnsafeDatabase)
		safeDatabase := strings.TrimSpace(backpressure.SafeDatabase)
		if safeDatabase == "" || safeDatabase == unsafeDatabase {
			return nil, nil, errors.New("storage_integrity.runtime.backpressure requires a non-empty safe_database distinct from unsafe_database")
		}
		pressure := opts.PartsPressure
		var pressureRunner StorageIntegrityPartsPressureLifecycle
		if pressure == nil {
			if opts.MergeConn == nil {
				return nil, nil, errors.New("storage_integrity.runtime.backpressure requires merge_conn (or set storage_integrity.runtime.backpressure.enabled: false)")
			}
			guard := sicore.NewPartsPressureGuard(opts.MergeConn, sicore.PartsPressureConfig{
				UnsafeDatabase:        unsafeDatabase,
				SafeDatabase:          safeDatabase,
				SoftPartsPerPartition: backpressure.SoftPartsPerPartition,
				HardPartsPerPartition: backpressure.HardPartsPerPartition,
			})
			supervisor := NewStorageIntegrityPartsPressureSupervisor(
				guard,
				backpressure.PollInterval.Duration,
				unsafeDatabase,
				safeDatabase,
			)
			pressureRunner = supervisor
			pressure = supervisor
		} else {
			var ok bool
			pressureRunner, ok = pressure.(StorageIntegrityPartsPressureLifecycle)
			if !ok {
				return nil, nil, errors.New("storage_integrity.runtime injected parts pressure must implement the parts pressure lifecycle (Refresh and Run)")
			}
		}
		if opts.SchemaResolver == nil {
			return nil, nil, errors.New("storage_integrity.runtime.backpressure requires a table schema resolver (StorageIntegrityRuntimeOptions.SchemaResolver)")
		}
		ingress.pressureRunner = pressureRunner
		ingress.WithPartsPressure(pressure, opts.SchemaResolver)
	}
	return ingress, mergeGuard, nil
}

func runtimeTableSchemaResolver(schemas []payloadexec.TableSchema) sicore.TableSchemaResolver {
	byID := make(map[string]payloadexec.TableSchema, len(schemas))
	for _, schema := range schemas {
		byID[schema.TableID] = schema
	}
	return sicore.TableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		schema, ok := byID[tableID]
		return schema, ok
	})
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
	if runtime.pressureRunner != nil {
		if err := runtime.pressureRunner.Refresh(ctx); err != nil {
			return fmt.Errorf("storage_integrity.backpressure: initial parts snapshot: %w", err)
		}
	}
	if err := runtime.RecoverPending(ctx); err != nil {
		return fmt.Errorf("storage_integrity.recovery: %w", err)
	}
	if runtime.pressureRunner != nil {
		runtime.pressureRunner.Invalidate()
		if err := runtime.pressureRunner.Refresh(ctx); err != nil {
			return fmt.Errorf("storage_integrity.backpressure: post-recovery parts snapshot: %w", err)
		}
	}
	runtime.StartBackground(ctx)
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
