package housegate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// StorageIntegrityMergeGuard keeps native merges off the hg_* tables and
// reports per logical table (spec 2026-09-24 §9.4): activeTableIDs are
// required to exist, every other present hg_* table is checked when present.
// *storageintegrity.MergeGuard satisfies it; hosts may inject their own
// implementation at the library boundary. The error return is reserved for a
// failure that applies to every table.
type StorageIntegrityMergeGuard interface {
	AssertTables(ctx context.Context, activeTableIDs []string) (sicore.MergeGuardReport, error)
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
	// TableSchemas is the complete authoritative startup schema set. Runtime
	// construction validates its frozen physical outputs globally before any
	// listener, DDL, or Keeper-backed role can mix distinct logical tables.
	TableSchemas  []payloadexec.TableSchema
	PartsPressure StorageIntegrityPartsPressure
}

// buildStorageIntegrityRuntimeConsumer builds the runtime over the table-state
// port. state is always non-nil (the runtime requires storage_integrity.enabled);
// static is non-nil exactly when the table set is the configured
// storage_integrity.tables, and only then are the startup schema set, the
// physical-name injectivity and the config-to-schema bijection checked
// (dynamic hosts rely on the arbiter's physical_name_collision admission rule).
func buildStorageIntegrityRuntimeConsumer(runtimeCfg config.StorageIntegrityRuntimeConfig, state sitable.TableState, static *sitable.Static, opts StorageIntegrityRuntimeOptions) (*StorageIntegrityIngress, *StorageIntegrityMergeSupervisor, error) {
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

	rawMergeGuard := buildStorageIntegrityMergeGuard(opts)

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
	if state == nil {
		errs = append(errs, errors.New("storage_integrity.runtime requires storage_integrity.enabled and a table-state source"))
	}
	if static != nil {
		if len(opts.TableSchemas) == 0 {
			errs = append(errs, errors.New("storage_integrity.runtime requires the authoritative table schema set (StorageIntegrityRuntimeOptions.TableSchemas)"))
		} else {
			if err := sicore.ValidatePhysicalTableNames(opts.TableSchemas); err != nil {
				errs = append(errs, fmt.Errorf("storage_integrity.runtime schema set: %w", err))
			}
			if err := validateStorageIntegrityRuntimeTableSchemas(static.TableIDs(), opts.TableSchemas); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if joined := errors.Join(errs...); joined != nil {
		return nil, nil, joined
	}
	mergeGuard := NewStorageIntegrityMergeSupervisor(
		rawMergeGuard,
		state,
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
	schemaResolver := tableStateSchemaResolver(state)
	ingress.WithTableSchemas(schemaResolver)
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
				RefreshTimeout:        backpressure.RefreshTimeout.Duration,
				SnapshotTTL:           backpressure.SnapshotTTL.Duration,
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
		ingress.pressureRunner = pressureRunner
		ingress.WithPartsPressure(pressure, schemaResolver)
	}
	return ingress, mergeGuard, nil
}

func validateStorageIntegrityRuntimeTableSchemas(tables []string, schemas []payloadexec.TableSchema) error {
	configured := make(map[string]struct{}, len(tables))
	for _, tableID := range tables {
		configured[tableID] = struct{}{}
	}
	resolved := make(map[string]struct{}, len(schemas))
	var errs []error
	for _, schema := range schemas {
		resolved[schema.TableID] = struct{}{}
		if _, ok := configured[schema.TableID]; !ok {
			errs = append(errs, fmt.Errorf("storage_integrity.runtime table schema %q is not listed in storage_integrity.tables", schema.TableID))
		}
	}
	for _, tableID := range tables {
		if _, ok := resolved[tableID]; !ok {
			errs = append(errs, fmt.Errorf("storage_integrity.tables entry %q has no authoritative runtime table schema", tableID))
		}
	}
	return errors.Join(errs...)
}

// tableStateSchemaResolver resolves journal-recovery and cleanup schemas from
// the current snapshot, which answers Active and Gone tables (spec 2026-09-24
// §9.2), so statements admitted before a retirement still finish. Under
// sitable.Static it answers the startup schema set, exactly as before.
func tableStateSchemaResolver(state sitable.TableState) StorageIntegrityTableSchemaResolver {
	return StorageIntegrityTableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		table, ok := state.Current().Schema(tableID)
		if !ok || table.Schema.TableID == "" {
			return payloadexec.TableSchema{}, false
		}
		return table.Schema, true
	})
}

// startStorageIntegrityRuntime asserts the merge guard once before recovery.
// failOnTableError keeps the static table set's startup fail-fast; a dynamic
// host starts with the failing tables' latches closed, so one unready table
// blocks only its own admissions (spec 2026-09-24 §9.4).
func startStorageIntegrityRuntime(ctx context.Context, runtime *StorageIntegrityIngress, guard *StorageIntegrityMergeSupervisor, failOnTableError bool) error {
	if guard != nil {
		if err := guard.Assert(ctx); err != nil {
			if failOnTableError {
				return fmt.Errorf("storage_integrity.merge_guard: %w", err)
			}
			log.Warnw("storage_integrity: merge guard unhealthy at startup; affected tables refuse admission until a reassert succeeds", "error", err)
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

func buildStorageIntegrityMergeGuard(opts StorageIntegrityRuntimeOptions) StorageIntegrityMergeGuard {
	if opts.MergeGuard != nil {
		return opts.MergeGuard
	}
	if opts.MergeConn == nil {
		return nil
	}
	return sicore.NewMergeGuard(opts.MergeConn, nil)
}
