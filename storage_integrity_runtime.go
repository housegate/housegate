package housegate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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
	// PayloadPutAuthorizer, when set, authorises every put made through
	// PayloadStoreClient (the payload store's write authentication). Nil
	// sends puts without authorisation. It does not apply to a host-supplied
	// PayloadWriter.
	PayloadPutAuthorizer sicore.PayloadPutAuthorizer

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
	// RecoveryGate, when set, defers the drain of recovered intake records
	// past preServe. preServe then only restores them (their source frontiers
	// stay fenced, so a new admission for such a source waits behind them) and
	// the listeners open; a background task drains them once RecoveryGate
	// returns nil. An embedding host whose source can prepare only after its
	// listeners are up (sentio-node's SNode role starts after the HouseGate
	// surface is ready) must set it: without it a recovered statement that
	// needs a fresh source prepare blocks preServe forever. Nil drains in
	// preServe, before any listener admits a write.
	RecoveryGate func(context.Context) error
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
		payloadWriter = sicore.NewAuthorizedArbiterPayloadStoreWriter(opts.PayloadStoreClient, opts.PayloadPutAuthorizer)
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
		ExpectedSource:        expectedSource,
		Journal:               journal,
		PayloadLeaseManager:   leaseManager,
		StageObserver:         intakeStageMetrics{},
		RecoveryRetryObserver: newRecoveryRetryLogger(time.Now, recoveryRetryLogEvery).observe,
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
	ingress.requireAdmissionSchema = true
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
// §9.2), so statements admitted before a retirement still finish; a Refused
// table that carries a schema (a quarantined indexer's, spec 2026-10-10 §10),
// so statements admitted before a quarantine still recover and clean up while
// the host refuses the table; and a Pending table that carries one (an Active
// incarnation its host holds back, not ready on its SNode or with an unclean
// owner hg_safe audit, spec 2026-10-14 §14), so a record admitted while the
// table was Active still recovers when a restart finds it held. A table
// without a schema never resolves. Under sitable.Static it answers the
// startup schema set, exactly as before.
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
//
// With recoveryGate set, recovery is split: the pending records are restored
// here and drained in the background once the gate opens (see
// StorageIntegrityRuntimeOptions.RecoveryGate).
func startStorageIntegrityRuntime(ctx context.Context, runtime *StorageIntegrityIngress, guard *StorageIntegrityMergeSupervisor, failOnTableError bool, recoveryGate func(context.Context) error) error {
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
	if recoveryGate != nil {
		if err := runtime.RestorePending(ctx); err != nil {
			return fmt.Errorf("storage_integrity.recovery: %w", err)
		}
		runtime.StartBackground(ctx)
		refresh := func(ctx context.Context) {
			if runtime.pressureRunner == nil {
				return
			}
			runtime.pressureRunner.Invalidate()
			if err := runtime.pressureRunner.Refresh(ctx); err != nil && ctx.Err() == nil {
				log.Warnw("storage_integrity.backpressure: post-recovery parts snapshot failed; the periodic refresh retries", "error", err)
			}
		}
		if !runtime.startRecoveryDrain(ctx, recoveryGate, refresh) {
			return fmt.Errorf("storage_integrity.recovery: the ingress is closed")
		}
		log.Info("storage_integrity durable intake records restored; the drain waits for the host's recovery gate")
		return nil
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
	log.Info("storage_integrity durable intake recovery completed")
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

// recoveryRetryLogEvery bounds how often one pending statement's recovery
// retries are logged: the first attempt that leaves it pending, then at most
// once per interval. Recovery retries every second, so an unbounded log would
// flood; a silent one hid why a devnet host could not finish starting
// (2026-11-14).
const recoveryRetryLogEvery = 30 * time.Second

// recoveryRetryLogger logs recovery attempts that left a statement pending,
// rate-limited per statement.
type recoveryRetryLogger struct {
	mu    sync.Mutex
	now   func() time.Time
	every time.Duration
	last  map[string]time.Time
	count map[string]int
}

func newRecoveryRetryLogger(now func() time.Time, every time.Duration) *recoveryRetryLogger {
	return &recoveryRetryLogger{now: now, every: every, last: map[string]time.Time{}, count: map[string]int{}}
}

// observe is a sicore.OrchestratorConfig.RecoveryRetryObserver.
func (l *recoveryRetryLogger) observe(statementID string, res sicore.IntakeResult, err error) {
	l.mu.Lock()
	l.count[statementID]++
	attempts := l.count[statementID]
	at := l.now()
	last, seen := l.last[statementID]
	if seen && at.Sub(last) < l.every {
		l.mu.Unlock()
		return
	}
	l.last[statementID] = at
	l.mu.Unlock()
	reason := res.Reason
	if reason == "" {
		reason = res.Submit.Reason
	}
	log.Warnw("storage_integrity: intake recovery attempt left the statement pending; retrying",
		"statement_id", statementID, "attempts", attempts, "lifecycle", res.Lifecycle,
		"submit", res.Submit.Category, "reason", reason, "error", err)
}
