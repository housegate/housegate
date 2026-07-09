package housegate

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/credentials"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
	si "housegate/housegate/pkg/storageintegrity"
)

func buildStorageIntegrityBackgroundTasks(cfg *config.Config, creds credentials.CredentialProvider, selfIndexerID uint64) ([]backgroundTask, func(), error) {
	if cfg == nil || !cfg.StorageIntegrity.Enabled {
		return nil, nil, nil
	}
	workers := normalizedStorageIntegrityWorkers(cfg.StorageIntegrity.Workers)
	if !storageIntegrityWorkersActive(workers) {
		return nil, nil, nil
	}
	workerID := storageIntegrityWorkerID(workers.WorkerID, selfIndexerID)
	leaderVerifier, err := si.NewLeaderSignatureVerifier(cfg.StorageIntegrity.LeaderPublicKeyHex)
	if err != nil {
		return nil, nil, fmt.Errorf("storage integrity leader verifier: %w", err)
	}
	arbiter := si.NewHTTPArbiterClient(cfg.StorageIntegrity.ControlPlaneEndpoint()).WithWorkerID(workerID)
	da := si.NewHTTPDAClient(cfg.StorageIntegrity.DAEndpoint)
	pollInterval := workers.PollInterval.Duration
	errorBackoff := workers.ErrorBackoff.Duration

	var (
		conn        clickhouse.Conn
		hasher      si.TableHasher
		active      si.ActivePartReader
		rootReader  si.PartitionRootReader
		seqStore    si.PromotionSeqStore
		partScanner *si.CachingPartScanner
		cleanup     func()
	)
	needClickHouse := storageIntegrityWorkersNeedClickHouse(workers) || storageIntegrityReplayNeedsActiveVerification(cfg, workers)
	if needClickHouse {
		var err error
		conn, cleanup, err = openStorageIntegrityClickHouse(cfg, workers, creds)
		if err != nil {
			return nil, nil, err
		}
		hasher = si.NewClickHouseTableHasher(conn, "")
		activeReader := si.NewClickHouseActivePartReader(conn, "")
		active = activeReader
		rootReader = si.ActivePartPartitionRootReader{ActiveParts: active}
		seqStore = si.ClickHousePromotionSeqStore{
			Exec:             conn,
			Query:            si.NewClickHouseHashConn(conn),
			MetadataDatabase: workers.MetadataDatabase,
		}
		// Local, discardable part-LtHash cache fast path (opt-in). Keyed by
		// physical part content, it only elides row scans; a miss recomputes via
		// the same fold, so submitted evidence is unchanged. The concrete
		// activeReader provides the cache-miss ReadNamedParts scanner.
		if cfg.StorageIntegrity.PartLtHashCache.Enabled {
			hashConn := si.NewClickHouseHashConn(conn)
			partScanner = &si.CachingPartScanner{
				Inspector: si.ClickHousePartInspector{Conn: hashConn},
				Cache:     si.NewInMemoryPartLtHashCache(cfg.StorageIntegrity.PartLtHashCache.MaxEntries),
				Scanner:   activeReader,
				Schema:    si.ClickHouseSchemaHashReader{Conn: hashConn},
				// Shared across the byte-side scanner (hg_unsafe) and the mutation
				// base commitment (hg_safe); the label is observability only.
				Source: "storage_integrity_part_cache",
			}
		}
	}

	resolver := si.SnapshotResolver{Reader: arbiter, ActiveParts: active}
	guard := si.ClickHouseTableController{
		Conn:                   conn,
		Query:                  si.NewClickHouseHashConn(conn),
		StopMerges:             cfg.StorageIntegrity.SafeTables.StopMerges,
		EnforceNoMergeSettings: cfg.StorageIntegrity.SafeTables.EnforceNoMergeSettings,
	}
	var tasks []backgroundTask
	if needClickHouse && storageIntegrityTableGuardEnabled(cfg) {
		databases := uniqueStorageIntegrityDatabases(append([]string{cfg.StorageIntegrity.SafeDatabase}, cfg.StorageIntegrity.EffectiveUnsafeDatabases()...))
		tasks = append(tasks, backgroundTask{
			Label: "storage-integrity-table-guard",
			Run: superviseStorageIntegrityWorker("storage-integrity-table-guard", errorBackoff, func(ctx context.Context) error {
				return runStorageIntegrityTableGuard(ctx, guard, databases, pollInterval)
			}),
		})
	}
	if workers.Replay || workers.UnsafeValidation {
		var replayVerifier si.ReplayVerifier
		if workers.Replay {
			signer, err := buildReplaySigner(workers, workerID)
			if err != nil {
				if cleanup != nil {
					cleanup()
				}
				return nil, nil, err
			}
			revision := workers.NativePayloadRevision
			if revision == 0 {
				revision = 54460
			}
			replayVerifier = &replay.Verifier{
				Snapshots: si.SnapshotStoreAdapter{Resolver: resolver},
				Payloads:  da,
				Executor:  si.NativePayloadExecutor{Revision: revision},
				Signer:    signer,
			}
			if cfg.StorageIntegrity.SafeTables.VerifyPhysicalActiveMatchesManifest {
				replayVerifier = manifestCheckingReplayVerifier{
					Verifier:     replayVerifier,
					Resolver:     resolver,
					SafeDatabase: cfg.StorageIntegrity.SafeDatabase,
				}
			}
		}
		var scanner si.ByteSideScanner
		if workers.UnsafeValidation {
			// gap-26b: scan real physical parts (ActivePartReader reads _part) so
			// RCRecord candidate parts carry actual part names for the byte-side
			// check; falls back to the per-partition hasher if no active reader is
			// available. FastScan (opt-in) fronts the fold with the part-LtHash
			// cache and is preferred when wired.
			scanner = si.HashingByteSideScanner{FastScan: partScanner, ActiveParts: active, Hasher: hasher, WorkerID: workerID}
		}
		worker := si.VerifierWorker{
			WorkerID:        workerID,
			Arbiter:         arbiter,
			ReplayVerifier:  replayVerifier,
			ByteSideScanner: scanner,
			PollInterval:    pollInterval,
		}
		tasks = append(tasks, backgroundTask{
			Label: "storage-integrity-verifier",
			Run:   superviseStorageIntegrityWorker("storage-integrity-verifier", errorBackoff, worker.Run),
		})
	}
	if workers.Promotion {
		promoter := si.ClickHousePromoter{
			Conn:               conn,
			ActiveParts:        active,
			PartitionRoots:     rootReader,
			PromotionSeqs:      seqStore,
			PromoteDatabase:    workers.PromoteDatabase,
			CleanupUnsafe:      true,
			DropPromoteTable:   true,
			StrictVerification: cfg.StorageIntegrity.Promotion.StrictVerification,
		}
		worker := si.PromotionWorker{
			WorkerID: workerID,
			Arbiter:  arbiter,
			Promoter: si.GuardingPromoter{
				Guard:        guard,
				Resolver:     resolver,
				VerifyActive: cfg.StorageIntegrity.SafeTables.VerifyPhysicalActiveMatchesManifest,
				Promoter:     promoter,
			},
			PollInterval:   pollInterval,
			LeaderVerifier: leaderVerifier,
		}
		tasks = append(tasks, backgroundTask{
			Label: "storage-integrity-promotion",
			Run:   superviseStorageIntegrityWorker("storage-integrity-promotion", errorBackoff, worker.Run),
		})
	}
	if workers.Mutation {
		claimSigner, err := buildMutationClaimSigner(workers, cfg.RelayPrivateKeyHex, workerID)
		if err != nil {
			if cleanup != nil {
				cleanup()
			}
			return nil, nil, err
		}
		executor := si.ClickHouseMutationExecutor{
			Conn:            conn,
			Hasher:          hasher,
			ActiveParts:     active,
			// BaseScan (opt-in) serves the safe base commitment from the part cache
			// instead of a full-table scan; nil when the cache is disabled. Shared
			// with the byte-side scanner — the cache is content-addressed and
			// table-scoped, so reuse across hg_unsafe/hg_safe is safe.
			BaseScan:        partScanner,
			ClaimSigner:     claimSigner,
			WorkerID:        workerID,
			ScratchDatabase: cfg.StorageIntegrity.Mutations.ScratchDatabase,
			MutationsSync:   cfg.StorageIntegrity.Mutations.WaitMutationsSync,
			QueryTimeout:    cfg.StorageIntegrity.Mutations.QueryTimeout.Duration,
		}
		worker := si.MutationWorker{
			WorkerID:          workerID,
			Arbiter:           arbiter,
			Executor:          si.GuardingMutationExecutor{Guard: guard, Resolver: resolver, VerifyActive: cfg.StorageIntegrity.SafeTables.VerifyPhysicalActiveMatchesManifest, Executor: executor},
			SnapshotReader:    arbiter,
			MaxRebindAttempts: cfg.StorageIntegrity.Mutations.MaxRebindAttempts,
			PollInterval:      pollInterval,
		}
		tasks = append(tasks, backgroundTask{
			Label: "storage-integrity-mutation",
			Run:   superviseStorageIntegrityWorker("storage-integrity-mutation", errorBackoff, worker.Run),
		})
	}
	if workers.Rollback {
		worker := si.RollbackWorker{
			WorkerID:     workerID,
			Arbiter:      arbiter,
			Executor:     si.ClickHouseRollbackExecutor{Conn: conn},
			PollInterval: pollInterval,
		}
		tasks = append(tasks, backgroundTask{
			Label: "storage-integrity-rollback",
			Run:   superviseStorageIntegrityWorker("storage-integrity-rollback", errorBackoff, worker.Run),
		})
	}
	if workers.RepairSync {
		executor := si.ClickHouseRepairSyncExecutor{Conn: conn, Hasher: hasher, ActiveParts: active}
		worker := si.RepairSyncWorker{
			WorkerID:     workerID,
			Arbiter:      arbiter,
			Executor:     si.GuardingRepairSyncExecutor{Guard: guard, Executor: executor},
			PollInterval: pollInterval,
		}
		tasks = append(tasks, backgroundTask{
			Label: "storage-integrity-repair-sync",
			Run:   superviseStorageIntegrityWorker("storage-integrity-repair-sync", errorBackoff, worker.Run),
		})
	}
	if workers.SafeAudit {
		auditor := si.ClickHouseSafeAuditor{Hasher: hasher, ActiveParts: active, WorkerID: workerID}
		worker := si.SafeAuditWorker{
			WorkerID:     workerID,
			Arbiter:      arbiter,
			Auditor:      si.GuardingSafeAuditor{Guard: guard, Auditor: auditor},
			PollInterval: pollInterval,
		}
		tasks = append(tasks, backgroundTask{
			Label: "storage-integrity-safe-audit",
			Run:   superviseStorageIntegrityWorker("storage-integrity-safe-audit", errorBackoff, worker.Run),
		})
	}
	if workers.Compaction {
		compactor := si.ClickHouseCompactor{
			Conn:             conn,
			ActiveParts:      active,
			PartitionRoots:   rootReader,
			CompactDatabase:  workers.CompactDatabase,
			DropCompactTable: true,
		}
		worker := si.CompactionWorker{
			WorkerID:       workerID,
			Arbiter:        arbiter,
			Executor:       si.GuardingCompactor{Guard: guard, Compactor: compactor},
			PollInterval:   pollInterval,
			LeaderVerifier: leaderVerifier,
		}
		tasks = append(tasks, backgroundTask{
			Label: "storage-integrity-compaction",
			Run:   superviseStorageIntegrityWorker("storage-integrity-compaction", errorBackoff, worker.Run),
		})
	}
	return tasks, cleanup, nil
}

func startBackgroundTasks(ctx context.Context, tasks []backgroundTask) {
	for _, task := range tasks {
		task := task
		go func() {
			if err := task.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Warnfe(err, "background task %s stopped", task.Label)
			}
		}()
		log.Infow("background task started", "label", task.Label)
	}
}

func superviseStorageIntegrityWorker(label string, backoff time.Duration, run func(context.Context) error) func(context.Context) error {
	if backoff <= 0 {
		backoff = 5 * time.Second
	}
	return func(ctx context.Context) error {
		for {
			err := run(ctx)
			if err == nil || errors.Is(err, context.Canceled) {
				return err
			}
			log.Warnfe(err, "%s iteration failed; retrying", label)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

func normalizedStorageIntegrityWorkers(workers config.StorageIntegrityWorkersConfig) config.StorageIntegrityWorkersConfig {
	if workers.Enabled && !storageIntegrityAnyWorkerRole(workers) {
		workers.Replay = true
		workers.UnsafeValidation = true
		workers.Promotion = true
		workers.Mutation = true
		workers.Rollback = true
		workers.RepairSync = true
		workers.SafeAudit = true
		workers.Compaction = true
	}
	return workers
}

func storageIntegrityWorkersActive(workers config.StorageIntegrityWorkersConfig) bool {
	return workers.Enabled || storageIntegrityAnyWorkerRole(workers)
}

func storageIntegrityAnyWorkerRole(workers config.StorageIntegrityWorkersConfig) bool {
	return workers.Replay || workers.UnsafeValidation || workers.Promotion || workers.Mutation ||
		workers.Rollback || workers.RepairSync || workers.SafeAudit || workers.Compaction
}

func storageIntegrityWorkersNeedClickHouse(workers config.StorageIntegrityWorkersConfig) bool {
	return workers.UnsafeValidation || workers.Promotion || workers.Mutation || workers.Rollback ||
		workers.RepairSync || workers.SafeAudit || workers.Compaction
}

func storageIntegrityReplayNeedsActiveVerification(cfg *config.Config, workers config.StorageIntegrityWorkersConfig) bool {
	return cfg != nil && workers.Replay && cfg.StorageIntegrity.SafeTables.VerifyPhysicalActiveMatchesManifest
}

func storageIntegrityWorkerID(configured string, selfIndexerID uint64) string {
	if configured != "" {
		return configured
	}
	if selfIndexerID != 0 {
		return strconv.FormatUint(selfIndexerID, 10)
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "housegate"
}

func openStorageIntegrityClickHouse(cfg *config.Config, workers config.StorageIntegrityWorkersConfig, creds credentials.CredentialProvider) (clickhouse.Conn, func(), error) {
	addr := workers.ClickHouseAddr
	if addr == "" {
		addr = storageIntegrityLocalClickHouseAddr(cfg)
	}
	if addr == "" {
		return nil, nil, fmt.Errorf("storage_integrity.workers.clickhouse_addr is required when workers need ClickHouse and no local upstream/shard is configured")
	}
	user := workers.ClickHouseUsername
	password := workers.ClickHousePassword
	if user == "" && password == "" && creds != nil {
		var err error
		user, password, err = creds.GetDefaultCredential()
		if err != nil {
			return nil, nil, fmt.Errorf("storage integrity worker credentials: %w", err)
		}
	}
	database := workers.ClickHouseDatabase
	if database == "" {
		database = "default"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open storage integrity ClickHouse connection: %w", err)
	}
	return conn, func() { _ = conn.Close() }, nil
}

func storageIntegrityLocalClickHouseAddr(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	if cfg.Upstream != "" {
		return cfg.Upstream
	}
	if cfg.Shard == nil || len(cfg.Shard.Replicas) == 0 {
		return ""
	}
	for _, replica := range cfg.Shard.Replicas {
		if !replica.IsBackup {
			return replica.Addr()
		}
	}
	return cfg.Shard.Replicas[0].Addr()
}

func buildReplaySigner(workers config.StorageIntegrityWorkersConfig, workerID string) (replay.Signer, error) {
	seedHex := strings.TrimPrefix(strings.TrimSpace(workers.ReplaySignerSeedHex), "0x")
	if seedHex == "" {
		return nil, fmt.Errorf("storage_integrity.workers.replay_signer_seed_hex is required when replay worker is enabled")
	}
	seed, err := hex.DecodeString(seedHex)
	if err != nil {
		return nil, fmt.Errorf("decode replay signer seed: %w", err)
	}
	signer, err := payloadexec.NewEd25519Signer(workerID, seed)
	if err != nil {
		return nil, err
	}
	return signer, nil
}

func buildMutationClaimSigner(workers config.StorageIntegrityWorkersConfig, fallbackPrivateKey, workerID string) (si.MutationClaimSigner, error) {
	key := workers.ClaimPrivateKeyHex
	if key == "" {
		key = fallbackPrivateKey
	}
	if key == "" {
		return nil, fmt.Errorf("storage_integrity.workers.claim_private_key_hex or relay_private_key_hex is required when mutation worker is enabled")
	}
	return si.NewSecp256k1MutationClaimSigner(key, workerID)
}

type manifestCheckingReplayVerifier struct {
	Verifier     si.ReplayVerifier
	Resolver     si.SnapshotResolver
	SafeDatabase string
}

func (v manifestCheckingReplayVerifier) Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error) {
	if v.Verifier == nil {
		return replay.ReplayAttestation{}, fmt.Errorf("replay verifier is required")
	}
	if job.PrevSafeSnapshotID == "" {
		return replay.ReplayAttestation{}, fmt.Errorf("prev_safe_snapshot_id is required for replay active part verification")
	}
	seen := map[string]struct{}{}
	for _, stmt := range job.Statements {
		if stmt.TargetTableID == "" {
			continue
		}
		if _, ok := seen[stmt.TargetTableID]; ok {
			continue
		}
		seen[stmt.TargetTableID] = struct{}{}
		if err := v.Resolver.VerifyLocalTable(ctx, job.PrevSafeSnapshotID, stmt.TargetTableID, storageIntegritySafeTable(v.SafeDatabase, stmt.TargetTableID), nil); err != nil {
			return replay.ReplayAttestation{}, fmt.Errorf("verify replay active parts for %s: %w", stmt.TargetTableID, err)
		}
	}
	return v.Verifier.Verify(ctx, job)
}

func storageIntegrityTableGuardEnabled(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	return cfg.StorageIntegrity.SafeTables.StopMerges || cfg.StorageIntegrity.SafeTables.EnforceNoMergeSettings
}

func runStorageIntegrityTableGuard(ctx context.Context, guard si.ClickHouseTableController, databases []string, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		for _, database := range databases {
			if err := guard.PrepareDatabase(ctx, database); err != nil {
				return err
			}
		}
		timer.Reset(interval)
	}
}

func storageIntegritySafeTable(safeDatabase, tableID string) string {
	if safeDatabase == "" {
		safeDatabase = "hg_safe"
	}
	table := tableID
	if idx := strings.LastIndex(table, "."); idx >= 0 {
		table = table[idx+1:]
	}
	return storageIntegrityQuoteIdent(safeDatabase) + "." + storageIntegrityQuoteIdent(table)
}

func uniqueStorageIntegrityDatabases(values []string) []string {
	seen := map[string]struct{}{}
	var out []string
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
	return out
}

func storageIntegrityQuoteIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}
