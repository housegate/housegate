package rewriter

import (
	"context"
	"fmt"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/sitable"
)

const (
	// storageIntegrityProbeSQL is the fixed DESCRIBE rewritten by the startup
	// build probe. Both engines construct its output byte-identically.
	storageIntegrityProbeSQL = "DESCRIBE TABLE db1.t"

	storageIntegrityProbeUnmodelledSQL           = "SYSTEM RELOAD CONFIG"
	storageIntegrityProbeUnmodelledMessage       = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"
	storageIntegrityProbePhysicalSystemSQL       = "SYSTEM START MERGES hg_unsafe.db1__t"
	storageIntegrityProbePhysicalSystemMessage   = "storage-integrity physical table hg_unsafe.db1__t is not directly addressable"
	storageIntegrityProbePhysicalDatabaseSQL     = "TRUNCATE DATABASE hg_safe"
	storageIntegrityProbePhysicalDatabaseMessage = "storage-integrity physical database hg_safe is not directly addressable"

	// A tagged heredoc is the Spec N D6 version discriminator. Polyglot encodes
	// it as literal_type "dollar_string" with the tag packed into the value as
	// "<tag>\x00<body>"; an engine that reads the raw value without consulting
	// literal_type is handed "tag\x00hg_safe", matches nothing, and forwards a
	// statement its own generator re-emits as merge('hg_safe', ...). Every
	// rewriter-go build before v0.10.0 answers Success here.
	storageIntegrityProbeHeredocSQL     = "SELECT * FROM merge($tag$hg_safe$tag$, 'db1__t')"
	storageIntegrityProbeHeredocMessage = "storage-integrity physical table hg_safe.db1__t is not directly addressable"

	// Contract V2 (spec 2026-09-24 §8.2). DROP of an SI table succeeds and
	// drops only the ordinary physical table; the catch-all is activated by
	// the contract version even when the table map is empty. Both values are
	// copied from plan A's "Handoff to plan B" section.
	storageIntegrityProbeDropSQL         = "DROP TABLE db1.t"
	storageIntegrityProbeEmptyMapMessage = "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"

	// reserved_databases under an empty table map: the engine must protect
	// hg_safe although no table map entry names it (plan A handoff item 6).
	storageIntegrityProbeReservedSQL     = "SELECT * FROM hg_safe.db1__t"
	storageIntegrityProbeReservedMessage = "storage-integrity physical table hg_safe.db1__t is not directly addressable"
)

// The exact V2 rewrite of storageIntegrityProbeDropSQL under the fixed probe
// arguments: the ordinary physical table only, hg_safe / hg_unsafe untouched.
// The engines differ in identifier quoting only (plan A handoff item 3), so
// the probe pins one exact string per engine.
const (
	StorageIntegrityProbeDropExpectedSQLNative = `DROP TABLE phys."db1.t"`
	StorageIntegrityProbeDropExpectedSQLGRPC   = "DROP TABLE phys.`db1.t`"
)

// StorageIntegrityProbeExpectedSQL is the exact output a compatible Spec I
// engine emits for storageIntegrityProbeSQL under the fixed probe arguments.
// It must stay identical to the shared si_describe_metadata_select corpus case.
const StorageIntegrityProbeExpectedSQL = "SELECT name, type, default_kind AS default_type, default_expression, comment, '' AS codec_expression, '' AS ttl_expression FROM system.columns WHERE database = 'hg_safe' AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position"

// The released Go and gRPC build floors that carry storage-integrity
// contract V2. The probe itself identifies the required behavior rather than
// trusting a version string alone — this text is only what a startup
// refusal tells the operator to deploy.
const storageIntegrityProbeRequiredBuild = "rewriter-go >= v0.13.0 or rewriter-grpc >= v0.15.0 (storage-integrity contract V2)"

// StorageIntegrityProbeFactory is a Factory whose concrete engine behavior can
// be verified at startup. Contract V2 alone cannot distinguish patch builds.
type StorageIntegrityProbeFactory interface {
	Factory
	ProbeStorageIntegrityBuild(ctx context.Context) error
}

// storageIntegrityProbeArgs returns the fixed probe arguments; emptyTables
// sends the V2 contract with an empty table map. Every request names the
// reserved databases, as every production request does.
func storageIntegrityProbeArgs(emptyTables bool) *pb.RewriteTableDynamicArgs {
	tables := map[string]*pb.StorageIntegrityArgs_Table{
		"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
	}
	if emptyTables {
		tables = map[string]*pb.StorageIntegrityArgs_Table{}
	}
	return &pb.RewriteTableDynamicArgs{
		DatabaseMap:            map[string]string{"db1": "phys"},
		KnownPhysicalDatabases: []string{"phys"},
		Delim:                  "_",
		StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables:              tables,
			ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
			ReservedRowIdColumn: DefaultReservedRowIDColumn,
			ContractVersion:     StorageIntegrityContractV2,
			ReservedDatabases:   sitable.ReservedDatabases(),
		},
	}
}

type storageIntegrityBuildProbe struct {
	name          string
	emptyTables   bool
	sql           string
	code          pb.RewriteCode
	statementType pb.StatementType
	sqlAfter      string
	// sqlAfterByEngine, when set, replaces sqlAfter with the engine's own
	// exact output (keyed by EngineGRPC / EngineNative).
	sqlAfterByEngine map[string]string
	message          string
}

var storageIntegrityBuildProbes = []storageIntegrityBuildProbe{
	{
		name:          "describe-fingerprint",
		sql:           storageIntegrityProbeSQL,
		code:          pb.RewriteCode_Success,
		statementType: pb.StatementType_STATEMENT_TYPE_DESCRIBE,
		sqlAfter:      StorageIntegrityProbeExpectedSQL,
		message:       "success",
	},
	{
		name:          "unmodelled-catch-all",
		sql:           storageIntegrityProbeUnmodelledSQL,
		code:          pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbeUnmodelledSQL,
		message:       storageIntegrityProbeUnmodelledMessage,
	},
	{
		name:          "protected-physical-system-target",
		sql:           storageIntegrityProbePhysicalSystemSQL,
		code:          pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbePhysicalSystemSQL,
		message:       storageIntegrityProbePhysicalSystemMessage,
	},
	{
		// This D2 invariant is intentionally not a version discriminator:
		// older engines also rejected TRUNCATE, but HouseGate must require the
		// deterministic protected-database classification and message.
		name:          "protected-physical-database",
		sql:           storageIntegrityProbePhysicalDatabaseSQL,
		code:          pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbePhysicalDatabaseSQL,
		message:       storageIntegrityProbePhysicalDatabaseMessage,
	},
	{
		// Spec N D6. Unlike the D2 invariant above, this one IS a version
		// discriminator, and it is the reason the required-build floor moved.
		// It discriminates the ENGINE build, not the FFI library: v0.9.0 and
		// v0.10.0 ship byte-identical polyglot artifacts (same SHA256SUMS), so
		// the fix lives entirely in rewriter-go's Go code and no library pin
		// could have caught a stale one. A behavioural probe can, which is why
		// the floor is enforced here rather than only declared in go.mod.
		name:          "tagged-heredoc-namespace",
		sql:           storageIntegrityProbeHeredocSQL,
		code:          pb.RewriteCode_RewriteError,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbeHeredocSQL,
		message:       storageIntegrityProbeHeredocMessage,
	},
	{
		// V2 D1: the DROP succeeds, rewritten to the ordinary physical table.
		// Every V1-only build rejects it.
		name:          "v2-si-drop-ordinary-physical",
		sql:           storageIntegrityProbeDropSQL,
		code:          pb.RewriteCode_Success,
		statementType: pb.StatementType_STATEMENT_TYPE_DROP_TABLE,
		sqlAfterByEngine: map[string]string{
			EngineNative: StorageIntegrityProbeDropExpectedSQLNative,
			EngineGRPC:   StorageIntegrityProbeDropExpectedSQLGRPC,
		},
		message: "success",
	},
	{
		// V2 D2 (H6): the catch-all fires under an empty table map. A V1-only
		// build activates it by table count and answers Success here.
		name:          "v2-empty-map-catch-all",
		emptyTables:   true,
		sql:           storageIntegrityProbeUnmodelledSQL,
		code:          pb.RewriteCode_UnsupportedStatement,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbeUnmodelledSQL,
		message:       storageIntegrityProbeEmptyMapMessage,
	},
	{
		// reserved_databases: with no table map entry the engine still
		// protects hg_safe. A build that ignores the field forwards this read.
		name:          "v2-empty-map-reserved-database",
		emptyTables:   true,
		sql:           storageIntegrityProbeReservedSQL,
		code:          pb.RewriteCode_RewriteError,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED,
		sqlAfter:      storageIntegrityProbeReservedSQL,
		message:       storageIntegrityProbeReservedMessage,
	},
}

// ProbeStorageIntegrityBuild issues a bounded suite of fixed SI rewrites. The
// exact DESCRIBE fingerprint proves the read shape; the rejection probes prove
// the Spec I fail-closed surface that older engines could acknowledge as v1
// without implementing.
func (f *SentioNetworkFactory) ProbeStorageIntegrityBuild(ctx context.Context) error {
	engine := f.options.Engine
	if engine == "" {
		engine = EngineGRPC
	}
	if f.backend == nil {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): no rewrite backend", engine)
	}
	timeout := f.options.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for _, probe := range storageIntegrityBuildProbes {
		resp, err := f.backend.Rewrite(probeCtx, &pb.RewriteSQLRequest{
			Sql:     probe.sql,
			Options: []*pb.RewriteOption{rewriteOption(storageIntegrityProbeArgs(probe.emptyTables))},
		})
		if err != nil {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): %w; deploy %s",
				engine, probe.name, err, storageIntegrityProbeRequiredBuild)
		}
		if resp == nil {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): nil response; deploy %s",
				engine, probe.name, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetStorageIntegrityContractVersion() != StorageIntegrityContractV2 {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): contract acknowledgement %s, want %s; deploy %s",
				engine, probe.name, resp.GetStorageIntegrityContractVersion(), StorageIntegrityContractV2, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetCode() != probe.code {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): code=%s, want %s; deploy %s",
				engine, probe.name, resp.GetCode(), probe.code, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetStatementType() != probe.statementType {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): statement type=%s, want %s; deploy %s",
				engine, probe.name, resp.GetStatementType(), probe.statementType, storageIntegrityProbeRequiredBuild)
		}
		sqlAfter := probe.sqlAfter
		if probe.sqlAfterByEngine != nil {
			sqlAfter = probe.sqlAfterByEngine[engine]
		}
		if resp.GetSqlAfterRewrite() != sqlAfter {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): SQL fingerprint mismatch; deploy %s",
				engine, probe.name, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetMessage() != probe.message {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): message fingerprint mismatch; deploy %s",
				engine, probe.name, storageIntegrityProbeRequiredBuild)
		}
	}
	return nil
}

var _ StorageIntegrityProbeFactory = (*SentioNetworkFactory)(nil)
