package rewriter

import (
	"context"
	"fmt"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

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

// The released Go and gRPC build floors HouseGate requires. The probe itself
// identifies the required behavior rather than trusting a version string
// alone — this text is only what a startup refusal tells the operator to
// deploy. Contract V2 shipped in rewriter-go v0.13.0 / rewriter-grpc v0.15.0,
// but every server now also runs the table-reference probe, whose floor is
// rewriter-go v0.16.0 / rewriter-grpc v0.16.0; naming the older floor here
// would send an operator to a build the next startup refuses.
const storageIntegrityProbeRequiredBuild = "rewriter-go >= v0.16.0 or rewriter-grpc >= v0.16.0 (storage-integrity contract V2)"

// StorageIntegrityProbeFactory is a Factory whose concrete engine behavior can
// be verified at startup. Contract V2 alone cannot distinguish patch builds.
type StorageIntegrityProbeFactory interface {
	Factory
	ProbeStorageIntegrityBuild(ctx context.Context) error
}

// storageIntegrityProbeArgs returns the fixed probe arguments; emptyTables
// sends the V2 contract with an empty table map. Every request names the
// reserved databases and carries the protected namespace
// (protectedDatabases), as every production request does: the probe proves
// the engine's answers under the request shape production sends.
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
		ProtectedDatabases:     protectedDatabases("phys"),
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
	engine := f.engineName()
	if f.backend == nil {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): no rewrite backend", engine)
	}
	probeCtx, cancel := context.WithTimeout(ctx, f.probeTimeout())
	defer cancel()

	for _, probe := range storageIntegrityBuildProbes {
		resp, err := f.backend.Rewrite(probeCtx, &pb.RewriteSQLRequest{
			Sql:     probe.sql,
			Options: []*pb.RewriteOption{rewriteOption(storageIntegrityProbeArgs(probe.emptyTables))},
		})
		if err := checkProbeAnswer("storage-integrity engine probe", engine, probe,
			StorageIntegrityContractV2, storageIntegrityProbeRequiredBuild, resp, err); err != nil {
			return err
		}
	}
	return nil
}

var _ StorageIntegrityProbeFactory = (*SentioNetworkFactory)(nil)

// expectedSQL is the probe's exact output for the given engine.
func (p storageIntegrityBuildProbe) expectedSQL(engine string) string {
	if p.sqlAfterByEngine != nil {
		return p.sqlAfterByEngine[engine]
	}
	return p.sqlAfter
}

func (f *SentioNetworkFactory) engineName() string {
	if f.options.Engine == "" {
		return EngineGRPC
	}
	return f.options.Engine
}

func (f *SentioNetworkFactory) probeTimeout() time.Duration {
	if f.options.Timeout == 0 {
		return 5 * time.Second
	}
	return f.options.Timeout
}

// checkProbeAnswer compares one probe answer field by field. The error never
// quotes SQL or engine messages, so it cannot leak protocol-owned names.
func checkProbeAnswer(label, engine string, probe storageIntegrityBuildProbe, wantAck pb.StorageIntegrityContractVersion, requiredBuild string, resp *pb.RewriteSQLResponse, err error) error {
	prefix := fmt.Sprintf("%s (engine=%s probe=%s)", label, engine, probe.name)
	switch {
	case err != nil:
		return fmt.Errorf("%s: %w; deploy %s", prefix, err, requiredBuild)
	case resp == nil:
		return fmt.Errorf("%s: nil response; deploy %s", prefix, requiredBuild)
	case resp.GetStorageIntegrityContractVersion() != wantAck:
		return fmt.Errorf("%s: contract acknowledgement %s, want %s; deploy %s", prefix, resp.GetStorageIntegrityContractVersion(), wantAck, requiredBuild)
	case resp.GetCode() != probe.code:
		return fmt.Errorf("%s: code=%s, want %s; deploy %s", prefix, resp.GetCode(), probe.code, requiredBuild)
	case resp.GetStatementType() != probe.statementType:
		return fmt.Errorf("%s: statement type=%s, want %s; deploy %s", prefix, resp.GetStatementType(), probe.statementType, requiredBuild)
	case resp.GetSqlAfterRewrite() != probe.expectedSQL(engine):
		return fmt.Errorf("%s: SQL fingerprint mismatch; deploy %s", prefix, requiredBuild)
	case resp.GetMessage() != probe.message:
		return fmt.Errorf("%s: message fingerprint mismatch; deploy %s", prefix, requiredBuild)
	}
	return nil
}

// TableReferenceProbeRequiredBuild is what a failed table-reference probe tells
// the operator to deploy. rewriter-go v0.15.0 fails the analyzer-off and
// engine-argument cases.
const TableReferenceProbeRequiredBuild = "rewriter-go >= v0.16.0 or rewriter-grpc >= v0.16.0 (table-reference policy, spec 2026-09-26)"

// TableReferenceProbeFactory is a Factory whose engine can prove the spec
// 2026-09-26 table-reference policy at startup. buildServer requires it of
// every concrete or injected factory, with or without storage integrity (T13).
type TableReferenceProbeFactory interface {
	Factory
	ProbeTableReferencePolicy(ctx context.Context) error
}

// tableReferenceProbeArgs is the request an ordinary session of logical
// database db1 sends with storage integrity disabled: no StorageIntegrityArgs,
// no physical context, and the protected namespace every production request
// carries.
func tableReferenceProbeArgs() *pb.RewriteTableDynamicArgs {
	return &pb.RewriteTableDynamicArgs{
		DatabaseMap:                      map[string]string{"db1": "phys"},
		KnownPhysicalDatabases:           []string{"phys"},
		UpstreamLogicalDatabaseInContext: "db1",
		Delim:                            "_",
		ProtectedDatabases:               protectedDatabases("phys"),
	}
}

// rejectedProbe is a policy case both engines refuse: the input SQL echoed,
// no statement type.
func rejectedProbe(name, sql string, code pb.RewriteCode, message string) storageIntegrityBuildProbe {
	return storageIntegrityBuildProbe{name: name, sql: sql, code: code,
		statementType: pb.StatementType_STATEMENT_TYPE_UNSPECIFIED, sqlAfter: sql, message: message}
}

// tableReferenceProbes are the policy cases (spec 2026-09-26 §9.5 with plan
// deviations D1–D3). Every answer was measured on 2026-10-01 for this exact
// request: rewriter-go v0.16.0 through the native FFI, and rewriter-grpc
// v0.16.0 (source c24ed33) through RewriterServiceImpl::Rewrite. Both
// engines echo the input SQL on a rejection and acknowledge no contract
// without StorageIntegrityArgs. Changing a pin needs a re-measurement against
// both engines; never loosen the comparison to make one pass.
var tableReferenceProbes = []storageIntegrityBuildProbe{
	rejectedProbe("param-table-position", "SELECT * FROM {p:Identifier}",
		pb.RewriteCode_InvalidRewriteRequest, "query parameters are not supported in a database or table position"),
	rejectedProbe("protected-in-operand", "SELECT * FROM db1.o WHERE a IN phys.`db2.x`",
		pb.RewriteCode_InvalidRewriteRequest, "protected database phys is not addressable"),
	// phys is protected as a database_map value anyway; only a name outside
	// the map proves the engine reads protected_databases. rewriter-go
	// v0.16.0 answers Success for this SQL when the field is omitted.
	rejectedProbe("protected-reserved-database", "SELECT * FROM hg_promote.`db2.x`",
		pb.RewriteCode_InvalidRewriteRequest, "protected database hg_promote is not addressable"),
	{
		name: "in-operand-rewritten", sql: "SELECT * FROM db1.o WHERE a IN db1.p",
		code: pb.RewriteCode_Success, statementType: pb.StatementType_STATEMENT_TYPE_SELECT, message: "success",
		sqlAfterByEngine: map[string]string{
			EngineNative: `SELECT * FROM phys."db1.o" "db1.o" WHERE a IN phys."db1.p"`,
			EngineGRPC:   "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (phys.`db1.p`)",
		},
	},
	{
		name: "insert-select-source-rewritten", sql: "INSERT INTO db1.o SELECT * FROM db1.p",
		code: pb.RewriteCode_Success, statementType: pb.StatementType_STATEMENT_TYPE_INSERT, message: "success",
		sqlAfterByEngine: map[string]string{
			EngineNative: `INSERT INTO phys."db1.o" SELECT * FROM phys."db1.p" "db1.p"`,
			EngineGRPC:   "INSERT INTO phys.`db1.o` SELECT * FROM phys.`db1.p` AS `db1.p`",
		},
	},
	// D1: T3 precedes T5, so a protected carrier argument names the database.
	rejectedProbe("protected-carrier-argument", "SELECT * FROM merge('phys', 'db2')",
		pb.RewriteCode_InvalidRewriteRequest, "protected database phys is not addressable"),
	rejectedProbe("table-function-refused", "SELECT * FROM merge('db1', 'o')",
		pb.RewriteCode_UnsupportedStatement, "table function merge is not accepted"),
	rejectedProbe("use-protected", "USE phys",
		pb.RewriteCode_InvalidRewriteRequest, "protected database phys is not addressable"),
	rejectedProbe("unmodelled-class-refused", "SYSTEM RELOAD CONFIG",
		pb.RewriteCode_UnsupportedStatement, "statement is not supported"),
	rejectedProbe("system-table-refused", "SELECT * FROM system.processes",
		pb.RewriteCode_UnsupportedStatement, "system table system.processes is not accessible"),
	// Spec §13 round 2; rewriter-go v0.15.0 answers Success.
	rejectedProbe("analyzer-off-refused", "SELECT * FROM db1.o SETTINGS allow_experimental_analyzer = 0",
		pb.RewriteCode_UnsupportedStatement, "table setting allow_experimental_analyzer is not accepted"),
	// Spec §13; rewriter-go v0.15.0 answers Success.
	rejectedProbe("engine-argument-read-refused", "CREATE TABLE db1.n (d Date, n UInt8) ENGINE = MergeTree(d, (SELECT max(n) FROM db1.o), 8192)",
		pb.RewriteCode_UnsupportedStatement, "statement is not supported"),
}

// ProbeTableReferencePolicy issues the fixed policy cases (spec 2026-09-26
// §9.5, T13) and requires each exact answer.
func (f *SentioNetworkFactory) ProbeTableReferencePolicy(ctx context.Context) error {
	engine := f.engineName()
	if f.backend == nil {
		return fmt.Errorf("rewriter table-reference probe (engine=%s): no rewrite backend; deploy %s", engine, TableReferenceProbeRequiredBuild)
	}
	probeCtx, cancel := context.WithTimeout(ctx, f.probeTimeout())
	defer cancel()
	for _, probe := range tableReferenceProbes {
		resp, err := f.backend.Rewrite(probeCtx, &pb.RewriteSQLRequest{
			Sql:     probe.sql,
			Options: []*pb.RewriteOption{rewriteOption(tableReferenceProbeArgs())},
		})
		if err := checkProbeAnswer("rewriter table-reference probe", engine, probe,
			pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED, TableReferenceProbeRequiredBuild, resp, err); err != nil {
			return err
		}
	}
	return nil
}

// TableReferenceProbeAnswer returns the answer a conforming engine of the
// given kind (EngineGRPC or EngineNative) gives to req when req is one of the
// startup policy probes, and false for any other request. Test doubles that
// stand in for an engine use it to pass the startup probe and to keep probe
// traffic out of what they record.
func TableReferenceProbeAnswer(req *pb.RewriteSQLRequest, engine string) (*pb.RewriteSQLResponse, bool) {
	opts := req.GetOptions()
	if len(opts) != 1 || !proto.Equal(opts[0], rewriteOption(tableReferenceProbeArgs())) {
		return nil, false
	}
	for _, probe := range tableReferenceProbes {
		if probe.sql == req.GetSql() {
			return &pb.RewriteSQLResponse{Code: probe.code, StatementType: probe.statementType,
				SqlAfterRewrite: probe.expectedSQL(engine), Message: probe.message}, true
		}
	}
	return nil, false
}

var _ TableReferenceProbeFactory = (*SentioNetworkFactory)(nil)
