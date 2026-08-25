package rewriter

import (
	"context"
	"fmt"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"
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
)

// StorageIntegrityProbeExpectedSQL is the exact output a compatible Spec I
// engine emits for storageIntegrityProbeSQL under the fixed probe arguments.
// It must stay identical to the shared si_describe_metadata_select corpus case.
const StorageIntegrityProbeExpectedSQL = "SELECT name, type, default_kind AS default_type, default_expression, comment, '' AS codec_expression, '' AS ttl_expression FROM system.columns WHERE database = 'hg_safe' AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position"

// The final release tags are pinned separately when the fixed Go and C++
// engines are published. The probe itself identifies the required behavior
// without guessing an unreleased version.
const storageIntegrityProbeRequiredBuild = "rewriter-go >= v0.9.0 or rewriter-grpc >= v0.13.0 (storage-integrity Spec I)"

// StorageIntegrityProbeFactory is a Factory whose concrete engine behavior can
// be verified at startup. Contract v1 alone cannot distinguish patch builds.
type StorageIntegrityProbeFactory interface {
	Factory
	ProbeStorageIntegrityBuild(ctx context.Context) error
}

func storageIntegrityProbeArgs() *pb.RewriteTableDynamicArgs {
	return &pb.RewriteTableDynamicArgs{
		DatabaseMap:            map[string]string{"db1": "phys"},
		KnownPhysicalDatabases: []string{"phys"},
		Delim:                  "_",
		StorageIntegrity: &pb.StorageIntegrityArgs{
			Tables: map[string]*pb.StorageIntegrityArgs_Table{
				"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
			},
			ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
			ReservedRowIdColumn: DefaultReservedRowIDColumn,
			ContractVersion:     StorageIntegrityContractV1,
		},
	}
}

type storageIntegrityBuildProbe struct {
	name          string
	sql           string
	code          pb.RewriteCode
	statementType pb.StatementType
	sqlAfter      string
	message       string
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
			Options: []*pb.RewriteOption{rewriteOption(storageIntegrityProbeArgs())},
		})
		if err != nil {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): %w; deploy %s",
				engine, probe.name, err, storageIntegrityProbeRequiredBuild)
		}
		if resp == nil {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): nil response; deploy %s",
				engine, probe.name, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetStorageIntegrityContractVersion() != StorageIntegrityContractV1 {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): contract acknowledgement %s, want %s; deploy %s",
				engine, probe.name, resp.GetStorageIntegrityContractVersion(), StorageIntegrityContractV1, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetCode() != probe.code {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): code=%s, want %s; deploy %s",
				engine, probe.name, resp.GetCode(), probe.code, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetStatementType() != probe.statementType {
			return fmt.Errorf("storage-integrity engine probe (engine=%s probe=%s): statement type=%s, want %s; deploy %s",
				engine, probe.name, resp.GetStatementType(), probe.statementType, storageIntegrityProbeRequiredBuild)
		}
		if resp.GetSqlAfterRewrite() != probe.sqlAfter {
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
