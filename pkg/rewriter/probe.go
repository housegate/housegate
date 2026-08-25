package rewriter

import (
	"context"
	"fmt"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// storageIntegrityProbeSQL is the fixed statement rewritten by the startup
// build probe. Both engines construct its output byte-identically.
const storageIntegrityProbeSQL = "DESCRIBE TABLE db1.t"

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

// ProbeStorageIntegrityBuild issues one fixed SI DESCRIBE through the backend
// and requires both the exact SQL fingerprint and a v1 acknowledgement.
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

	resp, err := f.backend.Rewrite(probeCtx, &pb.RewriteSQLRequest{
		Sql:     storageIntegrityProbeSQL,
		Options: []*pb.RewriteOption{rewriteOption(storageIntegrityProbeArgs())},
	})
	if err != nil {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): %w", engine, err)
	}
	if resp == nil {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): nil response", engine)
	}
	if resp.GetStorageIntegrityContractVersion() != StorageIntegrityContractV1 {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): contract acknowledgement %s, want %s; deploy %s",
			engine, resp.GetStorageIntegrityContractVersion(), StorageIntegrityContractV1, storageIntegrityProbeRequiredBuild)
	}
	if resp.GetCode() != pb.RewriteCode_Success {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): code=%s message=%q; deploy %s",
			engine, resp.GetCode(), resp.GetMessage(), storageIntegrityProbeRequiredBuild)
	}
	if got := resp.GetSqlAfterRewrite(); got != StorageIntegrityProbeExpectedSQL {
		return fmt.Errorf("storage-integrity engine probe (engine=%s): unexpected build\n got: %s\nwant: %s\ndeploy %s",
			engine, got, StorageIntegrityProbeExpectedSQL, storageIntegrityProbeRequiredBuild)
	}
	return nil
}

var _ StorageIntegrityProbeFactory = (*SentioNetworkFactory)(nil)
