package rewriter

import (
	"github.com/housegate/housegate/pkg/sitable"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// buildDynamicArgs builds RewriteTableDynamicArgs.
//
// `databaseMap` is the auth-filtered logical→physical map (only
// databases the account has read or write permission on appear; owner
// databases are added by the caller). `knownPhysical` is the set of
// names that should be USE'd / SELECT'd as-is. The two context fields
// reflect the session's current state. `protected` is the protected
// namespace (protectedDatabases).
//
// `logicalToRemoteIndex` and `remoteUpstreams` route logical DBs that
// live on a different indexer through `remote(...)`; both empty means
// every logical resolves locally.
func buildDynamicArgs(
	databaseMap map[string]string,
	knownPhysical []string,
	protected []string,
	logicalCtx string,
	physicalCtx string,
	delim string,
	logicalToRemoteIndex map[string]string,
	remoteUpstreams map[string]*pb.RewriteTableDynamicArgs_RemoteUpstream,
	si *pb.StorageIntegrityArgs,
) *pb.RewriteTableDynamicArgs {
	out := &pb.RewriteTableDynamicArgs{
		DatabaseMap:                          databaseMap,
		KnownPhysicalDatabases:               knownPhysical,
		ProtectedDatabases:                   protected,
		UpstreamLogicalDatabaseInContext:     logicalCtx,
		Delim:                                delim,
		LogicalDatabaseToRemoteUpstreamIndex: logicalToRemoteIndex,
		RemoteUpstreams:                      remoteUpstreams,
		StorageIntegrity:                     si,
	}
	if physicalCtx != "" {
		v := physicalCtx
		out.UpstreamPhysicalDatabaseInContext = &v
	}
	return out
}

// rewriteOption returns a single TableNameRewrite option carrying
// the dynamic-args arm. The static-args arm has been retired — every
// caller passed nil after the sentio table-mapper path was removed.
func rewriteOption(dyn *pb.RewriteTableDynamicArgs) *pb.RewriteOption {
	return &pb.RewriteOption{
		Op: pb.RewriteOp_TableNameRewrite,
		Value: &pb.RewriteOption_TableNameArgs{
			TableNameArgs: &pb.RewriteTableNameArgs{
				DynamicArgs: dyn,
			},
		},
	}
}

// protectedDatabases is the namespace caller SQL may never address (spec
// 2026-09-26 T1/T3): the deployment's physical database and the
// protocol-owned databases, physical first, deduplicated. It is sent on every
// request whatever storage_integrity.enabled says; an empty physical name is
// omitted.
func protectedDatabases(physical string) []string {
	reserved := sitable.ReservedDatabases()
	out := make([]string, 0, 1+len(reserved))
	if physical != "" {
		out = append(out, physical)
	}
	for _, db := range reserved {
		if db != physical {
			out = append(out, db)
		}
	}
	return out
}
