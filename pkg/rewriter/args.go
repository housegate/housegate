package rewriter

import (
	pb "housegate/housegate/protos"
)

// buildStaticArgs builds RewriteTableStaticArgs from the resolved
// local/remote mappings. It is the static-mode payload of every
// rewrite call: the gRPC service applies these explicit
// origin→target mappings whenever the corresponding lookup hits.
//
// The two maps are mutually exclusive per key — a table is either
// rewritten to a local "<db>.<table>" reference or to a remote()
// call. Callers are expected to put each ParsedTable.FullMatch into
// exactly one map.
//
// Returning nil signals "no static-mode override" — the rewriter
// then drops back to the dynamic-mode policy (per
// RewriteTableNameArgs precedence).
func buildStaticArgs(local map[string]TableWithDatabase, remote map[string]RemoteTable) *pb.RewriteTableStaticArgs {
	if len(local) == 0 && len(remote) == 0 {
		return nil
	}
	out := &pb.RewriteTableStaticArgs{
		TableWithDatabaseMap: make(map[string]*pb.RewriteTableStaticArgs_TableWithDatabase, len(local)),
		RemoteTableMap:       make(map[string]*pb.RewriteTableStaticArgs_RemoteTable, len(remote)),
	}
	for k, v := range local {
		out.TableWithDatabaseMap[k] = &pb.RewriteTableStaticArgs_TableWithDatabase{
			Database: v.Database,
			Table:    v.Table,
		}
	}
	for k, v := range remote {
		out.RemoteTableMap[k] = &pb.RewriteTableStaticArgs_RemoteTable{
			Addr:     v.Addr,
			Database: v.Database,
			Table:    v.Table,
			User:     v.User,
			Password: v.Password,
		}
	}
	return out
}

// buildDynamicArgs builds RewriteTableDynamicArgs.
//
// `databaseMap` is the auth-filtered logical→physical map (only
// databases the account has read or write permission on appear; owner
// databases are added by the caller). `knownPhysical` is the set of
// names that should be USE'd / SELECT'd as-is. The two context fields
// reflect the session's current state.
//
// `logicalToRemoteIndex` and `remoteUpstreams` route logical DBs that
// live on a different indexer through `remote(...)`; both empty means
// every logical resolves locally.
func buildDynamicArgs(
	databaseMap map[string]string,
	knownPhysical []string,
	logicalCtx string,
	physicalCtx string,
	delim string,
	logicalToRemoteIndex map[string]string,
	remoteUpstreams map[string]*pb.RewriteTableDynamicArgs_RemoteUpstream,
) *pb.RewriteTableDynamicArgs {
	out := &pb.RewriteTableDynamicArgs{
		DatabaseMap:                          databaseMap,
		KnownPhysicalDatabases:               knownPhysical,
		UpstreamLogicalDatabaseInContext:     logicalCtx,
		Delim:                                delim,
		LogicalDatabaseToRemoteUpstreamIndex: logicalToRemoteIndex,
		RemoteUpstreams:                      remoteUpstreams,
	}
	if physicalCtx != "" {
		v := physicalCtx
		out.UpstreamPhysicalDatabaseInContext = &v
	}
	return out
}

// rewriteOption returns a single TableNameRewrite option carrying
// both arms of the table-name args. Either or both may be nil; the
// rewriter picks via the static→dynamic precedence rule (a non-nil
// static_args wins, even when its maps are empty).
func rewriteOption(static *pb.RewriteTableStaticArgs, dyn *pb.RewriteTableDynamicArgs) *pb.RewriteOption {
	return &pb.RewriteOption{
		Op: pb.RewriteOp_TableNameRewrite,
		Value: &pb.RewriteOption_TableNameArgs{
			TableNameArgs: &pb.RewriteTableNameArgs{
				StaticArgs:  static,
				DynamicArgs: dyn,
			},
		},
	}
}
