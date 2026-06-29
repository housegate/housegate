package rewriter

import (
	pb "github.com/housegate/rewriter-go/gen/pb"
)

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

func staticRewriteOption(tableMap map[string]string) *pb.RewriteOption {
	return &pb.RewriteOption{
		Op: pb.RewriteOp_TableNameRewrite,
		Value: &pb.RewriteOption_TableNameArgs{
			TableNameArgs: &pb.RewriteTableNameArgs{
				StaticArgs: &pb.RewriteTableStaticArgs{
					TableMap:             sameDatabaseTableMap(tableMap),
					TableWithDatabaseMap: tableWithDatabaseMap(tableMap),
				},
			},
		},
	}
}

func sameDatabaseTableMap(tableMap map[string]string) map[string]string {
	out := map[string]string{}
	for from, to := range tableMap {
		db, table := splitRewriteTableTarget(to)
		fromDB, _ := splitRewriteTableTarget(from)
		if db == "" || db == fromDB {
			out[from] = table
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func tableWithDatabaseMap(tableMap map[string]string) map[string]*pb.RewriteTableStaticArgs_TableWithDatabase {
	out := map[string]*pb.RewriteTableStaticArgs_TableWithDatabase{}
	for from, to := range tableMap {
		db, table := splitRewriteTableTarget(to)
		fromDB, _ := splitRewriteTableTarget(from)
		if db == "" || db == fromDB {
			continue
		}
		out[from] = &pb.RewriteTableStaticArgs_TableWithDatabase{
			Database: db,
			Table:    table,
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func splitRewriteTableTarget(target string) (database, table string) {
	for i, r := range target {
		if r == '.' {
			return target[:i], target[i+1:]
		}
	}
	return "", target
}
