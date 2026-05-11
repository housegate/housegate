package rewriter

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"housegate/housegate/pkg/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"housegate/housegate/pkg/cluster"
	"housegate/housegate/pkg/credentials"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/peer"
	"housegate/housegate/pkg/route"
	"housegate/housegate/pkg/sqlmeta"
	pb "housegate/housegate/protos"
	"sentioxyz/sentio-core/network/registry"
)

// statementTypeFromProto narrows the protobuf enum to the
// sqlmeta-owned type. Unrecognised values pass through as-is so
// future proto additions don't get silently dropped to UNSPECIFIED.
func statementTypeFromProto(t pb.StatementType) sqlmeta.StatementType {
	return sqlmeta.StatementType(t)
}

// accessedTablesFromProto converts the proto AccessedTable list into
// the sqlmeta-owned struct form. Returns nil for empty input so
// downstream "did the rewriter populate this?" checks via len() / nil
// continue to work.
func accessedTablesFromProto(in []*pb.AccessedTable) []sqlmeta.AccessedTable {
	if len(in) == 0 {
		return nil
	}
	out := make([]sqlmeta.AccessedTable, len(in))
	for i, t := range in {
		out[i] = sqlmeta.AccessedTable{
			OriginalDatabase: t.GetOriginalDatabase(),
			OriginalTable:    t.GetOriginalTable(),
			LogicalDatabase:  t.GetLogicalDatabase(),
			PhysicalDatabase: t.GetPhysicalDatabase(),
			IsRemote:         t.GetIsRemote(),
		}
	}
	return out
}

// privilegesDeltasFromProto converts the proto PrivilegeDelta list into
// the sqlmeta-owned struct form. Returns nil for empty input so the
// "did the rewriter populate this?" check via len()/nil keeps working.
func privilegesDeltasFromProto(in []*pb.PrivilegeDelta) []sqlmeta.PrivilegeDelta {
	if len(in) == 0 {
		return nil
	}
	out := make([]sqlmeta.PrivilegeDelta, len(in))
	for i, d := range in {
		var grantees []sqlmeta.PrivilegeGrantee
		if g := d.GetGrantees(); len(g) > 0 {
			grantees = make([]sqlmeta.PrivilegeGrantee, len(g))
			for j, gg := range g {
				grantees[j] = sqlmeta.PrivilegeGrantee{
					Name:          gg.GetName(),
					IsCurrentUser: gg.GetIsCurrentUser(),
				}
			}
		}
		privs := d.GetPrivileges()
		out[i] = sqlmeta.PrivilegeDelta{
			Action:           sqlmeta.PrivilegeAction(d.GetAction()),
			Scope:            sqlmeta.PrivilegeScope(d.GetScope()),
			OriginalDatabase: d.GetOriginalDatabase(),
			LogicalDatabase:  d.GetLogicalDatabase(),
			PhysicalDatabase: d.GetPhysicalDatabase(),
			OriginalTable:    d.GetOriginalTable(),
			PhysicalTable:    d.GetPhysicalTable(),
			Privileges:       privs,
			Category:         sqlmeta.CategorizePrivileges(privs),
			Grantees:         grantees,
		}
		out[i].GrantOption = d.GetGrantOption()
	}
	return out
}

// SentioNetworkFactory is the production Factory implementation. It
// owns the gRPC connection to the sql-rewriter service and the
// long-lived references the per-connection Rewriters need
// (NetworkState, table-mapper factory, callback addr, and optional
// cluster manager / credential provider).
//
// Construction dials the rewriter service synchronously and fails
// fast if it cannot connect — the proxy treats a missing rewriter as
// "rewriting disabled" rather than continuing to retry forever.
type SentioNetworkFactory struct {
	options            Options
	networkState       network.State
	grpcConn           *grpc.ClientConn
	grpcClient         pb.RewriterServiceClient
	tableMapperFactory SentioNetworkTableMapperFactory
	callbackAddr       string                         // resolved address for remote() callback
	clusterMgr         cluster.Cluster                // optional, for multi-replica isLocal detection
	credProvider       credentials.CredentialProvider // optional, for remote() credential replacement
	getIndexerId       func() uint64                  // optional, identifies "this proxy" for remote-upstream routing
	peerSigner         PeerSigner                     // optional, mints peer-relay JWS for remote() calls
	peerTokenTTL       time.Duration                  // peer JWS validity window; 0 → 5m default
}

// peerLoginTokenTTL is the default validity window of the peer-relay
// JWS the rewriter mints for outbound remote() user/password fields.
//
// Has to cover more than just "rewrite → first dial". ClickHouse keeps
// the rewritten SQL (with the JWS embedded as remote() user/password
// args) inside its Cluster object and re-uses it whenever it needs to
// re-dial — connection-pool eviction, server restart on the upstream
// proxy, query plan referenced from an MV refresh, etc. A 5-minute
// window gets routinely outrun in practice; 1 hour is the sweet spot
// where most stale-conn re-handshakes still succeed and exposure if a
// query log leaks is bounded.
//
// Operators can override via Options.PeerTokenTTL when 1h is too
// generous (high-leak risk) or not enough (very long pipeline waits).
const peerLoginTokenTTL = 1 * time.Minute

// NewSentioNetworkFactory dials the sql-rewriter gRPC service and
// returns a Factory that produces per-connection Rewriters. The
// table-mapper factory must be supplied — it is the bridge to
// sentio-core's TableMapper.
func NewSentioNetworkFactory(opts Options, state network.State, tableMapperFactory SentioNetworkTableMapperFactory) (*SentioNetworkFactory, error) {
	if opts.ServiceAddr == "" {
		return nil, fmt.Errorf("rewriter service_addr is required when rewriter is enabled")
	}
	if opts.EnableStaticMapping && tableMapperFactory == nil {
		return nil, fmt.Errorf("table mapper factory is required when EnableStaticMapping=true (must be backed by sentio-core TableMapper)")
	}

	callbackAddr := resolveCallbackAddr(opts.CallbackAddr, opts.Upstream, opts.Listen)
	log.Infow("resolved remote() callback address", "addr", callbackAddr)

	kaParams := keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             5 * time.Second,
		PermitWithoutStream: true,
	}
	connectTimeout := opts.Timeout
	if connectTimeout == 0 {
		connectTimeout = 10 * time.Second
	}
	connectCtx, connectCancel := context.WithTimeout(context.Background(), connectTimeout)
	defer connectCancel()

	conn, err := grpc.DialContext(connectCtx, opts.ServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rewriter service at %s: %w", opts.ServiceAddr, err)
	}
	log.Infow("connected to rewriter service", "service_addr", opts.ServiceAddr)

	return &SentioNetworkFactory{
		options:            opts,
		networkState:       state,
		grpcConn:           conn,
		grpcClient:         pb.NewRewriterServiceClient(conn),
		tableMapperFactory: tableMapperFactory,
		callbackAddr:       callbackAddr,
	}, nil
}

// SetClusterManager wires a cluster manager so isLocal detection can
// account for multi-replica shards. Optional. The argument may be a
// *cluster.Manager (the production case) or any other cluster.Cluster
// implementation.
func (f *SentioNetworkFactory) SetClusterManager(m cluster.Cluster) { f.clusterMgr = m }

// SetCredentialProvider wires a credential provider so remote() calls
// substitute real ClickHouse credentials instead of the
// client-supplied ones. Optional.
func (f *SentioNetworkFactory) SetCredentialProvider(cp credentials.CredentialProvider) {
	f.credProvider = cp
}

// SetPeerTokenTTL overrides the default peer-relay JWS validity window
// (peerLoginTokenTTL = 1h). Operators tune this when the default trade-off
// — long enough to outlive ClickHouse connection-pool churn, short enough
// to bound replay if a query log leaks — does not match their threat
// model. Values <= 0 are treated as "use the default". Optional.
func (f *SentioNetworkFactory) SetPeerTokenTTL(d time.Duration) {
	f.peerTokenTTL = d
}

// SetPeerSigner wires a peer-relay JWS signer so the rewriter can
// authenticate this proxy to other housegates when emitting remote()
// clauses. With a signer set, every remote() target is given:
//
//	user     = "__peer__|<signer.Address()>"  (wrapped in route envelope
//	            when going through the local callback)
//	password = peer-relay JWS bound to the receiving proxy's indexer id
//
// The receiving proxy's credential plugin verifies the JWS and marks
// the session SessionState.IsPeerTrusted, which causes its auth and
// rewrite plugins to bypass themselves so the inner SQL emitted by
// this rewriter is not re-validated or re-rewritten.
//
// Optional. With no signer, the rewriter falls back to the legacy
// credProvider lookup (static ClickHouse user/password), which works
// only when the receiving end has not enabled peer auth.
func (f *SentioNetworkFactory) SetPeerSigner(s PeerSigner) {
	f.peerSigner = s
}

// SetGetIndexerId wires the getter that resolves this proxy's own
// indexer id. Used when populating
// RewriteTableDynamicArgs.remote_upstreams: a logical database whose
// DatabaseInfo.IndexerId differs from the value returned here is
// routed through `remote(...)` instead of the local physical DB.
//
// Optional. With no getter (or a getter returning 0), every logical
// is treated as local — the rewriter never emits a remote upstream
// for the dynamic-args path.
func (f *SentioNetworkFactory) SetGetIndexerId(getter func() uint64) {
	f.getIndexerId = getter
}

// NewRewriter implements Factory. The returned Rewriter holds a
// reference to sess and will read account / database state from it on
// every Rewrite call.
func (f *SentioNetworkFactory) NewRewriter(sess Session) Rewriter {
	return &sentioRewriter{factory: f, sess: sess}
}

// Close tears down the underlying gRPC connection. Safe to call
// multiple times.
func (f *SentioNetworkFactory) Close() error {
	if f.grpcConn == nil {
		return nil
	}
	return f.grpcConn.Close()
}

// sentioRewriter is the per-connection Rewriter. It is intentionally
// small — all heavy resources live on the Factory; the per-conn state
// is just (factory pointer + session reference + last SQL + last
// effective account so RewriteErrorMessage can reuse the same gating
// principal Rewrite used).
type sentioRewriter struct {
	factory *SentioNetworkFactory
	sess    Session

	mu                   sync.Mutex
	lastSQL              string
	lastEffectiveAccount string

	closed atomic.Bool
}

func (r *sentioRewriter) Close() error {
	r.closed.Store(true)
	return nil
}

// Rewrite is the unified entry point. Flow:
//
//  1. Phase 1 — call rewriter with empty options to get
//     `original_accessed_tables` (used to discover SELECT-side
//     tables we need to mapper-resolve).
//  2. Build SelectStmtArgs from the table mapper for the discovered
//     tables, build DynamicArgs from the auth-filtered network state.
//  3. Phase 2 — call rewriter with both arg arms; the rewriter
//     applies whichever matches the parsed statement type.
//  4. Stash the input SQL so RewriteErrorMessage can use it as
//     context.
//
// Error handling: callers (the rewrite plugin) are expected to be
// fail-open — log the returned error and forward the original SQL.
// We return errors for genuine failures (parse error, network drop,
// InvalidRewriteRequest); UnsupportedStatement is downgraded to
// (sql, nil) since "rewriter doesn't handle it" is a passthrough
// signal, not a failure.
func (r *sentioRewriter) Rewrite(ctx context.Context, sql, effectiveAccount string) (RewriteResult, error) {
	if r.closed.Load() {
		return RewriteResult{}, fmt.Errorf("rewriter closed")
	}

	r.mu.Lock()
	r.lastSQL = sql
	r.lastEffectiveAccount = effectiveAccount
	r.mu.Unlock()

	// Static-args path is opt-in. When disabled, we skip phase 1
	// (the empty-options call that enumerates accessed tables) and
	// every sentio table-mapper lookup, sending only dynamic_args on
	// a single round-trip.
	var staticArgs *pb.RewriteTableStaticArgs
	if r.factory.options.EnableStaticMapping {
		astTableNames, err := r.discoverAccessedTables(ctx, sql)
		if err != nil {
			return RewriteResult{}, fmt.Errorf("phase 1 AST parse: %w", err)
		}
		tables := r.parseTables(astTableNames)
		localMap, remoteMap, err := r.factory.buildSentioTableMappings(ctx, tables)
		if err != nil {
			return RewriteResult{}, fmt.Errorf("build sentio table mappings: %w", err)
		}
		staticArgs = buildStaticArgs(localMap, remoteMap)
	}

	dbMap, knownPhys, err := r.factory.buildDatabaseMap(effectiveAccount)
	if err != nil {
		return RewriteResult{}, fmt.Errorf("build database map: %w", err)
	}
	logicalToRemote, remoteUpstreams := r.factory.buildRemoteUpstreams(dbMap)
	dynArgs := buildDynamicArgs(dbMap, knownPhys, r.sess.LogicalDatabaseName(), r.sess.PhysicalDatabaseName(), r.factory.options.Delim, logicalToRemote, remoteUpstreams)

	if staticArgs == nil && len(dbMap) == 0 && len(knownPhys) == 0 && r.sess.LogicalDatabaseName() == "" {
		// Nothing to do — no mappings, no session context. No gRPC
		// call, so classification / accessed-tables / rewrite maps
		// are unknown.
		return RewriteResult{SQL: sql}, nil
	}

	req := &pb.RewriteSQLRequest{
		Sql:     sql,
		Options: []*pb.RewriteOption{rewriteOption(staticArgs, dynArgs)},
	}
	resp, err := r.callWithTimeout(ctx, req)
	if err != nil {
		return RewriteResult{}, fmt.Errorf("phase 2 rewrite: %w", err)
	}
	switch resp.Code {
	case pb.RewriteCode_Success:
		r.maybeUpdateLogicalDatabase(sql, resp)
		return RewriteResult{
			SQL:              resp.GetSqlAfterRewrite(),
			StatementType:    statementTypeFromProto(resp.GetStatementType()),
			AccessedTables:   accessedTablesFromProto(resp.GetOriginalAccessedTables()),
			TableRewrites:    resp.GetTableRewrites(),
			DatabaseRewrites: resp.GetDatabaseRewrites(),
			PrivilegesDeltas: privilegesDeltasFromProto(resp.GetPrivilegesDeltas()),
		}, nil
	case pb.RewriteCode_UnsupportedStatement:
		log.Debugw("rewriter unsupported statement, forwarding original SQL", "message", resp.Message)
		// Per the proto contract, statement_type / table_rewrites /
		// database_rewrites are UNSPECIFIED-or-empty on non-Success;
		// original_accessed_tables may still be populated when
		// classification ran far enough to enumerate them. Propagate
		// whatever the rewriter set.
		return RewriteResult{
			SQL:              sql,
			StatementType:    statementTypeFromProto(resp.GetStatementType()),
			AccessedTables:   accessedTablesFromProto(resp.GetOriginalAccessedTables()),
			TableRewrites:    resp.GetTableRewrites(),
			DatabaseRewrites: resp.GetDatabaseRewrites(),
			PrivilegesDeltas: privilegesDeltasFromProto(resp.GetPrivilegesDeltas()),
		}, nil
	default:
		return RewriteResult{}, fmt.Errorf("rewriter rejected SQL (code=%s): %s", resp.Code, resp.Message)
	}
}

// maybeUpdateLogicalDatabase mirrors a `USE` observation back into
// the session when the rewriter classified the SQL as
// STATEMENT_TYPE_USE. Two cases:
//
//   - Mapped USE (target was in `database_map`): `database_rewrites`
//     has a single entry whose KEY is the logical name the user
//     typed. Use that.
//   - Known-physical USE (target was in `known_physical_databases`):
//     `database_rewrites` is empty (the SQL was forwarded
//     unchanged), but the session still moved to that database.
//     Recover the name from the input SQL via a tiny regex.
//
// SHOW TABLES also populates `database_rewrites` per the proto, but
// it does not change the session's current database — we ignore it
// here.
func (r *sentioRewriter) maybeUpdateLogicalDatabase(sql string, resp *pb.RewriteSQLResponse) {
	if resp.GetStatementType() != pb.StatementType_STATEMENT_TYPE_USE {
		return
	}
	if rewrites := resp.GetDatabaseRewrites(); len(rewrites) > 0 {
		for k := range rewrites {
			r.sess.SetLogicalDatabase(k)
			return
		}
	}
	if name := extractUseTarget(sql); name != "" {
		r.sess.SetLogicalDatabase(name)
	}
}

// useTargetRE is the cheap fallback when database_rewrites is empty
// (the user typed a known-physical name and the rewriter forwarded
// the SQL unchanged). Anchored to the start of the statement; weird
// formatting is treated as "no USE detected" rather than risking a
// false positive.
var useTargetRE = regexp.MustCompile(`(?is)^\s*USE\s+([a-zA-Z_][a-zA-Z0-9_]*)\b`)

func extractUseTarget(sql string) string {
	if m := useTargetRE.FindStringSubmatch(sql); m != nil {
		return m[1]
	}
	return ""
}

// RewriteErrorMessage calls the rewriter's RewriteErrorMessage RPC
// using the SQL of the most recent Rewrite call as context. If no
// Rewrite has happened on this connection, the message is returned
// unchanged.
func (r *sentioRewriter) RewriteErrorMessage(ctx context.Context, message string) (string, error) {
	if r.closed.Load() {
		return message, nil
	}
	r.mu.Lock()
	sql := r.lastSQL
	effectiveAccount := r.lastEffectiveAccount
	r.mu.Unlock()
	if sql == "" {
		return message, nil
	}

	// effectiveAccount is the principal the originating Rewrite call
	// gated against (owner when SQL_x_payer was validated, signer
	// otherwise). Reuse it so the reverse-map sees the same database_map
	// — falling back to sess.Account() would lose owner-only logical DBs
	// from the map and miss reverse-mappings for them. Empty when
	// Rewrite has not run on this connection yet (sql == "" already
	// returned above).
	if effectiveAccount == "" {
		effectiveAccount = r.sess.Account()
	}
	dbMap, knownPhys, err := r.factory.buildDatabaseMap(effectiveAccount)
	if err != nil {
		return message, fmt.Errorf("build database map: %w", err)
	}
	logicalToRemote, remoteUpstreams := r.factory.buildRemoteUpstreams(dbMap)
	dynArgs := buildDynamicArgs(dbMap, knownPhys, r.sess.LogicalDatabaseName(), r.sess.PhysicalDatabaseName(), r.factory.options.Delim, logicalToRemote, remoteUpstreams)

	req := &pb.RewriteErrorMessageRequest{
		Sql:          sql,
		ErrorMessage: message,
		Options:      []*pb.RewriteOption{rewriteOption(nil, dynArgs)},
	}
	timeout := r.factory.options.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := r.factory.grpcClient.RewriteErrorMessage(ctxWithTimeout, req)
	if err != nil {
		return message, fmt.Errorf("RewriteErrorMessage: %w", err)
	}
	if resp.Code != pb.RewriteCode_Success {
		log.Debugw("RewriteErrorMessage non-success", "code", resp.Code, "message", resp.Message)
		return message, nil
	}
	return resp.ErrorAfterRewrite, nil
}

// discoverAccessedTables runs a phase-1 rewrite call with empty
// options to obtain the list of tables the SQL references. The
// rewriter returns structured AccessedTable entries (proto tag 12);
// we project each to "<original_db>.<original_table>" for parseTables
// to consume. Entries with an empty OriginalDatabase project to a
// bare table name and parseTables filters them out — phase 1 only
// resolves sentio table mappings for fully-qualified references.
func (r *sentioRewriter) discoverAccessedTables(ctx context.Context, sql string) ([]string, error) {
	resp, err := r.callWithTimeout(ctx, &pb.RewriteSQLRequest{Sql: sql})
	if err != nil {
		return nil, fmt.Errorf("gRPC: %w", err)
	}
	switch resp.Code {
	case pb.RewriteCode_Success:
		entries := resp.GetOriginalAccessedTables()
		out := make([]string, 0, len(entries))
		for _, t := range entries {
			out = append(out, t.GetOriginalDatabase()+"."+t.GetOriginalTable())
		}
		return out, nil
	case pb.RewriteCode_UnsupportedStatement:
		// Phase 2 will short-circuit the same way.
		return nil, nil
	default:
		return nil, fmt.Errorf("phase 1 (code=%s): %s", resp.Code, resp.Message)
	}
}

// callWithTimeout wraps the gRPC Rewrite call with the configured
// per-call timeout.
func (r *sentioRewriter) callWithTimeout(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	timeout := r.factory.options.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return r.factory.grpcClient.Rewrite(ctxWithTimeout, req)
}

// parseTables splits "<db>.<table>" references into ParsedTable. With
// the deprecation of the `sentio_<id>.` prefix, the dbPart IS the
// logical database / processor id.
func (r *sentioRewriter) parseTables(astTableNames []string) []ParsedTable {
	seen := make(map[string]bool)
	var out []ParsedTable
	for _, name := range astTableNames {
		if seen[name] {
			continue
		}
		parts := strings.SplitN(name, ".", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			continue
		}
		seen[name] = true
		out = append(out, ParsedTable{
			FullMatch:   name,
			ProcessorId: parts[0],
			TableName:   parts[1],
		})
	}
	return out
}

// resolveRemoteCredentials returns the (user, password) pair the
// rewriter should put into a remote() clause that targets the given
// peer indexer.
//
// When a peer-relay signer is wired, the proxy authenticates itself
// as a peer: user = peer.FormatUser(signer.Address()) and password =
// freshly-minted peer-relay JWS. The receiving proxy's credential
// plugin validates the JWS at OnHello and marks the session
// IsPeerTrusted; downstream auth/rewrite plugins bypass themselves
// for the rest of the connection.
//
// Without a peer signer (or when signing fails), falls back to the
// legacy credential-provider lookup (static ClickHouse credentials).
func (f *SentioNetworkFactory) resolveRemoteCredentials(peerIndexerInfo network.IndexerInfo) (user, password string) {
	if f.peerSigner != nil {
		ttl := f.peerTokenTTL
		if ttl <= 0 {
			ttl = peerLoginTokenTTL
		}
		u, p, err := peer.SignPeerHello(f.peerSigner, ttl, peerIndexerInfo, nil)
		if err == nil {
			return u, p
		}
		log.Warnw("peer-relay sign failed; falling back to credProvider",
			"indexer_id", peerIndexerInfo.IndexerId, "err", err)
	}
	if f.credProvider != nil {
		u, p, err := peer.SignPeerHello(nil, 0, peerIndexerInfo, f.credProvider)
		if err == nil {
			return u, p
		}
		log.Warnw("credential lookup failed",
			"indexer_id", peerIndexerInfo.IndexerId, "err", err)
	}
	return "", ""
}

// buildSentioTableMappings resolves each ParsedTable to either a
// local "<database>.<physical_table>" mapping or a remote() mapping,
// using the table-mapper factory + NetworkState.
func (f *SentioNetworkFactory) buildSentioTableMappings(ctx context.Context, tables []ParsedTable) (map[string]TableWithDatabase, map[string]RemoteTable, error) {
	if len(tables) == 0 {
		return nil, nil, nil
	}
	localMap := make(map[string]TableWithDatabase)
	remoteMap := make(map[string]RemoteTable)

	processorTables := make(map[string][]ParsedTable)
	for _, t := range tables {
		processorTables[t.ProcessorId] = append(processorTables[t.ProcessorId], t)
	}

	for processorId, pTables := range processorTables {
		allocations, ok := f.networkState.RetrieveProcessorAllocation(processorId)
		if !ok || len(allocations) == 0 {
			log.Debugw("no processor allocation, skipping sentio table rewrite", "processor_id", processorId)
			continue
		}
		allocation := allocations[0]

		indexerInfo, ok := f.networkState.RetrieveIndexerInfo(allocation.IndexerId)
		if !ok {
			return nil, nil, fmt.Errorf("indexer info not found for indexer_id=%d (processor_id=%s)", allocation.IndexerId, processorId)
		}

		processorInfo, ok := f.networkState.RetrieveProcessorInfo(processorId)
		if !ok {
			processorInfo = network.ProcessorInfo{ProcessorId: processorId}
		}

		mapper, err := f.tableMapperFactory(ctx, processorId, indexerInfo, processorInfo)
		if err != nil {
			return nil, nil, fmt.Errorf("create table mapper for processor_id=%s: %w", processorId, err)
		}
		database := mapper.Database()

		indexerAddr := fmt.Sprintf("%s:%d", indexerInfo.IndexerUrl, indexerInfo.ClickhouseProxyPort)
		isLocal := false
		if f.clusterMgr != nil {
			isLocal = f.clusterMgr.HasReplica(indexerAddr)
		} else {
			isLocal = indexerAddr == f.options.Upstream
		}

		all := mapper.All()
		for _, t := range pTables {
			physical, ok := all[t.TableName]
			if !ok {
				continue
			}
			if isLocal {
				localMap[t.FullMatch] = TableWithDatabase{Database: database, Table: physical}
				continue
			}
			user, password := f.resolveRemoteCredentials(indexerInfo)
			remoteMap[t.FullMatch] = RemoteTable{
				Addr:     f.callbackAddr,
				Database: database,
				Table:    physical,
				User:     route.FormatRouteUser(indexerAddr, user),
				Password: password,
			}
		}
	}
	return localMap, remoteMap, nil
}

// buildDatabaseMap returns the database_map (logical → physical) and
// the list of "known physical" databases.
//
// Three branches, governed by Options.AuthEnabled and the account:
//
//   - AuthEnabled=false AND account=="" → "auth-off + anonymous":
//     every known logical database is mapped. There is no scoping
//     signal and treating it as "see nothing" would break local-dev
//     and auth-disabled deployments.
//   - AuthEnabled=true AND account=="" → "auth-on + unauthenticated":
//     restrictive empty map. The connection has not established an
//     identity yet, so no logical database is accessible.
//   - account != "" → permission-filtered: a database is included
//     when the account has any of Read / Write / Admin / Owner on it.
//     Each bit is checked independently — there is no implicit
//     Owner-⇒-Admin promotion at this layer; that promotion is the
//     PermissionCommitGateObserver's job. RetrieveDatabasePermissions
//     does merge in the wildcard (address(0)) row before returning.
//
// The physical name is always Options.PhysicalDatabase — current
// housegate convention is one physical DB per deployment. An empty
// PhysicalDatabase short-circuits to (nil, nil); the rewriter then
// has no map to consult.
func (f *SentioNetworkFactory) buildDatabaseMap(account string) (map[string]string, []string, error) {
	if f.options.PhysicalDatabase == "" {
		return nil, nil, nil
	}

	if account == "" {
		if f.options.AuthEnabled {
			// Auth is on but identity has not been established yet
			// (or the connection is genuinely anonymous and that's
			// not allowed). No DBs accessible.
			return nil, []string{f.options.PhysicalDatabase}, nil
		}
		// Auth disabled deployment — every known DB is fair game.
		all := f.networkState.RetrieveAllDatabaseInfos()
		dbMap := make(map[string]string, len(all))
		for db, info := range all {
			if info.PendingDelete {
				continue
			}
			dbMap[string(db)] = f.options.PhysicalDatabase
		}
		return dbMap, []string{f.options.PhysicalDatabase}, nil
	}

	perms, _ := f.networkState.RetrieveDatabasePermissions(network.AccountAddress(account))
	// Bulk-fetch the full database snapshot once so we can filter out
	// pending-delete logicals without an O(N) round-trip per permission
	// entry against the statemirror.
	allDBs := f.networkState.RetrieveAllDatabaseInfos()
	dbMap := make(map[string]string, len(perms))
	for db, auth := range perms {
		if auth&(registry.DbAuthRead|registry.DbAuthWrite|registry.DbAuthAdmin|registry.DbAuthOwner) == 0 {
			continue
		}
		if info, ok := allDBs[db]; ok && info.PendingDelete {
			continue
		}
		dbMap[string(db)] = f.options.PhysicalDatabase
	}
	return dbMap, []string{f.options.PhysicalDatabase}, nil
}

// buildRemoteUpstreams partitions databaseMap into local and remote
// logicals based on each logical's DatabaseInfo.IndexerId vs. this
// proxy's own indexer id, and returns the routing pair the rewriter
// expects:
//
//   - logicalToIndex: logical → key (the indexer id stringified) for
//     RewriteTableDynamicArgs.logical_database_to_remote_upstream_index.
//   - remoteUpstreams: key → (addr, user, password) for
//     RewriteTableDynamicArgs.remote_upstreams.
//
// Returns (nil, nil) when there is no getIndexerId getter wired (every
// logical is treated as local) or when nothing in databaseMap is
// remote.
//
// Each remote upstream's Addr is the local callback address and User
// is route.FormatRouteUser(peerAddr, ...) so CH-A dials the local
// proxy and the local proxy's Stripper forwards to the real peer —
// matching the static-path convention so a single firewall rule
// (CH-A → local proxy) covers all cross-shard traffic. The inner
// user / password come from resolveRemoteCredentials, which produces
// either a peer-relay envelope (when peerSigner is wired) or static
// credProvider creds.
//
// Logicals whose IndexerInfo lookup misses are skipped with a warn —
// we'd rather emit a partial map than reject the whole rewrite.
func (f *SentioNetworkFactory) buildRemoteUpstreams(databaseMap map[string]string) (map[string]string, map[string]*pb.RewriteTableDynamicArgs_RemoteUpstream) {
	if len(databaseMap) == 0 || f.getIndexerId == nil {
		return nil, nil
	}
	selfId := f.getIndexerId()

	// Both lookups are Redis round-trips against the statemirror in
	// production. Bulk-fetch each map once and resolve every logical
	// against the cached snapshot — keeps the rewrite path at two
	// Redis hits regardless of how many logicals the account holds.
	allDBs := f.networkState.RetrieveAllDatabaseInfos()
	if len(allDBs) == 0 {
		return nil, nil
	}
	var allIndexers map[uint64]network.IndexerInfo // lazy: only fetch if a remote logical is found

	var (
		logicalToIndex  map[string]string
		remoteUpstreams map[string]*pb.RewriteTableDynamicArgs_RemoteUpstream
	)

	for logical := range databaseMap {
		info, ok := allDBs[network.Database(logical)]
		if !ok {
			continue
		}
		if info.IndexerId == selfId {
			continue
		}
		if allIndexers == nil {
			allIndexers = f.networkState.RetrieveAllIndexerInfos()
		}
		indexerInfo, ok := allIndexers[info.IndexerId]
		if !ok {
			log.Warnw("remote upstream skipped: indexer info missing",
				"logical", logical, "indexer_id", info.IndexerId)
			continue
		}
		if indexerInfo.IndexerUrl == "" || indexerInfo.ClickhouseProxyPort == 0 {
			log.Warnw("remote upstream skipped: indexer info incomplete",
				"logical", logical, "indexer_id", info.IndexerId,
				"url", indexerInfo.IndexerUrl, "port", indexerInfo.ClickhouseProxyPort)
			continue
		}

		key := strconv.FormatUint(info.IndexerId, 10)
		if logicalToIndex == nil {
			logicalToIndex = make(map[string]string)
			remoteUpstreams = make(map[string]*pb.RewriteTableDynamicArgs_RemoteUpstream)
		}
		logicalToIndex[logical] = key
		if _, exists := remoteUpstreams[key]; !exists {
			user, password := f.resolveRemoteCredentials(indexerInfo)
			peerAddr := fmt.Sprintf("%s:%d", indexerInfo.IndexerUrl, indexerInfo.ClickhouseProxyPort)
			// Mirror the static-path convention: dial the local
			// callback address and smuggle the real peer target via
			// the route envelope. CH-A only ever needs reachability to
			// the local proxy; the local proxy's Stripper peels the
			// outer __route__|<peer>| layer in OnHello and forwards to
			// the peer. Without the wrap, dynamic-path remote() would
			// require CH-A to dial every peer directly — different
			// failure mode than the static path and a firewall pain.
			remoteUpstreams[key] = &pb.RewriteTableDynamicArgs_RemoteUpstream{
				Addr:     f.callbackAddr,
				User:     route.FormatRouteUser(peerAddr, user),
				Password: password,
			}
		}
	}
	return logicalToIndex, remoteUpstreams
}

// PhysicalDatabase returns the Options.PhysicalDatabase value. The
// rewrite plugin's OnHello hook reads this so deployments wire the
// physical database name in exactly one place (rewriter.Options) and
// every layer that needs it agrees.
func (f *SentioNetworkFactory) PhysicalDatabase() string {
	return f.options.PhysicalDatabase
}

// resolveCallbackAddr determines the address ClickHouse should use to
// connect back to this proxy when executing remote() calls.
func resolveCallbackAddr(callback, upstream, listen string) string {
	var listenHost, port string
	if strings.HasPrefix(listen, ":") {
		port = listen[1:]
	} else if h, p, err := net.SplitHostPort(listen); err == nil {
		listenHost = h
		port = p
	} else {
		port = listen
	}
	if callback != "" {
		callbackHost, callbackPort, err := net.SplitHostPort(callback)
		if err != nil {
			log.Warnfe(err, "invalid callback address, falling back to listen parsing", "callback", callback)
		} else if callbackPort != port {
			log.Warnw("callback port differs from listen port, using listen port for remote() callback",
				"callback_port", callbackPort, "listen_port", port)
			return net.JoinHostPort(callbackHost, port)
		}
		return callback
	}

	if listenHost != "" && listenHost != "0.0.0.0" && listenHost != "::" {
		return listenHost + ":" + port
	}
	if upstream == "" {
		return "localhost:" + port
	}

	conn, err := net.Dial("udp", upstream)
	if err != nil {
		log.Warnfe(err, "failed to detect local IP for upstream upstream=%v", upstream)
		return "localhost:" + port
	}
	localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	conn.Close()

	if !ok || localAddr.IP.IsUnspecified() {
		return "localhost:" + port
	}
	if localAddr.IP.IsLoopback() {
		probe, err := net.Dial("udp", "8.8.8.8:53")
		if err == nil {
			if probeAddr, ok := probe.LocalAddr().(*net.UDPAddr); ok && !probeAddr.IP.IsUnspecified() && !probeAddr.IP.IsLoopback() {
				probe.Close()
				return fmt.Sprintf("%s:%s", probeAddr.IP.String(), port)
			}
			probe.Close()
		}
		return "localhost:" + port
	}
	return fmt.Sprintf("%s:%s", localAddr.IP.String(), port)
}
