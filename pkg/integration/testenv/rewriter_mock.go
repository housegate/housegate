package testenv

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"

	"housegate/housegate/pkg/config"
	pb "housegate/housegate/protos"
)

// RewriterMock implements protos.RewriterServiceServer with the
// minimum behaviour needed to unblock the rest of the proxy chain in
// integration tests. The production rewriter parses the SQL via
// ClickHouse's ParserQuery and applies table-name prefixing and a
// dozen other transformations; the mock parses NOTHING.
//
// What it does:
//   - Returns the input SQL unchanged in `sql_after_rewrite`.
//   - Classifies the statement by case-insensitive keyword prefix.
//     That single byte of metadata is what unblocks commitgate's
//     PermissionObserver (which rejects every Unspecified statement)
//     and the sessionstate.SetLogicalDatabase callback the rewriter
//     fires on STATEMENT_TYPE_USE.
//   - Captures every received SQL string for tests that want to
//     assert on what the proxy actually shipped to the rewriter.
//
// What it does NOT do:
//   - No AST parsing, no table-name prefix injection, no
//     `database_map` consumption, no `remote()` cross-indexer
//     routing — those features are part of the closed-source
//     production rewriter and are out of scope for the open-source
//     integration suite.
//
// Forward-compat: embeds UnimplementedRewriterServiceServer so a new
// RPC added to the proto file fails to compile here rather than
// silently returning Unimplemented gRPC status.
type RewriterMock struct {
	pb.UnimplementedRewriterServiceServer

	addr   string
	server *grpc.Server
	lis    net.Listener

	mu       sync.Mutex
	seen     []string                       // every SQL received, in order
	seenArgs []*pb.RewriteTableDynamicArgs  // dynamic_args sent by proxy, parallel to seen
	failNext int                            // number of subsequent Rewrite calls to fail (decremented per call)

	// accessed maps an UPPER-cased SQL prefix to the AccessedTable list
	// the mock should attach to the response. Used by commitgate DDL
	// tests so PermissionObserver can run its owner/write check against
	// a real (logical, table) pair instead of the empty-AccessedTables
	// allowsEmptyAccess fall-through (which only covers FROM-less
	// SELECT). The map is checked with strings.HasPrefix; the first
	// matching key wins. Keys should include the trailing space to
	// avoid matching unrelated statements (e.g. "CREATE TABLE " not
	// "CREATE").
	accessed map[string][]*pb.AccessedTable
}

// StartRewriterMock binds 127.0.0.1:0, starts a gRPC server with the
// mock service registered, and returns the running instance. Cleanup
// is wired via t.Cleanup — the mock's GracefulStop runs at test
// teardown, which is AFTER the housegate proxy's own t.Cleanup
// (registered later by StartServerProxy) — so housegate gets to drop
// its rewriter connection cleanly before the mock disappears.
func StartRewriterMock(t *testing.T) *RewriterMock {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("rewriter mock listen: %v", err)
	}

	srv := grpc.NewServer()
	m := &RewriterMock{
		addr:   lis.Addr().String(),
		server: srv,
		lis:    lis,
	}
	pb.RegisterRewriterServiceServer(srv, m)

	go func() {
		// Serve returns when the listener is closed (cleanup path);
		// any other error is fatal-worthy but the test goroutine has
		// no t to fail through, so just stash it for surfacing.
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.GracefulStop()
	})
	return m
}

// Addr returns the host:port the mock is bound to. Pass this to
// cfg.Rewriter.ServiceAddr so housegate dials the mock instead of the
// real (closed-source) rewriter service.
func (m *RewriterMock) Addr() string { return m.addr }

// SeenSQL returns a copy of every SQL string the mock has received so
// far, in receive order. Useful for tests that want to assert the
// proxy actually shipped the SQL through the rewriter (and didn't,
// e.g., short-circuit on a session-state cache).
func (m *RewriterMock) SeenSQL() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.seen))
	copy(out, m.seen)
	return out
}

// FailNext arms the mock to fail the next n Rewrite calls with a
// non-Success RewriteCode. The proxy's rewriter wrapper translates
// that into a Go error and OnQuery fails-open by forwarding the
// original SQL. SeenSQL still records the requests — failing is a
// post-receipt response.
func (m *RewriterMock) FailNext(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNext = n
}

// SetAccessedTables attaches a list of AccessedTable to every Rewrite
// response whose SQL starts (case-insensitive) with prefix. Required
// for commitgate DDL tests — PermissionCommitGateObserver loops over
// AccessedTables and rejects when empty (except for the allowsEmptyAccess
// SELECT/SHOW DATABASES carve-out), so DDL paths need at least one
// concrete (logical, table) pair to clear.
//
// Pass prefix WITH any trailing space ("CREATE TABLE ", "DROP TABLE ")
// to avoid matching unrelated statements that share a leading word.
func (m *RewriterMock) SetAccessedTables(prefix string, tables []*pb.AccessedTable) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.accessed == nil {
		m.accessed = make(map[string][]*pb.AccessedTable)
	}
	m.accessed[strings.ToUpper(prefix)] = tables
}

// SeenDynamicArgs returns a copy of the dynamic_args payload the proxy
// shipped to the mock for each Rewrite call, in receive order. Index
// parallel to SeenSQL.
func (m *RewriterMock) SeenDynamicArgs() []*pb.RewriteTableDynamicArgs {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*pb.RewriteTableDynamicArgs, len(m.seenArgs))
	copy(out, m.seenArgs)
	return out
}

func (m *RewriterMock) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	sql := req.GetSql()
	upper := strings.ToUpper(stripLeadingWhitespace(sql))

	m.mu.Lock()
	m.seen = append(m.seen, sql)
	m.seenArgs = append(m.seenArgs, extractDynamicArgs(req))
	shouldFail := m.failNext > 0
	if shouldFail {
		m.failNext--
	}
	var tables []*pb.AccessedTable
	for prefix, t := range m.accessed {
		if strings.HasPrefix(upper, prefix) {
			tables = t
			break
		}
	}
	m.mu.Unlock()

	if shouldFail {
		// RewriteError (not gRPC error). The proxy's sentioRewriter
		// switch turns any non-Success+non-UnsupportedStatement code
		// into a Go error, which OnQuery then fail-opens on.
		return &pb.RewriteSQLResponse{
			Code:    pb.RewriteCode_RewriteError,
			Message: "rewriter mock: forced failure via FailNext",
		}, nil
	}

	return &pb.RewriteSQLResponse{
		Code:                    pb.RewriteCode_Success,
		SqlAfterRewrite:         sql,
		StatementType:           classify(sql),
		OriginalAccessedTables:  tables,
	}, nil
}

func (m *RewriterMock) RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error) {
	// Pass the error message through unchanged. The production
	// service reverse-maps rewritten table names back to their
	// original forms; the mock did no rewriting, so there's nothing
	// to reverse.
	return &pb.RewriteErrorMessageResponse{
		Code:               pb.RewriteCode_Success,
		ErrorAfterRewrite:  req.GetErrorMessage(),
	}, nil
}

func (m *RewriterMock) Optimize(ctx context.Context, req *pb.OptimizeRequest) (*pb.OptimizeResponse, error) {
	// Identity transform. Optimize is best-effort by contract; callers
	// fall back to the input SQL on any non-OK response, so a no-op
	// pass-through is fine.
	return &pb.OptimizeResponse{
		SqlAfterOptimize: req.GetSql(),
		Code:             pb.OptimizeCode_OptimizeOK,
	}, nil
}

// classify pattern-matches the statement kind from the leading keyword.
// Production code does this via a SQL parser; we use string prefix
// matching, which is enough for housegate's plugins downstream because
// they only care about the enum value, not the parse tree.
//
// Order matters when keywords share a prefix (CREATE DATABASE before
// CREATE TABLE / VIEW, DROP DATABASE before DROP TABLE / VIEW).
func classify(sql string) pb.StatementType {
	upper := strings.ToUpper(stripLeadingWhitespace(sql))
	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return pb.StatementType_STATEMENT_TYPE_SELECT
	case strings.HasPrefix(upper, "INSERT"):
		return pb.StatementType_STATEMENT_TYPE_INSERT
	case strings.HasPrefix(upper, "UPDATE"):
		return pb.StatementType_STATEMENT_TYPE_UPDATE
	case strings.HasPrefix(upper, "DELETE"):
		return pb.StatementType_STATEMENT_TYPE_DELETE
	case strings.HasPrefix(upper, "USE "), strings.HasPrefix(upper, "USE\t"):
		return pb.StatementType_STATEMENT_TYPE_USE
	case strings.HasPrefix(upper, "SHOW TABLES"):
		return pb.StatementType_STATEMENT_TYPE_SHOW_TABLES
	case strings.HasPrefix(upper, "SHOW DATABASES"):
		return pb.StatementType_STATEMENT_TYPE_SHOW_DATABASES
	case strings.HasPrefix(upper, "SHOW CREATE TABLE"):
		return pb.StatementType_STATEMENT_TYPE_SHOW_CREATE_TABLE
	case strings.HasPrefix(upper, "EXISTS TABLE"):
		return pb.StatementType_STATEMENT_TYPE_EXISTS_TABLE
	case strings.HasPrefix(upper, "TRUNCATE TABLE"):
		return pb.StatementType_STATEMENT_TYPE_TRUNCATE_TABLE
	case strings.HasPrefix(upper, "CREATE DATABASE"):
		return pb.StatementType_STATEMENT_TYPE_CREATE_DATABASE
	case strings.HasPrefix(upper, "DROP DATABASE"):
		return pb.StatementType_STATEMENT_TYPE_DROP_DATABASE
	case strings.HasPrefix(upper, "CREATE TABLE"):
		return pb.StatementType_STATEMENT_TYPE_CREATE_TABLE
	case strings.HasPrefix(upper, "DROP TABLE"):
		return pb.StatementType_STATEMENT_TYPE_DROP_TABLE
	case strings.HasPrefix(upper, "CREATE MATERIALIZED VIEW"):
		return pb.StatementType_STATEMENT_TYPE_CREATE_MATERIALIZED_VIEW
	case strings.HasPrefix(upper, "CREATE VIEW"):
		return pb.StatementType_STATEMENT_TYPE_CREATE_VIEW
	case strings.HasPrefix(upper, "DROP VIEW"):
		return pb.StatementType_STATEMENT_TYPE_DROP_VIEW
	case strings.HasPrefix(upper, "ALTER TABLE"):
		return pb.StatementType_STATEMENT_TYPE_ALTER_TABLE
	case strings.HasPrefix(upper, "RENAME TABLE"):
		return pb.StatementType_STATEMENT_TYPE_RENAME_TABLE
	case strings.HasPrefix(upper, "GRANT"):
		return pb.StatementType_STATEMENT_TYPE_GRANT
	case strings.HasPrefix(upper, "REVOKE"):
		return pb.StatementType_STATEMENT_TYPE_REVOKE
	default:
		return pb.StatementType_STATEMENT_TYPE_UNSPECIFIED
	}
}

func stripLeadingWhitespace(s string) string {
	return strings.TrimLeft(s, " \t\n\r")
}

// extractDynamicArgs digs the RewriteTableDynamicArgs out of the request's
// options slice. The proxy wraps it in a RewriteOption →
// RewriteTableNameArgs.DynamicArgs; we just pick the first one set. Returns
// nil if no option carries dynamic args (e.g. the proxy chose static mode
// or has nothing to forward).
func extractDynamicArgs(req *pb.RewriteSQLRequest) *pb.RewriteTableDynamicArgs {
	for _, opt := range req.GetOptions() {
		if tn := opt.GetTableNameArgs(); tn != nil {
			if dyn := tn.GetDynamicArgs(); dyn != nil {
				return dyn
			}
		}
	}
	return nil
}

// WithRewriterMock starts a rewriter mock and returns a ProxyOption that
// wires cfg.Rewriter.ServiceAddr to it. Tests that want to introspect
// the mock should call StartRewriterMock directly and pass its address
// via WithConfigMutator instead.
func WithRewriterMock(t *testing.T) (ProxyOption, *RewriterMock) {
	t.Helper()
	mock := StartRewriterMock(t)
	opt := WithConfigMutator(func(cfg *config.Config) {
		cfg.Rewriter.ServiceAddr = mock.Addr()
		// PhysicalDatabase stays empty — the mock does no
		// database_map injection, so giving it a physical name would
		// just add noise to the dynamic_args the proxy builds.
	})
	return opt, mock
}
