package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-go/gen/pb"

	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/sqlmeta"
)

// fakeBackend lets tests script the transport without a gRPC server or
// FFI library — the first time sentioRewriter's request/response handling
// is unit-testable.
type fakeBackend struct {
	resp       *pb.RewriteSQLResponse
	err        error
	lastReq    *pb.RewriteSQLRequest
	lastErrReq *pb.RewriteErrorMessageRequest

	matResp    *pb.MaterializeSQLResponse
	matErr     error
	lastMatReq *pb.MaterializeSQLRequest
}

func (f *fakeBackend) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	f.lastReq = req
	return f.resp, f.err
}

func (f *fakeBackend) RewriteErrorMessage(_ context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error) {
	f.lastErrReq = req
	return &pb.RewriteErrorMessageResponse{Code: pb.RewriteCode_Success, ErrorAfterRewrite: "inverted"}, nil
}

func (f *fakeBackend) MaterializeSQL(_ context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error) {
	f.lastMatReq = req
	return f.matResp, f.matErr
}

func (f *fakeBackend) Close() error { return nil }

type fakeSession struct {
	account, logical, physical string
	setLogical                 []string
}

func (s *fakeSession) Account() string              { return s.account }
func (s *fakeSession) LogicalDatabaseName() string  { return s.logical }
func (s *fakeSession) PhysicalDatabaseName() string { return s.physical }
func (s *fakeSession) SetLogicalDatabase(n string)  { s.setLogical = append(s.setLogical, n) }

func newFakeFactory(be backend) *SentioNetworkFactory {
	st := network.NewInMemoryNetworkState()
	st.DatabaseInfos["db1"] = network.DatabaseInfo{DatabaseId: "db1"}
	return &SentioNetworkFactory{
		options:  Options{PhysicalDatabase: "phys", AuthEnabled: false},
		registry: st,
		backend:  be,
	}
}

func TestSentioRewriter_SuccessPopulatesResult(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:            pb.RewriteCode_Success,
		SqlAfterRewrite: "SELECT a FROM phys.db1_t",
		StatementType:   pb.StatementType_STATEMENT_TYPE_SELECT,
		TableRewrites:   map[string]string{"db1.t": "phys.db1_t"},
	}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(context.Background(), "SELECT a FROM db1.t", "")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if res.SQL != "SELECT a FROM phys.db1_t" {
		t.Errorf("SQL = %q", res.SQL)
	}
	if res.TableRewrites["db1.t"] != "phys.db1_t" {
		t.Errorf("TableRewrites = %v", res.TableRewrites)
	}
	if be.lastReq.GetSql() != "SELECT a FROM db1.t" {
		t.Errorf("backend saw %q", be.lastReq.GetSql())
	}
}

func TestSentioRewriter_UnsupportedForwardsOriginal(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:    pb.RewriteCode_UnsupportedStatement,
		Message: "nope",
	}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(context.Background(), "OPTIMIZE TABLE x", "")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if res.SQL != "OPTIMIZE TABLE x" {
		t.Errorf("SQL = %q, want original", res.SQL)
	}
}

func TestSentioRewriter_RejectIsAnError(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:    pb.RewriteCode_SyntaxError,
		Message: "parse failed",
	}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(context.Background(), "garbage((", ""); err == nil || !strings.Contains(err.Error(), "parse failed") {
		t.Fatalf("err = %v, want rewriter rejection", err)
	}
}

func TestSentioRewriter_BackendErrorPropagates(t *testing.T) {
	be := &fakeBackend{err: errors.New("transport down")}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(context.Background(), "SELECT 1", ""); err == nil {
		t.Fatal("want error when backend fails (caller is fail-open)")
	}
}

func TestSentioRewriter_UseMirrorsLogicalDatabase(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:             pb.RewriteCode_Success,
		SqlAfterRewrite:  "USE phys",
		StatementType:    pb.StatementType_STATEMENT_TYPE_USE,
		DatabaseRewrites: map[string]string{"db1": "phys"},
	}}
	sess := &fakeSession{}
	rw := newFakeFactory(be).NewRewriter(sess)
	if _, err := rw.Rewrite(context.Background(), "USE db1", ""); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if len(sess.setLogical) != 1 || sess.setLogical[0] != "db1" {
		t.Errorf("SetLogicalDatabase calls = %v, want [db1]", sess.setLogical)
	}
}

func TestNewSentioNetworkFactory_UnknownEngine(t *testing.T) {
	_, err := NewSentioNetworkFactory(Options{Engine: "carrier-pigeon"}, network.NewInMemoryNetworkState())
	if err == nil || !strings.Contains(err.Error(), "carrier-pigeon") {
		t.Fatalf("err = %v, want unknown-engine rejection", err)
	}
}

// TestSentioRewriter_UnsupportedPropagatesBestEffortFields: the
// Unsupported branch must forward the rewriter's best-effort
// classification fields (accessed tables, existence clause) — commitgate
// reads them even when the SQL passes through unrewritten.
func TestSentioRewriter_UnsupportedPropagatesBestEffortFields(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:    pb.RewriteCode_UnsupportedStatement,
		Message: "nope",
		OriginalAccessedTables: []*pb.AccessedTable{{
			OriginalDatabase: "db1", OriginalTable: "t",
		}},
		ExistenceClause: pb.ExistenceClause_EXISTENCE_CLAUSE_IF_EXISTS,
	}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(context.Background(), "DROP TABLE IF EXISTS db1.t SYNC", "")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if len(res.AccessedTables) != 1 || res.AccessedTables[0].OriginalTable != "t" {
		t.Errorf("AccessedTables = %v, want the best-effort table", res.AccessedTables)
	}
	if res.ExistenceClause != sqlmeta.ExistenceClause(pb.ExistenceClause_EXISTENCE_CLAUSE_IF_EXISTS) {
		t.Errorf("ExistenceClause = %v, want IF_EXISTS", res.ExistenceClause)
	}
}

// TestSentioRewriter_UseRegexFallbackMirrorsKnownPhysical: a USE of a
// known-physical database comes back with empty database_rewrites (SQL
// forwarded unchanged); the session must still move — via the regex
// fallback on the input SQL.
func TestSentioRewriter_UseRegexFallbackMirrorsKnownPhysical(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:            pb.RewriteCode_Success,
		SqlAfterRewrite: "USE phys",
		StatementType:   pb.StatementType_STATEMENT_TYPE_USE,
	}}
	sess := &fakeSession{}
	rw := newFakeFactory(be).NewRewriter(sess)
	if _, err := rw.Rewrite(context.Background(), "USE phys", ""); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if len(sess.setLogical) != 1 || sess.setLogical[0] != "phys" {
		t.Errorf("SetLogicalDatabase calls = %v, want [phys] via regex fallback", sess.setLogical)
	}
}

// TestSentioRewriter_RewriteErrorMessage: no-op before any Rewrite (no
// backend call), then after a Rewrite the stashed SQL travels in the
// request and the backend's inversion is returned.
func TestSentioRewriter_RewriteErrorMessage(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:            pb.RewriteCode_Success,
		SqlAfterRewrite: "SELECT 1",
		StatementType:   pb.StatementType_STATEMENT_TYPE_SELECT,
	}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{})

	out, err := rw.RewriteErrorMessage(context.Background(), "boom")
	if err != nil || out != "boom" {
		t.Fatalf("pre-Rewrite passthrough: out=%q err=%v, want boom/nil", out, err)
	}
	if be.lastErrReq != nil {
		t.Fatal("backend must not be called before any Rewrite stashed SQL")
	}

	if _, err := rw.Rewrite(context.Background(), "SELECT 1", "acct"); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	out, err = rw.RewriteErrorMessage(context.Background(), "Table phys.x does not exist")
	if err != nil {
		t.Fatalf("RewriteErrorMessage: %v", err)
	}
	if out != "inverted" {
		t.Errorf("out = %q, want the backend inversion", out)
	}
	if be.lastErrReq.GetSql() != "SELECT 1" {
		t.Errorf("request sql = %q, want the stashed last SQL", be.lastErrReq.GetSql())
	}
}
