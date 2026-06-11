package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-go/gen/pb"

	"housegate/housegate/pkg/network"
)

// fakeBackend lets tests script the transport without a gRPC server or
// FFI library — the first time sentioRewriter's request/response handling
// is unit-testable.
type fakeBackend struct {
	resp    *pb.RewriteSQLResponse
	err     error
	lastReq *pb.RewriteSQLRequest
}

func (f *fakeBackend) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	f.lastReq = req
	return f.resp, f.err
}

func (f *fakeBackend) RewriteErrorMessage(_ context.Context, _ *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error) {
	return &pb.RewriteErrorMessageResponse{Code: pb.RewriteCode_Success, ErrorAfterRewrite: "inverted"}, nil
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
