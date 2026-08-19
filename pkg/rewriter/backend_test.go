package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/sqlmeta"
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

	// lastMatCtxHadDeadline records whether the ctx passed to the most
	// recent MaterializeSQL call carried a deadline — used to assert
	// that sentioMaterializer applies its per-call timeout.
	lastMatCtxHadDeadline bool
}

func (f *fakeBackend) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	f.lastReq = req
	return f.resp, f.err
}

func (f *fakeBackend) RewriteErrorMessage(_ context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error) {
	f.lastErrReq = req
	return &pb.RewriteErrorMessageResponse{Code: pb.RewriteCode_Success, ErrorAfterRewrite: "inverted"}, nil
}

func (f *fakeBackend) MaterializeSQL(ctx context.Context, req *pb.MaterializeSQLRequest) (*pb.MaterializeSQLResponse, error) {
	f.lastMatReq = req
	_, ok := ctx.Deadline()
	f.lastMatCtxHadDeadline = ok
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

func TestSentioRewriter_AccessedTablesCarryStorageIntegrityFlag(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code:            pb.RewriteCode_Success,
		SqlAfterRewrite: "SELECT 1",
		StatementType:   pb.StatementType_STATEMENT_TYPE_SELECT,
		OriginalAccessedTables: []*pb.AccessedTable{{
			OriginalDatabase:   "db1",
			OriginalTable:      "t",
			IsStorageIntegrity: true,
		}},
	}}
	res, err := newFakeFactory(be).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT a FROM db1.t", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.AccessedTables) != 1 || !res.AccessedTables[0].IsStorageIntegrity {
		t.Fatalf("AccessedTables = %+v, want IsStorageIntegrity=true", res.AccessedTables)
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

func acknowledgedSIResponse(resp *pb.RewriteSQLResponse) *pb.RewriteSQLResponse {
	resp.StorageIntegrityContractVersion = StorageIntegrityContractV1
	return resp
}

func newSIFactory(be backend, rs StorageIntegrityReadState, insertLane bool) *SentioNetworkFactory {
	f := newFakeFactory(be)
	f.options.StorageIntegrity = siOpts(rs)
	f.options.StorageIntegrity.InsertLaneEnabled = insertLane
	return f
}

func TestSentioRewriter_ShipsStorageIntegrityArgs(t *testing.T) {
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "x", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT})}
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}}}
	rw := newSIFactory(be, rs, true).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(context.Background(), "SELECT a FROM db1.t", ""); err != nil {
		t.Fatal(err)
	}
	si := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
	if si.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE || si.GetContractVersion() != StorageIntegrityContractV1 || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
		t.Fatalf("default-mode args = %v", si)
	}
	ctx := WithReadMode(context.Background(), ReadModeUnsafeLatest)
	if _, err := rw.Rewrite(ctx, "SELECT a FROM db1.t", ""); err != nil {
		t.Fatal(err)
	}
	si = be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
	if si.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST || len(si.GetTables()["db1.t"].GetExcludedUnsafeParts()) != 1 {
		t.Fatalf("per-query unsafe_latest args = %v", si)
	}
}

func TestSentioRewriter_UnsafeLatestWithoutPortIsRejectedBeforeBackend(t *testing.T) {
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success})}
	rw := newSIFactory(be, nil, true).NewRewriter(&fakeSession{})
	_, err := rw.Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want RejectedError", err)
	}
	if be.lastReq != nil {
		t.Fatal("backend must not be called when the mode is unavailable")
	}
}

func TestSentioRewriter_StorageIntegrityRejectIsFailClosed(t *testing.T) {
	for _, code := range []pb.RewriteCode{pb.RewriteCode_UnsupportedStatement, pb.RewriteCode_RewriteError} {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code: code, Message: "storage-integrity table db1.t accepts writes only through the signed statement lane",
			OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", IsStorageIntegrity: true}},
		})}
		rw := newSIFactory(be, nil, true).NewRewriter(&fakeSession{})
		_, err := rw.Rewrite(context.Background(), "DROP TABLE db1.t", "")
		var rej *RejectedError
		if !errors.As(err, &rej) || rej.Code != code || !strings.Contains(rej.Message, "signed statement lane") {
			t.Fatalf("code %v: err = %v, want RejectedError carrying the rewriter message", code, err)
		}
	}
	// Non-SI Unsupported keeps today's pass-through contract.
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, Message: "nope",
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "other", OriginalTable: "u"}}})}
	res, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "OPTIMIZE TABLE other.u", "")
	if err != nil || res.SQL != "OPTIMIZE TABLE other.u" || res.StorageIntegrityContractVersion != StorageIntegrityContractV1 {
		t.Fatalf("non-SI unsupported must pass through: %v %v", res, err)
	}
}

func TestSentioRewriter_OldSuccessfulBackendWithoutSIAcknowledgementFailsClosed(t *testing.T) {
	// Simulates an older protobuf server: it ignores StorageIntegrityArgs,
	// returns Success, and leaves the additive response field at zero.
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1"}}
	_, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "contract acknowledgement") {
		t.Fatalf("err = %v, want fail-closed missing-ack RejectedError", err)
	}
}

func TestSentioRewriter_BackendWithWrongSIAcknowledgementFailsClosed(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1",
		StorageIntegrityContractVersion: pb.StorageIntegrityContractVersion(99),
	}}
	_, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "contract acknowledgement") {
		t.Fatalf("err = %v, want fail-closed wrong-ack RejectedError", err)
	}
}

func TestSentioRewriter_AcknowledgedBackendAllowsNonSITableQuery(t *testing.T) {
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
		Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1",
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "system", OriginalTable: "one"}},
	})}
	res, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	if err != nil || res.StorageIntegrityContractVersion != StorageIntegrityContractV1 {
		t.Fatalf("acknowledged non-SI query = %+v, %v", res, err)
	}
}

func TestSentioRewriter_ConfiguredSISurfaceUnavailableFailsClosed(t *testing.T) {
	for name, be := range map[string]*fakeBackend{
		"transport":    {err: errors.New("transport down")},
		"nil response": {},
	} {
		for _, insertLane := range []bool{false, true} {
			_, err := newSIFactory(be, nil, insertLane).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "INSERT INTO db1.t FORMAT Native", "")
			var rej *RejectedError
			if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_RewriteError || !strings.Contains(rej.Message, "classification unavailable") {
				t.Fatalf("%s insertLane=%v err=%v, want fail-closed RejectedError", name, insertLane, err)
			}
		}
	}
	// The identical outage retains an ordinary error only when no SI
	// membership is configured; the plugin will fail open on that error.
	be := &fakeBackend{err: errors.New("transport down")}
	_, err := newFakeFactory(be).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if err == nil || errors.As(err, &rej) {
		t.Fatalf("empty-SI backend error = %v, want ordinary error", err)
	}

	siClosed := newSIFactory(&fakeBackend{}, nil, false).NewRewriter(&fakeSession{})
	if err := siClosed.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = siClosed.Rewrite(context.Background(), "INSERT INTO db1.t FORMAT Native", "")
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "classification unavailable") {
		t.Fatalf("configured-SI closed rewriter = %v, want RejectedError", err)
	}
	emptyClosed := newFakeFactory(&fakeBackend{}).NewRewriter(&fakeSession{})
	if err := emptyClosed.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = emptyClosed.Rewrite(context.Background(), "SELECT 1", "")
	if err == nil || errors.As(err, &rej) {
		t.Fatalf("empty-SI closed rewriter = %v, want ordinary error", err)
	}
}

func TestSentioRewriter_InsertIntoSITableWithoutLaneIsRejected(t *testing.T) {
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
		Code: pb.RewriteCode_Success, SqlAfterRewrite: `INSERT INTO phys."db1.t" (a) VALUES (1)`, StatementType: pb.StatementType_STATEMENT_TYPE_INSERT,
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", LogicalDatabase: "db1", IsStorageIntegrity: true}},
	})}
	_, err := newSIFactory(be, nil, false).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "INSERT INTO db1.t (a) VALUES (1)", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Message != "storage-integrity table db1.t accepts writes only through the signed statement lane" {
		t.Fatalf("err = %v", err)
	}
	res, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "INSERT INTO db1.t (a) VALUES (1)", "")
	if err != nil || res.StatementType != sqlmeta.StatementTypeInsert {
		t.Fatalf("with the lane enabled the INSERT proceeds to the ingress: %v %v", res, err)
	}
}
