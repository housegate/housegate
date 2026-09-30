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

	analyzeFn       func(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error)
	prepareFn       func(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error)
	analyzeRequests []*pb.AnalyzeSnapshotQueryRequest
	prepareRequests []*pb.PrepareSnapshotQueryRequest
	closeFn         func() error
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

func (f *fakeBackend) AnalyzeSnapshotQuery(ctx context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	f.analyzeRequests = append(f.analyzeRequests, req)
	if f.analyzeFn == nil {
		return nil, nil
	}
	return f.analyzeFn(ctx, req)
}

func (f *fakeBackend) PrepareSnapshotQuery(ctx context.Context, req *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
	f.prepareRequests = append(f.prepareRequests, req)
	if f.prepareFn == nil {
		return nil, nil
	}
	return f.prepareFn(ctx, req)
}

func (f *fakeBackend) Close() error {
	if f.closeFn != nil {
		return f.closeFn()
	}
	return nil
}

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

// TestSentioRewriter_UnsupportedIsRejected pins spec 2026-09-26 T8: with
// storage integrity disabled an UnsupportedStatement answer is a rejection,
// not a pass-through. There is no way to tell a harmless unmodelled statement
// from DETACH TABLE of another tenant's physical table.
func TestSentioRewriter_UnsupportedIsRejected(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, Message: "statement is not supported"}}
	res, err := newFakeFactory(be).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "DETACH TABLE phys.`db2.x`", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_UnsupportedStatement || rej.Message != "statement is not supported" {
		t.Fatalf("err = %v, want RejectedError(UnsupportedStatement)", err)
	}
	if res.SQL != "" {
		t.Fatalf("SQL = %q, want the zero result on rejection", res.SQL)
	}
}

// TestSentioRewriter_EveryNonSuccessCodeIsRejectedWithoutSI covers each
// non-Success code with storage integrity disabled: the answer is a
// RejectedError carrying the engine's code and message verbatim.
func TestSentioRewriter_EveryNonSuccessCodeIsRejectedWithoutSI(t *testing.T) {
	for _, code := range []pb.RewriteCode{
		pb.RewriteCode_UnsupportedStatement,
		pb.RewriteCode_SyntaxError,
		pb.RewriteCode_InvalidRewriteRequest,
		pb.RewriteCode_RewriteError,
		pb.RewriteCode(99), // a future code
	} {
		be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: code, Message: "engine says no"}}
		_, err := newFakeFactory(be).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
		var rej *RejectedError
		if !errors.As(err, &rej) || rej.Code != code || rej.Message != "engine says no" {
			t.Fatalf("code %s: err = %v, want RejectedError carrying the engine answer", code, err)
		}
	}
}

func TestSentioRewriter_InvalidRequestIsRejectedWithoutSI(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_InvalidRewriteRequest, Message: "protected database phys is not addressable"}}
	_, err := newFakeFactory(be).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "USE phys", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Message != "protected database phys is not addressable" {
		t.Fatalf("err = %v", err)
	}
}

// TestSentioRewriter_TransportFailureFollowsTheSwitch pins the only remaining
// fail-open (spec 2026-09-26 T8, review M1): a failure before the request
// reached the engine (a backend *UnavailableError, or a closed rewriter)
// returns that *UnavailableError, which the plugin forwards past, only when
// fail_open_on_unavailable is set and storage integrity is disabled.
func TestSentioRewriter_TransportFailureFollowsTheSwitch(t *testing.T) {
	outages := map[string]func() (*SentioNetworkFactory, Rewriter){
		"connect failure": func() (*SentioNetworkFactory, Rewriter) {
			f := newFakeFactory(&fakeBackend{err: &UnavailableError{Cause: errors.New("dial tcp 127.0.0.1:50051: connection refused")}})
			return f, nil
		},
		"closed rewriter": func() (*SentioNetworkFactory, Rewriter) {
			f := newFakeFactory(&fakeBackend{})
			rw := f.NewRewriter(&fakeSession{})
			_ = rw.Close()
			return f, rw
		},
	}
	for name, mk := range outages {
		for _, tc := range []struct {
			name     string
			failOpen bool
			si       bool
			wantRej  bool
		}{
			{"default fails closed", false, false, true},
			{"switch on fails open", true, false, false},
			{"switch on with SI still fails closed", true, true, true},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				f, rw := mk()
				f.options.FailOpenOnUnavailable = tc.failOpen
				if tc.si {
					f.options.StorageIntegrity = siOpts(nil)
				}
				if rw == nil {
					rw = f.NewRewriter(&fakeSession{})
				}
				_, err := rw.Rewrite(context.Background(), "SELECT 1", "")
				if err == nil {
					t.Fatal("an outage must always return an error")
				}
				var rej *RejectedError
				if got := errors.As(err, &rej); got != tc.wantRej {
					t.Fatalf("RejectedError = %v, want %v (err=%v)", got, tc.wantRej, err)
				}
				if tc.wantRej && rej.Code != pb.RewriteCode_RewriteError {
					t.Fatalf("code = %s, want RewriteError", rej.Code)
				}
				var unavailable *UnavailableError
				if !tc.wantRej && !errors.As(err, &unavailable) {
					t.Fatalf("fail-open error = %v, want an *UnavailableError", err)
				}
			})
		}
	}
}

// TestSentioRewriter_FailureAfterReceiptIsRejectedEvenUnderTheSwitch pins
// review M1: an error the backend returns after the request reached the
// engine (a native handler error, an engine crash or internal error, a nil
// response) is statement-dependent, so it is a rejection whatever
// fail_open_on_unavailable says.
func TestSentioRewriter_FailureAfterReceiptIsRejectedEvenUnderTheSwitch(t *testing.T) {
	for name, be := range map[string]*fakeBackend{
		"plain backend error": {err: errors.New("engine: generate: unexpected node")},
		"nil response":        {},
	} {
		for _, failOpen := range []bool{false, true} {
			f := newFakeFactory(be)
			f.options.FailOpenOnUnavailable = failOpen
			_, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
			var rej *RejectedError
			if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_RewriteError {
				t.Fatalf("%s switch=%v: err = %v, want RejectedError(RewriteError)", name, failOpen, err)
			}
		}
	}
}

// TestSentioRewriter_FailureMessagesAreGeneric pins review L4: the message
// that reaches the client names no address or transport detail; the cause
// stays on the error for the server log.
func TestSentioRewriter_FailureMessagesAreGeneric(t *testing.T) {
	const detail = "dial tcp 10.1.2.3:50051: connection refused"
	for name, be := range map[string]*fakeBackend{
		"before receipt": {err: &UnavailableError{Cause: errors.New(detail)}},
		"after receipt":  {err: errors.New(detail)},
	} {
		for _, si := range []bool{false, true} {
			f := newFakeFactory(be)
			if si {
				f.options.StorageIntegrity = siOpts(nil)
			}
			_, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
			var rej *RejectedError
			if !errors.As(err, &rej) {
				t.Fatalf("%s si=%v: err = %v, want RejectedError", name, si, err)
			}
			if strings.Contains(rej.Error(), "10.1.2.3") || strings.Contains(rej.Error(), "dial") {
				t.Fatalf("%s si=%v: client message %q leaks transport detail", name, si, rej.Error())
			}
			if rej.Cause == nil || !strings.Contains(rej.Cause.Error(), "10.1.2.3") {
				t.Fatalf("%s si=%v: cause %v, want the detail kept for the log", name, si, rej.Cause)
			}
		}
	}
}

// TestSentioRewriter_TransportFailurePreservesCauseWithoutSI keeps the
// backend error reachable through errors.Is on the fail-closed path.
func TestSentioRewriter_TransportFailurePreservesCauseWithoutSI(t *testing.T) {
	backendErr := errors.New("backend unavailable")
	_, err := newFakeFactory(&fakeBackend{err: &UnavailableError{Cause: backendErr}}).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || !errors.Is(err, backendErr) || rej.Message != "rewriter unavailable" {
		t.Fatalf("err = %v, want RejectedError(rewriter unavailable) wrapping the cause", err)
	}
}

// TestSentioRewriter_SuccessWithEmptySQLIsRejected pins review L3: a
// Success answer without SQL would forward an empty statement while the
// classification drives downstream policy; it is a rejection.
func TestSentioRewriter_SuccessWithEmptySQLIsRejected(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, StatementType: pb.StatementType_STATEMENT_TYPE_SELECT}}
	_, err := newFakeFactory(be).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_RewriteError {
		t.Fatalf("err = %v, want RejectedError(RewriteError)", err)
	}
}

// TestSentioRewriter_NoPhysicalDatabaseStillConsultsTheEngine pins review
// L2: with a rewriter configured, a session with no database and no
// physical_database is still rewritten, so an engine refusal is enforced
// instead of the original SQL being forwarded unexamined.
func TestSentioRewriter_NoPhysicalDatabaseStillConsultsTheEngine(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, Message: "statement is not supported"}}
	f := newFakeFactory(be)
	f.options.PhysicalDatabase = ""
	_, err := f.NewRewriter(&fakeSession{}).Rewrite(context.Background(), "DETACH TABLE otherdb.t", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_UnsupportedStatement {
		t.Fatalf("err = %v, want the engine refusal", err)
	}
	if be.lastReq.GetSql() != "DETACH TABLE otherdb.t" {
		t.Fatalf("engine saw %q, want the statement", be.lastReq.GetSql())
	}
}

// TestSentioRewriter_UnsupportedDoesNotLeakBestEffortFields: an
// UnsupportedStatement answer used to forward the original SQL with the
// engine's best-effort classification. It is now a rejection, so the result
// is the zero value and nothing downstream reads a partial classification.
func TestSentioRewriter_UnsupportedDoesNotLeakBestEffortFields(t *testing.T) {
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
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want RejectedError", err)
	}
	if res.AccessedTables != nil || res.ExistenceClause != sqlmeta.ExistenceClause(pb.ExistenceClause_EXISTENCE_CLAUSE_UNSPECIFIED) {
		t.Fatalf("result = %+v, want the zero value", res)
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
	resp.StorageIntegrityContractVersion = StorageIntegrityContractV2
	return resp
}

func newSIFactory(be backend, rs StorageIntegrityReadState, insertLane bool) *SentioNetworkFactory {
	f := newFakeFactory(be)
	f.options.StorageIntegrity = siOpts(rs)
	f.options.StorageIntegrity.InsertLaneEnabled = insertLane
	return f
}

func TestSentioRewriter_ShipsStorageIntegrityArgs(t *testing.T) {
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "x", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT,
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", LogicalDatabase: "db1", IsStorageIntegrity: true}}})}
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}}}
	rw := newSIFactory(be, rs, true).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(context.Background(), "SELECT a FROM db1.t", ""); err != nil {
		t.Fatal(err)
	}
	si := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
	if si.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE || si.GetContractVersion() != StorageIntegrityContractV2 || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
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
	// With SI configured, even an Unsupported response naming an ordinary
	// table is refused: a non-Success answer cannot prove the statement is safe.
	be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_UnsupportedStatement, Message: "nope",
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "other", OriginalTable: "u"}}})}
	_, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "OPTIMIZE TABLE other.u", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_UnsupportedStatement {
		t.Fatalf("configured-SI unsupported response = %v, want RejectedError", err)
	}
}

func TestSentioRewriter_FailsClosedOnAnyNonSuccessWhenSIConfigured(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     pb.RewriteCode
		accessed []*pb.AccessedTable
	}{
		{"unsupported with no accessed table", pb.RewriteCode_UnsupportedStatement, nil},
		{"rewrite error with no accessed table", pb.RewriteCode_RewriteError, nil},
		{"syntax error with no accessed table", pb.RewriteCode_SyntaxError, nil},
		{"invalid request with no accessed table", pb.RewriteCode_InvalidRewriteRequest, nil},
		{"unsupported naming an ordinary table", pb.RewriteCode_UnsupportedStatement,
			[]*pb.AccessedTable{{OriginalDatabase: "other", OriginalTable: "u"}}},
		{"unsupported naming an SI table", pb.RewriteCode_UnsupportedStatement,
			[]*pb.AccessedTable{{OriginalDatabase: "db1", OriginalTable: "t", IsStorageIntegrity: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
				Code:                   tc.code,
				Message:                "storage-integrity physical database hg_safe is not directly addressable",
				SqlAfterRewrite:        "TRUNCATE DATABASE hg_safe",
				OriginalAccessedTables: tc.accessed,
			})}
			_, err := newSIFactory(be, nil, true).NewRewriter(&fakeSession{}).
				Rewrite(context.Background(), "TRUNCATE DATABASE hg_safe", "")
			var rej *RejectedError
			if !errors.As(err, &rej) {
				t.Fatalf("err = %v, want *RejectedError", err)
			}
			if rej.Code != tc.code {
				t.Fatalf("rejected code = %v, want %v", rej.Code, tc.code)
			}
			if !strings.Contains(rej.Message, "hg_safe is not directly addressable") {
				t.Fatalf("rejected message = %q, want the engine message", rej.Message)
			}
		})
	}
}

// TestSentioRewriter_EmptySIRejectsUnsupported flips the former empty-SI
// pass-through pin (spec 2026-09-26 T8): OPTIMIZE TABLE of another tenant's
// table is no longer forwarded when storage integrity is disabled.
func TestSentioRewriter_EmptySIRejectsUnsupported(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{
		Code: pb.RewriteCode_UnsupportedStatement, Message: "nope",
		OriginalAccessedTables: []*pb.AccessedTable{{OriginalDatabase: "other", OriginalTable: "u"}}}}
	_, err := newFakeFactory(be).NewRewriter(&fakeSession{}).
		Rewrite(context.Background(), "OPTIMIZE TABLE other.u", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_UnsupportedStatement {
		t.Fatalf("err = %v, want RejectedError(UnsupportedStatement)", err)
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
	if err != nil || res.StorageIntegrityContractVersion != StorageIntegrityContractV2 {
		t.Fatalf("acknowledged non-SI query = %+v, %v", res, err)
	}
}

func TestSentioRewriter_ConfiguredSISurfaceUnavailableFailsClosed(t *testing.T) {
	for name, be := range map[string]*fakeBackend{
		"transport":     {err: &UnavailableError{Cause: errors.New("transport down")}},
		"after receipt": {err: errors.New("engine failed")},
		"nil response":  {},
	} {
		for _, insertLane := range []bool{false, true} {
			_, err := newSIFactory(be, nil, insertLane).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "INSERT INTO db1.t FORMAT Native", "")
			var rej *RejectedError
			if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_RewriteError || !strings.Contains(rej.Message, "classification unavailable") {
				t.Fatalf("%s insertLane=%v err=%v, want fail-closed RejectedError", name, insertLane, err)
			}
		}
	}
	// Without storage integrity the identical outage retains an ordinary
	// error only under fail_open_on_unavailable; the plugin then forwards.
	be := &fakeBackend{err: &UnavailableError{Cause: errors.New("transport down")}}
	openFactory := newFakeFactory(be)
	openFactory.options.FailOpenOnUnavailable = true
	_, err := openFactory.NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if err == nil || errors.As(err, &rej) {
		t.Fatalf("empty-SI backend error under the switch = %v, want ordinary error", err)
	}

	siClosed := newSIFactory(&fakeBackend{}, nil, false).NewRewriter(&fakeSession{})
	if err := siClosed.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = siClosed.Rewrite(context.Background(), "INSERT INTO db1.t FORMAT Native", "")
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "classification unavailable") {
		t.Fatalf("configured-SI closed rewriter = %v, want RejectedError", err)
	}
	openClosedFactory := newFakeFactory(&fakeBackend{})
	openClosedFactory.options.FailOpenOnUnavailable = true
	emptyClosed := openClosedFactory.NewRewriter(&fakeSession{})
	if err := emptyClosed.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = emptyClosed.Rewrite(context.Background(), "SELECT 1", "")
	if err == nil || errors.As(err, &rej) {
		t.Fatalf("empty-SI closed rewriter = %v, want ordinary error", err)
	}
}

func TestSentioRewriter_ConfiguredSIFailurePreservesBackendCause(t *testing.T) {
	backendErr := errors.New("backend unavailable")
	rw := newSIFactory(&fakeBackend{err: backendErr}, nil, true).NewRewriter(&fakeSession{})
	_, err := rw.Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want fail-closed RejectedError", err)
	}
	if !errors.Is(err, backendErr) {
		t.Fatalf("SI rejection must preserve backend cause: %v", err)
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
