package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/sitable"
)

// sequenceBackend answers successive Rewrite calls from a script and records
// every request, so a test can assert on each pass of one query.
type sequenceBackend struct {
	fakeBackend
	script   []*pb.RewriteSQLResponse
	requests []*pb.RewriteSQLRequest
}

func (b *sequenceBackend) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	b.requests = append(b.requests, req)
	if len(b.requests) > len(b.script) {
		return nil, errors.New("unexpected extra rewrite pass")
	}
	return b.script[len(b.requests)-1], nil
}

func siArgsOf(req *pb.RewriteSQLRequest) *pb.StorageIntegrityArgs {
	return req.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
}

func siAccessed(ids ...string) []*pb.AccessedTable {
	var out []*pb.AccessedTable
	for _, id := range ids {
		db, table, _ := strings.Cut(id, ".")
		out = append(out, &pb.AccessedTable{OriginalDatabase: db, OriginalTable: table, LogicalDatabase: db, IsStorageIntegrity: true})
	}
	return out
}

func dynamicSIFactory(be backend, state sitable.TableState, rs StorageIntegrityReadState) *SentioNetworkFactory {
	f := newFakeFactory(be)
	f.options.StorageIntegrity = StorageIntegrityOptions{Enabled: true, TableState: state, ReadState: rs, InsertLaneEnabled: true}
	return f
}

func TestSentioRewriter_SendsV2WithEmptyTableMap(t *testing.T) {
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{acknowledgedSIResponse(&pb.RewriteSQLResponse{
		Code: pb.RewriteCode_UnsupportedStatement, Message: "storage-integrity is configured; statement class is not modelled by the rewriter and cannot be forwarded"})}}
	rw := dynamicSIFactory(be, sitable.NewFake(sitable.Pending), nil).NewRewriter(&fakeSession{})
	_, err := rw.Rewrite(context.Background(), "SET max_threads = 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("err = %v, want the catch-all refusal", err)
	}
	si := siArgsOf(be.requests[0])
	if si == nil || si.GetContractVersion() != StorageIntegrityContractV2 || len(si.GetTables()) != 0 {
		t.Fatalf("args = %v, want V2 with an empty table map", si)
	}
	if strings.Join(si.GetReservedDatabases(), ",") != "hg_safe,hg_unsafe,hg_promote" {
		t.Fatalf("reserved databases = %v, want them on a request with no Active table", si.GetReservedDatabases())
	}
}

func TestSentioRewriter_UsesTheContextSnapshot(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	pinned := fake.Current()
	fake.Set() // the table leaves the Active set after the query took its snapshot
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "x"})}}
	rw := dynamicSIFactory(be, fake, nil).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(WithTableSnapshot(context.Background(), pinned), "SELECT a FROM db1.t", ""); err != nil {
		t.Fatal(err)
	}
	if _, ok := siArgsOf(be.requests[0]).GetTables()["db1.t"]; !ok {
		t.Fatalf("args = %v, want the query snapshot's Active set, not Current()", siArgsOf(be.requests[0]))
	}
}

func TestSentioRewriter_UnsafeLatestFetchesPartsForAccessedTablesOnly(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "db1.t", Status: sitable.Active},
		sitable.Table{ID: "db1.u", Status: sitable.Active},
	)
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}, "db1.u": {"all_9_9_0"}}}
	first := acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass1", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT, OriginalAccessedTables: siAccessed("db1.t")})
	second := acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass2", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT, OriginalAccessedTables: siAccessed("db1.t")})
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{first, second}}
	rw := dynamicSIFactory(be, fake, rs).NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT a FROM db1.t", "")
	if err != nil {
		t.Fatal(err)
	}
	if res.SQL != "pass2" {
		t.Fatalf("SQL = %q, want the second pass", res.SQL)
	}
	if strings.Join(rs.calls, ",") != "db1.t" {
		t.Fatalf("port calls = %v, want only the accessed db1.t", rs.calls)
	}
	if got := siArgsOf(be.requests[0]).GetTables()["db1.t"].GetExcludedUnsafeParts(); len(got) != 0 {
		t.Fatalf("classification pass excluded %v, want none", got)
	}
	tables := siArgsOf(be.requests[1]).GetTables()
	if got := tables["db1.t"].GetExcludedUnsafeParts(); len(got) != 1 || got[0] != "all_1_1_0" {
		t.Fatalf("rewrite pass excluded %v for db1.t", got)
	}
	if got := tables["db1.u"].GetExcludedUnsafeParts(); len(got) != 0 {
		t.Fatalf("an unaccessed Active table must carry no parts, got %v", got)
	}
}

func TestSentioRewriter_UnsafeLatestWithoutPromotedPartsIsOnePass(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass1", OriginalAccessedTables: siAccessed("db1.t")})}}
	rw := dynamicSIFactory(be, fake, &fakeReadState{}).NewRewriter(&fakeSession{})
	if _, err := rw.Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT a FROM db1.t", ""); err != nil {
		t.Fatal(err)
	}
	if len(be.requests) != 1 {
		t.Fatalf("passes = %d, want 1 when no accessed table has promoted parts", len(be.requests))
	}
}

func TestSentioRewriter_UnsafeLatestRejectsAMovingAccessedSet(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active}, sitable.Table{ID: "db1.u", Status: sitable.Active})
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}}}
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{
		acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass1", OriginalAccessedTables: siAccessed("db1.t")}),
		acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass2", OriginalAccessedTables: siAccessed("db1.t", "db1.u")}),
	}}
	_, err := dynamicSIFactory(be, fake, rs).NewRewriter(&fakeSession{}).Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT a FROM db1.t", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "unsafe_latest rewrite accessed") {
		t.Fatalf("err = %v, want a RejectedError for the moved accessed set", err)
	}
}

func TestSentioRewriter_RequiresV2Acknowledgement(t *testing.T) {
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1", StorageIntegrityContractVersion: StorageIntegrityContractV1}}}
	_, err := dynamicSIFactory(be, sitable.NewFake(sitable.Pending), nil).NewRewriter(&fakeSession{}).Rewrite(context.Background(), "SELECT 1", "")
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "contract acknowledgement") {
		t.Fatalf("a V1 acknowledgement must fail closed, got %v", err)
	}
}

// unsafeLatestRewrite runs one unsafe_latest query against a scripted backend
// over db1.t (Active, with a promoted part).
func unsafeLatestRewrite(t *testing.T, active []sitable.Table, script ...*pb.RewriteSQLResponse) (*sequenceBackend, *fakeReadState, error) {
	t.Helper()
	fake := sitable.NewFake(sitable.Ordinary, active...)
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}, "db1.x": {"all_7_7_0"}, "db1.t,db1.u": {"all_8_8_0"}}}
	be := &sequenceBackend{script: script}
	_, err := dynamicSIFactory(be, fake, rs).NewRewriter(&fakeSession{}).Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT a FROM db1.t", "")
	return be, rs, err
}

// TestSentioRewriter_UnsafeLatestRejectsAnAccessedIDOutsideTheActiveSet pins
// final ruling I2: the exclusions are keyed by the snapshot's Active ids, so an
// SI-flagged accessed id that is not one of them would have its promoted parts
// silently dropped from the second pass. It is refused instead, before the
// promotion port is consulted.
func TestSentioRewriter_UnsafeLatestRejectsAnAccessedIDOutsideTheActiveSet(t *testing.T) {
	for name, accessed := range map[string][]*pb.AccessedTable{
		"not active":        siAccessed("db1.x"),
		"no database":       {{OriginalTable: "t", IsStorageIntegrity: true}},
		"one of two absent": siAccessed("db1.t", "db1.x"),
	} {
		t.Run(name, func(t *testing.T) {
			be, rs, err := unsafeLatestRewrite(t, []sitable.Table{{ID: "db1.t", Status: sitable.Active}},
				acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass1", OriginalAccessedTables: accessed}),
				acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass2", OriginalAccessedTables: accessed}),
			)
			var rej *RejectedError
			if !errors.As(err, &rej) || !strings.Contains(rej.Message, "not an Active storage-integrity table") {
				t.Fatalf("err = %v, want a RejectedError for the unknown accessed id", err)
			}
			if len(be.requests) != 1 || len(rs.calls) != 0 {
				t.Fatalf("passes = %d port calls = %v, want the refusal before the port and the second pass", len(be.requests), rs.calls)
			}
		})
	}
}

// TestSentioRewriter_UnsafeLatestComparesAccessedSetsExactly: the accessed-set
// comparison is element-wise, so ids that join to the same string still differ.
func TestSentioRewriter_UnsafeLatestComparesAccessedSetsExactly(t *testing.T) {
	active := []sitable.Table{{ID: "db1.t,db1.u", Status: sitable.Active}, {ID: "db1.t", Status: sitable.Active}, {ID: "db1.u", Status: sitable.Active}}
	be, _, err := unsafeLatestRewrite(t, active,
		acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass1", OriginalAccessedTables: siAccessed("db1.t,db1.u")}),
		acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass2", OriginalAccessedTables: siAccessed("db1.t", "db1.u")}),
	)
	if len(be.requests) != 2 {
		t.Fatalf("passes = %d, want the comparison to run after the second pass", len(be.requests))
	}
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "unsafe_latest rewrite accessed") {
		t.Fatalf("err = %v, want a RejectedError for the moved accessed set", err)
	}
}

// TestSentioRewriter_UnsafeLatestSecondPassFailsClosed: the second pass is held
// to the same acknowledgement and code rules as the first.
func TestSentioRewriter_UnsafeLatestSecondPassFailsClosed(t *testing.T) {
	active := []sitable.Table{{ID: "db1.t", Status: sitable.Active}}
	first := func() *pb.RewriteSQLResponse {
		return acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass1", OriginalAccessedTables: siAccessed("db1.t")})
	}
	for name, tc := range map[string]struct {
		second *pb.RewriteSQLResponse
		want   string
	}{
		"V1 acknowledgement": {
			&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "pass2", OriginalAccessedTables: siAccessed("db1.t"), StorageIntegrityContractVersion: StorageIntegrityContractV1},
			"contract acknowledgement unavailable",
		},
		"non-Success code": {
			acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_RewriteError, Message: "engine refused the exclusions", OriginalAccessedTables: siAccessed("db1.t")}),
			"engine refused the exclusions",
		},
	} {
		t.Run(name, func(t *testing.T) {
			be, _, err := unsafeLatestRewrite(t, active, first(), tc.second)
			var rej *RejectedError
			if !errors.As(err, &rej) || !strings.Contains(rej.Message, tc.want) {
				t.Fatalf("err = %v, want a RejectedError containing %q", err, tc.want)
			}
			if len(be.requests) != 2 {
				t.Fatalf("passes = %d, want the second pass to have run", len(be.requests))
			}
		})
	}
}
