package testenv

import (
	"context"
	"testing"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/rewriter"
)

// TestRewriterMock_AnswersTheStartupProbeWithoutRecordingIt pins Review
// Focus 7 of the table-reference plan: a library-built gRPC factory pointed
// at the mock passes the real startup policy probe, and the probe neither
// shows up in SeenSQL / SeenDynamicArgs nor consumes an armed FailNext.
func TestRewriterMock_AnswersTheStartupProbeWithoutRecordingIt(t *testing.T) {
	m := StartRewriterMock(t)
	m.FailNext(1)

	f, err := rewriter.NewSentioNetworkFactory(rewriter.Options{
		Engine:           rewriter.EngineGRPC,
		ServiceAddr:      m.Addr(),
		PhysicalDatabase: "phys",
		Timeout:          5 * time.Second,
	}, network.NewInMemoryNetworkState())
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory: %v", err)
	}
	defer f.Close()

	if err := f.ProbeTableReferencePolicy(context.Background()); err != nil {
		t.Fatalf("the mock must answer the startup probe exactly: %v", err)
	}
	if seen := m.SeenSQL(); len(seen) != 0 {
		t.Fatalf("SeenSQL = %q, want the probe kept out", seen)
	}
	if args := m.SeenDynamicArgs(); len(args) != 0 {
		t.Fatalf("SeenDynamicArgs has %d entries, want the probe kept out", len(args))
	}

	// FailNext is still armed for the first session query, and a probe SQL
	// sent with session-shaped args is ordinary traffic: recorded and
	// answered by the mock's own rules.
	resp, err := m.Rewrite(context.Background(), &pb.RewriteSQLRequest{Sql: "USE phys"})
	if err != nil || resp.GetCode() != pb.RewriteCode_RewriteError {
		t.Fatalf("first session query = %v, %v; want the armed FailNext", resp, err)
	}
	if seen := m.SeenSQL(); len(seen) != 1 || seen[0] != "USE phys" {
		t.Fatalf("SeenSQL = %q, want the session query recorded", seen)
	}
}

// TestRewriterMock_MapDatabase pins the textual qualifier swap: only an
// unquoted `<logical>.` at an identifier boundary is replaced, and the
// AccessedTables prefix still matches the SQL as received.
func TestRewriterMock_MapDatabase(t *testing.T) {
	m := StartRewriterMock(t)
	m.MapDatabase("tenant", "phys")
	m.SetAccessedTables("INSERT INTO tenant.t", []*pb.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "t"}})
	resp, err := m.Rewrite(context.Background(), &pb.RewriteSQLRequest{
		Sql: "INSERT INTO tenant.t SELECT * FROM tenant.u, xtenant.v, tenant_2.w WHERE tenant = 1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "INSERT INTO phys.t SELECT * FROM phys.u, xtenant.v, tenant_2.w WHERE tenant = 1"; resp.GetSqlAfterRewrite() != want {
		t.Fatalf("SqlAfterRewrite = %q, want %q", resp.GetSqlAfterRewrite(), want)
	}
	if got := resp.GetOriginalAccessedTables(); len(got) != 1 || got[0].GetOriginalDatabase() != "tenant" {
		t.Fatalf("OriginalAccessedTables = %v, want the prefix matched on the received SQL", got)
	}
}
