package testenv

import (
	"context"
	"testing"

	pb "github.com/housegate/rewriter-go/gen/pb"
)

func TestRewriterMockRewritesInsertWithDynamicDatabaseMap(t *testing.T) {
	m := &RewriterMock{}
	resp, err := m.Rewrite(context.Background(), &pb.RewriteSQLRequest{
		Sql: "INSERT INTO tenant_a.events FORMAT Native",
		Options: []*pb.RewriteOption{{
			Op: pb.RewriteOp_TableNameRewrite,
			Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{
				DynamicArgs: &pb.RewriteTableDynamicArgs{
					DatabaseMap: map[string]string{
						"tenant_a": "hg_unsafe",
					},
					UpstreamLogicalDatabaseInContext: "tenant_a",
				},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if resp.GetCode() != pb.RewriteCode_Success {
		t.Fatalf("code = %s message=%q", resp.GetCode(), resp.GetMessage())
	}
	if got, want := resp.GetSqlAfterRewrite(), "INSERT INTO hg_unsafe.events FORMAT Native"; got != want {
		t.Fatalf("sql_after_rewrite = %q, want %q", got, want)
	}
	tables := resp.GetOriginalAccessedTables()
	if len(tables) != 1 {
		t.Fatalf("accessed tables = %d, want 1", len(tables))
	}
	if got := tables[0].GetLogicalDatabase(); got != "tenant_a" {
		t.Fatalf("logical database = %q", got)
	}
	if got := tables[0].GetPhysicalDatabase(); got != "hg_unsafe" {
		t.Fatalf("physical database = %q", got)
	}
	if got := resp.GetTableRewrites()["tenant_a.events"]; got != "hg_unsafe.events" {
		t.Fatalf("table rewrite = %q", got)
	}
}
