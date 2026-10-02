package rewriter

import (
	"context"
	"slices"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

func TestProtectedDatabases(t *testing.T) {
	for _, tc := range []struct {
		physical string
		want     []string
	}{
		{"phys", []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"}},
		{"", []string{"hg_safe", "hg_unsafe", "hg_promote"}},
		{"hg_safe", []string{"hg_safe", "hg_unsafe", "hg_promote"}},
	} {
		if got := protectedDatabases(tc.physical); !slices.Equal(got, tc.want) {
			t.Errorf("protectedDatabases(%q) = %v, want %v", tc.physical, got, tc.want)
		}
	}
}

// TestRewriteSendsProtectedDatabases is spec 2026-09-26 T1/T3: the protected
// namespace rides on every request whatever storage_integrity says, including
// the error-message reverse map.
func TestRewriteSendsProtectedDatabases(t *testing.T) {
	be := &fakeBackend{resp: &pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: "SELECT 1", StatementType: pb.StatementType_STATEMENT_TYPE_SELECT}}
	rw := newFakeFactory(be).NewRewriter(&fakeSession{logical: "db1"})
	if _, err := rw.Rewrite(context.Background(), "SELECT 1", ""); err != nil {
		t.Fatal(err)
	}
	want := []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"}
	if got := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetProtectedDatabases(); !slices.Equal(got, want) {
		t.Fatalf("Rewrite protected_databases = %v, want %v", got, want)
	}
	if _, err := rw.RewriteErrorMessage(context.Background(), "Table phys.x does not exist"); err != nil {
		t.Fatal(err)
	}
	if got := be.lastErrReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetProtectedDatabases(); !slices.Equal(got, want) {
		t.Fatalf("RewriteErrorMessage protected_databases = %v, want %v", got, want)
	}
}
