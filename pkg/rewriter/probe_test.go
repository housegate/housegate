package rewriter

import (
	"context"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
)

func TestProbeStorageIntegrityBuild(t *testing.T) {
	t.Run("correct build passes", func(t *testing.T) {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:            pb.RewriteCode_Success,
			StatementType:   pb.StatementType_STATEMENT_TYPE_DESCRIBE,
			SqlAfterRewrite: StorageIntegrityProbeExpectedSQL,
		})}
		f := newSIFactory(be, nil, true)
		if err := f.ProbeStorageIntegrityBuild(context.Background()); err != nil {
			t.Fatalf("probe: %v", err)
		}
		si := be.lastReq.GetOptions()[0].GetTableNameArgs().GetDynamicArgs().GetStorageIntegrity()
		if si.GetContractVersion() != StorageIntegrityContractV1 || si.GetTables()["db1.t"].GetSafeTable() != "hg_safe.db1__t" {
			t.Fatalf("probe request did not carry the fixed SI args: %v", si)
		}
	})

	t.Run("old build is refused", func(t *testing.T) {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code:          pb.RewriteCode_Success,
			StatementType: pb.StatementType_STATEMENT_TYPE_DESCRIBE,
			SqlAfterRewrite: "SELECT name, type, default_type, default_expression, comment, " +
				"codec_expression, ttl_expression FROM system.columns WHERE database = 'hg_safe' " +
				"AND table = 'db1__t' AND name != '_hg_row_id' ORDER BY position",
		})}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "storage-integrity engine probe") {
			t.Fatalf("err = %v, want a build-probe refusal", err)
		}
	})

	t.Run("missing acknowledgement is refused", func(t *testing.T) {
		be := &fakeBackend{resp: &pb.RewriteSQLResponse{
			Code: pb.RewriteCode_Success, SqlAfterRewrite: StorageIntegrityProbeExpectedSQL}}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "acknowledgement") {
			t.Fatalf("err = %v, want an acknowledgement refusal", err)
		}
	})

	t.Run("rejected probe is refused", func(t *testing.T) {
		be := &fakeBackend{resp: acknowledgedSIResponse(&pb.RewriteSQLResponse{
			Code: pb.RewriteCode_UnsupportedStatement, Message: "nope"})}
		err := newSIFactory(be, nil, true).ProbeStorageIntegrityBuild(context.Background())
		if err == nil || !strings.Contains(err.Error(), "UnsupportedStatement") {
			t.Fatalf("err = %v, want a rejected-probe refusal", err)
		}
	})
}
