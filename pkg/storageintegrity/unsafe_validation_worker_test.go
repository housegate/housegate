package storageintegrity

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestUnsafeReplicaHashVerifierRejectsDigestMismatch(t *testing.T) {
	verifier := UnsafeReplicaHashVerifier{
		Reader: unsafeDigestReaderFunc(func(_ context.Context, replica UnsafeReplica, _ string) (UnsafeReplicaDigest, error) {
			switch replica.ReplicaID {
			case "r1":
				return UnsafeReplicaDigest{ReplicaID: "r1", RowCount: 2, RowsHash: "0xaaa"}, nil
			case "r2":
				return UnsafeReplicaDigest{ReplicaID: "r2", RowCount: 2, RowsHash: "0xbbb"}, nil
			default:
				t.Fatalf("unexpected replica: %+v", replica)
				return UnsafeReplicaDigest{}, nil
			}
		}),
	}
	_, err := verifier.VerifyUnsafe(context.Background(), UnsafeValidationTask{
		ValidationID: "uv-1",
		StatementID:  "stmt-1",
		TableID:      "dual_hg_auth.t",
		UnsafeTable:  "`hg_unsafe`.`dual_hg_auth.t`",
		Replicas: []UnsafeReplica{
			{ReplicaID: "r1", Addr: "127.0.0.1:9000"},
			{ReplicaID: "r2", Addr: "127.0.0.1:9001"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe replica digest mismatch") {
		t.Fatalf("VerifyUnsafe error = %v, want digest mismatch", err)
	}
}

func TestUnsafeReplicaHashVerifierAcceptsMatchingReplicas(t *testing.T) {
	verifier := UnsafeReplicaHashVerifier{
		Reader: unsafeDigestReaderFunc(func(_ context.Context, replica UnsafeReplica, _ string) (UnsafeReplicaDigest, error) {
			return UnsafeReplicaDigest{ReplicaID: replica.ReplicaID, RowCount: 2, RowsHash: "0xaaa"}, nil
		}),
	}
	result, err := verifier.VerifyUnsafe(context.Background(), UnsafeValidationTask{
		ValidationID: "uv-1",
		StatementID:  "stmt-1",
		TableID:      "dual_hg_auth.t",
		UnsafeTable:  "`hg_unsafe`.`dual_hg_auth.t`",
		Replicas: []UnsafeReplica{
			{ReplicaID: "r1", Addr: "127.0.0.1:9000"},
			{ReplicaID: "r2", Addr: "127.0.0.1:9001"},
		},
	})
	if err != nil {
		t.Fatalf("VerifyUnsafe: %v", err)
	}
	if result.RowCount != 2 || result.RowsHash != "0xaaa" {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Replicas) != 2 {
		t.Fatalf("replica results = %+v", result.Replicas)
	}
}

func TestUnsafeReplicaHashVerifierAcceptsSingleLocalReplica(t *testing.T) {
	verifier := UnsafeReplicaHashVerifier{
		Reader: unsafeDigestReaderFunc(func(_ context.Context, replica UnsafeReplica, _ string) (UnsafeReplicaDigest, error) {
			return UnsafeReplicaDigest{ReplicaID: replica.ReplicaID, RowCount: 2, RowsHash: "0xaaa"}, nil
		}),
	}
	result, err := verifier.VerifyUnsafe(context.Background(), UnsafeValidationTask{
		ValidationID: "uv-local",
		StatementID:  "stmt-1",
		TableID:      "dual_hg_auth.t",
		UnsafeTable:  "`hg_unsafe`.`dual_hg_auth.t`",
		Replicas: []UnsafeReplica{
			{ReplicaID: "hg-1", Addr: "127.0.0.1:9000"},
		},
	})
	if err != nil {
		t.Fatalf("VerifyUnsafe: %v", err)
	}
	if result.RowCount != 2 || result.RowsHash != "0xaaa" || len(result.Replicas) != 1 || result.Replicas[0].ReplicaID != "hg-1" {
		t.Fatalf("result = %+v", result)
	}
}

func TestUnsafeReplicaHashVerifierTimesOutReplicaRead(t *testing.T) {
	verifier := UnsafeReplicaHashVerifier{
		ReplicaTimeout: 10 * time.Millisecond,
		Reader: unsafeDigestReaderFunc(func(ctx context.Context, _ UnsafeReplica, _ string) (UnsafeReplicaDigest, error) {
			<-ctx.Done()
			return UnsafeReplicaDigest{}, ctx.Err()
		}),
	}
	started := time.Now()
	_, err := verifier.VerifyUnsafe(context.Background(), UnsafeValidationTask{
		ValidationID: "uv-timeout",
		StatementID:  "stmt-timeout",
		TableID:      "dual_hg_auth.t",
		UnsafeTable:  "`hg_unsafe`.`dual_hg_auth.t`",
		Replicas: []UnsafeReplica{
			{ReplicaID: "r1", Addr: "127.0.0.1:9000"},
			{ReplicaID: "r2", Addr: "127.0.0.1:9001"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("VerifyUnsafe error = %v, want replica timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("VerifyUnsafe took %s, want timeout to cut off blocked replica read", elapsed)
	}
}

func TestActivePartsDigestQueriesUseCountDigestBeforePartsMetadata(t *testing.T) {
	queries := activePartsDigestQueries("hg_unsafe", "realbin.t")
	if len(queries) < 3 {
		t.Fatalf("activePartsDigestQueries len = %d, want at least 3", len(queries))
	}
	if got, want := queries[0].SQL, "SELECT count() FROM `hg_unsafe`.`realbin.t`"; got != want {
		t.Fatalf("first digest query = %q, want %q", got, want)
	}
	if len(queries[0].Args) != 0 {
		t.Fatalf("first digest args = %#v, want table query without system table args", queries[0].Args)
	}
	if !strings.Contains(queries[1].SQL, "bytes_on_disk") ||
		!strings.Contains(queries[1].SQL, "system.parts") {
		t.Fatalf("second digest query = %q, want lightweight system.parts bytes_on_disk fallback", queries[1].SQL)
	}
	if len(queries[1].Args) != 3 || queries[1].Args[1] != "realbin.t" || queries[1].Args[2] != "realbin%2Et" {
		t.Fatalf("second digest args = %#v, want logical and escaped table names", queries[1].Args)
	}
	if !strings.Contains(queries[2].SQL, "hash_of_all_files") {
		t.Fatalf("third digest query = %q, want file hash fallback", queries[2].SQL)
	}
}

func TestSystemPartsTableNameCandidatesIncludeEscapedDottedName(t *testing.T) {
	candidates := systemPartsTableNameCandidates("realbin.t")
	if len(candidates) != 2 || candidates[0] != "realbin.t" || candidates[1] != "realbin%2Et" {
		t.Fatalf("systemPartsTableNameCandidates = %#v, want logical and escaped names", candidates)
	}
}

func TestSplitClickHouseQualifiedTableName(t *testing.T) {
	tests := []struct {
		name      string
		database  string
		table     string
		wantError bool
	}{
		{name: "`hg_unsafe`.`realbin.t`", database: "hg_unsafe", table: "realbin.t"},
		{name: "hg_unsafe.realbin_t", database: "hg_unsafe", table: "realbin_t"},
		{name: "`hg``unsafe`.`realbin.t`", database: "hg`unsafe", table: "realbin.t"},
		{name: "realbin_t", wantError: true},
		{name: "`hg_unsafe`.`realbin.t", wantError: true},
	}
	for _, tt := range tests {
		database, table, err := splitClickHouseQualifiedTableName(tt.name)
		if tt.wantError {
			if err == nil {
				t.Fatalf("splitClickHouseQualifiedTableName(%q) succeeded, want error", tt.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("splitClickHouseQualifiedTableName(%q): %v", tt.name, err)
		}
		if database != tt.database || table != tt.table {
			t.Fatalf("splitClickHouseQualifiedTableName(%q) = %q/%q, want %q/%q", tt.name, database, table, tt.database, tt.table)
		}
	}
}

type unsafeDigestReaderFunc func(context.Context, UnsafeReplica, string) (UnsafeReplicaDigest, error)

func (f unsafeDigestReaderFunc) ReadUnsafeDigest(ctx context.Context, replica UnsafeReplica, table string) (UnsafeReplicaDigest, error) {
	return f(ctx, replica, table)
}
