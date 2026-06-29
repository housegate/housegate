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
		UnsafeTable:  "`hg_unsafe`.`dual_hg_auth.t_a`",
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
		UnsafeTable:  "`hg_unsafe`.`dual_hg_auth.t_a`",
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
		UnsafeTable:  "`hg_unsafe`.`dual_hg_auth.t_a`",
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

type unsafeDigestReaderFunc func(context.Context, UnsafeReplica, string) (UnsafeReplicaDigest, error)

func (f unsafeDigestReaderFunc) ReadUnsafeDigest(ctx context.Context, replica UnsafeReplica, table string) (UnsafeReplicaDigest, error) {
	return f(ctx, replica, table)
}
