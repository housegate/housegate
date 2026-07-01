package rewriter

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/housegate/rewriter-go/gen/pb"
)

// newTestMaterializer builds a sentioMaterializer for tests. timeout is
// normally 0 (no per-call deadline); pass a non-zero value to exercise
// the deadline-application path.
func newTestMaterializer(be backend, poolSize int, profileID string, timeout time.Duration) *sentioMaterializer {
	return &sentioMaterializer{be: be, poolSize: poolSize, profileID: profileID, timeout: timeout}
}

func TestMaterialize_SuccessWithReplacements(t *testing.T) {
	fb := &fakeBackend{matResp: &pb.MaterializeSQLResponse{
		Code:                    pb.MaterializeCode_MaterializeSuccess,
		SqlAfterMaterialization: "INSERT INTO t VALUES ('2026-07-01 00:00:00')",
		Replacements:            []*pb.MaterializedReplacement{{FunctionName: "now"}},
	}}
	m := newTestMaterializer(fb, 8, "", 0)
	out, err := m.Materialize(context.Background(), "INSERT INTO t VALUES (now())")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !out.Changed || out.SQL != "INSERT INTO t VALUES ('2026-07-01 00:00:00')" {
		t.Fatalf("want changed materialized SQL, got %+v", out)
	}
	// Inputs must be populated so the engine can resolve now()/rand()/uuid.
	if fb.lastMatReq.GetInputs().GetNowUnixNs() == 0 {
		t.Fatalf("now_unix_ns not set")
	}
	if got := len(fb.lastMatReq.GetInputs().GetRandomUint64Values()); got != 8 {
		t.Fatalf("random_uint64 pool = %d, want 8", got)
	}
	if got := len(fb.lastMatReq.GetInputs().GetRandomFloat64Values()); got != 8 {
		t.Fatalf("random_float64 pool = %d, want 8", got)
	}
	if got := len(fb.lastMatReq.GetInputs().GetUuidValues()); got != 8 {
		t.Fatalf("uuid pool = %d, want 8", got)
	}
}

func TestMaterialize_SuccessNoReplacements(t *testing.T) {
	fb := &fakeBackend{matResp: &pb.MaterializeSQLResponse{
		Code:                    pb.MaterializeCode_MaterializeSuccess,
		SqlAfterMaterialization: "SELECT 1",
	}}
	m := newTestMaterializer(fb, 4, "", 0)
	out, err := m.Materialize(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out.Changed {
		t.Fatalf("no replacements → Changed must be false, got %+v", out)
	}
	if out.SQL != "SELECT 1" {
		t.Fatalf("SQL should be original, got %q", out.SQL)
	}
}

func TestMaterialize_NonSuccessKeepsOriginal(t *testing.T) {
	fb := &fakeBackend{matResp: &pb.MaterializeSQLResponse{
		Code:    pb.MaterializeCode_MaterializeSyntaxError,
		Message: "parse error",
	}}
	m := newTestMaterializer(fb, 4, "", 0)
	out, err := m.Materialize(context.Background(), "NOT SQL")
	if err != nil {
		t.Fatalf("non-success must not be an error (fail-open), got %v", err)
	}
	if out.Changed || out.SQL != "NOT SQL" {
		t.Fatalf("want original SQL unchanged, got %+v", out)
	}
	if out.Code != pb.MaterializeCode_MaterializeSyntaxError {
		t.Fatalf("code not propagated: %v", out.Code)
	}
}

func TestMaterialize_TransportErrorPropagates(t *testing.T) {
	fb := &fakeBackend{matErr: errors.New("grpc down")}
	m := newTestMaterializer(fb, 4, "", 0)
	out, err := m.Materialize(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatalf("transport error must propagate so the caller falls open")
	}
	if out.SQL != "SELECT 1" {
		t.Fatalf("outcome SQL should be original on transport error, got %q", out.SQL)
	}
}

// TestMaterialize_AppliesPerCallTimeout proves that a non-zero timeout on
// sentioMaterializer is actually applied as a per-call ctx deadline before
// the backend call — otherwise materialize.timeout would be dead config.
func TestMaterialize_AppliesPerCallTimeout(t *testing.T) {
	fb := &fakeBackend{matResp: &pb.MaterializeSQLResponse{
		Code:                    pb.MaterializeCode_MaterializeSuccess,
		SqlAfterMaterialization: "SELECT 1",
	}}
	m := newTestMaterializer(fb, 4, "", 5*time.Second)
	if _, err := m.Materialize(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !fb.lastMatCtxHadDeadline {
		t.Fatal("want backend ctx to carry a deadline when timeout > 0")
	}
}
