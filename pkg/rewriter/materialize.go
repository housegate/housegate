package rewriter

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// MaterializeOutcome is the result of one materialization call. SQL is the
// text the caller should use downstream: the materialized SQL on Success,
// the original SQL on any non-Success code (fail-open). Changed is true
// only when the engine applied at least one replacement.
type MaterializeOutcome struct {
	SQL     string
	Changed bool
	// Code is meaningful only when the accompanying error is nil.
	Code    pb.MaterializeCode
	Message string
}

// Materializer rewrites non-deterministic functions in a SQL statement to
// literal constants (rewriter-go MaterializeSQL). It is the agent-side
// Phase-1 seam: SQL in → deterministic SQL out. Safe for concurrent use.
type Materializer interface {
	Materialize(ctx context.Context, sql string) (MaterializeOutcome, error)
	Close() error
}

// NewMaterializer builds a Materializer over the grpc/native backend
// selected by opts.Engine (reusing newBackend). poolSize is how many
// random/uuid values are supplied per call (must be > 0); profileID
// selects the materialization profile ("" → engine default).
func NewMaterializer(opts Options, poolSize int, profileID string) (Materializer, error) {
	if poolSize <= 0 {
		return nil, fmt.Errorf("materializer pool size must be > 0, got %d", poolSize)
	}
	be, err := newBackend(opts)
	if err != nil {
		return nil, err
	}
	return &sentioMaterializer{be: be, poolSize: poolSize, profileID: profileID, timeout: opts.Timeout}, nil
}

type sentioMaterializer struct {
	be        backend
	poolSize  int
	profileID string
	// timeout, when > 0, bounds each Materialize call with a per-call
	// deadline (independent of any gRPC dial timeout). This makes a
	// hung rewriter fail open promptly instead of blocking the query.
	timeout time.Duration
}

func (m *sentioMaterializer) Materialize(ctx context.Context, sql string) (MaterializeOutcome, error) {
	if m.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}
	now := time.Now().UnixNano()
	req := &pb.MaterializeSQLRequest{
		Sql: sql,
		Inputs: &pb.MaterializationInputs{
			NowUnixNs:           &now,
			RandomUint64Values:  randUint64Slice(m.poolSize),
			RandomFloat64Values: randFloat64Slice(m.poolSize),
			UuidValues:          uuidSlice(m.poolSize),
		},
		Policy: &pb.MaterializationPolicy{ProfileId: m.profileID},
	}
	resp, err := m.be.MaterializeSQL(ctx, req)
	if err != nil {
		return MaterializeOutcome{SQL: sql}, err
	}
	if resp.GetCode() != pb.MaterializeCode_MaterializeSuccess {
		return MaterializeOutcome{SQL: sql, Code: resp.GetCode(), Message: resp.GetMessage()}, nil
	}
	return MaterializeOutcome{
		SQL:     resp.GetSqlAfterMaterialization(),
		Changed: len(resp.GetReplacements()) > 0,
		Code:    resp.GetCode(),
	}, nil
}

func (m *sentioMaterializer) Close() error { return m.be.Close() }

// randUint64Slice / randFloat64Slice / uuidSlice generate the per-call
// input pools. The values need NOT be reproducible: the signed SQL carries
// the resolved constants, and replay reads those constants rather than
// re-materializing. crypto/rand is used for a good-quality, dependency-free
// source.
func randUint64Slice(n int) []uint64 {
	out := make([]uint64, n)
	var b [8]byte
	for i := range out {
		_, _ = crand.Read(b[:])
		out[i] = binary.LittleEndian.Uint64(b[:])
	}
	return out
}

func randFloat64Slice(n int) []float64 {
	out := make([]float64, n)
	var b [8]byte
	for i := range out {
		_, _ = crand.Read(b[:])
		// 53-bit mantissa → uniform in [0, 1).
		out[i] = float64(binary.LittleEndian.Uint64(b[:])>>11) / (1 << 53)
	}
	return out
}

func uuidSlice(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = uuid.NewString()
	}
	return out
}

var _ Materializer = (*sentioMaterializer)(nil)
