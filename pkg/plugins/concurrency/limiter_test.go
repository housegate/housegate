package concurrency

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

// fakeLimiter records Acquire/Release calls and returns scripted results.
// One acquireResult is consumed per Acquire call; if the queue is empty,
// returns the zero Permit and nil err (a quota-permissive default that
// keeps tests focused on the dim resolution path).
type fakeLimiter struct {
	mu             sync.Mutex
	acquireResults []acquireResult
	acquireCalls   [][]Dimension
	releaseCalls   []Permit
}

type acquireResult struct {
	permit Permit
	err    error
}

func (f *fakeLimiter) Acquire(_ context.Context, dims []Dimension) (Permit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Copy so test assertions are not affected by later caller mutation.
	dimsCopy := append([]Dimension(nil), dims...)
	f.acquireCalls = append(f.acquireCalls, dimsCopy)
	if len(f.acquireResults) == 0 {
		return Permit{}, nil
	}
	r := f.acquireResults[0]
	f.acquireResults = f.acquireResults[1:]
	return r.permit, r.err
}

func (f *fakeLimiter) Release(_ context.Context, p Permit) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls = append(f.releaseCalls, p)
}

// permit constructs a Permit for fake-acquire results. Tests don't
// inspect the keys field; an empty slice is fine.
func makePermit(id string) Permit { return Permit{ID: id} }

// newSession returns a Session backed by one half of a net.Pipe, with
// an optional pre-set identity.
func newSession(t *testing.T, id int64, userID string) chsession.Session {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	sess := chsession.New(id, clientConn)
	if userID != "" {
		sess.State().Identity = chsession.IdentityClaims{UserID: userID}
	}
	return sess
}

func newQueryContext(sess chsession.Session, sql string) *plugin.QueryContext {
	return &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: sql,
		Query:       &chproto.Query{Body: sql},
	}
}

// ---------------------------------------------------------------------------

func TestPlugin_AcquireAndComplete_Pair(t *testing.T) {
	const userID = "0xabc"
	sess := newSession(t, 1, userID)
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{
		acquireResults: []acquireResult{{permit: makePermit("permit-1")}},
	}
	p := &Plugin{
		Limiter:   fl,
		Resolvers: []DimensionResolver{UserDimension(10)},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if got := len(fl.acquireCalls); got != 1 {
		t.Fatalf("acquire calls=%d, want 1", got)
	}
	want := []Dimension{{Name: DimensionUser, Value: userID, Quota: 10}}
	if !reflect.DeepEqual(fl.acquireCalls[0], want) {
		t.Errorf("dims=%+v, want %+v", fl.acquireCalls[0], want)
	}

	p.OnQueryComplete(context.Background(), sess)
	if got := len(fl.releaseCalls); got != 1 {
		t.Fatalf("release calls=%d, want 1", got)
	}
	if fl.releaseCalls[0].ID != "permit-1" {
		t.Errorf("released id=%q, want permit-1", fl.releaseCalls[0].ID)
	}
}

func TestPlugin_QuotaExceeded_ReturnsError(t *testing.T) {
	sess := newSession(t, 2, "0xabc")
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{
		acquireResults: []acquireResult{{
			err: fmt.Errorf("%w: user:1/1", ErrQuotaExceeded),
		}},
	}
	p := &Plugin{Limiter: fl, Resolvers: []DimensionResolver{UserDimension(1)}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatal("expected error when quota exceeded")
	}
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("error should wrap ErrQuotaExceeded: %v", err)
	}
	if !strings.Contains(err.Error(), "user:1/1") {
		t.Errorf("error should include dim:current/limit detail: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess)
	if len(fl.releaseCalls) != 0 {
		t.Errorf("release should not fire after quota reject, got: %+v", fl.releaseCalls)
	}
}

func TestPlugin_RedisError_FailOpen(t *testing.T) {
	sess := newSession(t, 3, "0xabc")
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{
		acquireResults: []acquireResult{{
			err: fmt.Errorf("%w: dial tcp: connection refused", ErrAcquireFailed),
		}},
	}
	p := &Plugin{Limiter: fl, Resolvers: []DimensionResolver{UserDimension(1)}, FailOpen: true}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("FailOpen=true should swallow infra error, got: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess)
	if len(fl.releaseCalls) != 0 {
		t.Errorf("release should not fire after fail-open, got: %+v", fl.releaseCalls)
	}
}

func TestPlugin_RedisError_FailClose(t *testing.T) {
	sess := newSession(t, 4, "0xabc")
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{
		acquireResults: []acquireResult{{
			err: fmt.Errorf("%w: dial tcp: connection refused", ErrAcquireFailed),
		}},
	}
	p := &Plugin{Limiter: fl, Resolvers: []DimensionResolver{UserDimension(1)}, FailOpen: false}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatal("FailOpen=false should surface infra error")
	}
	if !errors.Is(err, ErrAcquireFailed) {
		t.Errorf("error should wrap ErrAcquireFailed: %v", err)
	}
}

// TestPlugin_AnonymousSession_UserResolverEmpty proves that when the
// only resolver returns an empty Value (anonymous session), the Plugin
// short-circuits without calling Acquire.
func TestPlugin_AnonymousSession_UserResolverEmpty(t *testing.T) {
	sess := newSession(t, 5, "") // empty UserID → anonymous
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{}
	p := &Plugin{Limiter: fl, Resolvers: []DimensionResolver{UserDimension(1)}}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("anonymous OnQuery: %v", err)
	}
	if len(fl.acquireCalls) != 0 {
		t.Errorf("anonymous queries must not call Acquire, got: %+v", fl.acquireCalls)
	}
	p.OnClose(sess)
	if len(fl.releaseCalls) != 0 {
		t.Errorf("anonymous OnClose must not call Release, got: %+v", fl.releaseCalls)
	}
}

func TestPlugin_NoResolvers_NoOp(t *testing.T) {
	sess := newSession(t, 6, "0xabc")
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{}
	p := &Plugin{Limiter: fl} // no Resolvers

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(fl.acquireCalls) != 0 {
		t.Errorf("no-resolvers must not call Acquire, got: %+v", fl.acquireCalls)
	}
}

// TestPlugin_MultipleResolvers_BothContribute verifies that two
// resolvers each contributing a non-empty Dimension result in a single
// Acquire call carrying both dimensions in resolver order.
func TestPlugin_MultipleResolvers_BothContribute(t *testing.T) {
	sess := newSession(t, 7, "0xabc")
	qctx := newQueryContext(sess, "SELECT 1")

	stakeResolver := func(_ chsession.Session, _ *plugin.QueryContext) Dimension {
		return Dimension{Name: DimensionStakeLevel, Value: "gold", Quota: 5}
	}

	fl := &fakeLimiter{
		acquireResults: []acquireResult{{permit: makePermit("permit-multi")}},
	}
	p := &Plugin{
		Limiter:   fl,
		Resolvers: []DimensionResolver{UserDimension(10), stakeResolver},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	want := []Dimension{
		{Name: DimensionUser, Value: "0xabc", Quota: 10},
		{Name: DimensionStakeLevel, Value: "gold", Quota: 5},
	}
	if !reflect.DeepEqual(fl.acquireCalls[0], want) {
		t.Errorf("dims=%+v, want %+v", fl.acquireCalls[0], want)
	}
}

// TestPlugin_ResolverWithEmptyValueSkipped verifies that one resolver
// returning an empty Value (e.g. NoneStakeLevelResolver) is dropped
// while the other still contributes.
func TestPlugin_ResolverWithEmptyValueSkipped(t *testing.T) {
	sess := newSession(t, 8, "0xabc")
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{
		acquireResults: []acquireResult{{permit: makePermit("permit-skipped")}},
	}
	p := &Plugin{
		Limiter:   fl,
		Resolvers: []DimensionResolver{UserDimension(10), NoneStakeLevelResolver()},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(fl.acquireCalls) != 1 || len(fl.acquireCalls[0]) != 1 {
		t.Fatalf("expected exactly one dimension on Acquire, got: %+v", fl.acquireCalls)
	}
	if fl.acquireCalls[0][0].Name != DimensionUser {
		t.Errorf("expected user dimension only, got: %+v", fl.acquireCalls[0])
	}
}

// TestPlugin_AllResolversEmpty_NoAcquire verifies that when every
// resolver yields an empty Value the Plugin short-circuits without
// touching the limiter.
func TestPlugin_AllResolversEmpty_NoAcquire(t *testing.T) {
	sess := newSession(t, 9, "0xabc") // user is set, but we only register the placeholder
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{}
	p := &Plugin{Limiter: fl, Resolvers: []DimensionResolver{NoneStakeLevelResolver()}}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(fl.acquireCalls) != 0 {
		t.Errorf("Acquire must not be called when all resolvers return empty: %+v", fl.acquireCalls)
	}
}

func TestPlugin_OnClose_SafetyNetReleases(t *testing.T) {
	sess := newSession(t, 10, "0xabc")
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{
		acquireResults: []acquireResult{{permit: makePermit("permit-x")}},
	}
	p := &Plugin{Limiter: fl, Resolvers: []DimensionResolver{UserDimension(5)}}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	// Skip OnQueryComplete — simulate client disconnect mid-stream.
	p.OnClose(sess)
	if len(fl.releaseCalls) != 1 {
		t.Fatalf("OnClose safety net should release; got: %+v", fl.releaseCalls)
	}
	if fl.releaseCalls[0].ID != "permit-x" {
		t.Errorf("released id=%q, want permit-x", fl.releaseCalls[0].ID)
	}
}

func TestPlugin_DoubleComplete_IsNoop(t *testing.T) {
	sess := newSession(t, 11, "0xabc")
	qctx := newQueryContext(sess, "SELECT 1")

	fl := &fakeLimiter{
		acquireResults: []acquireResult{{permit: makePermit("permit-y")}},
	}
	p := &Plugin{Limiter: fl, Resolvers: []DimensionResolver{UserDimension(5)}}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess)
	p.OnQueryComplete(context.Background(), sess) // second call must be no-op
	p.OnClose(sess)                               // OnClose after release: no-op
	if len(fl.releaseCalls) != 1 {
		t.Errorf("expected exactly one Release, got %d: %+v", len(fl.releaseCalls), fl.releaseCalls)
	}
}

func TestPlugin_NilLimiter_NoPanic(t *testing.T) {
	sess := newSession(t, 12, "0xabc")
	qctx := newQueryContext(sess, "SELECT 1")

	p := &Plugin{Limiter: nil, Resolvers: []DimensionResolver{UserDimension(1)}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("nil Limiter must be a no-op, got: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess) // must not panic
	p.OnClose(sess)                               // must not panic
}
