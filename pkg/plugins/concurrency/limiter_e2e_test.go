package concurrency

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// startMiniredis spins up an in-process Redis fake and returns a
// connected client. Both are torn down via t.Cleanup.
func startMiniredis(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(mr.Close)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	return rdb
}

func userDim(value string, quota int) Dimension {
	return Dimension{Name: DimensionUser, Value: value, Quota: quota}
}

// TestRedisLimiter_E2E_QuotaEnforced exercises the full Acquire/Release
// cycle against the real Lua scripts under miniredis.
func TestRedisLimiter_E2E_QuotaEnforced(t *testing.T) {
	rdb := startMiniredis(t)
	lim := NewRedisLimiter(rdb, 60*time.Second)

	const userID = "0xdeadbeef"
	dims := []Dimension{userDim(userID, 1)}

	p1, err := lim.Acquire(context.Background(), dims)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if p1.IsZero() {
		t.Fatal("first acquire returned zero permit")
	}

	_, err = lim.Acquire(context.Background(), dims)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("second acquire should hit ErrQuotaExceeded, got: %v", err)
	}
	if !strings.Contains(err.Error(), DimensionUser) {
		t.Errorf("error should name the offending dimension: %v", err)
	}

	lim.Release(context.Background(), p1)

	p2, err := lim.Acquire(context.Background(), dims)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	lim.Release(context.Background(), p2)
}

func TestRedisLimiter_E2E_DistinctUsersIsolated(t *testing.T) {
	rdb := startMiniredis(t)
	lim := NewRedisLimiter(rdb, 60*time.Second)

	pA, err := lim.Acquire(context.Background(), []Dimension{userDim("0xaaa", 1)})
	if err != nil {
		t.Fatalf("user A acquire: %v", err)
	}
	pB, err := lim.Acquire(context.Background(), []Dimension{userDim("0xbbb", 1)})
	if err != nil {
		t.Fatalf("user B acquire (different owner) should succeed: %v", err)
	}

	lim.Release(context.Background(), pA)
	lim.Release(context.Background(), pB)
}

// TestRedisLimiter_E2E_StaleReap proves the timeout window reaps
// abandoned permits on the next Acquire.
func TestRedisLimiter_E2E_StaleReap(t *testing.T) {
	rdb := startMiniredis(t)
	lim := NewRedisLimiter(rdb, 1*time.Second)

	dims := []Dimension{userDim("0xstale", 1)}
	if _, err := lim.Acquire(context.Background(), dims); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := lim.Acquire(context.Background(), dims); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("immediate second acquire should be quota-rejected, got: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)

	p, err := lim.Acquire(context.Background(), dims)
	if err != nil {
		t.Fatalf("acquire after reap window: %v", err)
	}
	lim.Release(context.Background(), p)
}

// TestRedisLimiter_E2E_MultiDimension_RejectionNamesDimension verifies
// that when one of several dimensions is at cap, the rejection cites
// the right dimension name.
func TestRedisLimiter_E2E_MultiDimension_RejectionNamesDimension(t *testing.T) {
	rdb := startMiniredis(t)
	lim := NewRedisLimiter(rdb, 60*time.Second)

	// First acquire: user has plenty of room (quota 10), stake_level
	// "gold" has quota 1.
	dims := []Dimension{
		userDim("0xuser", 10),
		{Name: DimensionStakeLevel, Value: "gold", Quota: 1},
	}
	p1, err := lim.Acquire(context.Background(), dims)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// Second acquire from a DIFFERENT user but the same stake_level
	// should be rejected on the stake_level dimension, not the user.
	dims2 := []Dimension{
		userDim("0xother", 10),
		{Name: DimensionStakeLevel, Value: "gold", Quota: 1},
	}
	_, err = lim.Acquire(context.Background(), dims2)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got: %v", err)
	}
	if !strings.Contains(err.Error(), DimensionStakeLevel) {
		t.Errorf("error should name stake_level dimension, got: %v", err)
	}

	// Cleanup.
	lim.Release(context.Background(), p1)
}

// TestRedisLimiter_E2E_MultiDimension_AllReleased verifies Release
// removes the permit from every dimension key it was added to.
func TestRedisLimiter_E2E_MultiDimension_AllReleased(t *testing.T) {
	rdb := startMiniredis(t)
	lim := NewRedisLimiter(rdb, 60*time.Second)

	dims := []Dimension{
		userDim("0xuser", 1),
		{Name: DimensionStakeLevel, Value: "silver", Quota: 1},
	}
	p, err := lim.Acquire(context.Background(), dims)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// Release should clear both dimensions; a follow-up acquire on the
	// same dims must succeed.
	lim.Release(context.Background(), p)

	if _, err := lim.Acquire(context.Background(), dims); err != nil {
		t.Fatalf("acquire after release of multi-dim permit: %v", err)
	}
}

// TestRedisLimiter_E2E_EmptyDims_NoOp verifies that an empty dims slice
// returns a zero permit and never hits Redis.
func TestRedisLimiter_E2E_EmptyDims_NoOp(t *testing.T) {
	rdb := startMiniredis(t)
	lim := NewRedisLimiter(rdb, 60*time.Second)

	p, err := lim.Acquire(context.Background(), nil)
	if err != nil {
		t.Fatalf("empty acquire: %v", err)
	}
	if !p.IsZero() {
		t.Errorf("expected zero permit, got: %+v", p)
	}
	// Release on a zero permit must be a no-op (no panic, no Redis ops).
	lim.Release(context.Background(), p)
}

// TestRedisLimiter_E2E_PluginIntegration drives the Plugin against a
// real limiter to confirm the OnQuery/OnQueryComplete pair actually
// frees Redis state, not just the in-process map.
func TestRedisLimiter_E2E_PluginIntegration(t *testing.T) {
	rdb := startMiniredis(t)
	lim := NewRedisLimiter(rdb, 60*time.Second)
	p := &Plugin{
		Limiter:   lim,
		Resolvers: []DimensionResolver{UserDimension(1), NoneStakeLevelResolver()},
	}

	const userID = "0xcafe"
	sess1 := newSession(t, 401, userID)
	sess2 := newSession(t, 402, userID)

	if err := p.OnQuery(context.Background(), newQueryContext(sess1, "SELECT 1")); err != nil {
		t.Fatalf("session 1 OnQuery: %v", err)
	}

	err := p.OnQuery(context.Background(), newQueryContext(sess2, "SELECT 2"))
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("session 2 should be quota-rejected, got: %v", err)
	}

	// Releasing session 1 via the safety-net path also frees the slot.
	p.OnClose(sess1)

	if err := p.OnQuery(context.Background(), newQueryContext(sess2, "SELECT 2")); err != nil {
		t.Fatalf("session 2 acquire after release: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess2)
}
