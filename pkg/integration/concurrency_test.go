package integration

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/cfgtypes"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/integration/testenv"
	authplugin "housegate/housegate/pkg/plugins/auth"
	"housegate/housegate/pkg/plugins/concurrency"
)

// Concurrency integration tests. Constraints carried over from the
// auth tests:
//
//   - The concurrency limiter keys on user identity (UserDimension),
//     which comes from the auth plugin. Auth must be on, so every
//     test signs queries with a RelaySigner — and PermissionObserver
//     comes along with auth.Enabled, so the rewriter mock has to be
//     attached too.
//   - Permits live in Redis. We use miniredis (process-local fake)
//     via testenv.WithMiniredis.

// startConcurrencyProxy stands up the common posture: rewriter mock +
// miniredis + auth on (allowing all given signers) + concurrency on
// with PerUser=1.
func startConcurrencyProxy(t *testing.T, signers ...*auth.RelaySigner) *testenv.TestProxy {
	t.Helper()
	addrs := make([]string, 0, len(signers))
	for _, s := range signers {
		addrs = append(addrs, s.Address())
	}

	rewriterOpt, _ := testenv.WithRewriterMock(t)
	return testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		testenv.WithMiniredis(t),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Auth = authplugin.Config{
				Enabled:          true,
				AllowedAddresses: addrs,
				MaxTokenAge:      cfgtypes.Duration{Duration: 5 * time.Minute},
				AllowNoAuth:      false,
			}
			cfg.ConcurrencyLimit = concurrency.Config{
				Enabled:  true,
				PerUser:  1,
				Timeout:  cfgtypes.Duration{Duration: 30 * time.Second},
				FailOpen: false,
			}
		}),
	)
}

// TestConcurrency_QuotaEnforced: with PerUser=1, a second query from
// the SAME user while the first is still running is rejected with a
// quota error.
//
// Mechanic: Q1 starts SELECT sleep(2) and holds the slot. After a
// small wait to ensure Q1 has reached the limiter, Q2 fires on a
// second conn (clickhouse-go connections are independent). Q2's
// OnQuery Acquire sees the existing permit and returns
// ErrQuotaExceeded.
func TestConcurrency_QuotaEnforced(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	proxy := startConcurrencyProxy(t, signer)

	conn1 := openSignedConn(t, proxy.Addr, signer)
	conn2 := openSignedConn(t, proxy.Addr, signer)

	q1Err := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var v uint8
		q1Err <- conn1.QueryRow(context.Background(), "SELECT sleep(2)").Scan(&v)
	}()

	// Wait long enough for Q1's OnQuery to reach Acquire. The plugin
	// chain is fast (sub-ms) but the goroutine scheduler can delay
	// us a bit; 250ms is comfortable.
	time.Sleep(250 * time.Millisecond)

	q2Err := conn2.QueryRow(context.Background(), "SELECT 1").Scan(new(uint8))
	if q2Err == nil {
		t.Fatal("expected quota rejection for second concurrent query, got nil error")
	}
	if !strings.Contains(q2Err.Error(), "quota") {
		t.Logf("note: rejection text was %q — surface may have changed", q2Err.Error())
	}

	wg.Wait()
	// Q1 should have succeeded; if it didn't, the test infrastructure
	// (rewriter mock, auth, etc.) is the suspect, not concurrency.
	if err := <-q1Err; err != nil {
		t.Fatalf("Q1 (holder) failed unexpectedly: %v", err)
	}
}

// TestConcurrency_SlotReleased: after a query completes, the slot it
// occupied is released and a subsequent query from the same user
// succeeds. Validates the OnQueryComplete → Release path.
func TestConcurrency_SlotReleased(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	proxy := startConcurrencyProxy(t, signer)
	conn := openSignedConn(t, proxy.Addr, signer)
	ctx := context.Background()

	var v uint8
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("first SELECT 1: %v", err)
	}
	// If the slot did not release, this second query under the same
	// user identity would be rejected with quota error.
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("second SELECT 1 (slot should be released): %v", err)
	}
}

// TestConcurrency_FailOpen pins the infra-error escape hatch: when
// Redis dies after the proxy has already started and FailOpen=true,
// the limiter's Acquire returns an infrastructure error and the plugin
// must swallow it (warn-log) so the query still reaches CH.
//
// Mechanic: stand the proxy up against miniredis with FailOpen=true,
// close miniredis to break every subsequent Redis op, then issue a
// signed SELECT 1. Without FailOpen this would surface to the client
// as a 5xx-style error; with it, the query returns 1.
func TestConcurrency_FailOpen(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	rewriterOpt, _ := testenv.WithRewriterMock(t)
	redisOpt, mr := testenv.WithMiniredisHandle(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		redisOpt,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Auth = authplugin.Config{
				Enabled:          true,
				AllowedAddresses: []string{signer.Address()},
				MaxTokenAge:      cfgtypes.Duration{Duration: 5 * time.Minute},
				AllowNoAuth:      false,
			}
			cfg.ConcurrencyLimit = concurrency.Config{
				Enabled:  true,
				PerUser:  1,
				Timeout:  cfgtypes.Duration{Duration: 30 * time.Second},
				FailOpen: true,
			}
		}),
	)

	// Yank the redis fake out from under the proxy. The miniredis
	// Close() shuts down the TCP listener, so any next Redis command
	// from the limiter fails with a connection error.
	mr.Close()

	conn := openSignedConn(t, proxy.Addr, signer)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("SELECT 1 with FailOpen=true and redis down: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}
}

// TestConcurrency_DifferentUsersIndependent: PerUser=1 limits each
// user independently. Two signers = two users; one user holding its
// slot must not affect the other user's ability to query.
//
// This pins the per-user dimensioning of the limiter — a regression
// that accidentally globalised the quota (e.g. by hashing all users
// to the same key) would surface here as the second user's query
// failing with quota.
func TestConcurrency_DifferentUsersIndependent(t *testing.T) {
	signerA, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner A: %v", err)
	}
	signerB, err := auth.NewRelaySigner(authTestKey2)
	if err != nil {
		t.Fatalf("NewRelaySigner B: %v", err)
	}
	proxy := startConcurrencyProxy(t, signerA, signerB)

	connA := openSignedConn(t, proxy.Addr, signerA)
	connB := openSignedConn(t, proxy.Addr, signerB)

	// User A holds their slot with a long sleep.
	var wg sync.WaitGroup
	wg.Add(1)
	aErr := make(chan error, 1)
	go func() {
		defer wg.Done()
		var v uint8
		aErr <- connA.QueryRow(context.Background(), "SELECT sleep(2)").Scan(&v)
	}()
	time.Sleep(250 * time.Millisecond) // let A reach Acquire

	// User B should be completely unaffected — their PerUser=1 slot is
	// still free because the quota is keyed on identity.
	var bV uint8
	if err := connB.QueryRow(context.Background(), "SELECT 1").Scan(&bV); err != nil {
		t.Fatalf("user B SELECT 1 (while user A holds their slot): %v", err)
	}
	if bV != 1 {
		t.Errorf("user B SELECT 1 = %d, want 1", bV)
	}

	wg.Wait()
	if err := <-aErr; err != nil {
		t.Fatalf("user A sleep(2) (holder) failed unexpectedly: %v", err)
	}
}
