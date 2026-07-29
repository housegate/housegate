package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/cfgtypes"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	authplugin "github.com/housegate/housegate/pkg/plugins/auth"
)

// Auth integration tests.
//
// In the current housegate build, enabling auth (cfg.Auth.Enabled=true)
// automatically wires PermissionCommitGateObserver into the commitgate
// chain (build.go:255-260). That observer rejects every statement whose
// StatementType is Unspecified. Without a rewriter to classify, EVERY
// query is Unspecified, so auth-on traffic is fail-closed by design.
//
// Two postures are covered here:
//
//   - Rejection paths (TestAuth_NoToken / _WrongKey) need no rewriter:
//     the auth plugin runs FIRST in the OnQuery chain so it short-
//     circuits before commitgate gets a chance.
//   - Happy paths (TestAuth_ValidToken / _AllowNoAuthSoftMode) need the
//     statement classified, so they ship via the in-process rewriter
//     mock (testenv.WithRewriterMock).
//
// TestAuth_RejectsWithoutRewriter exists to pin the auth-on-rewriter-off
// contract itself: a properly signed query is STILL rejected without a
// rewriter, because commitgate's PermissionObserver fires. If that
// test starts passing, the contract has changed.

// Distinct 32-byte hex secp256k1 private keys for the two signer
// identities the auth tests need. The trailing decimal byte is just so
// they read as different at a glance — any pair of valid keys works.
const (
	authTestKey1 = "1111111111111111111111111111111111111111111111111111111111111111"
	authTestKey2 = "2222222222222222222222222222222222222222222222222222222222222222"
)

// authProxyConfig flips auth on and pins the allowed signer set. Tests
// pass it to StartServerProxy via WithConfigMutator.
func authProxyConfig(addresses []string, allowNoAuth bool) testenv.ProxyOption {
	return testenv.WithConfigMutator(func(cfg *config.Config) {
		cfg.Auth = authplugin.Config{
			Enabled:          true,
			AllowedAddresses: addresses,
			MaxTokenAge:      cfgtypes.Duration{Duration: 5 * time.Minute},
			AllowNoAuth:      allowNoAuth,
		}
	})
}

// openSignedConn opens a clickhouse-go connection whose every query is
// JWS-signed by the given signer. The fork we use exposes Options.SignFunc;
// the driver calls it once per Query and stuffs the returned token into
// the request's Settings map under the key the auth plugin reads.
func openSignedConn(t *testing.T, proxyAddr string, signer *auth.RelaySigner) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{proxyAddr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
		SignFunc: signer.SignToken,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open (signed): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestAuth_NoToken: an unsigned query is rejected when AllowNoAuth=false.
// The auth plugin's OnQuery hook short-circuits before commitgate sees
// the statement, so this works without a rewriter.
func TestAuth_NoToken(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		authProxyConfig([]string{signer.Address()}, false),
	)
	conn := openConn(t, proxy.Addr)

	err = conn.QueryRow(context.Background(), "SELECT 1").Scan(new(uint8))
	if err == nil {
		t.Fatal("expected rejection for unsigned query, got nil error")
	}
}

// TestAuth_WrongKey: a JWS signed by a key NOT in AllowedAddresses is
// rejected. Same short-circuit as TestAuth_NoToken — the auth plugin
// rejects before commitgate runs.
func TestAuth_WrongKey(t *testing.T) {
	allowed, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner allowed: %v", err)
	}
	rogue, err := auth.NewRelaySigner(authTestKey2)
	if err != nil {
		t.Fatalf("NewRelaySigner rogue: %v", err)
	}
	// Sanity check the two signers are actually distinct — a typo in
	// the test constants would otherwise let this test silently pass.
	if strings.EqualFold(allowed.Address(), rogue.Address()) {
		t.Fatalf("test setup broken: both signers resolve to %s", allowed.Address())
	}

	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		authProxyConfig([]string{allowed.Address()}, false),
	)
	conn := openSignedConn(t, proxy.Addr, rogue)

	err = conn.QueryRow(context.Background(), "SELECT 1").Scan(new(uint8))
	if err == nil {
		t.Fatal("expected rejection for wrong-key token, got nil error")
	}
}

// TestAuth_RejectsWithoutRewriter pins the auth-on-rewriter-off contract:
// even a properly signed token is rejected, because commitgate's
// PermissionObserver gates on StatementType and refuses Unspecified.
//
// If this test ever STARTS failing, the contract has changed — likely
// because PermissionObserver was taught to default-allow SELECT-shaped
// queries lacking a rewriter classification, or because the build
// stopped auto-wiring the observer with auth.Enabled. Either way the
// auth happy-path tests (which need a rewriter today) should be
// revisited.
func TestAuth_RejectsWithoutRewriter(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		authProxyConfig([]string{signer.Address()}, false),
	)
	conn := openSignedConn(t, proxy.Addr, signer)

	err = conn.QueryRow(context.Background(), "SELECT 1").Scan(new(uint8))
	if err == nil {
		t.Fatal("expected commitgate rejection (Unspecified statement type)")
	}
	if !strings.Contains(err.Error(), "commitgate") &&
		!strings.Contains(err.Error(), "Unspecified") {
		t.Logf("note: rejection reason was %q — surface text may have changed",
			err.Error())
	}
}

// TestAuth_ValidToken: with auth on AND the rewriter mock attached, a
// query signed by an allowed key flows through the full plugin chain
// and returns the correct result from ClickHouse. The mock classifies
// "SELECT 1" → STATEMENT_TYPE_SELECT, which commitgate's
// PermissionObserver allows under DbAuthRead.
//
// Failure modes this catches:
//   - auth plugin's JWS verification regresses (would fail even though
//     mock classifies correctly)
//   - PermissionObserver tightens to reject SELECT without a database
//     permission (would surface as a code:403 error)
//   - rewriter wiring forgets to forward sql_after_rewrite to the
//     upstream codec (would surface as syntax error from CH or
//     unrelated query result)
func TestAuth_ValidToken(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	rewriterOpt, _ := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
	)
	conn := openSignedConn(t, proxy.Addr, signer)

	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("signed SELECT 1: %v", err)
	}
	if v != 1 {
		t.Errorf("signed SELECT 1 = %d, want 1", v)
	}
}

// TestAuth_QueryHashMismatch pins the SQL-binding semantics of the JWS:
// the token's QueryHash claim is keccak256 of the *signed* SQL body, and
// the validator recomputes that hash from the *received* SQL. If the
// caller signs SQL A but sends SQL B, the proxy must reject with the
// "query hash mismatch" error — preventing token reuse across statements.
//
// We attack the binding directly: a SignFunc that signs a fixed SQL
// regardless of what the driver hands it, paired with a different
// actual query. Without the binding (e.g. validator drops the QueryHash
// check) this would silently succeed.
func TestAuth_QueryHashMismatch(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	rewriterOpt, _ := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
	)

	// Tamper with the SignFunc: always sign "SELECT 2" regardless of the
	// SQL the driver is about to ship. The on-wire SQL is whatever the
	// caller runs (here: SELECT 1).
	tamper := func(_ string) (string, error) {
		return signer.SignToken("SELECT 2")
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{proxy.Addr},
		Auth:     clickhouse.Auth{Database: chEnv.Database, Username: chEnv.User, Password: chEnv.Password},
		Protocol: clickhouse.Native,
		SignFunc: tamper,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open (tampered sign): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	err = conn.QueryRow(context.Background(), "SELECT 1").Scan(new(uint8))
	if err == nil {
		t.Fatal("expected query hash mismatch rejection, got nil error")
	}
	if !strings.Contains(err.Error(), "query hash mismatch") {
		t.Errorf("error = %q, want to contain 'query hash mismatch'", err.Error())
	}
}

// TestAuth_AllowNoAuthBlockedByCommitgate pins a non-obvious contract:
// AllowNoAuth=true makes the auth plugin let unsigned queries through,
// but PermissionCommitGateObserver — auto-wired by Auth.Enabled —
// requires an authenticated account regardless and rejects the
// statement at the next stage. Net effect: AllowNoAuth alone does not
// produce a working soft-rollout in the default build.
//
// If this test ever STARTS failing (i.e. AllowNoAuth unsigned queries
// start succeeding), the commitgate policy has loosened — confirm the
// change is intentional and update test naming to match.
func TestAuth_AllowNoAuthBlockedByCommitgate(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	rewriterOpt, _ := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, true), // allowNoAuth=true
	)
	conn := openConn(t, proxy.Addr)

	err = conn.QueryRow(context.Background(), "SELECT 1").Scan(new(uint8))
	if err == nil {
		t.Fatal("unsigned SELECT with AllowNoAuth=true succeeded; " +
			"commitgate's authenticated-account requirement may have " +
			"been relaxed — re-evaluate AllowNoAuth coverage")
	}
	if !strings.Contains(err.Error(), "commitgate") &&
		!strings.Contains(err.Error(), "authenticated") {
		t.Logf("note: rejection text was %q — surface may have changed",
			err.Error())
	}
}
