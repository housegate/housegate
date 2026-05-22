package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/integration/testenv"
)

// TestCancel_KillsUpstream pins the cancel-propagation contract: when
// a client closes its side mid-query (via ctx cancellation), the proxy
// must not leave the upstream query running on ClickHouse. The mechanic
// here is "client conn closes → relay tears down → CH detects upstream
// disconnect → CH cancels the query": the proxy's role is to NOT mask
// the disconnect.
//
// Failure modes this catches:
//   - relay buffers the cancel and never forwards it
//   - relay keeps its upstream socket open after the client side dies
//     (would surface as the long sleep continuing in system.processes)
//
// We tag the long query with a uniquely-recognisable WHERE clause so a
// follow-up SELECT against system.processes can pick it out without
// false matches from other concurrent test traffic.
func TestCancel_KillsUpstream(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	rewriterOpt, _ := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
	)

	// Unique tag inside the SQL — a string literal CH will not optimise
	// away. We grep on it via system.processes' query column.
	tag := fmt.Sprintf("cancel_test_%d", time.Now().UnixNano())
	longSQL := fmt.Sprintf("SELECT sleep(3), '%s'", tag)

	conn := openSignedConn(t, proxy.Addr, signer)
	ctx, cancel := context.WithCancel(context.Background())

	// Fire the long query and immediately cancel.
	done := make(chan error, 1)
	go func() {
		var s string
		// sleep returns a single row; Scan satisfies the driver.
		done <- conn.QueryRow(ctx, longSQL).Scan(new(uint8), &s)
	}()

	// Give CH long enough to register the query in system.processes.
	time.Sleep(500 * time.Millisecond)
	cancel()

	// The QueryRow call should return non-nil error after cancel.
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancelled query to error out, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled query did not return within 5s — relay may not have torn down")
	}

	// Now poll system.processes on a fresh signed conn. CH detects the
	// upstream socket close on the kernel timeout / next write attempt;
	// in practice this is sub-second. Give it up to 3s.
	probe := openSignedConn(t, proxy.Addr, signer)
	deadline := time.Now().Add(3 * time.Second)
	var lastCount uint64
	// The probe SQL itself contains the tag (because LIKE patterns are
	// embedded as literals) and is visible in system.processes while it
	// runs — so we filter out any query that touches system.processes.
	// The original sleep query does not, so this isolates it.
	probeSQL := fmt.Sprintf(
		"SELECT count() FROM system.processes WHERE query LIKE '%%%s%%' AND query NOT LIKE '%%system.processes%%'", tag)
	for time.Now().Before(deadline) {
		if err := probe.QueryRow(context.Background(), probeSQL).Scan(&lastCount); err != nil {
			t.Fatalf("probe system.processes: %v", err)
		}
		if lastCount == 0 {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Errorf("after cancel, CH still has %d sleep query (tag=%s) running — upstream cancel not propagated",
		lastCount, tag)
}
