package integration

import (
	"context"
	"testing"
	"time"

	"housegate/housegate/pkg/integration/testenv"
)

// TestUpstream_DiesMidQuery pins the failure-propagation contract: when
// the upstream ClickHouse goes away while a query is streaming, the
// client must see an error within a bounded time — the proxy must NOT
// hang on the half-open upstream socket. Without correct teardown, the
// in-flight Read against upstream would block indefinitely and the
// client conn would never get an answer or an exception.
//
// Mechanic: use a *dedicated* ClickHouse instance (not the shared
// chEnv — stopping that would break every subsequent test in the same
// binary), point a fresh proxy at it, start a long SELECT sleep, then
// docker-stop the container. We then assert the in-flight QueryRow
// returns within a generous deadline.
//
// Auth + rewriter are intentionally off here: this test exercises the
// raw transport teardown path, and adding the auth chain (which is
// fail-closed without rewriter) would only obscure the failure mode.
func TestUpstream_DiesMidQuery(t *testing.T) {
	dedicatedCH := testenv.StartClickHouse(t)
	proxy := testenv.StartServerProxy(t, dedicatedCH.Addr)
	conn := openConn(t, proxy.Addr)

	// Long enough that a healthy upstream would still be streaming when
	// we kill the container, short enough that the test itself does
	// not time out if for some reason the upstream survives.
	const sleepSeconds = 10
	done := make(chan error, 1)
	go func() {
		var v uint8
		done <- conn.QueryRow(context.Background(),
			"SELECT sleep(?)", sleepSeconds).Scan(&v)
	}()

	// Give the upstream long enough to register the query and start
	// executing. Native protocol handshake + auth lookup + sleep start
	// typically lands inside 300ms.
	time.Sleep(500 * time.Millisecond)

	// Pull the plug. This is the moment of truth: with the upstream
	// gone, the proxy's upstream-side ReadRaw should fail and the
	// relay should tear the client side down with an error.
	dedicatedCH.Stop(t)

	select {
	case err := <-done:
		// We don't pin the exact error text — different layers may
		// surface "EOF" / "connection reset" / "unexpected EOF" / a
		// CH-style exception depending on where the kill landed. The
		// invariant is just that the call returned at all (not hung)
		// and that the result is an error (not a silent success).
		if err == nil {
			t.Fatal("expected error after upstream killed mid-query, got nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("client hung past 10s after upstream killed — relay leaked the half-open upstream socket")
	}
}
