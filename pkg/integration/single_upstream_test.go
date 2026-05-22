package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/integration/testenv"
)

// TestInsertSelectRoundtrip exercises the Data-block path in both
// directions: clickhouse-go ships an INSERT batch over the wire and
// reads back the result rows. Failure here usually means Codec.Splice,
// proto.Reader framing, or upstream Data-block forwarding is broken.
func TestInsertSelectRoundtrip(t *testing.T) {
	proxy := testenv.StartServerProxy(t, chEnv.Addr)
	conn := openConn(t, proxy.Addr)
	ctx := context.Background()

	table := uniqueTable(t)
	if err := conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s (v UInt64) ENGINE=Memory", table,
	)); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	t.Cleanup(func() {
		// Best effort — the container is torn down anyway, but
		// keeping the namespace clean lets parallel test runs share
		// a container if we ever want that later.
		_ = conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	want := []uint64{1, 2, 3, 5, 8, 13, 21}
	for _, v := range want {
		if err := batch.Append(v); err != nil {
			t.Fatalf("batch.Append(%d): %v", v, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch.Send: %v", err)
	}

	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT v FROM %s ORDER BY v", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	var got []uint64
	for rows.Next() {
		var v uint64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("rows.Scan: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if !equalUint64(got, want) {
		t.Errorf("rows: got %v, want %v", got, want)
	}
}

// TestLargeStream pulls a million-row result through the proxy. Stresses
// the chunk-by-chunk upstream→client copy path and the rewritten
// `notchunked` ServerHello caps. If chunked re-enables itself somewhere
// or the byte loop stalls, this is where it surfaces.
func TestLargeStream(t *testing.T) {
	proxy := testenv.StartServerProxy(t, chEnv.Addr)
	conn := openConn(t, proxy.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const n = 1_000_000
	rows, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT number FROM system.numbers LIMIT %d", n,
	))
	if err != nil {
		t.Fatalf("SELECT numbers: %v", err)
	}
	defer rows.Close()

	var count uint64
	for rows.Next() {
		var v uint64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("rows.Scan at %d: %v", count, err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != n {
		t.Errorf("got %d rows, want %d", count, n)
	}
}

// TestException sends invalid SQL and expects the client to receive an
// exception. The proxy's first-byte heuristic for ServerExceptionCode
// fires the OnException hook best-effort; we do NOT assert on the hook
// because the client-visible error is the load-bearing contract.
func TestException(t *testing.T) {
	proxy := testenv.StartServerProxy(t, chEnv.Addr)
	conn := openConn(t, proxy.Addr)
	ctx := context.Background()

	err := conn.QueryRow(ctx, "SELECT * FROM table_that_does_not_exist_xyz").Scan(new(uint64))
	if err == nil {
		t.Fatal("expected exception for unknown table, got nil error")
	}
	// ClickHouse signals UNKNOWN_TABLE as error code 60. clickhouse-go
	// embeds the message in the error string. Sanity-check that the
	// proxy did not corrupt or replace the message with a generic one.
	msg := err.Error()
	if !strings.Contains(strings.ToLower(msg), "table_that_does_not_exist_xyz") &&
		!strings.Contains(msg, "60") {
		t.Errorf("exception lost its identifying detail (got %q)", msg)
	}
}

// TestGracefulShutdown verifies that calling proxy.Close after a
// successful query returns within a tight deadline. proxy.Close's own
// internal timeout is 10s; we wrap it in a 5s deadline so a regression
// that hangs RunWith past graceful-stop is caught here rather than
// masked by the broader timeout.
func TestGracefulShutdown(t *testing.T) {
	proxy := testenv.StartServerProxy(t, chEnv.Addr)
	conn := openConn(t, proxy.Addr)

	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("ping before shutdown: %v", err)
	}
	_ = conn.Close()

	done := make(chan struct{})
	go func() {
		proxy.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("proxy.Close did not return within 5s")
	}
}

// TestCancelMidQuery exercises the ClientCancel packet path. The client
// kicks off a long-running query, cancels the context partway through,
// and we verify two things:
//
//  1. The cancelled query returns promptly (within a small multiple of
//     the cancel deadline) — proxy must forward ClientCancel to the
//     upstream so CH stops generating rows, then carry the resulting
//     ServerEndOfStream back to the client without stalling.
//
//  2. A fresh query on a NEW connection succeeds immediately afterwards.
//     clickhouse-go closes the conn it cancelled (its docstring is
//     explicit: "don't reuse a cancelled query as we don't drain the
//     connection"), so we cannot exercise the same connection — but a
//     proxy that deadlocked, leaked goroutines on the cancelled session,
//     or failed to release session state would surface as the second
//     query hanging or erroring.
//
// The codec treats ClientCancel as a body-less packet and forwards it
// via Splice (no OnQuery hook), and the ServerEndOfStream that comes
// back fires first-byte detection in relay.go (clearing ActiveRewrite
// + firing OnQueryComplete). Neither path has been exercised by any
// other test so far.
func TestCancelMidQuery(t *testing.T) {
	proxy := testenv.StartServerProxy(t, chEnv.Addr)
	conn := openConn(t, proxy.Addr)

	ctx, cancel := context.WithCancel(context.Background())

	queryErr := make(chan error, 1)
	go func() {
		// sleep(3) is the longest supported sleep in CH; we cancel
		// well before it would naturally complete. Asking for a row
		// guarantees the server starts streaming results, so a
		// cancel races against an in-flight response — the case
		// the proxy needs to handle.
		var v uint8
		queryErr <- conn.QueryRow(ctx, "SELECT sleep(3)").Scan(&v)
	}()

	// Give the query enough time to reach CH and start the sleep.
	// Without this we sometimes cancel before CH even sees the query,
	// which exercises a different (much simpler) code path.
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Cancel propagation budget. clickhouse-go writes the Cancel
	// packet, flushes, and closes the conn from its side; the proxy
	// then has to relay Cancel to CH, see EndOfStream come back, and
	// fire OnQueryComplete. 3 seconds is generous — anything in this
	// range that hangs indicates a real proxy bug.
	select {
	case err := <-queryErr:
		// The exact error type varies across clickhouse-go versions
		// (context.Canceled, "use of closed network connection", an
		// OpError wrapping either). We do not assert on the kind —
		// what matters is that the call returned, not how.
		t.Logf("cancelled query returned: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled query did not return within 3s of ctx.Cancel")
	}

	// Fresh connection, fresh query: if the proxy deadlocked or
	// leaked the upstream pool slot, this would hang or fail.
	conn2 := openConn(t, proxy.Addr)
	postCancelCtx, postCancelCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer postCancelCancel()
	var v uint8
	if err := conn2.QueryRow(postCancelCtx, "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("post-cancel query failed: %v", err)
	}
	if v != 1 {
		t.Errorf("post-cancel SELECT 1 = %d, want 1", v)
	}
}

func uniqueTable(t *testing.T) string {
	t.Helper()
	// t.Name() may include `/` for subtests; ClickHouse identifiers
	// don't like that. Just strip non-alphanumerics — the result is
	// still unique per test because Name() is.
	r := strings.NewReplacer("/", "_", ".", "_", "-", "_")
	return "t_" + r.Replace(t.Name())
}

func equalUint64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
