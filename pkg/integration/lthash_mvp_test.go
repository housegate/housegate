package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/lthash"
	lthashplugin "github.com/housegate/housegate/pkg/plugins/lthash"
)

// openConnNoCompression opens a native connection with compression
// disabled. The MVP commitment pipeline only decodes uncompressed Data
// blocks (compressed blocks are explicitly out of scope and the plugin
// skips them), so the test client must not compress.
func openConnNoCompression(t *testing.T, proxyAddr string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{proxyAddr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol:    clickhouse.Native,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionNone},
	})
	if err != nil {
		t.Fatalf("clickhouse.Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestLtHashMVP is the MVP feasibility check: data INSERTed through the
// proxy is hashed wire-side by the lthash plugin, and that commitment must
// equal the LtHash recomputed by reading the rows back out of ClickHouse.
// If they match, the wire-side payload decode + canonical encoding agree
// with the data that actually landed in parts.
func TestLtHashMVP(t *testing.T) {
	lthashplugin.DefaultRegistry.Reset()

	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.LtHash.Enabled = true
		}),
	)
	conn := openConnNoCompression(t, proxy.Addr)
	ctx := context.Background()

	table := uniqueTable(t)
	// MergeTree (not Memory) so the rows materialize into real parts;
	// the column types are exactly the MVP canonical whitelist.
	if err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			user_id String,
			balance UInt64,
			score   Int32,
			ratio   Float64,
			created DateTime
		) ENGINE=MergeTree ORDER BY user_id`, table)); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})

	type row struct {
		user    string
		balance uint64
		score   int32
		ratio   float64
		created time.Time
	}
	// Includes a deliberately duplicated row to exercise the documented
	// duplicate caveat (both copies are hashed; the accumulator counts
	// them twice, which the read-back side reproduces).
	rows := []row{
		{"0x123", 10, -5, 1.5, time.Unix(1_700_000_000, 0).UTC()},
		{"0x123", 10, -5, 1.5, time.Unix(1_700_000_000, 0).UTC()},
		{"0xabc", 250, 99, -0.25, time.Unix(1_700_000_100, 0).UTC()},
		{"0xdef", 0, 0, 0, time.Unix(1_700_000_200, 0).UTC()},
	}

	// Two separate INSERT statements to confirm the registry folds
	// multiple statements into one table accumulator.
	insert := func(batchRows []row) {
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))
		if err != nil {
			t.Fatalf("PrepareBatch: %v", err)
		}
		for _, r := range batchRows {
			if err := batch.Append(r.user, r.balance, r.score, r.ratio, r.created); err != nil {
				t.Fatalf("batch.Append: %v", err)
			}
		}
		if err := batch.Send(); err != nil {
			t.Fatalf("batch.Send: %v", err)
		}
	}
	insert(rows[:2])
	insert(rows[2:])

	// Wire-side commitment, accumulated by the plugin as data streamed.
	got, ok := lthashplugin.DefaultRegistry.Table(table)
	if !ok {
		t.Fatalf("registry has no digest for table %q (plugin did not run)", table)
	}
	if got.Rows != uint64(len(rows)) {
		t.Fatalf("registry rows = %d, want %d", got.Rows, len(rows))
	}
	if got.Statements != 2 {
		t.Fatalf("registry statements = %d, want 2", got.Statements)
	}

	// Read-back commitment: pull every row out of ClickHouse and
	// recompute the LtHash independently of the wire path.
	want := lthash.New()
	cols := []lthash.Column{
		{Name: "user_id", Type: "String"},
		{Name: "balance", Type: "UInt64"},
		{Name: "score", Type: "Int32"},
		{Name: "ratio", Type: "Float64"},
		{Name: "created", Type: "DateTime"},
	}
	readBack, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT user_id, balance, score, ratio, created FROM %s", table))
	if err != nil {
		t.Fatalf("read-back SELECT: %v", err)
	}
	defer readBack.Close()

	n := 0
	for readBack.Next() {
		var (
			user    string
			balance uint64
			score   int32
			ratio   float64
			created time.Time
		)
		if err := readBack.Scan(&user, &balance, &score, &ratio, &created); err != nil {
			t.Fatalf("read-back Scan: %v", err)
		}
		h, err := lthash.RowHash(table, cols, []any{user, balance, score, ratio, created.UTC()})
		if err != nil {
			t.Fatalf("read-back RowHash: %v", err)
		}
		want.AddHash(h)
		n++
	}
	if err := readBack.Err(); err != nil {
		t.Fatalf("read-back rows.Err: %v", err)
	}
	if n != len(rows) {
		t.Fatalf("read-back returned %d rows, want %d", n, len(rows))
	}

	if !got.Acc.Equal(want) {
		t.Fatalf("wire-side commitment != read-back commitment\n  wire:     %s\n  readback: %s",
			got.Acc.Hex(), want.Hex())
	}
	t.Logf("LtHash MVP verified: table=%s rows=%d digest=%s", table, got.Rows, got.Acc.Hex())
}
