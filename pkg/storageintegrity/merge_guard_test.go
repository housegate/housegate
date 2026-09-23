package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const pinnedEngine = "MergeTree ORDER BY _hg_row_id SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, index_granularity = 8192"

// fakeMergeConn is an in-memory MergeConn. It records Exec statements in order,
// answers the engine settings probe from engines and the merges probe from
// merging, and records when each probe ran relative to the execs.
type fakeMergeConn struct {
	execs         []string
	execErr       error
	engines       map[MergeTable]string
	merging       []MergeTable
	settingsErr   error
	mergesErr     error
	settingsAt    int
	mergesAt      int
	settingsRan   bool
	mergesProbeOK bool
}

func newFakeMergeConn() *fakeMergeConn {
	return &fakeMergeConn{engines: map[MergeTable]string{
		{Database: "hg_safe", Table: "events"}:   pinnedEngine,
		{Database: "hg_unsafe", Table: "events"}: pinnedEngine,
	}}
}

func (c *fakeMergeConn) Exec(_ context.Context, query string, _ ...any) error {
	if c.execErr != nil {
		return c.execErr
	}
	c.execs = append(c.execs, query)
	return nil
}

func (c *fakeMergeConn) Query(_ context.Context, query string, _ ...any) (MergeRows, error) {
	switch {
	case strings.Contains(query, "system.tables"):
		c.settingsRan = true
		c.settingsAt = len(c.execs)
		if c.settingsErr != nil {
			return nil, c.settingsErr
		}
		var rows [][]string
		for t, e := range c.engines {
			rows = append(rows, []string{t.Database, t.Table, e})
		}
		return &fakeMergeRows{rows: rows}, nil
	case strings.Contains(query, "system.merges"):
		c.mergesProbeOK = true
		c.mergesAt = len(c.execs)
		if c.mergesErr != nil {
			return nil, c.mergesErr
		}
		var rows [][]string
		for _, t := range c.merging {
			rows = append(rows, []string{t.Database, t.Table})
		}
		return &fakeMergeRows{rows: rows}, nil
	}
	return nil, errors.New("unexpected query: " + query)
}

type fakeMergeRows struct {
	rows [][]string
	i    int
}

func (r *fakeMergeRows) Next() bool { return r.i < len(r.rows) }
func (r *fakeMergeRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	if len(dest) != len(row) {
		return errors.New("scan arity mismatch")
	}
	for i := range dest {
		*(dest[i].(*string)) = row[i]
	}
	return nil
}
func (r *fakeMergeRows) Err() error   { return nil }
func (r *fakeMergeRows) Close() error { return nil }

func guardFixture(conn MergeConn) *MergeGuard {
	return NewMergeGuard(conn, []MergeTable{
		{Database: "hg_safe", Table: "events"},
		{Database: "hg_unsafe", Table: "events"},
	})
}

func TestMergeGuard_BuildStartMergesStatements(t *testing.T) {
	stmts := guardFixture(newFakeMergeConn()).BuildStartMergesStatements()
	want := []string{"SYSTEM START MERGES `hg_safe`.`events`", "SYSTEM START MERGES `hg_unsafe`.`events`"}
	if strings.Join(stmts, "\n") != strings.Join(want, "\n") {
		t.Fatalf("statements = %v, want %v", stmts, want)
	}
	for _, s := range stmts {
		if strings.Contains(s, "STOP MERGES") {
			t.Fatalf("the guard must never stop merges: %q", s)
		}
	}
}

func TestMergeGuard_ProbesAreScopedToGuardedTables(t *testing.T) {
	g := guardFixture(newFakeMergeConn())
	settings := g.BuildEngineSettingsQuery()
	if !strings.Contains(settings, "system.tables") || !strings.Contains(settings, "engine_full") ||
		!strings.Contains(settings, "name = 'events'") {
		t.Fatalf("engine settings probe = %q", settings)
	}
	merges := g.BuildVerifyMergesQuery()
	if !strings.Contains(merges, "system.merges") || !strings.Contains(merges, "table = 'events'") {
		t.Fatalf("merges probe = %q", merges)
	}
	for _, q := range []string{settings, merges} {
		if !strings.Contains(q, "'hg_safe'") || !strings.Contains(q, "'hg_unsafe'") {
			t.Fatalf("probe must name both guarded databases: %q", q)
		}
	}
	if got := NewMergeGuard(newFakeMergeConn(), nil).BuildVerifyMergesQuery(); !strings.HasSuffix(got, "WHERE 0") {
		t.Fatalf("an empty guard must match nothing, got %q", got)
	}
}

func TestMergeGuard_AssertVerifiesThenStartsThenProbes(t *testing.T) {
	conn := newFakeMergeConn()
	if err := guardFixture(conn).AssertStopMerges(context.Background()); err != nil {
		t.Fatalf("clean assert: %v", err)
	}
	if !conn.settingsRan || conn.settingsAt != 0 {
		t.Fatalf("pinned-setting probe must run before any statement (ran=%v at %d)", conn.settingsRan, conn.settingsAt)
	}
	if len(conn.execs) != 2 || !strings.HasPrefix(conn.execs[0], "SYSTEM START MERGES") {
		t.Fatalf("execs = %v", conn.execs)
	}
	if !conn.mergesProbeOK || conn.mergesAt != 2 {
		t.Fatalf("merges probe must run after both START statements (at %d)", conn.mergesAt)
	}
}

func TestMergeGuard_RefusesUnpinnedTableBeforeStartingMerges(t *testing.T) {
	for name, engine := range map[string]string{
		"no settings":      "MergeTree ORDER BY _hg_row_id",
		"nonzero":          "MergeTree ORDER BY _hg_row_id SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1, index_granularity = 8192",
		"other setting":    "MergeTree ORDER BY _hg_row_id SETTINGS index_granularity = 8192",
		"prefix lookalike": "MergeTree ORDER BY _hg_row_id SETTINGS xmax_bytes_to_merge_at_max_space_in_pool = 0",
	} {
		t.Run(name, func(t *testing.T) {
			conn := newFakeMergeConn()
			conn.engines[MergeTable{Database: "hg_unsafe", Table: "events"}] = engine
			err := guardFixture(conn).AssertStopMerges(context.Background())
			if !errors.Is(err, ErrMergeSettingNotPinned) || !strings.Contains(err.Error(), "hg_unsafe.events") {
				t.Fatalf("err = %v", err)
			}
			if len(conn.execs) != 0 {
				t.Fatalf("no START MERGES may run on an unpinned table set, got %v", conn.execs)
			}
		})
	}
}

func TestMergeGuard_RefusesMissingTable(t *testing.T) {
	conn := newFakeMergeConn()
	delete(conn.engines, MergeTable{Database: "hg_safe", Table: "events"})
	err := guardFixture(conn).AssertStopMerges(context.Background())
	if !errors.Is(err, ErrMergeSettingNotPinned) || !strings.Contains(err.Error(), "hg_safe.events (table missing)") {
		t.Fatalf("err = %v", err)
	}
	if len(conn.execs) != 0 {
		t.Fatalf("execs = %v", conn.execs)
	}
}

func TestMergeGuard_FailsClosedOnActiveMerge(t *testing.T) {
	conn := newFakeMergeConn()
	conn.merging = []MergeTable{{Database: "hg_unsafe", Table: "events"}}
	err := guardFixture(conn).AssertStopMerges(context.Background())
	if !errors.Is(err, ErrNativeMergesEnabled) || !strings.Contains(err.Error(), "hg_unsafe.events") {
		t.Fatalf("err = %v", err)
	}
}

func TestMergeGuard_IsIdempotent(t *testing.T) {
	conn := newFakeMergeConn()
	g := guardFixture(conn)
	for i := 0; i < 2; i++ {
		if err := g.AssertStopMerges(context.Background()); err != nil {
			t.Fatalf("re-assert %d: %v", i, err)
		}
	}
}

func TestMergeGuard_ErrorsSurface(t *testing.T) {
	t.Run("settings probe", func(t *testing.T) {
		conn := newFakeMergeConn()
		conn.settingsErr = errors.New("connection reset")
		if err := guardFixture(conn).AssertStopMerges(context.Background()); err == nil || len(conn.execs) != 0 {
			t.Fatalf("err=%v execs=%v", err, conn.execs)
		}
	})
	t.Run("start exec", func(t *testing.T) {
		conn := newFakeMergeConn()
		conn.execErr = errors.New("connection reset")
		if err := guardFixture(conn).AssertStopMerges(context.Background()); err == nil || conn.mergesProbeOK {
			t.Fatalf("a failed START must surface before the merges probe: err=%v", err)
		}
	})
	t.Run("merges probe", func(t *testing.T) {
		conn := newFakeMergeConn()
		conn.mergesErr = errors.New("connection reset")
		if err := guardFixture(conn).AssertStopMerges(context.Background()); err == nil {
			t.Fatal("a failed merges probe must fail closed")
		}
	})
}

func TestEngineSettingsParsesRenderedClause(t *testing.T) {
	got := engineSettings("ReplicatedMergeTree('/p/{shard}', '{replica}') PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, storage_policy = 'default'")
	if got[PinnedMergeSetting] != "0" || got["parts_to_delay_insert"] != "1000" || got["storage_policy"] != "'default'" {
		t.Fatalf("settings = %v", got)
	}
}
