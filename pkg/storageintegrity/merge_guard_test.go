package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeMergeConn is an in-memory MergeConn: it records the Exec statements in
// order and returns a configured set of "still merging" rows from Query.
type fakeMergeConn struct {
	execs     []string
	execErr   error
	merging   []MergeTable // rows the verify probe returns
	queryErr  error
	queryRan  bool
	queriedAt int // len(execs) when Query was called, to assert ordering
}

func (c *fakeMergeConn) Exec(_ context.Context, query string, _ ...any) error {
	if c.execErr != nil {
		return c.execErr
	}
	c.execs = append(c.execs, query)
	return nil
}

func (c *fakeMergeConn) Query(_ context.Context, _ string, _ ...any) (MergeRows, error) {
	c.queryRan = true
	c.queriedAt = len(c.execs)
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return &fakeMergeRows{rows: c.merging}, nil
}

type fakeMergeRows struct {
	rows []MergeTable
	i    int
}

func (r *fakeMergeRows) Next() bool { return r.i < len(r.rows) }
func (r *fakeMergeRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	if len(dest) != 2 {
		return errors.New("expected 2 scan dests")
	}
	*(dest[0].(*string)) = row.Database
	*(dest[1].(*string)) = row.Table
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

func TestMergeGuard_BuildStopMergesStatements(t *testing.T) {
	g := guardFixture(&fakeMergeConn{})
	stmts := g.BuildStopMergesStatements()
	if len(stmts) != 2 {
		t.Fatalf("expected one STOP MERGES per guarded table, got %d: %v", len(stmts), stmts)
	}
	// Both hg_safe and hg_unsafe must be covered, each backtick-quoted and
	// table-scoped (never a bare global STOP).
	joined := strings.Join(stmts, "\n")
	for _, want := range []string{"SYSTEM STOP MERGES `hg_safe`.`events`", "SYSTEM STOP MERGES `hg_unsafe`.`events`"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("statements %v missing %q", stmts, want)
		}
	}
	for _, s := range stmts {
		if strings.TrimSpace(s) == "SYSTEM STOP MERGES" {
			t.Fatal("must never emit a bare global SYSTEM STOP MERGES")
		}
	}
}

func TestMergeGuard_BuildVerifyMergesQuery(t *testing.T) {
	g := guardFixture(&fakeMergeConn{})
	q := g.BuildVerifyMergesQuery()
	if !strings.Contains(q, "system.merges") {
		t.Fatalf("verify query must probe system.merges, got %q", q)
	}
	if !strings.Contains(q, "database") || !strings.Contains(q, "table") {
		t.Fatalf("verify query must select database,table, got %q", q)
	}
	// It must filter to the guarded tables, not scan all of system.merges blindly.
	if !strings.Contains(q, "hg_safe") || !strings.Contains(q, "hg_unsafe") {
		t.Fatalf("verify query must be scoped to the guarded tables, got %q", q)
	}
}

func TestMergeGuard_AssertStopMergesEmitsThenVerifies(t *testing.T) {
	conn := &fakeMergeConn{} // no active merges
	g := guardFixture(conn)
	if err := g.AssertStopMerges(context.Background()); err != nil {
		t.Fatalf("clean AssertStopMerges: %v", err)
	}
	if len(conn.execs) != 2 {
		t.Fatalf("expected 2 STOP MERGES execs, got %d", len(conn.execs))
	}
	if !conn.queryRan {
		t.Fatal("AssertStopMerges must run the verify probe")
	}
	if conn.queriedAt != 2 {
		t.Fatalf("verify probe must run AFTER all stop statements; ran at exec count %d", conn.queriedAt)
	}
}

func TestMergeGuard_AssertStopMergesFailsClosedOnActiveMerge(t *testing.T) {
	conn := &fakeMergeConn{merging: []MergeTable{{Database: "hg_unsafe", Table: "events"}}}
	g := guardFixture(conn)
	err := g.AssertStopMerges(context.Background())
	if err == nil {
		t.Fatal("an active merge on a guarded table must fail closed")
	}
	if !errors.Is(err, ErrNativeMergesEnabled) {
		t.Fatalf("expected ErrNativeMergesEnabled, got %v", err)
	}
	if !strings.Contains(err.Error(), "hg_unsafe") {
		t.Fatalf("error must name the offending table, got %v", err)
	}
}

func TestMergeGuard_AssertStopMergesIsIdempotent(t *testing.T) {
	conn := &fakeMergeConn{}
	g := guardFixture(conn)
	for i := 0; i < 2; i++ {
		if err := g.AssertStopMerges(context.Background()); err != nil {
			t.Fatalf("re-assert %d: %v", i, err)
		}
	}
}

func TestMergeGuard_ExecErrorSurfaces(t *testing.T) {
	conn := &fakeMergeConn{execErr: errors.New("connection reset")}
	g := guardFixture(conn)
	err := g.AssertStopMerges(context.Background())
	if err == nil {
		t.Fatal("a failed STOP MERGES exec must surface an error")
	}
	if conn.queryRan {
		t.Fatal("must not run the verify probe after a failed stop exec")
	}
}
