package chexec

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// TestNewMaterializerInRefusesABadDatabaseOrPrefix pins the two refusals of
// the database-and-prefix materializer (spec 2026-10-14 §8.5, last paragraph):
// the database must be a plain lowercase ClickHouse name and the prefix a
// bounded token, because both are interpolated into every scratch statement.
// The database is checked first.
func TestNewMaterializerInRefusesABadDatabaseOrPrefix(t *testing.T) {
	for _, database := range []string{"", "Hg_rstage", "1db", "hg-rstage", "hg.rstage", "hg`x", "hg_rstage\n", "hg rstage"} {
		m, err := NewMaterializerIn("net-1", panicConn{}, database, "")
		want := fmt.Sprintf("chexec: materializer database %q must match [a-z_][a-z0-9_]*", database)
		if m != nil || err == nil || err.Error() != want {
			t.Errorf("NewMaterializerIn(database %q) = %v, %v; want nil, %q", database, m, err, want)
		}
	}
	for _, prefix := range []string{"", strings.Repeat("a", 97), "a.b", "a`b", "a b", "a/b", "m1_\n", "é"} {
		m, err := NewMaterializerIn("net-1", panicConn{}, "hg_rstage", prefix)
		want := fmt.Sprintf("chexec: materializer prefix %q must match [A-Za-z0-9_-]{1,96}", prefix)
		if m != nil || err == nil || err.Error() != want {
			t.Errorf("NewMaterializerIn(prefix %q) = %v, %v; want nil, %q", prefix, m, err, want)
		}
	}
	for _, tc := range []struct{ database, prefix string }{
		{"hg_rstage", "m-1_boot_"},
		{"_x", strings.Repeat("Z", 96)},
		{"hg_rstage", "-"},
	} {
		if m, err := NewMaterializerIn("net-1", panicConn{}, tc.database, tc.prefix); err != nil || m == nil {
			t.Errorf("NewMaterializerIn(%q, %q) = %v, %v; want a materializer", tc.database, tc.prefix, m, err)
		}
	}
}

// refusingCreateConn records every Exec like recordingConn and refuses each
// CREATE TABLE, so a test sees the scratch name in the materializer's error.
type refusingCreateConn struct{ recordingConn }

func (c *refusingCreateConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	if strings.HasPrefix(query, "CREATE TABLE ") {
		return errors.New("refused")
	}
	return nil
}

// TestMaterializerScratchStatementsNameTheirTable pins, without ClickHouse,
// the scratch table every statement of a materializer names: NewMaterializer
// keeps today's unqualified _hg_replay_scratch_<n> in the connection's default
// database byte for byte (statements and error texts), and NewMaterializerIn
// qualifies `<database>`.`<prefix>replay_<n>`; n counts per materializer, and
// the scratch is dropped on exit when the INSERT or the CREATE fails.
func TestMaterializerScratchStatementsNameTheirTable(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	st := replay.PreparedStatement{
		Statement: replay.Statement{StatementID: "0xabc:1:n", TargetTableID: "db.t", PayloadFormat: payloadexec.PayloadFormatCSVWithNames},
		Payload:   []byte("v\n1\n"),
	}
	for _, tc := range []struct {
		name     string
		database string   // "" builds NewMaterializer
		refs     []string // the scratch of the first and second statement, as SQL names it
		first    string   // the first scratch, as an error names it
	}{
		{"NewMaterializer", "", []string{"`_hg_replay_scratch_1`", "`_hg_replay_scratch_2`"}, "_hg_replay_scratch_1"},
		{"NewMaterializerIn", "hg_rstage", []string{"`hg_rstage`.`m-1_replay_1`", "`hg_rstage`.`m-1_replay_2`"}, "hg_rstage.m-1_replay_1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			materializer := func(conn clickhouse.Conn) *Materializer {
				if tc.database == "" {
					return NewMaterializer("net-1", conn)
				}
				m, err := NewMaterializerIn("net-1", conn, tc.database, "m-1_")
				if err != nil {
					t.Fatalf("NewMaterializerIn: %v", err)
				}
				return m
			}
			conn := &recordingConn{}
			m := materializer(conn)
			var want []string
			for i, ref := range tc.refs {
				// recordingConn refuses every batch, so each Materialize stops at
				// the INSERT after its CREATE and still drops its scratch.
				if _, err := m.Materialize(context.Background(), schema, st); err == nil || err.Error() != "prepare insert batch: unused" {
					t.Fatalf("Materialize %d = %v, want the recording connection's batch refusal", i+1, err)
				}
				want = append(want,
					"CREATE TABLE "+ref+" (_hg_row_id FixedString(32), `v` UInt64) ENGINE = MergeTree ORDER BY tuple()",
					"DROP TABLE IF EXISTS "+ref)
			}
			if !slices.Equal(conn.execs, want) {
				t.Fatalf("statements = %q, want %q", conn.execs, want)
			}

			refusing := &refusingCreateConn{}
			_, err := materializer(refusing).Materialize(context.Background(), schema, st)
			if wantErr := "create scratch table " + tc.first + ": refused"; err == nil || err.Error() != wantErr {
				t.Fatalf("Materialize with a refused CREATE = %v, want %q", err, wantErr)
			}
			if last := refusing.execs[len(refusing.execs)-1]; last != "DROP TABLE IF EXISTS "+tc.refs[0] {
				t.Fatalf("last statement after a refused CREATE = %q, want the drop of %s", last, tc.refs[0])
			}
		})
	}
}

// TestNewMaterializerInWithStoragePolicyRefusesUnsafeNames keeps policy text
// out of SQL unless it is a bounded identifier; the legacy name checks run first.
func TestNewMaterializerInWithStoragePolicyRefusesUnsafeNames(t *testing.T) {
	for _, policy := range []string{"", "1member", "a-b", "a.b", "a'b", "a`b", "a\\b", "a\nb", strings.Repeat("a", 129), "成员"} {
		m, err := NewMaterializerInWithStoragePolicy("net-1", panicConn{}, "hg_rstage", "m1_", policy)
		want := fmt.Sprintf("chexec: materializer storage policy %q must match [A-Za-z_][A-Za-z0-9_]{0,127}", policy)
		if m != nil || err == nil || err.Error() != want {
			t.Fatalf("policy %q: got %v, %v; want nil, %q", policy, m, err, want)
		}
	}
	for _, policy := range []string{"member", "_member", "Member_1", strings.Repeat("a", 128)} {
		if m, err := NewMaterializerInWithStoragePolicy("net-1", panicConn{}, "hg_rstage", "m1_", policy); m == nil || err != nil {
			t.Fatalf("policy %q: %v, %v", policy, m, err)
		}
	}
	if _, err := NewMaterializerInWithStoragePolicy("net-1", panicConn{}, "Bad", "m1_", "member"); err == nil || !strings.Contains(err.Error(), "materializer database") {
		t.Fatalf("database validation: %v", err)
	}
	if _, err := NewMaterializerInWithStoragePolicy("net-1", panicConn{}, "hg_rstage", "", "member"); err == nil || !strings.Contains(err.Error(), "materializer prefix") {
		t.Fatalf("prefix validation: %v", err)
	}
}

// TestMaterializerStoragePolicyIsExplicitOnlyForTheNewConstructor pins all
// legacy CREATE bytes while requiring the new constructor's explicit policy.
func TestMaterializerStoragePolicyIsExplicitOnlyForTheNewConstructor(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	st := replay.PreparedStatement{Statement: replay.Statement{StatementID: "0xabc:1:n", TargetTableID: "db.t", PayloadFormat: payloadexec.PayloadFormatCSVWithNames}, Payload: []byte("v\n1\n")}
	for _, tc := range []struct{ name, policy, ref string }{
		{"legacy", "", "`_hg_replay_scratch_1`"}, {"named", "", "`hg_rstage`.`m1_replay_1`"}, {"policy", "member", "`hg_rstage`.`m1_replay_1`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &recordingConn{}
			var m *Materializer
			var err error
			switch tc.name {
			case "legacy":
				m = NewMaterializer("net-1", conn)
			case "named":
				m, err = NewMaterializerIn("net-1", conn, "hg_rstage", "m1_")
			default:
				m, err = NewMaterializerInWithStoragePolicy("net-1", conn, "hg_rstage", "m1_", tc.policy)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = m.Materialize(context.Background(), schema, st); err == nil || err.Error() != "prepare insert batch: unused" {
				t.Fatalf("Materialize = %v", err)
			}
			want := "CREATE TABLE " + tc.ref + " (_hg_row_id FixedString(32), `v` UInt64) ENGINE = MergeTree ORDER BY tuple()"
			if tc.policy != "" {
				want += " SETTINGS storage_policy = '" + tc.policy + "'"
			}
			if !slices.Equal(conn.execs, []string{want, "DROP TABLE IF EXISTS " + tc.ref}) {
				t.Fatalf("SQL = %q, want CREATE %q and exact DROP", conn.execs, want)
			}
		})
	}
}
