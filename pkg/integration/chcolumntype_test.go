package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// TestColumnProfileCanonicalSpellingsSurviveClickHouse is Spec Q Q-D5's proof.
// For every admitted declaration: CREATE a table with that column type, read the
// type back from system.columns, and assert ClickHouse's own spelling equals our
// canonical form.
//
// The canonical spelling is hashed twice — lthash.EncodeRow frames it into every
// row element and tableSchemaHash digests it again — so a type ClickHouse
// renormalizes differently from us is a drift bug waiting to happen in
// verify-only DDL mode, and this round trip is the only way to find it.
func TestColumnProfileCanonicalSpellingsSurviveClickHouse(t *testing.T) {
	conn := openDirectCH(t)
	db := "hg_chcolumntype"
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })

	for i, declared := range payloadexec.AdmittedColumnTypeVectors() {
		t.Run(declared, func(t *testing.T) {
			table := fmt.Sprintf("t_%d", i)
			mustExec(t, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db, table))
			mustExec(t, conn, fmt.Sprintf(
				"CREATE TABLE %s.%s (c %s) ENGINE = MergeTree ORDER BY tuple()", db, table, declared))
			if got := readBackColumnType(t, db, table); got != declared {
				t.Fatalf("ClickHouse reports %q for declared %q; the canonical spelling and ClickHouse's disagree", got, declared)
			}
		})
	}
}

// TestColumnProfileCanonicalizationAgreesWithClickHouse proves the other half of
// Q-D5: the non-canonical spellings the profile tolerates round-trip to the same
// form ClickHouse itself normalizes them to, so the two are not merely
// self-consistent.
//
// The FixedString rows are driven through CanonicalColumnType directly rather
// than through the admitted-vector loop, because Q-D7 narrows the admitted set
// to one width while the spelling tolerance itself is unchanged.
func TestColumnProfileCanonicalizationAgreesWithClickHouse(t *testing.T) {
	conn := openDirectCH(t)
	db := "hg_chcolumntype_canon"
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })

	for i, declared := range []string{
		"FixedString( 4 )", "FixedString(+4)", "FixedString(04)",
		"FixedString( 32 )", "FixedString(+32)", "FixedString(032)",
		"DateTime( 'UTC' )",
		"DateTime64( 03 )", "DateTime64(+3)",
		"DateTime64(3,'UTC')", "DateTime64( 03 , 'UTC' )",
	} {
		t.Run(declared, func(t *testing.T) {
			want, err := payloadexec.CanonicalColumnType(declared)
			if err != nil {
				t.Fatalf("CanonicalColumnType(%q): %v", declared, err)
			}
			table := fmt.Sprintf("c_%d", i)
			mustExec(t, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db, table))
			mustExec(t, conn, fmt.Sprintf(
				"CREATE TABLE %s.%s (c %s) ENGINE = MergeTree ORDER BY tuple()", db, table, declared))
			if got := readBackColumnType(t, db, table); got != want {
				t.Fatalf("declared %q: ClickHouse normalizes to %q, CanonicalColumnType to %q", declared, got, want)
			}
		})
	}
}

func readBackColumnType(t *testing.T, database, table string) string {
	t.Helper()
	conn := openDirectCH(t)
	var got string
	row := conn.QueryRow(context.Background(),
		"SELECT type FROM system.columns WHERE database = ? AND table = ? AND name = 'c'", database, table)
	if err := row.Scan(&got); err != nil {
		t.Fatalf("read back %s.%s column type: %v", database, table, err)
	}
	return got
}
