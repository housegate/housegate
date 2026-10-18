package payloadexec

import (
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
)

// ledgerManifest replays two blocks into a table partitioned by shard, so one
// partition holds two parts and the other one (spec 2026-10-14 §8.5 step 1: M
// is the multiset of part row hashes S lists for (T, p), R its root).
func ledgerManifest(t *testing.T) replay.SafeSnapshotManifest {
	t.Helper()
	e := New(testNetwork, TableSchema{
		TableID:     testTable,
		PartitionBy: "shard",
		Columns: []lthash.Column{
			{Name: "shard", Type: "String"},
			{Name: "name", Type: "String"},
			{Name: "balance", Type: "UInt64"},
		},
	})
	snap := genesis(t, e)
	snap, _ = applyInsert(t, e, snap, "stmt-1", "p1", "shard,name,balance\na,alice,10\nb,bob,20\n")
	snap, _ = applyInsert(t, e, snap, "stmt-2", "p2", "shard,name,balance\na,carol,30\n")
	if len(snap.Tables) != 1 || len(snap.Tables[0].PartitionRoots) != 2 || len(snap.Tables[0].ActiveParts) != 3 {
		t.Fatalf("ledger manifest has %d tables, want one table with 2 partition roots and 3 parts: %+v", len(snap.Tables), snap.Tables)
	}
	return snap
}

// copyLedgerManifest copies the table slices a test mutates, so a mutation
// never reaches the replayed manifest.
func copyLedgerManifest(m replay.SafeSnapshotManifest) replay.SafeSnapshotManifest {
	m.Tables = slices.Clone(m.Tables)
	for i := range m.Tables {
		m.Tables[i].PartitionRoots = slices.Clone(m.Tables[i].PartitionRoots)
		m.Tables[i].ActiveParts = slices.Clone(m.Tables[i].ActiveParts)
	}
	return m
}

// TestVerifyLedgerIsTheExecutorsLedgerCheck pins the exported ledger check of
// spec 2026-10-14 §8.4 ("every partition root equals the sum of its listed
// part row hashes"): it accepts what the executor replays and refuses, with
// the executor's own texts, a root that is not its parts' sum, an orphan part
// and a part hash that is not hex; the unexported check the executor applies
// to every previous snapshot answers exactly the same.
func TestVerifyLedgerIsTheExecutorsLedgerCheck(t *testing.T) {
	good := ledgerManifest(t)
	if err := VerifyLedger(good); err != nil {
		t.Fatalf("VerifyLedger(replayed manifest) = %v", err)
	}
	if err := verifyLedger(good); err != nil {
		t.Fatalf("verifyLedger(replayed manifest) = %v", err)
	}
	table := good.Tables[0]
	single := ""
	counts := map[string]int{}
	for _, p := range table.ActiveParts {
		counts[p.PartitionID]++
	}
	for id, n := range counts {
		if n == 1 {
			single = id
		}
	}
	if single == "" {
		t.Fatalf("no single-part partition in %+v", table.ActiveParts)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*replay.TableManifest)
		want   string
	}{
		{
			name:   "a root that is not its parts' sum",
			mutate: func(tm *replay.TableManifest) { tm.PartitionRoots[0].Root = lthashHex(lthash.New()) },
			want:   "table " + testTable + " partition " + table.PartitionRoots[0].PartitionID + ": parts sum != root",
		},
		{
			name: "a part whose partition declares no root",
			mutate: func(tm *replay.TableManifest) {
				tm.PartitionRoots = slices.DeleteFunc(tm.PartitionRoots, func(pc replay.PartitionCommitment) bool { return pc.PartitionID == single })
			},
			want: "table " + testTable + " partition " + single + ": active parts present with no partition root",
		},
		{
			name:   "a part hash that is not hex",
			mutate: func(tm *replay.TableManifest) { tm.ActiveParts[0].PartRowLtHash = "0xzz" },
			want:   "table " + testTable + " part " + table.ActiveParts[0].PartName + ": decode lthash hex: encoding/hex: invalid byte: U+007A 'z'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := copyLedgerManifest(good)
			tc.mutate(&m.Tables[0])
			err := VerifyLedger(m)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("VerifyLedger = %v, want %q", err, tc.want)
			}
			if inner := verifyLedger(m); inner == nil || inner.Error() != err.Error() {
				t.Fatalf("verifyLedger = %v, want the exported check's %q", inner, err)
			}
		})
	}
	if err := VerifyLedger(good); err != nil {
		t.Fatalf("a refused copy changed the replayed manifest: %v", err)
	}
}

// TestSumPartRowLtHashesEqualsThePartitionRoot is spec 2026-10-14 §8.5 step 6's
// arithmetic: the sum of the part row hashes a manifest lists for a partition
// is its root, in any order, and a repeated hash counts once per occurrence.
func TestSumPartRowLtHashesEqualsThePartitionRoot(t *testing.T) {
	table := ledgerManifest(t).Tables[0]
	byPartition := map[string][]string{}
	for _, p := range table.ActiveParts {
		byPartition[p.PartitionID] = append(byPartition[p.PartitionID], p.PartRowLtHash)
	}
	sawTwoParts := false
	for _, pc := range table.PartitionRoots {
		hashes := byPartition[pc.PartitionID]
		got, err := SumPartRowLtHashes(hashes)
		if err != nil || got != pc.Root {
			t.Fatalf("SumPartRowLtHashes(partition %s, %d parts) = %q, %v; want the root %q", pc.PartitionID, len(hashes), got, err, pc.Root)
		}
		reversed := slices.Clone(hashes)
		slices.Reverse(reversed)
		if again, err := SumPartRowLtHashes(reversed); err != nil || again != pc.Root {
			t.Fatalf("SumPartRowLtHashes(reversed partition %s) = %q, %v; the order must not matter", pc.PartitionID, again, err)
		}
		sawTwoParts = sawTwoParts || len(hashes) == 2
	}
	if !sawTwoParts {
		t.Fatalf("want a partition with two parts, got %v", byPartition)
	}

	h := table.ActiveParts[0].PartRowLtHash
	once, err := SumPartRowLtHashes([]string{h})
	if err != nil || once != h {
		t.Fatalf("SumPartRowLtHashes([h]) = %q, %v; want h itself", once, err)
	}
	if !strings.HasPrefix(once, "0x") || len(once) != 2+2*lthash.Size || strings.ToLower(once) != once {
		t.Fatalf("SumPartRowLtHashes form = %.20q… (len %d), want 0x and %d lowercase hex digits", once, len(once), 2*lthash.Size)
	}
	twice, err := SumPartRowLtHashes([]string{h, h})
	if err != nil || twice == once {
		t.Fatalf("SumPartRowLtHashes([h, h]) = %.20q…, %v; a repeated part must count twice", twice, err)
	}
	upper, err := SumPartRowLtHashes([]string{"0x" + strings.ToUpper(h[2:])})
	if err != nil || upper != once {
		t.Fatalf("SumPartRowLtHashes(uppercase h) = %.20q…, %v; want the lowercase form of h", upper, err)
	}
}

// TestSumPartRowLtHashesOfNothingIsTheEmptyRoot pins the sum of no parts: the
// zero accumulator, which is the root VerifyLedger accepts for a declared
// partition that has no part.
func TestSumPartRowLtHashesOfNothingIsTheEmptyRoot(t *testing.T) {
	zero := lthashHex(lthash.New())
	for _, in := range [][]string{nil, {}} {
		got, err := SumPartRowLtHashes(in)
		if err != nil || got != zero {
			t.Fatalf("SumPartRowLtHashes(%#v) = %.20q…, %v; want the zero accumulator", in, got, err)
		}
	}
	m := replay.SafeSnapshotManifest{Tables: []replay.TableManifest{{
		TableID:        testTable,
		PartitionRoots: []replay.PartitionCommitment{{TableID: testTable, PartitionID: "all", Root: zero}},
	}}}
	if err := VerifyLedger(m); err != nil {
		t.Fatalf("VerifyLedger(a declared partition without parts, root = the empty sum) = %v", err)
	}
}

// TestSumPartRowLtHashesRefusesBadHex: a hash that is not an accumulator names
// its position and wraps the executor's decode error, so a caller can still
// match the hex error underneath; nothing is summed.
func TestSumPartRowLtHashesRefusesBadHex(t *testing.T) {
	good := ledgerManifest(t).Tables[0].ActiveParts[0].PartRowLtHash
	for _, tc := range []struct {
		in    []string
		want  string
		wraps error
	}{
		{[]string{good, "0xzz"}, "part row lthash 1: decode lthash hex: encoding/hex: invalid byte: U+007A 'z'", hex.InvalidByteError('z')},
		{[]string{"0xabc"}, "part row lthash 0: decode lthash hex: encoding/hex: odd length hex string", hex.ErrLength},
		{[]string{good, good, "0x00"}, "part row lthash 2: lthash: accumulator must be exactly 2048 bytes", nil},
	} {
		got, err := SumPartRowLtHashes(tc.in)
		if err == nil || err.Error() != tc.want || got != "" {
			t.Errorf("SumPartRowLtHashes(%d hashes) = %q, %v; want \"\" and %q", len(tc.in), got, err, tc.want)
		}
		if tc.wraps != nil && !errors.Is(err, tc.wraps) {
			t.Errorf("SumPartRowLtHashes(%d hashes) = %v, which does not wrap %v", len(tc.in), err, tc.wraps)
		}
	}
}
