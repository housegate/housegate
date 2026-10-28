package rewriter

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/sitable"
)

type fakeReadState struct {
	parts map[string][]string
	err   error
	calls []string
}

func (f *fakeReadState) PromotedUnsafeParts(tableID string) ([]string, error) {
	f.calls = append(f.calls, tableID)
	if f.err != nil {
		return nil, f.err
	}
	return f.parts[tableID], nil
}

func siOpts(rs StorageIntegrityReadState) StorageIntegrityOptions {
	return StorageIntegrityOptions{
		Enabled:         true,
		TableState:      sitable.NewStatic([]string{"db1.t"}, nil, "net"),
		DefaultReadMode: ReadModeSafe,
		ReadState:       rs,
	}
}

func siSnap() sitable.Snapshot {
	return sitable.NewStatic([]string{"db1.t"}, nil, "net").Current()
}

func TestParseReadMode(t *testing.T) {
	for raw, want := range map[string]ReadMode{"safe": ReadModeSafe, "'unsafe_latest'": ReadModeUnsafeLatest, `" safe "`: ReadModeSafe} {
		got, err := ParseReadMode(raw)
		if err != nil || got != want {
			t.Fatalf("ParseReadMode(%q) = %q, %v", raw, got, err)
		}
	}
	for _, bad := range []string{"", "latest", "SAFE "} {
		if _, err := ParseReadMode(bad); err == nil {
			t.Fatalf("ParseReadMode(%q) must fail", bad)
		}
	}
}

func TestBuildStorageIntegrityArgs(t *testing.T) {
	if got, err := buildStorageIntegrityArgs(StorageIntegrityOptions{}, siSnap(), ReadModeSafe, nil); got != nil || err != nil {
		t.Fatalf("disabled → nil args, got %v %v", got, err)
	}
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0", "all_2_2_0"}}}
	got, err := buildStorageIntegrityArgs(siOpts(rs), siSnap(), ReadModeSafe, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE || got.GetReservedRowIdColumn() != "_hg_row_id" ||
		got.GetContractVersion() != StorageIntegrityContractV2 || len(got.GetReservedDatabases()) != 7 {
		t.Fatalf("args = %v", got)
	}
	tbl := got.GetTables()["db1.t"]
	if tbl.GetSafeTable() != "hg_safe.db1__t" || tbl.GetUnsafeTable() != "hg_unsafe.db1__t" || len(tbl.GetExcludedUnsafeParts()) != 0 {
		t.Fatalf("safe mode table = %v (must not consult the port)", tbl)
	}
	got, err = buildStorageIntegrityArgs(siOpts(rs), siSnap(), ReadModeUnsafeLatest, map[string][]string{"db1.t": {"all_1_1_0", "all_2_2_0"}})
	if err != nil {
		t.Fatal(err)
	}
	if parts := got.GetTables()["db1.t"].GetExcludedUnsafeParts(); len(parts) != 2 || parts[0] != "all_1_1_0" {
		t.Fatalf("excluded = %v", parts)
	}
	if got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST {
		t.Fatalf("mode = %v", got.GetReadMode())
	}
	if got, err := buildStorageIntegrityArgs(siOpts(rs), siSnap(), "", nil); err != nil || got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE {
		t.Fatalf("empty mode must mean safe: %v %v", got, err)
	}
	if len(rs.calls) != 0 {
		t.Fatalf("building args must never call the port: %v", rs.calls)
	}
}

// TestBuildStorageIntegrityArgs_EmptyTableMapStillSendsV2 is spec 2026-09-24
// H6: an enabled deployment with no Active table still sends the contract,
// so the engine's catch-all refuses session SET and SYSTEM.
func TestBuildStorageIntegrityArgs_EmptyTableMapStillSendsV2(t *testing.T) {
	opts := StorageIntegrityOptions{Enabled: true, TableState: sitable.NewFake(sitable.Pending)}
	got, err := buildStorageIntegrityArgs(opts, opts.TableState.Current(), ReadModeSafe, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.GetContractVersion() != StorageIntegrityContractV2 || len(got.GetTables()) != 0 || got.GetTables() == nil {
		t.Fatalf("args = %v, want V2 with an empty (non-nil) table map", got)
	}
	// With no Active table the engines learn the protected databases only
	// from reserved_databases, so they must be sent anyway.
	if strings.Join(got.GetReservedDatabases(), ",") != "hg_safe,hg_unsafe,hg_promote,hg_member_unsafe,hg_replica,hg_rstage,hg_xfer" {
		t.Fatalf("reserved databases = %v, want the three SNode databases and the four stage-3 member databases", got.GetReservedDatabases())
	}
}

func TestBuildStorageIntegrityArgs_ListsOnlyActiveTables(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "db1.a", Status: sitable.Active},
		sitable.Table{ID: "db1.p", Status: sitable.Pending},
		sitable.Table{ID: "db1.r", Status: sitable.Refused},
		sitable.Table{ID: "db1.g", Status: sitable.Gone},
	)
	got, err := buildStorageIntegrityArgs(StorageIntegrityOptions{Enabled: true, TableState: fake}, fake.Current(), ReadModeSafe, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.GetTables()) != 1 || got.GetTables()["db1.a"].GetSafeTable() != "hg_safe.db1__a" {
		t.Fatalf("tables = %v, want only the Active db1.a", got.GetTables())
	}
}

func TestPromotedPartsFor(t *testing.T) {
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}}}
	got, err := promotedPartsFor(rs, []string{"db1.t", "db1.u"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got["db1.t"]) != 1 || strings.Join(rs.calls, ",") != "db1.t,db1.u" {
		t.Fatalf("parts = %v calls = %v, want exactly the accessed tables asked and only non-empty kept", got, rs.calls)
	}
	journalErr := errors.New("journal locked")
	_, err = promotedPartsFor(&fakeReadState{err: journalErr}, []string{"db1.t"})
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "journal locked") || !errors.Is(err, journalErr) {
		t.Fatalf("port error must surface as RejectedError preserving the cause: %v", err)
	}
}

func TestBuildStorageIntegrityArgs_unsafeLatestWithoutPortIsRejected(t *testing.T) {
	_, err := buildStorageIntegrityArgs(siOpts(nil), siSnap(), ReadModeUnsafeLatest, nil)
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "unsafe_latest") {
		t.Fatalf("err = %v, want RejectedError about unsafe_latest", err)
	}
}

func TestReadModeContext(t *testing.T) {
	if _, ok := ReadModeFromContext(context.Background()); ok {
		t.Fatal("empty ctx must report no mode")
	}
	ctx := WithReadMode(context.Background(), ReadModeUnsafeLatest)
	if m, ok := ReadModeFromContext(ctx); !ok || m != ReadModeUnsafeLatest {
		t.Fatalf("got %q %v", m, ok)
	}
}

func TestStorageIntegrityScrubber(t *testing.T) {
	s := NewStorageIntegrityScrubber(siSnap())
	for _, tc := range []struct{ in, want string }{
		{"Table hg_safe.db1__t does not exist", "Table db1.t does not exist"},
		{"Missing columns: '_hg_row_id' while processing hg_unsafe.db1__t",
			"Missing columns: '<storage-integrity>' while processing db1.t"},
		{"Database hg_safe does not exist", "Database <storage-integrity> does not exist"},
		{"Table other.u does not exist", "Table other.u does not exist"},
		{"", ""},
	} {
		if got := s.Scrub(tc.in); got != tc.want {
			t.Errorf("Scrub(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	var none *StorageIntegrityScrubber
	if got := none.Scrub("Table hg_safe.db1__t does not exist"); got != "Table hg_safe.db1__t does not exist" {
		t.Errorf("a nil scrubber must be a no-op, got %q", got)
	}
}

// TestStorageIntegrityScrubber_EmptySnapshotStillRedactsReservedNames is spec
// 2026-09-24 §6.2: the bare reserved databases (hg_safe, hg_unsafe, hg_promote)
// and _hg_row_id are always scrubbed.
func TestStorageIntegrityScrubber_EmptySnapshotStillRedactsReservedNames(t *testing.T) {
	s := NewStorageIntegrityScrubber(sitable.NewFake(sitable.Pending).Current())
	got := s.Scrub("Table hg_unsafe.db9__x does not exist in hg_safe (column _hg_row_id)")
	want := "Table <storage-integrity>.db9__x does not exist in <storage-integrity> (column <storage-integrity>)"
	if got != want {
		t.Fatalf("Scrub = %q, want %q", got, want)
	}
}

func TestStorageIntegrityScrubberCacheBuildsOncePerVersion(t *testing.T) {
	fake := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "db1.t", Status: sitable.Active})
	var cache StorageIntegrityScrubberCache
	first := cache.For(fake.Current())
	if cache.For(fake.Current()) != first || cache.Builds() != 1 {
		t.Fatalf("same version must reuse the scrubber; builds = %d", cache.Builds())
	}
	fake.Set(sitable.Table{ID: "db1.t", Status: sitable.Active}, sitable.Table{ID: "db1.u", Status: sitable.Active})
	second := cache.For(fake.Current())
	if second == first || cache.Builds() != 2 {
		t.Fatalf("a new version must rebuild; builds = %d", cache.Builds())
	}
	if got := second.Scrub("hg_safe.db1__u"); got != "db1.u" {
		t.Fatalf("rebuilt scrubber = %q, want db1.u", got)
	}
}

// TestStorageIntegrityScrubber_RedactsPromoteDatabase is spec 2026-09-26 T11.
func TestStorageIntegrityScrubber_RedactsPromoteDatabase(t *testing.T) {
	s := NewStorageIntegrityScrubber(sitable.NewFake(sitable.Pending).Current())
	if got := s.Scrub("Table hg_promote.db1__t does not exist"); strings.Contains(got, "hg_promote") {
		t.Fatalf("Scrub = %q, want hg_promote redacted", got)
	}
}

// TestStorageIntegrityScrubber_RedactsStage3Databases is spec 2026-10-14 §14:
// the stage-3 member databases are reserved, so an upstream message naming one
// reaches no client, even with no Active table. hg_member_unsafe is redacted
// whole, and with an Active table the qualified hg_unsafe name of that table
// still maps back to its logical id while its member-database name does not.
func TestStorageIntegrityScrubber_RedactsStage3Databases(t *testing.T) {
	s := NewStorageIntegrityScrubber(sitable.NewFake(sitable.Pending).Current())
	for _, tc := range []struct{ in, want string }{
		{"Table hg_replica.db1__t__i3 does not exist in hg_rstage", "Table <storage-integrity>.db1__t__i3 does not exist in <storage-integrity>"},
		{"Table hg_member_unsafe.db1__t does not exist", "Table <storage-integrity>.db1__t does not exist"},
		{"Database hg_xfer does not exist", "Database <storage-integrity> does not exist"},
	} {
		if got := s.Scrub(tc.in); got != tc.want {
			t.Errorf("Scrub(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	active := NewStorageIntegrityScrubber(siSnap())
	if got := active.Scrub("hg_unsafe.db1__t and hg_member_unsafe.db1__t"); got != "db1.t and <storage-integrity>.db1__t" {
		t.Fatalf("Scrub with an Active table = %q, want the unsafe name mapped back and the member name redacted", got)
	}
}

// replicaServingState is a host that governs db1.t (Active) and serves rrdb.a
// and rrdb.b, owned by indexer 2, from its member's copies (spec 2026-10-14
// §11).
func replicaServingState() *sitable.Fake {
	return sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "db1.t", Status: sitable.Active},
		sitable.Table{ID: "rrdb.a", Status: sitable.Replica, CopyTable: "rrdb__a__i3", ReplicaOf: 2},
		sitable.Table{ID: "rrdb.b", Status: sitable.Replica, CopyTable: "rrdb__b__i5", ReplicaOf: 2},
	)
}

// TestBuildStorageIntegrityArgsMapsReplicaTablesToTheCopy: a Replica table is
// listed with SafeTable = UnsafeTable = hg_replica.<CopyTable> and no excluded
// parts in either read mode; Active tables keep their hg_safe / hg_unsafe
// entries.
func TestBuildStorageIntegrityArgsMapsReplicaTablesToTheCopy(t *testing.T) {
	state := replicaServingState()
	rs := &fakeReadState{}
	parts := map[string][]string{"db1.t": {"all_1_1_0"}, "rrdb.a": {"all_2_2_0"}}
	for _, mode := range []ReadMode{ReadModeSafe, ReadModeUnsafeLatest} {
		t.Run(string(mode), func(t *testing.T) {
			got, err := buildStorageIntegrityArgs(StorageIntegrityOptions{Enabled: true, TableState: state, ReadState: rs}, state.Current(), mode, parts)
			if err != nil {
				t.Fatal(err)
			}
			tables := got.GetTables()
			if len(tables) != 3 {
				t.Fatalf("tables = %v, want db1.t, rrdb.a and rrdb.b", tables)
			}
			for id, copyTable := range map[string]string{"rrdb.a": "hg_replica.rrdb__a__i3", "rrdb.b": "hg_replica.rrdb__b__i5"} {
				tbl := tables[id]
				if tbl.GetSafeTable() != copyTable || tbl.GetUnsafeTable() != copyTable || len(tbl.GetExcludedUnsafeParts()) != 0 {
					t.Fatalf("%s = %v, want safe and unsafe %s and no excluded parts", id, tbl, copyTable)
				}
			}
			if tbl := tables["db1.t"]; tbl.GetSafeTable() != "hg_safe.db1__t" || tbl.GetUnsafeTable() != "hg_unsafe.db1__t" {
				t.Fatalf("db1.t = %v, want its hg_safe / hg_unsafe entry", tbl)
			}
		})
	}
	if len(rs.calls) != 0 {
		t.Fatalf("building args must never call the port: %v", rs.calls)
	}
}

// TestBuildStorageIntegrityArgsWithoutReplicaTablesIsUnchanged: with no
// Replica table the block is byte-identical to the one built before replica
// reads.
func TestBuildStorageIntegrityArgsWithoutReplicaTablesIsUnchanged(t *testing.T) {
	state := sitable.NewFake(sitable.Ordinary,
		sitable.Table{ID: "db1.t", Status: sitable.Active},
		sitable.Table{ID: "db1.p", Status: sitable.Pending},
		sitable.Table{ID: "db1.g", Status: sitable.Gone},
	)
	parts := map[string][]string{"db1.t": {"all_1_1_0"}}
	for _, tc := range []struct {
		mode ReadMode
		want *pb.StorageIntegrityArgs
	}{
		{ReadModeSafe, &pb.StorageIntegrityArgs{
			Tables:              map[string]*pb.StorageIntegrityArgs_Table{"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"}},
			ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
			ReservedRowIdColumn: DefaultReservedRowIDColumn,
			ContractVersion:     StorageIntegrityContractV2,
			ReservedDatabases:   sitable.ReservedDatabases(),
		}},
		{ReadModeUnsafeLatest, &pb.StorageIntegrityArgs{
			Tables:              map[string]*pb.StorageIntegrityArgs_Table{"db1.t": {SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t", ExcludedUnsafeParts: []string{"all_1_1_0"}}},
			ReadMode:            pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST,
			ReservedRowIdColumn: DefaultReservedRowIDColumn,
			ContractVersion:     StorageIntegrityContractV2,
			ReservedDatabases:   sitable.ReservedDatabases(),
		}},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			got, err := buildStorageIntegrityArgs(StorageIntegrityOptions{Enabled: true, TableState: state, ReadState: &fakeReadState{}}, state.Current(), tc.mode, parts)
			if err != nil {
				t.Fatal(err)
			}
			deterministic := proto.MarshalOptions{Deterministic: true}
			gotBytes, err := deterministic.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			wantBytes, err := deterministic.Marshal(tc.want)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(gotBytes, wantBytes) {
				t.Fatalf("args = %v, want %v", got, tc.want)
			}
		})
	}
}

// servedSnapshot overrides a snapshot's Active and Served lists, so a test can
// build the inconsistent snapshots a host must never publish.
type servedSnapshot struct {
	sitable.Snapshot
	active, served []sitable.Table
}

func (s servedSnapshot) Active() []sitable.Table { return s.active }
func (s servedSnapshot) Served() []sitable.Table { return s.served }

// TestBuildStorageIntegrityArgsRefusesATableBothActiveAndReplica: a table id
// listed both Active and Replica, or a Replica table without a copy table, is
// refused rather than mapped to either surface.
func TestBuildStorageIntegrityArgsRefusesATableBothActiveAndReplica(t *testing.T) {
	base := sitable.NewFake(sitable.Ordinary).Current()
	for name, tc := range map[string]struct {
		snap sitable.Snapshot
		id   string
	}{
		"both Active and Replica": {servedSnapshot{Snapshot: base,
			active: []sitable.Table{{ID: "rrdb.a", Status: sitable.Active}},
			served: []sitable.Table{{ID: "rrdb.a", Status: sitable.Replica, CopyTable: "rrdb__a__i3", ReplicaOf: 2}},
		}, "rrdb.a"},
		"Replica without a copy table": {servedSnapshot{Snapshot: base,
			served: []sitable.Table{{ID: "rrdb.b", Status: sitable.Replica, ReplicaOf: 2}},
		}, "rrdb.b"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := buildStorageIntegrityArgs(StorageIntegrityOptions{Enabled: true}, tc.snap, ReadModeSafe, nil)
			want := "storage-integrity table " + tc.id + " is listed both Active and Replica, or Replica without a copy table"
			var rej *RejectedError
			if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_RewriteError || rej.Message != want {
				t.Fatalf("err = %v, want RewriteError %q", err, want)
			}
		})
	}
}

// TestUnsafeLatestIsRefusedForReplicaTables is spec 2026-10-14 §11 Read modes:
// an unsafe_latest read of a Replica table is refused after the classification
// pass, naming safe mode and the owner, before the promotion port and before
// anything is returned for ClickHouse; the same read in safe mode passes, and
// unsafe_latest of an Active table still takes its two passes.
func TestUnsafeLatestIsRefusedForReplicaTables(t *testing.T) {
	const want = "storage_integrity read mode unsafe_latest is not available for rrdb.a: it is served read-only from a copy of indexer 2's published data; read it with SQL_x_read_mode='safe'"
	classified := func(sql string, ids ...string) *pb.RewriteSQLResponse {
		return acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: sql, StatementType: pb.StatementType_STATEMENT_TYPE_SELECT, OriginalAccessedTables: siAccessed(ids...)})
	}
	for name, accessed := range map[string][]string{
		"the Replica table alone":    {"rrdb.a"},
		"with an Active table":       {"db1.t", "rrdb.a"},
		"with another Replica table": {"rrdb.a", "rrdb.b"},
	} {
		t.Run(name, func(t *testing.T) {
			rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}}}
			be := &sequenceBackend{script: []*pb.RewriteSQLResponse{classified("pass1", accessed...), classified("pass2", accessed...)}}
			res, err := dynamicSIFactory(be, replicaServingState(), rs).NewRewriter(&fakeSession{}).Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT count() FROM rrdb.a", "")
			var rej *RejectedError
			if !errors.As(err, &rej) || rej.Code != pb.RewriteCode_RewriteError || rej.Message != want {
				t.Fatalf("err = %v, want RewriteError %q", err, want)
			}
			if res.SQL != "" || len(be.requests) != 1 || len(rs.calls) != 0 {
				t.Fatalf("SQL=%q passes=%d port calls=%v, want nothing returned, one pass and no port call", res.SQL, len(be.requests), rs.calls)
			}
		})
	}

	safe := &sequenceBackend{script: []*pb.RewriteSQLResponse{classified("safe pass", "rrdb.a")}}
	if res, err := dynamicSIFactory(safe, replicaServingState(), &fakeReadState{}).NewRewriter(&fakeSession{}).Rewrite(WithReadMode(context.Background(), ReadModeSafe), "SELECT count() FROM rrdb.a", ""); err != nil || res.SQL != "safe pass" {
		t.Fatalf("safe read of a Replica table = %q, %v; want it rewritten", res.SQL, err)
	}

	active := &sequenceBackend{script: []*pb.RewriteSQLResponse{classified("pass1", "db1.t"), classified("pass2", "db1.t")}}
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0"}}}
	if res, err := dynamicSIFactory(active, replicaServingState(), rs).NewRewriter(&fakeSession{}).Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT count() FROM db1.t", ""); err != nil || res.SQL != "pass2" {
		t.Fatalf("unsafe_latest of an Active table = %q, %v; want the second pass", res.SQL, err)
	}
}

// TestUnsafeLatestNeverSendsTheCopyTwice: SafeTable = UnsafeTable = the copy,
// so an engine's unsafe_latest rendering reads the copy twice (hg_safe UNION
// ALL hg_unsafe). That classification pass is never returned for ClickHouse:
// the Rewrite result is empty, no second pass runs, and the copy entry the
// engine saw carries no excluded parts.
func TestUnsafeLatestNeverSendsTheCopyTwice(t *testing.T) {
	const doubled = "SELECT count() FROM (SELECT * FROM hg_replica.rrdb__a__i3 UNION ALL SELECT * FROM hg_replica.rrdb__a__i3 WHERE _part NOT IN ())"
	be := &sequenceBackend{script: []*pb.RewriteSQLResponse{
		acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: doubled, StatementType: pb.StatementType_STATEMENT_TYPE_SELECT, OriginalAccessedTables: siAccessed("rrdb.a")}),
		acknowledgedSIResponse(&pb.RewriteSQLResponse{Code: pb.RewriteCode_Success, SqlAfterRewrite: doubled, StatementType: pb.StatementType_STATEMENT_TYPE_SELECT, OriginalAccessedTables: siAccessed("rrdb.a")}),
	}}
	rs := &fakeReadState{parts: map[string][]string{"rrdb.a": {"all_9_9_0"}}}
	res, err := dynamicSIFactory(be, replicaServingState(), rs).NewRewriter(&fakeSession{}).Rewrite(WithReadMode(context.Background(), ReadModeUnsafeLatest), "SELECT count() FROM rrdb.a", "")
	if err == nil || res.SQL != "" || strings.Contains(res.SQL, "hg_replica") {
		t.Fatalf("Rewrite = %q, %v; want a refusal and no statement for ClickHouse", res.SQL, err)
	}
	if len(be.requests) != 1 || len(rs.calls) != 0 {
		t.Fatalf("passes=%d port calls=%v, want the classification pass only", len(be.requests), rs.calls)
	}
	entry := siArgsOf(be.requests[0]).GetTables()["rrdb.a"]
	if entry.GetSafeTable() != "hg_replica.rrdb__a__i3" || entry.GetUnsafeTable() != "hg_replica.rrdb__a__i3" || len(entry.GetExcludedUnsafeParts()) != 0 {
		t.Fatalf("classification entry = %v, want the copy on both surfaces and no excluded parts", entry)
	}
}

// TestScrubberMapsTheCopyBackToItsLogicalTable: an upstream message naming a
// served copy maps back to the logical table; any other hg_replica name stays
// redacted.
func TestScrubberMapsTheCopyBackToItsLogicalTable(t *testing.T) {
	s := NewStorageIntegrityScrubber(replicaServingState().Current())
	for _, tc := range []struct{ in, want string }{
		{"Table hg_replica.rrdb__a__i3 does not exist", "Table rrdb.a does not exist"},
		{"Missing columns: 'x' while processing hg_replica.rrdb__b__i5", "Missing columns: 'x' while processing rrdb.b"},
		{"Table hg_replica.rrdb__a__i2 does not exist", "Table <storage-integrity>.rrdb__a__i2 does not exist"},
		{"Database hg_replica does not exist", "Database <storage-integrity> does not exist"},
		{"Table hg_safe.db1__t does not exist", "Table db1.t does not exist"},
	} {
		if got := s.Scrub(tc.in); got != tc.want {
			t.Errorf("Scrub(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
