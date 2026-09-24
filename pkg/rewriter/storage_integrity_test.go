package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"

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
		got.GetContractVersion() != StorageIntegrityContractV2 || len(got.GetReservedDatabases()) != 3 {
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
	if strings.Join(got.GetReservedDatabases(), ",") != "hg_safe,hg_unsafe,hg_promote" {
		t.Fatalf("reserved databases = %v, want hg_safe, hg_unsafe, hg_promote", got.GetReservedDatabases())
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
// 2026-09-24 §6.2: bare hg_safe, hg_unsafe and _hg_row_id are always scrubbed.
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
