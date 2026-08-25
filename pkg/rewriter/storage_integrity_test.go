package rewriter

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/housegate/rewriter-proto/gen/pb"
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
		Tables:          []StorageIntegrityTable{{TableID: "db1.t", SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"}},
		DefaultReadMode: ReadModeSafe,
		ReadState:       rs,
	}
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
	if got, err := buildStorageIntegrityArgs(StorageIntegrityOptions{}, ReadModeSafe); got != nil || err != nil {
		t.Fatalf("no tables → nil args, got %v %v", got, err)
	}
	rs := &fakeReadState{parts: map[string][]string{"db1.t": {"all_1_1_0", "all_2_2_0"}}}
	got, err := buildStorageIntegrityArgs(siOpts(rs), ReadModeSafe)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE || got.GetReservedRowIdColumn() != "_hg_row_id" ||
		got.GetContractVersion() != StorageIntegrityContractV1 {
		t.Fatalf("args = %v", got)
	}
	tbl := got.GetTables()["db1.t"]
	if tbl.GetSafeTable() != "hg_safe.db1__t" || tbl.GetUnsafeTable() != "hg_unsafe.db1__t" || len(tbl.GetExcludedUnsafeParts()) != 0 {
		t.Fatalf("safe mode table = %v (must not consult the port)", tbl)
	}
	if len(rs.calls) != 0 {
		t.Fatalf("safe mode must not call the port: %v", rs.calls)
	}
	got, err = buildStorageIntegrityArgs(siOpts(rs), ReadModeUnsafeLatest)
	if err != nil {
		t.Fatal(err)
	}
	if parts := got.GetTables()["db1.t"].GetExcludedUnsafeParts(); len(parts) != 2 || parts[0] != "all_1_1_0" {
		t.Fatalf("excluded = %v", parts)
	}
	if got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST {
		t.Fatalf("mode = %v", got.GetReadMode())
	}
	if got, err := buildStorageIntegrityArgs(siOpts(rs), ""); err != nil || got.GetReadMode() != pb.StorageIntegrityArgs_READ_MODE_SAFE {
		t.Fatalf("empty mode must mean safe: %v %v", got, err)
	}
}

func TestBuildStorageIntegrityArgs_unsafeLatestWithoutPortIsRejected(t *testing.T) {
	_, err := buildStorageIntegrityArgs(siOpts(nil), ReadModeUnsafeLatest)
	var rej *RejectedError
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "unsafe_latest") {
		t.Fatalf("err = %v, want RejectedError about unsafe_latest", err)
	}
	journalErr := errors.New("journal locked")
	_, err = buildStorageIntegrityArgs(siOpts(&fakeReadState{err: journalErr}), ReadModeUnsafeLatest)
	if !errors.As(err, &rej) || !strings.Contains(rej.Message, "journal locked") {
		t.Fatalf("port error must surface as RejectedError: %v", err)
	}
	if !errors.Is(err, journalErr) {
		t.Fatalf("port rejection must preserve the read-state cause: %v", err)
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
	s := NewStorageIntegrityScrubber(StorageIntegrityOptions{Tables: []StorageIntegrityTable{
		{TableID: "db1.t", SafeTable: "hg_safe.db1__t", UnsafeTable: "hg_unsafe.db1__t"},
	}})
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
	if NewStorageIntegrityScrubber(StorageIntegrityOptions{}) != nil {
		t.Error("an empty SI surface must produce no scrubber")
	}
}
