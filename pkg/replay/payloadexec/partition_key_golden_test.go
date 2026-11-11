package payloadexec

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
)

// The golden vectors in testdata/partition_key_golden were measured on
// ClickHouse 25.8.33.6 and 26.8.36.3, each with server timezone UTC and
// Asia/Tokyo. Per case they record the declared PARTITION BY, the
// system.tables.partition_key ClickHouse printed for it, and per row the
// column value (toString(c)), the key value (toString(key)) and its type, and
// per partition the system.parts.partition text. (The literal-form evidence of
// the full measurement is omitted here; it does not concern this package.)

type goldenFile struct {
	Server struct {
		Version  string `json:"version"`
		Timezone string `json:"timezone"`
	} `json:"server"`
	Cases []goldenCase `json:"cases"`
}

type goldenCase struct {
	Name         string `json:"name"`
	ColumnType   string `json:"column_type"`
	ColumnName   string `json:"column_name"`
	PartitionBy  string `json:"partition_by"`
	PartitionKey string `json:"partition_key"`
	CreateError  string `json:"create_error"`
	InsertError  string `json:"insert_error"`
	Rows         []struct {
		CV      string `json:"cv"`
		Key     string `json:"key"`
		KeyType string `json:"key_type"`
	} `json:"rows"`
	Parts []struct {
		Partition string `json:"partition"`
	} `json:"parts"`
}

func (c goldenCase) column() string {
	if c.ColumnName != "" {
		return c.ColumnName
	}
	return "c"
}

func loadGoldenFiles(t *testing.T) map[string]goldenFile {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("testdata", "partition_key_golden", "clickhouse-*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 4 {
		t.Fatalf("want 4 golden files (25.8 and 26.8, UTC and Asia/Tokyo), got %v", paths)
	}
	out := map[string]goldenFile{}
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var f goldenFile
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		out[filepath.Base(p)] = f
	}
	return out
}

// expectedAdmittedKeys is the D2/D3 admitted set written out as spellings, for
// one column: an encoding independent of ParsePartitionKey's parser.
func expectedAdmittedKeys(column, columnType string) map[string]string {
	utcDT64 := map[string]bool{}
	for p := 0; p <= 9; p++ {
		utcDT64[fmt.Sprintf("DateTime64(%d, 'UTC')", p)] = true
	}
	in := func(set ...string) bool {
		for _, s := range set {
			if s == columnType {
				return true
			}
		}
		return false
	}
	out := map[string]string{} // spelling -> result type
	if columnType == "String" {
		out[column] = "String"
		return out
	}
	if quotedByAnyVersion(column) {
		return out
	}
	if in("UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64", "Date", "Date32", "DateTime('UTC')") || utcDT64[columnType] {
		out[column] = columnType
	}
	if in("Date", "Date32", "DateTime('UTC')") || utcDT64[columnType] {
		out["toYYYYMM("+column+")"] = "UInt32"
		out["toYYYYMMDD("+column+")"] = "UInt32"
	}
	if in("Date", "DateTime('UTC')") {
		out["toDate("+column+")"] = "Date"
		out["toStartOfMonth("+column+")"] = "Date"
		out["toMonday("+column+")"] = "Date"
	}
	if in("DateTime('UTC')") {
		out["toStartOfDay("+column+")"] = "DateTime('UTC')"
	}
	max := map[string]uint64{"UInt32": math.MaxUint32, "UInt64": math.MaxUint64, "Int32": math.MaxInt32, "Int64": math.MaxInt64}
	if m, ok := max[columnType]; ok {
		// The admitted divisors are 2..max; list the ones the vectors use.
		for _, n := range []uint64{2, 1000, m} {
			out["intDiv("+column+", "+strconv.FormatUint(n, 10)+")"] = columnType
		}
	}
	return out
}

// quotedByAnyVersion: the names 25.8 or 26.8 prints backquoted, and every name
// that is not a plain identifier (measured: identifier/* cases).
func quotedByAnyVersion(name string) bool {
	switch strings.ToLower(name) {
	case "all", "distinct", "from", "null", "select", "table", "values":
		return true
	}
	for i, r := range name {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (i > 0 && r >= '0' && r <= '9')) {
			return true
		}
	}
	return false
}

func TestPartitionKeyGoldenVectors(t *testing.T) {
	files := loadGoldenFiles(t)
	admittedResults := map[string]map[string][]string{} // case -> file -> rendered keys
	for file, f := range files {
		for _, c := range f.Cases {
			if c.CreateError != "" {
				continue // ClickHouse itself refused the declaration
			}
			schema := TableSchema{TableID: "m.t", PartitionBy: c.PartitionKey, Columns: []lthash.Column{{Name: "id", Type: "UInt32"}, {Name: c.column(), Type: c.ColumnType}}}
			wantResult, wantAdmitted := expectedAdmittedKeys(c.column(), c.ColumnType)[c.PartitionKey]
			key, err := ParsePartitionKey(schema)
			if !wantAdmitted {
				if err == nil {
					t.Errorf("%s %s: partition key %q over %s admitted, want refused", file, c.Name, c.PartitionKey, c.ColumnType)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s %s: partition key %q over %s refused: %v", file, c.Name, c.PartitionKey, c.ColumnType, err)
				continue
			}
			if key.String() != c.PartitionKey || key.ResultType() != wantResult || key.Column() != c.column() {
				t.Errorf("%s %s: parsed %q result %q column %q", file, c.Name, key.String(), key.ResultType(), key.Column())
			}
			if c.InsertError != "" {
				t.Errorf("%s %s: admitted key whose measured INSERT failed: %s", file, c.Name, c.InsertError)
				continue
			}
			parts := map[string]bool{}
			for _, p := range c.Parts {
				parts["p_"+p.Partition] = true
				if err := key.ValidatePartitionID("p_" + p.Partition); err != nil {
					t.Errorf("%s %s: ValidatePartitionID(p_%s): %v", file, c.Name, p.Partition, err)
				}
			}
			seen := map[string]bool{}
			var rendered []string
			for _, row := range c.Rows {
				if row.KeyType != key.ResultType() {
					t.Errorf("%s %s: measured key type %s, parsed result type %s", file, c.Name, row.KeyType, key.ResultType())
				}
				v, err := parseValue(c.ColumnType, row.CV)
				if err != nil {
					t.Errorf("%s %s: parse %q as %s: %v", file, c.Name, row.CV, c.ColumnType, err)
					continue
				}
				values := []any{uint32(0), v}
				id, err := key.PartitionID(values)
				if err != nil {
					t.Errorf("%s %s: PartitionID(%q): %v", file, c.Name, row.CV, err)
					continue
				}
				if id != "p_"+row.Key {
					t.Errorf("%s %s: value %q: partition id %q, ClickHouse key %q", file, c.Name, row.CV, id, row.Key)
				}
				if !parts[id] {
					t.Errorf("%s %s: value %q: partition id %q is not among system.parts %v", file, c.Name, row.CV, id, parts)
				}
				if legacy, err := PartitionIDForRow(schema, values); err != nil || legacy != id {
					t.Errorf("%s %s: PartitionIDForRow = %q, %v; want %q", file, c.Name, legacy, err, id)
				}
				seen[id] = true
				rendered = append(rendered, id)
			}
			if len(seen) != len(parts) {
				t.Errorf("%s %s: rows name partitions %v, system.parts has %v", file, c.Name, seen, parts)
			}
			if admittedResults[c.Name] == nil {
				admittedResults[c.Name] = map[string][]string{}
			}
			admittedResults[c.Name][file] = append([]string{c.PartitionKey}, rendered...)
		}
	}
	// Every admitted case must mean the same on every measured server.
	names := make([]string, 0, len(admittedResults))
	for n := range admittedResults {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		var first string
		var firstFile string
		for file, r := range admittedResults[name] {
			got := strings.Join(r, "|")
			if first == "" {
				first, firstFile = got, file
				continue
			}
			if got != first {
				t.Errorf("%s: %s gives %s, %s gives %s", name, firstFile, first, file, got)
			}
		}
	}
	if len(names) < 45 {
		t.Fatalf("only %d admitted golden cases; the vectors did not load as expected", len(names))
	}
}

// TestPartitionKeyGoldenSpellingsAgreeAcrossVersions: every admitted spelling
// is printed identically by 25.8 and 26.8, which is what makes the declared
// schema (and its hash) replica-independent.
func TestPartitionKeyGoldenSpellingsAgreeAcrossVersions(t *testing.T) {
	files := loadGoldenFiles(t)
	byCase := map[string]map[string]goldenCase{}
	for file, f := range files {
		for _, c := range f.Cases {
			if byCase[c.Name] == nil {
				byCase[c.Name] = map[string]goldenCase{}
			}
			byCase[c.Name][file] = c
		}
	}
	for name, per := range byCase {
		admittedSomewhere := false
		printed := map[string]bool{}
		for _, c := range per {
			printed[c.PartitionKey] = true
			schema := TableSchema{TableID: "m.t", PartitionBy: c.PartitionKey, Columns: []lthash.Column{{Name: "id", Type: "UInt32"}, {Name: c.column(), Type: c.ColumnType}}}
			if c.CreateError == "" {
				if _, err := ParsePartitionKey(schema); err == nil {
					admittedSomewhere = true
				}
			}
		}
		if admittedSomewhere && len(printed) != 1 {
			// A declaration two versions print differently may be admitted on
			// one version only (the printed form on the other is refused),
			// but never with two admitted spellings.
			admittedSpellings := 0
			for p := range printed {
				for _, c := range per {
					if c.PartitionKey != p {
						continue
					}
					schema := TableSchema{TableID: "m.t", PartitionBy: p, Columns: []lthash.Column{{Name: "id", Type: "UInt32"}, {Name: c.column(), Type: c.ColumnType}}}
					if _, err := ParsePartitionKey(schema); err == nil {
						admittedSpellings++
					}
					break
				}
			}
			if admittedSpellings > 1 {
				t.Errorf("%s: printed as %v, more than one admitted", name, printed)
			}
		}
	}
}
