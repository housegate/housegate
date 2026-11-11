package integration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// partitionKeyCHCase is one admitted partition key (table admission rules v3)
// with the literal values inserted under it.
type partitionKeyCHCase struct {
	columnType string
	key        string
	values     []string
}

func partitionKeyCHCases() []partitionKeyCHCase {
	dates := []string{"'1970-01-01'", "'1970-01-03'", "'2024-01-15'", "'2024-02-29'", "'2149-06-06'"}
	date32s := []string{"'1900-01-01'", "'1969-12-31'", "'2024-01-15'", "'2299-12-31'"}
	dateTimes := []string{"'1970-01-01 00:00:00'", "'1970-01-04 23:59:59'", "'2024-01-14 15:00:00'", "'2024-01-15 23:59:59'", "'2106-02-07 06:28:15'"}
	// DateTime64 values the Native lane can represent (1677..2262 in
	// nanoseconds); TestPartitionKeyCHDateTime64OutOfRange covers the rest.
	dateTime64s := []string{"'1900-01-01 00:00:00.000'", "'1969-12-31 23:59:59.999'", "'2024-01-15 23:59:59.999'", "'2262-04-11 23:47:16.854'"}
	ints := map[string][]string{
		"UInt8": {"0", "255"}, "UInt16": {"0", "65535"}, "UInt32": {"0", "999", "1000", "4294967295"},
		"UInt64": {"0", "999", "1000", "18446744073709551615"},
		"Int8": {"-128", "127"}, "Int16": {"-32768", "32767"},
		"Int32": {"-2147483648", "-1500", "-1000", "-999", "0", "1000", "2147483647"},
		"Int64": {"-9223372036854775808", "-1500", "-1", "0", "1500", "9223372036854775807"},
	}
	cases := []partitionKeyCHCase{
		{"String", "c", []string{"''", "'a'", "'it\\'s'", "'with space'", "'tuple()'"}},
		{"Date", "c", dates}, {"Date32", "c", date32s}, {"DateTime('UTC')", "c", dateTimes},
		{"DateTime64(0, 'UTC')", "c", []string{"'1900-01-01 00:00:00'", "'2262-04-11 23:47:16'"}},
		{"DateTime64(3, 'UTC')", "c", dateTime64s},
		{"DateTime64(9, 'UTC')", "c", []string{"'1969-12-31 23:59:59.999999999'", "'2262-04-11 23:47:16.854775807'"}},
	}
	for _, t := range []string{"UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64"} {
		cases = append(cases, partitionKeyCHCase{t, "c", ints[t]})
	}
	for _, f := range []string{"toYYYYMM", "toYYYYMMDD"} {
		cases = append(cases,
			partitionKeyCHCase{"Date", f + "(c)", dates}, partitionKeyCHCase{"Date32", f + "(c)", date32s},
			partitionKeyCHCase{"DateTime('UTC')", f + "(c)", dateTimes}, partitionKeyCHCase{"DateTime64(3, 'UTC')", f + "(c)", dateTime64s})
	}
	for _, f := range []string{"toDate", "toStartOfMonth", "toMonday"} {
		cases = append(cases, partitionKeyCHCase{"Date", f + "(c)", dates}, partitionKeyCHCase{"DateTime('UTC')", f + "(c)", dateTimes})
	}
	cases = append(cases, partitionKeyCHCase{"DateTime('UTC')", "toStartOfDay(c)", dateTimes})
	maxima := map[string]uint64{"UInt32": math.MaxUint32, "UInt64": math.MaxUint64, "Int32": math.MaxInt32, "Int64": math.MaxInt64}
	for _, t := range []string{"UInt32", "UInt64", "Int32", "Int64"} {
		for _, n := range []uint64{2, 1000, maxima[t]} {
			cases = append(cases, partitionKeyCHCase{t, "intDiv(c, " + strconv.FormatUint(n, 10) + ")", ints[t]})
		}
	}
	return cases
}

// TestPartitionKeyCH checks every admitted partition-key shape against the
// running ClickHouse (25.8 and 26.8 in CI): ClickHouse prints the declared key
// exactly as admitted, ParsePartitionKey admits that printed form with the
// result type ClickHouse reports, and the Native lane's partition id of every
// row is p_ followed by that row's system.parts.partition text. Each case runs
// twice, the second time inserting under session_timezone = 'Asia/Tokyo',
// which must not move a UTC-typed key.
func TestPartitionKeyCH(t *testing.T) {
	conn := openDirectCH(t)
	db := "hg_partition_key"
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
	for i, tc := range partitionKeyCHCases() {
		for _, sessionTZ := range []string{"", "Asia/Tokyo"} {
			name := fmt.Sprintf("%s/%s", tc.key, tc.columnType)
			if sessionTZ != "" {
				name += "/session_tz"
			}
			t.Run(name, func(t *testing.T) {
				table := fmt.Sprintf("%s.t_%d", db, i)
				mustExec(t, conn, "DROP TABLE IF EXISTS "+table)
				mustExec(t, conn, fmt.Sprintf("CREATE TABLE %s (id UInt32, c %s) ENGINE = MergeTree PARTITION BY %s ORDER BY id", table, tc.columnType, tc.key))
				schema := payloadexec.TableSchema{TableID: table, PartitionBy: readPartitionKey(t, conn, db, fmt.Sprintf("t_%d", i)), Columns: []lthash.Column{{Name: "id", Type: "UInt32"}, {Name: "c", Type: tc.columnType}}}
				if schema.PartitionBy != tc.key {
					t.Fatalf("ClickHouse prints partition_key %q for declared %q", schema.PartitionBy, tc.key)
				}
				key, err := payloadexec.ParsePartitionKey(schema)
				if err != nil {
					t.Fatalf("ParsePartitionKey(%q): %v", schema.PartitionBy, err)
				}
				tuples := make([]string, len(tc.values))
				for j, v := range tc.values {
					tuples[j] = fmt.Sprintf("(%d, %s)", j+1, v)
				}
				ctx := context.Background()
				if sessionTZ != "" {
					ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"session_timezone": sessionTZ}))
				}
				if err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(tuples, ", "))); err != nil {
					t.Fatalf("insert: %v", err)
				}
				var keyType string
				if err := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT toTypeName(%s) FROM %s LIMIT 1", tc.key, table)).Scan(&keyType); err != nil {
					t.Fatal(err)
				}
				if keyType != key.ResultType() {
					t.Fatalf("ClickHouse key type %s, ParsePartitionKey result type %s", keyType, key.ResultType())
				}
				ids, keyTexts, payload := readPartitionRows(t, conn, table, tc.columnType)
				rows, err := nativepayload.Decode(schema, 54460, payload)
				if err != nil {
					t.Fatalf("nativepayload.Decode: %v", err)
				}
				gotParts := map[string]bool{}
				for j, row := range rows {
					if want := "p_" + keyTexts[ids[j]]; row.PartitionID != want {
						t.Errorf("row %d (%s): partition id %q, ClickHouse toString(key) %q", ids[j], tc.values[ids[j]-1], row.PartitionID, want)
					}
					gotParts[row.PartitionID] = true
				}
				wantParts := map[string]bool{}
				for _, p := range readPartitionTexts(t, conn, db, fmt.Sprintf("t_%d", i)) {
					wantParts["p_"+p] = true
					if err := key.ValidatePartitionID("p_" + p); err != nil {
						t.Errorf("ValidatePartitionID(p_%s): %v", p, err)
					}
				}
				if !reflect.DeepEqual(gotParts, wantParts) {
					t.Errorf("Native lane partitions %v, system.parts %v", sortedKeys(gotParts), sortedKeys(wantParts))
				}
			})
		}
	}
}

// TestPartitionKeyCHRefusedSpellings: declarations ClickHouse accepts whose
// printed key is refused, either because it is outside the admitted set or
// because 25.8 and 26.8 print it differently (and so the printed form is not
// the admitted spelling on at least one of them).
func TestPartitionKeyCHRefusedSpellings(t *testing.T) {
	conn := openDirectCH(t)
	db := "hg_partition_key_refused"
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
	for i, tc := range []struct{ columnType, declared string }{
		{"DateTime", "c"}, {"DateTime", "toDate(c)"}, {"DateTime64(3)", "toYYYYMM(c)"},
		{"Date", "toStartOfDay(c)"}, {"Date32", "toMonday(c)"}, {"DateTime64(3, 'UTC')", "toDate(c)"},
		{"Bool", "c"}, {"UUID", "c"}, {"FixedString(32)", "c"},
		{"DateTime('UTC')", "toDate(c, 'UTC')"}, {"DateTime('UTC')", "toStartOfWeek(c, 1)"},
		{"UInt64", "intDiv(c, 1)"}, {"UInt64", "c % 16"}, {"UInt16", "intDiv(c, 1000)"},
		{"UInt32", "intDiv(c, 4294967296)"}, {"Int32", "intDiv(c, 2147483648)"}, {"Int64", "intDiv(c, 9223372036854775808)"},
		{"UInt64", "intDiv(c, 1e3)"},
	} {
		t.Run(tc.declared+"/"+tc.columnType, func(t *testing.T) {
			table := fmt.Sprintf("r_%d", i)
			mustExec(t, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db, table))
			mustExec(t, conn, fmt.Sprintf("CREATE TABLE %s.%s (c %s) ENGINE = MergeTree PARTITION BY %s ORDER BY tuple()", db, table, tc.columnType, tc.declared))
			printed := readPartitionKey(t, conn, db, table)
			schema := payloadexec.TableSchema{TableID: db + "." + table, PartitionBy: printed, Columns: []lthash.Column{{Name: "c", Type: tc.columnType}}}
			if _, err := payloadexec.ParsePartitionKey(schema); !errors.Is(err, payloadexec.ErrPartitionKeyNotAdmitted) {
				t.Fatalf("printed key %q admitted (%v), want refused", printed, err)
			}
		})
	}
}

// TestPartitionKeyCHIdentifiers measures how ClickHouse prints a column name
// inside a partition key. A name the running version prints backquoted must be
// refused, and so must every name some supported version (25.8 or 26.8)
// quotes, even where this version does not: the declared spelling would
// otherwise depend on the replica's version.
func TestPartitionKeyCHIdentifiers(t *testing.T) {
	conn := openDirectCH(t)
	db := "hg_partition_key_ident"
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
	quotedSomewhere := map[string]bool{"all": true, "distinct": true, "from": true, "null": true, "select": true, "table": true, "values": true}
	i := 0
	for _, name := range []string{"select", "Select", "from", "values", "all", "distinct", "table", "null", "NULL", "true", "date", "index", "key", "_x", "x1", "ts"} {
		for _, shape := range []struct{ columnType, key string }{
			{"UInt64", "`%s`"}, {"DateTime('UTC')", "toYYYYMM(`%s`)"}, {"UInt64", "intDiv(`%s`, 1000)"},
		} {
			i++
			table := fmt.Sprintf("i_%d", i)
			declared := fmt.Sprintf(shape.key, name)
			mustExec(t, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", db, table))
			mustExec(t, conn, fmt.Sprintf("CREATE TABLE %s.%s (`%s` %s) ENGINE = MergeTree PARTITION BY %s ORDER BY tuple()", db, table, name, shape.columnType, declared))
			printed := readPartitionKey(t, conn, db, table)
			schema := payloadexec.TableSchema{TableID: db + "." + table, PartitionBy: printed, Columns: []lthash.Column{{Name: name, Type: shape.columnType}}}
			_, err := payloadexec.ParsePartitionKey(schema)
			quotedHere := strings.Contains(printed, "`")
			if quotedHere && !quotedSomewhere[strings.ToLower(name)] {
				t.Errorf("this ClickHouse quotes %q (%s), which the refused-name set does not list", name, printed)
			}
			wantRefused := quotedHere || quotedSomewhere[strings.ToLower(name)]
			if wantRefused != (err != nil) {
				t.Errorf("name %q printed %q: ParsePartitionKey err = %v, want refused = %v", name, printed, err, wantRefused)
			}
		}
	}
}

// TestPartitionKeyCHDateTime64OutOfRange: a DateTime64 partition value outside
// the int64-nanosecond range (here 2299, which ClickHouse stores) cannot be
// decoded without wrapping, so the Native lane refuses the payload instead of
// deriving a partition ClickHouse does not hold the row in.
func TestPartitionKeyCHDateTime64OutOfRange(t *testing.T) {
	conn := openDirectCH(t)
	db := "hg_partition_key_dt64"
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })
	for i, key := range []string{"c", "toYYYYMM(c)"} {
		table := fmt.Sprintf("%s.o_%d", db, i)
		mustExec(t, conn, fmt.Sprintf("CREATE TABLE %s (id UInt32, c DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY %s ORDER BY id", table, key))
		mustExec(t, conn, "INSERT INTO "+table+" VALUES (1, '2299-12-31 23:59:59.999')")
		schema := payloadexec.TableSchema{TableID: table, PartitionBy: key, Columns: []lthash.Column{{Name: "id", Type: "UInt32"}, {Name: "c", Type: "DateTime64(3, 'UTC')"}}}
		// The row as a correct encoder (clickhouse-client, say) sends it:
		// milliseconds since the epoch, which ClickHouse stores as 2299.
		var raw int64
		if err := conn.QueryRow(context.Background(), "SELECT toUnixTimestamp64Milli(c) FROM "+table).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if want := time.Date(2299, 12, 31, 23, 59, 59, 999000000, time.UTC); raw != want.UnixMilli() {
			t.Fatalf("stored raw %d, want %d", raw, want.UnixMilli())
		}
		ids := &proto.ColUInt32{}
		ids.Append(1)
		col := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
		col.Data = append(col.Data, proto.DateTime64(raw))
		var buf proto.Buffer
		buf.PutUVarInt(uint64(proto.ClientCodeData))
		buf.PutString("")
		if err := (proto.Block{Rows: 1, Columns: 2}).EncodeBlock(&buf, 54460, proto.Input{{Name: "id", Data: ids}, {Name: "c", Data: col}}); err != nil {
			t.Fatal(err)
		}
		payload := buf.Buf
		if _, err := nativepayload.Decode(schema, 54460, payload); err == nil || !strings.Contains(err.Error(), "outside the representable nanosecond range") {
			t.Errorf("%s: Decode = %v, want the out-of-range refusal", key, err)
		}
	}
}

func readPartitionKey(t *testing.T, conn clickhouse.Conn, db, table string) string {
	t.Helper()
	var key string
	if err := conn.QueryRow(context.Background(), "SELECT partition_key FROM system.tables WHERE database = ? AND name = ?", db, table).Scan(&key); err != nil {
		t.Fatalf("read partition_key: %v", err)
	}
	return key
}

func readPartitionTexts(t *testing.T, conn clickhouse.Conn, db, table string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT DISTINCT partition FROM system.parts WHERE database = ? AND table = ? AND active", db, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// readPartitionRows reads the table back ordered by id and returns the ids,
// toString(key) per id, and the rows re-encoded as one Native client Data
// packet (revision 54460), the payload the Native lane replays.
func readPartitionRows(t *testing.T, conn clickhouse.Conn, table, columnType string) ([]uint32, map[uint32]string, []byte) {
	t.Helper()
	var key string
	db, name, _ := strings.Cut(table, ".")
	key = readPartitionKey(t, conn, db, name)
	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT id, c, toString(%s) FROM %s ORDER BY id", key, table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ids := &proto.ColUInt32{}
	col, appendValue := nativePartitionColumn(t, columnType)
	var order []uint32
	texts := map[uint32]string{}
	for rows.Next() {
		var id uint32
		var text string
		holder := reflect.New(scanType(t, columnType))
		if err := rows.Scan(&id, holder.Interface(), &text); err != nil {
			t.Fatal(err)
		}
		ids.Append(id)
		appendValue(holder.Elem().Interface())
		order = append(order, id)
		texts[id] = text
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: len(order), Columns: 2}).EncodeBlock(&buf, 54460, proto.Input{{Name: "id", Data: ids}, {Name: "c", Data: col}}); err != nil {
		t.Fatalf("encode Native payload: %v", err)
	}
	return order, texts, append([]byte(nil), buf.Buf...)
}

func scanType(t *testing.T, columnType string) reflect.Type {
	t.Helper()
	profile, err := payloadexec.ResolveColumnProfile(columnType)
	if err != nil {
		t.Fatal(err)
	}
	return profile.GoType
}

// nativePartitionColumn returns the ch-go column for an admitted key column
// type and a function appending one read-back value to it.
func nativePartitionColumn(t *testing.T, columnType string) (proto.ColInput, func(any)) {
	t.Helper()
	switch columnType {
	case "String":
		c := &proto.ColStr{}
		return c, func(v any) { c.Append(v.(string)) }
	case "UInt8":
		c := &proto.ColUInt8{}
		return c, func(v any) { c.Append(v.(uint8)) }
	case "UInt16":
		c := &proto.ColUInt16{}
		return c, func(v any) { c.Append(v.(uint16)) }
	case "UInt32":
		c := &proto.ColUInt32{}
		return c, func(v any) { c.Append(v.(uint32)) }
	case "UInt64":
		c := &proto.ColUInt64{}
		return c, func(v any) { c.Append(v.(uint64)) }
	case "Int8":
		c := &proto.ColInt8{}
		return c, func(v any) { c.Append(v.(int8)) }
	case "Int16":
		c := &proto.ColInt16{}
		return c, func(v any) { c.Append(v.(int16)) }
	case "Int32":
		c := &proto.ColInt32{}
		return c, func(v any) { c.Append(v.(int32)) }
	case "Int64":
		c := &proto.ColInt64{}
		return c, func(v any) { c.Append(v.(int64)) }
	case "Date":
		c := &proto.ColDate{}
		return c, func(v any) { c.Append(v.(time.Time)) }
	case "Date32":
		c := &proto.ColDate32{}
		return c, func(v any) { c.Append(v.(time.Time)) }
	case "DateTime('UTC')":
		c := &proto.ColDateTime{Location: time.UTC}
		return c, func(v any) { c.Append(v.(time.Time)) }
	}
	if p, err := payloadexec.ResolveColumnProfile(columnType); err == nil && p.Family == payloadexec.FamilyDateTime64 && p.Timezone == "UTC" {
		// Encode the raw value from seconds and nanoseconds: ch-go's Append
		// goes through UnixNano and wraps outside 1677..2262.
		c := new(proto.ColDateTime64).WithPrecision(proto.Precision(p.Precision)).WithLocation(time.UTC)
		perSecond := int64(1)
		for i := 0; i < p.Precision; i++ {
			perSecond *= 10
		}
		return c, func(v any) {
			ts := v.(time.Time)
			c.Data = append(c.Data, proto.DateTime64(ts.Unix()*perSecond+int64(ts.Nanosecond())/(1_000_000_000/perSecond)))
		}
	}
	t.Fatalf("no Native column for %s", columnType)
	return nil, nil
}
