package storageintegrity

import (
	"errors"
	"strings"
	"testing"
)

func TestInsertPayloadEncodingAcceptsStreamingPayloadFormats(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "implicit native client data",
			sql:  "INSERT INTO events",
			want: PayloadEncodingClickHouseNativeData,
		},
		{
			name: "explicit native",
			sql:  "INSERT INTO events FORMAT Native",
			want: PayloadEncodingClickHouseNativeData,
		},
		{
			name: "CSVWithNames in SQL still rides the Native wire",
			sql:  "INSERT INTO events FORMAT CSVWithNames",
			want: PayloadEncodingClickHouseNativeData,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := InsertPayloadEncoding(tc.sql)
			if err != nil {
				t.Fatalf("InsertPayloadEncoding: %v", err)
			}
			if got != tc.want {
				t.Fatalf("encoding = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestInsertPayloadEncodingRejectsInlineAndUnsupportedFormats(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO events VALUES (1)",
		"INSERT INTO events SELECT * FROM source",
		"INSERT INTO events WITH 1 AS id SELECT id",
		"INSERT INTO events FORMAT CSV",
		"INSERT INTO events FORMAT JSONEachRow",
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := InsertPayloadEncoding(sql); err == nil {
				t.Fatal("InsertPayloadEncoding accepted unsupported INSERT payload form")
			}
		})
	}
}

func TestParseInsertTargetHandlesTableCommentsAndStructuredQuotedNames(t *testing.T) {
	target, err := ParseInsertTarget("/*lead*/ InSeRt /*a*/ InTo /*b*/ TABLE /*c*/ `shop.prod` . `ord``ers` /*tail*/ (id) FORMAT Native")
	if err != nil {
		t.Fatal(err)
	}
	if target.Database != "shop.prod" || target.Table != "ord`ers" || !target.ExplicitDatabase {
		t.Fatalf("target = %#v", target)
	}
	if got, want := target.CanonicalID(), "`shop.prod`.`ord``ers`"; got != want {
		t.Fatalf("CanonicalID = %q, want %q", got, want)
	}
	resolved, err := ResolveInsertTarget("INSERT INTO `order.items` FORMAT Native", "shop-2")
	if err != nil || resolved.Database != "shop-2" || resolved.Table != "order.items" || resolved.ExplicitDatabase {
		t.Fatalf("resolved = %#v, %v", resolved, err)
	}
}

func TestParseInsertTargetRejectsTableFunction(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO FUNCTION file('x', Native) FORMAT Native",
		"insert /*c*/ into /*c*/ function file('x', Native) format Native",
	} {
		if _, err := ParseInsertTarget(sql); !errors.Is(err, ErrInsertIntoFunction) {
			t.Fatalf("%q: %v, want ErrInsertIntoFunction", sql, err)
		}
		if _, err := InsertPayloadEncoding(sql); !errors.Is(err, ErrInsertIntoFunction) {
			t.Fatalf("InsertPayloadEncoding(%q) = %v, want ErrInsertIntoFunction", sql, err)
		}
	}
}

func TestInsertColumnListPreservesSQLOrderAcrossComments(t *testing.T) {
	sql := "INSERT INTO TABLE `shop-2`.`order.items` /*target*/ ( /*a*/ `amount`, -- line\n id, `reg``ion` ) FORMAT Native"
	got, explicit, err := InsertColumnList(sql)
	if err != nil || !explicit || strings.Join(got, ",") != "amount,id,reg`ion" {
		t.Fatalf("InsertColumnList = %v, %v, %v", got, explicit, err)
	}
}

func TestInlineInsertSettingKeysSkipsCommentsAndPreservesKeys(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"INSERT INTO t SeTtInGs /* before */ AsYnC_InSeRt = 1 FORMAT Native", "AsYnC_InSeRt"},
		{"INSERT INTO t SETTINGS SQL_x_internal=1, /* between */ input_format_x=2 FORMAT Native", "SQL_x_internal,input_format_x"},
		{"INSERT INTO t /* SETTINGS async_insert=1 */ FORMAT Native", ""},
	}
	for _, tc := range tests {
		got, err := InlineInsertSettingKeys(tc.sql)
		if err != nil || strings.Join(got, ",") != tc.want {
			t.Fatalf("%q: keys=%v err=%v, want %q", tc.sql, got, err, tc.want)
		}
	}
}

func TestParseUseDatabaseHandlesCommentsAndEscapedQuotes(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"USE /* comment */ `shop-2`;", "shop-2"},
		{" use -- line\n `shop``prod` ", "shop`prod"},
		{"USE \"shop.prod\";", "shop.prod"},
	} {
		got, ok := ParseUseDatabase(tc.sql)
		if !ok || got != tc.want {
			t.Fatalf("%q: got %q/%v, want %q/true", tc.sql, got, ok, tc.want)
		}
	}
	if _, ok := ParseUseDatabase("USE shop SETTINGS x=1"); ok {
		t.Fatal("USE with trailing clause must not match")
	}
}

func TestStructuredSQLParserRejectsBackslashEscapedIdentifiers(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO `foo\\nbar` FORMAT Native",
		"INSERT INTO \"foo\\nbar\" FORMAT Native",
		"INSERT INTO db.`foo\\`bar` FORMAT Native",
	} {
		if _, err := ParseInsertTarget(sql); !errors.Is(err, ErrBackslashEscapedIdentifier) {
			t.Fatalf("ParseInsertTarget(%q) = %v, want ErrBackslashEscapedIdentifier", sql, err)
		}
	}

	if target, err := ParseInsertTarget("INSERT INTO foonbar FORMAT Native"); err != nil || target.Table != "foonbar" {
		t.Fatalf("plain non-colliding target = %#v, %v", target, err)
	}
	if _, _, err := InsertColumnList("INSERT INTO db.t (`foo\\nbar`) FORMAT Native"); !errors.Is(err, ErrBackslashEscapedIdentifier) {
		t.Fatalf("InsertColumnList backslash escape = %v, want ErrBackslashEscapedIdentifier", err)
	}
	if _, ok := ParseUseDatabase("USE `foo\\nbar`"); ok {
		t.Fatal("ParseUseDatabase accepted a backslash-escaped identifier")
	}
	if _, ok, err := ParseUseDatabaseStrict("USE `foo\\nbar`"); ok || !errors.Is(err, ErrBackslashEscapedIdentifier) {
		t.Fatalf("ParseUseDatabaseStrict = %v/%v, want ErrBackslashEscapedIdentifier", ok, err)
	}
}

func TestResolveInsertTargetPreservesExactQuotedSessionDatabase(t *testing.T) {
	db, ok := ParseUseDatabase("USE ` shop `")
	if !ok || db != " shop " {
		t.Fatalf("ParseUseDatabase = %q/%v, want exact quoted whitespace", db, ok)
	}
	target, err := ResolveInsertTarget("INSERT INTO orders FORMAT Native", db)
	if err != nil {
		t.Fatal(err)
	}
	if target.Database != " shop " || target.Table != "orders" {
		t.Fatalf("target = %#v, want exact session database", target)
	}
	if got, want := target.CanonicalID(), "` shop `.orders"; got != want {
		t.Fatalf("CanonicalID = %q, want %q", got, want)
	}
}
