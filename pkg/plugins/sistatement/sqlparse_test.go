package sistatement

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func testSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "shop.orders",
		PartitionBy: "region",
		Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "region", Type: "String"},
			{Name: "amount", Type: "Float64"},
		},
	}
}

func TestResolveTargetTableID(t *testing.T) {
	cases := []struct {
		sql, sessionDB, want, wantErr string
	}{
		{"INSERT INTO shop.orders FORMAT Native", "", "shop.orders", ""},
		{"INSERT INTO `shop`.`orders` (id, region, amount) FORMAT Native", "", "shop.orders", ""},
		{"/*lead*/ INSERT /*a*/ INTO /*b*/ TABLE /*c*/ `shop-2`.`order.items` FORMAT Native", "", "`shop-2`.`order.items`", ""},
		{"insert into orders format Native", "shop", "shop.orders", ""},
		{"insert into `order.items` format Native", "shop.prod", "`shop.prod`.`order.items`", ""},
		{"INSERT INTO FUNCTION file('x', Native) FORMAT Native", "shop", "", "FUNCTION"},
		{"INSERT INTO `foo\\nbar` FORMAT Native", "shop", "", "backslash-escaped"},
		{"INSERT INTO orders FORMAT Native", "", "", "database-qualified"},
		{"SELECT 1", "", "", "not an INSERT"},
	}
	for _, tc := range cases {
		got, err := resolveTargetTableID(tc.sql, tc.sessionDB)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("%q: err=%v want %q", tc.sql, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Fatalf("%q: got %q err=%v, want %q", tc.sql, got, err, tc.want)
		}
	}
}

func TestInsertColumnList(t *testing.T) {
	cases := []struct {
		sql      string
		want     []string
		explicit bool
		wantErr  bool
	}{
		{"INSERT INTO shop.orders FORMAT Native", nil, false, false},
		{"INSERT INTO shop.orders (id, region, amount) FORMAT Native", []string{"id", "region", "amount"}, true, false},
		{"INSERT INTO shop.orders (`region`, \"id\", amount) FORMAT Native", []string{"region", "id", "amount"}, true, false},
		{"INSERT INTO TABLE shop.orders /*target*/ (`region`, /* c */ \"id\", amount) FORMAT Native", []string{"region", "id", "amount"}, true, false},
		{"INSERT INTO shop.orders ( id ,region ) FORMAT Native", []string{"id", "region"}, true, false},
		{"INSERT INTO shop.orders (id, ) FORMAT Native", nil, true, true},
		{"INSERT INTO shop.orders (id region) FORMAT Native", nil, true, true},
	}
	for _, tc := range cases {
		got, explicit, err := insertColumnList(tc.sql)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%q: expected error", tc.sql)
			}
			continue
		}
		if err != nil || explicit != tc.explicit || strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Fatalf("%q: got %v explicit=%v err=%v", tc.sql, got, explicit, err)
		}
	}
}

func TestSampleColumnsFor(t *testing.T) {
	schema := testSchema()
	all, err := sampleColumnsFor(schema, nil)
	if err != nil || len(all) != 3 || all[0].Name != "id" || all[0].Type != "UInt64" || all[2].Name != "amount" {
		t.Fatalf("schema order: %v err=%v", all, err)
	}
	perm, err := sampleColumnsFor(schema, []string{"amount", "id", "region"})
	if err != nil || perm[0].Name != "amount" || perm[1].Name != "id" || perm[2].Name != "region" || perm[0].Type != "Float64" {
		t.Fatalf("permutation order: %v err=%v", perm, err)
	}
	if _, err := sampleColumnsFor(schema, []string{"id", "region"}); err == nil || !strings.Contains(err.Error(), "amount") {
		t.Fatalf("subset must be rejected naming the missing column: %v", err)
	}
	if _, err := sampleColumnsFor(schema, []string{"id", "region", "amount", "extra"}); err == nil || !strings.Contains(err.Error(), "extra") {
		t.Fatalf("unknown column must be rejected: %v", err)
	}
	if _, err := sampleColumnsFor(schema, []string{"id", "id", "region"}); err == nil {
		t.Fatal("duplicate column must be rejected")
	}
	if _, err := sampleColumnsFor(payloadexec.TableSchema{TableID: "x.y"}, nil); err == nil {
		t.Fatal("empty schema must be rejected")
	}
}

func TestMatchUse(t *testing.T) {
	if db, ok := matchUse("USE shop"); !ok || db != "shop" {
		t.Fatalf("USE shop → %q %v", db, ok)
	}
	if db, ok := matchUse("  use `shop-2`; "); !ok || db != "shop-2" {
		t.Fatalf("quoted USE → %q %v", db, ok)
	}
	if db, ok := matchUse("USE /* route */ `shop``prod`; "); !ok || db != "shop`prod" {
		t.Fatalf("commented/escaped USE → %q %v", db, ok)
	}
	if _, ok := matchUse("USE shop SETTINGS x=1"); ok {
		t.Fatal("USE with SETTINGS must not match")
	}
	if _, ok := matchUse("USE `foo\\nbar`"); ok {
		t.Fatal("backslash-escaped USE must fail closed")
	}
	if _, ok := matchUse("SELECT 1"); ok {
		t.Fatal("SELECT must not match")
	}
}
