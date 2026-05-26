package ids

import (
	"bytes"
	"testing"
)

func TestDBID_Stable(t *testing.T) {
	a := DBID("mydb")
	b := DBID("mydb")
	if a != b {
		t.Errorf("DBID should be deterministic; got %x vs %x", a, b)
	}
}

func TestDBID_Differs(t *testing.T) {
	if DBID("a") == DBID("b") {
		t.Error("different names must produce different ids")
	}
}

func TestTableID_DistinctFromDBID(t *testing.T) {
	if DBID("foo") == TableID("foo") {
		t.Error("DBID and TableID must use distinct domain separators")
	}
}

func TestNamespace_HasMVPPrefixAndLen10(t *testing.T) {
	ns := Namespace("mydb", "mytbl")
	if len(ns) != 10 {
		t.Fatalf("namespace must be 10 bytes, got %d", len(ns))
	}
	if !bytes.Equal(ns[:4], []byte("hgmv")) {
		t.Errorf("namespace must start with ASCII 'hgmv', got %x", ns[:4])
	}
}

func TestNamespace_StablePerTable(t *testing.T) {
	a := Namespace("d", "t")
	b := Namespace("d", "t")
	if !bytes.Equal(a, b) {
		t.Error("namespace should be deterministic")
	}
}

func TestNamespace_DifferentTable(t *testing.T) {
	if bytes.Equal(Namespace("db", "t1"), Namespace("db", "t2")) {
		t.Error("different tables must produce different namespaces")
	}
}

func TestNamespace_DifferentDB(t *testing.T) {
	if bytes.Equal(Namespace("db1", "tbl"), Namespace("db2", "tbl")) {
		t.Error("different databases must produce different namespaces")
	}
}
