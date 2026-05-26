package schema

import "testing"

func TestCanonicalize_ExtractsColumnsOnly(t *testing.T) {
	in := "CREATE TABLE default.foo\n(\n    `id` UInt64,\n    `name` String\n)\nENGINE = MergeTree\nORDER BY id\nSETTINGS index_granularity = 8192"
	want := "`id` UInt64,`name` String"
	got, err := Canonicalize(in)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestCanonicalize_StripsLineComments(t *testing.T) {
	in := "CREATE TABLE t (\n  -- this is a comment\n  `a` UInt64\n) ENGINE = MergeTree ORDER BY a"
	got, err := Canonicalize(in)
	if err != nil {
		t.Fatal(err)
	}
	if got != "`a` UInt64" {
		t.Errorf("got %q", got)
	}
}

func TestHash_WhitespaceInsensitive(t *testing.T) {
	a, _ := Hash("CREATE TABLE t (`a` UInt64) ENGINE=MergeTree ORDER BY a")
	b, _ := Hash("CREATE TABLE t\n(\n    `a` UInt64\n)\nENGINE=MergeTree\nORDER BY a")
	if a != b {
		t.Error("hash should be the same regardless of formatting")
	}
}

func TestHash_TypeChangeDifferentHash(t *testing.T) {
	a, _ := Hash("CREATE TABLE t (`a` UInt64) ENGINE=MergeTree ORDER BY a")
	b, _ := Hash("CREATE TABLE t (`a` String) ENGINE=MergeTree ORDER BY a")
	if a == b {
		t.Error("hash should differ when a column type changes")
	}
}

func TestCanonicalize_NoOpenParen(t *testing.T) {
	_, err := Canonicalize("CREATE TABLE t")
	if err == nil {
		t.Error("expected error on malformed input")
	}
}
