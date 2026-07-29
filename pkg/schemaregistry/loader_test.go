package schemaregistry

import (
	"context"
	"strings"
	"testing"
)

func TestLoad_ValidatesRefs(t *testing.T) {
	l := NewClickHouseLoader(nil)
	for name, refs := range map[string][]TableRef{
		"empty set":     {},
		"no table id":   {{Database: "db", Table: "t"}},
		"no database":   {{TableID: "a", Table: "t"}},
		"no table":      {{TableID: "a", Database: "db"}},
		"duplicate ids": {{TableID: "a", Database: "db", Table: "t1"}, {TableID: "a", Database: "db", Table: "t2"}},
	} {
		if _, err := l.Load(context.Background(), refs); err == nil {
			t.Errorf("%s must fail before any query", name)
		}
	}
}

func TestLoad_NilConnFailsClosed(t *testing.T) {
	_, err := NewClickHouseLoader(nil).Load(context.Background(), []TableRef{{TableID: "a", Database: "db", Table: "t"}})
	if err == nil || !strings.Contains(err.Error(), "connection") {
		t.Fatalf("nil conn must fail with a pointed error, got %v", err)
	}
}
