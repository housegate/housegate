package testenv

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeConn records the query and context of the last Exec; every other method
// panics through the nil embedded interface.
type fakeConn struct {
	driver.Conn
	query string
	ctx   context.Context
}

func (f *fakeConn) Exec(ctx context.Context, query string, _ ...any) error {
	f.query, f.ctx = query, ctx
	return nil
}

func TestSignedConn(t *testing.T) {
	var signed string
	sign := func(q string) (string, error) { signed = q; return "tok", nil }
	fake := &fakeConn{}
	conn := SignedConn(fake, sign)

	ctx := WithQuerySettings(context.Background(), clickhouse.Settings{"max_threads": 1})
	if err := conn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if signed != "SELECT 1" || fake.query != "SELECT 1" {
		t.Fatalf("signed %q, sent %q; want both SELECT 1", signed, fake.query)
	}
	if fake.ctx == ctx {
		t.Fatal("context was not replaced with a signed one")
	}

	if err := conn.Exec(ctx, "SELECT ?", 1); !errors.Is(err, ErrSignedQueryArgs) {
		t.Fatalf("Exec with args: %v, want ErrSignedQueryArgs", err)
	}
	if err := conn.QueryRow(ctx, "SELECT ?", 1).Err(); !errors.Is(err, ErrSignedQueryArgs) {
		t.Fatalf("QueryRow with args: %v, want ErrSignedQueryArgs", err)
	}

	boom := errors.New("boom")
	failing := SignedConn(fake, func(string) (string, error) { return "", boom })
	if err := failing.Exec(ctx, "SELECT 1"); !errors.Is(err, boom) {
		t.Fatalf("sign error: %v, want boom", err)
	}
}
