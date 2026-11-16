package testenv

import (
	"context"
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/housegate/housegate/pkg/auth"
)

// ErrSignedQueryArgs is returned when a signed test query carries bind
// arguments. The signature must cover the exact SQL text the server receives,
// and the driver does not expose the text after argument binding.
var ErrSignedQueryArgs = errors.New("testenv: signed test queries must be sent without bind arguments")

type settingsKey struct{}

// WithQuerySettings is clickhouse.Context(ctx, clickhouse.WithSettings(s)) for
// contexts that are passed to a SignedConn. The driver offers no way to read
// settings back from a context, so the settings are also recorded under a
// testenv key, which SignedConn merges with the signature token.
func WithQuerySettings(ctx context.Context, s clickhouse.Settings) context.Context {
	merged := clickhouse.Settings{}
	if prev, ok := ctx.Value(settingsKey{}).(clickhouse.Settings); ok {
		for k, v := range prev {
			merged[k] = v
		}
	}
	for k, v := range s {
		merged[k] = v
	}
	ctx = context.WithValue(ctx, settingsKey{}, merged)
	return clickhouse.Context(ctx, clickhouse.WithSettings(merged))
}

type signedConn struct {
	driver.Conn
	sign func(query string) (string, error)
}

// SignedConn wraps conn so that every Exec, Query, QueryRow, Select,
// PrepareBatch and AsyncInsert signs its query text with sign and sends the
// token as the auth token query setting. Settings set through
// WithQuerySettings are preserved; settings set directly with
// clickhouse.WithSettings are replaced. Queries with bind arguments are
// refused with ErrSignedQueryArgs.
func SignedConn(conn driver.Conn, sign func(query string) (string, error)) driver.Conn {
	return &signedConn{Conn: conn, sign: sign}
}

func (c *signedConn) signedCtx(ctx context.Context, query string, args []any) (context.Context, error) {
	if len(args) > 0 {
		return nil, ErrSignedQueryArgs
	}
	token, err := c.sign(query)
	if err != nil {
		return nil, err
	}
	merged := clickhouse.Settings{}
	if prev, ok := ctx.Value(settingsKey{}).(clickhouse.Settings); ok {
		for k, v := range prev {
			merged[k] = v
		}
	}
	merged[auth.AuthTokenSettingKey] = clickhouse.CustomSetting{Value: token}
	return clickhouse.Context(ctx, clickhouse.WithSettings(merged)), nil
}

func (c *signedConn) Exec(ctx context.Context, query string, args ...any) error {
	sctx, err := c.signedCtx(ctx, query, args)
	if err != nil {
		return err
	}
	return c.Conn.Exec(sctx, query)
}

func (c *signedConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	sctx, err := c.signedCtx(ctx, query, args)
	if err != nil {
		return err
	}
	return c.Conn.AsyncInsert(sctx, query, wait)
}

func (c *signedConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	sctx, err := c.signedCtx(ctx, query, args)
	if err != nil {
		return err
	}
	return c.Conn.Select(sctx, dest, query)
}

func (c *signedConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	sctx, err := c.signedCtx(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return c.Conn.Query(sctx, query)
}

func (c *signedConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	sctx, err := c.signedCtx(ctx, query, args)
	if err != nil {
		return errRow{err}
	}
	return c.Conn.QueryRow(sctx, query)
}

func (c *signedConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	sctx, err := c.signedCtx(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	return c.Conn.PrepareBatch(sctx, query, opts...)
}

type errRow struct{ err error }

func (r errRow) Err() error           { return r.err }
func (r errRow) Scan(...any) error    { return r.err }
func (r errRow) ScanStruct(any) error { return r.err }
