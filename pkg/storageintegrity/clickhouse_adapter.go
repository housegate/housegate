package storageintegrity

import (
	"context"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func NewClickHouseTableHasher(conn chdriver.Conn, tableID string) ClickHouseTableHasher {
	return ClickHouseTableHasher{Conn: clickHouseHashConn{conn: conn}, TableID: tableID}
}

func NewClickHouseActivePartReader(conn chdriver.Conn, tableID string) ClickHouseActivePartReader {
	return ClickHouseActivePartReader{Conn: clickHouseHashConn{conn: conn}, TableID: tableID}
}

func NewClickHouseHashConn(conn chdriver.Conn) HashQueryConn {
	return clickHouseHashConn{conn: conn}
}

type clickHouseHashConn struct {
	conn chdriver.Conn
}

func (c clickHouseHashConn) Query(ctx context.Context, query string, args ...any) (HashRows, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return clickHouseHashRows{Rows: rows}, nil
}

type clickHouseHashRows struct {
	chdriver.Rows
}

func (r clickHouseHashRows) ColumnTypes() []HashColumnType {
	types := r.Rows.ColumnTypes()
	out := make([]HashColumnType, 0, len(types))
	for _, typ := range types {
		out = append(out, clickHouseHashColumnType{ColumnType: typ})
	}
	return out
}

type clickHouseHashColumnType struct {
	chdriver.ColumnType
}
