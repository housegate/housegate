package payloadexec

import "context"

// RowSource iterates complete materialized rows. Only io.EOF denotes success.
// Callers own the source and must close it, including after errors.
type RowSource interface {
	Next(context.Context) (Row, error)
	Close() error
}
