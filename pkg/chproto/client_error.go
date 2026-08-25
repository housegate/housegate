package chproto

import "errors"

// CodeTooManyParts is ClickHouse error code 252 (TOO_MANY_PARTS). Existing
// clients already treat it as retryable.
const CodeTooManyParts int32 = 252

// ClientError lets a plugin choose the ClickHouse exception code and exact
// client-facing message. Err remains the server-side cause and is not sent.
type ClientError struct {
	Code    int32
	Message string
	Err     error
	// KeepSession marks a rejection that ends the current query at a clean
	// packet boundary without tearing down the client connection.
	KeepSession bool
}

func (e *ClientError) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (e *ClientError) Unwrap() error { return e.Err }

// KeepsSession reports whether err wraps a session-preserving ClientError.
func KeepsSession(err error) bool {
	var clientErr *ClientError
	return errors.As(err, &clientErr) && clientErr.KeepSession
}
