package chproto

import (
	"errors"
	"strings"
)

// CodeTooManyParts is ClickHouse error code 252 (TOO_MANY_PARTS). Existing
// clients already treat it as retryable.
const CodeTooManyParts int32 = 252

// ClickHouse error codes of the storage-integrity table lifecycle refusals
// (spec 2026-09-24 §7.4). None of them is 252 back-pressure or the 403 generic
// plugin rejection, and neither ClickHouse client treats them specially.
const (
	// CodeUnknownTable is UNKNOWN_TABLE: a Gone table reads as any missing table.
	CodeUnknownTable int32 = 60
	// CodeQueryIsProhibited is QUERY_IS_PROHIBITED: a non-retryable refusal.
	CodeQueryIsProhibited int32 = 392
	// CodeTableIsBeingRestarted is TABLE_IS_BEING_RESTARTED: the table exists
	// but is temporarily unavailable, so the refusal is retryable.
	CodeTableIsBeingRestarted int32 = 733
)

// Table-activation refusal (controller ruling on plan B Task 7 minor 1). A
// storage-integrity admission whose target table has just become Active is
// refused until the merge guard has asserted that table. The refusal carries
// CodeTableIsBeingRestarted with KeepSession; because the Exception frame has
// no KeepSession bit, the relay recognises exactly this message shape on the
// wire, so the ingress builds it and the relay matches it here.
const (
	tableActivatingPrefix = "storage_integrity: table "
	tableActivatingSuffix = " is being activated; retry shortly (retryable)"
)

// TableActivatingMessage is the client-facing message of the retryable
// table-activation refusal for tableID.
func TableActivatingMessage(tableID string) string {
	return tableActivatingPrefix + tableID + tableActivatingSuffix
}

// IsTableActivatingMessage reports whether message is a table-activation
// refusal built by TableActivatingMessage for a non-empty table id.
func IsTableActivatingMessage(message string) bool {
	return len(message) > len(tableActivatingPrefix)+len(tableActivatingSuffix) &&
		strings.HasPrefix(message, tableActivatingPrefix) &&
		strings.HasSuffix(message, tableActivatingSuffix)
}

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
