package chproto

import (
	"errors"
	"strings"
	"unicode"
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

// Session-preserving storage-integrity table refusals. The ClickHouse
// Exception frame has no KeepSession bit, so the relay recognises exactly these
// message shapes on the wire; the ingress builds them and the relay matches
// them here, so the two cannot drift.
//
//   - Table activation (controller ruling on plan B Task 7 minor 1): an
//     admission whose target has just become Active is refused, retryably with
//     CodeTableIsBeingRestarted, until the merge guard has asserted it.
//   - No longer accepts writes (spec 2026-09-24 §9.6): the table retired before
//     the arbiter sequenced the statement; refused non-retryably with
//     CodeQueryIsProhibited.
const (
	tableRefusalPrefix         = "storage_integrity: table "
	tableActivatingSuffix      = " is being activated; retry shortly (retryable)"
	tableNoLongerAcceptsSuffix = " no longer accepts writes"
)

// TableActivatingMessage is the client-facing message of the retryable
// table-activation refusal for tableID.
func TableActivatingMessage(tableID string) string {
	return tableRefusalPrefix + tableID + tableActivatingSuffix
}

// IsTableActivatingMessage reports whether message is exactly a
// table-activation refusal for a non-empty, whitespace-free table id.
func IsTableActivatingMessage(message string) bool {
	return isTableRefusalMessage(message, tableActivatingSuffix)
}

// TableNoLongerAcceptsWritesMessage is the client-facing message of the
// non-retryable spec §9.6 refusal for tableID.
func TableNoLongerAcceptsWritesMessage(tableID string) string {
	return tableRefusalPrefix + tableID + tableNoLongerAcceptsSuffix
}

// IsTableNoLongerAcceptsWritesMessage reports whether message is exactly a
// §9.6 refusal for a non-empty, whitespace-free table id.
func IsTableNoLongerAcceptsWritesMessage(message string) bool {
	return isTableRefusalMessage(message, tableNoLongerAcceptsSuffix)
}

func isTableRefusalMessage(message, suffix string) bool {
	if !strings.HasPrefix(message, tableRefusalPrefix) || !strings.HasSuffix(message, suffix) ||
		len(message) <= len(tableRefusalPrefix)+len(suffix) {
		return false
	}
	id := message[len(tableRefusalPrefix) : len(message)-len(suffix)]
	return strings.IndexFunc(id, unicode.IsSpace) < 0
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
