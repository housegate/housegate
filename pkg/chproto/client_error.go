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
	// CodeAccessDenied is ACCESS_DENIED: the storage-integrity ingress refuses
	// a write by a denylisted signer or owner, an invalid operator relation, or
	// a principal that is not a writer of the database (spec 2026-10-09 R6).
	CodeAccessDenied int32 = 497
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
//   - Source unavailable (spec 2026-10-10 §6.4, §9): the arbiter refused the
//     statement because its table's owning indexer has no registered, Active
//     SNode yet; refused retryably with CodeTableIsBeingRestarted.
//   - Indexer quarantined (spec 2026-10-10 §7.4, §9): the arbiter refused the
//     statement because its table's owning SI indexer is quarantined; refused
//     non-retryably with CodeQueryIsProhibited until an authority releases
//     the indexer. Other refusals share 392, so only this exact message keeps
//     a session on the deferred lane.
const (
	tableRefusalPrefix         = "storage_integrity: table "
	tableActivatingSuffix      = " is being activated; retry shortly (retryable)"
	tableNoLongerAcceptsSuffix = " no longer accepts writes"
	sourceUnavailablePrefix    = "storage_integrity: the source of table "
	sourceUnavailableSuffix    = " is not active yet; retry shortly (retryable)"
	indexerQuarantinedPrefix   = "storage_integrity: the indexer that owns table "
	indexerQuarantinedSuffix   = " is quarantined; writes to it are refused until the indexer is released (not retryable)"
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

// SourceUnavailableMessage is the client-facing message of the retryable
// refusal of a statement whose table's source SNode is not Active yet (the
// arbiter's SOURCE_UNAVAILABLE, spec 2026-10-10 §6.4, §9).
func SourceUnavailableMessage(tableID string) string {
	return sourceUnavailablePrefix + tableID + sourceUnavailableSuffix
}

// IsSourceUnavailableMessage reports whether message is exactly a
// source-unavailable refusal for a non-empty, whitespace-free table id.
func IsSourceUnavailableMessage(message string) bool {
	return isRefusalMessage(message, sourceUnavailablePrefix, sourceUnavailableSuffix)
}

// IndexerQuarantinedMessage is the client-facing message of the non-retryable
// refusal of a statement whose table's owning SI indexer is quarantined (the
// arbiter's INDEXER_QUARANTINED, spec 2026-10-10 §7.4, §9).
func IndexerQuarantinedMessage(tableID string) string {
	return indexerQuarantinedPrefix + tableID + indexerQuarantinedSuffix
}

// IsIndexerQuarantinedMessage reports whether msg is exactly an
// indexer-quarantined refusal for a non-empty, whitespace-free table id.
func IsIndexerQuarantinedMessage(msg string) bool {
	return isRefusalMessage(msg, indexerQuarantinedPrefix, indexerQuarantinedSuffix)
}

// The read side of an indexer quarantine (spec 2026-10-10 §10): the owner's
// host reports each of its SI tables Refused with the host-synthesized code
// "indexer_quarantined", and sitablestate refuses every read and write of it
// with TableRefusedMessage. The agent fails such a read over to a replica
// host (spec 2026-10-14 §11, Plan S3-C4 K46). The text is never
// session-preserving: only the arbiter's write-side IndexerQuarantinedMessage
// keeps a deferred-lane session.
const (
	tableRefusedInfix           = " was refused: "
	indexerQuarantinedReadInfix = tableRefusedInfix + "indexer_quarantined: "
)

// TableRefusedMessage is the client-facing message of sitablestate's
// refusal of a Refused table: the arbiter's refused_code and refused_reason,
// or the host's code "indexer_quarantined" and its reason.
func TableRefusedMessage(tableID, code, reason string) string {
	return tableRefusalPrefix + tableID + tableRefusedInfix + code + ": " + reason
}

// IsIndexerQuarantinedReadMessage reports whether msg, without one trailing
// SeqUnspentSuffix, is exactly TableRefusedMessage(id, "indexer_quarantined",
// reason) for a non-empty, whitespace-free table id and a non-empty reason.
func IsIndexerQuarantinedReadMessage(msg string) bool {
	msg = TrimSeqUnspentSuffix(msg)
	if !strings.HasPrefix(msg, tableRefusalPrefix) {
		return false
	}
	rest := msg[len(tableRefusalPrefix):]
	end := strings.Index(rest, indexerQuarantinedReadInfix)
	if end <= 0 {
		return false
	}
	id, reason := rest[:end], rest[end+len(indexerQuarantinedReadInfix):]
	return strings.IndexFunc(id, unicode.IsSpace) < 0 && reason != ""
}

func isTableRefusalMessage(message, suffix string) bool {
	return isRefusalMessage(message, tableRefusalPrefix, suffix)
}

func isRefusalMessage(message, prefix, suffix string) bool {
	message = TrimSeqUnspentSuffix(message)
	if !strings.HasPrefix(message, prefix) || !strings.HasSuffix(message, suffix) ||
		len(message) <= len(prefix)+len(suffix) {
		return false
	}
	id := message[len(prefix) : len(message)-len(suffix)]
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
	// SeqUnspent marks a refusal that provably leaves the statement's
	// client_seq coordinate unspent (spec 2026-10-09 §6.6). Relay renders it
	// as SeqUnspentSuffix; the agent returns the seq to its free list.
	SeqUnspent bool
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

// SeqUnspentSuffix is appended, once, to the client message of every refusal
// that provably left the client_seq coordinate unspent (spec 2026-10-09 D16).
// The agent matches it to recycle the seq; nothing else may emit it.
const SeqUnspentSuffix = " [client_seq unspent]"

type seqUnspentError struct{ err error }

func (e *seqUnspentError) Error() string { return e.err.Error() }
func (e *seqUnspentError) Unwrap() error { return e.err }

// MarkSeqUnspent flags err as a provably-unspent refusal. It wraps without
// changing the text, so the flag survives further %w wrapping and every
// errors.As on the wrapped chain still works.
func MarkSeqUnspent(err error) error {
	if err == nil || IsSeqUnspent(err) {
		return err
	}
	return &seqUnspentError{err: err}
}

// IsSeqUnspent reports whether err carries the unspent flag, either through
// MarkSeqUnspent or a ClientError with SeqUnspent set.
func IsSeqUnspent(err error) bool {
	var marked *seqUnspentError
	if errors.As(err, &marked) {
		return true
	}
	var clientErr *ClientError
	return errors.As(err, &clientErr) && clientErr.SeqUnspent
}

// HasSeqUnspentSuffix reports whether a rendered Exception message carries
// the marker.
func HasSeqUnspentSuffix(message string) bool {
	return strings.HasSuffix(strings.TrimSpace(message), SeqUnspentSuffix)
}

// TrimSeqUnspentSuffix removes one trailing marker; message matchers compare
// the text before it.
func TrimSeqUnspentSuffix(message string) string {
	return strings.TrimSuffix(strings.TrimSpace(message), SeqUnspentSuffix)
}
