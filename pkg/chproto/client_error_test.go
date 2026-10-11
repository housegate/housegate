package chproto

import (
	"errors"
	"fmt"
	"testing"
)

func TestClientError_UnwrapsAndFormats(t *testing.T) {
	cause := errors.New("partition p_2026 has 2400 active parts")
	clientErr := &ClientError{Code: CodeTooManyParts, Message: "storage_integrity: back-pressure: retry later", Err: cause}
	wrapped := fmt.Errorf("storage_integrity admission rejected for s1: %w", clientErr)
	var got *ClientError
	if !errors.As(wrapped, &got) || got.Code != 252 {
		t.Fatalf("errors.As failed or code=%d", got.Code)
	}
	if !errors.Is(wrapped, cause) {
		t.Fatal("ClientError must unwrap to its cause")
	}
	if clientErr.Error() != "storage_integrity: back-pressure: retry later: partition p_2026 has 2400 active parts" {
		t.Fatalf("Error() = %q", clientErr.Error())
	}
	if (&ClientError{Code: 1, Message: "m"}).Error() != "m" {
		t.Fatal("Error() without cause must be the message alone")
	}
}

func TestKeepsSession(t *testing.T) {
	throttle := &ClientError{Code: CodeTooManyParts, Message: "storage_integrity: back-pressure: retry later", KeepSession: true}
	if !KeepsSession(fmt.Errorf("wrapped: %w", throttle)) {
		t.Fatal("a wrapped KeepSession ClientError must be recognised")
	}
	if KeepsSession(&ClientError{Code: 403, Message: "denied"}) {
		t.Fatal("an ordinary ClientError must not keep the session")
	}
	if KeepsSession(errors.New("boom")) {
		t.Fatal("a plain error must not keep the session")
	}
	if KeepsSession(nil) {
		t.Fatal("nil must not keep the session")
	}
}

// TestTableActivatingMessage pins the exact client-facing text; the relay's
// session-preserving recognition depends on it byte for byte.
func TestTableActivatingMessage(t *testing.T) {
	if got := TableActivatingMessage("net1.events"); got != "storage_integrity: table net1.events is being activated; retry shortly (retryable)" {
		t.Fatalf("TableActivatingMessage = %q", got)
	}
	if !IsTableActivatingMessage(TableActivatingMessage("net1.events")) {
		t.Fatal("IsTableActivatingMessage rejected its own message")
	}
	for _, msg := range []string{
		TableActivatingMessage(""),
		"storage_integrity: table net1.events is pending activation (retryable)",
		"storage_integrity: back-pressure: retry later",
		"Table net1.events is being activated; retry shortly (retryable)",
		TableActivatingMessage("net1 events"),
		TableActivatingMessage(" net1.events"),
		TableActivatingMessage("net1.events\t"),
		TableActivatingMessage("a is being activated; retry shortly (retryable) b"),
	} {
		if IsTableActivatingMessage(msg) {
			t.Fatalf("IsTableActivatingMessage(%q) = true, want false", msg)
		}
	}
}

// TestTableNoLongerAcceptsWritesMessage pins the exact spec §9.6 text; the
// relay's session-preserving recognition depends on it byte for byte.
func TestTableNoLongerAcceptsWritesMessage(t *testing.T) {
	if got := TableNoLongerAcceptsWritesMessage("net1.events"); got != "storage_integrity: table net1.events no longer accepts writes" {
		t.Fatalf("TableNoLongerAcceptsWritesMessage = %q", got)
	}
	if !IsTableNoLongerAcceptsWritesMessage(TableNoLongerAcceptsWritesMessage("net1.events")) {
		t.Fatal("IsTableNoLongerAcceptsWritesMessage rejected its own message")
	}
	for _, msg := range []string{
		TableNoLongerAcceptsWritesMessage(""),
		TableNoLongerAcceptsWritesMessage("net1 events"),
		TableNoLongerAcceptsWritesMessage("net1.events\n"),
		"storage_integrity: table net1.events was refused: CODE: reason",
		"storage_integrity: table state is unavailable for this query",
		TableActivatingMessage("net1.events"),
		"storage_integrity: table net1.events no longer accepts writes: extra",
	} {
		if IsTableNoLongerAcceptsWritesMessage(msg) {
			t.Fatalf("IsTableNoLongerAcceptsWritesMessage(%q) = true, want false", msg)
		}
	}
}

func TestMarkSeqUnspent(t *testing.T) {
	plain := errors.New("refused")
	if IsSeqUnspent(plain) {
		t.Fatal("unmarked error reported unspent")
	}
	marked := MarkSeqUnspent(plain)
	if !IsSeqUnspent(marked) || marked.Error() != "refused" || !errors.Is(marked, plain) {
		t.Fatalf("marked = %v (unspent=%v)", marked, IsSeqUnspent(marked))
	}
	if MarkSeqUnspent(marked) != marked {
		t.Fatal("MarkSeqUnspent must be idempotent")
	}
	if MarkSeqUnspent(nil) != nil {
		t.Fatal("MarkSeqUnspent(nil) must stay nil")
	}
	keep := &ClientError{Code: CodeTooManyParts, Message: "storage_integrity: back-pressure: retry later", KeepSession: true}
	wrapped := fmt.Errorf("strict: %w", MarkSeqUnspent(keep))
	if !IsSeqUnspent(wrapped) || !KeepsSession(wrapped) {
		t.Fatal("marking must not hide the wrapped ClientError")
	}
	if !IsSeqUnspent(&ClientError{Code: 497, Message: "x", SeqUnspent: true}) {
		t.Fatal("ClientError.SeqUnspent must count as marked")
	}
}

func TestSeqUnspentSuffixHelpers(t *testing.T) {
	msg := TableActivatingMessage("db1.t") + SeqUnspentSuffix
	if !HasSeqUnspentSuffix(msg) || TrimSeqUnspentSuffix(msg) != TableActivatingMessage("db1.t") {
		t.Fatalf("suffix helpers on %q", msg)
	}
	if !IsTableActivatingMessage(msg) {
		t.Fatal("IsTableActivatingMessage must accept the marker suffix")
	}
	if !IsTableNoLongerAcceptsWritesMessage(TableNoLongerAcceptsWritesMessage("db1.t") + SeqUnspentSuffix) {
		t.Fatal("IsTableNoLongerAcceptsWritesMessage must accept the marker suffix")
	}
	if HasSeqUnspentSuffix("storage_integrity: refused") {
		t.Fatal("unsuffixed message reported the marker")
	}
}

// TestSourceUnavailableMessage pins the exact spec 2026-10-10 §6.4 / §9 text;
// the relay's deferred-lane classifier depends on it.
func TestSourceUnavailableMessage(t *testing.T) {
	if got := SourceUnavailableMessage("net1.events"); got != "storage_integrity: the source of table net1.events is not active yet; retry shortly (retryable)" {
		t.Fatalf("SourceUnavailableMessage = %q", got)
	}
	if !IsSourceUnavailableMessage(SourceUnavailableMessage("net1.events")) {
		t.Fatal("IsSourceUnavailableMessage rejected its own message")
	}
	if !IsSourceUnavailableMessage(SourceUnavailableMessage("net1.events") + SeqUnspentSuffix) {
		t.Fatal("IsSourceUnavailableMessage must accept the marker suffix")
	}
	for _, msg := range []string{
		SourceUnavailableMessage(""),
		SourceUnavailableMessage("net1 events"),
		SourceUnavailableMessage("net1.events\t"),
		TableActivatingMessage("net1.events"),
		TableNoLongerAcceptsWritesMessage("net1.events"),
		"storage_integrity: the source of table net1.events is not active yet",
	} {
		if IsSourceUnavailableMessage(msg) {
			t.Fatalf("IsSourceUnavailableMessage(%q) = true, want false", msg)
		}
	}
	if IsTableActivatingMessage(SourceUnavailableMessage("net1.events")) || IsTableNoLongerAcceptsWritesMessage(SourceUnavailableMessage("net1.events")) {
		t.Fatal("the source-unavailable refusal must not match the table refusals")
	}
}
