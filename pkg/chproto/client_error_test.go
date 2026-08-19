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
