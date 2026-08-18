package proxy

import (
	"errors"
	"fmt"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
)

func TestExceptionForPluginError_DefaultsTo403(t *testing.T) {
	exception := exceptionForPluginError(errors.New("jws invalid"))
	if exception.Code != 403 || exception.Message != "jws invalid" || exception.Name != "DB::Exception" {
		t.Fatalf("exception = %+v", exception)
	}
}

func TestExceptionForPluginError_HonorsClientError(t *testing.T) {
	clientErr := &chproto.ClientError{Code: chproto.CodeTooManyParts, Message: "storage_integrity: back-pressure: hg_unsafe.db__t partition p_a has 2400 active parts (soft limit 2400); retry later", Err: errors.New("cause")}
	exception := exceptionForPluginError(fmt.Errorf("query input complete strict hook: %w", fmt.Errorf("storage_integrity admission rejected for s1: %w", clientErr)))
	if exception.Code != 252 {
		t.Fatalf("code = %d want 252", exception.Code)
	}
	if exception.Message != clientErr.Message {
		t.Fatalf("message = %q want the ClientError message verbatim", exception.Message)
	}
}
