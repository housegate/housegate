package billing

import (
	"strings"
	"testing"
)

func TestRejectionException(t *testing.T) {
	const payer, signer = "0xpayer", "0xsigner"
	cases := []struct {
		reason   RejectionReason
		code     int32
		name     string
		contains []string
	}{
		{RejectionInsufficientBalance, 1003, "INSUFFICIENT_BALANCE",
			[]string{"Insufficient balance: payer=0xpayer signer=0xsigner", UserGuideURL + "#funding-an-account"}},
		{RejectionUnauthorizedSigner, 1002, "UNAUTHORIZED_SIGNER",
			[]string{"payer=0xpayer signer=0xsigner", "operator", UserGuideURL + "#operator-keys"}},
		{RejectionUnknown, 1001, "QUERY_REJECTED",
			[]string{"Query rejected (reason=0) payer=0xpayer signer=0xsigner", "retry"}},
	}
	for _, tc := range cases {
		code, name, msg := RejectionException(tc.reason, payer, signer)
		if code != tc.code || name != tc.name {
			t.Errorf("reason %d: got (%d, %s), want (%d, %s)", tc.reason, code, name, tc.code, tc.name)
		}
		for _, want := range tc.contains {
			if !strings.Contains(msg, want) {
				t.Errorf("reason %d: message %q does not contain %q", tc.reason, msg, want)
			}
		}
	}
}
