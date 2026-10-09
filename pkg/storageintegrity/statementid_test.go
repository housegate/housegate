package storageintegrity

import (
	"errors"
	"strings"
	"testing"
)

const testAccount = "0x00000000000000000000000000000000000000a1"

func TestParseStatementID_RoundTrips(t *testing.T) {
	for _, flat := range []string{
		testAccount + ":42:9f1c",
		testAccount + ":5e1f0a2b7c9d3e4f:42:9f1c",
		"0xabc:1:n1", // any lowercase hex length, as today (spec §5.1)
		testAccount + ":18446744073709551615:n",
	} {
		id, err := ParseStatementID(flat)
		if err != nil {
			t.Fatalf("ParseStatementID(%q): %v", flat, err)
		}
		if got := id.Flat(); got != flat {
			t.Fatalf("Flat() = %q, want %q", got, flat)
		}
	}
}

func TestParseStatementID_Subject(t *testing.T) {
	legacy, err := ParseStatementID(testAccount + ":42:n")
	if err != nil || legacy.Subject() != testAccount || legacy.IsLaned() {
		t.Fatalf("legacy subject = %q laned=%v err=%v", legacy.Subject(), legacy.IsLaned(), err)
	}
	laned, err := ParseStatementID(testAccount + ":5e1f0a2b7c9d3e4f:42:n")
	if err != nil || laned.Subject() != testAccount+":5e1f0a2b7c9d3e4f" || !laned.IsLaned() || laned.Seq != 42 {
		t.Fatalf("laned = %+v subject=%q err=%v", laned, laned.Subject(), err)
	}
}

func TestParseStatementID_RejectsMalformed(t *testing.T) {
	for name, flat := range map[string]string{
		"two segments":       testAccount + ":42",
		"five segments":      testAccount + ":5e1f0a2b7c9d3e4f:42:n:x",
		"lane 15 hex":        testAccount + ":5e1f0a2b7c9d3e4:42:n",
		"lane 17 hex":        testAccount + ":5e1f0a2b7c9d3e4f0:42:n",
		"lane uppercase":     testAccount + ":5E1F0A2B7C9D3E4F:42:n",
		"lane not hex":       testAccount + ":5e1f0a2b7c9d3e4g:42:n",
		"uppercase account":  "0x00000000000000000000000000000000000000A1:42:n",
		"account without 0x": "00000000000000000000000000000000000000a1:42:n",
		"empty account hex":  "0x:42:n",
		"leading-zero seq":   testAccount + ":042:n",
		"zero seq":           testAccount + ":0:n",
		"signed seq":         testAccount + ":+4:n",
		"seq overflow":       testAccount + ":18446744073709551616:n",
		"empty nonce":        testAccount + ":42:",
		"nonce whitespace":   testAccount + ":42: n",
		"laned leading zero": testAccount + ":5e1f0a2b7c9d3e4f:01:n",
		"laned empty nonce":  testAccount + ":5e1f0a2b7c9d3e4f:1:",
		"empty":              "",
	} {
		if _, err := ParseStatementID(flat); err == nil {
			t.Errorf("%s: ParseStatementID(%q) accepted a malformed id", name, flat)
		}
	}
}

func TestParseLegacyStatementID_RefusesLanes(t *testing.T) {
	_, err := ParseLegacyStatementID(testAccount + ":5e1f0a2b7c9d3e4f:42:n")
	if !errors.Is(err, ErrClientLanesNotEnabled) {
		t.Fatalf("err = %v, want ErrClientLanesNotEnabled", err)
	}
	if !strings.Contains(err.Error(), "client lanes are not enabled on this network") {
		t.Fatalf("message = %q", err)
	}
	if _, err := ParseLegacyStatementID(testAccount + ":42:n"); err != nil {
		t.Fatalf("legacy id refused: %v", err)
	}
}

func TestParseFlatStatementID_IsLegacyOnly(t *testing.T) {
	account, seq, nonce, err := ParseFlatStatementID(testAccount + ":7:abc")
	if err != nil || account != testAccount || seq != 7 || nonce != "abc" {
		t.Fatalf("got %s/%d/%s err=%v", account, seq, nonce, err)
	}
	if _, _, _, err := ParseFlatStatementID(testAccount + ":5e1f0a2b7c9d3e4f:7:abc"); !errors.Is(err, ErrClientLanesNotEnabled) {
		t.Fatalf("laned id err = %v, want ErrClientLanesNotEnabled", err)
	}
}
