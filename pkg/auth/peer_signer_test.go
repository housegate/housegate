package auth

import (
	"strings"
	"testing"
	"time"
)

const peerTestKey = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestPeerLogin_RoundTrip(t *testing.T) {
	signer, err := NewRelaySigner(peerTestKey)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	v := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)

	const aud = "42"
	token, err := signer.SignPeerLogin(aud, time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	got, err := v.ValidatePeerLogin(token, aud)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !strings.EqualFold(got, signer.Address()) {
		t.Errorf("recovered=%q want %q", got, signer.Address())
	}
}

func TestPeerLogin_AudienceMismatch(t *testing.T) {
	signer, _ := NewRelaySigner(peerTestKey)
	v := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)

	token, _ := signer.SignPeerLogin("42", time.Minute)
	if _, err := v.ValidatePeerLogin(token, "99"); err == nil {
		t.Fatal("expected audience mismatch error, got nil")
	}
}

func TestPeerLogin_Expired(t *testing.T) {
	signer, _ := NewRelaySigner(peerTestKey)
	v := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)

	// Negative TTL produces a token whose exp is in the past.
	token, _ := signer.SignPeerLogin("42", -time.Minute)
	if _, err := v.ValidatePeerLogin(token, "42"); err == nil {
		t.Fatal("expected expired error, got nil")
	}
}

func TestPeerLogin_AddressNotAllowlisted(t *testing.T) {
	signer, _ := NewRelaySigner(peerTestKey)
	// Allowlist a different address — recovered signer is rejected.
	v := NewEthValidator([]string{"0x000000000000000000000000000000000000dead"}, time.Minute, true, false, "", nil)

	token, _ := signer.SignPeerLogin("42", time.Minute)
	if _, err := v.ValidatePeerLogin(token, "42"); err == nil {
		t.Fatal("expected allowlist rejection, got nil")
	}
}

func TestPeerLogin_QueryTokenRejectedAsPeer(t *testing.T) {
	// A SQL-binding token must not validate as a peer login (purpose
	// claim differs).
	signer, _ := NewRelaySigner(peerTestKey)
	v := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)

	queryToken, _ := signer.SignToken("SELECT 1")
	if _, err := v.ValidatePeerLogin(queryToken, "42"); err == nil {
		t.Fatal("expected purpose mismatch, got nil")
	}
}
