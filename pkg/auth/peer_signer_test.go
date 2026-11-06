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

// A peer login skips local auth and rewrite on the receiving proxy, so it
// needs a positive trust decision. An empty allowlist opens the SQL-binding
// path to every account; it must not open the peer path with it.
func TestPeerLogin_EmptyAllowlistRejectsUnknownSigner(t *testing.T) {
	signer, _ := NewRelaySigner(peerTestKey)
	v := NewEthValidator(nil, time.Minute, true, false, "", nil)

	token, _ := signer.SignPeerLogin("42", time.Minute)
	if got, err := v.ValidatePeerLogin(token, "42"); err == nil {
		t.Fatalf("empty allowlist accepted a peer login from unknown signer %s", got)
	}
}

// indexerSigners is a fixed IndexerSignerSet for tests.
type indexerSigners map[string]bool

func (s indexerSigners) IsIndexerSigner(address string) bool {
	return s[strings.ToLower(address)]
}

// With no allowlist, the registry of indexer signers is what separates a peer
// from a stranger.
func TestPeerLogin_EmptyAllowlistFollowsIndexerSigners(t *testing.T) {
	signer, _ := NewRelaySigner(peerTestKey)
	token, _ := signer.SignPeerLogin("42", time.Minute)

	registered := NewEthValidator(nil, time.Minute, true, false, "", nil)
	registered.PeerSigners = indexerSigners{strings.ToLower(signer.Address()): true}
	got, err := registered.ValidatePeerLogin(token, "42")
	if err != nil {
		t.Fatalf("registered indexer signer refused: %v", err)
	}
	if !strings.EqualFold(got, signer.Address()) {
		t.Errorf("recovered=%q want %q", got, signer.Address())
	}

	other := NewEthValidator(nil, time.Minute, true, false, "", nil)
	other.PeerSigners = indexerSigners{"0x000000000000000000000000000000000000dead": true}
	if got, err := other.ValidatePeerLogin(token, "42"); err == nil {
		t.Fatalf("signer %s is not a registered indexer signer but was accepted", got)
	}
}

// A configured allowlist stays the only trust anchor for peers: a registered
// indexer signer that the operator did not list is still refused.
func TestPeerLogin_AllowlistOutranksIndexerSigners(t *testing.T) {
	signer, _ := NewRelaySigner(peerTestKey)
	v := NewEthValidator([]string{"0x000000000000000000000000000000000000dead"}, time.Minute, true, false, "", nil)
	v.PeerSigners = indexerSigners{strings.ToLower(signer.Address()): true}

	token, _ := signer.SignPeerLogin("42", time.Minute)
	if got, err := v.ValidatePeerLogin(token, "42"); err == nil {
		t.Fatalf("signer %s is outside the allowlist but was accepted", got)
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
