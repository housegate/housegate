package peer

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/credentials"
)

// testRelayKeyHex is a fixed secp256k1 private key for tests.
const testRelayKeyHex = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

func TestSignPeerHello_WithSigner_BuildsPeerEnvelope(t *testing.T) {
	signer, err := auth.NewRelaySigner(testRelayKeyHex)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	user, password, err := SignPeerHello(signer, 30*time.Second, 42, nil)
	if err != nil {
		t.Fatalf("SignPeerHello: %v", err)
	}
	if want := FormatUser(signer.Address()); user != want {
		t.Errorf("user = %q want %q", user, want)
	}

	validator := auth.NewEthValidator([]string{signer.Address()}, time.Hour, true, false, "", nil)
	addr, err := validator.ValidatePeerLogin(password, "42")
	if err != nil {
		t.Fatalf("validator rejected token meant for indexer 42: %v", err)
	}
	if addr != signer.Address() {
		t.Errorf("recovered address %q != signer address %q", addr, signer.Address())
	}
}

func TestSignPeerHello_NoSigner_FallsBackToStaticCreds(t *testing.T) {
	fallback := stubCredentialProvider{
		creds: map[uint64]stubCred{
			42: {user: "u-42", password: "p-42"},
		},
	}

	user, password, err := SignPeerHello(nil, 0, 42, fallback)
	if err != nil {
		t.Fatalf("SignPeerHello: %v", err)
	}
	if user != "u-42" || password != "p-42" {
		t.Errorf("expected static creds, got user=%q password=%q", user, password)
	}
}

func TestSignPeerHello_NoSignerNoFallback_Errors(t *testing.T) {
	_, _, err := SignPeerHello(nil, 0, 42, nil)
	if err == nil {
		t.Fatalf("expected error when both signer and fallback are nil")
	}
}

func TestSignPeerHello_SignerErrors_ReturnsWrappedError(t *testing.T) {
	sentinel := errors.New("hsm offline")
	signer := stubPeerSigner{address: "0xfeedface", err: sentinel}

	_, _, err := SignPeerHello(signer, 30*time.Second, 42, nil)
	if err == nil {
		t.Fatalf("expected error from failing signer")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error chain should preserve original; got %v", err)
	}
	if !strings.Contains(err.Error(), "audience=42") {
		t.Errorf("error should include audience field; got %q", err.Error())
	}
}

// stubPeerSigner is a test double for auth.PeerSigner that can simulate signing errors.
type stubPeerSigner struct {
	address string
	err     error
}

func (s stubPeerSigner) Address() string { return s.address }
func (s stubPeerSigner) SignPeerLogin(audience string, ttl time.Duration) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	return "stub-token", nil
}

// Compile-time guard: stubPeerSigner must satisfy auth.PeerSigner.
var _ auth.PeerSigner = stubPeerSigner{}

// stubCred holds a user/password pair for testing.
type stubCred struct {
	user     string
	password string
}

// stubCredentialProvider implements credentials.CredentialProvider for tests.
type stubCredentialProvider struct {
	creds map[uint64]stubCred
}

func (s stubCredentialProvider) GetDefaultCredential() (string, string, error) {
	return "", "", nil
}

func (s stubCredentialProvider) GetCredentialForIndexer(indexerId uint64) (string, string, error) {
	c, ok := s.creds[indexerId]
	if !ok {
		return "", "", &missingCredError{id: indexerId}
	}
	return c.user, c.password, nil
}

type missingCredError struct{ id uint64 }

func (e *missingCredError) Error() string {
	return "no credential for indexer"
}

// Compile-time guard: stubCredentialProvider must satisfy credentials.CredentialProvider.
var _ credentials.CredentialProvider = stubCredentialProvider{}
