package replicationproxy

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
)

const interserverPeerAuthTestKey = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

func TestInterserverPeerAuth_AttachAndValidate_acceptsValidToken(t *testing.T) {
	// Given
	carrier, signer := newInterserverPeerAuthCarrier(t, time.Minute)
	req := httptest.NewRequest("GET", "http://peer.example/replicas/path?part=1", nil)

	// When
	if err := carrier.Attach(req, 42); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	gotAddress, err := carrier.Validate(req, 42)

	// Then
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !strings.EqualFold(gotAddress, signer.Address()) {
		t.Fatalf("validated address=%q, want %q", gotAddress, signer.Address())
	}
	if got := req.Header.Get(DefaultInterserverPeerUserHeader); got != signer.Address() {
		t.Fatalf("peer user header=%q, want signer address %q", got, signer.Address())
	}
	if req.Header.Get(DefaultInterserverPeerTokenHeader) == "" {
		t.Fatal("peer token header is empty")
	}
}

func TestInterserverPeerAuth_Validate_rejectsMissingToken(t *testing.T) {
	// Given
	carrier, signer := newInterserverPeerAuthCarrier(t, time.Minute)
	req := httptest.NewRequest("GET", "http://peer.example/replicas/path", nil)
	req.Header.Set(DefaultInterserverPeerUserHeader, signer.Address())

	// When
	_, err := carrier.Validate(req, 42)

	// Then
	if err == nil {
		t.Fatal("expected missing token error, got nil")
	}
}

func TestInterserverPeerAuth_Validate_rejectsMissingUser(t *testing.T) {
	// Given
	carrier, signer := newInterserverPeerAuthCarrier(t, time.Minute)
	req := httptest.NewRequest("GET", "http://peer.example/replicas/path", nil)
	token, err := signer.SignPeerLogin("42", time.Minute)
	if err != nil {
		t.Fatalf("SignPeerLogin: %v", err)
	}
	req.Header.Set(DefaultInterserverPeerTokenHeader, token)

	// When
	_, err = carrier.Validate(req, 42)

	// Then
	if err == nil {
		t.Fatal("expected missing user error, got nil")
	}
}

func TestInterserverPeerAuth_Validate_rejectsBadAudience(t *testing.T) {
	// Given
	carrier, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	req := httptest.NewRequest("GET", "http://peer.example/replicas/path", nil)
	if err := carrier.Attach(req, 42); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	// When
	_, err := carrier.Validate(req, 99)

	// Then
	if err == nil {
		t.Fatal("expected bad audience error, got nil")
	}
}

func TestInterserverPeerAuth_Validate_rejectsExpiredToken(t *testing.T) {
	// Given
	carrier, signer := newInterserverPeerAuthCarrier(t, time.Minute)
	req := httptest.NewRequest("GET", "http://peer.example/replicas/path", nil)
	token, err := signer.SignPeerLogin("42", -time.Minute)
	if err != nil {
		t.Fatalf("SignPeerLogin: %v", err)
	}
	req.Header.Set(DefaultInterserverPeerUserHeader, signer.Address())
	req.Header.Set(DefaultInterserverPeerTokenHeader, token)

	// When
	_, err = carrier.Validate(req, 42)

	// Then
	if err == nil {
		t.Fatal("expected expired token error, got nil")
	}
}

func TestInterserverPeerAuth_Validate_rejectsUserHeaderMismatch(t *testing.T) {
	// Given
	carrier, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	req := httptest.NewRequest("GET", "http://peer.example/replicas/path", nil)
	if err := carrier.Attach(req, 42); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	req.Header.Set(DefaultInterserverPeerUserHeader, "0x000000000000000000000000000000000000dead")

	// When
	_, err := carrier.Validate(req, 42)

	// Then
	if err == nil {
		t.Fatal("expected user header mismatch error, got nil")
	}
}

func newInterserverPeerAuthCarrier(t *testing.T, ttl time.Duration) (*InterserverPeerAuth, *auth.RelaySigner) {
	t.Helper()
	signer, err := auth.NewRelaySigner(interserverPeerAuthTestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	carrier, err := NewInterserverPeerAuth(InterserverPeerAuthOptions{
		Signer:    signer,
		Validator: validator,
		TokenTTL:  ttl,
	})
	if err != nil {
		t.Fatalf("NewInterserverPeerAuth: %v", err)
	}
	return carrier, signer
}
