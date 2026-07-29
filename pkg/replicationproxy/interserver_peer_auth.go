package replicationproxy

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/auth"
)

const (
	DefaultInterserverPeerUserHeader  = "X-Housegate-Peer-User"
	DefaultInterserverPeerTokenHeader = "X-Housegate-Peer-Token"
)

var (
	ErrInvalidInterserverPeerAuthOptions = errors.New("replicationproxy: invalid interserver peer auth options")
	ErrInterserverPeerAuth               = errors.New("replicationproxy: interserver peer auth failed")
)

// InterserverPeerAuthOptions configures the HouseGate-owned HTTP headers used
// to carry existing peer-login JWS authentication between interserver proxies.
type InterserverPeerAuthOptions struct {
	Signer      auth.PeerSigner
	Validator   auth.PeerValidator
	TokenTTL    time.Duration
	UserHeader  string
	TokenHeader string
}

// InterserverPeerAuth signs and validates peer-login JWS tokens carried in
// explicit HTTP headers. It intentionally does not reuse the native ClickHouse
// __peer__ user/password envelope.
type InterserverPeerAuth struct {
	signer      auth.PeerSigner
	validator   auth.PeerValidator
	tokenTTL    time.Duration
	userHeader  string
	tokenHeader string
}

func NewInterserverPeerAuth(options InterserverPeerAuthOptions) (*InterserverPeerAuth, error) {
	if options.Signer == nil {
		return nil, fmt.Errorf("signer: %w", ErrInvalidInterserverPeerAuthOptions)
	}
	if options.Validator == nil {
		return nil, fmt.Errorf("validator: %w", ErrInvalidInterserverPeerAuthOptions)
	}
	if options.TokenTTL <= 0 {
		return nil, fmt.Errorf("token ttl %s: %w", options.TokenTTL, ErrInvalidInterserverPeerAuthOptions)
	}
	userHeader := strings.TrimSpace(options.UserHeader)
	if userHeader == "" {
		userHeader = DefaultInterserverPeerUserHeader
	}
	tokenHeader := strings.TrimSpace(options.TokenHeader)
	if tokenHeader == "" {
		tokenHeader = DefaultInterserverPeerTokenHeader
	}
	return &InterserverPeerAuth{
		signer:      options.Signer,
		validator:   options.Validator,
		tokenTTL:    options.TokenTTL,
		userHeader:  userHeader,
		tokenHeader: tokenHeader,
	}, nil
}

func (a *InterserverPeerAuth) Attach(req *http.Request, targetIndexerID uint64) error {
	if req == nil {
		return fmt.Errorf("nil request: %w", ErrInterserverPeerAuth)
	}
	audience := strconv.FormatUint(targetIndexerID, 10)
	token, err := a.signer.SignPeerLogin(audience, a.tokenTTL)
	if err != nil {
		return fmt.Errorf("sign interserver peer login (audience=%s): %w", audience, err)
	}
	req.Header.Set(a.userHeader, a.signer.Address())
	req.Header.Set(a.tokenHeader, token)
	return nil
}

func (a *InterserverPeerAuth) Validate(req *http.Request, selfIndexerID uint64) (string, error) {
	if req == nil {
		return "", fmt.Errorf("nil request: %w", ErrInterserverPeerAuth)
	}
	claimedUser := strings.TrimSpace(req.Header.Get(a.userHeader))
	if claimedUser == "" {
		return "", fmt.Errorf("missing %s: %w", a.userHeader, ErrInterserverPeerAuth)
	}
	token := strings.TrimSpace(req.Header.Get(a.tokenHeader))
	if token == "" {
		return "", fmt.Errorf("missing %s: %w", a.tokenHeader, ErrInterserverPeerAuth)
	}
	expectedAudience := strconv.FormatUint(selfIndexerID, 10)
	recoveredUser, err := a.validator.ValidatePeerLogin(token, expectedAudience)
	if err != nil {
		return "", fmt.Errorf("validate interserver peer login (audience=%s): %w", expectedAudience, err)
	}
	if !strings.EqualFold(claimedUser, recoveredUser) {
		return "", fmt.Errorf("peer user header %q does not match token signer %q: %w", claimedUser, recoveredUser, ErrInterserverPeerAuth)
	}
	return recoveredUser, nil
}
