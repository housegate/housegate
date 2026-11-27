package endpointtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// Signer-bound endpoint certificates.
//
// An indexer's endpoints are discovered from the network state, not chosen by
// name, so a client authenticates them by the Signer the network state
// registers for the indexer instead of by a host name and a public CA. The
// indexer generates an ephemeral P-256 key and a self-signed certificate at
// start (Go's TLS stack does not accept secp256k1 certificate keys) and signs,
// with its signer key, a digest that binds the certificate's public key to a
// chain id. The signature travels in one URI SAN of the certificate:
//
//	urn:housegate:endpoint-signer:v1:<chain id>:<0x-prefixed 65-byte signature>
//
// The digest is keccak256(SignerDomainTag || uint64be(chain id) ||
// sha256(SubjectPublicKeyInfo)), and the signature is the [R || S || V]
// recoverable secp256k1 signature crypto.Sign produces over it. The TLS
// handshake proves the server holds the certificate key, so a client that
// recovers the signer from the URI and finds it is the one registered for the
// dialed endpoint has authenticated the indexer. A URI SAN carries the
// signature rather than a private extension: a registered private OID arc
// would be needed for an extension, and Go cannot encode a UUID-based 2.25 arc.

// SignerDomainTag separates the signed digest from every other message an
// indexer's signer key signs.
const SignerDomainTag = "housegate endpoint signer v1\x00"

// signerURIPrefix starts the URI SAN that carries the signature.
const signerURIPrefix = "urn:housegate:endpoint-signer:v1:"

// SignerDigest is the digest the signer key signs for a certificate whose
// SubjectPublicKeyInfo is spki, on chainID.
func SignerDigest(chainID uint64, spki []byte) []byte {
	keyHash := sha256.Sum256(spki)
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], chainID)
	return crypto.Keccak256([]byte(SignerDomainTag), id[:], keyHash[:])
}

// SignerURI returns the URI SAN binding spki to the address of signer on
// chainID.
func SignerURI(signer *ecdsa.PrivateKey, chainID uint64, spki []byte) (*url.URL, error) {
	if signer == nil {
		return nil, errors.New("endpoint signer: nil signer key")
	}
	sig, err := crypto.Sign(SignerDigest(chainID, spki), signer)
	if err != nil {
		return nil, fmt.Errorf("endpoint signer: sign: %w", err)
	}
	return url.Parse(signerURIPrefix + strconv.FormatUint(chainID, 10) + ":0x" + hex.EncodeToString(sig))
}

// NewSignerCertificate generates an ephemeral P-256 key and a self-signed
// server certificate, valid for validity from now, whose URI SAN binds it to
// the address of signer (an Ethereum secp256k1 key) on chainID. dnsNames and
// ipAddresses only help clients that log or inspect the certificate: signer
// verification ignores them.
func NewSignerCertificate(signer *ecdsa.PrivateKey, chainID uint64, validity time.Duration) (tls.Certificate, error) {
	if validity <= 0 {
		return tls.Certificate{}, errors.New("endpoint signer: validity must be positive")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("endpoint signer: generate key: %w", err)
	}
	spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("endpoint signer: marshal public key: %w", err)
	}
	uri, err := SignerURI(signer, chainID, spki)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("endpoint signer: serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: crypto.PubkeyToAddress(signer.PublicKey).Hex()},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("endpoint signer: create certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("endpoint signer: parse certificate: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, nil
}

// CertificateSigner returns the lowercase 0x address that leaf's signer URI
// binds it to on chainID. It refuses a certificate outside its validity
// window at now, one without exactly one signer URI, a URI for another chain
// or one whose signature does not recover.
func CertificateSigner(leaf *x509.Certificate, chainID uint64, now time.Time) (string, error) {
	if leaf == nil {
		return "", errors.New("endpoint signer: no server certificate")
	}
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return "", fmt.Errorf("endpoint signer: certificate valid from %s to %s, not at %s",
			leaf.NotBefore.UTC().Format(time.RFC3339), leaf.NotAfter.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	}
	var found string
	for _, u := range leaf.URIs {
		if s := u.String(); strings.HasPrefix(s, signerURIPrefix) {
			if found != "" {
				return "", errors.New("endpoint signer: certificate carries more than one signer URI")
			}
			found = s
		}
	}
	if found == "" {
		return "", errors.New("endpoint signer: certificate carries no signer URI")
	}
	idText, sigText, ok := strings.Cut(strings.TrimPrefix(found, signerURIPrefix), ":")
	if !ok {
		return "", errors.New("endpoint signer: malformed signer URI")
	}
	gotID, err := strconv.ParseUint(idText, 10, 64)
	if err != nil {
		return "", errors.New("endpoint signer: malformed chain id in signer URI")
	}
	if gotID != chainID {
		return "", fmt.Errorf("endpoint signer: certificate is for chain %d, not %d", gotID, chainID)
	}
	if !strings.HasPrefix(sigText, "0x") {
		return "", errors.New("endpoint signer: malformed signature in signer URI")
	}
	sig, err := hex.DecodeString(sigText[2:])
	if err != nil || len(sig) != crypto.SignatureLength {
		return "", errors.New("endpoint signer: malformed signature in signer URI")
	}
	pub, err := crypto.SigToPub(SignerDigest(chainID, leaf.RawSubjectPublicKeyInfo), sig)
	if err != nil {
		return "", fmt.Errorf("endpoint signer: recover signer: %w", err)
	}
	return strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()), nil
}

// SignerLookup returns the signer address the network state registers for
// the indexer advertising addr ("host:port"). An error, or an empty address,
// refuses the connection.
type SignerLookup func(addr string) (string, error)

// signerVerifier checks a server certificate against the signer registered
// for the dialed endpoint.
type signerVerifier struct {
	chainID uint64
	lookup  SignerLookup
	now     func() time.Time
}

func (v *signerVerifier) verify(addr string, cs tls.ConnectionState) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("endpoint signer: server sent no certificate")
	}
	got, err := CertificateSigner(cs.PeerCertificates[0], v.chainID, v.now())
	if err != nil {
		return err
	}
	want, err := v.lookup(addr)
	if err != nil {
		return fmt.Errorf("endpoint signer: %s: %w", addr, err)
	}
	want = strings.ToLower(strings.TrimSpace(want))
	if want == "" {
		return fmt.Errorf("endpoint signer: the network state registers no signer for %s", addr)
	}
	if got != want {
		return fmt.Errorf("endpoint signer: %s presented a certificate of signer %s, the network state registers %s", addr, got, want)
	}
	return nil
}
