package secretsload

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"filippo.io/age"
)

// newTestIdentity creates a throwaway X25519 keypair for the test.
func newTestIdentity(t *testing.T) (*age.X25519Identity, *age.X25519Recipient) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id, id.Recipient()
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	id, recip := newTestIdentity(t)
	plain := []byte("user: admin\npassword: s3cret\n")

	cipher, err := Encrypt(plain, []age.Recipient{recip})
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if !IsEncrypted(cipher) {
		t.Fatal("IsEncrypted returned false for freshly-encrypted blob")
	}
	if bytes.Contains(cipher, []byte("s3cret")) {
		t.Fatal("ciphertext contains plaintext password")
	}

	got, err := Decrypt(cipher, []age.Identity{id})
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("roundtrip mismatch: got %q want %q", got, plain)
	}
}

func TestIsEncryptedNegative(t *testing.T) {
	for _, s := range []string{
		"",
		"plain yaml file\nfoo: bar\n",
		"{\"json\": true}",
		"-----BEGIN PGP MESSAGE-----", // different armor, not age
	} {
		if IsEncrypted([]byte(s)) {
			t.Errorf("IsEncrypted false positive on %q", s)
		}
	}
}

func TestResolvePassthroughForUnencrypted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plain.yaml")
	if err := os.WriteFile(path, []byte("a: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rp, err := Resolve(path)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	defer rp.Cleanup()
	if rp.Path != path {
		t.Fatalf("expected passthrough path %q, got %q", path, rp.Path)
	}
}

func TestResolveDecryptsEncrypted(t *testing.T) {
	id, recip := newTestIdentity(t)
	t.Setenv(EnvIdentity, id.String())

	plain := []byte("password: hunter2\n")
	cipher, err := Encrypt(plain, []age.Recipient{recip})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	encPath := filepath.Join(dir, "ckh.yaml.age")
	if err := os.WriteFile(encPath, cipher, 0o600); err != nil {
		t.Fatal(err)
	}

	rp, err := Resolve(encPath)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	defer rp.Cleanup()
	if rp.Path == encPath {
		t.Fatal("expected resolved path to differ from source for encrypted files")
	}
	got, err := os.ReadFile(rp.Path)
	if err != nil {
		t.Fatalf("read resolved path: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("plaintext mismatch: got %q want %q", got, plain)
	}
}

func TestResolveErrsWhenNoIdentity(t *testing.T) {
	// Ensure env is clean — t.Setenv restores at the end of the test.
	t.Setenv(EnvIdentity, "")
	t.Setenv(EnvIdentityFile, "")

	_, recip := newTestIdentity(t)
	cipher, err := Encrypt([]byte("x: 1\n"), []age.Recipient{recip})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "enc.yaml")
	if err := os.WriteFile(path, cipher, 0o600); err != nil {
		t.Fatal(err)
	}

	rp, err := Resolve(path)
	if err == nil {
		rp.Cleanup()
		t.Fatal("expected error when identity is missing")
	}
}

func TestLoadRecipientsFromEnvAndExtra(t *testing.T) {
	_, r1 := newTestIdentity(t)
	_, r2 := newTestIdentity(t)
	t.Setenv(EnvRecipients, r1.String())
	got, err := LoadRecipients(r2.String())
	if err != nil {
		t.Fatalf("LoadRecipients: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 recipients, got %d", len(got))
	}
}

func TestRecipientsFromSidecar(t *testing.T) {
	_, r1 := newTestIdentity(t)
	_, r2 := newTestIdentity(t)
	dir := t.TempDir()
	target := filepath.Join(dir, "f.yaml.age")
	sidecar := target + ".recipients"
	contents := "# comment\n\n" + r1.String() + "\n" + r2.String() + "\n"
	if err := os.WriteFile(sidecar, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := RecipientsFromSidecar(target)
	if err != nil {
		t.Fatalf("RecipientsFromSidecar: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 recipients in sidecar, got %d: %v", len(got), got)
	}
}
