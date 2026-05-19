// Package secretsload transparently decrypts age-encrypted config files
// so the proxy can be deployed with only ciphertext on disk.
//
// Threat model: a user with file-system access on the deployment host (e.g.
// "docker exec" into the container) must not see plaintext credentials by
// reading the config file. Anyone who can attach a debugger to the running
// process can still recover secrets from memory — no pure-userspace scheme
// defends against that.
//
// Format: the file is age-encrypted (ASCII-armored). On load the bytes are
// decrypted in memory and handed to the consumer via an anonymous in-memory
// path (memfd on Linux, tmpfs-backed tempfile elsewhere) that is unlinked as
// soon as the consumer returns. Plaintext never hits a visible filesystem
// entry.
//
// The identity (private key) is read from the environment, NOT embedded in
// the binary — see HOUSEGATE_AGE_IDENTITY / HOUSEGATE_AGE_IDENTITY_FILE. Typical deployment
// injects it via a Kubernetes Secret, Docker secret, or systemd credential.
package secretsload

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"filippo.io/age"
	"filippo.io/age/armor"
)

// Env vars that control how the identity (private key) is discovered.
const (
	EnvIdentity     = "HOUSEGATE_AGE_IDENTITY"      // inline AGE-SECRET-KEY-1... string
	EnvIdentityFile = "HOUSEGATE_AGE_IDENTITY_FILE" // path to a file containing one identity per line
	EnvRecipients   = "HOUSEGATE_AGE_RECIPIENTS"    // comma-separated public keys (for encrypt/edit)
)

// armorHeader is the first line of an ASCII-armored age file. We also accept
// the raw binary format whose header starts with "age-encryption.org/v1".
const (
	armorHeader  = "-----BEGIN AGE ENCRYPTED FILE-----"
	binaryHeader = "age-encryption.org/v1"
)

// IsEncrypted reports whether data looks like an age-encrypted payload.
// It's a cheap sniff, not a validation — pass the result to Decrypt to
// actually verify the MAC and recover plaintext.
func IsEncrypted(data []byte) bool {
	head := data
	if len(head) > 64 {
		head = head[:64]
	}
	s := string(head)
	return strings.Contains(s, armorHeader) || strings.Contains(s, binaryHeader)
}

// LoadIdentities resolves age identities from the environment.
// Returns an error if none are configured; callers that want optional
// decryption should sniff with IsEncrypted first.
func LoadIdentities() ([]age.Identity, error) {
	if raw := os.Getenv(EnvIdentity); raw != "" {
		ids, err := age.ParseIdentities(strings.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", EnvIdentity, err)
		}
		return ids, nil
	}
	if path := os.Getenv(EnvIdentityFile); path != "" {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open %s=%s: %w", EnvIdentityFile, path, err)
		}
		defer f.Close()
		ids, err := age.ParseIdentities(f)
		if err != nil {
			return nil, fmt.Errorf("parse identity file %s: %w", path, err)
		}
		return ids, nil
	}
	return nil, fmt.Errorf("no age identity configured (set %s or %s)", EnvIdentity, EnvIdentityFile)
}

// LoadRecipients resolves age recipients (public keys) from env or extra.
// extra entries (raw "age1..." strings) are appended, letting callers add
// recipients parsed from CLI flags or companion files.
func LoadRecipients(extra ...string) ([]age.Recipient, error) {
	var lines []string
	if env := os.Getenv(EnvRecipients); env != "" {
		for _, part := range strings.Split(env, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				lines = append(lines, part)
			}
		}
	}
	for _, r := range extra {
		r = strings.TrimSpace(r)
		if r != "" {
			lines = append(lines, r)
		}
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("no age recipients configured (set %s or pass -r)", EnvRecipients)
	}
	recips, err := age.ParseRecipients(strings.NewReader(strings.Join(lines, "\n")))
	if err != nil {
		return nil, fmt.Errorf("parse recipients: %w", err)
	}
	return recips, nil
}

// Decrypt reads age-encrypted bytes and returns plaintext using the provided
// identities. Accepts both armored and binary age format — if the stream
// starts with the armor header it's dearmored first.
func Decrypt(data []byte, ids []age.Identity) ([]byte, error) {
	var src io.Reader = bytes.NewReader(data)
	if bytes.Contains(data[:minInt(len(data), 64)], []byte(armorHeader)) {
		src = armor.NewReader(src)
	}
	r, err := age.Decrypt(src, ids...)
	if err != nil {
		return nil, fmt.Errorf("age decrypt: %w", err)
	}
	return io.ReadAll(r)
}

// Encrypt returns age-encrypted ASCII-armored output for the given plaintext.
// Armored output is YAML/JSON-safe (pure ASCII, line-wrapped) and survives
// text-mode editors and VCS line-ending conversion.
func Encrypt(plaintext []byte, recipients []age.Recipient) ([]byte, error) {
	var out bytes.Buffer
	armorW := armor.NewWriter(&out)
	w, err := age.Encrypt(armorW, recipients...)
	if err != nil {
		return nil, fmt.Errorf("age encrypt init: %w", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		return nil, fmt.Errorf("age encrypt write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("age encrypt close: %w", err)
	}
	if err := armorW.Close(); err != nil {
		return nil, fmt.Errorf("armor close: %w", err)
	}
	return out.Bytes(), nil
}

// ResolvedPath is returned by Resolve. Path is a filesystem path the caller
// can hand to any API that opens files. Cleanup must be invoked (typically
// deferred) once the consumer is done reading — it unlinks the tempfile or
// closes the memfd. Cleanup is always non-nil, even on error.
type ResolvedPath struct {
	Path    string
	Cleanup func()
}

// noopCleanup is returned when the original path is used directly (the file
// wasn't encrypted, so there's nothing to clean up).
func noopCleanup() {}

// Resolve returns a path whose contents are the decrypted file. If the file
// at path isn't age-encrypted, the original path is returned unchanged.
// Otherwise, plaintext is written to an anonymous in-memory file and its
// path is returned.
//
// The caller MUST invoke result.Cleanup() when done (ideally via defer
// immediately after the call).
func Resolve(path string) (ResolvedPath, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ResolvedPath{Cleanup: noopCleanup}, fmt.Errorf("read %s: %w", path, err)
	}
	if !IsEncrypted(raw) {
		return ResolvedPath{Path: path, Cleanup: noopCleanup}, nil
	}
	ids, err := LoadIdentities()
	if err != nil {
		return ResolvedPath{Cleanup: noopCleanup}, fmt.Errorf("%s is age-encrypted but identity load failed: %w", path, err)
	}
	plain, err := Decrypt(raw, ids)
	if err != nil {
		return ResolvedPath{Cleanup: noopCleanup}, fmt.Errorf("decrypt %s: %w", path, err)
	}
	memPath, closer, err := writeToAnonFile(plain)
	if err != nil {
		return ResolvedPath{Cleanup: noopCleanup}, fmt.Errorf("stage decrypted %s: %w", path, err)
	}
	return ResolvedPath{Path: memPath, Cleanup: closer}, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
