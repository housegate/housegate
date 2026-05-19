package secretsload

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"filippo.io/age"
)

// RecipientsFromCompanion reads public keys from "<target>.recipients", one
// per line, comments (#) allowed. Returns nil if the companion file is absent
// — callers should combine with env/flag sources via LoadRecipients.
func RecipientsFromCompanion(target string) ([]string, error) {
	f, err := os.Open(target + ".recipients")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out, sc.Err()
}

// ResolveRecipients combines explicit flag-provided keys, env (HOUSEGATE_AGE_RECIPIENTS),
// and the companion "<target>.recipients" file into a single recipient list.
// Sources are unioned; duplicates are allowed but harmless to age.
func ResolveRecipients(target string, extra []string) ([]age.Recipient, error) {
	companion, err := RecipientsFromCompanion(target)
	if err != nil {
		return nil, fmt.Errorf("read companion recipients: %w", err)
	}
	combined := append([]string(nil), extra...)
	combined = append(combined, companion...)
	return LoadRecipients(combined...)
}

// EncryptFile reads plaintext at src, encrypts it for the given recipients,
// and writes armored ciphertext to dst. Writes are atomic via a sibling
// tempfile + rename.
func EncryptFile(src, dst string, recipients []age.Recipient) error {
	plain, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	if IsEncrypted(plain) {
		return fmt.Errorf("%s is already age-encrypted; refusing to double-encrypt", src)
	}
	cipher, err := Encrypt(plain, recipients)
	if err != nil {
		return err
	}
	return atomicWrite(dst, cipher, 0o644)
}

// DecryptFileToStdout decrypts src using identities from the env and writes
// plaintext to w. Intended for scripts (housegate secret-decrypt).
func DecryptFileToStdout(src string, w io.Writer) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	if !IsEncrypted(raw) {
		return fmt.Errorf("%s does not look age-encrypted", src)
	}
	ids, err := LoadIdentities()
	if err != nil {
		return err
	}
	plain, err := Decrypt(raw, ids)
	if err != nil {
		return err
	}
	_, err = w.Write(plain)
	return err
}

// EditFile implements the interactive edit loop: decrypt → $EDITOR → re-encrypt
// in place. If the target is not yet encrypted, the initial decrypt step is
// skipped (this makes it usable to convert a plaintext config to encrypted).
// Returns nil without writing if the editor made no changes.
func EditFile(target string, extraRecipients []string) error {
	recips, err := ResolveRecipients(target, extraRecipients)
	if err != nil {
		return err
	}

	raw, err := os.ReadFile(target)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read %s: %w", target, err)
	}

	var plain []byte
	if IsEncrypted(raw) {
		ids, err := LoadIdentities()
		if err != nil {
			return err
		}
		plain, err = Decrypt(raw, ids)
		if err != nil {
			return fmt.Errorf("decrypt %s: %w", target, err)
		}
	} else {
		plain = raw // may be empty if file did not exist
	}

	tmpPath, err := editorTempfile(target, plain)
	if err != nil {
		return err
	}
	// Best-effort cleanup even on Ctrl-C. Editors handle SIGINT themselves;
	// if the user kills *us* mid-edit we still want the plaintext gone.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	cleanup := func() {
		signal.Stop(sigCh)
		_ = shredAndRemove(tmpPath)
	}
	go func() {
		if _, ok := <-sigCh; ok {
			cleanup()
			os.Exit(130)
		}
	}()
	defer cleanup()

	if err := runEditor(tmpPath); err != nil {
		return fmt.Errorf("editor exited with error: %w", err)
	}

	edited, err := os.ReadFile(tmpPath)
	if err != nil {
		return fmt.Errorf("read tempfile after edit: %w", err)
	}
	if bytes.Equal(edited, plain) {
		fmt.Fprintln(os.Stderr, "no changes; not re-encrypting")
		return nil
	}

	cipher, err := Encrypt(edited, recips)
	if err != nil {
		return err
	}
	return atomicWrite(target, cipher, 0o644)
}

// editorTempfile stages plaintext next to the target with a .edit suffix so
// $EDITOR syntax detection (YAML, JSON) still works via filename.
func editorTempfile(target string, plaintext []byte) (string, error) {
	base := filepath.Base(target)
	// Strip ".age" / ".age-encrypted" trailing suffixes if present, so the
	// editor sees a filename with the real extension (e.g. .yaml).
	base = strings.TrimSuffix(base, ".age")
	base = strings.TrimSuffix(base, ".encrypted")
	// Random token so concurrent edits of the same file don't collide.
	var tok [6]byte
	if _, err := rand.Read(tok[:]); err != nil {
		return "", err
	}
	name := filepath.Join(os.TempDir(), fmt.Sprintf(".housegate-edit-%x-%s", tok, base))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", fmt.Errorf("create edit tempfile: %w", err)
	}
	if _, err := f.Write(plaintext); err != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

func runEditor(path string) error {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = os.Getenv("VISUAL")
	}
	if editor == "" {
		editor = "vi"
	}
	// Split EDITOR so users can pass flags, e.g. EDITOR="code -w".
	parts := strings.Fields(editor)
	parts = append(parts, path)
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// atomicWrite writes data to a sibling tempfile and renames over dst so
// readers never observe a partial file or a moment of zero-byte truncation.
func atomicWrite(dst string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(dst)
	f, err := os.CreateTemp(dir, "."+filepath.Base(dst)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create tempfile near %s: %w", dst, err)
	}
	tmp := f.Name()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Chmod(mode); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, dst, err)
	}
	return nil
}

// shredAndRemove overwrites with zeros then unlinks. On tmpfs this is mostly
// belt-and-braces — the goal is to not leave plaintext recoverable from
// swap or a filesystem that lazy-discards blocks. Best-effort, never fatal.
func shredAndRemove(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if f, err := os.OpenFile(path, os.O_WRONLY, 0); err == nil {
		zero := make([]byte, 4096)
		var written int64
		for written < st.Size() {
			n := int64(len(zero))
			if st.Size()-written < n {
				n = st.Size() - written
			}
			if _, werr := f.Write(zero[:n]); werr != nil {
				break
			}
			written += n
		}
		_ = f.Sync()
		_ = f.Close()
	}
	return os.Remove(path)
}

// Keygen writes a new X25519 identity (private key, one-line AGE-SECRET-KEY)
// together with its derived recipient (public key) to w. Matches the output
// shape of "age-keygen" so operators can use either tool interchangeably.
func Keygen(w io.Writer) error {
	id, err := age.GenerateX25519Identity()
	if err != nil {
		return fmt.Errorf("generate identity: %w", err)
	}
	recipient := id.Recipient()
	fmt.Fprintln(w, "# created by: housegate secret-keygen")
	fmt.Fprintf(w, "# public key: %s\n", recipient.String())
	fmt.Fprintln(w, id.String())
	// Also print pubkey to stderr for convenience when redirecting stdout to a file.
	fmt.Fprintf(os.Stderr, "Public key: %s\n", recipient.String())
	return nil
}

