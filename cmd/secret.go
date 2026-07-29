package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/housegate/housegate/pkg/secretsload"
)

// secretSubcommand dispatches the "secret-*" CLI verbs to pkg/secretsload.
// Returns (handled, exitCode). handled=false means the caller should fall
// through to normal proxy startup. We keep this deliberately tiny so it can
// run before flag.Parse() without interacting with proxy-specific flags.
func secretSubcommand() (handled bool, exit int) {
	if len(os.Args) < 2 {
		return false, 0
	}
	verb := os.Args[1]
	switch verb {
	case "secret-keygen":
		return true, runKeygen(os.Args[2:])
	case "secret-encrypt":
		return true, runEncrypt(os.Args[2:])
	case "secret-decrypt":
		return true, runDecrypt(os.Args[2:])
	case "secret-edit":
		return true, runEdit(os.Args[2:])
	case "help", "-h", "--help":
		// Only intercept if the next arg names a secret verb. Otherwise
		// leave it for the proxy's own flag.Parse to handle.
		if len(os.Args) >= 3 && isSecretVerb(os.Args[2]) {
			printSecretHelp()
			return true, 0
		}
	}
	return false, 0
}

func isSecretVerb(s string) bool {
	switch s {
	case "secret-keygen", "secret-encrypt", "secret-decrypt", "secret-edit":
		return true
	}
	return false
}

func printSecretHelp() {
	fmt.Fprint(os.Stderr, `housegate secret commands:

  secret-keygen [-o <file>]
      Generate a new age identity (private+public keypair). Writes identity
      to <file> (or stdout) and the public key to stderr.

  secret-encrypt [-r <pubkey>]... <plaintext> <ciphertext>
      Encrypt <plaintext> with recipients from -r flags, $HOUSEGATE_AGE_RECIPIENTS,
      and a sibling "<ciphertext>.recipients" file. Output is ASCII-armored.

  secret-decrypt <ciphertext>
      Decrypt and stream plaintext to stdout. Identity from $HOUSEGATE_AGE_IDENTITY
      or $HOUSEGATE_AGE_IDENTITY_FILE.

  secret-edit [-r <pubkey>]... <file>
      Decrypt <file>, launch $EDITOR (falls back to vi) on the plaintext, then
      re-encrypt in place. Unchanged edits are skipped. If <file> is not yet
      encrypted, the first save encrypts it.

Environment:
  HOUSEGATE_AGE_IDENTITY       inline "AGE-SECRET-KEY-1..." string (decrypt)
  HOUSEGATE_AGE_IDENTITY_FILE  path to a file with one identity per line (decrypt)
  HOUSEGATE_AGE_RECIPIENTS     comma-separated public keys (encrypt/edit)

Companion file:
  If "<target>.recipients" exists (one pubkey per line, '#' comments allowed),
  its keys are merged into the recipient set on encrypt/edit. Useful for
  committing "who can read this file" alongside the ciphertext in git.
`)
}

func runKeygen(args []string) int {
	fs := flag.NewFlagSet("secret-keygen", flag.ContinueOnError)
	out := fs.String("o", "", "write identity to this file (default: stdout)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	w := os.Stdout
	if *out != "" {
		// Refuse to overwrite: private keys are one-shot material.
		if _, err := os.Stat(*out); err == nil {
			fmt.Fprintf(os.Stderr, "refusing to overwrite existing %s\n", *out)
			return 1
		}
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create %s: %v\n", *out, err)
			return 1
		}
		defer f.Close()
		w = f
	}
	if err := secretsload.Keygen(w); err != nil {
		fmt.Fprintf(os.Stderr, "keygen: %v\n", err)
		return 1
	}
	return 0
}

// recipientFlag collects repeated -r flags.
type recipientFlag []string

func (r *recipientFlag) String() string     { return fmt.Sprint([]string(*r)) }
func (r *recipientFlag) Set(v string) error { *r = append(*r, v); return nil }

func runEncrypt(args []string) int {
	fs := flag.NewFlagSet("secret-encrypt", flag.ContinueOnError)
	var rs recipientFlag
	fs.Var(&rs, "r", "recipient public key (age1...), may be repeated")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: housegate secret-encrypt [-r age1...] <plaintext> <ciphertext>")
		return 2
	}
	src, dst := fs.Arg(0), fs.Arg(1)

	// Validate the source before touching key material so operators get a
	// clear "already encrypted" message even when recipients aren't set.
	srcBytes, err := os.ReadFile(src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", src, err)
		return 1
	}
	if secretsload.IsEncrypted(srcBytes) {
		fmt.Fprintf(os.Stderr, "%s is already age-encrypted; refusing to double-encrypt\n", src)
		return 1
	}

	recips, err := secretsload.ResolveRecipients(dst, rs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "recipients: %v\n", err)
		return 1
	}
	if err := secretsload.EncryptFile(src, dst, recips); err != nil {
		fmt.Fprintf(os.Stderr, "encrypt: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d recipient(s))\n", dst, len(recips))
	return 0
}

func runDecrypt(args []string) int {
	fs := flag.NewFlagSet("secret-decrypt", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: housegate secret-decrypt <ciphertext>")
		return 2
	}
	if err := secretsload.DecryptFileToStdout(fs.Arg(0), os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "decrypt: %v\n", err)
		return 1
	}
	return 0
}

func runEdit(args []string) int {
	fs := flag.NewFlagSet("secret-edit", flag.ContinueOnError)
	var rs recipientFlag
	fs.Var(&rs, "r", "recipient public key (age1...), may be repeated")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: housegate secret-edit [-r age1...] <file>")
		return 2
	}
	target := fs.Arg(0)
	// Accept relative paths; make the error message easier to read.
	abs, err := filepath.Abs(target)
	if err == nil {
		target = abs
	}
	if err := secretsload.EditFile(target, rs); err != nil {
		fmt.Fprintf(os.Stderr, "edit: %v\n", err)
		return 1
	}
	return 0
}
