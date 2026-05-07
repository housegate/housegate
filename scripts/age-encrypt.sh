#!/usr/bin/env bash
#
# scripts/age-encrypt.sh — interactive wrapper around `housegate secret-encrypt`.
#
# Prompts for plaintext path / ciphertext path / recipients, then exec's the
# binary's built-in age-encrypt subcommand.
#
# Usage:
#   scripts/age-encrypt.sh                            # full interactive
#   scripts/age-encrypt.sh --src config.json
#   scripts/age-encrypt.sh --src config.json --dst config.json.age \
#                          --recipient age1abc... --recipient age1def...
#
# Recipient resolution (any single source is sufficient — the binary
# unions all of them):
#   --recipient KEY              flag (repeatable)
#   $HOUSEGATE_AGE_RECIPIENTS    env (comma-separated)
#   <dst>.recipients             sibling file (one age1... per line, # comments)
#   prompt                       if all of the above are empty, the script
#                                offers your local identity's pubkey or asks
#                                you to type recipients.

set -euo pipefail
cd "$(dirname "$0")/.."

usage() {
    cat <<'EOF'
Usage: scripts/age-encrypt.sh [--src PATH] [--dst PATH] [--recipient KEY]...

  --src         Plaintext file to encrypt. Prompted if missing.
  --dst         Ciphertext output path. Defaults to <src>.age.
  --recipient   age public key (age1...). Repeatable.
  -h, --help    Show this help.

If no recipient is supplied via flag, env, or a <dst>.recipients sibling,
the script offers to use the public key from your local identity file
($HOUSEGATE_AGE_IDENTITY_FILE, ./configs/local.age-key, or ~/.housegate.age).
EOF
}

src=""
dst=""
declare -a flag_recipients=()

while (( $# )); do
    case "$1" in
        --src)        src="${2:-}"; shift 2 ;;
        --dst)        dst="${2:-}"; shift 2 ;;
        --recipient)  flag_recipients+=("${2:-}"); shift 2 ;;
        -h|--help)    usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------------
# Locate the binary.
# ---------------------------------------------------------------------
binary="${HOUSEGATE_BIN:-}"
if [[ -z "$binary" ]]; then
    for cand in ./housegate ./bazel-bin/cmd/housegate_/housegate; do
        if [[ -x "$cand" ]]; then binary="$cand"; break; fi
    done
fi
if [[ -z "$binary" || ! -x "$binary" ]]; then
    cat >&2 <<EOF
Could not locate the housegate binary. Build it first:
    bazel build //cmd:housegate
or set HOUSEGATE_BIN=/path/to/housegate.
EOF
    exit 1
fi

# ---------------------------------------------------------------------
# Resolve src / dst.
# ---------------------------------------------------------------------
if [[ -z "$src" ]]; then
    if [[ ! -t 0 ]]; then
        echo "--src required (stdin is not a tty)" >&2; exit 1
    fi
    read -rp "plaintext file> " src
fi
if [[ -z "$src" || ! -f "$src" ]]; then
    echo "$src: not a file" >&2; exit 1
fi

if [[ -z "$dst" ]]; then
    dst="${src}.age"
fi

# Refuse to clobber unless the operator confirms.
if [[ -e "$dst" ]]; then
    if [[ -t 0 ]]; then
        read -rp "$dst exists. Overwrite? [y/N] " yn
        [[ "$yn" =~ ^[Yy] ]] || { echo "aborted." >&2; exit 1; }
        rm -f "$dst"
    else
        echo "$dst already exists; refusing to overwrite." >&2; exit 1
    fi
fi

# ---------------------------------------------------------------------
# Resolve recipients. The binary already merges -r flags, env, and the
# sibling .recipients file; we only step in if all three are empty AND
# we're interactive.
# ---------------------------------------------------------------------
sibling="${dst}.recipients"
if (( ${#flag_recipients[@]} == 0 )) \
    && [[ -z "${HOUSEGATE_AGE_RECIPIENTS:-}" ]] \
    && [[ ! -f "$sibling" ]]; then

    if [[ ! -t 0 ]]; then
        echo "no recipients (use --recipient, HOUSEGATE_AGE_RECIPIENTS, or $sibling)" >&2
        exit 1
    fi

    # Try to extract a pubkey from a known identity file as a one-tap shortcut.
    local_pub=""
    for cand in "${HOUSEGATE_AGE_IDENTITY_FILE:-}" ./configs/local.age-key "${HOME}/.housegate.age"; do
        [[ -n "$cand" && -f "$cand" ]] || continue
        local_pub=$(grep -oE 'age1[a-z0-9]+' "$cand" | head -1)
        [[ -n "$local_pub" ]] && break
    done

    if [[ -n "$local_pub" ]]; then
        read -rp "no recipients found. Use local pubkey ${local_pub}? [Y/n] " yn
        if [[ -z "$yn" || "$yn" =~ ^[Yy] ]]; then
            flag_recipients+=("$local_pub")
        fi
    fi

    if (( ${#flag_recipients[@]} == 0 )); then
        echo "Enter recipients (one age1... per line, blank line to finish):"
        while read -rp "  recipient> " r; do
            [[ -z "$r" ]] && break
            flag_recipients+=("$r")
        done
    fi

    if (( ${#flag_recipients[@]} == 0 )); then
        echo "no recipients given; aborting." >&2
        exit 1
    fi
fi

# ---------------------------------------------------------------------
# Build -r args and exec.
# ---------------------------------------------------------------------
declare -a r_args=()
for r in "${flag_recipients[@]}"; do
    [[ -n "$r" ]] && r_args+=(-r "$r")
done

echo
echo "Encrypting:"
echo "  src:        $src"
echo "  dst:        $dst"
if (( ${#flag_recipients[@]} > 0 )); then
    echo "  recipients: ${#flag_recipients[@]} explicit"
    for r in "${flag_recipients[@]}"; do echo "                $r"; done
elif [[ -f "$sibling" ]]; then
    echo "  recipients: from $sibling"
elif [[ -n "${HOUSEGATE_AGE_RECIPIENTS:-}" ]]; then
    echo "  recipients: from \$HOUSEGATE_AGE_RECIPIENTS"
fi
echo

exec "$binary" secret-encrypt "${r_args[@]}" "$src" "$dst"
