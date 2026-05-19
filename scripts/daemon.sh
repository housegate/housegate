#!/usr/bin/env bash
#
# scripts/daemon.sh — interactive launcher for housegate in sidecar mode.
#
# Auto-detects the host OS/arch, downloads the matching binary from the
# latest GitHub release (verifying the published SHA256), caches it under
# ~/.cache/housegate/bin/, then exec's it. Prompts for the network-state
# source and Ethereum signing key unless provided via flags or env.
#
# Usage:
#   scripts/daemon.sh                                   # full interactive
#   scripts/daemon.sh --state ./configs/local.network_state.yaml
#   scripts/daemon.sh --state redis.example.com:6379 \
#                     --private-key 0xabc... \
#                     --listen 127.0.0.1:9001
#   scripts/daemon.sh --version v0.6.0                  # pin a specific release
#   scripts/daemon.sh --help
#
# Env-var equivalents (any flag wins over the corresponding env):
#   HOUSEGATE_NETWORK_STATE_SOURCE  → --state
#   HOUSEGATE_SIDECAR_KEY           → --private-key
#   HOUSEGATE_SIDECAR_OWNER         → --owner
#   HOUSEGATE_LISTEN                → --listen
#   HOUSEGATE_VERSION               → --version  (skips the "latest" lookup)
#   HOUSEGATE_BIN                   → use this binary instead of downloading

set -euo pipefail

cd "$(dirname "$0")/.."

REPO="housegate/housegate"
CACHE_DIR="${HOUSEGATE_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/housegate/bin}"

usage() {
    cat <<EOF
Usage: scripts/daemon.sh [OPTIONS]

Launches the housegate daemon in sidecar mode. The matching release binary
is downloaded automatically the first time, then cached and reused.

Options:
  --state SOURCE       NetworkState source (yaml path / redis addr / RPC URL).
                       Default: https://testnet-gateway.sentio.xyz
                       Other examples:
                         ./configs/local.network_state.yaml
                         redis.example.com:6379
                         http://rpc.example.com:8000
  --private-key HEX    Sidecar Ethereum private key (0x-prefixed hex).
                       Prefer the HOUSEGATE_SIDECAR_KEY env var so the value
                       doesn't sit in shell history.
  --owner ADDR         Optional billed owner address (0x-prefixed Ethereum
                       address) when --private-key is an operator key
                       signing on behalf of an owner account. Omit when the
                       signer pays for itself.
  --listen ADDR        Listen address. Default: random free port on 127.0.0.1.
  --version TAG        Pin a specific release tag (e.g. v0.6.0). Default:
                       resolved from the GitHub "latest" release.
  --bin PATH           Use a pre-existing binary instead of downloading.
                       Equivalent to setting HOUSEGATE_BIN.
  -h, --help           Show this help and exit.

Env vars (flags win):
  HOUSEGATE_NETWORK_STATE_SOURCE, HOUSEGATE_SIDECAR_KEY,
  HOUSEGATE_SIDECAR_OWNER, HOUSEGATE_LISTEN, HOUSEGATE_VERSION,
  HOUSEGATE_BIN, HOUSEGATE_CACHE_DIR

Cache: ${CACHE_DIR}
Missing required values are prompted for interactively.
EOF
}

err() { echo "error: $*" >&2; exit 1; }

state="${HOUSEGATE_NETWORK_STATE_SOURCE:-https://testnet-gateway.sentio.xyz}"
private_key="${HOUSEGATE_SIDECAR_KEY:-}"
owner="${HOUSEGATE_SIDECAR_OWNER:-}"
listen="${HOUSEGATE_LISTEN:-}"
version="${HOUSEGATE_VERSION:-}"
binary="${HOUSEGATE_BIN:-}"

while (( $# )); do
    case "$1" in
        --state=*)        state="${1#*=}"; shift ;;
        --private-key=*)  private_key="${1#*=}"; shift ;;
        --owner=*)        owner="${1#*=}"; shift ;;
        --listen=*)       listen="${1#*=}"; shift ;;
        --version=*)      version="${1#*=}"; shift ;;
        --bin=*)          binary="${1#*=}"; shift ;;
        --state)          state="${2:-}"; shift 2 ;;
        --private-key)    private_key="${2:-}"; shift 2 ;;
        --owner)          owner="${2:-}"; shift 2 ;;
        --listen)         listen="${2:-}"; shift 2 ;;
        --version)        version="${2:-}"; shift 2 ;;
        --bin)            binary="${2:-}"; shift 2 ;;
        -h|--help)        usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------------
# Detect host platform → release artifact name.
# Released artifacts: housegate-<tag>-darwin-arm64, housegate-<tag>-linux-amd64
# ---------------------------------------------------------------------
detect_platform() {
    local os arch
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    arch="$(uname -m)"
    case "$arch" in
        arm64|aarch64) arch="arm64" ;;
        x86_64|amd64)  arch="amd64" ;;
        *) err "unsupported CPU architecture: $arch" ;;
    esac
    case "$os" in
        darwin)
            [[ "$arch" == "arm64" ]] || err "no published binary for darwin-$arch (only darwin-arm64)"
            echo "darwin-arm64" ;;
        linux)
            [[ "$arch" == "amd64" ]] || err "no published binary for linux-$arch (only linux-amd64)"
            echo "linux-amd64" ;;
        *) err "unsupported OS: $os" ;;
    esac
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        err "neither sha256sum nor shasum is available for checksum verification"
    fi
}

# Resolve the latest release tag via the GitHub API.
resolve_latest_version() {
    local body tag
    body=$(curl -fsSL \
        -H "Accept: application/vnd.github+json" \
        "https://api.github.com/repos/${REPO}/releases/latest") \
        || err "failed to query GitHub API for latest release"
    tag=$(printf '%s\n' "$body" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)
    [[ -n "$tag" ]] || err "could not parse tag_name from GitHub API response"
    echo "$tag"
}

# Download (and verify) the release binary, returning its path on stdout.
download_binary() {
    local tag="$1" platform="$2"
    local fname="housegate-${tag}-${platform}"
    local target="${CACHE_DIR}/${fname}"
    local sums="${CACHE_DIR}/SHA256SUMS-${tag}"
    local base="https://github.com/${REPO}/releases/download/${tag}"

    mkdir -p "$CACHE_DIR"

    if [[ ! -f "$sums" ]]; then
        curl -fsSL "${base}/SHA256SUMS" -o "${sums}.tmp" \
            || err "failed to download SHA256SUMS for ${tag}"
        mv "${sums}.tmp" "$sums"
    fi

    local expected
    expected=$(awk -v f="$fname" '$2 == f { print $1; exit }' "$sums")
    [[ -n "$expected" ]] || err "no SHA256 entry for ${fname} in SHA256SUMS"

    if [[ -x "$target" ]]; then
        local actual
        actual=$(sha256_of "$target")
        if [[ "$actual" == "$expected" ]]; then
            echo "$target"
            return
        fi
        echo "Cached ${fname} failed sha256 check, re-downloading..." >&2
    fi

    echo "Downloading ${fname} (sha256 ${expected:0:12}...)" >&2
    curl -fL --progress-bar "${base}/${fname}" -o "${target}.tmp" \
        || err "failed to download ${fname}"

    local actual
    actual=$(sha256_of "${target}.tmp")
    if [[ "$actual" != "$expected" ]]; then
        rm -f "${target}.tmp"
        err "sha256 mismatch for ${fname}: expected ${expected}, got ${actual}"
    fi

    chmod +x "${target}.tmp"
    mv "${target}.tmp" "$target"
    echo "$target"
}

# ---------------------------------------------------------------------
# Resolve the binary: explicit override first, otherwise download/cache.
# ---------------------------------------------------------------------
if [[ -n "$binary" ]]; then
    [[ -x "$binary" ]] || err "binary not executable: $binary"
else
    platform=$(detect_platform)
    if [[ -z "$version" ]]; then
        version=$(resolve_latest_version)
        echo "Latest release: $version ($platform)" >&2
    else
        echo "Pinned release: $version ($platform)" >&2
    fi
    binary=$(download_binary "$version" "$platform")
fi

# ---------------------------------------------------------------------
# Prompt for missing required values. tty check keeps the script usable
# in CI / pipelines (where prompting would just hang).
# ---------------------------------------------------------------------
prompt_required() {
    local var_name="$1" prompt="$2" hidden="${3:-0}"
    if [[ -z "${!var_name}" ]]; then
        if [[ ! -t 0 ]]; then
            echo "$var_name is required (stdin is not a tty, cannot prompt)" >&2
            exit 1
        fi
        if [[ "$hidden" == "1" ]]; then
            read -rsp "$prompt" "$var_name"
            echo  # newline after silent read
        else
            read -rp "$prompt" "$var_name"
        fi
    fi
    if [[ -z "${!var_name}" ]]; then
        echo "$var_name cannot be empty" >&2
        exit 1
    fi
}

prompt_required private_key "Sidecar private key (hex, hidden): " 1

# Normalize the hex prefix so users can paste with or without 0x.
[[ "$private_key" == 0x* ]] || private_key="0x${private_key}"

# --owner is optional; when set it must be a 0x-prefixed Ethereum address
# (the binary's validator also rejects malformed values, but failing here
# avoids spinning up before the user sees the message).
if [[ -n "$owner" ]]; then
    if ! [[ "$owner" =~ ^0x[0-9a-fA-F]{40}$ ]]; then
        err "--owner must be a 0x-prefixed 20-byte Ethereum address, got: $owner"
    fi
fi

# ---------------------------------------------------------------------
# Pick a random free port if --listen wasn't given. Uses python3 to ask
# the kernel for an available port — more robust than guessing.
# ---------------------------------------------------------------------
if [[ -z "$listen" ]]; then
    if ! command -v python3 >/dev/null 2>&1; then
        echo "--listen not provided and python3 is unavailable for random-port selection." >&2
        echo "Pass --listen explicitly, e.g. --listen 127.0.0.1:9001." >&2
        exit 1
    fi
    port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
    listen="127.0.0.1:${port}"
    echo "Picked random listen address: $listen"
fi

# ---------------------------------------------------------------------
# Launch. Pass private key + state through env so they don't appear in
# argv / process listings. -sidecar enables sidecar mode at the binary.
# ---------------------------------------------------------------------
echo
echo "Launching daemon..."
echo "  binary:  $binary"
echo "  listen:  $listen"
echo "  state:   $state"
echo "  key:     ${private_key:0:6}…${private_key: -4} (redacted)"
[[ -n "$owner" ]] && echo "  owner:   $owner"
echo

HOUSEGATE_SIDECAR_KEY="$private_key" \
HOUSEGATE_SIDECAR_OWNER="$owner" \
exec "$binary" -sidecar -listen "$listen" -state "$state"
