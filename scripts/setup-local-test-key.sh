#!/usr/bin/env bash
#
# setup-local-test-key.sh — generate fresh secp256k1 test keys for local
# development and patch them into the committed local config files:
#
#   configs/local.server.yaml             relay_{private,public}_key_hex
#   configs/local.server-mock-remote.yaml relay_{private,public}_key_hex
#   configs/local.sidecar.yaml            sidecar.private_key_hex
#   configs/local.sidecar-rpc.yaml        sidecar.private_key_hex
#   configs/local.network_state.yaml      database_permissions key for the sidecar
#
# Run this once after cloning a fresh tree, or any time you suspect the
# committed local keys have been compromised. The script is idempotent —
# it reads the current sidecar private key from sidecar.yaml to figure
# out which database_permissions entry to rotate.
#
# Requires: go (project default), bash, sed.

set -euo pipefail

cd "$(dirname "$0")/.."

GENKEY=(go run ./tools/genkey)

eval_with_prefix() {
    # Run the genkey command in $@... and re-emit its KEY=val output with
    # the given prefix so the caller can eval and get prefix-namespaced
    # shell variables.
    local prefix="$1"; shift
    local out
    out=$("$@")
    eval "$(printf '%s\n' "$out" | sed -e "s/^/${prefix}_/")"
}

# ---------------------------------------------------------------------
# 1. Discover the OLD sidecar address so we know which database_permissions
#    key to rewrite. We do this from local.sidecar.yaml (which is what
#    pairs with network_state.yaml in the test fixtures).
# ---------------------------------------------------------------------
old_sidecar_priv=$(
    grep -E '^[[:space:]]*private_key_hex:' configs/local.sidecar.yaml \
        | sed -E 's/.*"([^"]+)".*/\1/' \
        | head -1
)
if [[ -z "$old_sidecar_priv" ]]; then
    echo "could not extract old sidecar private_key_hex from configs/local.sidecar.yaml" >&2
    exit 1
fi
eval_with_prefix OLD_SIDECAR "${GENKEY[@]}" -from-priv "$old_sidecar_priv"

# ---------------------------------------------------------------------
# 2. Generate three fresh keypairs.
# ---------------------------------------------------------------------
echo "Generating relay key for proxy A (configs/local.server.yaml)..."
eval_with_prefix A "${GENKEY[@]}"

echo "Generating relay key for proxy B (configs/local.server-mock-remote.yaml)..."
eval_with_prefix B "${GENKEY[@]}"

echo "Generating sidecar key (configs/local.sidecar*.yaml + network_state.yaml)..."
eval_with_prefix SIDECAR "${GENKEY[@]}"

# ---------------------------------------------------------------------
# 3. Patch each file. sed -i.bak works on both BSD (macOS) and GNU sed;
#    we clean up the .bak files at the end.
# ---------------------------------------------------------------------
sed -i.bak \
    -e "s|^relay_private_key_hex:.*|relay_private_key_hex: \"${A_PRIVATE_KEY}\"|" \
    -e "s|^relay_public_key_hex:.*|relay_public_key_hex: \"${A_ADDRESS_CHECKSUM}\"|" \
    configs/local.server.yaml

sed -i.bak \
    -e "s|^relay_private_key_hex:.*|relay_private_key_hex: \"${B_PRIVATE_KEY}\"|" \
    -e "s|^relay_public_key_hex:.*|relay_public_key_hex: \"${B_ADDRESS_CHECKSUM}\"|" \
    configs/local.server-mock-remote.yaml

# Both sidecar configs share one identity — the network_state fixture
# only grants permissions to a single sidecar account.
sed -i.bak \
    -e "s|^\([[:space:]]*\)private_key_hex:.*|\1private_key_hex: \"${SIDECAR_PRIVATE_KEY}\"|" \
    configs/local.sidecar.yaml configs/local.sidecar-rpc.yaml

# Replace the sidecar's account address inside database_permissions. The
# YAML convention (per the file's own header) is lower-case.
sed -i.bak \
    -e "s|\"${OLD_SIDECAR_ADDRESS_LOWER}\"|\"${SIDECAR_ADDRESS_LOWER}\"|g" \
    configs/local.network_state.yaml

find configs -name '*.bak' -delete

# ---------------------------------------------------------------------
# 4. Summary so the operator has a paper trail.
# ---------------------------------------------------------------------
cat <<EOF

Done. New addresses (private keys are committed inside the YAML files):

  Proxy A relay:  ${A_ADDRESS_CHECKSUM}
  Proxy B relay:  ${B_ADDRESS_CHECKSUM}
  Sidecar:        ${SIDECAR_ADDRESS_CHECKSUM}
                  (lower-case form in network_state.yaml: ${SIDECAR_ADDRESS_LOWER})

Old sidecar address rotated out of network_state.yaml: ${OLD_SIDECAR_ADDRESS_LOWER}
EOF
