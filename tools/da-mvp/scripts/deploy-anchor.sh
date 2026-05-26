#!/usr/bin/env bash
# Deploys DAAnchor to a local anvil and prints the address.
# Requires anvil running on $ANVIL_RPC (default http://localhost:8545).
set -euo pipefail
RPC="${ANVIL_RPC:-http://localhost:8545}"
# Anvil's default deterministic account 0:
KEY="${DEPLOYER_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
cd "$(dirname "$0")/../contracts"
forge create --rpc-url "$RPC" --private-key "$KEY" --broadcast src/DAAnchor.sol:DAAnchor \
  | tee /tmp/damvp-deploy.txt
grep "Deployed to:" /tmp/damvp-deploy.txt | awk '{print $3}'
