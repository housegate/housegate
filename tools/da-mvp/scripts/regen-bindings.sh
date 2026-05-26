#!/usr/bin/env bash
# Re-generates contracts/abi/DAAnchor.json and contracts/binding/binding.go
# from contracts/src/DAAnchor.sol. Run after editing the contract.
#
# Assumes forge (Foundry) and abigen (go-ethereum) on PATH or at known
# locations. If they're not, prepend ~/.foundry/bin and ~/go/bin to PATH:
#   export PATH="$HOME/.foundry/bin:$HOME/go/bin:$PATH"
set -euo pipefail
cd "$(dirname "$0")/.."
forge build
jq '.abi' out/DAAnchor.sol/DAAnchor.json > abi/DAAnchor.json
abigen --abi abi/DAAnchor.json --pkg binding --type DAAnchor --out binding/binding.go
echo "Regenerated abi/DAAnchor.json and binding/binding.go"
