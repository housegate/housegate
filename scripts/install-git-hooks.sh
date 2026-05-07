#!/usr/bin/env bash
#
# install-git-hooks.sh — point this clone's git at scripts/git-hooks/.
#
# Run once after cloning. Sets `core.hooksPath` in this clone's
# .git/config (no sudo, no global mutation) so git starts invoking
# scripts/git-hooks/pre-commit on every `git commit`.
#
# Reverse with: `git config --unset core.hooksPath`.

set -euo pipefail

cd "$(dirname "$0")/.."

hooks_dir="scripts/git-hooks"

if [[ ! -d "$hooks_dir" ]]; then
    echo "$hooks_dir not found — run this from the housegate repo root or via scripts/install-git-hooks.sh." >&2
    exit 1
fi

git config core.hooksPath "$hooks_dir"

# Ensure every hook in the dir is executable. Git silently ignores
# non-executable hook files, which is a confusing failure mode.
chmod +x "$hooks_dir"/*

echo "Configured this clone to use git hooks from $hooks_dir/"
echo "Active hooks:"
for h in "$hooks_dir"/*; do
    [[ -f "$h" ]] || continue
    echo "  $(basename "$h")"
done
echo
echo "Test: stage a file containing a fake AWS key and try to commit."
echo "Bypass for emergencies: git commit --no-verify"
