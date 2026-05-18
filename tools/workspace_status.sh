#!/usr/bin/env bash
# Workspace status command — invoked by Bazel during every build.
# Outputs key-value pairs consumed by x_defs placeholders in BUILD files.
set -euo pipefail

# STABLE_ prefix: changes trigger a re-link (stable-status.txt)
echo "STABLE_GIT_COMMIT $(git log -1 --format='%H' 2>/dev/null || echo unknown)"
echo "STABLE_GIT_TAG $(git describe --tags --dirty --always 2>/dev/null || echo unknown)"

# No STABLE_ prefix: volatile, never trigger re-link (volatile-status.txt)
echo "BUILD_TIMESTAMP $(date -u +%Y-%m-%dT%H:%M:%SZ)"
