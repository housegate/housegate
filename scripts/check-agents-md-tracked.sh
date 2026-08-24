#!/usr/bin/env bash
# Fail when any AGENTS.md in the working tree is not tracked by git.
#
# A bare `AGENTS.md` line in a user's global core.excludesFile matches at every
# depth in every repo, so nested agent-guidance files can be silently invisible
# to review and to CI while still steering agents. This check makes that state
# a build failure instead of a surprise. Track a new file with `git add -f`.
#
# Spec J D6.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
manifest="scripts/agents-md-manifest.txt"
[ -f "$manifest" ] || { echo "error: $manifest is missing" >&2; exit 1; }

print_paths() {
  local path
  while IFS= read -r path; do
    printf '  %s\n' "$path"
  done
}

tracked_agents() {
  LC_ALL=C git ls-files \
    ':(glob)AGENTS.md' \
    ':(glob)**/AGENTS.md' \
    | LC_ALL=C sort
}

# Direction 1 (works in a clean CI checkout): every manifest path must still be
# tracked. This is what catches a deletion or untracking that was committed.
missing="$(
  LC_ALL=C comm -23 "$manifest" <(tracked_agents)
)"
if [ -n "$missing" ]; then
  echo "error: manifest lists AGENTS.md paths that are no longer tracked:" >&2
  print_paths <<< "$missing" >&2
  echo "If the removal is intentional, update scripts/agents-md-manifest.txt in the same commit." >&2
  exit 1
fi

# Direction 1b: a tracked AGENTS.md that the manifest does not list. Without
# this, a file added later never enters the manifest, and its eventual
# deletion is invisible to Direction 1 — the deletion gate would have a hole
# exactly for the files added after it was written.
extra="$(
  LC_ALL=C comm -13 "$manifest" <(tracked_agents)
)"
if [ -n "$extra" ]; then
  echo "error: tracked AGENTS.md paths missing from the manifest:" >&2
  print_paths <<< "$extra" >&2
  echo "Add them to scripts/agents-md-manifest.txt in the same commit so their removal is gated too." >&2
  exit 1
fi

# Direction 2 (local only): a file present but untracked. CI cannot see this —
# an uncommitted file is absent from its checkout — but a developer can, which
# is where a newly created AGENTS.md has to be caught. The repo-level
# !AGENTS.md negation is what makes it visible to git status at all.
# --others lists untracked files; adding --ignored also surfaces the ones a
# global ignore rule hides. --ignored collapses wholly-ignored directories into
# a single trailing-slash entry, so filter down to real AGENTS.md paths.
untracked="$(
  {
    git ls-files --others --ignored --exclude-standard -- \
      ':(glob)AGENTS.md' ':(glob)**/AGENTS.md'
    git ls-files --others --exclude-standard -- \
      ':(glob)AGENTS.md' ':(glob)**/AGENTS.md'
  } | { grep -E '(^|/)AGENTS\.md$' || [ "$?" -eq 1 ]; } | sort -u
)"

if [ -n "$untracked" ]; then
  echo "error: untracked AGENTS.md files found:" >&2
  print_paths <<< "$untracked" >&2
  echo >&2
  echo "Agent-guidance files must be tracked so review and CI can see them." >&2
  echo "Track them with: git add -f <path>" >&2
  exit 1
fi

echo "ok: every AGENTS.md in the tree is tracked"
