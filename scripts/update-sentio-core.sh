#!/bin/bash

# Script to update sentio-core to the latest version in MODULE.bazel
# This script will:
# 1. Get the latest commit hash from the sentio-core repository (or use provided commit)
# 2. Update MODULE.bazel with the new commit hash
# 3. Update the patch file if needed
#
# Usage: ./update-sentio-core.sh [commit_hash]
# If no commit is provided, uses the latest commit from main branch

set -e

COMMIT=$1

# Get the absolute directory path of the script
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$BASEDIR")"

# Configuration
REPO_URL="https://github.com/sentioxyz/sentio-core"
MODULE_FILE="$PROJECT_ROOT/MODULE.bazel"
PATCH_FILE="$PROJECT_ROOT/third_party/sentio-core.patch"
TEMP_DIR=$(mktemp -d)

if [ -z "$COMMIT" ]; then
  echo "No commit provided, using latest commit from main branch"
  COMMIT=$(git ls-remote --heads "$REPO_URL" main | cut -f1)
fi

echo "Using commit: $COMMIT"

# Get current commit hash from MODULE.bazel (specifically for sentio-core)
CURRENT_COMMIT=$(grep -A 10 "module_name = \"sentio-core\"" "$MODULE_FILE" | grep "commit = " | sed 's/.*commit = "\([^"]*\)".*/\1/')

if [ -z "$CURRENT_COMMIT" ]; then
  echo "Failed to get current commit hash from $MODULE_FILE"
  exit 1
fi

echo "Current commit: $CURRENT_COMMIT"

# Check if update is needed
if [ "$CURRENT_COMMIT" != "$COMMIT" ]; then
  echo "Updating from $CURRENT_COMMIT to $COMMIT"
fi

# Update the commit hash in MODULE.bazel (specifically for sentio-core)
sed -i "" "/module_name = \"sentio-core\"/,/^)/ s/commit = \"$CURRENT_COMMIT\"/commit = \"$COMMIT\"/" "$MODULE_FILE"

echo "Updated $MODULE_FILE with new commit hash"

# Download the MODULE.bazel file from the new commit
echo "Downloading MODULE.bazel from new commit..."
curl -s "https://raw.githubusercontent.com/sentioxyz/sentio-core/$COMMIT/MODULE.bazel" > "$TEMP_DIR/original_module.bazel"

if [ ! -s "$TEMP_DIR/original_module.bazel" ]; then
  echo "Failed to download MODULE.bazel from new commit"
  exit 1
fi

# Create a modified version by removing everything between the markers
echo "Removing everything between '<<< REMOVE_START >>>' and '<<< REMOVE_END >>>'..."

# Remove everything between the markers using sed
sed '/^# <<< REMOVE_START >>>$/,/^# <<< REMOVE_END >>>$/d' "$TEMP_DIR/original_module.bazel" > "$TEMP_DIR/modified_module.bazel"

# Create the patch with correct filenames
echo "Creating patch file..."
cd "$TEMP_DIR"
# Ensure the third_party directory exists
mkdir -p "$(dirname "$PATCH_FILE")"
# Create patch with correct MODULE.bazel filenames and a/, b/ prefixes
diff -u original_module.bazel modified_module.bazel | sed 's/original_module\.bazel/a\/MODULE.bazel/g; s/modified_module\.bazel/b\/MODULE.bazel/g' > "$PATCH_FILE"

cd - > /dev/null

# Verify patch was created
if [ ! -s "$PATCH_FILE" ]; then
  echo "Failed to create patch file"
  exit 1
fi

# Clean up
rm -rf "$TEMP_DIR"

echo "Update completed successfully!"