#!/usr/bin/env bash

set -euo pipefail

GOROOT="$(bazel run -- @rules_go//go env GOROOT)"
export GOROOT
GOPATH="$("$GOROOT/bin/go" env GOPATH)"
export GOPATH
export PATH="$GOROOT/bin:$GOPATH/bin:$PATH"

if [ ! -x "$GOPATH/bin/golines" ]; then
  echo "installing golines"
  "$GOROOT/bin/go" install github.com/segmentio/golines@latest
fi

if [ ! -x "$GOPATH/bin/goimports" ]; then
  echo "installing goimports"
  "$GOROOT/bin/go" install golang.org/x/tools/cmd/goimports@latest
fi

DEFAULT_ARGS=(-m 120 --shorten-comments)

echo "$GOPATH/bin/golines ${DEFAULT_ARGS[*]} $*"

"$GOPATH/bin/golines" "${DEFAULT_ARGS[@]}" "$@"
