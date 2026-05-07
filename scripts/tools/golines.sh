#!/bin/bash

set -e

export GOROOT=$(bazel run @go_sdk//:bin/go env GOROOT)
export PATH=$GOROOT/bin:$PATH
export GOPATH=$(go env GOPATH)

if ! command -v golines &> /dev/null; then
  echo "installing golines"
  go install github.com/segmentio/golines@latest
fi

if ! command -v goimports &> /dev/null; then
  echo "installing goimports"
  go install golang.org/x/tools/cmd/goimports@latest
fi

DEFAULT_ARGS="-m 120 --shorten-comments"

echo "$GOPATH/bin/golines $DEFAULT_ARGS $*"

$GOPATH/bin/golines $DEFAULT_ARGS "$@"
