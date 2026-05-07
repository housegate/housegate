#!/bin/bash
set -e

go mod tidy
#bazel run --config=prod @rules_go//go -- mod tidy
bazel mod tidy
bazel run --config=prod //:gazelle
