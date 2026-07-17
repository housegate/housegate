#!/usr/bin/env bash

set -euo pipefail

repo_root="${TEST_SRCDIR}/${TEST_WORKSPACE}"
ruby -c "${repo_root}/.github/scripts/update_homebrew_formula.rb"
ruby "${repo_root}/.github/scripts/update_homebrew_formula_test.rb"
