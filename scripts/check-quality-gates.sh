#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

require_command() {
	local cmd="$1"
	local install_hint="$2"

	if command -v "$cmd" >/dev/null 2>&1; then
		return 0
	fi

	printf 'missing required command: %s\n' "$cmd" >&2
	printf '%s\n' "$install_hint" >&2
	exit 1
}

run_step() {
	printf '\n==> %s\n' "$1"
	shift
	"$@"
}

require_command python3 'Install Python 3 and ensure `python3` is on PATH.'
require_command cmake 'Install CMake and ensure `cmake` is on PATH.'
require_command black 'Install formatter dependency with: pip install "black>=24"'
require_command clang-format 'Install LLVM/clang-format or ensure it is on PATH.'

if [[ -n "${TIDY_BINARY:-}" ]]; then
	if [[ ! -x "${TIDY_BINARY}" ]]; then
		printf 'configured TIDY_BINARY is not executable: %s\n' "${TIDY_BINARY}" >&2
		exit 1
	fi
elif [[ -x "/opt/homebrew/opt/llvm/bin/clang-tidy" ]]; then
	export TIDY_BINARY="/opt/homebrew/opt/llvm/bin/clang-tidy"
else
	require_command clang-tidy 'Install clang-tidy or set TIDY_BINARY to the desired executable.'
fi

run_step "format-check" make format-check
run_step "tidy-check" make tidy-check

if [[ "${HDFS_PRE_PUSH_RUN_BUILD:-0}" == "1" ]]; then
	run_step "release build" make release
fi

if [[ "${HDFS_PRE_PUSH_RUN_TESTS:-0}" == "1" ]]; then
	if [[ "${LINUX_CI_IN_DOCKER:-0}" == "1" ]]; then
		run_step "test_release" make test_release
	else
		printf '\n==> test_release skipped\n'
		printf '%s\n' \
			"Tests are disabled outside the Linux CI container in the shared DuckDB extension makefile." \
			"Set LINUX_CI_IN_DOCKER=1 only if your local environment matches that setup."
	fi
fi
