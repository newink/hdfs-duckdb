#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
duckdb_bin="${DUCKDB_BIN:-$repo_root/build/release/duckdb}"
extension_binary="${EXTENSION_BINARY:-$repo_root/build/release/extension/hdfs_duckdb/hdfs_duckdb.duckdb_extension}"
stage_root="${STAGE_ROOT:-$repo_root/build/e2e/repo}"

if [[ ! -x "$duckdb_bin" ]]; then
	printf 'missing duckdb binary: %s\n' "$duckdb_bin" >&2
	exit 1
fi

if [[ ! -f "$extension_binary" ]]; then
	printf 'missing extension binary: %s\n' "$extension_binary" >&2
	exit 1
fi

platform="$("$duckdb_bin" -unsigned -csv -c 'PRAGMA platform;' | tail -n 1)"
version="$("$duckdb_bin" -unsigned -csv -c 'SELECT library_version FROM pragma_version();' | tail -n 1)"

target_dir="$stage_root/hdfs_duckdb/latest/$version/$platform"
mkdir -p "$target_dir"
cp "$extension_binary" "$target_dir/hdfs_duckdb.duckdb_extension"

printf '%s\n' "$target_dir/hdfs_duckdb.duckdb_extension"
