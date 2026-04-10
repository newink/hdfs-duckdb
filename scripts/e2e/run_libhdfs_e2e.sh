#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${HDFS_E2E_COMPOSE_FILE:-$repo_root/test/e2e/docker-compose.yml}"
container_test_bin="${HDFS_E2E_CONTAINER_TEST_BIN:-/workspace/build/release/test/unittest}"
sql_test_file="${HDFS_E2E_SQL_TEST_FILE:-test/e2e/hdfs_duckdb_e2e_read.test}"

cleanup() {
	docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
}

if [[ "${HDFS_E2E_KEEP_SERVICES:-0}" != "1" ]]; then
	trap cleanup EXIT
fi

if [[ "$(uname -s)" != "Linux" ]]; then
	printf '%s\n' \
		"libhdfs E2E must run with a Linux DuckDB test binary because the command executes inside the Linux Hadoop container." \
		"Run this target on Linux, or set HDFS_E2E_CONTAINER_TEST_BIN to a Linux binary path that already exists inside the container." >&2
	exit 1
fi

cd "$repo_root"

env CCACHE_DISABLE=1 make release

docker compose -f "$compose_file" up -d namenode datanode

"$repo_root/scripts/e2e/wait_for_hdfs.sh"
"$repo_root/scripts/e2e/upload_fixtures_to_hdfs.sh"
"$repo_root/scripts/e2e/exec_duckdb_in_hdfs_container.sh" "$container_test_bin" "$sql_test_file"
