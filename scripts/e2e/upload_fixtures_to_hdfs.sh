#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${COMPOSE_FILE:-$repo_root/test/e2e/docker-compose.yml}"
hdfs_dir="${HDFS_FIXTURE_DIR:-/duckdb-hdfs-e2e/parquet}"

docker compose -f "$compose_file" exec -T namenode bash -lc "
	set -euo pipefail
	export PATH=/opt/hadoop-3.2.1/bin:\$PATH
	hdfs dfs -rm -f -r '$hdfs_dir' >/dev/null 2>&1 || true
	hdfs dfs -mkdir -p '$hdfs_dir'
	hdfs dfs -put -f /workspace/test/data/parquet/*.parquet '$hdfs_dir/'
"
