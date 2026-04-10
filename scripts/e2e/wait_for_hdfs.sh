#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${COMPOSE_FILE:-$repo_root/test/e2e/docker-compose.yml}"

for _ in $(seq 1 60); do
	if docker compose -f "$compose_file" exec -T namenode sh -lc 'PATH=/opt/hadoop-3.2.1/bin:$PATH hdfs dfs -ls /' >/dev/null 2>&1; then
		exit 0
	fi
	sleep 2
done

printf 'timed out waiting for HDFS to become ready\n' >&2
exit 1
