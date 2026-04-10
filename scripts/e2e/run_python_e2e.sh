#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="$repo_root/test/e2e/docker-compose.yml"
venv_dir="$repo_root/build/e2e/venv"
home_dir="$repo_root/build/e2e/home"
python_bin="${PYTHON_BIN:-python3}"

cleanup() {
	docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
}

if [[ "${HDFS_E2E_KEEP_SERVICES:-0}" != "1" ]]; then
	trap cleanup EXIT
fi

cd "$repo_root"

env CCACHE_DISABLE=1 make release
"$repo_root/scripts/e2e/stage_local_extension_repo.sh" >/dev/null

docker compose -f "$compose_file" up -d namenode datanode

"$repo_root/scripts/e2e/wait_for_hdfs.sh"
"$repo_root/scripts/e2e/upload_fixtures_to_hdfs.sh"

if [[ ! -x "$venv_dir/bin/python" ]]; then
	"$python_bin" -m venv "$venv_dir"
fi
mkdir -p "$home_dir"

"$venv_dir/bin/python" -m pip install --disable-pip-version-check --no-cache-dir -r "$repo_root/test/python/requirements.txt"

pytest_cmd=("$venv_dir/bin/python" -m pytest -q "$repo_root/test/python/test_hdfs_e2e.py")
if [[ -n "${HDFS_E2E_PYTEST_ARGS:-}" ]]; then
	# shellcheck disable=SC2206
	extra_pytest_args=(${HDFS_E2E_PYTEST_ARGS})
	pytest_cmd+=("${extra_pytest_args[@]}")
fi

env \
	HOME="$home_dir" \
	HDFS_E2E_EXTENSION_REPO="$repo_root/build/e2e/repo/hdfs_duckdb/latest" \
	HDFS_E2E_HDFS_FIXTURE_DIR="/duckdb-hdfs-e2e/parquet" \
	HDFS_E2E_COMPOSE_FILE="$repo_root/test/e2e/docker-compose.yml" \
	"${pytest_cmd[@]}"
