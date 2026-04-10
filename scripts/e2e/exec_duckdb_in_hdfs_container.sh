#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${HDFS_E2E_COMPOSE_FILE:-$repo_root/test/e2e/docker-compose.yml}"
container_service="${HDFS_E2E_CONTAINER_SERVICE:-namenode}"

if [[ $# -eq 0 ]]; then
	set -- /workspace/build/release/test/unittest
fi

printf -v quoted_cmd '%q ' "$@"

docker compose -f "$compose_file" exec -T "$container_service" bash -lc "
	set -euo pipefail
	export PATH=/opt/hadoop-3.2.1/bin:\$PATH
	export HADOOP_HOME=\${HADOOP_HOME:-/opt/hadoop-3.2.1}
	export JAVA_HOME=\${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
	export ARROW_LIBHDFS_DIR=\${ARROW_LIBHDFS_DIR:-\$HADOOP_HOME/lib/native}
	export LD_LIBRARY_PATH=\$ARROW_LIBHDFS_DIR:\${LD_LIBRARY_PATH:-}
	export CLASSPATH=\${CLASSPATH:-\$(hadoop classpath --glob)}
	export HDFS_E2E_REQUIRE_LIBHDFS=1
	${quoted_cmd}
"
