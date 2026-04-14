import os
import subprocess

import duckdb

EXTENSION_REPO = os.environ.get("HDFS_E2E_EXTENSION_REPO", "build/e2e/repo/hdfs_duckdb/latest")
HDFS_FIXTURE_DIR = os.environ.get("HDFS_E2E_HDFS_FIXTURE_DIR", "/duckdb-hdfs-e2e/parquet")
COMPOSE_FILE = os.environ.get("HDFS_E2E_COMPOSE_FILE", "test/e2e/docker-compose.yml")
EXTENSION_NAME = "hdfs_duckdb"
EXPECTED_URI_SCHEME = "hdfs://"


def connect_and_load():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    repo_path = EXTENSION_REPO.replace("'", "''")
    con.execute(f"SET custom_extension_repository='{repo_path}'")
    con.execute(f"INSTALL {EXTENSION_NAME}")
    con.execute(f"LOAD {EXTENSION_NAME}")
    return con


def list_hdfs_fixtures():
    command = [
        "docker-compose",
        "-f",
        COMPOSE_FILE,
        "exec",
        "-T",
        "namenode",
        "sh",
        "-lc",
        f"PATH=/opt/hadoop-3.2.1/bin:$PATH hdfs dfs -ls {HDFS_FIXTURE_DIR}",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return [line for line in result.stdout.splitlines() if line.strip().endswith(".parquet")]


def test_can_install_and_load_hdfs_from_local_repo():
    con = connect_and_load()
    try:
        loaded = con.execute(
            """
            SELECT loaded
            FROM duckdb_extensions()
            WHERE extension_name = 'hdfs_duckdb'
            """
        ).fetchone()
        assert loaded == (True,)
    finally:
        con.close()


def test_placeholder_function_works_after_load():
    con = connect_and_load()
    try:
        result = con.execute("SELECT hdfs_duckdb_template_status('bootstrap')").fetchone()
        assert result == (f"{EXTENSION_NAME} filesystem dispatch active for {EXPECTED_URI_SCHEME}: bootstrap",)
        uri_scheme = con.execute("SELECT hdfs_duckdb_uri_scheme()").fetchone()
        assert uri_scheme == (EXPECTED_URI_SCHEME,)
    finally:
        con.close()


def test_hdfs_fixture_files_are_present_in_hdfs():
    parquet_files = list_hdfs_fixtures()

    assert len(parquet_files) >= 2
    sizes = [int(line.split()[4]) for line in parquet_files]
    assert all(size > 0 for size in sizes)
