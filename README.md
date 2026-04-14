# Hdfs DuckDB

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This repository is now on DuckDB's **C++ extension template** and is the starting point for the HDFS filesystem extension
described in [docs/rfc-hdfs-filesystem-extension.md](docs/rfc-hdfs-filesystem-extension.md).

The current code is intentionally minimal. It only proves that:

- the repo layout matches `duckdb/extension-template`
- the extension is named `hdfs_duckdb`
- the DuckDB `1.4.2` C++ extension path is in place
- we have a safe placeholder function while the real `HdfsFileSystem` work is still pending


## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is optional at the moment because this scaffold currently has no third-party dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/hdfs_duckdb/hdfs_duckdb.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `hdfs_duckdb.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The current helper surface is:
```
D SELECT hdfs_duckdb_template_status('bootstrap') AS result;
D SELECT hdfs_duckdb_uri_scheme() AS scheme;
```

The DuckDB-facing filesystem URI scheme is `hdfs://`.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

For the local Dockerized Python end-to-end harness, run:
```sh
make e2e-python
```

This target will:

- build the release extension binary
- stage a local custom extension repository under `build/e2e/repo`
- start a no-auth HDFS cluster in Docker
- upload example Parquet fixtures into HDFS
- create a local virtualenv under `build/e2e/venv`
- run `pytest` on the host against an in-memory DuckDB connection

The Python tests intentionally use DuckDB's `INSTALL hdfs_duckdb; LOAD hdfs_duckdb;` flow from the staged local repository rather
than loading the extension binary directly by path.

If you want to keep the Docker services running for inspection after the test run, set:
```sh
HDFS_E2E_KEEP_SERVICES=1 make e2e-python
```

To pass extra pytest arguments through to the containerized test runner, set:
```sh
HDFS_E2E_PYTEST_ARGS="-k placeholder -vv" make e2e-python
```

## Local quality gates
CI currently enforces two code quality checks via GitHub Actions:

- `make format-check`
- `make tidy-check`

To run the same checks locally, use:
```sh
make quality-gates
```

The local prerequisites for this are:

- `python3`
- `cmake`
- `black==24.*`
- `clang-format`
- `clang-tidy` or `TIDY_BINARY=/path/to/clang-tidy`

`make quality-gates` enforces the same Black major version as CI to avoid formatter drift between local runs and GitHub Actions.

To install the repo-owned `pre-push` hook for this clone:
```sh
make install-git-hooks
```

The hook runs the same `make quality-gates` script before each push. If you also want it to run a local release build, set:
```sh
HDFS_PRE_PUSH_RUN_BUILD=1 git push
```

DuckDB extension tests are skipped outside the Linux CI container in the shared makefile. If your local environment matches that setup and you want to include tests too, set:
```sh
HDFS_PRE_PUSH_RUN_TESTS=1 LINUX_CI_IN_DOCKER=1 git push
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL hdfs_duckdb;
LOAD hdfs_duckdb;
```

## Setting up CLion

### Opening project
Configuring CLion with this extension requires a little work. Firstly, make sure that the DuckDB submodule is available.
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

### Debugging
To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`, and CMake Options to which the following flag should be added, with the path to the extension CMakeList:

```
-DDUCKDB_EXTENSION_CONFIGS=<path_to_the_exentension_CMakeLists.txt>
```

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`. Note that it is recommended to use the `unittest` executable for testing/development within CLion. The actual DuckDB CLI currently does not reliably work as a run target in CLion.
