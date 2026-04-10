PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=hdfs_duckdb
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

HOST_CPU_COUNT := $(shell sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)
ifeq ($(BUILD_THREADS),)
export CMAKE_BUILD_PARALLEL_LEVEL ?= $(HOST_CPU_COUNT)
else
export CMAKE_BUILD_PARALLEL_LEVEL := $(BUILD_THREADS)
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: quality-gates install-git-hooks e2e-python e2e-libhdfs

quality-gates:
	./scripts/check-quality-gates.sh

install-git-hooks:
	git config core.hooksPath .githooks

e2e-python:
	./scripts/e2e/run_python_e2e.sh

e2e-libhdfs:
	@if [ "$$(uname -s)" != "Linux" ]; then \
		printf '%s\n' \
			"e2e-libhdfs requires a Linux host because the command executes inside the Linux Hadoop container." \
			"Run this target on Linux, or invoke scripts/e2e/run_libhdfs_e2e.sh with HDFS_E2E_CONTAINER_TEST_BIN set to a Linux binary path that already exists inside the container." >&2; \
		exit 1; \
	fi
	./scripts/e2e/run_libhdfs_e2e.sh
