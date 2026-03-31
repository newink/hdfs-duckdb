PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=hdfs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: quality-gates install-git-hooks

quality-gates:
	./scripts/check-quality-gates.sh

install-git-hooks:
	git config core.hooksPath .githooks
