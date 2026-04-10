#pragma once

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/types/value.hpp"
#include "hdfs_path.hpp"

namespace duckdb {

class ClientContext;
class ExtensionLoader;

namespace hdfs_duckdb {

void RegisterHdfsSettings(ExtensionLoader &loader);
HdfsResolvedConfig ResolveHdfsConfig(const ClientContext &context);
HdfsResolvedConfig ResolveHdfsConfig(optional_ptr<FileOpener> opener = nullptr);
Value EmptyExtraConfValue();

} // namespace hdfs_duckdb
} // namespace duckdb
