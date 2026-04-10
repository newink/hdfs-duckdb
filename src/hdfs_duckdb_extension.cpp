#define DUCKDB_EXTENSION_MAIN

#include "hdfs_config.hpp"
#include "hdfs_constants.hpp"
#include "hdfs_duckdb_extension.hpp"
#include "hdfs_filesystem.hpp"
#include "hdfs_functions.hpp"
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

static void RegisterHdfsFileSystem(ExtensionLoader &loader) {
	auto &file_system = FileSystem::GetFileSystem(loader.GetDatabaseInstance());
	for (auto &name : file_system.ListSubSystems()) {
		if (name == hdfs_duckdb::FILESYSTEM_NAME) {
			return;
		}
	}
	file_system.RegisterSubSystem(make_uniq<HdfsFileSystem>());
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Native HDFS filesystem plumbing for hdfs:// paths");
	hdfs_duckdb::RegisterHdfsSettings(loader);
	RegisterHdfsFunctions(loader);
	RegisterHdfsFileSystem(loader);
}

void HdfsDuckdbExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string HdfsDuckdbExtension::Name() {
	return hdfs_duckdb::EXTENSION_NAME;
}

std::string HdfsDuckdbExtension::Version() const {
#ifdef EXT_VERSION_HDFS_DUCKDB
	return EXT_VERSION_HDFS_DUCKDB;
#elif defined(EXT_VERSION_HDFS)
	return EXT_VERSION_HDFS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hdfs_duckdb, loader) {
	duckdb::LoadInternal(loader);
}
}
