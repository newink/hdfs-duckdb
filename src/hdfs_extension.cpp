#define DUCKDB_EXTENSION_MAIN

#include "hdfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void HdfsTemplateStatusFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &phase_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(phase_vector, result, args.size(), [&](string_t phase) {
		return StringVector::AddString(result, "hdfs extension scaffold active: " + phase.GetString());
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Keep the template minimal until the HdfsFileSystem implementation lands.
	auto status_function =
	    ScalarFunction("hdfs_template_status", {LogicalType::VARCHAR}, LogicalType::VARCHAR, HdfsTemplateStatusFun);
	loader.RegisterFunction(status_function);
}

void HdfsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HdfsExtension::Name() {
	return "hdfs";
}

std::string HdfsExtension::Version() const {
#ifdef EXT_VERSION_HDFS
	return EXT_VERSION_HDFS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hdfs, loader) {
	duckdb::LoadInternal(loader);
}
}
