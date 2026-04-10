#include "hdfs_functions.hpp"

#include "hdfs_client_registry.hpp"
#include "hdfs_config.hpp"
#include "hdfs_constants.hpp"
#include "hdfs_path.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {
struct HdfsNormalizeBindData : public FunctionData {
	explicit HdfsNormalizeBindData(hdfs_duckdb::HdfsResolvedConfig config_p) : config(std::move(config_p)) {
	}

	hdfs_duckdb::HdfsResolvedConfig config;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<HdfsNormalizeBindData>(config);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<HdfsNormalizeBindData>();
		return config.default_fs == other.config.default_fs && config.effective_user == other.config.effective_user &&
		       config.ticket_cache_path == other.config.ticket_cache_path &&
		       config.force_new_instance == other.config.force_new_instance &&
		       config.extra_conf == other.config.extra_conf;
	}
};

inline void HdfsTemplateStatusFun(DataChunk &args, ExpressionState &, Vector &result) {
	auto &phase_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(phase_vector, result, args.size(), [&](string_t phase) {
		return StringVector::AddString(result, HdfsTemplateStatusMessage(phase.GetString()));
	});
}

inline void HdfsUriSchemeFun(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value(hdfs_duckdb::URI_SCHEME));
}

void HdfsNormalizeUriFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<HdfsNormalizeBindData>();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t uri) {
		auto runtime_config = bind_data.config;
		auto uri_string = uri.GetString();
		if (hdfs_duckdb::UriUsesDefaultFs(uri_string) && runtime_config.default_fs.empty()) {
			try {
				runtime_config.default_fs = hdfs_duckdb::ResolveRuntimeDefaultFs(runtime_config);
			} catch (const Exception &) {
			}
		}
		auto resolved = hdfs_duckdb::ResolveUri(uri_string, runtime_config);
		return StringVector::AddString(result, resolved.normalized_target_uri);
	});
}

unique_ptr<FunctionData> HdfsNormalizeUriBind(ClientContext &context, ScalarFunction &,
                                              vector<unique_ptr<Expression>> &) {
	return make_uniq<HdfsNormalizeBindData>(hdfs_duckdb::ResolveHdfsConfig(context));
}
} // namespace

std::string HdfsTemplateStatusMessage(const std::string &phase) {
	return std::string(hdfs_duckdb::EXTENSION_NAME) + " filesystem dispatch active for " +
	       std::string(hdfs_duckdb::URI_SCHEME) + ": " + phase;
}

void RegisterHdfsFunctions(ExtensionLoader &loader) {
	auto status_function = ScalarFunction(hdfs_duckdb::TEMPLATE_STATUS_FUNCTION, {LogicalType::VARCHAR},
	                                      LogicalType::VARCHAR, HdfsTemplateStatusFun);
	auto uri_scheme_function =
	    ScalarFunction(hdfs_duckdb::URI_SCHEME_FUNCTION, {}, LogicalType::VARCHAR, HdfsUriSchemeFun);
	auto normalize_uri_function = ScalarFunction(hdfs_duckdb::NORMALIZE_URI_FUNCTION, {LogicalType::VARCHAR},
	                                             LogicalType::VARCHAR, HdfsNormalizeUriFun, HdfsNormalizeUriBind);

	loader.RegisterFunction(status_function);
	loader.RegisterFunction(uri_scheme_function);
	loader.RegisterFunction(normalize_uri_function);
}

} // namespace duckdb
