#include "hdfs_config.hpp"

#include "hdfs_constants.hpp"
#include "hdfs_path.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace hdfs_duckdb {

Value EmptyExtraConfValue() {
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, vector<Value> {}, vector<Value> {});
}

static vector<std::pair<string, string>> NormalizeExtraConfPairs(const Value &value) {
	vector<std::pair<string, string>> extra_conf;
	for (auto &entry : MapValue::GetChildren(value)) {
		auto &children = StructValue::GetChildren(entry);
		D_ASSERT(children.size() == 2);

		auto key = children[0].DefaultCastAs(LogicalType::VARCHAR);
		if (key.IsNull() || StringValue::Get(key).empty()) {
			throw InvalidInputException("Setting %s does not allow empty configuration keys", SETTING_EXTRA_CONF);
		}

		auto val = children[1].DefaultCastAs(LogicalType::VARCHAR);
		if (val.IsNull()) {
			throw InvalidInputException("Setting %s does not allow NULL configuration values", SETTING_EXTRA_CONF);
		}
		extra_conf.emplace_back(StringValue::Get(key), StringValue::Get(val));
	}
	sort(extra_conf.begin(), extra_conf.end());
	return extra_conf;
}

static Value NormalizeExtraConfValue(const Value &value) {
	auto extra_conf = NormalizeExtraConfPairs(value);
	vector<Value> keys;
	vector<Value> values;
	keys.reserve(extra_conf.size());
	values.reserve(extra_conf.size());
	for (auto &entry : extra_conf) {
		keys.emplace_back(entry.first);
		values.emplace_back(entry.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

static void ValidateDefaultFsSetting(ClientContext &, SetScope, Value &parameter) {
	auto normalized = NormalizeDefaultFs(StringValue::Get(parameter.DefaultCastAs(LogicalType::VARCHAR)));
	parameter = Value(normalized);
}

static void ValidateExtraConfSetting(ClientContext &, SetScope, Value &parameter) {
	if (parameter.IsNull()) {
		parameter = EmptyExtraConfValue();
		return;
	}
	parameter = NormalizeExtraConfValue(parameter);
}

void RegisterHdfsSettings(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption(SETTING_DEFAULT_FS, "Default HDFS/ViewFS URI used to resolve hdfs:/// paths",
	                          LogicalType::VARCHAR, Value(""), ValidateDefaultFsSetting);
	config.AddExtensionOption(SETTING_USER, "Effective HDFS user override", LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption(SETTING_TICKET_CACHE_PATH, "Kerberos ticket cache override for HDFS access",
	                          LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption(SETTING_FORCE_NEW_INSTANCE,
	                          "Force a fresh libhdfs FileSystem instance for the resolved config", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(false));
	config.AddExtensionOption(SETTING_EXTRA_CONF, "Extra Hadoop configuration overrides as MAP(VARCHAR, VARCHAR)",
	                          LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), EmptyExtraConfValue(),
	                          ValidateExtraConfSetting);
	config.AddExtensionOption(SETTING_TRACE_ENABLED, "Enable JSONL tracing of HDFS filesystem operations",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption(SETTING_TRACE_FILE_PATH,
	                          "Path for the HDFS trace JSONL output file (empty string disables tracing)",
	                          LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption(SETTING_TRACE_ROTATE_MAX_BYTES,
	                          "Maximum trace file size in bytes before rotation (truncate); default 10 MiB",
	                          LogicalType::BIGINT, Value::BIGINT(INT64_C(10) * 1024 * 1024));
}

template <class LOOKUP>
static string GetStringSetting(LOOKUP lookup, const string &name) {
	Value value;
	if (!lookup(name, value) || value.IsNull()) {
		return "";
	}
	return StringValue::Get(value.DefaultCastAs(LogicalType::VARCHAR));
}

template <class LOOKUP>
static bool GetBooleanSetting(LOOKUP lookup, const string &name, bool default_value) {
	Value value;
	if (!lookup(name, value) || value.IsNull()) {
		return default_value;
	}
	return BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
}

template <class LOOKUP>
static vector<std::pair<string, string>> GetExtraConfSetting(LOOKUP lookup) {
	Value value;
	if (!lookup(SETTING_EXTRA_CONF, value) || value.IsNull()) {
		return {};
	}
	return NormalizeExtraConfPairs(value);
}

template <class LOOKUP>
static HdfsResolvedConfig ResolveHdfsConfigInternal(LOOKUP lookup) {
	HdfsResolvedConfig config;
	config.default_fs = GetStringSetting(lookup, SETTING_DEFAULT_FS);
	config.effective_user = GetStringSetting(lookup, SETTING_USER);
	config.ticket_cache_path = GetStringSetting(lookup, SETTING_TICKET_CACHE_PATH);
	config.force_new_instance = GetBooleanSetting(lookup, SETTING_FORCE_NEW_INSTANCE, false);
	config.extra_conf = GetExtraConfSetting(lookup);

	if (config.effective_user.empty()) {
		config.effective_user = FileSystem::GetEnvVariable("HADOOP_USER_NAME");
	}
	if (config.ticket_cache_path.empty()) {
		config.ticket_cache_path = FileSystem::GetEnvVariable("KRB5CCNAME");
	}
	return config;
}

HdfsResolvedConfig ResolveHdfsConfig(const ClientContext &context) {
	auto lookup = [&](const string &name, Value &value) {
		return static_cast<bool>(context.TryGetCurrentSetting(name, value));
	};
	return ResolveHdfsConfigInternal(lookup);
}

HdfsResolvedConfig ResolveHdfsConfig(optional_ptr<FileOpener> opener) {
	auto lookup = [&](const string &name, Value &value) {
		return static_cast<bool>(FileOpener::TryGetCurrentSetting(opener, name, value));
	};
	return ResolveHdfsConfigInternal(lookup);
}

} // namespace hdfs_duckdb
} // namespace duckdb
