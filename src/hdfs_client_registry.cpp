#include "hdfs_client_registry.hpp"

#include "hdfs_constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cerrno>
#include <mutex>
#include <functional>

namespace duckdb {
namespace hdfs_duckdb {

HdfsClient::~HdfsClient() {
	if (fs && api && api->hdfsDisconnect) {
		api->hdfsDisconnect(fs);
	}
}

string GetHdfsClientCacheKey(const HdfsResolvedUri &uri, const HdfsResolvedConfig &config) {
	string key = uri.target_filesystem;
	key += "\nuser=" + config.effective_user;
	key += "\nticket=" + config.ticket_cache_path;
	key += "\nforce=" + std::to_string(config.force_new_instance ? 1 : 0);
	for (auto &entry : config.extra_conf) {
		key += "\nconf:" + entry.first + "=" + entry.second;
	}
	return key;
}

static bool RequiresFreshLibhdfsInstance(const HdfsResolvedConfig &config) {
	return config.force_new_instance || !config.ticket_cache_path.empty() || !config.extra_conf.empty();
}

static shared_ptr<HdfsClient> CreateHdfsClient(const HdfsResolvedUri &uri, const HdfsResolvedConfig &config) {
	auto api = GetLibHdfsApi();
	auto builder = api->hdfsNewBuilder();
	if (!builder) {
		throw IOException("HdfsFileSystem: failed to allocate libhdfs builder for \"%s\"", uri.original_uri);
	}

	auto free_builder = true;
	try {
		api->hdfsBuilderSetNameNode(builder, uri.target_filesystem.c_str());
		if (!config.effective_user.empty()) {
			api->hdfsBuilderSetUserName(builder, config.effective_user.c_str());
		}
		if (!config.ticket_cache_path.empty()) {
			api->hdfsBuilderSetKerbTicketCachePath(builder, config.ticket_cache_path.c_str());
		}
		if (RequiresFreshLibhdfsInstance(config)) {
			api->hdfsBuilderSetForceNewInstance(builder);
		}
		for (auto &entry : config.extra_conf) {
			auto rc = api->hdfsBuilderConfSetStr(builder, entry.first.c_str(), entry.second.c_str());
			if (rc != 0) {
				throw IOException("HdfsFileSystem: failed to set Hadoop config \"%s\" for \"%s\"", entry.first,
				                  uri.original_uri);
			}
		}

		errno = 0;
		auto fs = api->hdfsBuilderConnect(builder);
		free_builder = false;
		if (!fs) {
			auto error = errno == 0 ? "no additional error detail from libhdfs" : string(strerror(errno));
			throw IOException(
			    "HdfsFileSystem: failed to connect to \"%s\" via libhdfs (%s). Check JAVA_HOME, HADOOP_HOME, "
			    "CLASSPATH, and Hadoop config such as core-site.xml and hdfs-site.xml.",
			    uri.target_filesystem, error);
		}

		return make_shared_ptr<HdfsClient>(api, fs, GetHdfsClientCacheKey(uri, config), uri.target_filesystem);
	} catch (...) {
		if (free_builder) {
			api->hdfsFreeBuilder(builder);
		}
		throw;
	}
}

shared_ptr<HdfsClient> GetHdfsClient(const HdfsResolvedUri &uri, const HdfsResolvedConfig &config) {
	static std::mutex registry_lock;
	static unordered_map<string, shared_ptr<HdfsClient>> registry;

	if (config.force_new_instance) {
		return CreateHdfsClient(uri, config);
	}

	auto key = GetHdfsClientCacheKey(uri, config);
	std::lock_guard<std::mutex> guard(registry_lock);
	auto entry = registry.find(key);
	if (entry != registry.end()) {
		return entry->second;
	}

	// Before inserting a new connection, sweep entries whose sole owner is the
	// registry itself (use_count == 1 means no active file handles hold it).
	// Expected registry size is 1-3; this guard handles pathological key churn.
	if (registry.size() >= 32) {
		for (auto it = registry.begin(); it != registry.end();) {
			if (it->second.use_count() == 1) {
				it = registry.erase(it);
			} else {
				++it;
			}
		}
	}

	auto client = CreateHdfsClient(uri, config);
	registry[key] = client;
	return client;
}

string ResolveRuntimeDefaultFs(const HdfsResolvedConfig &config) {
	if (!config.default_fs.empty()) {
		return NormalizeDefaultFs(config.default_fs);
	}

	auto api = GetLibHdfsApi();
	char *default_fs_ptr = nullptr;
	auto rc = api->hdfsConfGetStr("fs.defaultFS", &default_fs_ptr);
	duckdb::unique_ptr<char, std::function<void(char *)>> default_fs_guard(default_fs_ptr, [&](char *ptr) {
		if (ptr) {
			api->hdfsConfStrFree(ptr);
		}
	});

	if (rc != 0 || !default_fs_ptr || !default_fs_ptr[0]) {
		throw IOException("HdfsFileSystem: authority-less hdfs:/// paths require either %s to be set or Hadoop "
		                  "fs.defaultFS to be available through libhdfs configuration",
		                  SETTING_DEFAULT_FS);
	}
	return NormalizeDefaultFs(default_fs_ptr);
}

} // namespace hdfs_duckdb
} // namespace duckdb
