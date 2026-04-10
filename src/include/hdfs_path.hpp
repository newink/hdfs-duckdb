#pragma once

#include "duckdb/common/common.hpp"

#include <utility>

namespace duckdb {
namespace hdfs_duckdb {

struct HdfsResolvedConfig {
	string default_fs;
	string effective_user;
	string ticket_cache_path;
	bool force_new_instance = false;
	vector<std::pair<string, string>> extra_conf;
};

struct HdfsResolvedUri {
	string original_uri;
	string authority;
	string path;
	string target_filesystem;
	string normalized_target_uri;
	bool uses_default_fs = false;
};

bool HasHdfsScheme(const string &path);
bool UriUsesDefaultFs(const string &uri);
string NormalizeDefaultFs(const string &default_fs);
string NormalizeHdfsPath(const string &path);
string BuildDuckdbUri(const HdfsResolvedUri &uri, const string &normalized_path);
HdfsResolvedUri ResolveUri(const string &uri, const HdfsResolvedConfig &config);

} // namespace hdfs_duckdb
} // namespace duckdb
