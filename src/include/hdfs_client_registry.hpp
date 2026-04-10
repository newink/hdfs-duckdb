#pragma once

#include "hdfs_path.hpp"
#include "libhdfs.hpp"

namespace duckdb {
namespace hdfs_duckdb {

struct HdfsClient {
	HdfsClient(shared_ptr<LibHdfsApi> api_p, hdfsFS fs_p, string cache_key_p, string target_filesystem_p)
	    : api(std::move(api_p)), fs(fs_p), cache_key(std::move(cache_key_p)),
	      target_filesystem(std::move(target_filesystem_p)) {
	}
	~HdfsClient();

	shared_ptr<LibHdfsApi> api;
	hdfsFS fs = nullptr;
	string cache_key;
	string target_filesystem;
};

shared_ptr<HdfsClient> GetHdfsClient(const HdfsResolvedUri &uri, const HdfsResolvedConfig &config);
string GetHdfsClientCacheKey(const HdfsResolvedUri &uri, const HdfsResolvedConfig &config);
string ResolveRuntimeDefaultFs(const HdfsResolvedConfig &config);

} // namespace hdfs_duckdb
} // namespace duckdb
