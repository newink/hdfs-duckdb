#include "catch.hpp"
#include "hdfs_client_registry.hpp"
#include "libhdfs.hpp"

using namespace duckdb;
using namespace duckdb::hdfs_duckdb;

TEST_CASE("hdfs_duckdb libhdfs candidate search prefers explicit directories", "[hdfs_duckdb][libhdfs_loader]") {
	auto candidates = GetLibHdfsCandidatePaths("/opt/arrow-libhdfs", "/opt/hadoop");

	REQUIRE(candidates == vector<string> {
	                          "/opt/arrow-libhdfs/libhdfs.so",
	                          "/opt/arrow-libhdfs/libhdfs.so.0.0.0",
	                          "/opt/arrow-libhdfs/libhdfs.dylib",
	                          "/opt/hadoop/lib/native/libhdfs.so",
	                          "/opt/hadoop/lib/native/libhdfs.so.0.0.0",
	                          "/opt/hadoop/lib/native/libhdfs.dylib",
	                          "libhdfs.so",
	                          "libhdfs.so.0.0.0",
	                          "libhdfs.dylib",
	                      });
}

TEST_CASE("hdfs_duckdb libhdfs candidate search deduplicates overlapping directories",
          "[hdfs_duckdb][libhdfs_loader]") {
	auto candidates = GetLibHdfsCandidatePaths("/opt/hadoop/lib/native", "/opt/hadoop");

	REQUIRE(candidates == vector<string> {
	                          "/opt/hadoop/lib/native/libhdfs.so",
	                          "/opt/hadoop/lib/native/libhdfs.so.0.0.0",
	                          "/opt/hadoop/lib/native/libhdfs.dylib",
	                          "libhdfs.so",
	                          "libhdfs.so.0.0.0",
	                          "libhdfs.dylib",
	                      });
}

TEST_CASE("hdfs_duckdb client cache keys isolate connection-affecting config", "[hdfs_duckdb][libhdfs_loader]") {
	HdfsResolvedUri uri;
	uri.target_filesystem = "hdfs://namenode:9000";

	HdfsResolvedConfig base;
	base.effective_user = "duck";
	base.ticket_cache_path = "FILE:/tmp/krb5cc_duck";
	base.extra_conf = {{"dfs.replication", "1"}};

	auto base_key = GetHdfsClientCacheKey(uri, base);

	auto different_user = base;
	different_user.effective_user = "other";
	REQUIRE(base_key != GetHdfsClientCacheKey(uri, different_user));

	auto different_ticket = base;
	different_ticket.ticket_cache_path = "FILE:/tmp/krb5cc_other";
	REQUIRE(base_key != GetHdfsClientCacheKey(uri, different_ticket));

	auto different_conf = base;
	different_conf.extra_conf = {{"dfs.replication", "2"}};
	REQUIRE(base_key != GetHdfsClientCacheKey(uri, different_conf));

	auto force_new = base;
	force_new.force_new_instance = true;
	REQUIRE(base_key != GetHdfsClientCacheKey(uri, force_new));
}
