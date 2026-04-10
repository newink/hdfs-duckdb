#include "catch.hpp"
#include "hdfs_path.hpp"

using namespace duckdb;
using namespace duckdb::hdfs_duckdb;
using Catch::Matchers::Contains;

TEST_CASE("hdfs_duckdb resolves explicit authorities", "[hdfs_duckdb][path]") {
	HdfsResolvedConfig config;
	auto resolved = ResolveUri("hdfs://namenode:8020//warehouse/./events/../date=*/part-*.parquet", config);

	REQUIRE_FALSE(resolved.uses_default_fs);
	REQUIRE(resolved.authority == "namenode:8020");
	REQUIRE(resolved.path == "/warehouse/date=*/part-*.parquet");
	REQUIRE(resolved.target_filesystem == "hdfs://namenode:8020");
	REQUIRE(resolved.normalized_target_uri == "hdfs://namenode:8020/warehouse/date=*/part-*.parquet");
}

TEST_CASE("hdfs_duckdb resolves authority-less URIs through hdfs_default_fs", "[hdfs_duckdb][path]") {
	HdfsResolvedConfig config;
	config.default_fs = "viewfs://analytics/";

	auto resolved = ResolveUri("hdfs:///warehouse/./events/../part.parquet", config);

	REQUIRE(resolved.uses_default_fs);
	REQUIRE(resolved.authority.empty());
	REQUIRE(resolved.path == "/warehouse/part.parquet");
	REQUIRE(resolved.target_filesystem == "viewfs://analytics");
	REQUIRE(resolved.normalized_target_uri == "viewfs://analytics/warehouse/part.parquet");
}

TEST_CASE("hdfs_duckdb rejects malformed URIs and missing default_fs", "[hdfs_duckdb][path]") {
	HdfsResolvedConfig config;

	REQUIRE_THROWS_WITH(ResolveUri("hdfs://", config),
	                    Contains("expected either hdfs:///absolute/path or hdfs://authority/absolute/path"));
	REQUIRE_THROWS_WITH(ResolveUri("hdfs:///warehouse/data.parquet", config),
	                    Contains("requires hdfs_default_fs to be set"));
	REQUIRE_THROWS_WITH(ResolveUri("hdfs://alice@namenode/path", config), Contains("use hdfs_user instead"));
}
