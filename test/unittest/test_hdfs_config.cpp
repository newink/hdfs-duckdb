#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "hdfs_config.hpp"
#include "hdfs_duckdb_extension.hpp"
#include "test_helpers.hpp"

#include <cstdlib>

using namespace duckdb;
using namespace duckdb::hdfs_duckdb;
using Catch::Matchers::Contains;

namespace {
struct ScopedEnvVar {
	ScopedEnvVar(string name_p, string value_p) : name(std::move(name_p)) {
		auto old_value_ptr = std::getenv(name.c_str());
		if (old_value_ptr) {
			had_old_value = true;
			old_value = old_value_ptr;
		}
		Set(value_p);
	}

	~ScopedEnvVar() {
		if (had_old_value) {
			Set(old_value);
		} else {
			Unset();
		}
	}

	void Set(const string &value) {
#ifdef _WIN32
		_putenv_s(name.c_str(), value.c_str());
#else
		setenv(name.c_str(), value.c_str(), 1);
#endif
	}

	void Unset() {
#ifdef _WIN32
		_putenv_s(name.c_str(), "");
#else
		unsetenv(name.c_str());
#endif
	}

	string name;
	string old_value;
	bool had_old_value = false;
};

static HdfsResolvedConfig ResolveFromConnection(Connection &con) {
	ClientContextFileOpener opener(*con.context);
	return ResolveHdfsConfig(&opener);
}
} // namespace

TEST_CASE("hdfs_duckdb resolves settings from the active connection", "[hdfs_duckdb][config]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<HdfsDuckdbExtension>();
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("SET hdfs_default_fs='HDFS://nameservice1/'"));
	REQUIRE_NO_FAIL(con.Query("SET hdfs_user='duck'"));
	REQUIRE_NO_FAIL(con.Query("SET hdfs_ticket_cache_path='/tmp/krb5cc_duck'"));
	REQUIRE_NO_FAIL(con.Query("SET hdfs_force_new_instance=true"));
	REQUIRE_NO_FAIL(con.Query("SET hdfs_extra_conf=MAP(['dfs.z', 'dfs.a'], ['2', '1'])"));

	auto config = ResolveFromConnection(con);
	REQUIRE(config.default_fs == "hdfs://nameservice1");
	REQUIRE(config.effective_user == "duck");
	REQUIRE(config.ticket_cache_path == "/tmp/krb5cc_duck");
	REQUIRE(config.force_new_instance);
	REQUIRE(config.extra_conf == vector<std::pair<string, string>> {{"dfs.a", "1"}, {"dfs.z", "2"}});
}

TEST_CASE("hdfs_duckdb falls back to Hadoop/Kerberos environment variables", "[hdfs_duckdb][config]") {
	ScopedEnvVar hadoop_user("HADOOP_USER_NAME", "env-user");
	ScopedEnvVar krb5_ccname("KRB5CCNAME", "FILE:/tmp/krb5cc_env");

	DuckDB db(nullptr);
	db.LoadStaticExtension<HdfsDuckdbExtension>();
	Connection con(db);

	auto config = ResolveFromConnection(con);
	REQUIRE(config.effective_user == "env-user");
	REQUIRE(config.ticket_cache_path == "FILE:/tmp/krb5cc_env");

	REQUIRE_NO_FAIL(con.Query("SET hdfs_user='session-user'"));
	REQUIRE_NO_FAIL(con.Query("SET hdfs_ticket_cache_path='/tmp/session-ccache'"));
	config = ResolveFromConnection(con);
	REQUIRE(config.effective_user == "session-user");
	REQUIRE(config.ticket_cache_path == "/tmp/session-ccache");
}

TEST_CASE("hdfs_duckdb validates extension settings", "[hdfs_duckdb][config]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<HdfsDuckdbExtension>();
	Connection con(db);

	auto invalid_default_fs = con.Query("SET hdfs_default_fs='https://gateway.example.com'");
	REQUIRE_FAIL(invalid_default_fs);
	REQUIRE_THAT(invalid_default_fs->GetError(), Contains("must use hdfs:// or viewfs://"));

	auto invalid_extra_conf = con.Query("SET hdfs_extra_conf=MAP(['dfs.ok', ''], ['1', '2'])");
	REQUIRE_FAIL(invalid_extra_conf);
	REQUIRE_THAT(invalid_extra_conf->GetError(), Contains("does not allow empty configuration keys"));
}
