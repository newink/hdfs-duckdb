#pragma once

namespace duckdb {
namespace hdfs_duckdb {

constexpr const char *EXTENSION_NAME = "hdfs_duckdb";
constexpr const char *FILESYSTEM_NAME = "HdfsFileSystem";
constexpr const char *URI_SCHEME = "hdfs://";
constexpr const char *TEMPLATE_STATUS_FUNCTION = "hdfs_duckdb_template_status";
constexpr const char *URI_SCHEME_FUNCTION = "hdfs_duckdb_uri_scheme";
constexpr const char *NORMALIZE_URI_FUNCTION = "hdfs_duckdb_normalize_uri";

constexpr const char *SETTING_DEFAULT_FS = "hdfs_default_fs";
constexpr const char *SETTING_USER = "hdfs_user";
constexpr const char *SETTING_TICKET_CACHE_PATH = "hdfs_ticket_cache_path";
constexpr const char *SETTING_FORCE_NEW_INSTANCE = "hdfs_force_new_instance";
constexpr const char *SETTING_EXTRA_CONF = "hdfs_extra_conf";
constexpr const char *SETTING_TRACE_ENABLED = "hdfs_trace_enabled";
constexpr const char *SETTING_TRACE_FILE_PATH = "hdfs_trace_file_path";
constexpr const char *SETTING_TRACE_ROTATE_MAX_BYTES = "hdfs_trace_rotate_max_bytes";

constexpr const char *READ_ONLY_BACKEND_MESSAGE =
    "phase 2 implements a read-only libhdfs backend; write and directory mutation operations are not supported";

} // namespace hdfs_duckdb
} // namespace duckdb
