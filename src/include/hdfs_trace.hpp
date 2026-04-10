#pragma once

#include "duckdb/common/file_opener.hpp"

#include <atomic>
#include <cstdint>
#include <initializer_list>
#include <mutex>
#include <utility>

namespace duckdb {
namespace hdfs_duckdb {

// Append-only JSONL trace writer for HDFS filesystem operations.
//
// Thread-safe: writes are serialized via mu_. IsEnabled() is lock-free (atomic load).
// Lifecycle: owned as a mutable member of HdfsFileSystem (one per DatabaseInstance).
// The enabled_ flag is the fast-path gate — callers load it without acquiring the lock.
class HdfsTraceWriter {
public:
	~HdfsTraceWriter();

	// Configure (or reconfigure) the writer. Acquires mu_. If file_path changed,
	// closes the old fd and opens the new one with O_WRONLY|O_CREAT|O_APPEND.
	// Sets enabled_=true on success (fd open). Thread-safe.
	void Configure(const string &file_path, int64_t rotate_max_bytes);

	// Disable tracing and close the underlying fd. Thread-safe.
	void Disable();

	// Lock-free fast path: relaxed load of enabled_.
	bool IsEnabled() const {
		return enabled_.load(std::memory_order_relaxed);
	}

	// Emit one JSONL record. No-op when disabled. Thread-safe.
	// int_fields are emitted as JSON numbers; str_fields as JSON strings.
	// error (non-null) emits as a JSON string; nullptr emits as JSON null.
	void Trace(const char *op, const string &path, double duration_ms, bool ok, const char *error = nullptr,
	           std::initializer_list<std::pair<const char *, int64_t>> int_fields = {},
	           std::initializer_list<std::pair<const char *, const char *>> str_fields = {});

private:
	std::atomic<bool> enabled_ {false};
	std::mutex mu_;
	string file_path_;
	int64_t rotate_max_bytes_ = INT64_C(10) * 1024 * 1024; // 10 MiB default
	int fd_ = -1;
	int64_t bytes_written_ = 0;

	// All three helpers require mu_ to be held by the caller.
	void OpenFile();
	void RotateIfNeeded(int64_t pending_bytes); // truncate-on-overflow rotation
	void CloseFile();

	static string FormatTimestamp(); // ISO 8601 UTC with microsecond precision
	static string GetThreadName();   // pthread name or empty string on Windows
	static string JsonEscape(const char *s);
};

// Resolved trace configuration, read from DuckDB session settings.
struct HdfsTraceConfig {
	bool enabled = false;
	string file_path;
	int64_t rotate_max_bytes = INT64_C(10) * 1024 * 1024;
};

// Resolve the three hdfs_trace_* settings from a FileOpener.
HdfsTraceConfig ResolveTraceConfig(optional_ptr<FileOpener> opener);

// Apply a resolved config: Configure() when enabled and file_path non-empty, Disable() otherwise.
void ApplyTraceConfig(HdfsTraceWriter &writer, const HdfsTraceConfig &config);

} // namespace hdfs_duckdb
} // namespace duckdb
