#include "hdfs_trace.hpp"

#include "hdfs_constants.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <thread>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#else
#include <io.h>
#include <fcntl.h>
#endif

namespace duckdb {
namespace hdfs_duckdb {

// ─── HdfsTraceWriter ────────────────────────────────────────────────────────

HdfsTraceWriter::~HdfsTraceWriter() {
	std::lock_guard<std::mutex> lock(mu_);
	CloseFile();
}

void HdfsTraceWriter::Configure(const string &file_path, int64_t rotate_max_bytes) {
	std::lock_guard<std::mutex> lock(mu_);
	rotate_max_bytes_ = rotate_max_bytes;
	if (file_path == file_path_ && fd_ != -1) {
		// Same file already open — just update the rotate threshold and re-enable.
		enabled_.store(true, std::memory_order_relaxed);
		return;
	}
	CloseFile();
	file_path_ = file_path;
	OpenFile();
	// Only mark enabled when the fd is actually open; a failed open keeps us silent.
	if (fd_ != -1) {
		enabled_.store(true, std::memory_order_relaxed);
	}
}

void HdfsTraceWriter::Disable() {
	std::lock_guard<std::mutex> lock(mu_);
	enabled_.store(false, std::memory_order_relaxed);
	CloseFile();
}

// ─── Private helpers ─────────────────────────────────────────────────────────

void HdfsTraceWriter::OpenFile() {
	// mu_ must be held.
	if (file_path_.empty()) {
		return;
	}
#ifndef _WIN32
	fd_ = ::open(file_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
#else
	fd_ = ::_open(file_path_.c_str(), _O_WRONLY | _O_CREAT | _O_APPEND | _O_BINARY, _S_IWRITE);
#endif
	// fd_ == -1 on failure; the caller checks before setting enabled_.
	bytes_written_ = 0;
}

void HdfsTraceWriter::RotateIfNeeded(int64_t pending_bytes) {
	// mu_ must be held.  Called only when fd_ != -1.
	if (rotate_max_bytes_ <= 0) {
		return; // Rotation disabled by caller setting threshold to 0.
	}
	if (bytes_written_ + pending_bytes <= rotate_max_bytes_) {
		return; // Still within budget.
	}
	// Truncate: close and reopen with O_TRUNC.  Data since the last rotation is lost.
	::close(fd_);
	fd_ = -1;
	bytes_written_ = 0;
#ifndef _WIN32
	fd_ = ::open(file_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
#else
	fd_ = ::_open(file_path_.c_str(), _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY, _S_IWRITE);
#endif
}

void HdfsTraceWriter::CloseFile() {
	// mu_ must be held.
	if (fd_ != -1) {
		::close(fd_);
		fd_ = -1;
	}
	bytes_written_ = 0;
}

// ─── Static helpers ───────────────────────────────────────────────────────────

// Returns a minimal JSON-safe string: escapes \, ", and ASCII control chars.
// HDFS paths rarely contain these characters but correctness requires handling them.
string HdfsTraceWriter::JsonEscape(const char *s) {
	string result;
	if (!s) {
		return result;
	}
	// Reserve a bit more than input to amortise reallocations on escapes.
	result.reserve(strlen(s) + 8);
	for (; *s; ++s) {
		const unsigned char c = static_cast<unsigned char>(*s);
		if (c == '"') {
			result += "\\\"";
		} else if (c == '\\') {
			result += "\\\\";
		} else if (c == '\n') {
			result += "\\n";
		} else if (c == '\r') {
			result += "\\r";
		} else if (c == '\t') {
			result += "\\t";
		} else if (c < 0x20) {
			char esc[8];
			snprintf(esc, sizeof(esc), "\\u%04x", static_cast<unsigned int>(c));
			result += esc;
		} else {
			result += static_cast<char>(c);
		}
	}
	return result;
}

// ISO 8601 UTC timestamp with microsecond precision: 2026-04-07T17:55:47.123456+00:00
string HdfsTraceWriter::FormatTimestamp() {
	auto now = std::chrono::system_clock::now();
	auto since_epoch = now.time_since_epoch();
	auto us_part = std::chrono::duration_cast<std::chrono::microseconds>(since_epoch) % 1000000;
	auto time_t_val = std::chrono::system_clock::to_time_t(now);

	struct tm tm_utc;
#ifndef _WIN32
	gmtime_r(&time_t_val, &tm_utc); // Thread-safe UTC conversion.
#else
	gmtime_s(&tm_utc, &time_t_val);
#endif

	char base[32];
	strftime(base, sizeof(base), "%Y-%m-%dT%H:%M:%S", &tm_utc);

	char result[48];
	snprintf(result, sizeof(result), "%s.%06lld+00:00", base, static_cast<long long>(us_part.count()));
	return result;
}

// Returns the OS thread name, or an empty string on platforms that don't support it.
string HdfsTraceWriter::GetThreadName() {
#if defined(__APPLE__) || defined(__linux__)
	char name[64] = {};
	pthread_getname_np(pthread_self(), name, sizeof(name));
	return name;
#else
	return "";
#endif
}

// ─── Trace ────────────────────────────────────────────────────────────────────

void HdfsTraceWriter::Trace(const char *op, const string &path, double duration_ms, bool ok, const char *error,
                            std::initializer_list<std::pair<const char *, int64_t>> int_fields,
                            std::initializer_list<std::pair<const char *, const char *>> str_fields) {
	// Fast-path: single atomic load, no lock.
	if (!enabled_.load(std::memory_order_relaxed)) {
		return;
	}

	// Build the JSON line outside the lock to minimise contention.
	auto ts = FormatTimestamp();
	auto thread_name = GetThreadName();
	auto thread_id = std::hash<std::thread::id> {}(std::this_thread::get_id());

	string line;
	line.reserve(512);
	line += "{\"ts\":\"";
	line += ts;
	line += "\",\"op\":\"";
	line += JsonEscape(op);
	line += "\",\"path\":\"";
	line += JsonEscape(path.c_str());
	line += "\",\"duration_ms\":";

	char dur_buf[32];
	snprintf(dur_buf, sizeof(dur_buf), "%.3f", duration_ms);
	line += dur_buf;

	line += ",\"ok\":";
	line += ok ? "true" : "false";
	line += ",\"thread_name\":\"";
	line += JsonEscape(thread_name.c_str());
	line += "\",\"thread_id\":";
	line += std::to_string(thread_id);
	line += ",\"error\":";
	if (error) {
		line += "\"";
		line += JsonEscape(error);
		line += "\"";
	} else {
		line += "null";
	}

	for (auto &field : int_fields) {
		line += ",\"";
		line += field.first;
		line += "\":";
		line += std::to_string(field.second);
	}
	for (auto &field : str_fields) {
		line += ",\"";
		line += field.first;
		line += "\":\"";
		line += JsonEscape(field.second);
		line += "\"";
	}
	line += "}\n";

	const auto pending = static_cast<int64_t>(line.size());

	std::lock_guard<std::mutex> lock(mu_);
	if (fd_ == -1) {
		// Writer was disabled between the fast-path check and acquiring the lock.
		return;
	}
	RotateIfNeeded(pending);
	if (fd_ == -1) {
		// Rotation failed to reopen the file; drop this record silently.
		return;
	}
#ifndef _WIN32
	auto written = ::write(fd_, line.data(), line.size());
#else
	auto written = ::_write(fd_, line.data(), static_cast<unsigned int>(line.size()));
#endif
	if (written > 0) {
		bytes_written_ += written;
	}
}

// ─── Config helpers ────────────────────────────────────────────────────────────

HdfsTraceConfig ResolveTraceConfig(optional_ptr<FileOpener> opener) {
	HdfsTraceConfig config;
	Value value;

	if (FileOpener::TryGetCurrentSetting(opener, SETTING_TRACE_ENABLED, value) && !value.IsNull()) {
		config.enabled = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
	}
	if (FileOpener::TryGetCurrentSetting(opener, SETTING_TRACE_FILE_PATH, value) && !value.IsNull()) {
		config.file_path = StringValue::Get(value.DefaultCastAs(LogicalType::VARCHAR));
	}
	if (FileOpener::TryGetCurrentSetting(opener, SETTING_TRACE_ROTATE_MAX_BYTES, value) && !value.IsNull()) {
		config.rotate_max_bytes = BigIntValue::Get(value.DefaultCastAs(LogicalType::BIGINT));
	}
	return config;
}

void ApplyTraceConfig(HdfsTraceWriter &writer, const HdfsTraceConfig &config) {
	if (config.enabled && !config.file_path.empty()) {
		writer.Configure(config.file_path, config.rotate_max_bytes);
	} else {
		writer.Disable();
	}
}

} // namespace hdfs_duckdb
} // namespace duckdb
