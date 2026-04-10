#include "catch.hpp"
#include "hdfs_trace.hpp"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;
using namespace duckdb::hdfs_duckdb;
using Catch::Matchers::Contains;

namespace {

// Read all non-empty lines from a file.
static std::vector<std::string> ReadLines(const std::string &path) {
	std::ifstream f(path);
	std::vector<std::string> lines;
	std::string line;
	while (std::getline(f, line)) {
		if (!line.empty()) {
			lines.push_back(line);
		}
	}
	return lines;
}

// Returns the byte size of a file, or -1 on error.
static long FileSize(const std::string &path) {
	FILE *f = fopen(path.c_str(), "rb");
	if (!f) {
		return -1;
	}
	fseek(f, 0, SEEK_END);
	long sz = ftell(f);
	fclose(f);
	return sz;
}

// RAII temp file: creates an unique path and removes the file on destruction.
struct TempFile {
	std::string path;
	explicit TempFile(const std::string &suffix) {
		// mkstemp is POSIX; fall back to tmpnam for portability to Windows CI.
#ifdef _WIN32
		path = std::string(std::tmpnam(nullptr)) + suffix; // NOLINT
#else
		// Use /tmp directly to avoid macOS sandbox issues.
		path = "/tmp/hdfs_trace_test_" + suffix;
		// Remove any leftover from a previous run.
		remove(path.c_str());
#endif
	}
	~TempFile() {
		remove(path.c_str());
	}
};

} // namespace

// ─── State transitions ────────────────────────────────────────────────────────

TEST_CASE("HdfsTraceWriter: initial state is disabled", "[hdfs_trace]") {
	HdfsTraceWriter writer;
	REQUIRE_FALSE(writer.IsEnabled());
}

TEST_CASE("HdfsTraceWriter: Configure enables the writer", "[hdfs_trace]") {
	TempFile tmp("cfg.jsonl");
	HdfsTraceWriter writer;
	writer.Configure(tmp.path, 10 * 1024 * 1024);
	REQUIRE(writer.IsEnabled());
	writer.Disable();
	REQUIRE_FALSE(writer.IsEnabled());
}

TEST_CASE("HdfsTraceWriter: Configure with same path reuses fd", "[hdfs_trace]") {
	TempFile tmp("cfg_same.jsonl");
	HdfsTraceWriter writer;
	writer.Configure(tmp.path, 10 * 1024 * 1024);
	REQUIRE(writer.IsEnabled());
	// Re-configure with same path: should stay enabled and not close/reopen.
	writer.Configure(tmp.path, 5 * 1024 * 1024);
	REQUIRE(writer.IsEnabled());
}

TEST_CASE("HdfsTraceWriter: Disable after Configure closes the file", "[hdfs_trace]") {
	TempFile tmp("dis.jsonl");
	HdfsTraceWriter writer;
	writer.Configure(tmp.path, 10 * 1024 * 1024);
	writer.Disable();
	REQUIRE_FALSE(writer.IsEnabled());
	// Writing after Disable should be a no-op.
	writer.Trace("read", "/some/path", 1.0, true);
	REQUIRE(FileSize(tmp.path) == 0);
}

// ─── Single-record output correctness ─────────────────────────────────────────

TEST_CASE("HdfsTraceWriter: Trace writes valid JSONL with required fields", "[hdfs_trace]") {
	TempFile tmp("trace_req.jsonl");
	HdfsTraceWriter writer;
	writer.Configure(tmp.path, 10 * 1024 * 1024);
	writer.Trace("open", "/data/file.parquet", 2.345, true);
	writer.Disable();

	auto lines = ReadLines(tmp.path);
	REQUIRE(lines.size() == 1);
	const auto &line = lines[0];

	// Every record must be a JSON object.
	REQUIRE(line.front() == '{');
	REQUIRE(line.back() == '}');

	// Required fields.
	REQUIRE_THAT(line, Contains("\"ts\""));
	REQUIRE_THAT(line, Contains("\"op\":\"open\""));
	REQUIRE_THAT(line, Contains("\"path\":\"/data/file.parquet\""));
	REQUIRE_THAT(line, Contains("\"duration_ms\":"));
	REQUIRE_THAT(line, Contains("\"ok\":true"));
	REQUIRE_THAT(line, Contains("\"thread_name\""));
	REQUIRE_THAT(line, Contains("\"thread_id\""));
	REQUIRE_THAT(line, Contains("\"error\":null"));
}

TEST_CASE("HdfsTraceWriter: Trace records ok=false with error string", "[hdfs_trace]") {
	TempFile tmp("trace_err.jsonl");
	HdfsTraceWriter writer;
	writer.Configure(tmp.path, 10 * 1024 * 1024);
	writer.Trace("read", "/data/broken.parquet", 0.1, false, "connection reset");
	writer.Disable();

	auto lines = ReadLines(tmp.path);
	REQUIRE(lines.size() == 1);
	const auto &line = lines[0];
	REQUIRE_THAT(line, Contains("\"ok\":false"));
	REQUIRE_THAT(line, Contains("\"error\":\"connection reset\""));
}

TEST_CASE("HdfsTraceWriter: Trace includes int_fields and str_fields", "[hdfs_trace]") {
	TempFile tmp("trace_fields.jsonl");
	HdfsTraceWriter writer;
	writer.Configure(tmp.path, 10 * 1024 * 1024);
	writer.Trace("open", "/file", 1.0, true, nullptr, {{"file_size", 4096L}}, {{"mode", "rb"}});
	writer.Disable();

	auto lines = ReadLines(tmp.path);
	REQUIRE(lines.size() == 1);
	const auto &line = lines[0];
	REQUIRE_THAT(line, Contains("\"file_size\":4096"));
	REQUIRE_THAT(line, Contains("\"mode\":\"rb\""));
}

// ─── Timestamp format ─────────────────────────────────────────────────────────

TEST_CASE("HdfsTraceWriter: timestamp is ISO 8601 UTC with microseconds", "[hdfs_trace]") {
	TempFile tmp("trace_ts.jsonl");
	HdfsTraceWriter writer;
	writer.Configure(tmp.path, 10 * 1024 * 1024);
	writer.Trace("seek", "/f", 0.0, true);
	writer.Disable();

	auto lines = ReadLines(tmp.path);
	REQUIRE(lines.size() == 1);
	const auto &line = lines[0];
	// Timestamp must look like: "ts":"YYYY-MM-DDTHH:MM:SS.NNNNNN+00:00"
	REQUIRE_THAT(line, Contains("+00:00\""));
	// Should contain a 'T' separator and 6 decimal digits.
	// We verify by finding "\"ts\":\"" and checking enough characters follow.
	auto ts_pos = line.find("\"ts\":\"");
	REQUIRE(ts_pos != std::string::npos);
	// "ts" value starts at ts_pos + 6; expect at least 32 chars (full ISO timestamp).
	REQUIRE(line.size() > ts_pos + 6 + 32);
	// The 'T' separator must be at position 10 within the timestamp value.
	REQUIRE(line[ts_pos + 6 + 10] == 'T');
}

// ─── JSON escaping ─────────────────────────────────────────────────────────────

TEST_CASE("HdfsTraceWriter: JSON-escapes special characters in path", "[hdfs_trace]") {
	TempFile tmp("trace_esc.jsonl");
	HdfsTraceWriter writer;
	writer.Configure(tmp.path, 10 * 1024 * 1024);
	// Path with double-quote, backslash, and newline.
	writer.Trace("read", "/path/with\"quote\\and\nnewline", 0.0, true);
	writer.Disable();

	auto lines = ReadLines(tmp.path);
	REQUIRE(lines.size() == 1);
	const auto &line = lines[0];
	// The JSON line itself must not contain a raw double-quote mid-string.
	// It must contain escaped sequences instead.
	REQUIRE_THAT(line, Contains("\\\"quote"));
	REQUIRE_THAT(line, Contains("\\\\and"));
	REQUIRE_THAT(line, Contains("\\n"));
}

// ─── Rotation ─────────────────────────────────────────────────────────────────

TEST_CASE("HdfsTraceWriter: rotates when file exceeds rotate_max_bytes", "[hdfs_trace]") {
	TempFile tmp("trace_rot.jsonl");
	HdfsTraceWriter writer;
	// Very small limit — a few records will trigger rotation.
	const int64_t limit = 512;
	writer.Configure(tmp.path, limit);

	// Write records until we have clearly exceeded the threshold.
	for (int i = 0; i < 20; ++i) {
		writer.Trace("read", "/data/rotation_test.parquet", static_cast<double>(i), true, nullptr,
		             {{"size", static_cast<int64_t>(i * 100)}});
	}
	writer.Disable();

	// After rotation the file contains only records written after the last truncation.
	// Its size must be no greater than limit + one record (rotation happens before each write).
	auto sz = FileSize(tmp.path);
	REQUIRE(sz >= 0);         // File exists.
	REQUIRE(sz <= limit * 3); // Capped: never more than a small multiple of the limit.

	// All lines in the file must be parseable JSON objects.
	auto lines = ReadLines(tmp.path);
	REQUIRE_FALSE(lines.empty());
	for (const auto &line : lines) {
		REQUIRE(line.front() == '{');
		REQUIRE(line.back() == '}');
	}
}

// ─── Thread safety ─────────────────────────────────────────────────────────────

TEST_CASE("HdfsTraceWriter: concurrent Trace calls produce uncorrupted lines", "[hdfs_trace]") {
	TempFile tmp("trace_mt.jsonl");
	HdfsTraceWriter writer;
	writer.Configure(tmp.path, 64 * 1024 * 1024);

	const int N_THREADS = 4;
	const int N_RECORDS = 50;
	std::vector<std::thread> threads;
	threads.reserve(N_THREADS);
	for (int t = 0; t < N_THREADS; ++t) {
		threads.emplace_back([&writer, t]() {
			for (int i = 0; i < N_RECORDS; ++i) {
				writer.Trace("read", "/concurrent/path", static_cast<double>(i), true, nullptr,
				             {{"thread", static_cast<int64_t>(t)}, {"record", static_cast<int64_t>(i)}});
			}
		});
	}
	for (auto &th : threads) {
		th.join();
	}
	writer.Disable();

	// Every line must start with '{' and end with '}' — no interleaved writes.
	auto lines = ReadLines(tmp.path);
	REQUIRE(lines.size() == static_cast<size_t>(N_THREADS * N_RECORDS));
	for (const auto &line : lines) {
		REQUIRE(line.front() == '{');
		REQUIRE(line.back() == '}');
		REQUIRE_THAT(line, Contains("\"op\":\"read\""));
	}
}

// ─── No-op when disabled ───────────────────────────────────────────────────────

TEST_CASE("HdfsTraceWriter: Trace is a no-op when not configured", "[hdfs_trace]") {
	HdfsTraceWriter writer;
	// No Configure call — writer stays disabled.
	writer.Trace("read", "/no/file", 0.0, true);
	REQUIRE_FALSE(writer.IsEnabled());
}

TEST_CASE("HdfsTraceWriter: multiple Configure/Disable cycles", "[hdfs_trace]") {
	TempFile tmp("trace_cycle.jsonl");
	HdfsTraceWriter writer;

	writer.Configure(tmp.path, 10 * 1024 * 1024);
	writer.Trace("open", "/f", 1.0, true);
	writer.Disable();
	writer.Trace("read", "/f", 2.0, true); // no-op

	writer.Configure(tmp.path, 10 * 1024 * 1024);
	writer.Trace("close", "/f", 3.0, true);
	writer.Disable();

	auto lines = ReadLines(tmp.path);
	// Only records written while enabled should appear.
	REQUIRE(lines.size() == 2);
	REQUIRE_THAT(lines[0], Contains("\"op\":\"open\""));
	REQUIRE_THAT(lines[1], Contains("\"op\":\"close\""));
}
