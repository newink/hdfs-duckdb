#include "hdfs_filesystem.hpp"

#include "hdfs_client_registry.hpp"
#include "hdfs_config.hpp"
#include "hdfs_constants.hpp"
#include "hdfs_path.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/string_common.hpp"

#include <cerrno>
#include <cstring>
#include <limits>
#include <chrono>

#ifndef _WIN32
#include <fcntl.h>
#else
#define O_RDONLY 0
#endif

namespace duckdb {

namespace {

struct HdfsPathStatus {
	string normalized_path;
	FileType file_type = FileType::FILE_TYPE_INVALID;
	int64_t file_size = 0;
	timestamp_t last_modified;
};

class HdfsFileHandle : public FileHandle {
public:
	HdfsFileHandle(FileSystem &file_system_p, string path_p, FileOpenFlags flags_p,
	               shared_ptr<hdfs_duckdb::HdfsClient> client_p, hdfs_duckdb::hdfsFile file_p, string hdfs_path_p,
	               int64_t file_size_p, timestamp_t last_modified_p, FileType file_type_p,
	               hdfs_duckdb::HdfsTraceWriter *trace_writer_p = nullptr)
	    : FileHandle(file_system_p, std::move(path_p), flags_p), client(std::move(client_p)), file(file_p),
	      hdfs_path(std::move(hdfs_path_p)), file_size(file_size_p), last_modified(last_modified_p),
	      file_type(file_type_p), trace_writer(trace_writer_p) {
	}
	~HdfsFileHandle() override {
		try {
			Close();
		} catch (...) {
		}
	}

	void Close() override {
		if (!file) {
			return;
		}
		const bool tracing = trace_writer && trace_writer->IsEnabled();
		auto trace_start = tracing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
		errno = 0;
		auto close_rc = client->api->hdfsCloseFile(client->fs, file);
		file = nullptr;
		if (close_rc != 0) {
			auto error = errno == 0 ? "no additional error detail from libhdfs" : string(strerror(errno));
			if (tracing && trace_writer->IsEnabled()) {
				auto elapsed_ms =
				    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
				trace_writer->Trace("close", hdfs_path, elapsed_ms, false, error.c_str());
			}
			throw IOException("HdfsFileSystem: CloseFile failed for \"%s\": %s", path, error);
		}
		if (tracing && trace_writer->IsEnabled()) {
			auto elapsed_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
			trace_writer->Trace("close", hdfs_path, elapsed_ms, true);
		}
	}

	shared_ptr<hdfs_duckdb::HdfsClient> client;
	hdfs_duckdb::hdfsFile file = nullptr;
	string hdfs_path;
	int64_t file_size = 0;
	timestamp_t last_modified;
	FileType file_type = FileType::FILE_TYPE_INVALID;
	idx_t position = 0;
	// Non-owning pointer to the filesystem's trace writer. Valid for this handle's lifetime
	// because the filesystem outlives all handles it creates.
	hdfs_duckdb::HdfsTraceWriter *trace_writer = nullptr;
};

static const idx_t MAX_HDFS_IO_SIZE = NumericCast<idx_t>(std::numeric_limits<hdfs_duckdb::tSize>::max());

[[noreturn]] static void ThrowHdfsError(const string &operation, const string &path) {
	auto error = errno == 0 ? "no additional error detail from libhdfs" : string(strerror(errno));
	throw IOException("HdfsFileSystem: %s failed for \"%s\": %s", operation, path, error);
}

static string NormalizeHdfsInfoPath(const char *raw_name) {
	auto path = raw_name ? string(raw_name) : string("/");
	auto scheme_end = path.find("://");
	if (scheme_end != string::npos) {
		auto first_slash = path.find('/', scheme_end + 3);
		path = first_slash == string::npos ? "/" : path.substr(first_slash);
	}
	if (path.empty()) {
		return "/";
	}
	if (path[0] != '/') {
		path = "/" + path;
	}
	return hdfs_duckdb::NormalizeHdfsPath(path);
}

static string ExtractHdfsBaseName(const string &normalized_path) {
	if (normalized_path == "/") {
		return normalized_path;
	}
	auto slash = normalized_path.find_last_of('/');
	if (slash == string::npos) {
		return normalized_path;
	}
	return normalized_path.substr(slash + 1);
}

static string JoinHdfsPath(const string &lhs, const string &rhs) {
	if (lhs.empty() || lhs == "/") {
		return "/" + rhs;
	}
	return lhs + "/" + rhs;
}

static FileType ToDuckdbFileType(hdfs_duckdb::tObjectKind kind) {
	switch (kind) {
	case hdfs_duckdb::tObjectKind::kObjectKindFile:
		return FileType::FILE_TYPE_REGULAR;
	case hdfs_duckdb::tObjectKind::kObjectKindDirectory:
		return FileType::FILE_TYPE_DIR;
	default:
		return FileType::FILE_TYPE_INVALID;
	}
}

static HdfsPathStatus ToPathStatus(const hdfs_duckdb::hdfsFileInfo &info) {
	HdfsPathStatus status;
	status.normalized_path = NormalizeHdfsInfoPath(info.mName);
	status.file_type = ToDuckdbFileType(info.mKind);
	status.file_size = NumericCast<int64_t>(info.mSize);
	status.last_modified = Timestamp::FromTimeT(info.mLastMod);
	return status;
}

static void PopulateExtendedInfo(OpenFileInfo &info, const HdfsPathStatus &status) {
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	auto &options = info.extended_info->options;
	options.emplace("type", Value(status.file_type == FileType::FILE_TYPE_DIR ? "directory" : "file"));
	options.emplace("file_size", Value::BIGINT(status.file_size));
	options.emplace("last_modified", Value::TIMESTAMP(status.last_modified));
}

// Attempt to recover file metadata from the extended_info that DuckDB's multi-file
// reader pipeline carries through from Glob/ListFilesExtended.  When present, this
// avoids a round-trip hdfsGetPathInfo RPC for each file opened after a glob.
static unique_ptr<HdfsPathStatus> TryExtractCachedStatus(const OpenFileInfo &info) {
	if (!info.extended_info) {
		return nullptr;
	}
	auto &options = info.extended_info->options;
	auto type_it = options.find("type");
	auto size_it = options.find("file_size");
	auto time_it = options.find("last_modified");
	if (type_it == options.end() || size_it == options.end() || time_it == options.end()) {
		return nullptr;
	}
	auto status = make_uniq<HdfsPathStatus>();
	auto type_str = StringValue::Get(type_it->second);
	status->file_type = type_str == "directory" ? FileType::FILE_TYPE_DIR : FileType::FILE_TYPE_REGULAR;
	status->file_size = BigIntValue::Get(size_it->second);
	status->last_modified = TimestampValue::Get(time_it->second);
	return status;
}

static hdfs_duckdb::HdfsResolvedConfig ResolveRuntimeConfig(const string &uri, optional_ptr<FileOpener> opener) {
	auto config = hdfs_duckdb::ResolveHdfsConfig(opener);
	if (hdfs_duckdb::UriUsesDefaultFs(uri) && config.default_fs.empty()) {
		config.default_fs = hdfs_duckdb::ResolveRuntimeDefaultFs(config);
	}
	return config;
}

static unique_ptr<HdfsPathStatus> TryGetPathStatus(hdfs_duckdb::HdfsClient &client,
                                                   const hdfs_duckdb::HdfsResolvedUri &uri) {
	errno = 0;
	auto info = client.api->hdfsGetPathInfo(client.fs, uri.path.c_str());
	if (!info) {
		if (errno == ENOENT) {
			return nullptr;
		}
		ThrowHdfsError("GetPathInfo", uri.original_uri);
	}

	auto status = make_uniq<HdfsPathStatus>(ToPathStatus(*info));
	client.api->hdfsFreeFileInfo(info, 1);
	return status;
}

static vector<HdfsPathStatus> ListDirectoryStatuses(hdfs_duckdb::HdfsClient &client,
                                                    const hdfs_duckdb::HdfsResolvedUri &uri) {
	int entry_count = 0;
	errno = 0;
	auto infos = client.api->hdfsListDirectory(client.fs, uri.path.c_str(), &entry_count);
	if (!infos) {
		if (errno == 0) {
			return {};
		}
		ThrowHdfsError("ListDirectory", uri.original_uri);
	}

	vector<HdfsPathStatus> result;
	result.reserve(NumericCast<idx_t>(entry_count));
	for (int i = 0; i < entry_count; i++) {
		result.push_back(ToPathStatus(infos[i]));
	}
	client.api->hdfsFreeFileInfo(infos, entry_count);
	sort(result.begin(), result.end(), [](const HdfsPathStatus &lhs, const HdfsPathStatus &rhs) {
		return lhs.normalized_path < rhs.normalized_path;
	});
	return result;
}

static vector<string> SplitHdfsPath(const string &normalized_path) {
	vector<string> segments;
	if (normalized_path == "/") {
		return segments;
	}

	idx_t start = 1;
	while (start < normalized_path.size()) {
		auto end = normalized_path.find('/', start);
		if (end == string::npos) {
			end = normalized_path.size();
		}
		auto segment = normalized_path.substr(start, end - start);
		if (!segment.empty()) {
			segments.push_back(segment);
		}
		start = end + 1;
	}
	return segments;
}

static bool HasMultipleCrawl(const vector<string> &segments) {
	return std::count(segments.begin(), segments.end(), "**") > 1;
}

// Query-local cache for directory listings within a single Glob() call.  Stack-
// allocated in Glob() and destroyed when that call returns, so there is no
// cross-query or cross-thread sharing.  Eliminates redundant hdfsListDirectory
// RPCs for paths visited more than once, which is common with '**' patterns.
struct GlobMetadataCache {
	unordered_map<string, vector<HdfsPathStatus>> listings;

	const vector<HdfsPathStatus> &ListDirectory(hdfs_duckdb::HdfsClient &client,
	                                            const hdfs_duckdb::HdfsResolvedUri &uri) {
		auto it = listings.find(uri.path);
		if (it != listings.end()) {
			return it->second;
		}
		auto result = ListDirectoryStatuses(client, uri);
		auto [ins_it, ins_ok] = listings.emplace(uri.path, std::move(result));
		(void)ins_ok;
		return ins_it->second;
	}
};

static void RecursiveGlobDirectories(hdfs_duckdb::HdfsClient &client, const hdfs_duckdb::HdfsResolvedUri &base_uri,
                                     const string &path, vector<HdfsPathStatus> &result, bool match_directory,
                                     GlobMetadataCache &cache) {
	hdfs_duckdb::HdfsResolvedUri directory_uri = base_uri;
	directory_uri.path = path;
	for (auto &entry : cache.ListDirectory(client, directory_uri)) {
		auto is_directory = entry.file_type == FileType::FILE_TYPE_DIR;
		if (is_directory == match_directory) {
			result.push_back(entry);
		}
		if (is_directory) {
			RecursiveGlobDirectories(client, base_uri, entry.normalized_path, result, match_directory, cache);
		}
	}
}

static OpenFileInfo ToGlobResult(const hdfs_duckdb::HdfsResolvedUri &uri, const HdfsPathStatus &status) {
	OpenFileInfo info(hdfs_duckdb::BuildDuckdbUri(uri, status.normalized_path));
	PopulateExtendedInfo(info, status);
	return info;
}

} // namespace

string HdfsFileSystem::GetName() const {
	return hdfs_duckdb::FILESYSTEM_NAME;
}

bool HdfsFileSystem::CanHandleFile(const string &fpath) {
	return hdfs_duckdb::HasHdfsScheme(fpath);
}

string HdfsFileSystem::PathSeparator(const string &) {
	return "/";
}

void HdfsFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hdfs_file = handle.Cast<HdfsFileHandle>();
	const bool tracing = trace_writer_.IsEnabled();
	auto trace_start = tracing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	auto remaining = NumericCast<idx_t>(nr_bytes);
	auto offset = location;
	auto ptr = reinterpret_cast<data_ptr_t>(buffer);

	while (remaining > 0) {
		auto chunk = MinValue<idx_t>(remaining, MAX_HDFS_IO_SIZE);
		errno = 0;
		auto rc = hdfs_file.client->api->hdfsPreadFully(hdfs_file.client->fs, hdfs_file.file,
		                                                NumericCast<hdfs_duckdb::tOffset>(offset), ptr,
		                                                NumericCast<hdfs_duckdb::tSize>(chunk));
		if (rc != 0) {
			if (tracing && trace_writer_.IsEnabled()) {
				auto elapsed_ms =
				    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
				auto error = errno == 0 ? "no additional error detail from libhdfs" : string(strerror(errno));
				trace_writer_.Trace(
				    "read", hdfs_file.hdfs_path, elapsed_ms, false, error.c_str(),
				    {{"size", nr_bytes}, {"bytes_read", 0}, {"location", NumericCast<int64_t>(location)}});
			}
			ThrowHdfsError("Read", handle.GetPath());
		}
		offset += chunk;
		ptr += chunk;
		remaining -= chunk;
	}
	if (tracing && trace_writer_.IsEnabled()) {
		auto elapsed_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
		trace_writer_.Trace(
		    "read", hdfs_file.hdfs_path, elapsed_ms, true, nullptr,
		    {{"size", nr_bytes}, {"bytes_read", nr_bytes}, {"location", NumericCast<int64_t>(location)}});
	}
}

int64_t HdfsFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hdfs_file = handle.Cast<HdfsFileHandle>();
	const bool tracing = trace_writer_.IsEnabled();
	auto trace_start = tracing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	auto remaining = NumericCast<idx_t>(nr_bytes);
	auto ptr = reinterpret_cast<data_ptr_t>(buffer);
	int64_t total_read = 0;

	while (remaining > 0) {
		auto chunk = MinValue<idx_t>(remaining, MAX_HDFS_IO_SIZE);
		errno = 0;
		auto bytes_read = hdfs_file.client->api->hdfsPread(hdfs_file.client->fs, hdfs_file.file,
		                                                   NumericCast<hdfs_duckdb::tOffset>(hdfs_file.position), ptr,
		                                                   NumericCast<hdfs_duckdb::tSize>(chunk));
		if (bytes_read < 0) {
			if (tracing && trace_writer_.IsEnabled()) {
				auto elapsed_ms =
				    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
				auto error = errno == 0 ? "no additional error detail from libhdfs" : string(strerror(errno));
				trace_writer_.Trace("read", hdfs_file.hdfs_path, elapsed_ms, false, error.c_str(),
				                    {{"size", nr_bytes}, {"bytes_read", total_read}});
			}
			ThrowHdfsError("Read", handle.GetPath());
		}
		if (bytes_read == 0) {
			break;
		}
		hdfs_file.position += NumericCast<idx_t>(bytes_read);
		total_read += bytes_read;
		ptr += bytes_read;
		remaining -= NumericCast<idx_t>(bytes_read);
		if (NumericCast<idx_t>(bytes_read) < chunk) {
			break;
		}
	}
	if (tracing && trace_writer_.IsEnabled()) {
		auto elapsed_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
		trace_writer_.Trace("read", hdfs_file.hdfs_path, elapsed_ms, true, nullptr,
		                    {{"size", nr_bytes}, {"bytes_read", total_read}});
	}
	return total_read;
}

int64_t HdfsFileSystem::GetFileSize(FileHandle &handle) {
	return handle.Cast<HdfsFileHandle>().file_size;
}

timestamp_t HdfsFileSystem::GetLastModifiedTime(FileHandle &handle) {
	return handle.Cast<HdfsFileHandle>().last_modified;
}

FileType HdfsFileSystem::GetFileType(FileHandle &handle) {
	return handle.Cast<HdfsFileHandle>().file_type;
}

void HdfsFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &hdfs_file = handle.Cast<HdfsFileHandle>();
	const bool tracing = trace_writer_.IsEnabled();
	auto trace_start = tracing ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};
	hdfs_file.position = location;
	if (tracing && trace_writer_.IsEnabled()) {
		auto elapsed_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
		trace_writer_.Trace("seek", hdfs_file.hdfs_path, elapsed_ms, true, nullptr,
		                    {{"location", NumericCast<int64_t>(location)}});
	}
}

idx_t HdfsFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<HdfsFileHandle>().position;
}

[[noreturn]] void HdfsFileSystem::ThrowReadOnly(const string &operation, const string &path,
                                                optional_ptr<FileOpener> opener) const {
	auto config = ResolveRuntimeConfig(path, opener);
	(void)hdfs_duckdb::ResolveUri(path, config);
	throw NotImplementedException("%s: %s is not available; %s", GetName(), operation,
	                              hdfs_duckdb::READ_ONLY_BACKEND_MESSAGE);
}

[[noreturn]] void HdfsFileSystem::ThrowReadOnly(const string &operation, const string &source, const string &target,
                                                optional_ptr<FileOpener> opener) const {
	auto config = ResolveRuntimeConfig(source, opener);
	(void)hdfs_duckdb::ResolveUri(source, config);
	(void)hdfs_duckdb::ResolveUri(target, config);
	throw NotImplementedException("%s: %s is not available; %s", GetName(), operation,
	                              hdfs_duckdb::READ_ONLY_BACKEND_MESSAGE);
}

unique_ptr<FileHandle> HdfsFileSystem::OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
                                                        optional_ptr<FileOpener> opener) {
	if (!flags.OpenForReading() || flags.OpenForWriting() || flags.CreateFileIfNotExists() ||
	    flags.OpenForAppending()) {
		ThrowReadOnly("OpenFile(write)", path.path, opener);
	}

	// Resolve trace config alongside HDFS config so the writer is up-to-date before we time.
	auto trace_cfg = hdfs_duckdb::ResolveTraceConfig(opener);
	hdfs_duckdb::ApplyTraceConfig(trace_writer_, trace_cfg);
	auto trace_start = trace_cfg.enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};

	auto config = ResolveRuntimeConfig(path.path, opener);
	auto resolved = hdfs_duckdb::ResolveUri(path.path, config);
	auto client = hdfs_duckdb::GetHdfsClient(resolved, config);
	// Prefer pre-populated metadata from extended_info (set by Glob/ListFilesExtended)
	// before falling back to a live hdfsGetPathInfo RPC.
	auto status = TryExtractCachedStatus(path);
	if (!status) {
		status = TryGetPathStatus(*client, resolved);
	}
	if (!status) {
		if (flags.ReturnNullIfNotExists()) {
			return nullptr;
		}
		if (trace_writer_.IsEnabled()) {
			auto elapsed_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
			trace_writer_.Trace("open", resolved.path, elapsed_ms, false, "file does not exist", {}, {{"mode", "rb"}});
		}
		throw IOException("HdfsFileSystem: file \"%s\" does not exist", path.path);
	}
	if (status->file_type != FileType::FILE_TYPE_REGULAR) {
		if (trace_writer_.IsEnabled()) {
			auto elapsed_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
			trace_writer_.Trace("open", resolved.path, elapsed_ms, false, "not a regular file", {}, {{"mode", "rb"}});
		}
		throw IOException("HdfsFileSystem: \"%s\" is not a regular file", path.path);
	}

	errno = 0;
	auto file = client->api->hdfsOpenFile(client->fs, resolved.path.c_str(), O_RDONLY, 0, 0, 0);
	if (!file) {
		if (flags.ReturnNullIfNotExists() && errno == ENOENT) {
			return nullptr;
		}
		if (trace_writer_.IsEnabled()) {
			auto elapsed_ms =
			    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
			auto error = errno == 0 ? "no additional error detail from libhdfs" : string(strerror(errno));
			trace_writer_.Trace("open", resolved.path, elapsed_ms, false, error.c_str(), {}, {{"mode", "rb"}});
		}
		ThrowHdfsError("OpenFile", path.path);
	}

	if (trace_writer_.IsEnabled()) {
		auto elapsed_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
		trace_writer_.Trace("open", resolved.path, elapsed_ms, true, nullptr, {{"file_size", status->file_size}},
		                    {{"mode", "rb"}});
	}

	return make_uniq<HdfsFileHandle>(*this, path.path, flags, std::move(client), file, resolved.path, status->file_size,
	                                 status->last_modified, status->file_type, &trace_writer_);
}

bool HdfsFileSystem::ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
                                       optional_ptr<FileOpener> opener) {
	auto trace_cfg = hdfs_duckdb::ResolveTraceConfig(opener);
	hdfs_duckdb::ApplyTraceConfig(trace_writer_, trace_cfg);
	auto trace_start = trace_cfg.enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};

	auto config = ResolveRuntimeConfig(directory, opener);
	auto resolved = hdfs_duckdb::ResolveUri(directory, config);
	auto client = hdfs_duckdb::GetHdfsClient(resolved, config);
	auto status = TryGetPathStatus(*client, resolved);
	if (!status || status->file_type != FileType::FILE_TYPE_DIR) {
		return false;
	}

	auto statuses = ListDirectoryStatuses(*client, resolved);
	for (auto &entry : statuses) {
		OpenFileInfo info(ExtractHdfsBaseName(entry.normalized_path));
		PopulateExtendedInfo(info, entry);
		callback(info);
	}
	if (trace_writer_.IsEnabled()) {
		auto elapsed_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
		trace_writer_.Trace("ls", resolved.path, elapsed_ms, true, nullptr,
		                    {{"entry_count", static_cast<int64_t>(statuses.size())}});
	}
	return true;
}

vector<OpenFileInfo> HdfsFileSystem::Glob(const string &path, FileOpener *opener) {
	auto trace_cfg = hdfs_duckdb::ResolveTraceConfig(opener);
	hdfs_duckdb::ApplyTraceConfig(trace_writer_, trace_cfg);
	auto trace_start = trace_cfg.enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};

	// Wrap the core logic in a lambda so we have a single trace point for all exit paths.
	auto glob_impl = [&]() -> vector<OpenFileInfo> {
		auto config = ResolveRuntimeConfig(path, opener);
		auto resolved = hdfs_duckdb::ResolveUri(path, config);
		auto client = hdfs_duckdb::GetHdfsClient(resolved, config);
		GlobMetadataCache cache;

		if (!FileSystem::HasGlob(resolved.path)) {
			auto status = TryGetPathStatus(*client, resolved);
			if (!status || status->file_type != FileType::FILE_TYPE_REGULAR) {
				return {};
			}
			return {ToGlobResult(resolved, *status)};
		}

		auto segments = SplitHdfsPath(resolved.path);
		if (HasMultipleCrawl(segments)) {
			throw IOException("HdfsFileSystem: cannot use multiple '**' in one HDFS glob path");
		}

		HdfsPathStatus root_status;
		root_status.normalized_path = "/";
		root_status.file_type = FileType::FILE_TYPE_DIR;

		vector<HdfsPathStatus> previous_directories {root_status};
		for (idx_t i = 0; i < segments.size(); i++) {
			auto &segment = segments[i];
			auto is_last = i + 1 == segments.size();
			auto has_glob = FileSystem::HasGlob(segment);
			vector<HdfsPathStatus> next;

			if (!has_glob) {
				for (auto &previous_directory : previous_directories) {
					auto candidate_path = JoinHdfsPath(previous_directory.normalized_path, segment);
					auto candidate_uri = resolved;
					candidate_uri.path = candidate_path;
					candidate_uri.original_uri = hdfs_duckdb::BuildDuckdbUri(resolved, candidate_path);
					auto candidate_status = TryGetPathStatus(*client, candidate_uri);
					if (!candidate_status) {
						continue;
					}
					if (is_last ? candidate_status->file_type == FileType::FILE_TYPE_REGULAR
					            : candidate_status->file_type == FileType::FILE_TYPE_DIR) {
						next.push_back(std::move(*candidate_status));
					}
				}
			} else if (segment == "**") {
				if (!is_last) {
					next = previous_directories;
				}
				for (auto &previous_directory : previous_directories) {
					RecursiveGlobDirectories(*client, resolved, previous_directory.normalized_path, next, !is_last,
					                         cache);
				}
			} else {
				for (auto &previous_directory : previous_directories) {
					auto directory_uri = resolved;
					directory_uri.path = previous_directory.normalized_path;
					directory_uri.original_uri =
					    hdfs_duckdb::BuildDuckdbUri(resolved, previous_directory.normalized_path);
					for (auto &entry : cache.ListDirectory(*client, directory_uri)) {
						auto is_directory = entry.file_type == FileType::FILE_TYPE_DIR;
						if (is_directory != !is_last) {
							continue;
						}
						auto name = ExtractHdfsBaseName(entry.normalized_path);
						if (::duckdb::Glob(name.c_str(), name.size(), segment.c_str(), segment.size())) {
							next.push_back(std::move(entry));
						}
					}
				}
			}

			if (next.empty()) {
				return {};
			}
			if (is_last) {
				vector<OpenFileInfo> result;
				result.reserve(next.size());
				for (auto &entry : next) {
					result.push_back(ToGlobResult(resolved, entry));
				}
				sort(result.begin(), result.end(),
				     [](const OpenFileInfo &lhs, const OpenFileInfo &rhs) { return lhs.path < rhs.path; });
				return result;
			}
			previous_directories = std::move(next);
		}
		return {};
	};

	auto result = glob_impl();
	if (trace_writer_.IsEnabled()) {
		auto elapsed_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
		trace_writer_.Trace("glob", path, elapsed_ms, true, nullptr,
		                    {{"entry_count", static_cast<int64_t>(result.size())}});
	}
	return result;
}

bool HdfsFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto trace_cfg = hdfs_duckdb::ResolveTraceConfig(opener);
	hdfs_duckdb::ApplyTraceConfig(trace_writer_, trace_cfg);
	auto trace_start = trace_cfg.enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point {};

	auto config = ResolveRuntimeConfig(filename, opener);
	auto resolved = hdfs_duckdb::ResolveUri(filename, config);
	auto client = hdfs_duckdb::GetHdfsClient(resolved, config);
	auto status = TryGetPathStatus(*client, resolved);
	bool result = status && status->file_type == FileType::FILE_TYPE_REGULAR;
	if (trace_writer_.IsEnabled()) {
		auto elapsed_ms =
		    std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - trace_start).count();
		trace_writer_.Trace("exists", resolved.path, elapsed_ms, true, nullptr,
		                    {{"result", static_cast<int64_t>(result ? 1 : 0)}});
	}
	return result;
}

bool HdfsFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	auto config = ResolveRuntimeConfig(directory, opener);
	auto resolved = hdfs_duckdb::ResolveUri(directory, config);
	auto client = hdfs_duckdb::GetHdfsClient(resolved, config);
	auto status = TryGetPathStatus(*client, resolved);
	return status && status->file_type == FileType::FILE_TYPE_DIR;
}

void HdfsFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	ThrowReadOnly("CreateDirectory", directory, opener);
}

void HdfsFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	ThrowReadOnly("RemoveDirectory", directory, opener);
}

void HdfsFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	ThrowReadOnly("MoveFile", source, target, opener);
}

void HdfsFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	ThrowReadOnly("RemoveFile", filename, opener);
}

} // namespace duckdb
