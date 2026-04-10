#include "libhdfs.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"

#ifndef _WIN32
#include <dlfcn.h>
#endif

#include <mutex>

namespace duckdb {
namespace hdfs_duckdb {

LibHdfsApi::~LibHdfsApi() {
#ifndef _WIN32
	if (library_handle) {
		dlclose(library_handle);
	}
#endif
}

static void AddCandidate(vector<string> &candidates, const string &candidate) {
	if (candidate.empty()) {
		return;
	}
	if (std::find(candidates.begin(), candidates.end(), candidate) == candidates.end()) {
		candidates.push_back(candidate);
	}
}

vector<string> GetLibHdfsCandidatePaths(const string &arrow_libhdfs_dir, const string &hadoop_home) {
	vector<string> candidates;

	auto add_dir_candidates = [&](const string &dir) {
		if (dir.empty()) {
			return;
		}
		AddCandidate(candidates, dir + "/libhdfs.so");
		AddCandidate(candidates, dir + "/libhdfs.so.0.0.0");
		AddCandidate(candidates, dir + "/libhdfs.dylib");
	};

	add_dir_candidates(arrow_libhdfs_dir);
	if (!hadoop_home.empty()) {
		add_dir_candidates(hadoop_home + "/lib/native");
	}

	AddCandidate(candidates, "libhdfs.so");
	AddCandidate(candidates, "libhdfs.so.0.0.0");
	AddCandidate(candidates, "libhdfs.dylib");
	return candidates;
}

#ifndef _WIN32
template <class FUNCTION>
static FUNCTION LoadSymbol(void *handle, const char *name, const string &loaded_path) {
	dlerror();
	auto symbol = dlsym(handle, name);
	auto error = dlerror();
	if (error || !symbol) {
		throw IOException("Failed to load symbol \"%s\" from libhdfs at \"%s\": %s", name, loaded_path,
		                  error ? error : "symbol not found");
	}
	return reinterpret_cast<FUNCTION>(symbol);
}
#endif

static shared_ptr<LibHdfsApi> LoadLibHdfs() {
#ifdef _WIN32
	throw NotImplementedException("HdfsFileSystem: libhdfs runtime loading is not implemented on Windows");
#else
	auto arrow_libhdfs_dir = FileSystem::GetEnvVariable("ARROW_LIBHDFS_DIR");
	auto hadoop_home = FileSystem::GetEnvVariable("HADOOP_HOME");
	auto candidates = GetLibHdfsCandidatePaths(arrow_libhdfs_dir, hadoop_home);

	vector<string> failures;
	for (auto &candidate : candidates) {
		dlerror();
		auto handle = dlopen(candidate.c_str(), RTLD_NOW | RTLD_LOCAL);
		if (!handle) {
			auto error = dlerror();
			failures.push_back(candidate + ": " + string(error ? error : "dlopen failed"));
			continue;
		}

		auto api = make_shared_ptr<LibHdfsApi>(handle, candidate);
		try {
			api->hdfsNewBuilder = LoadSymbol<LibHdfsApi::NewBuilderFunction>(handle, "hdfsNewBuilder", candidate);
			api->hdfsBuilderConnect =
			    LoadSymbol<LibHdfsApi::BuilderConnectFunction>(handle, "hdfsBuilderConnect", candidate);
			api->hdfsBuilderSetNameNode =
			    LoadSymbol<LibHdfsApi::BuilderSetNameNodeFunction>(handle, "hdfsBuilderSetNameNode", candidate);
			api->hdfsBuilderSetUserName =
			    LoadSymbol<LibHdfsApi::BuilderSetUserNameFunction>(handle, "hdfsBuilderSetUserName", candidate);
			api->hdfsBuilderSetKerbTicketCachePath = LoadSymbol<LibHdfsApi::BuilderSetKerbTicketCachePathFunction>(
			    handle, "hdfsBuilderSetKerbTicketCachePath", candidate);
			api->hdfsBuilderSetForceNewInstance = LoadSymbol<LibHdfsApi::BuilderSetForceNewInstanceFunction>(
			    handle, "hdfsBuilderSetForceNewInstance", candidate);
			api->hdfsBuilderConfSetStr =
			    LoadSymbol<LibHdfsApi::BuilderConfSetStrFunction>(handle, "hdfsBuilderConfSetStr", candidate);
			api->hdfsFreeBuilder = LoadSymbol<LibHdfsApi::FreeBuilderFunction>(handle, "hdfsFreeBuilder", candidate);
			api->hdfsConfGetStr = LoadSymbol<LibHdfsApi::ConfGetStrFunction>(handle, "hdfsConfGetStr", candidate);
			api->hdfsConfStrFree = LoadSymbol<LibHdfsApi::ConfStrFreeFunction>(handle, "hdfsConfStrFree", candidate);
			api->hdfsDisconnect = LoadSymbol<LibHdfsApi::DisconnectFunction>(handle, "hdfsDisconnect", candidate);
			api->hdfsOpenFile = LoadSymbol<LibHdfsApi::OpenFileFunction>(handle, "hdfsOpenFile", candidate);
			api->hdfsCloseFile = LoadSymbol<LibHdfsApi::CloseFileFunction>(handle, "hdfsCloseFile", candidate);
			api->hdfsPread = LoadSymbol<LibHdfsApi::PreadFunction>(handle, "hdfsPread", candidate);
			api->hdfsPreadFully = LoadSymbol<LibHdfsApi::PreadFullyFunction>(handle, "hdfsPreadFully", candidate);
			api->hdfsGetPathInfo = LoadSymbol<LibHdfsApi::GetPathInfoFunction>(handle, "hdfsGetPathInfo", candidate);
			api->hdfsListDirectory =
			    LoadSymbol<LibHdfsApi::ListDirectoryFunction>(handle, "hdfsListDirectory", candidate);
			api->hdfsFreeFileInfo = LoadSymbol<LibHdfsApi::FreeFileInfoFunction>(handle, "hdfsFreeFileInfo", candidate);
			return api;
		} catch (...) {
			dlclose(handle);
			throw;
		}
	}

	auto searched = StringUtil::Join(candidates, ", ");
	auto failure_detail = failures.empty() ? "no candidates were generated" : StringUtil::Join(failures, "; ");
	throw IOException(
	    "HdfsFileSystem: unable to load libhdfs. Tried [%s]. Set ARROW_LIBHDFS_DIR or HADOOP_HOME so libhdfs is "
	    "discoverable. Loader errors: %s",
	    searched, failure_detail);
#endif
}

shared_ptr<LibHdfsApi> GetLibHdfsApi() {
	static std::mutex load_lock;
	static shared_ptr<LibHdfsApi> api;

	std::lock_guard<std::mutex> guard(load_lock);
	if (!api) {
		api = LoadLibHdfs();
	}
	return api;
}

} // namespace hdfs_duckdb
} // namespace duckdb
