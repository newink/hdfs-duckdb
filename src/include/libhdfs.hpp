#pragma once

#include "duckdb/common/common.hpp"

#include <cstdint>
#include <ctime>

namespace duckdb {
namespace hdfs_duckdb {

struct hdfsBuilder;
struct hdfs_internal;
struct hdfsFile_internal;

using hdfsFS = hdfs_internal *;
using hdfsFile = hdfsFile_internal *;
using tSize = int32_t;
using tTime = time_t;
using tOffset = int64_t;
using tPort = uint16_t;

enum class tObjectKind : char { kObjectKindFile = 'F', kObjectKindDirectory = 'D' };

struct hdfsFileInfo {
	tObjectKind mKind;
	char *mName;
	tTime mLastMod;
	tOffset mSize;
	short mReplication;
	tOffset mBlockSize;
	char *mOwner;
	char *mGroup;
	short mPermissions;
	tTime mLastAccess;
};

struct LibHdfsApi {
	explicit LibHdfsApi(void *library_handle_p, string loaded_path_p)
	    : library_handle(library_handle_p), loaded_path(std::move(loaded_path_p)) {
	}
	~LibHdfsApi();

	using NewBuilderFunction = hdfsBuilder *(*)();
	using BuilderConnectFunction = hdfsFS (*)(hdfsBuilder *);
	using BuilderSetNameNodeFunction = void (*)(hdfsBuilder *, const char *);
	using BuilderSetUserNameFunction = void (*)(hdfsBuilder *, const char *);
	using BuilderSetKerbTicketCachePathFunction = void (*)(hdfsBuilder *, const char *);
	using BuilderSetForceNewInstanceFunction = void (*)(hdfsBuilder *);
	using BuilderConfSetStrFunction = int (*)(hdfsBuilder *, const char *, const char *);
	using FreeBuilderFunction = void (*)(hdfsBuilder *);
	using ConfGetStrFunction = int (*)(const char *, char **);
	using ConfStrFreeFunction = void (*)(char *);
	using DisconnectFunction = int (*)(hdfsFS);
	using OpenFileFunction = hdfsFile (*)(hdfsFS, const char *, int, int, short, tSize);
	using CloseFileFunction = int (*)(hdfsFS, hdfsFile);
	using PreadFunction = tSize (*)(hdfsFS, hdfsFile, tOffset, void *, tSize);
	using PreadFullyFunction = int (*)(hdfsFS, hdfsFile, tOffset, void *, tSize);
	using GetPathInfoFunction = hdfsFileInfo *(*)(hdfsFS, const char *);
	using ListDirectoryFunction = hdfsFileInfo *(*)(hdfsFS, const char *, int *);
	using FreeFileInfoFunction = void (*)(hdfsFileInfo *, int);

	void *library_handle = nullptr;
	string loaded_path;

	NewBuilderFunction hdfsNewBuilder = nullptr;
	BuilderConnectFunction hdfsBuilderConnect = nullptr;
	BuilderSetNameNodeFunction hdfsBuilderSetNameNode = nullptr;
	BuilderSetUserNameFunction hdfsBuilderSetUserName = nullptr;
	BuilderSetKerbTicketCachePathFunction hdfsBuilderSetKerbTicketCachePath = nullptr;
	BuilderSetForceNewInstanceFunction hdfsBuilderSetForceNewInstance = nullptr;
	BuilderConfSetStrFunction hdfsBuilderConfSetStr = nullptr;
	FreeBuilderFunction hdfsFreeBuilder = nullptr;
	ConfGetStrFunction hdfsConfGetStr = nullptr;
	ConfStrFreeFunction hdfsConfStrFree = nullptr;
	DisconnectFunction hdfsDisconnect = nullptr;
	OpenFileFunction hdfsOpenFile = nullptr;
	CloseFileFunction hdfsCloseFile = nullptr;
	PreadFunction hdfsPread = nullptr;
	PreadFullyFunction hdfsPreadFully = nullptr;
	GetPathInfoFunction hdfsGetPathInfo = nullptr;
	ListDirectoryFunction hdfsListDirectory = nullptr;
	FreeFileInfoFunction hdfsFreeFileInfo = nullptr;
};

vector<string> GetLibHdfsCandidatePaths(const string &arrow_libhdfs_dir, const string &hadoop_home);
shared_ptr<LibHdfsApi> GetLibHdfsApi();

} // namespace hdfs_duckdb
} // namespace duckdb
