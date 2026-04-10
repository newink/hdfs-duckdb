#pragma once

#include "duckdb/common/file_system.hpp"
#include "hdfs_trace.hpp"

namespace duckdb {

namespace hdfs_duckdb {
struct HdfsResolvedUri;
}

class HdfsFileSystem : public FileSystem {
public:
	string GetName() const override;
	bool CanHandleFile(const string &fpath) override;
	string PathSeparator(const string &path) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;
	bool SupportsOpenFileExtended() const override {
		return true;
	}

	bool ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;
	bool SupportsListFilesExtended() const override {
		return true;
	}

private:
	[[noreturn]] void ThrowReadOnly(const string &operation, const string &path, optional_ptr<FileOpener> opener) const;
	[[noreturn]] void ThrowReadOnly(const string &operation, const string &source, const string &target,
	                                optional_ptr<FileOpener> opener) const;

	// Mutable because some FileSystem virtual methods are const-qualified.
	mutable hdfs_duckdb::HdfsTraceWriter trace_writer_;
};

} // namespace duckdb
