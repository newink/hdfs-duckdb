#include "hdfs_path.hpp"

#include "hdfs_constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace hdfs_duckdb {

static void ThrowMalformedUri(const string &uri, const string &message) {
	throw InvalidInputException("Malformed hdfs URI \"%s\": %s", uri, message);
}

static void ValidateNoQueryOrFragment(const string &value, const string &uri) {
	if (value.find('?') != string::npos || value.find('#') != string::npos) {
		ThrowMalformedUri(uri, "query and fragment components are not supported");
	}
}

static void ValidateAuthority(const string &authority, const string &uri) {
	if (authority.empty()) {
		ThrowMalformedUri(uri, "authority must not be empty");
	}
	if (authority.find('@') != string::npos) {
		ThrowMalformedUri(uri, "user-info is not supported; use hdfs_user instead");
	}
	if (authority.find('\\') != string::npos) {
		ThrowMalformedUri(uri, "authority must use URI separators");
	}
	for (auto ch : authority) {
		if (StringUtil::CharacterIsSpace(ch)) {
			ThrowMalformedUri(uri, "authority must not contain whitespace");
		}
	}
}

bool HasHdfsScheme(const string &path) {
	return StringUtil::StartsWith(StringUtil::Lower(path), URI_SCHEME);
}

bool UriUsesDefaultFs(const string &uri) {
	if (!HasHdfsScheme(uri)) {
		return false;
	}
	auto remainder = uri.substr(strlen(URI_SCHEME));
	return !remainder.empty() && remainder[0] == '/';
}

string NormalizeDefaultFs(const string &default_fs) {
	if (default_fs.empty()) {
		return default_fs;
	}
	ValidateNoQueryOrFragment(default_fs, default_fs);

	auto scheme_end = default_fs.find("://");
	if (scheme_end == string::npos || scheme_end == 0) {
		throw InvalidInputException("Setting %s must be a URI like hdfs://nameservice1 or viewfs://cluster",
		                            SETTING_DEFAULT_FS);
	}

	auto scheme = StringUtil::Lower(default_fs.substr(0, scheme_end));
	if (scheme != "hdfs" && scheme != "viewfs") {
		throw InvalidInputException("Setting %s must use hdfs:// or viewfs://, got \"%s\"", SETTING_DEFAULT_FS,
		                            default_fs);
	}

	auto remainder = default_fs.substr(scheme_end + 3);
	auto slash = remainder.find('/');
	auto authority = slash == string::npos ? remainder : remainder.substr(0, slash);
	ValidateAuthority(authority, default_fs);

	if (slash != string::npos) {
		auto path = remainder.substr(slash);
		if (!path.empty() && path.find_first_not_of('/') != string::npos) {
			throw InvalidInputException("Setting %s must point at the filesystem root, got \"%s\"", SETTING_DEFAULT_FS,
			                            default_fs);
		}
	}

	return scheme + "://" + authority;
}

string NormalizeHdfsPath(const string &path) {
	if (path.empty() || path[0] != '/') {
		throw InvalidInputException("HDFS path \"%s\" must be absolute", path);
	}
	if (path.find('\\') != string::npos) {
		throw InvalidInputException("HDFS path \"%s\" must use '/' separators", path);
	}

	vector<string> segments;
	idx_t start = 1;
	while (start <= path.size()) {
		auto end = path.find('/', start);
		if (end == string::npos) {
			end = path.size();
		}
		auto segment = path.substr(start, end - start);
		if (!segment.empty() && segment != ".") {
			if (segment == "..") {
				if (!segments.empty()) {
					segments.pop_back();
				}
			} else {
				segments.push_back(segment);
			}
		}
		if (end == path.size()) {
			break;
		}
		start = end + 1;
	}

	if (segments.empty()) {
		return "/";
	}
	return "/" + StringUtil::Join(segments, "/");
}

string BuildDuckdbUri(const HdfsResolvedUri &uri, const string &normalized_path) {
	if (normalized_path.empty() || normalized_path[0] != '/') {
		throw InternalException("Expected normalized HDFS path, got \"%s\"", normalized_path);
	}
	if (uri.uses_default_fs) {
		return string(URI_SCHEME) + normalized_path;
	}
	return string(URI_SCHEME) + uri.authority + normalized_path;
}

HdfsResolvedUri ResolveUri(const string &uri, const HdfsResolvedConfig &config) {
	if (!HasHdfsScheme(uri)) {
		ThrowMalformedUri(uri, "expected scheme prefix \"hdfs://\"");
	}

	auto remainder = uri.substr(strlen(URI_SCHEME));
	if (remainder.empty()) {
		ThrowMalformedUri(uri, "expected either hdfs:///absolute/path or hdfs://authority/absolute/path");
	}
	ValidateNoQueryOrFragment(remainder, uri);

	HdfsResolvedUri result;
	result.original_uri = uri;

	if (remainder[0] == '/') {
		if (config.default_fs.empty()) {
			throw InvalidInputException(
			    "hdfs URI \"%s\" requires %s to be set because the URI does not include an authority", uri,
			    SETTING_DEFAULT_FS);
		}
		result.uses_default_fs = true;
		result.path = NormalizeHdfsPath(remainder);
		result.target_filesystem = NormalizeDefaultFs(config.default_fs);
		result.normalized_target_uri = result.target_filesystem + result.path;
		return result;
	}

	auto slash = remainder.find('/');
	result.authority = slash == string::npos ? remainder : remainder.substr(0, slash);
	ValidateAuthority(result.authority, uri);
	result.path = slash == string::npos ? "/" : NormalizeHdfsPath(remainder.substr(slash));
	result.target_filesystem = "hdfs://" + result.authority;
	result.normalized_target_uri = result.target_filesystem + result.path;
	return result;
}

} // namespace hdfs_duckdb
} // namespace duckdb
