#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

std::string HdfsTemplateStatusMessage(const std::string &phase);
void RegisterHdfsFunctions(ExtensionLoader &loader);

} // namespace duckdb
