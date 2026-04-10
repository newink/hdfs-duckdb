cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME hdfs_duckdb)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(
  EXTENSION_SOURCES
  src/hdfs_config.cpp
  src/hdfs_duckdb_extension.cpp
  src/hdfs_client_registry.cpp
  src/hdfs_filesystem.cpp
  src/hdfs_functions.cpp
  src/hdfs_path.cpp
  src/libhdfs_loader.cpp
  src/hdfs_trace.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(ENABLE_UNITTEST_CPP_TESTS)
  add_library_unity(
    test_hdfs_duckdb
    OBJECT
    test/unittest/test_hdfs_config.cpp
    test/unittest/test_libhdfs_loader.cpp
    test/unittest/test_hdfs_path.cpp
    test/unittest/test_hdfs_trace.cpp)
  target_include_directories(
    test_hdfs_duckdb
    PRIVATE ${CMAKE_SOURCE_DIR}/test/include ${CMAKE_SOURCE_DIR}/third_party/catch)
  set(
    ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_hdfs_duckdb>
    PARENT_SCOPE)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
