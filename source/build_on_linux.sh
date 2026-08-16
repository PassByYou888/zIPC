#!/bin/bash
# ============================================================
# z_ipc automated build script for Linux
# Boost is built as static library, z_ipc as shared library
# Usage: chmod +x build_on_linux.sh && ./build_on_linux.sh
# ============================================================

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
BOOST_TARBALL="boost_1_83_0.tar.gz"
BOOST_SRC="$PROJECT_DIR/boost_1_83_0"
BOOST_LIB="$BOOST_SRC/stage/lib"
BUILD_DIR="$PROJECT_DIR/build"

echo "=== z_ipc build (Linux) ==="
echo "Project root: $PROJECT_DIR"

# 1. Extract Boost if needed
if [ ! -d "$BOOST_SRC" ]; then
    echo "Extracting $BOOST_TARBALL ..."
    tar -xzf "$BOOST_TARBALL" -C "$PROJECT_DIR"
fi

# 2. Build Boost.Date_time as static library
if [ ! -f "$BOOST_LIB/libboost_date_time.a" ]; then
    echo "Building Boost.Date_time (static) ..."
    cd "$BOOST_SRC"
    ./bootstrap.sh --with-libraries=date_time
    ./b2 -j$(nproc) stage
    cd "$PROJECT_DIR"
else
    echo "Boost.Date_time already built, skipping."
fi

# 3. Generate a Linux-specific CMakeLists.txt (embedded)
echo "Generating CMakeLists.txt ..."
cat > "$PROJECT_DIR/CMakeLists.txt" << 'EOF'
cmake_minimum_required(VERSION 3.15)
project(z_ipc VERSION 1.0.0 LANGUAGES CXX)

option(ZIPC_BUILD_SHARED "Build shared library" ON)
option(ZIPC_ENABLE_LOGGING "Enable logging" OFF)
option(ZIPC_BUILD_TESTS "Build tests" OFF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
    add_compile_options(/MP)
endif()

if(APPLE)
    add_definitions(-DBOOST_INTERPROCESS_MANAGED_OPEN_OR_CREATE_INITIALIZE_MAX_TRIES=1)
endif()

# Use local Boost paths (static library)
if(NOT BOOST_INCLUDE_DIR)
    set(BOOST_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/boost_1_83_0")
endif()
if(NOT BOOST_DATE_TIME_LIB)
    set(BOOST_DATE_TIME_LIB "${CMAKE_CURRENT_SOURCE_DIR}/boost_1_83_0/stage/lib/libboost_date_time.a")
endif()

if(NOT EXISTS ${BOOST_INCLUDE_DIR}/boost/interprocess/ipc/message_queue.hpp)
    message(FATAL_ERROR "Boost header not found. Check BOOST_INCLUDE_DIR")
endif()
if(NOT EXISTS ${BOOST_DATE_TIME_LIB})
    message(FATAL_ERROR "Boost static library not found. Check BOOST_DATE_TIME_LIB")
endif()

set(SOURCES
    z_ipc_api.cpp
    z_ipc_client_impl.cpp
    z_ipc_server_impl.cpp
)

if(ZIPC_BUILD_SHARED)
    add_library(z_ipc SHARED ${SOURCES})
else()
    add_library(z_ipc STATIC ${SOURCES})
endif()

target_link_libraries(z_ipc PRIVATE
    ${BOOST_DATE_TIME_LIB}
    pthread rt
)

target_include_directories(z_ipc PRIVATE ${BOOST_INCLUDE_DIR})
target_compile_features(z_ipc PRIVATE cxx_std_17)

target_compile_definitions(z_ipc PRIVATE
    $<$<CONFIG:Debug>:ZIPC_ENABLE_LOG>
)
if(ZIPC_ENABLE_LOGGING)
    target_compile_definitions(z_ipc PRIVATE ZIPC_ENABLE_LOG)
endif()

if(MSVC)
    set_target_properties(z_ipc PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif()

include(GNUInstallDirs)
install(TARGETS z_ipc
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(FILES
    z_ipc_api.h
    z_ipc_client_impl.h
    z_ipc_server_impl.h
    z_ipc_md5.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/z_ipc
)

message(STATUS "z_ipc configuration:")
message(STATUS "  Build shared: ${ZIPC_BUILD_SHARED}")
message(STATUS "  Logging:      ${ZIPC_ENABLE_LOGGING}")
message(STATUS "  Boost include: ${BOOST_INCLUDE_DIR}")
message(STATUS "  Boost date_time lib: ${BOOST_DATE_TIME_LIB}")
EOF

# 4. Configure and build
echo "Configuring with CMake ..."
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
cmake .. -DZIPC_BUILD_SHARED=ON \
         -DZIPC_ENABLE_LOGGING=OFF \
         -DCMAKE_BUILD_TYPE=Release \
         -DBOOST_INCLUDE_DIR="$BOOST_SRC" \
         -DBOOST_DATE_TIME_LIB="$BOOST_LIB/libboost_date_time.a"		 

echo "Compiling ..."
make -j$(nproc)

echo "=== Build completed ==="
ls -l "$BUILD_DIR/libz_ipc"*
echo "Library: $BUILD_DIR/libz_ipc.so"
echo "Headers: $PROJECT_DIR"