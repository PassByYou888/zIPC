# =============================================================================
# CMake build configuration for z_ipc – cross‑process RPC + notification library
# based on Boost.Interprocess. This file creates a shared library with
# platform‑specific naming and debug suffix, supports parallel compilation,
# and is compatible with Linux, Windows, and macOS.
# =============================================================================

cmake_minimum_required(VERSION 3.15)
project(z_ipc VERSION 1.0.0 LANGUAGES CXX)

# -----------------------------------------------------------------------------
# Options
# -----------------------------------------------------------------------------
option(ZIPC_BUILD_SHARED "Build shared library" ON)
option(ZIPC_ENABLE_LOGGING "Enable internal logging (if ON, log messages are printed)" OFF)
option(ZIPC_BUILD_TESTS "Build test executables" OFF)

# -----------------------------------------------------------------------------
# C++ standard and compiler flags
# -----------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable parallel compilation (MSVC: /MP, other generators rely on external -j)
if(MSVC)
    add_compile_options(/MP)
endif()

# -----------------------------------------------------------------------------
# Platform‑specific definitions (required for Boost.Interprocess on macOS)
# -----------------------------------------------------------------------------
if(APPLE)
    add_definitions(-DBOOST_INTERPROCESS_MANAGED_OPEN_OR_CREATE_INITIALIZE_MAX_TRIES=1)
endif()

# -----------------------------------------------------------------------------
# Find Boost libraries (interprocess and date_time)
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Find Boost libraries (interprocess and date_time)
# -----------------------------------------------------------------------------

# vcpkg directory
if(WIN32 AND NOT DEFINED Boost_DIR)
    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
        set(Boost_DIR "D:/vcpkg/installed/x64-windows-static/share/boost")
    elseif(CMAKE_SIZEOF_VOID_P EQUAL 4)
        set(Boost_DIR "D:/vcpkg/installed/x86-windows-static/share/boost")
    endif()
endif()

find_package(Boost CONFIG REQUIRED COMPONENTS interprocess date_time)

if(NOT Boost_FOUND)
    message(FATAL_ERROR "Boost (interprocess, date_time) not found. Please install Boost.")
endif()

# -----------------------------------------------------------------------------
# Source files
# -----------------------------------------------------------------------------
set(SOURCES
    z_ipc_api.h
    z_ipc_api.cpp
    z_ipc_client_impl.h
    z_ipc_client_impl.cpp
    z_ipc_server_impl.h
    z_ipc_server_impl.cpp
    z_ipc_md5.h
)

# -----------------------------------------------------------------------------
# Create shared library (or static if requested)
# -----------------------------------------------------------------------------
if(ZIPC_BUILD_SHARED)
    add_library(z_ipc SHARED ${SOURCES})
else()
    add_library(z_ipc STATIC ${SOURCES})
endif()

# Set target properties for output naming (Windows)
if(WIN32)
    if(CMAKE_SIZEOF_VOID_P EQUAL 4)
        set_target_properties(z_ipc PROPERTIES OUTPUT_NAME "z_ipc_32")
    elseif(CMAKE_SIZEOF_VOID_P EQUAL 8)
        set_target_properties(z_ipc PROPERTIES OUTPUT_NAME "z_ipc_64")
    endif()
    set_target_properties(z_ipc PROPERTIES DEBUG_POSTFIX "d")
endif()

# Link Boost
target_link_libraries(z_ipc PRIVATE
    Boost::boost
    Boost::interprocess
    Boost::date_time
)

# Compile features
target_compile_features(z_ipc PRIVATE cxx_std_17)

# -----------------------------------------------------------------------------
# Compile definitions
# -----------------------------------------------------------------------------
target_compile_definitions(z_ipc PRIVATE
    $<$<CONFIG:Debug>:ZIPC_ENABLE_LOG>
)

# If ZIPC_ENABLE_LOGGING option is set, enable log even in Release
if(ZIPC_ENABLE_LOGGING)
    target_compile_definitions(z_ipc PRIVATE ZIPC_ENABLE_LOG)
endif()

# -----------------------------------------------------------------------------
# Symbol visibility (Windows exports all symbols by default; Unix uses default)
# -----------------------------------------------------------------------------
if(MSVC)
    set_target_properties(z_ipc PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
else()
    set_target_properties(z_ipc PROPERTIES CXX_VISIBILITY_PRESET default)
endif()

# -----------------------------------------------------------------------------
# Install targets (optional)
# -----------------------------------------------------------------------------
include(GNUInstallDirs)
install(TARGETS z_ipc
    EXPORT z_ipcTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(FILES
    z_ipc_api.h
    z_ipc_client_impl.h
    z_ipc_server_impl.h
    z_ipc_md5.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/z_ipc
)

# Export the package config
install(EXPORT z_ipcTargets
    FILE z_ipcTargets.cmake
    NAMESPACE z_ipc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/z_ipc
)

# -----------------------------------------------------------------------------
# Print summary
# -----------------------------------------------------------------------------
message(STATUS "z_ipc configuration:")
message(STATUS "  Build shared library: ${ZIPC_BUILD_SHARED}")
message(STATUS "  Enable logging:       ${ZIPC_ENABLE_LOGGING}")
message(STATUS "  Build tests:          ${ZIPC_BUILD_TESTS}")
message(STATUS "  C++ standard:         ${CMAKE_CXX_STANDARD}")
message(STATUS "  Install prefix:       ${CMAKE_INSTALL_PREFIX}")

# -----------------------------------------------------------------------------
# Tests (if enabled)
# -----------------------------------------------------------------------------
if(ZIPC_BUILD_TESTS)
    add_executable(test_ipc test_ipc.cpp)   # example, adapt to your tests
    target_link_libraries(test_ipc PRIVATE z_ipc)
endif()