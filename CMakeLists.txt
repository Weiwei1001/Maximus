cmake_minimum_required(VERSION 3.17 FATAL_ERROR)

###########################
#   PROJECT DEFINITION    #
###########################
project(maximus
        DESCRIPTION "Maximus: A Modular Accelerated Query Engine for Data Analytics on Heterogeneous Systems"
        HOMEPAGE_URL "https://gitlab.inf.ethz.ch/PUB-SYSTEMS/eth-dataprocessing/Maximus"
        VERSION 0.2.0
        LANGUAGES CXX C)

SET(MAXIMUS_SO_VERSION 1)
SET(MAXIMUS_VERSION ${PROJECT_VERSION})

set(CMAKE_EXPORT_COMPILE_COMMANDS "YES") # always write compile_commands.json

###########################
#     CMAKE POLICIES      #
###########################
# allow setting the {dependency}_ROOT variables
if(POLICY CMP0074)
    cmake_policy(SET CMP0074 NEW)
endif()

# if available, use the INTERFACE_LINK_LIBRARIES property
if(POLICY CMP0022)
    cmake_policy(SET CMP0022 NEW)
endif()

# https://cmake.org/cmake/help/latest/policy/CMP0135.html
#
# CMP0135 is for solving re-building and re-downloading.
# We don't have a real problem with the OLD behavior for now
# but we use the NEW behavior explicitly to suppress CMP0135
# warnings.
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()

###########################
#      CMAKE INCLUDE      #
###########################
# add local cmake module path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# always write compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS "YES")

###########################
#       BUILD TYPE        #
###########################
set(default_build_type ${CMAKE_BUILD_TYPE})
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(default_build_type "Release")
endif()
set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Profile." FORCE)

###########################
#     MAXIMUS OPTIONS     #
###########################
option(MAXIMUS_WITH_TESTS "Generate the test targets." ON)
# TODO: the examples folder still doesn't exist
option(MAXIMUS_WITH_EXAMPLES "Generate the example targets." OFF)
option(MAXIMUS_WITH_BENCHMARKS "Generate the benchmarks targets." OFF)
option(MAXIMUS_WITH_SANITIZERS "Enable the sanitizers to all the targets." OFF)
option(BUILD_SHARED_LIBS "Build as a shared library." ON)
option(MAXIMUS_WITH_GPU "Whether to enable the GPU (CUDA) support (OFF by default)." OFF)
option(MAXIMUS_WITH_PROFILING "Enable profiling." OFF)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(GCC_ABI_COMPILE_FLAGS "-D_GLIBCXX_USE_CXX11_ABI=0")

set(ARROW_INSTALL_NAME_RPATH OFF)

if (MAXIMUS_WITH_SANITIZERS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

###########################
#     INSTALL PATHS       #
###########################
# preserve rpaths when installing and make the install folder relocatable
# use `CMAKE_SKIP_INSTALL_RPATH` to skip this
# https://spack.readthedocs.io/en/latest/workflows.html#write-the-cmake-build
list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES
    "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}" isSystemDir)

# skip RPATH if MAXIMUS is installed to system directories
if(isSystemDir STREQUAL "-1")
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
    if(APPLE)
        set(basePoint @loader_path)
    else()
        set(basePoint $ORIGIN)
    endif()
    file(RELATIVE_PATH relDir ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_INSTALL_BINDIR}
        ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
    set(CMAKE_INSTALL_RPATH ${basePoint} ${basePoint}/${relDir})
endif()

###########################
#   MAXIMUS DEPENDENCIES  #
###########################
set(MAXIMUS_EXTERNAL_LIBS "")
set(MAXIMUS_EXTERNAL_INCLUDE_DIRS "")

###########
# cxxopts #
###########
if (MAXIMUS_WITH_EXAMPLES OR MAXIMUS_WITH_TESTS)
    add_subdirectory(third_party/cxxopts)
endif()

find_package(Arrow REQUIRED)
find_package(ArrowAcero REQUIRED)
find_package(Parquet REQUIRED)
find_package(Taskflow REQUIRED)

list(APPEND MAXIMUS_EXTERNAL_LIBS Arrow::arrow_shared ArrowAcero::arrow_acero_shared Taskflow::Taskflow Parquet::parquet_shared)
list(APPEND MAXIMUS_EXTERNAL_INCLUDE_DIRS ${ARROW_INCLUDE_DIRS})

message(STATUS "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")

######################
#    GPU-version     #
######################
set(MAXIMUS_WITH_CUDA "OFF")
set(MAXIMUS_WITH_ROCM "OFF")

# currently, libcudf only supports nvidia gpus
if (MAXIMUS_WITH_GPU)
    set(MAXIMUS_WITH_CUDA "ON")
endif()

if (MAXIMUS_WITH_CUDA STREQUAL "ON")
    enable_language(CUDA)
    find_package(CUDF REQUIRED)
    list(APPEND MAXIMUS_EXTERNAL_LIBS cudf::cudf)
endif()

###########################
#       PROFILER          #
###########################
if (MAXIMUS_WITH_PROFILING)
    find_package(caliper REQUIRED)
    list(APPEND MAXIMUS_EXTERNAL_LIBS caliper)
    list(APPEND MAXIMUS_EXTERNAL_INCLUDE_DIRS ${caliper_INCLUDE_DIR})
endif()

###########################
#       THIRD-PARTY       #
###########################
add_subdirectory(third_party)

list(APPEND MAXIMUS_EXTERNAL_LIBS sqlparser)

###########################
#        MAXIMUS          #
###########################
include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

install(TARGETS sqlparser EXPORT maximusTargets)

add_subdirectory(src/maximus)

install(DIRECTORY "${maximus_SOURCE_DIR}/src/maximus"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    FILES_MATCHING
    PATTERN "*.hpp")

write_basic_package_version_file(
    "${maximus_BINARY_DIR}/maximusConfigVersion.cmake"
    VERSION ${maximus_VERSION}
    COMPATIBILITY SameMajorVersion)

configure_file("${maximus_SOURCE_DIR}/cmake/maximus.pc.in"
    "${maximus_BINARY_DIR}/maximus.pc"
    @ONLY)

configure_file("${maximus_SOURCE_DIR}/cmake/maximusConfig.cmake.in"
    "${maximus_BINARY_DIR}/maximusConfig.cmake"
    @ONLY)

write_basic_package_version_file(
    "${maximus_BINARY_DIR}/maximusConfigVersion.cmake"
    VERSION "${maximus_VERSION}"
    COMPATIBILITY SameMajorVersion)

install(FILES "${maximus_BINARY_DIR}/maximusConfig.cmake"
  "${maximus_BINARY_DIR}/maximusConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/maximus")

install(FILES "${maximus_BINARY_DIR}/maximus.pc"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")

###########################
#     MAXIMUS TESTS       #
###########################
if (MAXIMUS_WITH_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

###########################
#     MAXIMUS EXAMPLES    #
###########################
if (MAXIMUS_WITH_EXAMPLES)
    add_subdirectory(examples)
endif()

###########################
#   MAXIMUS MICRO BENCH   #
###########################
add_subdirectory(benchmarks)

