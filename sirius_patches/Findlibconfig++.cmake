# Find libconfig++ using pkg-config
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(LIBCONFIG++ QUIET libconfig++)
endif()

if(NOT LIBCONFIG++_FOUND)
  find_path(LIBCONFIG++_INCLUDE_DIRS libconfig.h++ PATH_SUFFIXES include)
  find_library(LIBCONFIG++_LIBRARIES NAMES config++)
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(libconfig++
    REQUIRED_VARS LIBCONFIG++_LIBRARIES LIBCONFIG++_INCLUDE_DIRS)
endif()

if(LIBCONFIG++_FOUND AND NOT TARGET libconfig++::libconfig++)
  add_library(libconfig++::libconfig++ INTERFACE IMPORTED)
  set_target_properties(libconfig++::libconfig++ PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${LIBCONFIG++_INCLUDE_DIRS}"
    INTERFACE_LINK_LIBRARIES "${LIBCONFIG++_LIBRARIES}")
endif()
