cmake_minimum_required(VERSION 3.16)
project(axel C)

# 选项
option(BUILD_STATIC "Build a static axel binary" ON)
option(ENABLE_SSL   "Enable SSL support (OpenSSL)" ON)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# 默认编译类型
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# 源码位置（根据你的源码调整）
set(AXEL_SRC_DIR "${CMAKE_SOURCE_DIR}/src")

# Collect sources: include typical .c in src/ and subdirs, exclude gettext/po etc.
file(GLOB_RECURSE AXEL_SOURCES
    RELATIVE "${CMAKE_SOURCE_DIR}"
    "${AXEL_SRC_DIR}/*.c"
)

# Remove any test / gettext files heuristically (feel free to tweak)
list(FILTER AXEL_SOURCES EXCLUDE REGEX "po/|doc/|\\.m4|\\.in|gettext" )

# Create target
add_executable(axel ${AXEL_SOURCES})

# Include dirs
target_include_directories(axel PRIVATE "${AXEL_SRC_DIR}")

# Platform/Windows specific settings
if(WIN32 OR CMAKE_SYSTEM_NAME STREQUAL "Windows")
  # Ensure WIN32 macro visible to sources
  target_compile_definitions(axel PRIVATE WIN32)

  # For mingw, make sure winsock headers are available early.
  # Add -include winsock2.h -include ws2tcpip.h when using GCC/Clang on Windows.
  if (CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(axel PRIVATE -include winsock2.h -include ws2tcpip.h)
  endif()

  # Link win sockets and crypto system libs
  target_link_libraries(axel PRIVATE ws2_32)
  target_link_libraries(axel PRIVATE crypt32)
endif()

# OpenSSL support (optional)
if(ENABLE_SSL)
  find_package(OpenSSL QUIET)
  if(OPENSSL_FOUND)
    target_compile_definitions(axel PRIVATE HAVE_OPENSSL)
    target_include_directories(axel PRIVATE ${OPENSSL_INCLUDE_DIR})
    target_link_libraries(axel PRIVATE ${OPENSSL_LIBRARIES})
  else()
    message(WARNING "OpenSSL not found — building without SSL support")
  endif()
endif()

# Common portability defines
target_compile_definitions(axel PRIVATE _GNU_SOURCE)

# Compiler flags for static build
if(BUILD_STATIC)
  if (CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    # enable static linking (note: may fail if system libs are not static)
    set_target_properties(axel PROPERTIES
      LINK_FLAGS "-static"
    )
  endif()
endif()

# Additional libraries that axel may use (tweak if needed)
# Only attempt to link if library present in system (no hard failure)
find_library(LIBCRYPTO NAMES crypto libcrypto PATHS /usr/lib /usr/local/lib /mingw64/lib NO_DEFAULT_PATH)
if(LIBCRYPTO)
  target_link_libraries(axel PRIVATE ${LIBCRYPTO})
endif()

# Useful install rule
install(TARGETS axel RUNTIME DESTINATION bin)

# Helpful messages
message(STATUS "AXEL sources found: ${AXEL_SOURCES}")
message(STATUS "BUILD_STATIC = ${BUILD_STATIC}")
message(STATUS "ENABLE_SSL  = ${ENABLE_SSL}")
