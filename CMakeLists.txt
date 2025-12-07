cmake_minimum_required(VERSION 3.15)
project(super_downloader LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE OFF)

option(BUILD_STATIC "Link libcurl statically (if available)" OFF)

find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED) # 用于 sha256 校验，可选

add_executable(super_downloader src/main.cpp)

target_include_directories(super_downloader PRIVATE ${CURL_INCLUDE_DIRS} ${OPENSSL_INCLUDE_DIR})
target_link_libraries(super_downloader PRIVATE ${CURL_LIBRARIES} ${OPENSSL_LIBRARIES})

if (UNIX)
    target_link_libraries(super_downloader PRIVATE pthread)
endif()
