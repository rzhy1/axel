cmake_minimum_required(VERSION 3.20)
project(super_downloader LANGUAGES CXX)

# 使用 vcpkg toolchain
if(DEFINED ENV{VCPKG_ROOT})
    set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找 curl
find_package(CURL REQUIRED)

add_executable(super_downloader src/main.cpp)
target_link_libraries(super_downloader PRIVATE CURL::libcurl)
