cmake_minimum_required(VERSION 3.25)
project(super_downloader)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找依赖库
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

add_executable(super_downloader src/main.cpp)

target_include_directories(super_downloader PRIVATE ${CURL_INCLUDE_DIRS} ${OPENSSL_INCLUDE_DIR})
target_link_libraries(super_downloader PRIVATE ${CURL_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto)
