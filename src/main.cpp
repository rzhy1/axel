#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#ifdef _WIN32
#include <windows.h>
#include <wininet.h>
#pragma comment(lib, "wininet.lib")
#endif

// ---------------------- 工具函数 ----------------------

bool str_starts_with(const std::string& s, const std::string& p) {
    return s.size() >= p.size() && std::memcmp(s.data(), p.data(), p.size()) == 0;
}

std::string simple_format(const std::string& fmt, long long a, long long b, long long c = 0) {
    char buf[256];
    if (c == 0)
        std::snprintf(buf, sizeof(buf), fmt.c_str(), a, b);
    else
        std::snprintf(buf, sizeof(buf), fmt.c_str(), a, b, c);
    return std::string(buf);
}

// ---------------------- HTTP HEAD 获取文件大小 ----------------------

long long http_get_size(const std::string& url, bool skip_ssl) {
#ifdef _WIN32
    HINTERNET hInternet = InternetOpenA("super_downloader", INTERNET_OPEN_TYPE_PRECONFIG, NULL, NULL, 0);
    if (!hInternet) return -1;

    DWORD flags = INTERNET_FLAG_RELOAD | INTERNET_FLAG_NO_CACHE_WRITE;
    if (skip_ssl)
        flags |= INTERNET_FLAG_IGNORE_CERT_CN_INVALID | INTERNET_FLAG_IGNORE_CERT_DATE_INVALID | SECURITY_FLAG_IGNORE_UNKNOWN_CA;

    HINTERNET hFile = InternetOpenUrlA(hInternet, url.c_str(), NULL, 0, flags | INTERNET_FLAG_NO_UI | INTERNET_FLAG_NO_COOKIES, 0);
    if (!hFile) {
        InternetCloseHandle(hInternet);
        return -1;
    }

    char buffer[2048];
    DWORD size = 0;
    while (InternetReadFile(hFile, buffer, sizeof(buffer), &size) && size) {
        // 只读，不保存
    }

    // 再查询大小
    DWORD fileSizeHigh = 0;
    DWORD fileSizeLow = InternetQueryDataAvailable(hFile, NULL, 0, NULL);
    // WinInet 无法直接拿 HEAD 长度，换 Content-Length
    DWORD length = 0;
    DWORD lenSize = sizeof(length);
    if (HttpQueryInfoA(hFile, HTTP_QUERY_CONTENT_LENGTH | HTTP_QUERY_FLAG_NUMBER, &length, &lenSize, NULL)) {
        InternetCloseHandle(hFile);
        InternetCloseHandle(hInternet);
        return static_cast<long long>(length);
    }

    InternetCloseHandle(hFile);
    InternetCloseHandle(hInternet);
    return -1;
#else
    return -1; // 非 Windows
#endif
}

// ---------------------- 下载一个区块（写入内存） ----------------------

bool http_download_range(
    const std::string& url,
    long long startPos,
    long long endPos,
    std::vector<char>& out,
    bool skip_ssl
) {
#ifdef _WIN32
    HINTERNET hInternet = InternetOpenA("super_downloader", INTERNET_OPEN_TYPE_PRECONFIG, NULL, NULL, 0);
    if (!hInternet) return false;

    DWORD flags = INTERNET_FLAG_RELOAD | INTERNET_FLAG_NO_CACHE_WRITE;
    if (skip_ssl)
        flags |= INTERNET_FLAG_IGNORE_CERT_CN_INVALID | INTERNET_FLAG_IGNORE_CERT_DATE_INVALID | SECURITY_FLAG_IGNORE_UNKNOWN_CA;

    std::string range = "Range: bytes=" + std::to_string(startPos) + "-" + std::to_string(endPos);
    HINTERNET hFile = InternetOpenUrlA(hInternet, url.c_str(), range.c_str(), range.size(),
        flags | INTERNET_FLAG_NO_UI | INTERNET_FLAG_NO_COOKIES, 0);

    if (!hFile) {
        InternetCloseHandle(hInternet);
        return false;
    }

    out.clear();
    char buffer[8192];
    DWORD size = 0;

    while (InternetReadFile(hFile, buffer, sizeof(buffer), &size) && size) {
        out.insert(out.end(), buffer, buffer + size);
    }

    InternetCloseHandle(hFile);
    InternetCloseHandle(hInternet);
    return true;
#else
    return false;
#endif
}

// ---------------------- 主程序 ----------------------

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "[错误] URL 无效，必须以 http:// 或 https:// 开头\n";
        return 1;
    }

    std::string url = argv[1];
    bool skip_ssl = false;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--skip-ssl") == 0)
            skip_ssl = true;
    }

    if (!(str_starts_with(url, "http://") || str_starts_with(url, "https://"))) {
        std::cout << "[错误] URL 无效，必须以 http:// 或 https:// 开头\n";
        return 1;
    }

    std::cout << "[信息] 正在请求文件大小...\n";

    long long total = http_get_size(url, skip_ssl);
    if (total <= 0) {
        std::cout << "[错误] 无法获取文件大小，服务器可能不支持 HEAD\n";
        return 1;
    }

    std::cout << "[信息] 文件大小: " << total << " 字节\n";

    int threads = 4;
    long long block = total / threads;

    std::vector<std::vector<char>> parts(threads);
    std::vector<std::thread> ths;

    std::atomic<long long> downloaded(0);
    auto t0 = std::chrono::steady_clock::now();

    // ---------------------- 启动线程下载 ----------------------
    for (int i = 0; i < threads; ++i) {
        long long start = i * block;
        long long end = (i == threads - 1) ? (total - 1) : (start + block - 1);

        ths.emplace_back([&, i, start, end]() {
            http_download_range(url, start, end, parts[i], skip_ssl);
            downloaded += (end - start + 1);
        });
    }

    // ---------------------- 显示进度 ----------------------
    while (downloaded < total) {
        auto now = std::chrono::steady_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - t0).count();
        if (ms == 0) ms = 1;

        long long speed = downloaded / (ms / 1000.0);

        std::cout << "\r[下载中] " << downloaded << "/" << total
                  << "  (" << speed / 1024 << " KB/s)"
                  << std::flush;

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "\n[信息] 下载完成，正在合并...\n";

    // ---------------------- 合并到最终文件（无任何临时文件） ----------------------

    std::string outName = "output.bin";
    FILE* fp = std::fopen(outName.c_str(), "wb");
    if (!fp) {
        std::cout << "[错误] 无法创建输出文件\n";
        return 1;
    }

    for (auto& p : parts)
        std::fwrite(p.data(), 1, p.size(), fp);

    std::fclose(fp);

    std::cout << "[完成] 已保存到 " << outName << "\n";
    return 0;
}
