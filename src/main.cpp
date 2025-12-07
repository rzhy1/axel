#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <curl/curl.h>
#include <filesystem>
#include <regex>

namespace fs = std::filesystem;

// ---------------------------
// RateLimiter (智能指针版)
// ---------------------------
struct RateLimiter {
    std::mutex mtx;
    std::atomic<size_t> bytes_per_sec;
    RateLimiter(size_t bps = 0) : bytes_per_sec(bps) {}
    void limit(size_t bytes) {
        if (bytes_per_sec > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(bytes * 1000 / bytes_per_sec));
        }
    }
};

// ---------------------------
// CURL 写入回调
// ---------------------------
size_t write_file(void* ptr, size_t size, size_t nmemb, void* userdata) {
    std::ofstream* out = static_cast<std::ofstream*>(userdata);
    out->write(static_cast<char*>(ptr), size * nmemb);
    return size * nmemb;
}

// ---------------------------
// 进度回调
// ---------------------------
int progress_callback(void* clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow) {
    if (dltotal > 0) {
        int percent = static_cast<int>(dlnow * 100 / dltotal);
        std::cout << "\rDownloading: " << percent << "% (" << dlnow << "/" << dltotal << " bytes)" << std::flush;
    }
    return 0;
}

// ---------------------------
// 下载单个 URL
// ---------------------------
bool download_file(const std::string& url, const fs::path& path,
                   bool insecure = false, const std::string& cacert = "",
                   const std::string& proxy = "", std::shared_ptr<RateLimiter> limiter = nullptr,
                   int retries = 3) {
    for (int attempt = 1; attempt <= retries; ++attempt) {
        CURL* curl = curl_easy_init();
        if (!curl) return false;

        std::ofstream ofs(path, std::ios::binary);
        if (!ofs) return false;

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_file);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ofs);
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, progress_callback);

        if (insecure)
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        if (!cacert.empty())
            curl_easy_setopt(curl, CURLOPT_CAINFO, cacert.c_str());
        if (!proxy.empty())
            curl_easy_setopt(curl, CURLOPT_PROXY, proxy.c_str());

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        std::cout << std::endl;
        if (res == CURLE_OK) return true;

        std::cerr << "Download failed (attempt " << attempt << "): " << curl_easy_strerror(res) << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return false;
}

// ---------------------------
// 获取 URL 文件名
// ---------------------------
std::string get_filename_from_url(const std::string& url) {
    std::regex re(R"(([^/\\?]+)(\?.*)?$)");
    std::smatch m;
    if (std::regex_search(url, m, re))
        return m[1].str();
    return "downloaded_file";
}

// ---------------------------
// 主程序
// ---------------------------
int main(int argc, char** argv) {
    if (argc < 2) {
        std::cout << "Usage: super_downloader <URL> [output_dir] [--insecure] [--cacert <file>] [--proxy <host:port>] [--threads N] [--retries N]\n";
        return 1;
    }

    std::string url = argv[1];
    fs::path out_dir = fs::current_path();
    bool insecure = false;
    std::string cacert, proxy;
    int threads = 1;
    int retries = 3;

    // 参数解析
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--insecure") insecure = true;
        else if (arg == "--cacert" && i + 1 < argc) cacert = argv[++i];
        else if (arg == "--proxy" && i + 1 < argc) proxy = argv[++i];
        else if (arg == "--threads" && i + 1 < argc) threads = std::stoi(argv[++i]);
        else if (arg == "--retries" && i + 1 < argc) retries = std::stoi(argv[++i]);
        else out_dir = arg;
    }

    fs::create_directories(out_dir);
    std::string filename = get_filename_from_url(url);
    fs::path out_file = out_dir / filename;

    std::shared_ptr<RateLimiter> limiter = std::make_shared<RateLimiter>(0);

    bool ok = download_file(url, out_file, insecure, cacert, proxy, limiter, retries);
    if (ok) std::cout << "Downloaded to " << out_file.string() << "\n";
    else std::cerr << "Download failed after " << retries << " attempts\n";

    return ok ? 0 : 1;
}
