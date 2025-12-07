#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <filesystem>
#include <regex>
#include <chrono>
#include <sstream>
#include <curl/curl.h>

namespace fs = std::filesystem;

struct DownloadTask {
    std::string url;
    std::string filename;
    size_t start;
    size_t end;
    int retry_count;
};

struct Config {
    std::string output_dir = ".";
    int threads = 4;
    size_t speed_limit = 0;
    std::string proxy;
    std::string user_agent;
    int max_retry = 3;
    bool insecure = false;       // 跳过 SSL 验证
    std::string cacert_file;     // 指定 CA 文件
};

std::mutex cout_mtx;

// 写入回调
size_t write_data(void* ptr, size_t size, size_t nmemb, void* stream) {
    std::ofstream* ofs = static_cast<std::ofstream*>(stream);
    ofs->write(static_cast<char*>(ptr), size * nmemb);
    return size * nmemb;
}

// 获取文件名
std::string get_filename(const std::string& url, const std::string& content_disp="") {
    if(!content_disp.empty()) {
        std::regex re(R"(filename="?([^\";]+)"?)");
        std::smatch m;
        if(std::regex_search(content_disp, m, re)) return m[1];
    }
    auto pos = url.find_last_of("/\\");
    if(pos != std::string::npos && pos+1 < url.size()) return url.substr(pos+1);
    return "downloaded_file";
}

// 下载单段任务
bool download_range(DownloadTask task, const Config& cfg, std::atomic<size_t>& downloaded_bytes, size_t total_size) {
    for(int attempt = 0; attempt <= task.retry_count; ++attempt) {
        CURL* curl = curl_easy_init();
        if(!curl) return false;

        std::ofstream ofs(task.filename, std::ios::binary | std::ios::in | std::ios::out);
        if(!ofs.is_open()) return false;
        ofs.seekp(task.start);

        std::string range = std::to_string(task.start) + "-" + std::to_string(task.end);

        curl_easy_setopt(curl, CURLOPT_URL, task.url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ofs);
        curl_easy_setopt(curl, CURLOPT_RANGE, range.c_str());
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

        if(!cfg.proxy.empty()) curl_easy_setopt(curl, CURLOPT_PROXY, cfg.proxy.c_str());
        if(!cfg.user_agent.empty()) curl_easy_setopt(curl, CURLOPT_USERAGENT, cfg.user_agent.c_str());
        if(cfg.speed_limit > 0) curl_easy_setopt(curl, CURLOPT_MAX_RECV_SPEED_LARGE, cfg.speed_limit / cfg.threads);

        if(cfg.insecure) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        } else if(!cfg.cacert_file.empty()) {
            curl_easy_setopt(curl, CURLOPT_CAINFO, cfg.cacert_file.c_str());
        }

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        if(res == CURLE_OK) {
            downloaded_bytes += (task.end - task.start + 1);
            return true;
        } else {
            std::lock_guard<std::mutex> lock(cout_mtx);
            std::cout << "Retry " << attempt+1 << "/" << task.retry_count << " for range " << range 
                      << " failed: " << curl_easy_strerror(res) << std::endl;
        }
    }
    return false;
}

// 进度显示
void progress_thread(const std::atomic<size_t>& downloaded_bytes, size_t total_size) {
    using namespace std::chrono_literals;
    while(downloaded_bytes < total_size) {
        double pct = downloaded_bytes * 100.0 / total_size;
        std::cout << "\rProgress: " << pct << "% (" << downloaded_bytes << "/" << total_size << " bytes)" << std::flush;
        std::this_thread::sleep_for(500ms);
    }
    std::cout << "\rProgress: 100% (" << total_size << "/" << total_size << " bytes)\n";
}

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cout << "Usage: super_downloader <URL> [output_dir] [threads] [--insecure] [--cacert <file>]\n";
        return 1;
    }

    std::string url = argv[1];
    Config cfg;

    for(int i=2;i<argc;i++) {
        std::string arg = argv[i];
        if(arg == "--insecure") cfg.insecure = true;
        else if(arg == "--cacert" && i+1 < argc) { cfg.cacert_file = argv[++i]; }
        else if(cfg.output_dir == ".") cfg.output_dir = arg;
        else cfg.threads = std::stoi(arg);
    }

    if(!fs::exists(cfg.output_dir)) fs::create_directories(cfg.output_dir);

    CURL* curl = curl_easy_init();
    if(!curl) return 1;

    std::string filename;
    std::string content_disp;
    double filesize = 0;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, [](char* buffer, size_t size, size_t nitems, void* userdata) -> size_t {
        std::string header(buffer, size*nitems);
        std::string* content_disp = static_cast<std::string*>(userdata);
        std::regex re(R"(Content-Disposition:\s*attachment;\s*filename="?([^\";]+)"?)", std::regex_constants::icase);
        std::smatch m;
        if(std::regex_search(header, m, re)) {
            *content_disp = m[1];
        }
        return size*nitems;
    });
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &content_disp);

    if(cfg.insecure) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    } else if(!cfg.cacert_file.empty()) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, cfg.cacert_file.c_str());
    }

    CURLcode res = curl_easy_perform(curl);
    if(res != CURLE_OK) {
        std::cerr << "Failed to get file info: " << curl_easy_strerror(res) << "\n";
        curl_easy_cleanup(curl);
        return 1;
    }

    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &filesize);
    curl_easy_cleanup(curl);

    if(filename.empty()) filename = get_filename(url, content_disp);
    fs::path out_file = fs::path(cfg.output_dir) / filename;

    // 预创建文件
    {
        std::ofstream ofs(out_file, std::ios::binary);
        ofs.seekp(static_cast<std::streamoff>(filesize) - 1);
        ofs.write("", 1);
    }

    std::cout << "Downloading " << url << " -> " << out_file << " (" << filesize << " bytes) using " << cfg.threads << " threads\n";

    // 生成下载任务
    std::vector<DownloadTask> tasks;
    size_t part_size = static_cast<size_t>(filesize) / cfg.threads;
    size_t start = 0;
    for(int i=0;i<cfg.threads;i++) {
        size_t end = (i == cfg.threads-1) ? static_cast<size_t>(filesize)-1 : start+part_size-1;
        tasks.push_back({url, out_file.string(), start, end, cfg.max_retry});
        start += part_size;
    }

    std::atomic<size_t> downloaded_bytes(0);

    // 启动进度线程
    std::thread prog(progress_thread, std::ref(downloaded_bytes), static_cast<size_t>(filesize));

    // 启动下载线程
    std::vector<std::thread> threads;
    for(auto& t: tasks)
        threads.emplace_back(download_range, t, std::ref(cfg), std::ref(downloaded_bytes));

    for(auto& th: threads) th.join();
    prog.join();

    std::cout << "Download completed!\n";
    return 0;
}
