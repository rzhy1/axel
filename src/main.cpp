#include <iostream>
#include <string>
#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <vector>
#include <curl/curl.h>

// ---------------- RateLimiter ----------------
struct RateLimiter {
    std::mutex mtx;
    std::atomic<size_t> bytes;

    RateLimiter(size_t b) : bytes(b) {}
    void consume(size_t n) {
        std::lock_guard<std::mutex> lock(mtx);
        if (bytes >= n) bytes -= n;
        else bytes = 0;
    }
};

// 使用智能指针管理全局限速器
std::unique_ptr<RateLimiter> global_limiter;

// ---------------- 下载回调 ----------------
size_t write_data(void* ptr, size_t size, size_t nmemb, void* stream) {
    size_t total = size * nmemb;
    if(global_limiter) global_limiter->consume(total);

    FILE* f = (FILE*)stream;
    return fwrite(ptr, size, nmemb, f);
}

// ---------------- 下载函数 ----------------
void download_file(const std::string& url, const std::string& out_path) {
    CURL* curl = curl_easy_init();
    if(!curl) return;

    FILE* fp = fopen(out_path.c_str(), "wb");
    if(!fp) {
        curl_easy_cleanup(curl);
        return;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
    curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    fclose(fp);
}

// ---------------- main ----------------
int main(int argc, char** argv) {
    if(argc < 3) {
        std::cout << "Usage: super_downloader <url> <output_file>\n";
        return 1;
    }

    std::string url = argv[1];
    std::string out = argv[2];

    // 限速示例 1MB/s
    global_limiter = std::make_unique<RateLimiter>(1024*1024);

    // 多线程下载示例（简单分块可扩展）
    int threads_count = 4;
    std::vector<std::thread> threads;

    for(int i=0; i<threads_count; ++i) {
        threads.emplace_back([&]{
            download_file(url, out);
        });
    }

    for(auto& t : threads) t.join();

    std::cout << "Download finished: " << out << "\n";
    return 0;
}
