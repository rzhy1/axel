// src/main.cpp
// Multi-threaded downloader using libcurl (Windows/Unix)
// Built with C++17
// Improvements: RAII for resources, atomic progress tracking, robust size probing, optimized merging.

// ============================================================
// FIX FOR WINDOWS MSVC BUILD ERRORS
// These defines must be at the very top, before ANY includes.
// ============================================================
#define _CRT_SECURE_NO_WARNINGS
#define NOMINMAX
// ============================================================

#include <curl/curl.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <memory>
#include <algorithm>
#include <functional>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

// --- RAII Helpers ---
struct CurlDeleter { void operator()(CURL* c) { if(c) curl_easy_cleanup(c); } };
using CurlPtr = std::unique_ptr<CURL, CurlDeleter>;

struct FileDeleter { void operator()(FILE* f) { if(f) fclose(f); } };
using FilePtr = std::unique_ptr<FILE, FileDeleter>;

struct Options {
    std::string url;
    std::string out;
    bool force = false;
    bool insecure = false;
    std::string cacert;
    std::string proxy;
    long limit_rate = 0; // bytes/sec (global)
    int threads = 4;
    int retries = 3;
};

// --- Utils ---

static void print_help() {
    std::cout <<
        "SuperDownloader (libcurl + C++17)\n"
        "Usage:\n"
        "  superdl <url> [options]\n\n"
        "Options:\n"
        "  -o <file>         output file name (default from URL)\n"
        "  --force           overwrite existing file\n"
        "  --threads N       number of threads (default 4)\n"
        "  --retries N       per-segment retries (default 3)\n"
        "  --insecure        skip SSL verification\n"
        "  --cacert FILE     CA certificate bundle file\n"
        "  --proxy PROXY     proxy URL (http://host:port, socks5://...)\n"
        "  --limit-rate N    global limit, supports K/M suffix (e.g. 100K, 5M)\n"
        "  -h, --help        show help\n";
}

static bool is_url(const std::string& s) {
    return s.rfind("http://", 0) == 0 || s.rfind("https://", 0) == 0;
}

static long parse_rate(const std::string& s) {
    if (s.empty()) return 0;
    char last = s.back();
    std::string num = s;
    long mul = 1;
    if (last == 'K' || last == 'k') { mul = 1024; num = s.substr(0, s.size()-1); }
    else if (last == 'M' || last == 'm') { mul = 1024*1024; num = s.substr(0, s.size()-1); }
    else if (last == 'G' || last == 'g') { mul = 1024*1024*1024; num = s.substr(0, s.size()-1); }
    try {
        long v = std::stol(num);
        return v * mul;
    } catch(...) { return 0; }
}

static std::string human_readable(long long b) {
    if (b < 1024) return std::to_string(b) + " B";
    double v = static_cast<double>(b);
    const char* u[] = {" B"," KB"," MB"," GB"," TB"};
    int i = 0;
    while (v >= 1024.0 && i < 4) { v /= 1024.0; ++i; }
    char buf[64];
    snprintf(buf, sizeof(buf), "%.2f%s", v, u[i]);
    return std::string(buf);
}

static std::string format_time(double seconds) {
    if (seconds < 0) return "--";
    if (seconds < 60) return std::to_string(static_cast<int>(seconds)) + "s";
    if (seconds < 3600) {
        int m = static_cast<int>(seconds / 60);
        int s = static_cast<int>(seconds) % 60;
        return std::to_string(m) + "m" + (s < 10 ? "0" : "") + std::to_string(s) + "s";
    }
    int h = static_cast<int>(seconds / 3600);
    int m = static_cast<int>((seconds - h * 3600) / 60);
    return std::to_string(h) + "h" + (m < 10 ? "0" : "") + std::to_string(m) + "m";
}

// --- Logic ---

struct WriteCtx {
    FILE* fp;
    std::atomic<long long>* downloaded_counter;
};

static size_t write_cb(void* ptr, size_t size, size_t nmemb, void* userdata) {
    WriteCtx* ctx = static_cast<WriteCtx*>(userdata);
    if (!ctx || !ctx->fp) return 0;
    
    size_t total_size = size * nmemb;
    size_t written = fwrite(ptr, 1, total_size, ctx->fp);
    
    if (ctx->downloaded_counter && written > 0) {
        ctx->downloaded_counter->fetch_add(written, std::memory_order_relaxed);
    }
    return written;
}

struct Part {
    long long start;
    long long end;
    std::string partfile;
    std::atomic<long long> downloaded; // Bytes downloaded in this session (or detected on disk)
    std::atomic<bool> done;
    std::atomic<bool> failed;
    int idx;
    Part() : start(0), end(0), downloaded(0), done(false), failed(false), idx(-1) {}
};

// 改进的大小探测函数：更健壮的方法
static long long probe_file_size(const std::string& url, const Options& opt) {
    CURL* curl = curl_easy_init();
    if (!curl) return -1;
    
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L); // HEAD request
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
    
    // 设置User-Agent，有些服务器需要
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "Mozilla/5.0 (compatible; SuperDownloader/1.0)");
    
    if (opt.insecure) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
    
    if (!opt.cacert.empty()) curl_easy_setopt(curl, CURLOPT_CAINFO, opt.cacert.c_str());
    if (!opt.proxy.empty()) curl_easy_setopt(curl, CURLOPT_PROXY, opt.proxy.c_str());
    
    // 尝试HEAD请求
    CURLcode res = curl_easy_perform(curl);
    
    double content_length = -1;
    if (res == CURLE_OK) {
        curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
    }
    
    // 如果HEAD失败，尝试GET请求获取前几个字节来检查Range支持
    if (content_length <= 0) {
        std::cout << "HEAD request failed or no Content-Length, trying GET with Range...\n";
        
        curl_easy_reset(curl);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_RANGE, "0-0"); // 只请求第一个字节
        curl_easy_setopt(curl, CURLOPT_NOBODY, 0L); // GET请求
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
        
        // 不关心响应体，只检查头部
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, [](void*, size_t, size_t, void*) {
            return static_cast<size_t>(0); // 丢弃数据
        });
        
        if (opt.insecure) {
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        }
        if (!opt.cacert.empty()) curl_easy_setopt(curl, CURLOPT_CAINFO, opt.cacert.c_str());
        if (!opt.proxy.empty()) curl_easy_setopt(curl, CURLOPT_PROXY, opt.proxy.c_str());
        
        res = curl_easy_perform(curl);
        
        if (res == CURLE_OK) {
            long response_code = 0;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
            
            if (response_code == 206) {
                // 服务器支持Range请求，尝试获取完整大小
                std::cout << "Server supports Range requests but no Content-Length in HEAD.\n";
                // 返回-1触发单线程下载，但我们可以尝试多线程
                content_length = -2; // 特殊值表示支持Range但没有大小
            } else if (response_code == 200) {
                // 服务器返回200，可能不支持Range或忽略Range头
                std::cout << "Server may not support Range requests (got 200 instead of 206).\n";
                content_length = -1;
            }
        }
    }
    
    curl_easy_cleanup(curl);
    
    if (content_length > 0) {
        std::cout << "Detected file size: " << human_readable(static_cast<long long>(content_length)) << "\n";
        return static_cast<long long>(content_length);
    } else if (content_length == -2) {
        std::cout << "Server supports Range requests but file size unknown.\n";
        std::cout << "Will use multi-threaded download with unknown total size.\n";
        return -2; // 特殊值表示支持Range但没有大小
    }
    
    std::cout << "Cannot determine file size, server may not support required features.\n";
    return -1;
}

static void set_curl_options(CURL* curl, const Options& opt) {
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1024L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 30L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    
    // 设置User-Agent
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "Mozilla/5.0 (compatible; SuperDownloader/1.0)");
    
    if (opt.insecure) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
    
    if (!opt.cacert.empty()) curl_easy_setopt(curl, CURLOPT_CAINFO, opt.cacert.c_str());
    if (!opt.proxy.empty()) curl_easy_setopt(curl, CURLOPT_PROXY, opt.proxy.c_str());
}

static void worker_func(const Options& opt, Part* part, long long total_size) {
    // 打开部分文件用于追加
    FilePtr fp(fopen(part->partfile.c_str(), "ab+"));
    if (!fp) {
        std::cerr << "[Thread " << part->idx << "] Error opening file: " << part->partfile << "\n";
        part->failed = true;
        return;
    }

    // 检查现有大小
    if (fseek(fp.get(), 0, SEEK_END) != 0) {
        std::cerr << "[Thread " << part->idx << "] Error seeking in file\n";
        part->failed = true;
        return;
    }
    
    long long current_size = ftell(fp.get());
    if (current_size < 0) {
        std::cerr << "[Thread " << part->idx << "] Error getting file size\n";
        part->failed = true;
        return;
    }
    
    // 更新原子计数器以反映磁盘上已有的内容
    part->downloaded = current_size;

    // 如果不知道总大小，我们只检查是否已经下载了一些内容
    long long part_len = (total_size > 0) ? (part->end - part->start + 1) : LLONG_MAX;
    if (total_size > 0 && current_size >= part_len) {
        part->done = true;
        return; // 已经完成
    }

    CurlPtr c(curl_easy_init());
    if (!c) {
        part->failed = true;
        return;
    }

    // 基本设置
    curl_easy_setopt(c.get(), CURLOPT_URL, opt.url.c_str());
    set_curl_options(c.get(), opt);
    
    // 速率限制（每个线程的估计值）
    if (opt.limit_rate > 0) {
        long long per_thread = opt.limit_rate / (opt.threads > 0 ? opt.threads : 1);
        if (per_thread > 0) {
            curl_easy_setopt(c.get(), CURLOPT_MAX_RECV_SPEED_LARGE, static_cast<curl_off_t>(per_thread));
        }
    }

    WriteCtx ctx{fp.get(), &part->downloaded};
    curl_easy_setopt(c.get(), CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c.get(), CURLOPT_WRITEDATA, &ctx);

    int tries = 0;
    bool fatal_error = false;
    
    while (tries <= opt.retries && !part->done && !fatal_error) {
        // 基于当前文件大小重新计算范围（恢复）
        if (fflush(fp.get()) != 0) {
            std::cerr << "[Thread " << part->idx << "] Error flushing file\n";
            break;
        }
        
        if (fseek(fp.get(), 0, SEEK_END) != 0) {
            std::cerr << "[Thread " << part->idx << "] Error seeking in file\n";
            break;
        }
        
        long long now_have = ftell(fp.get());
        if (now_have < 0) {
            std::cerr << "[Thread " << part->idx << "] Error getting file position\n";
            break;
        }
        
        // 安全检查
        if (total_size > 0 && now_have >= part_len) {
            part->done = true;
            break;
        }
        
        // 在请求前将原子计数器与磁盘实际情况同步
        part->downloaded = now_have;

        if (total_size > 0) {
            long long req_start = part->start + now_have;
            if (req_start > part->end) {
                part->done = true;
                break;
            }
            
            char range_buf[64];
            snprintf(range_buf, sizeof(range_buf), "%lld-%lld", req_start, part->end);
            curl_easy_setopt(c.get(), CURLOPT_RANGE, range_buf);
        } else {
            // 不知道总大小，不设置Range头，让服务器决定发送什么
            curl_easy_setopt(c.get(), CURLOPT_RANGE, nullptr);
        }

        CURLcode res = curl_easy_perform(c.get());
        
        if (res == CURLE_OK) {
            // 如果我们知道总大小，验证我们是否获得了完整的范围
            if (total_size > 0) {
                fseek(fp.get(), 0, SEEK_END);
                long long final_size = ftell(fp.get());
                if (final_size >= part_len) {
                    part->done = true;
                    break;
                } else {
                    // 部分下载，继续重试
                    tries++;
                }
            } else {
                // 不知道总大小，检查是否收到了任何数据
                fseek(fp.get(), 0, SEEK_END);
                long long final_size = ftell(fp.get());
                if (final_size > now_have) {
                    // 收到了新数据，继续
                    part->downloaded = final_size;
                } else {
                    // 没有收到新数据，可能是完成或错误
                    // 检查HTTP响应码
                    long response_code = 0;
                    curl_easy_getinfo(c.get(), CURLINFO_RESPONSE_CODE, &response_code);
                    
                    if (response_code >= 200 && response_code < 300) {
                        // 可能是下载完成（服务器关闭连接）
                        part->done = true;
                        break;
                    } else {
                        tries++;
                    }
                }
            }
        } else if (res == CURLE_HTTP_RETURNED_ERROR) {
            // HTTP错误
            long response_code = 0;
            curl_easy_getinfo(c.get(), CURLINFO_RESPONSE_CODE, &response_code);
            
            // 416表示范围请求无法满足（可能已经下载完成）
            if (response_code == 416) {
                part->done = true;
                break;
            }
            
            // 不重试客户端错误（4xx），除了408（请求超时）和429（请求过多）
            if (response_code >= 400 && response_code < 500 && 
                response_code != 408 && response_code != 429) {
                std::cerr << "[Thread " << part->idx << "] HTTP Error " << response_code 
                          << ", stopping.\n";
                fatal_error = true;
                break;
            }
            
            tries++;
            if (tries <= opt.retries) {
                std::cerr << "[Thread " << part->idx << "] Retry " << tries << "/" << opt.retries 
                          << " (HTTP " << response_code << ")\n";
                // 指数退避
                std::this_thread::sleep_for(std::chrono::seconds(1 << (tries - 1)));
            } else {
                std::cerr << "[Thread " << part->idx << "] Failed after " << opt.retries << " retries\n";
                part->failed = true;
            }
        } else {
            // 其他CURL错误
            tries++;
            if (tries <= opt.retries) {
                std::cerr << "[Thread " << part->idx << "] Retry " << tries << "/" << opt.retries 
                          << " (" << curl_easy_strerror(res) << ")\n";
                // 指数退避
                std::this_thread::sleep_for(std::chrono::seconds(1 << (tries - 1)));
            } else {
                std::cerr << "[Thread " << part->idx << "] Failed after " << opt.retries << " retries: " 
                          << curl_easy_strerror(res) << "\n";
                part->failed = true;
            }
        }
    }
    
    if (part->done && total_size > 0) {
        // 最终大小检查
        fseek(fp.get(), 0, SEEK_END);
        long long final_size = ftell(fp.get());
        if (final_size < part_len) {
            std::cerr << "[Thread " << part->idx << "] Warning: Incomplete download ("
                      << final_size << "/" << part_len << " bytes)\n";
            part->failed = true;
        }
    }
}

static bool merge_parts(const std::string& out, int nparts, long long total_size) {
    std::cout << "\nMerging " << nparts << " parts into " << out << "...\n";
    
    // 首先检查各部分的总大小
    long long parts_total = 0;
    for (int i = 0; i < nparts; ++i) {
        std::string p = out + ".part" + std::to_string(i);
        std::error_code ec;
        if (!fs::exists(p, ec)) {
            std::cerr << "Error: Missing part file " << p << "\n";
            return false;
        }
        auto sz = fs::file_size(p, ec);
        if (ec) {
            std::cerr << "Error: Cannot get size of part file " << p << ": " << ec.message() << "\n";
            return false;
        }
        parts_total += static_cast<long long>(sz);
    }
    
    if (total_size > 0 && parts_total != total_size) {
        std::cerr << "Warning: Total parts size (" << human_readable(parts_total) 
                  << ") doesn't match expected size (" << human_readable(total_size) << ")\n";
    }
    
    // 使用合理的缓冲区大小
    const size_t BUF_SIZE = 1024 * 1024; // 1MB
    std::unique_ptr<char[]> buffer(new char[BUF_SIZE]);

    std::ofstream ofs(out, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        std::cerr << "Error creating output file: " << out << "\n";
        return false;
    }

    long long merged_bytes = 0;
    for (int i = 0; i < nparts; ++i) {
        std::string p = out + ".part" + std::to_string(i);
        std::ifstream ifs(p, std::ios::binary | std::ios::ate);
        if (!ifs.is_open()) {
            std::cerr << "Error: Cannot open part file " << p << "\n";
            return false;
        }
        
        auto file_size = ifs.tellg();
        ifs.seekg(0, std::ios::beg);
        
        long long part_bytes = 0;
        while (ifs) {
            ifs.read(buffer.get(), BUF_SIZE);
            std::streamsize cnt = ifs.gcount();
            if (cnt > 0) {
                ofs.write(buffer.get(), cnt);
                if (!ofs.good()) {
                    std::cerr << "Error writing to output file at " << merged_bytes << " bytes\n";
                    return false;
                }
                part_bytes += cnt;
                merged_bytes += cnt;
            }
        }
        
        if (part_bytes != file_size) {
            std::cerr << "Warning: Part " << i << " size mismatch: read " << part_bytes 
                      << ", expected " << file_size << "\n";
        }
        
        ifs.close();
        
        // 对于大文件显示进度
        if (parts_total > 100 * 1024 * 1024) { // > 100MB
            double progress = static_cast<double>(merged_bytes) / parts_total * 100.0;
            std::cout << "\rMerging: " << std::fixed << std::setprecision(1) << progress << "%" << std::flush;
        }
    }
    
    if (parts_total > 100 * 1024 * 1024) {
        std::cout << "\n";
    }
    
    return true;
}

static void remove_parts(const std::string& out, int nparts) {
    bool any_error = false;
    for (int i = 0; i < nparts; ++i) {
        std::string p = out + ".part" + std::to_string(i);
        std::error_code ec;
        if (fs::exists(p, ec)) {
            if (!fs::remove(p, ec)) {
                std::cerr << "Warning: Could not remove part file " << p << ": " << ec.message() << "\n";
                any_error = true;
            }
        }
    }
    if (any_error) {
        std::cerr << "Some part files could not be removed. You may need to clean them up manually.\n";
    }
}

// 单线程回退，用于不支持Range/HEAD的服务器
static bool download_single_thread(const Options& opt) {
    std::cout << "Downloading with single thread...\n";
    
    CurlPtr c(curl_easy_init());
    if (!c) {
        std::cerr << "Failed to initialize CURL\n";
        return false;
    }
    
    FilePtr fp(fopen(opt.out.c_str(), "wb"));
    if (!fp) {
        std::cerr << "Cannot open output file: " << opt.out << "\n";
        return false;
    }
    
    curl_easy_setopt(c.get(), CURLOPT_URL, opt.url.c_str());
    set_curl_options(c.get(), opt);
    
    std::atomic<long long> downloaded(0);
    WriteCtx ctx{fp.get(), &downloaded};
    curl_easy_setopt(c.get(), CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c.get(), CURLOPT_WRITEDATA, &ctx);
    
    if (opt.limit_rate > 0) {
        curl_easy_setopt(c.get(), CURLOPT_MAX_RECV_SPEED_LARGE, static_cast<curl_off_t>(opt.limit_rate));
    }
    
    // 进度回调
    auto start_time = std::chrono::steady_clock::now();
    auto last_update = start_time;
    long long last_bytes = 0;
    
    curl_easy_setopt(c.get(), CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(c.get(), CURLOPT_XFERINFOFUNCTION, [](void* clientp,
        curl_off_t dltotal, curl_off_t dlnow,
        curl_off_t, curl_off_t) -> int {
        
        auto now = std::chrono::steady_clock::now();
        auto* data = static_cast<std::tuple<std::chrono::steady_clock::time_point*, 
                                           long long*, long long*>*>(clientp);
        if (!data) return 0;
        
        auto& [last_update, last_bytes, downloaded] = *data;
        
        // 使用curl的进度更新我们的原子计数器
        long long new_bytes = dlnow;
        long long diff = new_bytes - *last_bytes;
        if (diff > 0) {
            downloaded += diff;
            *last_bytes = new_bytes;
        }
        
        // 每200ms更新一次显示，避免闪烁
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - *last_update).count() > 200) {
            double speed = 0;
            auto elapsed = std::chrono::duration<double>(now - *last_update).count();
            if (elapsed > 0) {
                speed = diff / elapsed;
            }
            
            double percent = dltotal > 0 ? (dlnow * 100.0 / dltotal) : 0;
            std::cout << "\rProgress: " << std::fixed << std::setprecision(1) << percent << "% "
                      << "(" << human_readable(dlnow) 
                      << (dltotal > 0 ? "/" + human_readable(dltotal) : "") << ") "
                      << "@ " << human_readable(static_cast<long long>(speed)) << "/s" << std::flush;
            
            *last_update = now;
            *last_bytes = new_bytes;
        }
        
        return 0;
    });
    
    auto progress_data = std::make_tuple(&last_update, &last_bytes, &downloaded);
    curl_easy_setopt(c.get(), CURLOPT_XFERINFODATA, &progress_data);

    CURLcode res = curl_easy_perform(c.get());
    std::cout << "\n";
    
    if (res != CURLE_OK) {
        std::cerr << "Download failed: " << curl_easy_strerror(res) << "\n";
        return false;
    }
    
    // 如果可用，获取最终大小
    double dltotal = 0;
    curl_easy_getinfo(c.get(), CURLINFO_CONTENT_LENGTH_DOWNLOAD, &dltotal);
    long long total = static_cast<long long>(dltotal);
    
    std::cout << "Download complete: " << opt.out 
              << (total > 0 ? " (" + human_readable(total) + ")" : "") << "\n";
    return true;
}

int main(int argc, char** argv) {
    Options opt;

    if (argc < 2) { 
        print_help(); 
        return 0; 
    }

    // 解析命令行参数
    bool url_provided = false;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "-h" || a == "--help") { 
            print_help(); 
            return 0; 
        }
        else if (a == "-o" && i + 1 < argc) { 
            opt.out = argv[++i]; 
        }
        else if (a == "--force") { 
            opt.force = true; 
        }
        else if (a == "--insecure") { 
            opt.insecure = true; 
        }
        else if (a == "--cacert" && i + 1 < argc) { 
            opt.cacert = argv[++i]; 
        }
        else if (a == "--proxy" && i + 1 < argc) { 
            opt.proxy = argv[++i]; 
        }
        else if (a == "--threads" && i + 1 < argc) { 
            int val = std::atoi(argv[++i]);
            opt.threads = (val > 0) ? val : 1;
        }
        else if (a == "--retries" && i + 1 < argc) { 
            int val = std::atoi(argv[++i]);
            opt.retries = (val >= 0) ? val : 3;
        }
        else if (a == "--limit-rate" && i + 1 < argc) { 
            opt.limit_rate = parse_rate(argv[++i]); 
        }
        else if (is_url(a)) { 
            opt.url = a; 
            url_provided = true;
        }
        else {
            std::cerr << "Unknown argument: " << a << "\n";
            print_help();
            return 1;
        }
    }

    if (!url_provided) { 
        std::cerr << "Error: No URL provided.\n"; 
        return 1; 
    }
    
    // 自动输出文件名
    if (opt.out.empty()) {
        size_t pos = opt.url.find_last_of('/');
        if (pos == std::string::npos) {
            opt.out = "download.bin";
        } else {
            std::string filename = opt.url.substr(pos + 1);
            // 移除查询参数
            size_t qpos = filename.find('?');
            if (qpos != std::string::npos) {
                filename = filename.substr(0, qpos);
            }
            // 移除URL片段
            size_t fpos = filename.find('#');
            if (fpos != std::string::npos) {
                filename = filename.substr(0, fpos);
            }
            if (filename.empty()) {
                opt.out = "download.bin";
            } else {
                opt.out = filename;
            }
        }
    }

    // 检查文件是否存在
    std::error_code ec;
    if (fs::exists(opt.out, ec) && !opt.force) {
        std::cerr << "Error: File exists: " << opt.out << " (use --force to overwrite)\n";
        return 1;
    }

    // 初始化libcurl
    if (curl_global_init(CURL_GLOBAL_ALL) != CURLE_OK) {
        std::cerr << "Failed to initialize libcurl\n";
        return 1;
    }

    // 获取文件大小
    std::cout << "Checking server capabilities...\n";
    long long total = probe_file_size(opt.url, opt);
    
    if (total == -1) {
        // 服务器不支持Range或HEAD，使用单线程
        std::cout << "\nServer does not support required features for multi-threaded download.\n";
        std::cout << "Falling back to single-threaded mode...\n";
        bool success = download_single_thread(opt);
        curl_global_cleanup();
        return success ? 0 : 1;
    }
    
    // 支持多线程下载
    if (total > 0) {
        std::cout << "File size: " << human_readable(total) << " (" << total << " bytes)\n";
    } else {
        std::cout << "File size unknown, but server supports Range requests.\n";
        std::cout << "Using multi-threaded download with unknown total size.\n";
    }
    
    std::cout << "Threads: " << opt.threads << ", Output: " << opt.out << "\n";

    // 调整小文件的线程数
    if (total > 0 && total < opt.threads * 1024LL) {
        int old_threads = opt.threads;
        opt.threads = static_cast<int>(std::max<long long>(1, total / 1024LL));
        if (opt.threads < old_threads) {
            std::cout << "Adjusting thread count from " << old_threads << " to " 
                      << opt.threads << " (file is small)\n";
        }
    }

    // 设置部分
    int n = opt.threads;
    std::vector<Part> parts(n);
    
    if (total > 0) {
        // 知道总大小，平均分配
        long long part_size = total / n;
        long long remainder = total % n;
        long long current_start = 0;
        
        for (int i = 0; i < n; ++i) {
            parts[i].idx = i;
            parts[i].start = current_start;
            long long size = part_size + (i < remainder ? 1 : 0);
            parts[i].end = current_start + size - 1;
            current_start += size;
            
            parts[i].partfile = opt.out + ".part" + std::to_string(i);
            
            // 如果指定了--force，删除现有的部分文件
            if (opt.force && fs::exists(parts[i].partfile, ec)) {
                fs::remove(parts[i].partfile, ec);
            }
        }
    } else {
        // 不知道总大小，所有线程下载整个文件（用于合并）
        for (int i = 0; i < n; ++i) {
            parts[i].idx = i;
            parts[i].start = 0;
            parts[i].end = -1; // 表示未知结束
            parts[i].partfile = opt.out + ".part" + std::to_string(i);
            
            if (opt.force && fs::exists(parts[i].partfile, ec)) {
                fs::remove(parts[i].partfile, ec);
            }
        }
    }

    // 启动线程
    std::vector<std::thread> workers;
    workers.reserve(n);
    
    for (int i = 0; i < n; ++i) {
        workers.emplace_back(worker_func, std::cref(opt), &parts[i], total);
    }

    // 监控进度
    auto start_time = std::chrono::steady_clock::now();
    auto last_time = start_time;
    long long last_sum = 0;
    
    std::cout << "Starting download...\n";
    
    while (true) {
        std::this_thread::sleep_for(500ms);

        long long sum = 0;
        bool all_done = true;
        bool any_failed = false;
        
        // 汇总原子计数器
        for (const auto& p : parts) {
            sum += p.downloaded.load(std::memory_order_relaxed);
            if (!p.done) all_done = false;
            if (p.failed) any_failed = true;
        }
        
        // 如果知道总大小，将sum限制在total以内
        if (total > 0 && sum > total) sum = total;

        auto now = std::chrono::steady_clock::now();
        double dt = std::chrono::duration<double>(now - last_time).count();
        double speed = 0;
        if (dt > 0) speed = static_cast<double>(sum - last_sum) / dt;

        double elapsed = std::chrono::duration<double>(now - start_time).count();
        double eta = 0;
        if (total > 0) {
            eta = (speed > 0 && sum < total) ? (total - sum) / speed : 0.0;
        }
        double pct = (total > 0) ? static_cast<double>(sum) * 100.0 / total : 0.0;

        // 清除行并打印进度
        if (total > 0) {
            std::cout << "\r\033[K[" << std::fixed << std::setprecision(1) << pct << "%] "
                      << human_readable(sum) << "/" << human_readable(total) 
                      << " @ " << human_readable(static_cast<long long>(speed)) << "/s "
                      << "ETA: " << format_time(eta)
                      << (any_failed ? " (some parts failed)" : "") << std::flush;
        } else {
            std::cout << "\r\033[KDownloaded: " << human_readable(sum)
                      << " @ " << human_readable(static_cast<long long>(speed)) << "/s "
                      << (any_failed ? " (some parts failed)" : "") << std::flush;
        }

        last_time = now;
        last_sum = sum;

        if (all_done || any_failed) break;
    }
    
    std::cout << "\n";

    // 等待线程结束
    for (auto& t : workers) {
        if (t.joinable()) t.join();
    }

    // 检查结果
    bool any_failed = false;
    bool all_complete = true;
    
    for(const auto& p : parts) {
        if (p.failed) any_failed = true;
        if (!p.done) all_complete = false;
    }

    if (any_failed || !all_complete) {
        std::cerr << "Error: Some parts failed to download completely.\n";
        std::cerr << "You can resume by running the command again.\n";
        curl_global_cleanup();
        return 1;
    }

    // 合并部分
    if (merge_parts(opt.out, n, total)) {
        remove_parts(opt.out, n);
        auto end_time = std::chrono::steady_clock::now();
        double total_time = std::chrono::duration<double>(end_time - start_time).count();
        double avg_speed = 0;
        
        // 获取实际下载的总大小
        std::error_code size_ec;
        auto final_size = fs::file_size(opt.out, size_ec);
        if (!size_ec && final_size > 0) {
            avg_speed = final_size / total_time;
            std::cout << "Success: " << opt.out << " (" << human_readable(final_size) << ")\n";
            std::cout << "Time: " << format_time(total_time) 
                      << ", Average speed: " << human_readable(static_cast<long long>(avg_speed)) << "/s\n";
        } else {
            std::cout << "Success: " << opt.out << "\n";
            std::cout << "Time: " << format_time(total_time) << "\n";
        }
    } else {
        std::cerr << "Merge failed! Parts kept for resume.\n";
        curl_global_cleanup();
        return 1;
    }

    curl_global_cleanup();
    return 0;
}
