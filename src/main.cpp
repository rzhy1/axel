// src/main.cpp
// Multi-threaded downloader using libcurl (Windows/Unix)
// Features: argument parsing, URL validation, multi-segment Range downloads,
// per-thread resume to final file, global progress/speed/ETA, limit-rate,
// proxy, --insecure, --cacert, --force, --threads, --retries.
//
// Build with: link to libcurl (vcpkg or system libcurl)

#include <curl/curl.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

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

static void print_help() {
    std::cout <<
        "SuperDownloader (libcurl)\n"
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

// write callback writes into FILE* and updates atomics
struct WriteCtx {
    FILE* fp;
    std::atomic<long long>* global_done;
    std::atomic<long long>* part_done;
};

static size_t write_cb(void* ptr, size_t size, size_t nmemb, void* userdata) {
    WriteCtx* ctx = static_cast<WriteCtx*>(userdata);
    if (!ctx || !ctx->fp) return 0;
    
    size_t total_size = size * nmemb;
    size_t written = fwrite(ptr, 1, total_size, ctx->fp);
    
    if (ctx->global_done) ctx->global_done->fetch_add(written, std::memory_order_relaxed);
    if (ctx->part_done) ctx->part_done->fetch_add(written, std::memory_order_relaxed);
    
    return written;
}

// Per-part info
struct Part {
    long long start;
    long long end;      // inclusive
    std::string partfile;
    std::atomic<long long> downloaded; // how many bytes this thread has downloaded in this run
    std::atomic<bool> done;
    int idx;
    
    Part() : start(0), end(0), downloaded(0), done(false), idx(-1) {}
};

// get content-length via HEAD (returns -1 on fail)
static long long probe_file_size(const std::string& url, const Options& opt) {
    CURL* c = curl_easy_init();
    if (!c) return -1;
    
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_FAILONERROR, 1L);
    
    if (opt.insecure) {
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, 0L);
    }
    
    if (!opt.cacert.empty()) curl_easy_setopt(c, CURLOPT_CAINFO, opt.cacert.c_str());
    if (!opt.proxy.empty()) curl_easy_setopt(c, CURLOPT_PROXY, opt.proxy.c_str());
    
    // Set timeout options
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, 30L);
    
    CURLcode res = curl_easy_perform(c);
    if (res != CURLE_OK) {
        curl_easy_cleanup(c);
        return -1;
    }
    
    double cl = 0;
    curl_easy_getinfo(c, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
    curl_easy_cleanup(c);
    
    if (cl < 0) return -1;
    return static_cast<long long>(cl);
}

// RAII wrapper for FILE*
class FileHandle {
public:
    FileHandle(const std::string& filename, const char* mode) {
        fp = fopen(filename.c_str(), mode);
    }
    
    ~FileHandle() {
        if (fp) fclose(fp);
    }
    
    FILE* get() const { return fp; }
    explicit operator bool() const { return fp != nullptr; }
    
private:
    FILE* fp = nullptr;
};

// Set common CURL options
static void set_curl_common_options(CURL* curl, const Options& opt) {
    if (opt.insecure) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
    
    if (!opt.cacert.empty()) curl_easy_setopt(curl, CURLOPT_CAINFO, opt.cacert.c_str());
    if (!opt.proxy.empty()) curl_easy_setopt(curl, CURLOPT_PROXY, opt.proxy.c_str());
    
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    
    // Timeout settings
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1024L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 30L);
}

// worker thread function
static void worker_func(const Options& opt, Part* part, std::atomic<long long>* global_done) {
    // Open part file for append
    FileHandle partfp(part->partfile, "ab+");
    if (!partfp) {
        std::cerr << "Thread " << part->idx << " cannot open part file: " << part->partfile << "\n";
        return;
    }
    
    // Get current size of part file
    fseek(partfp.get(), 0, SEEK_END);
    long long have = ftell(partfp.get());
    
    // Check if already complete
    long long part_size = part->end - part->start + 1;
    if (have >= part_size) {
        part->downloaded = have;
        part->done = true;
        return;
    }
    
    long long real_start = part->start + have;
    if (real_start > part->end) {
        part->done = true;
        return;
    }
    
    // Prepare CURL
    CURL* c = curl_easy_init();
    if (!c) {
        std::cerr << "Thread " << part->idx << " curl init fail\n";
        return;
    }
    
    // Set URL
    curl_easy_setopt(c, CURLOPT_URL, opt.url.c_str());
    
    // Set range
    char range_buf[128];
    snprintf(range_buf, sizeof(range_buf), "%lld-%lld", real_start, part->end);
    curl_easy_setopt(c, CURLOPT_RANGE, range_buf);
    
    // Write callback and context
    WriteCtx ctx;
    ctx.fp = partfp.get();
    ctx.global_done = global_done;
    ctx.part_done = &part->downloaded;
    
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &ctx);
    
    // Set common options
    set_curl_common_options(c, opt);
    
    // Rate limiting
    if (opt.limit_rate > 0 && opt.threads > 0) {
        long long per = opt.limit_rate / opt.threads;
        if (per > 0) {
            curl_easy_setopt(c, CURLOPT_MAX_RECV_SPEED_LARGE, static_cast<curl_off_t>(per));
        }
    }
    
    // Perform with retries
    int tries = 0;
    CURLcode res = CURLE_OK;
    
    while (tries <= opt.retries) {
        // Ensure file pointer at end for append
        fseek(partfp.get(), 0, SEEK_END);
        res = curl_easy_perform(c);
        
        if (res == CURLE_OK) {
            // Verify we downloaded the complete range
            fseek(partfp.get(), 0, SEEK_END);
            long long new_have = ftell(partfp.get());
            if (new_have >= part_size) {
                part->done = true;
                break;
            } else {
                // Partial download, continue with retry
                tries++;
            }
        } else {
            std::cerr << "Thread " << part->idx << " curl error: " << curl_easy_strerror(res)
                      << " (retry " << tries << "/" << opt.retries << ")\n";
            tries++;
            
            // Exponential backoff
            std::this_thread::sleep_for(std::chrono::milliseconds(500 * (1 << (tries - 1))));
            
            // Update range for retry
            fseek(partfp.get(), 0, SEEK_END);
            have = ftell(partfp.get());
            if (have >= part_size) {
                part->done = true;
                break;
            }
            real_start = part->start + have;
            snprintf(range_buf, sizeof(range_buf), "%lld-%lld", real_start, part->end);
            curl_easy_setopt(c, CURLOPT_RANGE, range_buf);
        }
    }
    
    if (tries > opt.retries && res != CURLE_OK) {
        std::cerr << "Thread " << part->idx << " failed after " << opt.retries << " retries\n";
    }
    
    curl_easy_cleanup(c);
}

// merge .partN into final file
static bool merge_parts(const std::string& out, int nparts) {
    std::ofstream ofs(out, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        std::cerr << "Cannot open output file for merging: " << out << "\n";
        return false;
    }
    
    const size_t BUF_SIZE = 64 * 1024;
    std::vector<char> buffer(BUF_SIZE);
    
    for (int i = 0; i < nparts; ++i) {
        std::string p = out + ".part" + std::to_string(i);
        std::ifstream ifs(p, std::ios::binary);
        
        if (!ifs.is_open()) {
            std::cerr << "Missing part file: " << p << "\n";
            return false;
        }
        
        while (ifs.good()) {
            ifs.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            std::streamsize r = ifs.gcount();
            if (r > 0) {
                ofs.write(buffer.data(), r);
                if (!ofs.good()) {
                    std::cerr << "Write error while merging part " << i << "\n";
                    return false;
                }
            }
        }
        
        if (ifs.bad()) {
            std::cerr << "Read error from part file " << i << "\n";
            return false;
        }
        
        ifs.close();
    }
    
    ofs.close();
    return true;
}

static void remove_parts(const std::string& out, int nparts) {
    for (int i = 0; i < nparts; ++i) {
        std::string p = out + ".part" + std::to_string(i);
        std::error_code ec;
        if (fs::exists(p)) {
            fs::remove(p, ec);
            if (ec) {
                std::cerr << "Warning: could not remove part file " << p << ": " << ec.message() << "\n";
            }
        }
    }
}

// human readable
static std::string human(long long b) {
    if (b < 1024) return std::to_string(b) + "B";
    
    double v = static_cast<double>(b);
    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    int i = 0;
    
    while (v >= 1024.0 && i < 4) {
        v /= 1024.0;
        ++i;
    }
    
    char buf[64];
    snprintf(buf, sizeof(buf), "%.2f%s", v, units[i]);
    return std::string(buf);
}

// Single-threaded download for unsupported servers
static bool download_single(const Options& opt) {
    std::cout << "Downloading with single thread...\n";
    
    CURL* c = curl_easy_init();
    if (!c) {
        std::cerr << "Failed to initialize CURL\n";
        return false;
    }
    
    FileHandle outfp(opt.out.c_str(), "wb");
    if (!outfp) {
        std::cerr << "Cannot open output file: " << opt.out << "\n";
        curl_easy_cleanup(c);
        return false;
    }
    
    curl_easy_setopt(c, CURLOPT_URL, opt.url.c_str());
    set_curl_common_options(c, opt);
    
    WriteCtx ctx{outfp.get(), nullptr, nullptr};
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &ctx);
    
    if (opt.limit_rate > 0) {
        curl_easy_setopt(c, CURLOPT_MAX_RECV_SPEED_LARGE, static_cast<curl_off_t>(opt.limit_rate));
    }
    
    // Progress callback for single-thread download
    curl_easy_setopt(c, CURLOPT_NOPROGRESS, 0L);
    auto progress_cb = [](void* clientp, curl_off_t dltotal, curl_off_t dlnow,
                          curl_off_t ultotal, curl_off_t ulnow) -> int {
        if (dltotal > 0) {
            double percent = (dlnow * 100.0) / dltotal;
            std::cout << "\rProgress: " << std::fixed << std::setprecision(2) << percent << "% "
                      << "(" << human(dlnow) << " / " << human(dltotal) << ")" << std::flush;
        }
        return 0;
    };
    curl_easy_setopt(c, CURLOPT_XFERINFOFUNCTION, progress_cb);
    
    CURLcode r = curl_easy_perform(c);
    
    if (r != CURLE_OK) {
        std::cerr << "\nDownload failed: " << curl_easy_strerror(r) << "\n";
        curl_easy_cleanup(c);
        return false;
    }
    
    std::cout << "\nDownload finished: " << opt.out << "\n";
    curl_easy_cleanup(c);
    return true;
}

int main(int argc, char** argv) {
    Options opt;
    
    if (argc < 2) {
        print_help();
        return 0;
    }
    
    // Parse args
    bool url_provided = false;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "-h" || a == "--help") {
            print_help();
            return 0;
        } else if (a == "-o" && i + 1 < argc) {
            opt.out = argv[++i];
        } else if (a == "--force") {
            opt.force = true;
        } else if (a == "--insecure") {
            opt.insecure = true;
        } else if (a == "--cacert" && i + 1 < argc) {
            opt.cacert = argv[++i];
        } else if (a == "--proxy" && i + 1 < argc) {
            opt.proxy = argv[++i];
        } else if (a == "--threads" && i + 1 < argc) {
            opt.threads = std::max(1, std::atoi(argv[++i]));
        } else if (a == "--retries" && i + 1 < argc) {
            opt.retries = std::max(0, std::atoi(argv[++i]));
        } else if (a == "--limit-rate" && i + 1 < argc) {
            opt.limit_rate = parse_rate(argv[++i]);
        } else if (is_url(a)) {
            opt.url = a;
            url_provided = true;
        } else {
            std::cerr << "Unknown argument: " << a << "\n";
            print_help();
            return 1;
        }
    }
    
    if (!url_provided) {
        std::cerr << "Error: URL is required\n";
        print_help();
        return 1;
    }
    
    if (!is_url(opt.url)) {
        std::cerr << "Error: URL must start with http:// or https://\n";
        return 1;
    }
    
    // Derive output filename
    if (opt.out.empty()) {
        size_t pos = opt.url.find_last_of('/');
        if (pos == std::string::npos || pos + 1 >= opt.url.size()) {
            opt.out = "download.bin";
        } else {
            std::string filename = opt.url.substr(pos + 1);
            // Remove query parameters if any
            size_t qpos = filename.find('?');
            if (qpos != std::string::npos) {
                filename = filename.substr(0, qpos);
            }
            if (filename.empty()) {
                opt.out = "download.bin";
            } else {
                opt.out = filename;
            }
        }
    }
    
    // Check if file exists
    if (fs::exists(opt.out) && !opt.force) {
        std::cerr << "Error: file exists: " << opt.out << " (use --force to overwrite)\n";
        return 1;
    }
    
    // Initialize libcurl
    if (curl_global_init(CURL_GLOBAL_ALL) != CURLE_OK) {
        std::cerr << "Failed to initialize libcurl\n";
        return 1;
    }
    
    // Get file size
    long long total = probe_file_size(opt.url, opt);
    
    if (total <= 0) {
        std::cout << "Server does not provide content-length or HEAD request failed.\n";
        std::cout << "Falling back to single-thread streaming...\n";
        
        bool success = download_single(opt);
        curl_global_cleanup();
        return success ? 0 : 1;
    }
    
    std::cout << "Total size: " << human(total) << " (" << total << " bytes)\n";
    
    // Adjust thread count if file is too small
    if (total < opt.threads * 1024LL) { // Less than threads * 1KB
        int old_threads = opt.threads;
        opt.threads = std::max(1, static_cast<int>(total / 1024LL));
        if (opt.threads < old_threads) {
            std::cout << "Adjusting thread count from " << old_threads << " to " << opt.threads
                      << " (file is small)\n";
        }
    }
    
    // Prepare part files
    int n = opt.threads;
    std::vector<Part> parts(n);
    long long part_size = total / n;
    
    for (int i = 0; i < n; ++i) {
        parts[i].start = i * part_size;
        parts[i].end = (i == n - 1) ? total - 1 : ((i + 1) * part_size - 1);
        parts[i].partfile = opt.out + ".part" + std::to_string(i);
        parts[i].downloaded = 0;
        parts[i].done = false;
        parts[i].idx = i;
        
        // Remove existing part files if --force is specified
        if (opt.force && fs::exists(parts[i].partfile)) {
            std::error_code ec;
            fs::remove(parts[i].partfile, ec);
        }
    }
    
    // Global done counter
    std::atomic<long long> global_done(0);
    
    // Launch workers
    std::vector<std::thread> workers;
    workers.reserve(n);
    
    for (int i = 0; i < n; ++i) {
        workers.emplace_back(worker_func, std::cref(opt), &parts[i], &global_done);
    }
    
    // Progress monitor
    long long last_done = 0;
    auto last_time = std::chrono::steady_clock::now();
    bool display_header = false;
    
    while (true) {
        // Sum downloaded from part files
        long long sum = 0;
        bool all_done = true;
        
        for (int i = 0; i < n; ++i) {
            std::error_code ec;
            if (fs::exists(parts[i].partfile)) {
                auto sz = fs::file_size(parts[i].partfile, ec);
                if (!ec) {
                    sum += static_cast<long long>(sz);
                }
            }
            
            if (!parts[i].done) {
                all_done = false;
            }
        }
        
        // Compute speed and ETA
        auto now = std::chrono::steady_clock::now();
        double dt = std::chrono::duration<double>(now - last_time).count();
        
        if (dt >= 0.5) { // Update display at most every 0.5s
            long long diff = sum - last_done;
            double speed = dt > 0 ? static_cast<double>(diff) / dt : 0.0;
            double eta = (speed > 0 && sum < total) ? static_cast<double>(total - sum) / speed : 0.0;
            
            // Display progress
            double pct = (total > 0) ? (static_cast<double>(sum) * 100.0 / static_cast<double>(total)) : 0.0;
            
            if (!display_header) {
                std::cout << "\n";
                display_header = true;
            }
            
            std::cout << "\rProgress: " << std::fixed << std::setprecision(2) << pct << "% "
                      << "(" << human(sum) << " / " << human(total) << ") "
                      << "Speed: " << human(static_cast<long long>(speed)) << "/s "
                      << "ETA: ";
            
            if (eta > 0) {
                if (eta < 60) {
                    std::cout << static_cast<int>(eta) << "s";
                } else if (eta < 3600) {
                    std::cout << static_cast<int>(eta / 60) << "m" << static_cast<int>(eta) % 60 << "s";
                } else {
                    std::cout << static_cast<int>(eta / 3600) << "h" << static_cast<int>((eta / 60)) % 60 << "m";
                }
            } else {
                std::cout << "-";
            }
            
            std::cout << "   " << std::flush;
            
            last_done = sum;
            last_time = now;
        }
        
        if (all_done) break;
        
        std::this_thread::sleep_for(100ms);
    }
    
    // Join all worker threads
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    
    std::cout << "\nMerging parts...\n";
    
    bool ok = merge_parts(opt.out, n);
    if (!ok) {
        std::cerr << "Merge failed\n";
        remove_parts(opt.out, n);
        curl_global_cleanup();
        return 1;
    }
    
    // Cleanup part files
    remove_parts(opt.out, n);
    
    // Verify final file size
    std::error_code ec;
    auto final_size = fs::file_size(opt.out, ec);
    if (!ec && final_size == static_cast<uintmax_t>(total)) {
        std::cout << "Download complete: " << opt.out << " (" << human(total) << ")\n";
    } else {
        std::cout << "Download completed but size verification failed\n";
        std::cout << "Expected: " << total << " bytes, Got: " << final_size << " bytes\n";
    }
    
    curl_global_cleanup();
    return 0;
}
