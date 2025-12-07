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
    try {
        long v = std::stol(num);
        return v * mul;
    } catch(...) { return 0; }
}

static std::string human_readable(long long b) {
    if (b < 1024) return std::to_string(b) + " B";
    double v = (double)b;
    const char* u[] = {" B"," KB"," MB"," GB"," TB"};
    int i = 0;
    while (v >= 1024.0 && i < 4) { v /= 1024.0; ++i; }
    char buf[64];
    snprintf(buf, sizeof(buf), "%.2f%s", v, u[i]);
    return std::string(buf);
}

// --- Logic ---

struct WriteCtx {
    FILE* fp;
    std::atomic<long long>* downloaded_counter; // Only update this thread's counter
};

static size_t write_cb(void* ptr, size_t size, size_t nmemb, void* userdata) {
    WriteCtx* ctx = static_cast<WriteCtx*>(userdata);
    if (!ctx || !ctx->fp) return 0;
    
    size_t written = fwrite(ptr, size, nmemb, ctx->fp);
    long long bytes = (long long)written * (long long)size;
    
    if (ctx->downloaded_counter) {
        ctx->downloaded_counter->fetch_add(bytes, std::memory_order_relaxed);
    }
    return written;
}

struct Part {
    long long start;
    long long end;
    std::string partfile;
    std::atomic<long long> downloaded; // Bytes downloaded in this session (or detected on disk)
    std::atomic<bool> done;
    int idx;
    Part() : start(0), end(0), downloaded(0), done(false), idx(-1) {}
};

// Robust size probing: Try HEAD, if fail try Range GET
static long long probe_file_size(const std::string& url, const Options& opt) {
    CurlPtr c(curl_easy_init());
    if (!c) return -1;

    // Common setup
    auto setup = [&](CURL* h) {
        curl_easy_setopt(h, CURLOPT_URL, url.c_str());
        curl_easy_setopt(h, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(h, CURLOPT_FAILONERROR, 1L);
        if (opt.insecure) {
            curl_easy_setopt(h, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(h, CURLOPT_SSL_VERIFYHOST, 0L);
        }
        if (!opt.cacert.empty()) curl_easy_setopt(h, CURLOPT_CAINFO, opt.cacert.c_str());
        if (!opt.proxy.empty()) curl_easy_setopt(h, CURLOPT_PROXY, opt.proxy.c_str());
    };

    // 1. Try HEAD
    setup(c.get());
    curl_easy_setopt(c.get(), CURLOPT_NOBODY, 1L);
    CURLcode res = curl_easy_perform(c.get());
    
    double cl = 0;
    if (res == CURLE_OK) {
        curl_easy_getinfo(c.get(), CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
        if (cl > 0) return (long long)cl;
    }

    // 2. If HEAD fails or gives -1, try GET with Range: 0-0
    // Reset handle slightly or make new one. Let's reuse.
    curl_easy_reset(c.get());
    setup(c.get());
    curl_easy_setopt(c.get(), CURLOPT_NOBODY, 1L); // Still nobody, but we depend on headers
    curl_easy_setopt(c.get(), CURLOPT_RANGE, "0-0");
    
    res = curl_easy_perform(c.get());
    if (res == CURLE_OK) {
        curl_easy_getinfo(c.get(), CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
    }
    
    // Fallback: if cl is still invalid, we return -1 and trigger single thread mode.
    return -1;
}

static void worker_func(const Options& opt, Part* part) {
    // Open part file for appending
    FilePtr fp(fopen(part->partfile.c_str(), "ab+"));
    if (!fp) {
        std::cerr << "[Thread " << part->idx << "] Error opening file: " << part->partfile << "\n";
        return;
    }

    // Check existing size
    fseek(fp.get(), 0, SEEK_END);
    long long current_size = ftell(fp.get());
    
    // Update atomic counter to reflect what's already on disk (so global progress starts correctly)
    part->downloaded = current_size;

    long long part_len = part->end - part->start + 1;
    if (current_size >= part_len) {
        part->done = true;
        return; // Already done
    }

    CurlPtr c(curl_easy_init());
    if (!c) return;

    // Basic setup
    curl_easy_setopt(c.get(), CURLOPT_URL, opt.url.c_str());
    curl_easy_setopt(c.get(), CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c.get(), CURLOPT_FAILONERROR, 1L);
    if (opt.insecure) {
        curl_easy_setopt(c.get(), CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(c.get(), CURLOPT_SSL_VERIFYHOST, 0L);
    }
    if (!opt.cacert.empty()) curl_easy_setopt(c.get(), CURLOPT_CAINFO, opt.cacert.c_str());
    if (!opt.proxy.empty()) curl_easy_setopt(c.get(), CURLOPT_PROXY, opt.proxy.c_str());
    
    // Rate Limit (per thread estimate)
    if (opt.limit_rate > 0) {
        long long per_thread = opt.limit_rate / (opt.threads > 0 ? opt.threads : 1);
        curl_easy_setopt(c.get(), CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)per_thread);
    }

    WriteCtx ctx{fp.get(), &part->downloaded};
    curl_easy_setopt(c.get(), CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c.get(), CURLOPT_WRITEDATA, &ctx);

    int tries = 0;
    while (tries <= opt.retries) {
        // Recalculate range based on current file size (resume)
        // Ensure we are appending to the end
        if (fp) fflush(fp.get());
        fseek(fp.get(), 0, SEEK_END);
        long long now_have = ftell(fp.get());
        
        // Safety check: if we downloaded more than needed (rare but possible with restarts)
        if (now_have >= part_len) {
            part->done = true;
            break;
        }
        
        // Sync atomic with disk reality before request
        part->downloaded = now_have;

        long long req_start = part->start + now_have;
        char range_buf[64];
        snprintf(range_buf, sizeof(range_buf), "%lld-%lld", req_start, part->end);
        curl_easy_setopt(c.get(), CURLOPT_RANGE, range_buf);

        CURLcode res = curl_easy_perform(c.get());
        
        if (res == CURLE_OK) {
            part->done = true;
            break;
        } else {
            // Error handling
            long response_code;
            curl_easy_getinfo(c.get(), CURLINFO_RESPONSE_CODE, &response_code);
            // Don't retry on 4xx errors (except maybe 429/408, but simple here)
            if (response_code >= 400 && response_code < 500) {
                std::cerr << "[Thread " << part->idx << "] HTTP Error " << response_code << ", stopping.\n";
                break;
            }

            tries++;
            if (tries <= opt.retries) {
                std::cerr << "[Thread " << part->idx << "] Retry " << tries << "/" << opt.retries 
                          << " (" << curl_easy_strerror(res) << ")\n";
                std::this_thread::sleep_for(1s * tries);
            } else {
                std::cerr << "[Thread " << part->idx << "] Failed after retries.\n";
            }
        }
    }
}

static bool merge_parts(const std::string& out, int nparts) {
    std::cout << "\nMerging " << nparts << " parts into " << out << "...\n";
    // Use a larger buffer for merging to improve IO performance
    const size_t BUF_SIZE = 1024 * 1024; // 1MB
    std::unique_ptr<char[]> buffer(new char[BUF_SIZE]);

    std::ofstream ofs(out, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        std::cerr << "Error creating output file: " << out << "\n";
        return false;
    }

    for (int i = 0; i < nparts; ++i) {
        std::string p = out + ".part" + std::to_string(i);
        std::ifstream ifs(p, std::ios::binary);
        if (!ifs.is_open()) {
            std::cerr << "Error: Missing part file " << p << "\n";
            return false;
        }
        while (ifs) {
            ifs.read(buffer.get(), BUF_SIZE);
            std::streamsize cnt = ifs.gcount();
            if (cnt > 0) ofs.write(buffer.get(), cnt);
        }
        ifs.close();
    }
    return true;
}

static void remove_parts(const std::string& out, int nparts) {
    for (int i = 0; i < nparts; ++i) {
        std::string p = out + ".part" + std::to_string(i);
        std::error_code ec;
        fs::remove(p, ec);
    }
}

// Single thread fallback for servers not supporting ranges/HEAD
static int download_single_thread(const Options& opt) {
    std::cout << "Falling back to single-threaded download...\n";
    CurlPtr c(curl_easy_init());
    FilePtr fp(fopen(opt.out.c_str(), "wb"));
    
    if (!c || !fp) return 1;

    curl_easy_setopt(c.get(), CURLOPT_URL, opt.url.c_str());
    curl_easy_setopt(c.get(), CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c.get(), CURLOPT_WRITEFUNCTION, write_cb);
    WriteCtx ctx{fp.get(), nullptr}; // No progress tracking for simple fallback currently
    curl_easy_setopt(c.get(), CURLOPT_WRITEDATA, &ctx);
    
    if (opt.insecure) curl_easy_setopt(c.get(), CURLOPT_SSL_VERIFYPEER, 0L);
    if (!opt.cacert.empty()) curl_easy_setopt(c.get(), CURLOPT_CAINFO, opt.cacert.c_str());
    if (opt.limit_rate > 0) curl_easy_setopt(c.get(), CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)opt.limit_rate);

    CURLcode res = curl_easy_perform(c.get());
    if (res != CURLE_OK) {
        std::cerr << "Download failed: " << curl_easy_strerror(res) << "\n";
        return 1;
    }
    std::cout << "Download complete: " << opt.out << "\n";
    return 0;
}

int main(int argc, char** argv) {
    Options opt;

    if (argc < 2) { print_help(); return 0; }

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "-h" || a == "--help") { print_help(); return 0; }
        else if (a == "-o" && i + 1 < argc) { opt.out = argv[++i]; }
        else if (a == "--force") { opt.force = true; }
        else if (a == "--insecure") { opt.insecure = true; }
        else if (a == "--cacert" && i + 1 < argc) { opt.cacert = argv[++i]; }
        else if (a == "--proxy" && i + 1 < argc) { opt.proxy = argv[++i]; }
        // Note: NOMINMAX defined at the top prevents std::max conflict with Windows macros
        else if (a == "--threads" && i + 1 < argc) { opt.threads = std::max(1, std::atoi(argv[++i])); }
        else if (a == "--retries" && i + 1 < argc) { opt.retries = std::max(0, std::atoi(argv[++i])); }
        else if (a == "--limit-rate" && i + 1 < argc) { opt.limit_rate = parse_rate(argv[++i]); }
        else if (is_url(a)) { opt.url = a; }
        else {
            std::cerr << "Unknown argument: " << a << "\n";
            return 1;
        }
    }

    if (opt.url.empty()) { std::cerr << "Error: No URL provided.\n"; return 1; }
    
    // Auto output filename
    if (opt.out.empty()) {
        size_t pos = opt.url.find_last_of('/');
        size_t qpos = opt.url.find('?');
        if (qpos != std::string::npos && qpos > pos) {
             // Handle url like http://.../file.zip?token=123
             opt.out = opt.url.substr(pos + 1, qpos - pos - 1);
        } else {
             if (pos == std::string::npos || pos + 1 >= opt.url.size()) opt.out = "download.bin";
             else opt.out = opt.url.substr(pos + 1);
        }
    }

    if (fs::exists(opt.out) && !opt.force) {
        std::cerr << "Error: file exists: " << opt.out << " (use --force to overwrite)\n";
        return 1;
    }

    curl_global_init(CURL_GLOBAL_ALL);

    long long total = probe_file_size(opt.url, opt);
    
    if (total <= 0) {
        int ret = download_single_thread(opt);
        curl_global_cleanup();
        return ret;
    }

    std::cout << "File size: " << human_readable(total) << " (" << total << " bytes)\n";
    std::cout << "Threads: " << opt.threads << ", Output: " << opt.out << "\n";

    // Setup parts
    int n = opt.threads;
    std::vector<Part> parts(n);
    long long part_size = total / n;
    long long remainder = total % n;

    long long current_start = 0;
    for (int i = 0; i < n; ++i) {
        parts[i].idx = i;
        parts[i].start = current_start;
        long long size = part_size + (i < remainder ? 1 : 0); // Distribute remainder
        parts[i].end = current_start + size - 1;
        current_start += size;
        
        parts[i].partfile = opt.out + ".part" + std::to_string(i);
    }

    // Launch threads
    std::vector<std::thread> workers;
    for (int i = 0; i < n; ++i) {
        workers.emplace_back(worker_func, std::cref(opt), &parts[i]);
    }

    // Monitor
    auto last_time = std::chrono::steady_clock::now();
    long long last_sum = 0;

    while (true) {
        std::this_thread::sleep_for(500ms);

        long long sum = 0;
        bool all_done = true;
        
        // Sum atomic counters (fast, no disk IO)
        for (const auto& p : parts) {
            sum += p.downloaded.load(std::memory_order_relaxed);
            if (!p.done) all_done = false;
        }
        
        // Cap sum at total (in case of slight over-read or resume oddities)
        if (sum > total) sum = total;

        auto now = std::chrono::steady_clock::now();
        double dt = std::chrono::duration<double>(now - last_time).count();
        double speed = 0;
        if (dt > 0) speed = (double)(sum - last_sum) / dt;

        double eta = (speed > 0) ? (total - sum) / speed : 0.0;
        double pct = (total > 0) ? (double)sum * 100.0 / total : 0.0;

        // ANSI escape code \33[2K clears the entire line
        std::cout << "\33[2K\r"
                  << "[" << std::fixed << std::setprecision(1) << pct << "%] "
                  << human_readable(sum) << "/" << human_readable(total) 
                  << " @ " << human_readable((long long)speed) << "/s "
                  << " ETA: " << (int)eta << "s" << std::flush;

        last_time = now;
        last_sum = sum;

        if (all_done) break;
    }
    std::cout << "\n";

    // Join threads
    for (auto& t : workers) {
        if (t.joinable()) t.join();
    }

    // Check if parts are actually done (in case of thread error)
    bool sanity_check = true;
    for(const auto& p : parts) {
        if (!p.done) sanity_check = false;
    }

    if (!sanity_check) {
        std::cerr << "Error: Some parts failed to download. Resume by running the command again.\n";
        curl_global_cleanup();
        return 1;
    }

    if (merge_parts(opt.out, n)) {
        remove_parts(opt.out, n);
        std::cout << "Success: " << opt.out << "\n";
    } else {
        std::cerr << "Merge failed! Parts kept for resume.\n";
    }

    curl_global_cleanup();
    return 0;
}
