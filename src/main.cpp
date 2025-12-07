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
#include <iostream>
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
        "  --proxy PROXY     proxy URL (http://host:port, socks5://...)\n        --limit-rate N   global limit, supports K/M suffix (e.g. 100K, 5M)\n"
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

// write callback writes into FILE* and updates atomics
struct WriteCtx {
    FILE* fp;
    std::atomic<long long>* global_done;
    std::atomic<long long>* part_done;
};
static size_t write_cb(void* ptr, size_t size, size_t nmemb, void* userdata) {
    size_t written = 0;
    WriteCtx* ctx = static_cast<WriteCtx*>(userdata);
    if (!ctx || !ctx->fp) return 0;
    written = fwrite(ptr, size, nmemb, ctx->fp);
    long long bytes = (long long)written * (long long)size;
    if (ctx->global_done) ctx->global_done->fetch_add(bytes, std::memory_order_relaxed);
    if (ctx->part_done) ctx->part_done->fetch_add(bytes, std::memory_order_relaxed);
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
    Part(): start(0), end(0), downloaded(0), done(false), idx(-1) {}
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
    CURLcode res = curl_easy_perform(c);
    if (res != CURLE_OK) {
        curl_easy_cleanup(c);
        return -1;
    }
    double cl = 0;
    curl_easy_getinfo(c, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
    curl_easy_cleanup(c);
    if (cl < 0) return -1;
    return (long long)cl;
}

// worker thread function
static void worker_func(const Options& opt, Part* part, std::atomic<long long>* global_done) {
    // open file for update (binary)
    FILE* fp = nullptr;
    // create part file pointer to same final file, and write at offset part->start+existing
    fp = fopen(part->partfile.c_str(), "rb+"); // as we pre-create output with size, open in rb+
    if (!fp) {
        std::cerr << "Thread " << part->idx << " unable to open file: " << part->partfile << "\n";
        return;
    }

    // compute existing size in this region:
    // we can get existing bytes by checking how many bytes are non-zero? simpler: fseek end and see file size of whole file and deduce
    // But because we're writing into full final file, we determine existing bytes by scanning (costly).
    // Simpler: check from start to end by reading? Instead, open and fseek to end of file to get full file size and compute existing for this part:
    fseek(fp, 0, SEEK_END);
    long long fullsize = ftell(fp);
    long long part_len = part->end - part->start + 1;
    long long existing = 0;
    if (fullsize > 0) {
        // check how many bytes in this part are already non-zero? that's expensive.
        // We'll instead use a simple approach: use ftell and then read from file to count trailing zeros is expensive.
        // For robust resume we should maintain metadata; but to keep simple and reliable: check file size on disk:
        // If the final file was preallocated to fullsize, we cannot detect per-part existing reliably without metadata.
        // So we attempt: open part temp metadata file .partN to store how many downloaded.
    }
    // Instead of writing directly into final file, we'll use .partN files (safe) and resume by checking each .partN size.
    fclose(fp);

    // We'll implement thread to write to .partN file
    // Determine current existing bytes in .partN
    FILE* partfp = fopen(part->partfile.c_str(), "ab+");
    if (!partfp) {
        std::cerr << "Thread " << part->idx << " cannot open part file: " << part->partfile << "\n";
        return;
    }
    fseek(partfp, 0, SEEK_END);
    long long have = ftell(partfp);
    if (have >= (part->end - part->start + 1)) {
        // already complete
        part->downloaded = have;
        part->done = true;
        fclose(partfp);
        return;
    }

    long long real_start = part->start + have;
    if (real_start > part->end) {
        part->done = true;
        fclose(partfp);
        return;
    }

    // prepare CURL
    CURL* c = curl_easy_init();
    if (!c) {
        std::cerr << "Thread " << part->idx << " curl init fail\n";
        fclose(partfp);
        return;
    }
    // set url
    curl_easy_setopt(c, CURLOPT_URL, opt.url.c_str());
    // follow
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    // set range
    char range_buf[128];
    snprintf(range_buf, sizeof(range_buf), "%lld-%lld", real_start, part->end);
    curl_easy_setopt(c, CURLOPT_RANGE, range_buf);
    // write callback and context
    WriteCtx ctx;
    ctx.fp = partfp;
    ctx.global_done = global_done;
    ctx.part_done = &part->downloaded;
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &ctx);

    // options
    curl_easy_setopt(c, CURLOPT_FAILONERROR, 1L); // treat HTTP >=400 as error
    if (opt.insecure) {
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, 0L);
    }
    if (!opt.cacert.empty()) curl_easy_setopt(c, CURLOPT_CAINFO, opt.cacert.c_str());
    if (!opt.proxy.empty()) curl_easy_setopt(c, CURLOPT_PROXY, opt.proxy.c_str());
    if (opt.limit_rate > 0) {
        // divide roughly per-thread limit; note: this is per-handle limit
        long long per = opt.limit_rate /  (opt.threads > 0 ? opt.threads : 1);
        curl_easy_setopt(c, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)per);
    }

    // perform with retries
    int tries = 0;
    CURLcode res = CURLE_OK;
    while (tries <= opt.retries) {
        // ensure file pointer at end for append
        fseek(partfp, 0, SEEK_END);
        res = curl_easy_perform(c);
        if (res == CURLE_OK) {
            part->done = true;
            break;
        } else {
            std::cerr << "Thread " << part->idx << " curl error: " << curl_easy_strerror(res)
                      << " (retry " << tries << ")\n";
            tries++;
            // short backoff
            std::this_thread::sleep_for(500ms * tries);
            // reset for retry: set new range from current file size
            fseek(partfp, 0, SEEK_END);
            long long new_have = ftell(partfp);
            if (new_have >= (part->end - part->start + 1)) { part->done = true; break; }
            long long new_start = part->start + new_have;
            snprintf(range_buf, sizeof(range_buf), "%lld-%lld", new_start, part->end);
            curl_easy_setopt(c, CURLOPT_RANGE, range_buf);
            // continue loop
        }
    }

    // cleanup
    curl_easy_cleanup(c);
    fclose(partfp);
}

// merge .partN into final file
static bool merge_parts(const std::string& out, int nparts) {
    std::ofstream ofs(out, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) return false;
    const size_t BUF = 64*1024;
    std::vector<char> buffer(BUF);
    for (int i = 0; i < nparts; ++i) {
        std::string p = out + ".part" + std::to_string(i);
        std::ifstream ifs(p, std::ios::binary);
        if (!ifs.is_open()) {
            std::cerr << "Missing part: " << p << "\n";
            return false;
        }
        while (ifs.good()) {
            ifs.read(buffer.data(), (std::streamsize)buffer.size());
            std::streamsize r = ifs.gcount();
            if (r > 0) ofs.write(buffer.data(), r);
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
        fs::remove(p, ec);
    }
}

// human readable
static std::string human(long long b) {
    if (b < 1024) return std::to_string(b) + "B";
    double v = (double)b;
    const char* u[] = {"B","KB","MB","GB","TB"};
    int i = 0;
    while (v >= 1024.0 && i < 4) { v /= 1024.0; ++i; }
    char buf[64];
    snprintf(buf, sizeof(buf), "%.2f%s", v, u[i]);
    return std::string(buf);
}

int main(int argc, char** argv) {
    Options opt;

    if (argc < 2) { print_help(); return 0; }

    // parse args (simple parser)
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "-h" || a == "--help") { print_help(); return 0; }
        else if (a == "-o" && i + 1 < argc) { opt.out = argv[++i]; }
        else if (a == "--force") { opt.force = true; }
        else if (a == "--insecure") { opt.insecure = true; }
        else if (a == "--cacert" && i + 1 < argc) { opt.cacert = argv[++i]; }
        else if (a == "--proxy" && i + 1 < argc) { opt.proxy = argv[++i]; }
        else if (a == "--threads" && i + 1 < argc) { opt.threads = std::max(1, std::atoi(argv[++i])); }
        else if (a == "--retries" && i + 1 < argc) { opt.retries = std::max(0, std::atoi(argv[++i])); }
        else if (a == "--limit-rate" && i + 1 < argc) { opt.limit_rate = parse_rate(argv[++i]); }
        else if (is_url(a)) { opt.url = a; }
        else {
            std::cerr << "Unknown argument or invalid URL: " << a << "\n";
            return 1;
        }
    }

    if (!is_url(opt.url)) { std::cerr << "Error: URL must start with http:// or https://\n"; return 1; }

    // derive output filename
    if (opt.out.empty()) {
        size_t pos = opt.url.find_last_of('/');
        if (pos == std::string::npos || pos + 1 >= opt.url.size()) opt.out = "download.bin";
        else opt.out = opt.url.substr(pos + 1);
    }

    if (fs::exists(opt.out) && !opt.force) {
        std::cerr << "Error: file exists: " << opt.out << " (use --force to overwrite)\n";
        return 1;
    }

    curl_global_init(CURL_GLOBAL_ALL);

    long long total = probe_file_size(opt.url, opt);
    if (total <= 0) {
        std::cerr << "Failed to get content length or server does not provide it; falling back to single-thread streaming...\n";
        // fallback single-thread simple downloader
        CURL* c = curl_easy_init();
        if (!c) { curl_global_cleanup(); return 1; }
        FILE* outfp = fopen(opt.out.c_str(), "wb");
        if (!outfp) { std::cerr << "Cannot open output file\n"; curl_easy_cleanup(c); curl_global_cleanup(); return 1; }
        curl_easy_setopt(c, CURLOPT_URL, opt.url.c_str());
        curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_cb);
        WriteCtx ctx{outfp, nullptr, nullptr};
        curl_easy_setopt(c, CURLOPT_WRITEDATA, &ctx);
        if (opt.insecure) curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, 0L);
        if (!opt.cacert.empty()) curl_easy_setopt(c, CURLOPT_CAINFO, opt.cacert.c_str());
        if (!opt.proxy.empty()) curl_easy_setopt(c, CURLOPT_PROXY, opt.proxy.c_str());
        if (opt.limit_rate > 0) curl_easy_setopt(c, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)opt.limit_rate);
        CURLcode r = curl_easy_perform(c);
        fclose(outfp);
        if (r != CURLE_OK) {
            std::cerr << "Download failed: " << curl_easy_strerror(r) << "\n";
            curl_easy_cleanup(c);
            curl_global_cleanup();
            return 1;
        } else {
            std::cout << "Download finished: " << opt.out << "\n";
            curl_easy_cleanup(c);
            curl_global_cleanup();
            return 0;
        }
    }

    std::cout << "Total size: " << human(total) << " (" << total << " bytes)\n";
    // prepare part files
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
    }

    // global done counter
    std::atomic<long long> global_done(0);

    // launch workers
    std::vector<std::thread> workers;
    for (int i = 0; i < n; ++i) {
        workers.emplace_back(worker_func, std::cref(opt), &parts[i], &global_done);
    }

    // progress monitor
    long long last_done = 0;
    auto last_time = std::chrono::steady_clock::now();
    while (true) {
        // sum downloaded : read sizes of part files for robust tally
        long long sum = 0;
        for (int i = 0; i < n; ++i) {
            std::error_code ec;
            auto sz = fs::file_size(parts[i].partfile, ec);
            if (!ec) {
                sum += (long long)sz;
                // update part.downloaded for display
                parts[i].downloaded = (long long)sz;
            }
        }
        // compute speed
        auto now = std::chrono::steady_clock::now();
        double dt = std::chrono::duration<double>(now - last_time).count();
        long long diff = sum - last_done;
        double speed = dt > 0 ? (double)diff / dt : 0.0;
        double eta = speed > 0 ? (double)(total - sum) / speed : -1.0;

        // print progress
        double pct = (double)sum * 100.0 / (double)total;
        std::cout << "\rProgress: " << std::fixed << std::setprecision(2) << pct << "% "
                  << "(" << human(sum) << " / " << human(total) << ") "
                  << "Speed: " << human((long long)speed) << "/s "
                  << "ETA: " << (eta >= 0 ? std::to_string((int)eta) + "s" : "-") << "   " << std::flush;

        last_done = sum;
        last_time = now;

        // check finished
        bool all_done = true;
        for (int i = 0; i < n; ++i) {
            if (!parts[i].done) { all_done = false; break; }
        }
        if (all_done) break;
        std::this_thread::sleep_for(500ms);
    }
    std::cout << "\nMerging parts...\n";

    bool ok = merge_parts(opt.out, n);
    if (!ok) {
        std::cerr << "Merge failed\n";
        curl_global_cleanup();
        return 1;
    }

    // cleanup part files
    remove_parts(opt.out, n);
    std::cout << "Download complete: " << opt.out << "\n";

    curl_global_cleanup();
    return 0;
}
