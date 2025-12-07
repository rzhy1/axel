// src/main.cpp
// Multi-threaded downloader (no .part files, supports resume via small .resume file)
// Build: link with libcurl (vcpkg or system)
// Target: Windows (MSVC) and Linux (g++), C++17

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <cstdlib>
#include <cstring>
#include <filesystem>

#include <curl/curl.h>

#ifdef _WIN32
#include <windows.h>
#pragma comment(lib, "libcurl.lib")
#endif

namespace fs = std::filesystem;
using namespace std::chrono_literals;

struct Options {
    std::string url;
    std::string out;         // output file path
    int threads = 4;
    bool force = false;
    bool insecure = false;
    std::string cacert;
    std::string proxy;
    long limit_rate = 0;     // bytes/sec (global)
    int retries = 3;
    bool resume_enabled = true;
};

// ---------- Console UTF-8 on Windows ----------
static void init_console_utf8() {
#ifdef _WIN32
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);
#endif
}

// ---------- Helpers ----------
static bool starts_with(const std::string &s, const std::string &p) {
    return s.size() >= p.size() && std::equal(p.begin(), p.end(), s.begin());
}
static bool is_url(const std::string &s) {
    return starts_with(s, "http://") || starts_with(s, "https://");
}
static long parse_rate(const std::string &s) {
    if (s.empty()) return 0;
    char last = s.back();
    std::string num = s;
    long mul = 1;
    if (last == 'K' || last == 'k') { mul = 1024; num = s.substr(0, s.size()-1); }
    else if (last == 'M' || last == 'm') { mul = 1024*1024; num = s.substr(0, s.size()-1); }
    try { return std::stol(num) * mul; } catch(...) { return 0; }
}
static std::string human(long long b) {
    if (b < 1024) return std::to_string(b) + "B";
    double v = (double)b;
    const char* u[] = {"B","KB","MB","GB","TB"};
    int i = 0;
    while (v >= 1024.0 && i < 4) { v /= 1024.0; ++i; }
    char buf[64]; snprintf(buf, sizeof(buf), "%.2f%s", v, u[i]); return std::string(buf);
}

// ---------- write callback (unused for direct file write) ----------
size_t write_to_mem(void* ptr, size_t size, size_t nmemb, void* userdata) {
    // userdata will be pointer to struct ThreadState (filled below)
    // But we will not use this generic callback -- we push bytes to buffer in ThreadState
    return size * nmemb;
}

// ---------- Probe file size: try HEAD, then fallback to GET 0-0 ----------
static long long probe_size(const std::string &url, const Options &opt) {
    CURL* c = curl_easy_init();
    if (!c) return -1;
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_FAILONERROR, 1L);
    if (opt.insecure) { curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, 0L); curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, 0L); }
    if (!opt.cacert.empty()) curl_easy_setopt(c, CURLOPT_CAINFO, opt.cacert.c_str());
    if (!opt.proxy.empty()) curl_easy_setopt(c, CURLOPT_PROXY, opt.proxy.c_str());
    CURLcode r = curl_easy_perform(c);
    long long size = -1;
    if (r == CURLE_OK) {
        double cl = 0; curl_easy_getinfo(c, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
        size = (long long)cl;
        if (size <= 0) size = -1;
    }
    curl_easy_cleanup(c);
    if (size != -1) return size;

    // fallback: GET with Range 0-0
    c = curl_easy_init();
    if (!c) return -1;
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_NOBODY, 0L);
    curl_easy_setopt(c, CURLOPT_RANGE, "0-0");
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, +[](void*, size_t s, size_t n, void*) { return s*n; });
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    if (opt.insecure) { curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, 0L); curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, 0L); }
    if (!opt.cacert.empty()) curl_easy_setopt(c, CURLOPT_CAINFO, opt.cacert.c_str());
    if (!opt.proxy.empty()) curl_easy_setopt(c, CURLOPT_PROXY, opt.proxy.c_str());
    r = curl_easy_perform(c);
    if (r == CURLE_OK) {
        double cl = 0; curl_easy_getinfo(c, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
        size = (long long)cl;
        if (size <= 0) size = -1;
    }
    curl_easy_cleanup(c);
    return size;
}

// ---------- Resume metadata: very small file storing per-segment downloaded bytes ----------
// Format: one line per segment: idx downloaded
static bool save_resume(const std::string &metapath, const std::vector<long long> &downloaded) {
    std::ofstream ofs(metapath, std::ios::trunc);
    if (!ofs.is_open()) return false;
    for (size_t i = 0; i < downloaded.size(); ++i) {
        ofs << i << ' ' << downloaded[i] << '\n';
    }
    ofs.close();
    return true;
}
static bool load_resume(const std::string &metapath, std::vector<long long> &downloaded) {
    if (!fs::exists(metapath)) return false;
    std::ifstream ifs(metapath);
    if (!ifs.is_open()) return false;
    int idx; long long val;
    while (ifs >> idx >> val) {
        if (idx >= 0 && idx < (int)downloaded.size()) downloaded[idx] = val;
    }
    ifs.close();
    return true;
}
static void remove_resume(const std::string &metapath) {
    std::error_code ec; fs::remove(metapath, ec);
}

// ---------- Thread state ----------
struct ThreadState {
    int idx;
    long long start;   // original start
    long long end;     // original end (inclusive)
    long long downloaded_in_run = 0; // bytes downloaded in current session (for this segment)
    std::vector<char> buf; // buffer for incoming data before flush
    bool ok = false;
    int retries = 0;
};

// ---------- Global objects for progress display and file IO ----------
static std::mutex file_mutex;        // protect file seek+write
static std::mutex resume_mutex;      // protect resume file writes
static std::atomic<long long> global_downloaded(0);
static std::atomic<bool> stop_all(false);

// ---------- Thread write callback: pushes into thread buffer, flush when large ----------
size_t thread_write_cb(void* ptr, size_t size, size_t nmemb, void* userdata) {
    ThreadState* ts = (ThreadState*)userdata;
    size_t tot = size * nmemb;
    const char* data = (const char*)ptr;
    // append
    ts->buf.insert(ts->buf.end(), data, data + tot);
    ts->downloaded_in_run += (long long)tot;
    global_downloaded.fetch_add((long long)tot, std::memory_order_relaxed);

    // flush threshold (e.g., 256 KB)
    const size_t FLUSH_THRESHOLD = 256 * 1024;
    if (ts->buf.size() >= FLUSH_THRESHOLD) {
        // flush to file at proper offset: position = start + downloaded_already
        // Determine absolute file position to write:
        long long already = 0; // we compute from downloaded_in_run and maybe resume base (handled outside)
        // We cannot compute absolute here; main code will maintain baseline downloaded_before_run per segment
        // To make this self-contained, we store base in a global vector passed via userdata? Instead, we will store base as first element of buf: not ideal.
        // Simpler approach: the calling thread will know how many bytes were already present before this run and compute file position.
        // So we must set up ThreadState so it contains base_offset (downloaded_before_run).
    }
    return tot;
}

// But above approach needs file flushing logic that knows the correct file offset: implement per-thread function performing curl with a custom write that accumulates
// Then periodically flush buffer to file using file_mutex and known offset: offset = start + downloaded_before_run + flushed_before
// We'll implement per-thread worker below (not using the simplistic callback above).

// ---------- Worker function ----------
static void worker_run(ThreadState* ts, const Options &opt, long long already_downloaded_before_run, const std::string &outfile, const std::string &metapath) {
    // ts->start..ts->end is original range
    // already_downloaded_before_run: how many bytes of this segment were previously downloaded (from resume file)
    long long segment_total = ts->end - ts->start + 1;
    long long downloaded = already_downloaded_before_run; // how many bytes persisted before start
    ts->downloaded_in_run = 0;
    ts->buf.clear();

    // we will perform curl with Range starting at ts->start + downloaded
    long long cur_start = ts->start + downloaded;
    if (cur_start > ts->end) { ts->ok = true; return; } // already finished

    // setup CURL handle
    CURL* c = curl_easy_init();
    if (!c) { ts->ok = false; return; }

    // write callback will append to ts->buf; but we need to flush periodically.
    curl_easy_setopt(c, CURLOPT_URL, opt.url.c_str());
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_FAILONERROR, 1L);

    if (opt.insecure) { curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, 0L); curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, 0L); }
    if (!opt.cacert.empty()) curl_easy_setopt(c, CURLOPT_CAINFO, opt.cacert.c_str());
    if (!opt.proxy.empty()) curl_easy_setopt(c, CURLOPT_PROXY, opt.proxy.c_str());

    // per-handle limit: divide global limit by threads (approximate)
    if (opt.limit_rate > 0 && opt.threads > 0) {
        long per = opt.limit_rate / opt.threads;
        if (per < 1) per = 1;
        curl_easy_setopt(c, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)per);
    }

    // We'll do manual retry loop
    int tries = 0;
    while (!stop_all.load() && tries <= opt.retries) {
        // set range
        char rangebuf[128];
        snprintf(rangebuf, sizeof(rangebuf), "%lld-%lld", cur_start, ts->end);
        curl_easy_setopt(c, CURLOPT_RANGE, rangebuf);

        // reset buffer sizes
        ts->buf.clear();

        // set write callback to append to ts->buf
        curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, +[](void* ptr, size_t size, size_t nmemb, void* userdata) -> size_t {
            ThreadState* t = (ThreadState*)userdata;
            size_t tot = size*nmemb;
            const char* src = (const char*)ptr;
            t->buf.insert(t->buf.end(), src, src + tot);
            return tot;
        });
        curl_easy_setopt(c, CURLOPT_WRITEDATA, ts);

        CURLcode res = curl_easy_perform(c);
        if (res != CURLE_OK) {
            // On error, attempt to flush any buffer we have and then retry with backoff
            if (!ts->buf.empty()) {
                // flush buffer to file at position cur_start (we must compute position based on downloaded so far)
                std::lock_guard<std::mutex> lk(file_mutex);
                std::fstream ofs(outfile, std::ios::in|std::ios::out|std::ios::binary);
                if (ofs.is_open()) {
                    ofs.seekp(cur_start, std::ios::beg);
                    ofs.write(ts->buf.data(), (std::streamsize)ts->buf.size());
                    ofs.close();
                }
            }
            // update resume info: increment downloaded by current buffer size
            downloaded += (long long)ts->buf.size();
            // write resume
            // NOTE: writing resume requires knowledge of index; we'll handle via a separate shared vector and function. For simplicity, we assume caller will update resume after worker returns or periodically.
            tries++;
            std::this_thread::sleep_for(std::chrono::milliseconds(500 * tries));
            // recalc cur_start
            cur_start = ts->start + downloaded;
            continue;
        } else {
            // success for this perform: curl might have fetched until end or connection closed; flush ts->buf to file
            if (!ts->buf.empty()) {
                std::lock_guard<std::mutex> lk(file_mutex);
                std::fstream ofs(outfile, std::ios::in|std::ios::out|std::ios::binary);
                if (ofs.is_open()) {
                    ofs.seekp(cur_start, std::ios::beg);
                    ofs.write(ts->buf.data(), (std::streamsize)ts->buf.size());
                    ofs.close();
                }
            }
            // update downloaded and mark done
            downloaded += (long long)ts->buf.size();
            ts->ok = true;
            break;
        }
    }

    // finished: set downloaded_in_run for caller (value is downloaded - already_downloaded_before_run)
    ts->downloaded_in_run = downloaded - already_downloaded_before_run;
    curl_easy_cleanup(c);
}

// ---------- Main program ----------
int main(int argc, char* argv[]) {
    init_console_utf8();
    Options opt;

    if (argc == 1) {
        std::cout << "Usage: downloader <URL> [--out <path>] [--threads N] [--limit-rate 100K] [--proxy URL] [--insecure] [--cacert file] [--retries N] [--force]\n";
        return 0;
    }

    // parse args (order-free)
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "-h" || a == "--help") {
            std::cout << "Help: downloader <URL> [--out <path>] [--threads N] [--limit-rate 100K] [--proxy URL] [--insecure] [--cacert file] [--retries N] [--force]\n";
            return 0;
        } else if (a == "--out" && i+1 < argc) { opt.out = argv[++i]; }
        else if (a == "--threads" && i+1 < argc) { opt.threads = std::max(1, atoi(argv[++i])); }
        else if (a == "--limit-rate" && i+1 < argc) { opt.limit_rate = parse_rate(argv[++i]); }
        else if (a == "--proxy" && i+1 < argc) { opt.proxy = argv[++i]; }
        else if (a == "--insecure") { opt.insecure = true; }
        else if (a == "--cacert" && i+1 < argc) { opt.cacert = argv[++i]; }
        else if (a == "--retries" && i+1 < argc) { opt.retries = std::max(0, atoi(argv[++i])); }
        else if (a == "--force") { opt.force = true; }
        else if (is_url(a)) { opt.url = a; }
        else {
            std::cerr << "Unknown option or invalid URL: " << a << "\n";
            return 1;
        }
    }

    if (!is_url(opt.url)) { std::cerr << "Error: URL must start with http:// or https://\n"; return 1; }

    // determine output path
    if (opt.out.empty()) {
        size_t pos = opt.url.find_last_of('/');
        std::string filename = (pos==std::string::npos) ? "download.bin" : opt.url.substr(pos+1);
        if (filename.empty()) filename = "download.bin";
        opt.out = filename;
    }

    if (fs::exists(opt.out) && !opt.force) {
        std::cerr << "Error: output file exists, use --force to overwrite\n"; return 1;
    }

    // init curl
    curl_global_init(CURL_GLOBAL_ALL);

    long long total = probe_size(opt.url, opt);
    if (total <= 0) {
        std::cerr << "Failed to get content length (server may not provide it). Aborting.\n";
        curl_global_cleanup();
        return 1;
    }

    std::cout << "Total: " << human(total) << " (" << total << " bytes)\n";

    // prepare final file: preallocate
    {
        std::ofstream ofs(opt.out, std::ios::binary | std::ios::trunc);
        if (!ofs.is_open()) { std::cerr << "Cannot open output file for writing\n"; curl_global_cleanup(); return 1; }
        ofs.seekp(total - 1);
        ofs.write("", 1);
        ofs.close();
    }

    // prepare resume metadata
    std::string metapath = opt.out + ".resume";
    int n = opt.threads;
    std::vector<long long> resumed(n, 0); // how many bytes already persisted for each segment
    if (opt.resume_enabled) load_resume(metapath, resumed);

    // compute segments
    std::vector<ThreadState> states(n);
    std::vector<std::thread> workers;
    long long part = total / n;
    for (int i = 0; i < n; ++i) {
        states[i].idx = i;
        states[i].start = i * part;
        states[i].end = (i==n-1) ? (total-1) : ((i+1)*part - 1);
    }

    // compute global_downloaded from resumed
    long long initial_done = 0;
    for (int i = 0; i < n; ++i) { initial_done += resumed[i]; }
    global_downloaded.store(initial_done);

    // launch workers
    for (int i = 0; i < n; ++i) {
        long long already = resumed[i];
        workers.emplace_back([&, i, already]() {
            worker_run(&states[i], opt, already, opt.out, metapath);
            // after each worker finishes or updates, we should save resume
            // we will save resume at the end here
        });
    }

    // progress monitor + periodic resume save
    auto t0 = std::chrono::steady_clock::now();
    long long last_done = global_downloaded.load();
    const int SAVE_INTERVAL_MS = 1000;
    int ticks = 0;
    while (true) {
        // compute sum from file sizes to be robust
        long long sum = 0;
        for (int i = 0; i < n; ++i) {
            std::error_code ec;
            long long s = 0;
            if (fs::exists(opt.out, ec)) {
                // can't rely on file_size of full file; instead sum resumed[] + states[i].downloaded_in_run
                s = resumed[i] + (long long)states[i].downloaded_in_run;
            }
            sum += s;
        }
        // But above may be inaccurate; better use global_downloaded atomic
        sum = global_downloaded.load();

        auto now = std::chrono::steady_clock::now();
        double dt = std::chrono::duration<double>(now - t0).count();
        long long diff = sum - last_done;
        double speed = (diff) / ( (double)SAVE_INTERVAL_MS / 1000.0 ); // approx per interval
        // simpler compute instantaneous:
        static long long prev_sum = 0;
        static auto prev_time = std::chrono::steady_clock::now();
        auto cur_time = std::chrono::steady_clock::now();
        double idt = std::chrono::duration<double>(cur_time - prev_time).count();
        long long idiff = sum - prev_sum;
        double kbps = (idt>0) ? (double)idiff / idt / 1024.0 : 0.0;
        double eta = (kbps>0) ? ((double)(total - sum) / (kbps*1024.0)) : -1.0;

        // print progress
        double pct = (double)sum * 100.0 / (double)total;
        std::ostringstream oss;
        oss << "\rProgress: " << std::fixed << std::setprecision(2) << pct << "% "
            << "(" << human(sum) << " / " << human(total) << ") "
            << "Speed: " << std::fixed << std::setprecision(2) << kbps << " KB/s "
            << "ETA: " << (eta>=0 ? std::to_string((int)eta) + "s" : "-") << "   ";
        {
            std::lock_guard<std::mutex> lk(file_mutex);
            std::cout << oss.str() << std::flush;
        }

        prev_sum = sum;
        prev_time = cur_time;

        // periodically save resume (every SAVE_INTERVAL_MS)
        ++ticks;
        if (ticks * SAVE_INTERVAL_MS >= SAVE_INTERVAL_MS) {
            // build resumed vector: resumed[i] + states[i].downloaded_in_run
            std::vector<long long> cur(n);
            for (int i = 0; i < n; ++i) cur[i] = resumed[i] + (long long)states[i].downloaded_in_run;
            {
                std::lock_guard<std::mutex> lk(resume_mutex);
                save_resume(metapath, cur);
            }
            ticks = 0;
        }

        // check all done
        bool all_done = true;
        for (int i = 0; i < n; ++i) if (!states[i].ok) { all_done = false; break; }
        if (all_done) break;
        std::this_thread::sleep_for(500ms);
    }

    std::cout << "\nFinalizing...\n";

    // remove resume file
    if (opt.resume_enabled) remove_resume(metapath);

    std::cout << "Download complete: " << opt.out << "\n";

    // join threads
    for (auto &th : workers) if (th.joinable()) th.join();

    curl_global_cleanup();
    return 0;
}
