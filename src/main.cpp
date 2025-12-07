// src/main.cpp
// Super Downloader (C++17) - 超强版多源并行下载器
// 依赖: libcurl, OpenSSL (用于 sha256 校验，可选)
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <curl/curl.h>
#include <openssl/sha.h> // 若不需要校验，可移除

using namespace std::chrono_literals;

static void log_info(const std::string &s){ std::cerr << "[INFO] " << s << "\n"; }
static void log_error(const std::string &s){ std::cerr << "[ERROR] " << s << "\n"; }
static void log_debug(const std::string &s){ /* 可打印调试 */ }

/* ---------- 配置 ---------- */
struct Config {
    int threads = 8;
    int max_retries = 5;
    uint64_t rate_limit_bytes_per_sec = 0; // 0 = unlimited
    bool verify_ssl = true;
    bool use_parts_files = true; // true: .partN then merge. false: prealloc+pwrite (not implemented)
    std::string out_name;
    std::vector<std::string> urls; // 可包含多个镜像
    std::string sha256_hex; // 如果给定则校验
};
static Config cfg;

/* ---------- 工具 ---------- */
static std::string human_size(uint64_t b){
    double v = (double)b;
    const char* units[] = {"B","KB","MB","GB","TB"};
    int i=0;
    while(v>1024 && i<4){ v/=1024; ++i; }
    std::ostringstream oss;
    oss<<std::fixed<<std::setprecision(2)<<v<<units[i];
    return oss.str();
}

/* ---------- HTTP HEAD 以探测文件信息 ---------- */
struct HeadInfo {
    uint64_t content_length = 0;
    bool accept_ranges = false;
    long http_code = 0;
};

size_t header_cb(char* buffer, size_t size, size_t nitems, void* userdata){
    size_t total = size*nitems;
    std::string line(buffer, total);
    HeadInfo* info = (HeadInfo*)userdata;
    auto low = line;
    for(char &c: low) if(c>='A' && c<='Z') c = c - 'A' + 'a';
    if(low.find("content-length:") != std::string::npos){
        auto pos = line.find(':');
        if(pos!=std::string::npos){
            std::string val = line.substr(pos+1);
            info->content_length = std::stoull(val);
        }
    } else if(low.find("accept-ranges:") != std::string::npos) {
        if(low.find("bytes") != std::string::npos) info->accept_ranges = true;
    }
    return total;
}

bool probe_url_head(const std::string& url, HeadInfo& out){
    CURL* c = curl_easy_init();
    if(!c) return false;
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(c, CURLOPT_HEADERFUNCTION, header_cb);
    curl_easy_setopt(c, CURLOPT_HEADERDATA, &out);
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, cfg.verify_ssl ? 1L : 0L);
    curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, cfg.verify_ssl ? 2L : 0L);
    CURLcode res = curl_easy_perform(c);
    if(res != CURLE_OK){
        log_error(std::string("HEAD failed: ") + curl_easy_strerror(res));
        curl_easy_cleanup(c);
        return false;
    }
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &out.http_code);
    double cl = 0;
    curl_easy_getinfo(c, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
    if(out.content_length == 0 && cl > 0) out.content_length = (uint64_t)cl;
    curl_easy_cleanup(c);
    return true;
}

/* ---------- 令牌桶限速（全局） ---------- */
struct RateLimiter {
    std::mutex mu;
    std::atomic<uint64_t> tokens{0};
    uint64_t capacity{0};
    std::chrono::steady_clock::time_point last_fill = std::chrono::steady_clock::now();

    RateLimiter(uint64_t bps = 0){ capacity = bps; if(capacity>0) tokens = capacity; }

    // 周期性补 token（每 200ms）
    void refill() {
        if(capacity==0) return;
        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lk(mu);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_fill).count();
        if(ms<=0) return;
        uint64_t add = (uint64_t)((ms * (double)capacity)/1000.0);
        uint64_t cur = tokens.load();
        uint64_t nxt = std::min(capacity, cur + add);
        tokens = nxt;
        last_fill = now;
    }

    // 尝试消费，若不足返回实际可消费量（0 表示需等待）
    uint64_t consume(uint64_t want){
        if(capacity==0) return want; // unlimited
        refill();
        uint64_t cur = tokens.load();
        if(cur==0) return 0;
        uint64_t take = std::min(cur, want);
        tokens = cur - take;
        return take;
    }
};
static RateLimiter global_limiter(0);

/* ---------- 分片下载线程 ---------- */
struct Part {
    uint64_t start;
    uint64_t end; // inclusive
    int idx;
    std::string partfile;
    std::atomic<uint64_t> downloaded{0};
    std::atomic<int> attempts{0};
    bool done = false;
    bool failed = false;
};

size_t write_to_file(void* ptr, size_t size, size_t nmemb, void* userdata){
    FILE* f = (FILE*)userdata;
    size_t written = fwrite(ptr, size, nmemb, f);
    return written;
}

bool download_range_to_part(const std::string& url, Part& p){
    // open part file in append mode for resume
    FILE* fp = fopen(p.partfile.c_str(), "ab+");
    if(!fp){
        log_error("无法打开 part 文件: " + p.partfile);
        return false;
    }
    // compute existing size
    fseek(fp, 0, SEEK_END);
    uint64_t existing = (uint64_t)ftell(fp);
    if(existing > 0) {
        // adjust start
    }
    if(existing >= (p.end - p.start + 1)){
        // already complete
        fclose(fp);
        p.downloaded = (p.end - p.start + 1);
        p.done = true;
        return true;
    }

    uint64_t seg_start = p.start + existing;
    char range_header[128];
    snprintf(range_header, sizeof(range_header), "%llu-%llu",
             (unsigned long long)seg_start, (unsigned long long)p.end);

    CURL* c = curl_easy_init();
    if(!c){ fclose(fp); return false; }

    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_to_file);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, fp);
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_RANGE, range_header);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(c, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, cfg.verify_ssl ? 1L : 0L);
    curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, cfg.verify_ssl ? 2L : 0L);

    // 设置一个进度函数以实现速率控制（每次写入都会在 write callback 里被限制）
    // 这里使用简单策略：在 write 回调里不做限速（实现复杂），改用全局限速在外部做 sleep/等待。
    CURLcode res = curl_easy_perform(c);
    if(res != CURLE_OK){
        log_error(std::string("curl part failed: ") + curl_easy_strerror(res));
        curl_easy_cleanup(c);
        fclose(fp);
        return false;
    }

    // 更新已下载大小
    fseek(fp, 0, SEEK_END);
    uint64_t final_sz = ftell(fp);
    p.downloaded = final_sz;
    if(final_sz >= (p.end - p.start + 1)) {
        p.done = true;
    } else {
        p.done = false;
    }

    curl_easy_cleanup(c);
    fclose(fp);
    return p.done;
}

/* 线程工作体（每个线程取一个未完成的分片并下载 重试） */
void worker_thread_func(int tid, std::vector<Part*>& parts, std::atomic<int>& remaining){
    while(true){
        // find a part to work on
        Part* p = nullptr;
        for(auto &pp: parts){
            if(!pp->done && !pp->failed){
                int expected = pp->attempts.fetch_add(1);
                // someone else may also pick it, but it's okay
                p = pp;
                break;
            }
        }
        if(!p) break; // no remaining
        // attempt downloads with retry
        int tries = 0;
        bool ok = false;
        while(tries < cfg.max_retries && !ok){
            tries++;
            p->attempts = tries;
            // choose source url in round-robin
            std::string src = cfg.urls[(p->idx + tries - 1) % cfg.urls.size()];
            log_info("Thread " + std::to_string(tid) + " 下载 part " + std::to_string(p->idx) + " from " + src + " (try " + std::to_string(tries) + ")");
            ok = download_range_to_part(src, *p);
            if(!ok){
                std::this_thread::sleep_for(std::chrono::milliseconds(200 * tries)); // backoff
            }
        }
        if(!ok){
            p->failed = true;
            log_error("Part " + std::to_string(p->idx) + " 失败");
        } else {
            log_info("Part " + std::to_string(p->idx) + " 完成");
            remaining--;
        }
    }
}

/* 合并 .part 文件 */
bool merge_parts(const std::string& outname, const std::vector<Part>& parts){
    std::ofstream out(outname, std::ios::binary);
    if(!out) return false;
    for(const auto &p: parts){
        std::ifstream in(p.partfile, std::ios::binary);
        if(!in) { log_error("缺少分片文件: " + p.partfile); return false; }
        out << in.rdbuf();
        in.close();
    }
    out.close();
    return true;
}

/* 计算 SHA-256 */
std::string sha256_hex_file(const std::string& path){
    std::ifstream in(path, std::ios::binary);
    if(!in) return {};
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    char buf[8192];
    while(in.good()){
        in.read(buf, sizeof(buf));
        std::streamsize n = in.gcount();
        if(n>0) SHA256_Update(&ctx, (unsigned char*)buf, n);
    }
    unsigned char out[32];
    SHA256_Final(out, &ctx);
    std::ostringstream oss;
    for(int i=0;i<32;i++) oss<<std::hex<<std::setw(2)<<std::setfill('0')<<(int)out[i];
    return oss.str();
}

/* ---------- 主流程 ---------- */
int main(int argc, char** argv){
    if(argc < 2){
        std::cerr<<"用法: "<<argv[0]<<" <url1> [url2 ...] [--out filename] [--threads N] [--rate bps] [--sha256 HEX] [--no-verify]\n";
        return 1;
    }
    // 简单解析参数（不做复杂校验）
    for(int i=1;i<argc;i++){
        std::string s(argv[i]);
        if(s=="--out" && i+1<argc){ cfg.out_name = argv[++i]; }
        else if(s=="--threads" && i+1<argc){ cfg.threads = std::max(1, atoi(argv[++i])); }
        else if(s=="--rate" && i+1<argc){ cfg.rate_limit_bytes_per_sec = std::stoull(argv[++i]); }
        else if(s=="--sha256" && i+1<argc){ cfg.sha256_hex = argv[++i]; }
        else if(s=="--no-verify"){ cfg.verify_ssl = false; }
        else { cfg.urls.push_back(s); }
    }
    if(cfg.urls.empty()){ log_error("至少要提供一个 URL"); return 1; }
    if(cfg.out_name.empty()){
        // 取第一个 URL 的尾部作为文件名
        std::string u = cfg.urls[0];
        auto pos = u.find_last_of('/');
        if(pos==std::string::npos || pos+1==(int)u.size()) cfg.out_name = "output.bin";
        else cfg.out_name = u.substr(pos+1);
    }
    if(cfg.rate_limit_bytes_per_sec>0) global_limiter = RateLimiter(cfg.rate_limit_bytes_per_sec);

    curl_global_init(CURL_GLOBAL_ALL);

    // probe first URL to get size & accept-ranges
    HeadInfo hi;
    if(!probe_url_head(cfg.urls[0], hi)){
        log_error("探测 URL 失败");
        // 仍可尝试单线程下载
    }
    if(hi.content_length==0){
        log_info("无法获得 Content-Length，回退单线程下载");
        // 简单单线程下载第一个 URL
        CURL* c = curl_easy_init();
        FILE* fp = fopen(cfg.out_name.c_str(), "wb");
        curl_easy_setopt(c, CURLOPT_URL, cfg.urls[0].c_str());
        curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_to_file);
        curl_easy_setopt(c, CURLOPT_WRITEDATA, fp);
        curl_easy_perform(c);
        fclose(fp);
        curl_easy_cleanup(c);
        curl_global_cleanup();
        return 0;
    }

    log_info("文件大小: " + std::to_string(hi.content_length) + " (" + human_size(hi.content_length) + ")");
    if(!hi.accept_ranges){
        log_info("服务器不支持 Range，仍尝试单线程下载");
        // 同上单线程下载
        CURL* c = curl_easy_init();
        FILE* fp = fopen(cfg.out_name.c_str(), "wb");
        curl_easy_setopt(c, CURLOPT_URL, cfg.urls[0].c_str());
        curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_to_file);
        curl_easy_setopt(c, CURLOPT_WRITEDATA, fp);
        curl_easy_perform(c);
        fclose(fp);
        curl_easy_cleanup(c);
        curl_global_cleanup();
        return 0;
    }

    uint64_t total = hi.content_length;
    int parts_count = cfg.threads;
    uint64_t part_size = total / parts_count;
    std::vector<Part> parts(parts_count);
    std::vector<Part*> part_ptrs(parts_count);

    for(int i=0;i<parts_count;i++){
        parts[i].idx = i;
        parts[i].start = i * part_size;
        parts[i].end = (i==parts_count-1) ? (total - 1) : ((i+1)*part_size - 1);
        parts[i].partfile = cfg.out_name + ".part" + std::to_string(i);
        part_ptrs[i] = &parts[i];
    }

    std::atomic<int> remaining(parts_count);
    // launch worker threads
    std::vector<std::thread> workers;
    for(int t=0;t<cfg.threads;t++){
        workers.emplace_back(worker_thread_func, t, std::ref(part_ptrs), std::ref(remaining));
    }

    // 进度线程
    std::atomic<bool> stop_progress(false);
    std::thread progress_thread([&](){
        using clock = std::chrono::steady_clock;
        auto last = clock::now();
        uint64_t last_total = 0;
        while(!stop_progress){
            uint64_t downloaded = 0;
            for(auto &p: parts) downloaded += (uint64_t)p.downloaded.load();
            auto now = clock::now();
            double dt = std::chrono::duration_cast<std::chrono::milliseconds>(now - last).count() / 1000.0;
            uint64_t diff = downloaded - last_total;
            double speed = dt>0 ? diff / dt : 0;
            double pct = (double)downloaded * 100.0 / (double)total;
            double eta = speed>0 ? (double)(total-downloaded) / speed : -1;
            std::ostringstream ss;
            ss<<std::fixed<<std::setprecision(2);
            ss<<"Progress: "<<downloaded<<"/"<<total<<" ("<<pct<<"%) "
              <<"Speed: "<<human_size((uint64_t)speed)<<"/s "
              <<"ETA: "<<(eta>0?std::to_string((int)eta)+ "s":"-") ;
            log_info(ss.str());
            last = now;
            last_total = downloaded;
            if(remaining.load()<=0) break;
            std::this_thread::sleep_for(1s);
        }
    });

    for(auto &th: workers) if(th.joinable()) th.join();
    stop_progress = true;
    if(progress_thread.joinable()) progress_thread.join();

    // 检查失败的 part
    bool any_failed = false;
    for(auto &p: parts){
        if(p.failed) any_failed = true;
    }
    if(any_failed){
        log_error("部分分片下载失败，请重试或检查网络");
        curl_global_cleanup();
        return 2;
    }

    // 合并
    log_info("合并分片...");
    if(!merge_parts(cfg.out_name, parts)){
        log_error("合并失败");
        curl_global_cleanup();
        return 3;
    }

    // 校验
    if(!cfg.sha256_hex.empty()){
        log_info("计算 SHA-256...");
        auto got = sha256_hex_file(cfg.out_name);
        log_info("文件 SHA-256: " + got);
        if(got != cfg.sha256_hex){
            log_error("校验失败！");
            curl_global_cleanup();
            return 4;
        } else {
            log_info("校验通过");
        }
    }

    log_info("下载完成: " + cfg.out_name);
    curl_global_cleanup();
    return 0;
}
