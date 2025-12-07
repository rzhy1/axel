#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <fstream>
#include <filesystem>

#include <windows.h>
#include <curl/curl.h>

#pragma comment(lib, "libcurl.lib")

using namespace std;

static mutex g_out;

// ---------------- 控制台 UTF-8 ----------------
void init_console() {
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);
}

// ---------------- 安全打印 ----------------
void print_err(const string& s) {
    lock_guard<mutex> lock(g_out);
    cerr << "[错误] " << s << endl;
}
void print_info(const string& s) {
    lock_guard<mutex> lock(g_out);
    cout << s << endl;
}

// ---------------- 字符串前缀判断 ----------------
bool starts_with(const string& s, const string& p) {
    if (s.size() < p.size()) return false;
    return s.compare(0, p.size(), p) == 0;
}

bool is_url(const string& s) {
    return starts_with(s, "http://") || starts_with(s, "https://");
}

// ---------------- curl 写入 ----------------
size_t write_cb(void* ptr, size_t size, size_t nm, void* userdata) {
    FILE* fp = (FILE*)userdata;
    return fwrite(ptr, size, nm, fp);
}

// ---------------- 尝试用 HEAD 获取大小 ----------------
long long try_head_size(CURL* curl, const string& url) {
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_HEADER, 0L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    CURLcode r = curl_easy_perform(curl);
    if (r != CURLE_OK) return -1;

    double cl = -1;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
    return (long long)cl;
}

// ---------------- HEAD 不行 → fallback GET 获取大小 ----------------
long long try_get_size_with_range(CURL* curl, const string& url) {
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
    curl_easy_setopt(curl, CURLOPT_RANGE, "0-0");
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, nullptr);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, +[](void*, size_t s, size_t n, void*) { return s*n; });
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    CURLcode r = curl_easy_perform(curl);
    if (r != CURLE_OK) return -1;

    double cl = -1;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
    return (long long)cl;
}

// ---------------- 获取大小（自动 fallback） ----------------
long long get_total_size(const string& url, bool skipSSL, const string& cafile, const string& proxy) {
    CURL* curl = curl_easy_init();
    if (!curl) return -1;

    if (!proxy.empty()) curl_easy_setopt(curl, CURLOPT_PROXY, proxy.c_str());
    if (skipSSL) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    } else if (!cafile.empty()) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, cafile.c_str());
    }

    long long size1 = try_head_size(curl, url);
    if (size1 > 0) {
        curl_easy_cleanup(curl);
        return size1;
    }

    long long size2 = try_get_size_with_range(curl, url);
    curl_easy_cleanup(curl);
    return size2;
}

// ---------------- 分段下载 ----------------
bool download_range(const string& url, const string& tmp, long long start, long long end,
                    bool skipSSL, const string& cafile, const string& proxy) {

    CURL* curl = curl_easy_init();
    if (!curl) return false;

    char range[100];
    sprintf(range, "%lld-%lld", start, end);

    FILE* fp = fopen(tmp.c_str(), "wb");
    if (!fp) { curl_easy_cleanup(curl); return false; }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_RANGE, range);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    if (!proxy.empty()) curl_easy_setopt(curl, CURLOPT_PROXY, proxy.c_str());
    if (skipSSL) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    } else if (!cafile.empty()) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, cafile.c_str());
    }

    CURLcode r = curl_easy_perform(curl);
    fclose(fp);
    curl_easy_cleanup(curl);
    return r == CURLE_OK;
}

// ---------------- 合并 ----------------
bool merge_parts(const string& out, int threads) {
    ofstream o(out, ios::binary);
    if (!o) return false;

    for (int i = 0; i < threads; i++) {
        string p = out + ".part" + to_string(i);
        ifstream in(p, ios::binary);
        if (!in) return false;
        o << in.rdbuf();
        in.close();
        filesystem::remove(p);
    }
    return true;
}

// ---------------- 自动文件名 ----------------
string get_filename(const string& url) {
    auto pos = url.find_last_of('/');
    if (pos == string::npos) return "download.bin";
    auto n = url.substr(pos + 1);
    return n.empty() ? "download.bin" : n;
}

// ==========================================================
//                        主函数
// ==========================================================
int main(int argc, char* argv[]) {
    init_console();

    if (argc == 1) {
        print_info("用法: a <URL> [选项]");
        return 0;
    }

    // ----------- 参数解析（不会把选项当 URL） ------------
    string url = "";
    int threads = 4;
    bool force = false;
    string proxy = "";
    bool skipSSL = false;
    string cafile = "";
    string outdir = ".";

    for (int i = 1; i < argc; i++) {
        string arg = argv[i];

        if (arg == "-h" || arg == "--help") {
            print_info("使用说明:\n"
                       "  a <URL> [选项]\n"
                       "选项:\n"
                       "  --threads N   线程数\n"
                       "  --force       覆盖文件\n"
                       "  --proxy URL   设置代理\n"
                       "  --skip-ssl    跳过 SSL 验证\n"
                       "  --ca FILE     指定 CA 文件\n");
            return 0;
        }

        if (arg == "-v" || arg == "--version") {
            print_info("super_downloader v1.0");
            return 0;
        }

        if (arg == "--threads" && i + 1 < argc) { threads = atoi(argv[++i]); continue; }
        if (arg == "--force") { force = true; continue; }
        if (arg == "--proxy" && i + 1 < argc) { proxy = argv[++i]; continue; }
        if (arg == "--skip-ssl") { skipSSL = true; continue; }
        if (arg == "--ca" && i + 1 < argc) { cafile = argv[++i]; continue; }

        // ---------- 只有不是选项的才是 URL ----------
        if (is_url(arg)) url = arg;
    }

    // ---------- 没有 URL ----------
    if (url.empty()) {
        print_err("缺少 URL");
        return 1;
    }

    // 检查 URL
    if (!is_url(url)) {
        print_err("URL 无效，必须 http:// 或 https://");
        return 1;
    }

    // ---------- 获取大小（支持跳过 HEAD） ----------
    curl_global_init(CURL_GLOBAL_ALL);
    long long size = get_total_size(url, skipSSL, cafile, proxy);

    if (size <= 0) {
        print_err("无法获取文件大小（HEAD/Range 都失败）");
        return 1;
    }

    print_info("文件大小: " + to_string(size) + " 字节");

    // ---------- 输出文件 ----------
    string name = get_filename(url);
    string outfile = (filesystem::path(outdir) / name).string();

    if (filesystem::exists(outfile) && !force) {
        print_err("文件已存在，使用 --force 覆盖");
        return 1;
    }

    // ---------- 多线程 ----------
    long long part = size / threads;
    vector<thread> pool;

    for (int i = 0; i < threads; i++) {
        long long s = i * part;
        long long e = (i == threads - 1) ? size - 1 : s + part - 1;
        string tmp = outfile + ".part" + to_string(i);

        pool.emplace_back([&, s, e, tmp, i]() {
            bool ok = download_range(url, tmp, s, e, skipSSL, cafile, proxy);
            if (!ok)
                print_err("线程 " + to_string(i) + " 下载失败");
        });
    }

    for (auto& t : pool) t.join();

    print_info("合并文件中...");
    if (!merge_parts(outfile, threads)) {
        print_err("合并失败");
        return 1;
    }

    print_info("下载完成！");
    curl_global_cleanup();
    return 0;
}
