#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <fstream>
#include <filesystem>
#include <mutex>

#include <windows.h>
#include <curl/curl.h>

#pragma comment(lib, "libcurl.lib")

using namespace std;

static mutex g_console_mutex;

// ------ UTF-8 控制台 ------
void init_console_utf8() {
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);
}

// ------ 输出 ------
void print_error(const string& msg) {
    lock_guard<mutex> lock(g_console_mutex);
    cerr << "[错误] " << msg << endl;
}

void print_info(const string& msg) {
    lock_guard<mutex> lock(g_console_mutex);
    cout << msg << endl;
}

// ------ URL 检查（兼容 C++11） ------
bool starts_with(const string& s, const string& prefix) {
    if (s.size() < prefix.size()) return false;
    return equal(prefix.begin(), prefix.end(), s.begin());
}

bool is_valid_url(const string& url) {
    return starts_with(url, "http://") || starts_with(url, "https://");
}

// ------ 写入回调 ------
size_t write_callback(void* ptr, size_t size, size_t nmemb, void* userdata) {
    FILE* fp = (FILE*)userdata;
    return fwrite(ptr, size, nmemb, fp);
}

// ------ 获取文件大小 ------
long long get_file_size(const string& url, CURL* curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_HEADER, 0L);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) return -1;

    double cl = 0;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);

    return (long long)cl;
}

// ------ 分段下载 ------
bool download_range(const string& url, const string& tmpfile,
                    long long start, long long end,
                    bool skipSSL, const string& cafile, const string& proxy) {

    CURL* curl = curl_easy_init();
    if (!curl) return false;

    FILE* fp = fopen(tmpfile.c_str(), "wb");
    if (!fp) return false;

    char range_str[100];
    sprintf(range_str, "%lld-%lld", start, end);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_RANGE, range_str);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);

    if (!proxy.empty())
        curl_easy_setopt(curl, CURLOPT_PROXY, proxy.c_str());

    if (skipSSL) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    } else if (!cafile.empty()) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, cafile.c_str());
    }

    CURLcode res = curl_easy_perform(curl);
    fclose(fp);
    curl_easy_cleanup(curl);

    return res == CURLE_OK;
}

// ------ 合并分段 ------
bool merge_parts(const string& outfile, int threads) {
    ofstream out(outfile, ios::binary);
    if (!out) return false;

    for (int i = 0; i < threads; i++) {
        string part = outfile + ".part" + to_string(i);
        ifstream in(part, ios::binary);
        if (!in) return false;

        out << in.rdbuf();
        in.close();
        filesystem::remove(part);
    }
    out.close();
    return true;
}

// ------ 自动文件名 ------
string auto_filename(const string& url) {
    size_t pos = url.find_last_of('/');
    if (pos == string::npos) return "download.bin";
    string name = url.substr(pos + 1);
    if (name.empty()) return "download.bin";
    return name;
}

// ------------------ 主程序 ------------------
int main(int argc, char* argv[]) {
    init_console_utf8();

    if (argc < 2) {
        cout <<
            "用法: downloader <URL> [选项]\n"
            "--threads N     设置线程数（默认 4）\n"
            "--out DIR       指定下载目录\n"
            "--force         覆盖已存在的文件\n"
            "--proxy URL     设置代理\n"
            "--skip-ssl      跳过 SSL 验证\n"
            "--ca FILE       指定 CA 文件\n";
        return 0;
    }

    string url = argv[1];
    if (!is_valid_url(url)) {
        print_error("URL 无效，必须以 http:// 或 https:// 开头");
        return 1;
    }

    int threads = 4;
    bool force = false;
    string outdir = ".";
    string proxy = "";
    bool skipSSL = false;
    string cafile = "";

    for (int i = 2; i < argc; i++) {
        string arg = argv[i];

        if (arg == "--threads" && i + 1 < argc) threads = stoi(argv[++i]);
        else if (arg == "--out" && i + 1 < argc) outdir = argv[++i];
        else if (arg == "--force") force = true;
        else if (arg == "--proxy" && i + 1 < argc) proxy = argv[++i];
        else if (arg == "--skip-ssl") skipSSL = true;
        else if (arg == "--ca" && i + 1 < argc) cafile = argv[++i];
        else {
            print_error("未知选项: " + arg);
            return 1;
        }
    }

    filesystem::create_directories(outdir);

    string filename = auto_filename(url);
    string outfile = (filesystem::path(outdir) / filename).string();

    if (filesystem::exists(outfile) && !force) {
        print_error("文件已存在，使用 --force 才能覆盖");
        return 1;
    }

    curl_global_init(CURL_GLOBAL_ALL);
    CURL* curl = curl_easy_init();

    long long totalsize = get_file_size(url, curl);
    if (totalsize <= 0) {
        print_error("无法获取文件大小，服务器可能不支持 HEAD");
        curl_easy_cleanup(curl);
        return 1;
    }

    char size_msg[100];
    sprintf(size_msg, "文件大小: %lld 字节", totalsize);
    print_info(size_msg);

    long long part = totalsize / threads;
    vector<thread> pool;

    for (int i = 0; i < threads; i++) {
        long long start = part * i;
        long long end = (i == threads - 1) ? totalsize - 1 : (start + part - 1);
        string tmp = outfile + ".part" + to_string(i);

        pool.emplace_back([&, i, start, end, tmp]() {
            bool ok = download_range(url, tmp, start, end, skipSSL, cafile, proxy);
            if (!ok) {
                print_error("线程 " + to_string(i) + " 下载失败");
            }
        });
    }

    for (auto& t : pool) t.join();

    print_info("正在合并文件...");
    if (!merge_parts(outfile, threads)) {
        print_error("合并失败");
        return 1;
    }

    print_info("下载完成！");
    curl_easy_cleanup(curl);
    curl_global_cleanup();

    return 0;
}
