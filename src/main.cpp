#include <curl/curl.h>
#include <cstdio>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <filesystem>

using namespace std;
namespace fs = std::filesystem;

struct Options {
    string url;
    string output;
    bool force = false;
    bool insecure = false;
    string proxy;
    string cacert;
    long limit_rate = 0;

    int threads = 4;
};

void print_help() {
    cout <<
        "SimpleDownloader (libcurl)\n"
        "用法： downloader [选项] <URL>\n\n"
        "选项：\n"
        "  -o <文件>            指定输出文件名\n"
        "  --force              强制覆盖已有文件\n"
        "  --threads N          启用 N 线程下载（默认 4）\n"
        "  --insecure           跳过 SSL 证书验证\n"
        "  --cacert FILE        指定 CA 证书文件\n"
        "  --proxy PROXY        使用代理，例如 http://127.0.0.1:7890\n"
        "  --limit-rate N       限速（字节/秒），支持 100K、5M 等\n"
        "  -h, --help           显示帮助\n";
}

bool is_url(const string& s) {
    return s.rfind("http://", 0) == 0 || s.rfind("https://", 0) == 0;
}

long parse_rate(const string& s) {
    long base = stol(s);
    if (s.back() == 'K' || s.back() == 'k') return base * 1024;
    if (s.back() == 'M' || s.back() == 'm') return base * 1024 * 1024;
    return base;
}

size_t write_data(void* ptr, size_t size, size_t nmemb, void* stream) {
    FILE* fp = (FILE*)stream;
    return fwrite(ptr, size, nmemb, fp);
}

struct SegmentTask {
    long start;
    long end;
    long downloaded;
    bool ok = false;
};

bool download_range(const Options& opt, SegmentTask &seg, const string &filename) {
    CURL* curl = curl_easy_init();
    if (!curl) return false;

    FILE* fp = fopen(filename.c_str(), "rb+");
    fseek(fp, seg.start, SEEK_SET);

    string range = to_string(seg.start) + "-" + to_string(seg.end);

    curl_easy_setopt(curl, CURLOPT_URL, opt.url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
    curl_easy_setopt(curl, CURLOPT_RANGE, range.c_str());

    if (opt.proxy.size()) curl_easy_setopt(curl, CURLOPT_PROXY, opt.proxy.c_str());
    if (opt.insecure) curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    if (opt.cacert.size()) curl_easy_setopt(curl, CURLOPT_CAINFO, opt.cacert.c_str());
    if (opt.limit_rate > 0) curl_easy_setopt(curl, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)opt.limit_rate);

    CURLcode res = curl_easy_perform(curl);
    fclose(fp);

    if (res != CURLE_OK) {
        cerr << "[线程] 下载分段失败: " << curl_easy_strerror(res) << endl;
        curl_easy_cleanup(curl);
        return false;
    }

    seg.ok = true;
    curl_easy_cleanup(curl);
    return true;
}

bool get_file_size(const Options& opt, long &size) {
    CURL* curl = curl_easy_init();
    if (!curl) return false;

    curl_easy_setopt(curl, CURLOPT_URL, opt.url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_HEADER, 1L);

    if (opt.insecure) curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    if (opt.proxy.size()) curl_easy_setopt(curl, CURLOPT_PROXY, opt.proxy.c_str());
    if (opt.cacert.size()) curl_easy_setopt(curl, CURLOPT_CAINFO, opt.cacert.c_str());

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        cerr << "无法获取文件大小: " << curl_easy_strerror(res) << endl;
        curl_easy_cleanup(curl);
        return false;
    }

    double cl = 0;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);

    if (cl <= 0) {
        cerr << "服务器未提供 Content-Length，无法进行分段下载\n";
        curl_easy_cleanup(curl);
        return false;
    }

    size = (long)cl;
    curl_easy_cleanup(curl);
    return true;
}

string human_size(long bytes) {
    const char* units[] = {"B", "K", "M", "G"};
    int i = 0;
    double size = bytes;
    while (size > 1024 && i < 3) {
        size /= 1024;
        i++;
    }
    ostringstream out;
    out << fixed << setprecision(1) << size << units[i];
    return out.str();
}

void print_progress(long total, vector<SegmentTask>& segs) {
    long done = 0;
    for (auto &s : segs) done += s.downloaded;

    double pct = done * 100.0 / total;

    cout << "\r进度: "
         << fixed << setprecision(1) << pct << "% ("
         << human_size(done) << "/" << human_size(total) << ")"
         << flush;
}

int main(int argc, char* argv[]) {
    if (argc == 1) {
        print_help();
        return 0;
    }

    Options opt;
    vector<string> args;

    for (int i = 1; i < argc; i++) args.push_back(argv[i]);

    for (size_t i = 0; i < args.size(); i++) {
        string a = args[i];

        if (a == "-h" || a == "--help") {
            print_help();
            return 0;
        }
        else if (a == "-o" && i + 1 < args.size()) {
            opt.output = args[++i];
        }
        else if (a == "--force") {
            opt.force = true;
        }
        else if (a == "--threads" && i + 1 < args.size()) {
            opt.threads = stoi(args[++i]);
        }
        else if (a == "--insecure") {
            opt.insecure = true;
        }
        else if (a == "--proxy" && i + 1 < args.size()) {
            opt.proxy = args[++i];
        }
        else if (a == "--cacert" && i + 1 < args.size()) {
            opt.cacert = args[++i];
        }
        else if (a == "--limit-rate" && i + 1 < args.size()) {
            opt.limit_rate = parse_rate(args[++i]);
        }
        else if (is_url(a)) {
            opt.url = a;
        }
        else {
            cerr << "未知选项: " << a << endl;
            return 1;
        }
    }

    if (opt.url.empty() || !is_url(opt.url)) {
        cerr << "错误：请提供正确的下载链接（必须以 http:// 或 https:// 开头）\n";
        return 1;
    }

    if (opt.output.empty()) {
        size_t pos = opt.url.find_last_of('/');
        opt.output = (pos == string::npos ? "download.dat" : opt.url.substr(pos + 1));
    }

    if (fs::exists(opt.output) && !opt.force) {
        cerr << "错误：文件已存在，使用 --force 才能覆盖\n";
        return 1;
    }

    curl_global_init(CURL_GLOBAL_ALL);

    long total_size = 0;
    if (!get_file_size(opt, total_size)) return 1;

    cout << "文件大小: " << human_size(total_size) << endl;

    // 创建占位文件
    {
        FILE* fp = fopen(opt.output.c_str(), "wb");
        fseek(fp, total_size - 1, SEEK_SET);
        fputc('\0', fp);
        fclose(fp);
    }

    // 分段
    vector<SegmentTask> segs;
    long part = total_size / opt.threads;

    for (int i = 0; i < opt.threads; i++) {
        SegmentTask s;
        s.start = i * part;
        s.end = (i == opt.threads - 1 ? total_size - 1 : (i + 1) * part - 1);
        s.downloaded = 0;
        segs.push_back(s);
    }

    cout << "开始下载 (" << opt.threads << " 线程)…\n";

    vector<thread> ths;

    for (int i = 0; i < opt.threads; i++) {
        ths.emplace_back([&, i]() {
            download_range(opt, segs[i], opt.output);
        });
    }

    // 进度条
    while (true) {
        long done = 0;
        for (auto &s : segs) done += s.downloaded;

        print_progress(total_size, segs);

        bool finished = true;
        for (auto &s : segs)
            if (!s.ok)
                finished = false;

        if (finished) break;

        this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    cout << "\n下载完成！文件保存在：" << opt.output << endl;

    curl_global_cleanup();
    return 0;
}
