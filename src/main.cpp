#include <iostream>
#include <string>
#include <curl/curl.h>
#include <fstream>

// 写入回调
size_t write_data(void* ptr, size_t size, size_t nmemb, void* stream) {
    std::ofstream* ofs = static_cast<std::ofstream*>(stream);
    ofs->write(static_cast<char*>(ptr), size * nmemb);
    return size * nmemb;
}

int main(int argc, char** argv) {
    if(argc < 3) {
        std::cout << "Usage: super_downloader <URL> <output_file>\n";
        return 1;
    }

    std::string url = argv[1];
    std::string output_file = argv[2];

    CURL* curl = curl_easy_init();
    if(!curl) {
        std::cerr << "Failed to init curl\n";
        return 1;
    }

    std::ofstream ofs(output_file, std::ios::binary);
    if(!ofs.is_open()) {
        std::cerr << "Failed to open file: " << output_file << "\n";
        curl_easy_cleanup(curl);
        return 1;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ofs);

    CURLcode res = curl_easy_perform(curl);
    if(res != CURLE_OK) {
        std::cerr << "Download failed: " << curl_easy_strerror(res) << "\n";
        curl_easy_cleanup(curl);
        return 1;
    }

    std::cout << "Download finished: " << output_file << "\n";
    curl_easy_cleanup(curl);
    return 0;
}
