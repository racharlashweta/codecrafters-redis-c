#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <thread>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <condition_variable>

enum DataType { T_STRING, T_LIST };

struct Node {
    DataType type = T_STRING;
    std::string string_val;
    std::vector<std::string> list_val;
};

std::unordered_map<std::string, Node> kv_store;
std::mutex kv_mutex;
std::condition_variable cv;

std::string to_lowercase(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::tolower(c); });
    return s;
}

std::string to_bulk_string(const std::string& s) {
    return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
}

void handle_client(int client_fd) {
    char buffer[4096]; // Increased buffer for larger LRANGE/RPUSH
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_received <= 0) break;

        std::string input(buffer, bytes_received);
        size_t offset = 0;

        // Redis protocol can send multiple commands in one packet
        while (offset < input.size() && input[offset] == '*') {
            std::vector<std::string> parts;
            size_t pos = input.find("\r\n", offset);
            int num_elements = std::stoi(input.substr(offset + 1, pos - offset - 1));
            offset = pos + 2;

            for (int i = 0; i < num_elements; ++i) {
                pos = input.find("\r\n", offset);
                int len = std::stoi(input.substr(offset + 1, pos - offset - 1));
                offset = pos + 2;
                parts.push_back(input.substr(offset, len));
                offset += len + 2;
            }

            if (parts.empty()) continue;
            std::string command = to_lowercase(parts[0]);
            std::string response = "";

            if (command == "ping") {
                response = "+PONG\r\n";
            } 
            else if (command == "type" && parts.size() >= 2) {
                std::lock_guard<std::mutex> lock(kv_mutex);
                if (kv_store.find(parts[1]) == kv_store.end()) response = "+none\r\n";
                else response = (kv_store[parts[1]].type == T_LIST) ? "+list\r\n" : "+string\r\n";
            }
            else if (command == "set" && parts.size() >= 3) {
                std::lock_guard<std::mutex> lock(kv_mutex);
                kv_store[parts[1]] = {T_STRING, parts[2]};
                response = "+OK\r\n";
            }
            else if (command == "rpush" || command == "lpush") {
                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    Node &n = kv_store[parts[1]];
                    n.type = T_LIST;
                    for (size_t i = 2; i < parts.size(); ++i) {
                        if (command == "rpush") n.list_val.push_back(parts[i]);
                        else n.list_val.insert(n.list_val.begin(), parts[i]);
                    }
                    response = ":" + std::to_string(n.list_val.size()) + "\r\n";
                }
                cv.notify_all();
            }
            else if (command == "lpop" && parts.size() >= 2) {
                std::lock_guard<std::mutex> lock(kv_mutex);
                if (kv_store.find(parts[1]) == kv_store.end() || kv_store[parts[1]].list_val.empty()) {
                    response = "$-1\r\n";
                } else {
                    auto &list = kv_store[parts[1]].list_val;
                    if (parts.size() == 2) {
                        response = to_bulk_string(list[0]);
                        list.erase(list.begin());
                    } else {
                        int count = std::stoi(parts[2]);
                        int to_pop = std::min(count, (int)list.size());
                        response = "*" + std::to_string(to_pop) + "\r\n";
                        for (int i = 0; i < to_pop; ++i) {
                            response += to_bulk_string(list[0]);
                            list.erase(list.begin());
                        }
                    }
                }
            }
            else if (command == "blpop" && parts.size() >= 3) {
                std::string key = parts[1];
                double timeout = std::stod(parts[2]);
                std::unique_lock<std::mutex> lock(kv_mutex);
                auto pred = [&] { return kv_store.count(key) && !kv_store[key].list_val.empty(); };

                bool ready = (timeout == 0) ? (cv.wait(lock, pred), true) 
                                           : cv.wait_for(lock, std::chrono::duration<double>(timeout), pred);
                if (ready) {
                    std::string val = kv_store[key].list_val[0];
                    kv_store[key].list_val.erase(kv_store[key].list_val.begin());
                    response = "*2\r\n" + to_bulk_string(key) + to_bulk_string(val);
                } else {
                    response = "*-1\r\n";
                }
                lock.unlock();
            }
            else if (command == "lrange" && parts.size() >= 4) {
                std::lock_guard<std::mutex> lock(kv_mutex);
                if (kv_store.count(parts[1]) && kv_store[parts[1]].type == T_LIST) {
                    auto &list = kv_store[parts[1]].list_val;
                    int n = list.size();
                    int start = std::stoi(parts[2]), stop = std::stoi(parts[3]);
                    if (start < 0) start = n + start;
                    if (stop < 0) stop = n + stop;
                    start = std::max(0, start); stop = std::min(n - 1, stop);
                    if (start >= n || start > stop) response = "*0\r\n";
                    else {
                        response = "*" + std::to_string(stop - start + 1) + "\r\n";
                        for (int i = start; i <= stop; ++i) response += to_bulk_string(list[i]);
                    }
                } else {
                    response = "*0\r\n";
                }
            }

            if (!response.empty()) {
                send(client_fd, response.c_str(), response.length(), 0);
            }
        }
    }
    close(client_fd);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in addr = {AF_INET, htons(6379), INADDR_ANY};
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);
    while (true) {
        int cf = accept(server_fd, NULL, NULL);
        if (cf > 0) std::thread(handle_client, cf).detach();
    }
    return 0;
}