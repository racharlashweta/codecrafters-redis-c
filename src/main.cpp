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
#include <map>
#include <deque>

enum DataType { T_STRING, T_LIST, T_STREAM };

struct StreamEntry {
    std::string id;
    std::map<std::string, std::string> fields;
};

struct Node {
    DataType type = T_STRING;
    std::string string_val;
    std::deque<std::string> list_val; 
    std::vector<StreamEntry> stream_val;
    std::chrono::steady_clock::time_point expiry;
    bool has_expiry = false;
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
    char buffer[8192];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_received <= 0) break;

        std::string input(buffer, bytes_received);
        size_t offset = 0;

        while (offset < input.size() && input[offset] == '*') {
            try {
                size_t pos = input.find("\r\n", offset);
                int num_elements = std::stoi(input.substr(offset + 1, pos - offset - 1));
                offset = pos + 2;

                std::vector<std::string> parts;
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

                if (command == "xadd" && parts.size() >= 4) {
                    StreamEntry entry;
                    entry.id = parts[2];
                    for (size_t i = 3; i + 1 < parts.size(); i += 2) {
                        entry.fields[parts[i]] = parts[i+1];
                    }
                    {
                        std::lock_guard<std::mutex> lock(kv_mutex);
                        Node &n = kv_store[parts[1]];
                        n.type = T_STREAM;
                        n.stream_val.push_back(entry);
                    }
                    response = to_bulk_string(entry.id);
                }
                else if (command == "type" && parts.size() >= 2) {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (!kv_store.count(parts[1])) response = "+none\r\n";
                    else {
                        DataType t = kv_store[parts[1]].type;
                        if (t == T_STREAM) response = "+stream\r\n";
                        else if (t == T_LIST) response = "+list\r\n";
                        else response = "+string\r\n";
                    }
                }
                else if (command == "lrange" && parts.size() >= 4) {
                    std::string key = parts[1];
                    int start = std::stoi(parts[2]);
                    int end = std::stoi(parts[3]);
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (!kv_store.count(key)) response = "*0\r\n";
                    else {
                        auto &list = kv_store[key].list_val;
                        int size = (int)list.size();
                        if (start < 0) start = size + start;
                        if (end < 0) end = size + end;
                        start = std::max(0, start);
                        end = std::min(size - 1, end);
                        if (start > end || start >= size) response = "*0\r\n";
                        else {
                            response = "*" + std::to_string(end - start + 1) + "\r\n";
                            for (int i = start; i <= end; ++i) response += to_bulk_string(list[i]);
                        }
                    }
                }
                else if (command == "lpop" && parts.size() >= 2) {
                    std::string key = parts[1];
                    int count = (parts.size() >= 3) ? std::stoi(parts[2]) : 1;
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (!kv_store.count(key) || kv_store[key].list_val.empty()) response = "$-1\r\n";
                    else {
                        auto &list = kv_store[key].list_val;
                        int to_remove = std::min((int)list.size(), count);
                        if (parts.size() >= 3) {
                            response = "*" + std::to_string(to_remove) + "\r\n";
                            for (int i = 0; i < to_remove; ++i) {
                                response += to_bulk_string(list.front());
                                list.pop_front();
                            }
                        } else {
                            response = to_bulk_string(list.front());
                            list.pop_front();
                        }
                    }
                }
                else if (command == "blpop" && parts.size() >= 3) {
                    std::string key = parts[1];
                    double timeout = std::stod(parts[2]);
                    std::unique_lock<std::mutex> lock(kv_mutex);
                    auto pred = [&] { return kv_store.count(key) && !kv_store[key].list_val.empty(); };
                    bool ready = (timeout == 0) ? (cv.wait(lock, pred), true) : cv.wait_for(lock, std::chrono::duration<double>(timeout), pred);
                    if (ready) {
                        std::string val = kv_store[key].list_val.front();
                        kv_store[key].list_val.pop_front();
                        response = "*2\r\n" + to_bulk_string(key) + to_bulk_string(val);
                    } else response = "*-1\r\n";
                    lock.unlock();
                    send(client_fd, response.c_str(), response.length(), 0);
                    response = ""; 
                }
                else if (command == "rpush" || command == "lpush") {
                    {
                        std::lock_guard<std::mutex> lock(kv_mutex);
                        Node &n = kv_store[parts[1]];
                        n.type = T_LIST;
                        for (size_t i = 2; i < parts.size(); ++i) {
                            if (command == "rpush") n.list_val.push_back(parts[i]);
                            else n.list_val.push_front(parts[i]);
                        }
                        response = ":" + std::to_string(n.list_val.size()) + "\r\n";
                    }
                    cv.notify_all();
                }
                else if (command == "set" && parts.size() >= 3) {
                    Node n; n.type = T_STRING; n.string_val = parts[2];
                    if (parts.size() >= 5 && to_lowercase(parts[3]) == "px") {
                        n.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(std::stoll(parts[4]));
                        n.has_expiry = true;
                    }
                    { std::lock_guard<std::mutex> lock(kv_mutex); kv_store[parts[1]] = n; }
                    response = "+OK\r\n";
                }
                else if (command == "get" && parts.size() >= 2) {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (kv_store.count(parts[1])) {
                        Node &n = kv_store[parts[1]];
                        if (n.has_expiry && std::chrono::steady_clock::now() >= n.expiry) {
                            kv_store.erase(parts[1]); response = "$-1\r\n";
                        } else response = to_bulk_string(n.string_val);
                    } else response = "$-1\r\n";
                }
                else if (command == "ping") response = "+PONG\r\n";
                else if (command == "echo" && parts.size() >= 2) response = to_bulk_string(parts[1]);
                else response = "-ERR unknown command\r\n";

                if (!response.empty()) send(client_fd, response.c_str(), response.length(), 0);
            } catch (...) { break; }
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