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
    char buffer[1024];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_received <= 0) break;

        std::string input(buffer, bytes_received);
        if (input[0] == '*') {
            std::vector<std::string> parts;
            size_t pos = 0;
            std::string temp = input;
            while ((pos = temp.find("\r\n")) != std::string::npos) {
                std::string line = temp.substr(0, pos);
                if (line[0] != '*' && line[0] != '$') parts.push_back(line);
                temp.erase(0, pos + 2);
            }

            if (parts.empty()) continue;
            std::string command = to_lowercase(parts[0]);

            if (command == "ping") {
                send(client_fd, "+PONG\r\n", 7, 0);
            } 
            else if (command == "echo" && parts.size() >= 2) {
                std::string res = to_bulk_string(parts[1]);
                send(client_fd, res.c_str(), res.length(), 0);
            }
            else if (command == "set" && parts.size() >= 3) {
                Node node; node.type = T_STRING; node.string_val = parts[2];
                if (parts.size() >= 5 && to_lowercase(parts[3]) == "px") {
                    node.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(std::stoll(parts[4]));
                    node.has_expiry = true;
                }
                { std::lock_guard<std::mutex> lock(kv_mutex); kv_store[parts[1]] = node; }
                send(client_fd, "+OK\r\n", 5, 0);
            }
            else if (command == "get" && parts.size() >= 2) {
                std::string res = "$-1\r\n";
                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (kv_store.count(parts[1])) {
                        Node &n = kv_store[parts[1]];
                        if (n.has_expiry && std::chrono::steady_clock::now() >= n.expiry) kv_store.erase(parts[1]);
                        else if (n.type == T_STRING) res = to_bulk_string(n.string_val);
                    }
                }
                send(client_fd, res.c_str(), res.length(), 0);
            }
            else if (command == "rpush" || command == "lpush") {
                int new_len = 0;
                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    Node &n = kv_store[parts[1]];
                    n.type = T_LIST;
                    for (size_t i = 2; i < parts.size(); ++i) {
                        if (command == "rpush") n.list_val.push_back(parts[i]);
                        else n.list_val.insert(n.list_val.begin(), parts[i]);
                    }
                    new_len = n.list_val.size();
                }
                cv.notify_all(); 
                std::string res = ":" + std::to_string(new_len) + "\r\n";
                send(client_fd, res.c_str(), res.length(), 0);
            }
            else if (command == "lpop" && parts.size() >= 2) {
                std::string res;
                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (!kv_store.count(parts[1]) || kv_store[parts[1]].type != T_LIST || kv_store[parts[1]].list_val.empty()) {
                        res = "$-1\r\n";
                    } else {
                        std::vector<std::string> &list = kv_store[parts[1]].list_val;
                        if (parts.size() == 2) {
                            std::string val = list[0];
                            list.erase(list.begin());
                            res = to_bulk_string(val);
                        } else {
                            int count = std::stoi(parts[2]);
                            int to_pop = std::min(count, (int)list.size());
                            res = "*" + std::to_string(to_pop) + "\r\n";
                            for (int i = 0; i < to_pop; ++i) {
                                res += to_bulk_string(list[0]);
                                list.erase(list.begin());
                            }
                        }
                    }
                }
                send(client_fd, res.c_str(), res.length(), 0);
            }
            else if (command == "blpop" && parts.size() >= 3) {
                std::string key = parts[1];
                std::string res;
                {
                    std::unique_lock<std::mutex> lock(kv_mutex);
                    cv.wait(lock, [&key] {
                        return kv_store.count(key) && !kv_store[key].list_val.empty();
                    });
                    std::vector<std::string> &list = kv_store[key].list_val;
                    std::string val = list[0];
                    list.erase(list.begin());
                    res = "*2\r\n" + to_bulk_string(key) + to_bulk_string(val);
                }
                send(client_fd, res.c_str(), res.length(), 0);
            }
            else if (command == "llen" && parts.size() >= 2) {
                int length = 0;
                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (kv_store.count(parts[1]) && kv_store[parts[1]].type == T_LIST) 
                        length = kv_store[parts[1]].list_val.size();
                }
                std::string res = ":" + std::to_string(length) + "\r\n";
                send(client_fd, res.c_str(), res.length(), 0);
            }
            else if (command == "lrange" && parts.size() >= 4) {
                std::string res;
                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (kv_store.count(parts[1]) && kv_store[parts[1]].type == T_LIST) {
                        std::vector<std::string> &list = kv_store[parts[1]].list_val;
                        int n = (int)list.size();
                        int start = std::stoi(parts[2]);
                        int stop = std::stoi(parts[3]);
                        if (start < 0) start = n + start;
                        if (stop < 0) stop = n + stop;
                        start = std::max(0, start);
                        stop = std::min(n - 1, stop);
                        if (start >= n || start > stop) { res = "*0\r\n"; }
                        else {
                            res = "*" + std::to_string(stop - start + 1) + "\r\n";
                            for (int i = start; i <= stop; ++i) res += to_bulk_string(list[i]);
                        }
                    } else { res = "*0\r\n"; }
                }
                send(client_fd, res.c_str(), res.length(), 0);
            }
        }
    }
    close(client_fd);
}

int main(int argc, char **argv) {
    std::setvbuf(stdout, NULL, _IOLBF, 0);
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in addr = {AF_INET, htons(6379), INADDR_ANY};
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);
    while (true) {
        int cf = accept(server_fd, NULL, NULL);
        if (cf > 0) std::thread(handle_client, cf).detach();
    }
    return 0;
}