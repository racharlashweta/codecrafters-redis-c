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

std::string to_lowercase(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::tolower(c); });
    return s;
}

// Helper to format a string into a RESP Bulk String
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
            else if (command == "rpush" && parts.size() >= 3) {
                int new_len = 0;
                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    Node &n = kv_store[parts[1]];
                    n.type = T_LIST;
                    for (size_t i = 2; i < parts.size(); ++i) n.list_val.push_back(parts[i]);
                    new_len = n.list_val.size();
                }
                std::string res = ":" + std::to_string(new_len) + "\r\n";
                send(client_fd, res.c_str(), res.length(), 0);
            }
            else if (command == "lrange" && parts.size() >= 4) {
                std::string key = parts[1];
                int start = std::stoi(parts[2]);
                int stop = std::stoi(parts[3]);
                std::string response;

                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (kv_store.count(key) && kv_store[key].type == T_LIST) {
                        std::vector<std::string> &list = kv_store[key].list_val;
                        int n = list.size();
                        if (start < 0) start = 0; // Simple handling for non-negative stage
                        if (stop >= n) stop = n - 1;

                        if (start >= n || start > stop) {
                            response = "*0\r\n";
                        } else {
                            int count = stop - start + 1;
                            response = "*" + std::to_string(count) + "\r\n";
                            for (int i = start; i <= stop; ++i) {
                                response += to_bulk_string(list[i]);
                            }
                        }
                    } else {
                        response = "*0\r\n";
                    }
                }
                send(client_fd, response.c_str(), response.length(), 0);
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