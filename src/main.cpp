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
std::condition_variable cv; // Used to wake up BLPOP threads

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
            else if (command == "rpush" || command == "lpush") {
                std::string key = parts[1];
                int new_len = 0;
                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    Node &n = kv_store[key];
                    n.type = T_LIST;
                    for (size_t i = 2; i < parts.size(); ++i) {
                        if (command == "rpush") n.list_val.push_back(parts[i]);
                        else n.list_val.insert(n.list_val.begin(), parts[i]);
                    }
                    new_len = n.list_val.size();
                }
                // Notify one waiting BLPOP thread that data is ready
                cv.notify_all(); 
                
                std::string res = ":" + std::to_string(new_len) + "\r\n";
                send(client_fd, res.c_str(), res.length(), 0);
            }
            else if (command == "blpop" && parts.size() >= 3) {
                std::string key = parts[1];
                // Timeout is parts[2], but we assume 0 (infinite) for now
                std::string response;

                std::unique_lock<std::mutex> lock(kv_mutex);
                // Wait while the list is missing or empty
                cv.wait(lock, [&key] {
                    return kv_store.count(key) && !kv_store[key].list_val.empty();
                });

                // Once woken up and list has data
                std::vector<std::string> &list = kv_store[key].list_val;
                std::string val = list[0];
                list.erase(list.begin());

                // BLPOP returns: [key, value]
                response = "*2\r\n" + to_bulk_string(key) + to_bulk_string(val);
                send(client_fd, response.c_str(), response.length(), 0);
            }
            else if (command == "lpop" && parts.size() >= 2) {
                // (Existing LPOP logic...)
                std::string res = "$-1\r\n";
                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (kv_store.count(parts[1]) && !kv_store[parts[1]].list_val.empty()) {
                        std::string val = kv_store[parts[1]].list_val[0];
                        kv_store[parts[1]].list_val.erase(kv_store[parts[1]].list_val.begin());
                        res = to_bulk_string(val);
                    }
                }
                send(client_fd, res.c_str(), res.length(), 0);
            }
            // (Include other commands: SET, GET, LLEN, LRANGE as before)
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