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

struct Node {
    std::string value;
    // Using steady_clock for monotonic time (ignores system clock jumps)
    std::chrono::steady_clock::time_point expiry;
    bool has_expiry = false;
};

std::unordered_map<std::string, Node> kv_store;
std::mutex kv_mutex;

std::string to_lowercase(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::tolower(c); });
    return s;
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
                std::string res = "$" + std::to_string(parts[1].length()) + "\r\n" + parts[1] + "\r\n";
                send(client_fd, res.c_str(), res.length(), 0);
            }
            else if (command == "set" && parts.size() >= 3) {
                Node node;
                node.value = parts[2];
                
                // Check for PX argument (case-insensitive)
                if (parts.size() >= 5 && to_lowercase(parts[3]) == "px") {
                    long long ms = std::stoll(parts[4]);
                    node.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
                    node.has_expiry = true;
                }

                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    kv_store[parts[1]] = node;
                }
                send(client_fd, "+OK\r\n", 5, 0);
            }
            else if (command == "get" && parts.size() >= 2) {
                std::string key = parts[1];
                std::string response = "$-1\r\n"; // Default to null

                {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (kv_store.count(key)) {
                        Node &node = kv_store[key];
                        auto now = std::chrono::steady_clock::now();
                        
                        if (node.has_expiry && now >= node.expiry) {
                            kv_store.erase(key); // "Passive" deletion
                        } else {
                            response = "$" + std::to_string(node.value.length()) + "\r\n" + node.value + "\r\n";
                        }
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
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd > 0) std::thread(handle_client, client_fd).detach();
    }
    return 0;
}