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

// Global database and mutex for thread-safe access
std::unordered_map<std::string, std::string> kv_store;
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
            std::string temp_input = input;

            // RESP Parser: Extracting actual data strings from the buffer
            while ((pos = temp_input.find("\r\n")) != std::string::npos) {
                std::string line = temp_input.substr(0, pos);
                // Skip length headers (*, $) and process the data content
                if (line[0] != '*' && line[0] != '$') {
                    parts.push_back(line);
                }
                temp_input.erase(0, pos + 2);
            }

            if (parts.empty()) continue;

            std::string command = to_lowercase(parts[0]);

            if (command == "ping") {
                send(client_fd, "+PONG\r\n", 7, 0);
            } 
            else if (command == "echo" && parts.size() >= 2) {
                std::string content = parts[1];
                std::string response = "$" + std::to_string(content.length()) + "\r\n" + content + "\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
            }
            else if (command == "set" && parts.size() >= 3) {
                std::string key = parts[1];
                std::string value = parts[2];
                
                {
                    // Lock the mutex before writing to the map
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    kv_store[key] = value;
                }
                
                send(client_fd, "+OK\r\n", 5, 0);
            }
            else if (command == "get" && parts.size() >= 2) {
                std::string key = parts[1];
                std::string response;
                
                {
                    // Lock the mutex before reading from the map
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (kv_store.find(key) != kv_store.end()) {
                        std::string value = kv_store[key];
                        response = "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
                    } else {
                        // Key not found: return Null Bulk String per Redis spec
                        response = "$-1\r\n";
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
    std::setvbuf(stderr, NULL, _IOLBF, 0);
    
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) return 1;
    if (listen(server_fd, 5) != 0) return 1;

    std::cout << "Redis Server listening for SET/GET...\n";

    while (true) {
        struct sockaddr_in client_addr;
        int client_addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
        if (client_fd < 0) continue;
        
        // Handle concurrent clients via detached threads
        std::thread(handle_client, client_fd).detach();
    }

    close(server_fd);
    return 0;
}