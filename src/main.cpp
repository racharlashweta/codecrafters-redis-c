#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <thread>
#include <algorithm>

// Helper to handle case-insensitive command matching
std::string to_lowercase(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::tolower(c); });
    return s;
}

void handle_client(int client_fd) {
    char buffer[1024];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        
        // If 0, client disconnected; if -1, error occurred
        if (bytes_received <= 0) break;

        std::string input(buffer, bytes_received);
        
        // Redis commands are sent as RESP Arrays (starting with '*')
        if (input[0] == '*') {
            std::vector<std::string> parts;
            size_t pos = 0;
            
            // Basic RESP tokenizer: find all strings between \r\n markers
            while ((pos = input.find("\r\n")) != std::string::npos) {
                // Only collect the actual data (skip the length headers like $4 or *2)
                // We identify data lines because they don't start with '*' or '$'
                std::string line = input.substr(0, pos);
                if (line[0] != '*' && line[0] != '$') {
                    parts.push_back(line);
                }
                input.erase(0, pos + 2);
            }

            if (parts.empty()) continue;

            std::string command = to_lowercase(parts[0]);

            if (command == "ping") {
                send(client_fd, "+PONG\r\n", 7, 0);
            } 
            else if (command == "echo" && parts.size() >= 2) {
                // For ECHO, we return a Bulk String: $<length>\r\n<data>\r\n
                std::string content = parts[1];
                std::string response = "$" + std::to_string(content.length()) + "\r\n" + content + "\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
            }
        }
    }
    close(client_fd);
}

int main(int argc, char **argv) {
    // Standard CodeCrafters output buffering setup
    std::setvbuf(stdout, NULL, _IOLBF, 0);
    std::setvbuf(stderr, NULL, _IOLBF, 0);
    
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        return 1;
    }

    if (listen(server_fd, 5) != 0) return 1;

    std::cout << "Redis Server Started on Port 6379...\n";

    while (true) {
        struct sockaddr_in client_addr;
        int client_addr_len = sizeof(client_addr);

        int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
        if (client_fd < 0) continue;
        
        // Spawn a detached thread for every concurrent client
        std::thread(handle_client, client_fd).detach();
    }

    close(server_fd);
    return 0;
}