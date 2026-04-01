#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread> // Required for multi-threading

// Function to handle a single client's connection life cycle
void handle_client(int client_fd) {
    char buffer[1024];
    while (true) {
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        
        if (bytes_received <= 0) {
            std::cout << "Client disconnected.\n";
            break;
        }

        // Hardcoded RESP PONG response
        const char *response = "+PONG\r\n";
        send(client_fd, response, strlen(response), 0);
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

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        return 1;
    }

    listen(server_fd, 5);

    while (true) {
        struct sockaddr_in client_addr;
        int client_addr_len = sizeof(client_addr);

        std::cout << "Waiting for a client...\n";
        int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
        
        if (client_fd < 0) {
            std::cerr << "Accept failed\n";
            continue;
        }
        
        std::cout << "Client connected! Spawning thread...\n";

        // Create a new thread for the client. 
        // .detach() allows the thread to run independently so main can return to accept()
        std::thread(handle_client, client_fd).detach();
    }

    close(server_fd);
    return 0;
}