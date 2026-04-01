#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

int main(int argc, char **argv) {
    // Standard setup for CodeCrafters
    std::setvbuf(stdout, NULL, _IOLBF, 0);
    std::setvbuf(stderr, NULL, _IOLBF, 0);
    
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr));
    listen(server_fd, 5);

    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);

    std::cout << "Waiting for a client...\n";
    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    std::cout << "Client connected\n";

    // --- START OF STAGE #WY1 LOGIC ---
    char buffer[1024]; 
    while (true) {
        // recv returns the number of bytes read
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        
        if (bytes_received <= 0) {
            // bytes_received == 0 means the client closed the connection
            // bytes_received < 0 means an error occurred
            std::cout << "Client disconnected or error occurred.\n";
            break; 
        }

        // For now, we don't parse the buffer (which contains *1\r\n$4\r\nPING\r\n)
        // We just hardcode the response for every command received.
        const char *response = "+PONG\r\n";
        send(client_fd, response, strlen(response), 0);
    }
    // --- END OF STAGE #WY1 LOGIC ---

    close(client_fd);
    close(server_fd);

    return 0;
}