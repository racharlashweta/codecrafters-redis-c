#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>

struct StreamEntry {
    std::string id;
    std::vector<std::string> fields;
};

// Storage for streams
std::map<std::string, std::vector<StreamEntry>> streams;

// Helper: Formats a string into RESP Bulk String format
std::string to_bulk(const std::string& s) {
    return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
}

// Helper: Parses ID "100-1" into {100, 1} for comparison
std::pair<long long, long long> parse_id(const std::string& id_str, bool is_start) {
    size_t dash = id_str.find('-');
    if (dash == std::string::npos) {
        long long ms = std::stoll(id_str);
        // If no sequence: start defaults to 0, end defaults to max
        return {ms, is_start ? 0 : 9223372036854775807LL};
    }
    return {std::stoll(id_str.substr(0, dash)), std::stoll(id_str.substr(dash + 1))};
}

// --- COMMAND HANDLERS ---

std::string handle_xadd(const std::vector<std::string>& args) {
    if (args.size() < 3) return "-ERR missing args\r\n";
    std::string key = args[0];
    std::string id = args[1];
    std::vector<std::string> fields(args.begin() + 2, args.end());

    streams[key].push_back({id, fields});
    return to_bulk(id); // Crucial: XADD must return the ID
}

std::string handle_xrange(const std::vector<std::string>& args) {
    if (args.size() < 3) return "-ERR missing args\r\n";
    std::string key = args[0];
    if (streams.find(key) == streams.end()) return "*0\r\n";

    auto start_limit = parse_id(args[1], true);
    auto end_limit = parse_id(args[2], false);

    std::string res = "";
    int count = 0;

    for (const auto& entry : streams[key]) {
        auto curr = parse_id(entry.id, true);
        if (curr >= start_limit && curr <= end_limit) {
            count++;
            // Each entry is an array of 2: [ID, [fields]]
            res += "*2\r\n";
            res += to_bulk(entry.id);
            res += "*" + std::to_string(entry.fields.size()) + "\r\n";
            for (const auto& f : entry.fields) res += to_bulk(f);
        }
    }
    return "*" + std::to_string(count) + "\r\n" + res;
}

// --- SERVER BOILERPLATE ---

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in serv_addr = {AF_INET, htons(6379), {INADDR_ANY}};
    bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    listen(server_fd, 5);

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        char buffer[2048] = {0};
        int bytes_received = read(client_fd, buffer, 2048);
        
        if (bytes_received > 0) {
            // NOTE: You must implement a proper RESP parser here to extract 
            // the command and arguments into a std::vector<std::string> cmd_parts.
            // For now, let's assume cmd_parts is populated.

            std::vector<std::string> cmd_parts = {"XADD", "banana", "0-1", "orange", "raspberry"}; // Mock example
            
            std::string cmd = cmd_parts[0];
            std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
            std::vector<std::string> args(cmd_parts.begin() + 1, cmd_parts.end());

            std::string response;
            if (cmd == "XADD") response = handle_xadd(args);
            else if (cmd == "XRANGE") response = handle_xrange(args);
            else response = "+OK\r\n";

            send(client_fd, response.c_str(), response.length(), 0);
        }
        close(client_fd);
    }
    return 0;
}