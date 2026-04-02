#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <sstream>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

struct StreamEntry {
    std::string id;
    std::vector<std::string> fields;
};

// Global storage for our streams
std::map<std::string, std::vector<StreamEntry>> streams;

// Helper to convert "1000-1" into a comparable pair of longs
std::pair<long long, long long> parse_id(const std::string& id_str, bool is_start) {
    size_t dash_pos = id_str.find('-');
    if (dash_pos == std::string::npos) {
        // If no sequence provided, use 0 for start and max for end
        long long ms = std::stoll(id_str);
        return {ms, is_start ? 0 : 9223372036854775807LL};
    }
    long long ms = std::stoll(id_str.substr(0, dash_pos));
    long long seq = std::stoll(id_str.substr(dash_pos + 1));
    return {ms, seq};
}

std::string to_bulk_string(const std::string& s) {
    return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
}

std::string handle_xrange(const std::vector<std::string>& args) {
    if (args.size() < 3) return "-ERR wrong number of arguments\r\n";

    std::string key = args[0];
    if (streams.find(key) == streams.end()) return "*0\r\n";

    auto start_bound = parse_id(args[1], true);
    auto end_bound = parse_id(args[2], false);

    std::vector<const StreamEntry*> matches;
    for (const auto& entry : streams[key]) {
        auto current = parse_id(entry.id, true);
        if (current >= start_bound && current <= end_bound) {
            matches.push_back(&entry);
        }
    }

    // RESP Construction: Array of Entries
    std::string response = "*" + std::to_string(matches.size()) + "\r\n";
    for (const auto* entry : matches) {
        // Each entry is an array of 2: [ID, [FieldList]]
        response += "*2\r\n";
        response += to_bulk_string(entry->id);
        
        // FieldList is its own array
        response += "*" + std::to_string(entry->fields.size()) + "\r\n";
        for (const auto& field : entry->fields) {
            response += to_bulk_string(field);
        }
    }
    return response;
}

// Simple logic to handle XADD so XRANGE has data to query
std::string handle_xadd(const std::vector<std::string>& args) {
    if (args.size() < 3) return "-ERR missing args\r\n";
    std::string key = args[0];
    std::string id = args[1];
    std::vector<std::string> fields(args.begin() + 2, args.end());
    
    streams[key].push_back({id, fields});
    return to_bulk_string(id);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(6379);

    bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    listen(server_fd, 5);

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_addr_len);

        char buffer[1024] = {0};
        read(client_fd, buffer, 1024);

        // This is a VERY simplified RESP parser for the challenge context
        // In a real app, you'd parse the *<num> and $<len> properly
        std::vector<std::string> cmd_parts; 
        // ... (Insert your existing RESP parsing logic here to populate cmd_parts)

        if (!cmd_parts.empty()) {
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