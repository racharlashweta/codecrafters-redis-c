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

// Global storage for streams
std::map<std::string, std::vector<StreamEntry>> streams;

// --- HELPERS ---

std::string to_bulk(const std::string& s) {
    return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
}

std::pair<long long, long long> parse_id(const std::string& id_str, bool is_start) {
    size_t dash = id_str.find('-');
    if (dash == std::string::npos) {
        long long ms = std::stoll(id_str);
        return {ms, is_start ? 0 : 9223372036854775807LL};
    }
    return {std::stoll(id_str.substr(0, dash)), std::stoll(id_str.substr(dash + 1))};
}

// --- COMMANDS ---

std::string handle_xadd(const std::vector<std::string>& args) {
    if (args.size() < 3) return "-ERR missing args\r\n";
    std::string key = args[0];
    std::string id = args[1];
    std::vector<std::string> fields(args.begin() + 2, args.end());
    streams[key].push_back({id, fields});
    return to_bulk(id);
}

std::string handle_xrange(const std::vector<std::string>& args) {
    std::string key = args[0];
    auto start_lim = parse_id(args[1], true);
    auto end_lim = parse_id(args[2], false);

    std::vector<const StreamEntry*> matches;
    for (const auto& entry : streams[key]) {
        auto curr = parse_id(entry.id, true);
        if (curr >= start_lim && curr <= end_lim) matches.push_back(&entry);
    }

    std::string res = "*" + std::to_string(matches.size()) + "\r\n";
    for (auto m : matches) {
        res += "*2\r\n"; // Entry Wrapper
        res += to_bulk(m->id);
        res += "*" + std::to_string(m->fields.size()) + "\r\n"; // Fields Array
        for (const auto& f : m->fields) res += to_bulk(f);
    }
    return res;
}

// --- CORE SERVER ---

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in serv_addr = {AF_INET, htons(6379), {INADDR_ANY}};
    bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    listen(server_fd, 5);

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        
        // INNER LOOP: Key for persistent connections
        while (true) {
            char buffer[4096] = {0};
            int bytes = recv(client_fd, buffer, sizeof(buffer), 0);
            if (bytes <= 0) break; 

            // Simple RESP parser (Splits by \r\n and skips RESP metadata symbols)
            std::vector<std::string> parts;
            char* line = strtok(buffer, "\r\n");
            while (line != NULL) {
                if (line[0] != '*' && line[0] != '$') parts.push_back(line);
                line = strtok(NULL, "\r\n");
            }

            if (parts.empty()) continue;

            std::string cmd = parts[0];
            std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
            std::vector<std::string> args(parts.begin() + 1, parts.end());

            std::string response;
            if (cmd == "XADD") response = handle_xadd(args);
            else if (cmd == "XRANGE") response = handle_xrange(args);
            else if (cmd == "PING") response = "+PONG\r\n";
            else response = "+OK\r\n";

            send(client_fd, response.c_str(), response.length(), 0);
        }
        close(client_fd);
    }
    return 0;
}