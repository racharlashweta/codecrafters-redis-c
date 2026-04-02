#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <chrono>

struct StreamEntry {
    std::string id;
    std::vector<std::string> fields;
};

std::map<std::string, std::vector<StreamEntry>> streams;

// --- HELPERS ---

std::string to_bulk(const std::string& s) {
    return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
}

// Parses "1000-1" into {1000, 1}
std::pair<long long, long long> parse_id(const std::string& id_str, bool is_start) {
    size_t dash = id_str.find('-');
    if (dash == std::string::npos) {
        try {
            long long ms = std::stoll(id_str);
            return {ms, is_start ? 0 : 9223372036854775807LL};
        } catch (...) { return {0, 0}; }
    }
    return {std::stoll(id_str.substr(0, dash)), std::stoll(id_str.substr(dash + 1))};
}

// --- XADD LOGIC ---

std::string handle_xadd(const std::vector<std::string>& args) {
    if (args.size() < 2) return "-ERR missing args\r\n";
    
    std::string key = args[0];
    std::string id_input = args[1];
    std::string final_id;

    if (id_input == "*") {
        long long now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        
        long long seq = 0;
        if (!streams[key].empty()) {
            auto last = parse_id(streams[key].back().id, true);
            if (now_ms <= last.first) {
                now_ms = last.first;
                seq = last.second + 1;
            }
        } else if (now_ms == 0) {
            seq = 1;
        }
        final_id = std::to_string(now_ms) + "-" + std::to_string(seq);
    } else {
        final_id = id_input;
    }

    // Key-value pairs start at args[2]
    std::vector<std::string> fields;
    for (size_t i = 2; i < args.size(); ++i) {
        fields.push_back(args[i]);
    }
    
    streams[key].push_back({final_id, fields});
    return to_bulk(final_id);
}

// --- XRANGE LOGIC ---

std::string handle_xrange(const std::vector<std::string>& args) {
    std::string key = args[0];
    auto start_lim = parse_id(args[1], true);
    auto end_lim = parse_id(args[2], false);

    std::string res = "";
    int count = 0;
    if (streams.count(key)) {
        for (const auto& entry : streams[key]) {
            auto curr = parse_id(entry.id, true);
            if (curr >= start_lim && curr <= end_lim) {
                count++;
                res += "*2\r\n" + to_bulk(entry.id);
                res += "*" + std::to_string(entry.fields.size()) + "\r\n";
                for (const auto& f : entry.fields) res += to_bulk(f);
            }
        }
    }
    return "*" + std::to_string(count) + "\r\n" + res;
}

// --- SERVER MAIN ---

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in serv_addr = {AF_INET, htons(6379), {INADDR_ANY}};
    bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    listen(server_fd, 5);

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        while (true) {
            char buffer[8192] = {0};
            int bytes = recv(client_fd, buffer, sizeof(buffer), 0);
            if (bytes <= 0) break;

            // REPRODUCIBLE PARSER
            // This logic strictly extracts only the string values between RESP markers
            std::vector<std::string> parts;
            std::string temp = "";
            bool reading = false;
            for(int i=0; i < bytes; ++i) {
                if (buffer[i] == '\r' || buffer[i] == '\n') {
                    if (reading && !temp.empty()) {
                        // Ignore lines that are just numbers (RESP lengths)
                        if (temp[0] != '*' && temp[0] != '$') {
                            parts.push_back(temp);
                        }
                    }
                    temp = "";
                    reading = true;
                } else {
                    temp += buffer[i];
                }
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