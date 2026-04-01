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
#include <map>
#include <deque>

enum DataType { T_STRING, T_LIST, T_STREAM };

struct StreamEntry {
    std::string id;
    std::map<std::string, std::string> fields;
};

struct Node {
    DataType type = T_STRING;
    std::string string_val;
    std::deque<std::string> list_val; 
    std::vector<StreamEntry> stream_val;
    std::chrono::steady_clock::time_point expiry;
    bool has_expiry = false;
};

std::unordered_map<std::string, Node> kv_store;
std::mutex kv_mutex;
std::condition_variable cv;

void parse_range_id(const std::string& id_str, long long& ms, long long& seq, bool is_start) {
    size_t dash_pos = id_str.find('-');
    if (dash_pos == std::string::npos) {
        ms = std::stoll(id_str);
        seq = is_start ? 0 : 9223372036854775807LL;
    } else {
        ms = std::stoll(id_str.substr(0, dash_pos));
        seq = std::stoll(id_str.substr(dash_pos + 1));
    }
}

bool parse_id(const std::string& id_str, long long& ms, long long& seq) {
    size_t dash_pos = id_str.find('-');
    if (dash_pos == std::string::npos) return false;
    try {
        ms = std::stoll(id_str.substr(0, dash_pos));
        seq = std::stoll(id_str.substr(dash_pos + 1));
        return true;
    } catch (...) { return false; }
}

std::string to_lowercase(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::tolower(c); });
    return s;
}

std::string to_bulk_string(const std::string& s) {
    return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
}

void handle_client(int client_fd) {
    char buffer[8192];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_received <= 0) break;

        std::string input(buffer, bytes_received);
        size_t offset = 0;

        while (offset < input.size() && input[offset] == '*') {
            try {
                size_t pos = input.find("\r\n", offset);
                int num_elements = std::stoi(input.substr(offset + 1, pos - offset - 1));
                offset = pos + 2;

                std::vector<std::string> parts;
                for (int i = 0; i < num_elements; ++i) {
                    pos = input.find("\r\n", offset);
                    int len = std::stoi(input.substr(offset + 1, pos - offset - 1));
                    offset = pos + 2;
                    parts.push_back(input.substr(offset, len));
                    offset += len + 2;
                }

                if (parts.empty()) continue;
                std::string command = to_lowercase(parts[0]);
                std::string response = "";

                if (command == "blpop" && parts.size() >= 3) {
                    std::string key = parts[1];
                    double timeout_val = std::stod(parts[2]);
                    std::unique_lock<std::mutex> lock(kv_mutex);
                    
                    auto pred = [&] { return kv_store.count(key) && !kv_store[key].list_val.empty(); };
                    
                    bool ready = false;
                    if (timeout_val == 0) {
                        cv.wait(lock, pred);
                        ready = true;
                    } else {
                        ready = cv.wait_for(lock, std::chrono::duration<double>(timeout_val), pred);
                    }

                    if (ready) {
                        std::string val = kv_store[key].list_val.front();
                        kv_store[key].list_val.pop_front();
                        response = "*2\r\n" + to_bulk_string(key) + to_bulk_string(val);
                    } else {
                        response = "*-1\r\n"; // NULL Array
                    }
                    lock.unlock();
                    send(client_fd, response.c_str(), response.length(), 0);
                    response = ""; // Prevent double sending or error falling through
                }
                else if (command == "xrange" && parts.size() >= 4) {
                    std::string key = parts[1];
                    long long s_ms, s_seq, e_ms, e_seq;
                    parse_range_id(parts[2], s_ms, s_seq, true);
                    parse_range_id(parts[3], e_ms, e_seq, false);
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (!kv_store.count(key)) response = "*0\r\n";
                    else {
                        std::vector<StreamEntry> res;
                        for (auto& entry : kv_store[key].stream_val) {
                            long long m, s; parse_id(entry.id, m, s);
                            if (((m > s_ms) || (m == s_ms && s >= s_seq)) && ((m < e_ms) || (m == e_ms && s <= e_seq)))
                                res.push_back(entry);
                        }
                        response = "*" + std::to_string(res.size()) + "\r\n";
                        for (auto& entry : res) {
                            response += "*2\r\n" + to_bulk_string(entry.id) + "*" + std::to_string(entry.fields.size()*2) + "\r\n";
                            for (auto const& [f, v] : entry.fields) response += to_bulk_string(f) + to_bulk_string(v);
                        }
                    }
                }
                else if (command == "xadd" && parts.size() >= 4) {
                    std::string key = parts[1], id_in = parts[2], fid = "";
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    Node &n = kv_store[key]; n.type = T_STREAM;
                    long long lms = -1, lseq = -1;
                    if (!n.stream_val.empty()) parse_id(n.stream_val.back().id, lms, lseq);

                    if (id_in == "*") {
                        long long now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                        long long s = (now > lms) ? (now == 0 ? 1 : 0) : (lseq + 1);
                        fid = std::to_string(now) + "-" + std::to_string(s);
                    } else if (id_in.find("-*") != std::string::npos) {
                        long long m = std::stoll(id_in.substr(0, id_in.find('-')));
                        long long s = (m == 0) ? (lms == 0 ? lseq + 1 : 1) : (m == lms ? lseq + 1 : 0);
                        fid = std::to_string(m) + "-" + std::to_string(s);
                    } else fid = id_in;

                    long long nms, nseq;
                    if (fid == "0-0") response = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                    else if (!parse_id(fid, nms, nseq)) response = "-ERR Invalid ID\r\n";
                    else if (lms != -1 && (nms < lms || (nms == lms && nseq <= lseq)))
                        response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    else {
                        StreamEntry e; e.id = fid;
                        for (size_t i = 3; i + 1 < parts.size(); i += 2) e.fields[parts[i]] = parts[i+1];
                        n.stream_val.push_back(e); response = to_bulk_string(fid);
                    }
                }
                else if (command == "rpush" || command == "lpush") {
                    {
                        std::lock_guard<std::mutex> lock(kv_mutex);
                        Node &n = kv_store[parts[1]]; n.type = T_LIST;
                        for (size_t i = 2; i < parts.size(); ++i) {
                            if (command == "rpush") n.list_val.push_back(parts[i]);
                            else n.list_val.push_front(parts[i]);
                        }
                        response = ":" + std::to_string(n.list_val.size()) + "\r\n";
                    }
                    cv.notify_all();
                }
                else if (command == "type" && parts.size() >= 2) {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (!kv_store.count(parts[1])) response = "+none\r\n";
                    else {
                        DataType t = kv_store[parts[1]].type;
                        response = (t == T_STREAM) ? "+stream\r\n" : (t == T_LIST ? "+list\r\n" : "+string\r\n");
                    }
                }
                else if (command == "set") {
                    Node n; n.type = T_STRING; n.string_val = parts[2];
                    if (parts.size() >= 5 && to_lowercase(parts[3]) == "px") {
                        n.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(std::stoll(parts[4]));
                        n.has_expiry = true;
                    }
                    std::lock_guard<std::mutex> lock(kv_mutex); kv_store[parts[1]] = n;
                    response = "+OK\r\n";
                }
                else if (command == "get") {
                    std::lock_guard<std::mutex> lock(kv_mutex);
                    if (kv_store.count(parts[1])) {
                        Node &n = kv_store[parts[1]];
                        if (n.has_expiry && std::chrono::steady_clock::now() >= n.expiry) { kv_store.erase(parts[1]); response = "$-1\r\n"; }
                        else response = to_bulk_string(n.string_val);
                    } else response = "$-1\r\n";
                }
                else if (command == "ping") response = "+PONG\r\n";
                else if (command == "echo") response = to_bulk_string(parts[1]);
                else response = "-ERR unknown command\r\n";

                if (!response.empty()) send(client_fd, response.c_str(), response.length(), 0);
            } catch (...) { break; }
        }
    }
    close(client_fd);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in addr = {AF_INET, htons(6379), INADDR_ANY};
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);
    while (true) {
        int cf = accept(server_fd, NULL, NULL);
        if (cf > 0) std::thread(handle_client, cf).detach();
    }
    return 0;
}