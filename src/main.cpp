#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/time.h>

// --- Constants and Types ---
#define TABLE_SIZE 100

enum EntryType {
    TypeString = 0,
    TypeStream,
    TypeList,
    TypeSortedSet
};

struct StreamEntry {
    uint64_t ms_time;
    int sequence_num;
    std::string id_str;
    std::vector<std::string> fields;
    StreamEntry* next;
};

struct Entry {
    std::string key;
    std::string value;
    uint64_t expiry;
    EntryType type;
    StreamEntry* stream_head;
    StreamEntry* stream_tail;
    Entry* next; // For HashMap chaining

    Entry() : expiry(0), type(TypeString), stream_head(nullptr), stream_tail(nullptr), next(nullptr) {}
};

// --- Global State ---
class RedisMap {
public:
    Entry* table[TABLE_SIZE];

    RedisMap() {
        for (int i = 0; i < TABLE_SIZE; i++) table[i] = nullptr;
    }

    unsigned int hash(const std::string& str) {
        unsigned int h = 5381;
        for (char c : str) h = ((h << 5) + h) + c;
        return h % TABLE_SIZE;
    }

    void put(const std::string& key, const std::string& val, uint64_t exp, EntryType t) {
        unsigned int idx = hash(key);
        Entry* curr = table[idx];
        while (curr) {
            if (curr->key == key) {
                curr->value = val;
                curr->expiry = exp;
                return;
            }
            curr = curr->next;
        }
        Entry* new_e = new Entry();
        new_e->key = key;
        new_e->value = val;
        new_e->expiry = exp;
        new_e->type = t;
        new_e->next = table[idx];
        table[idx] = new_e;
    }

    Entry* get(const std::string& key) {
        unsigned int idx = hash(key);
        Entry* curr = table[idx];
        while (curr) {
            if (curr->key == key) return curr;
            curr = curr->next;
        }
        return nullptr;
    }
};

RedisMap* global_map = new RedisMap();

// --- Helpers ---
uint64_t get_curr_time_ms() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
}

std::string to_bulk(const std::string& s) {
    return "$" + std::to_string(s.length()) + "\r\n" + s + "\r\n";
}

// --- XADD Logic (The Fix) ---
std::string handle_xadd(const std::vector<std::string>& tokens) {
    if (tokens.size() < 3) return "-ERR missing args\r\n";

    std::string key = tokens[1];
    std::string id_input = tokens[2];
    uint64_t t_ms = 0;
    int t_seq = 0;

    Entry* e = global_map->get(key);
    if (!e) {
        global_map->put(key, "", 0xFFFFFFFFFFFFFFFFULL, TypeStream);
        e = global_map->get(key);
    }

    if (id_input == "*") {
        t_ms = get_curr_time_ms();
        if (e->stream_tail) {
            if (t_ms <= e->stream_tail->ms_time) {
                t_ms = e->stream_tail->ms_time;
                t_seq = e->stream_tail->sequence_num + 1;
            }
        } else if (t_ms == 0) {
            t_seq = 1;
        }
    } else {
        size_t dash = id_input.find('-');
        if (dash != std::string::npos) {
            t_ms = std::stoull(id_input.substr(0, dash));
            t_seq = std::stoi(id_input.substr(dash + 1));
        } else {
            t_ms = std::stoull(id_input);
            t_seq = 0;
        }
    }

    std::string final_id = std::to_string(t_ms) + "-" + std::to_string(t_seq);
    
    StreamEntry* new_se = new StreamEntry();
    new_se->ms_time = t_ms;
    new_se->sequence_num = t_seq;
    new_se->id_str = final_id;
    new_se->next = nullptr;
    
    for (size_t i = 3; i < tokens.size(); i++) {
        new_se->fields.push_back(tokens[i]);
    }

    if (!e->stream_head) {
        e->stream_head = e->stream_tail = new_se;
    } else {
        e->stream_tail->next = new_se;
        e->stream_tail = new_se;
    }

    return to_bulk(final_id);
}

// --- Server Main ---
int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in serv_addr = {AF_INET, htons(6379), {INADDR_ANY}};
    bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    listen(server_fd, 5);

    while (true) {
        int client_sock = accept(server_fd, nullptr, nullptr);
        
        while (true) {
            char buffer[4096] = {0};
            int bytes = recv(client_sock, buffer, sizeof(buffer), 0);
            if (bytes <= 0) break;

            // Robust RESP Parser
            std::vector<std::string> tokens;
            char* line = strtok(buffer, "\r\n");
            while (line) {
                if (line[0] != '*' && line[0] != '$') {
                    tokens.push_back(line);
                }
                line = strtok(NULL, "\r\n");
            }

            if (tokens.empty()) continue;

            std::string cmd = tokens[0];
            std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

            std::string response;
            if (cmd == "XADD") {
                response = handle_xadd(tokens);
            } else if (cmd == "PING") {
                response = "+PONG\r\n";
            } else {
                response = "+OK\r\n";
            }

            send(client_sock, response.c_str(), response.length(), 0);
        }
        close(client_sock);
    }
    return 0;
}