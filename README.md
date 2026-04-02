# Redis-Lite: Custom Distributed Key-Value Store

A high-performance, multi-threaded Redis clone built from scratch in C. This project implements core Redis functionality, including advanced data structures, master-slave replication, and cryptographic data integrity.

## 🛠 Technical Highlights

### 1. Advanced Data Structures
* **Skip Lists:** Powering `Sorted Sets` (ZSET). Implemented with a probabilistic balancing strategy to ensure $O(\log n)$ search and insertion without the overhead of tree rebalancing.
* **Geospatial Indexing:** Uses **Morton Coding (Bit Interleaving)** to map 2D coordinates into a 1D searchable space, enabling efficient location-based queries.
* **Custom Hash Map:** Optimized key-value storage using the `djb2` algorithm with separate chaining for collision resolution.

### 2. Distributed Systems & Replication
* **Master-Slave Handshake:** Full implementation of the Redis replication protocol, supporting `PING`, `REPLCONF`, and `PSYNC` for state synchronization.
* **Atomic Transactions:** Supports `MULTI`, `EXEC`, and `DISCARD` commands via a thread-safe command-queueing mechanism.
* **RDB Persistence:** Custom binary parser for `.rdb` files, allowing the server to restore state and handle sub-second TTL (Time-To-Live) logic.

### 3. Security & Performance
* **SHA-256 Integrity:** Integrated cryptographic hashing to ensure data consistency and secure identifier generation.
* **Concurrency:** Multi-threaded architecture using `pthreads` and `poll()` to handle multiple concurrent client connections and replication ACKs.

---

## 🚀 Getting Started

### Prerequisites
* Linux environment (Tested on Ubuntu 24.04 LTS)
* GCC Compiler

### Compilation
Use the following command to compile with optimizations and necessary libraries:

```bash
gcc -Wall -Wextra -O2 -o redis-lite main.c sha256.c -lpthread -lm
