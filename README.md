# FluxDB ⚡

**FluxDB** is a high-performance, multi-model, in-memory NoSQL database written from scratch in modern C++. It combines the flexibility of document storage (like MongoDB) with the real-time capabilities of a message broker (like Redis).

Designed as a lightweight, self-optimizing engine, it features **Adaptive Indexing**, **Real-Time Pub/Sub**, and **Crash-Safe Persistence**.

-----

## 🌟 Key Features

  * **🚀 High Performance**: Built on a multi-threaded TCP server architecture using raw sockets.
  * **🧠 Adaptive Indexing**: The database "learns" your query patterns. It automatically creates Hash Indexes (O(1)) for equality searches and Sorted Indexes (O(log n)) for range queries based on usage frequency.
  * **💾 Robust Persistence**:
      * **Write-Ahead Logging (WAL)**: Ensures zero data loss on crashes.
      * **Snapshots**: Fast startup by loading compressed database states.
  * **⚡ Real-Time Engine**:
      * **Pub/Sub**: Built-in message broker for real-time event broadcasting.
      * **TTL (Time-To-Live)**: Automatic document expiration for session management.
  * **🛡️ Security**: Simple password-based authentication (`AUTH`).
  * **🏢 Multi-Tenancy**: Create and manage multiple isolated databases on a single server instance.
  * **🔎 Smart Query Engine**: Supports complex operators (`$gt`, `$lt`, `$ne`) and range queries.

-----

## 📥 Installation & Setup

### Option A: Download Binaries (Recommended)

You can download the pre-compiled server and CLI for Windows from the Releases page.

1.  Go to **[FluxDB v1.0 Release](https://github.com/PranavKndpl/FluxDB/releases/tag/V1.0)**.
2.  Download `FluxDB_v1.0.zip`.
3.  Extract the folder.
4.  Run `fluxd.exe` to start the server.

### Option B: Build from Source

Requirements: C++17 Compiler (MinGW/MSVC) & CMake (Optional).

```bash
# Clone the repository
git clone https://github.com/PranavKndpl/FluxDB.git
cd FluxDB

# Compile the Server
g++ src/server.cpp -o bin/fluxd.exe -O3 -std=c++17 -lws2_32

# Compile the CLI Client
g++ src/client.cpp -o bin/flux.exe -O3 -std=c++17 -lws2_32

# Run
./bin/fluxd.exe
```

-----

## 🐍 Python Driver

FluxDB has an official, production-ready Python driver.

### 1\. Install via PyPI (Easiest)

You can install the driver directly using pip:

```bash
pip install fluxdb-driver
```

*View on PyPI: [fluxdb-driver](https://www.google.com/search?q=https://pypi.org/project/fluxdb-driver/)*

### 2\. Install from Repository (For Developers)

If you want to modify the driver source code:

```bash
cd drivers/python
pip install .
```

-----

## 🚀 Quick Start Guide

### 1\. Start the Server

Run the server executable. By default, it runs on port `8080` and stores data in the `./data` directory.

```bash
./fluxd.exe
# Output: === FluxDB Server Running on Port 8080 ===
```

### 2\. Connect via Python

```python
from fluxdb import FluxDB

# Connect to localhost:8080 (Default password: 'flux_admin')
db = FluxDB(password="flux_admin")

# 1. Switch/Create Database
db.use("game_data")

# 2. Insert Data
uid = db.insert({"username": "Player1", "score": 100, "rank": "Silver"})
print(f"Inserted User ID: {uid}")

# 3. Smart Query (Greater Than)
# This will trigger Adaptive Indexing if run frequently!
top_players = db.find({"score": {"$gt": 50}})
print("Top Players:", top_players)

# 4. Real-Time Pub/Sub
db.publish("global_chat", "Hello World from Python!")
```

-----

## 📚 Command Reference

You can use the built-in CLI (`flux.exe`) or send raw text commands via TCP.

| Category | Command | Description |
| :--- | :--- | :--- |
| **Auth** | `AUTH <password>` | Authenticate session. |
| **System** | `USE <db_name>` | Switch/Create a database. |
| | `SHOW DBS` | List all databases. |
| | `DROP DATABASE <name>` | Delete a database permanently. |
| | `STATS` | View document count and schema sample. |
| | `HELP` | Show this help menu. |
| **CRUD** | `INSERT <json>` | Create a document. |
| | `GET <id>` | Retrieve document by ID. |
| | `FIND <json_query>` | Search (e.g. `{"age": {"$gt": 18}}`). |
| | `UPDATE <id> <json>` | Update a document. |
| | `DELETE <id>` | Delete a document. |
| **Real-Time** | `EXPIRE <id> <sec>` | Set a TTL (auto-delete). |
| | `SUBSCRIBE <ch>` | Listen to a channel. |
| | `PUBLISH <ch> <msg>` | Broadcast a message. |
| **Config** | `CONFIG ADAPTIVE 1` | Enable Adaptive Indexing. |
| | `CONFIG PUBSUB 1` | Enable Pub/Sub module. |

-----

## 🏗️ Architecture

FluxDB uses a modular, layered architecture for stability and maintainability.

  * **Interface Layer**: `Server` (TCP), `PubSubManager` (Message Routing), `DatabaseManager` (Multi-Tenancy).
  * **Logic Layer**: `QueryProcessor` (Parsing, Auth, Smart Matching).
  * **Engine Layer**:
      * `StorageEngine`: Manages in-memory data and Adaptive Indexes.
      * `PersistenceManager`: Handles WAL appending and Snapshot recovery.
      * `ExpiryManager`: Uses a Min-Heap for O(1) TTL eviction.

-----

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.
