#ifndef COLLECTION_HPP
#define COLLECTION_HPP

#include <iostream>
#include <unordered_map>
#include <optional>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>
#include <cmath> // dynamic threshold
#include <set>   // STATS sampling

#include "document.hpp"
#include "index_manager.hpp"
#include "serializer.hpp"
#include "expiry_manager.hpp" 

using Id = std::uint64_t;

class Collection {
private:
    std::string db_name;
    std::string full_wal_path;      
    std::string full_snapshot_path; 

    std::unordered_map<Id, fluxdb::Document> db;
    fluxdb::IndexManager indexer;
    std::ofstream wal_file;
    fluxdb::Serializer serializer;
    
    fluxdb::ExpiryManager expiry_manager; 

    Id next_id = 1;

    mutable std::shared_mutex rw_lock;
    std::thread janitor_thread;
    std::thread ttl_thread;
    std::condition_variable cv;
    std::mutex cv_m;
    std::atomic<bool> running{true};
    const size_t MAX_WAL_SIZE = 10 * 1024 * 1024; // 10 MB limit

    // Adaptive Indexing State
    std::atomic<bool> adaptive_mode{false};
    std::unordered_map<std::string, int> miss_counter;
    std::unordered_map<std::string, bool> needs_sorted_index;

    // --- INTERNAL HELPERS (No Locks) ---

    void insert_internal(Id id, const fluxdb::Document& doc) {
        std::vector<uint8_t> serializedDoc = serializer.serialize(doc);
        logOperation(0x01, id, serializedDoc);
        indexer.addDocument(id, doc);
        db.emplace(id, doc);
    }

    void insert_internal(Id id, fluxdb::Document&& doc) {
        std::vector<uint8_t> serializedDoc = serializer.serialize(doc);
        logOperation(0x01, id, serializedDoc);
        indexer.addDocument(id, doc);
        db.emplace(id, std::move(doc));
    }

    bool update_internal(Id id, const fluxdb::Document& doc) {
        auto it = db.find(id);
        if (it == db.end()) return false;

        indexer.removeDocument(id, it->second);
        it->second = doc;
        indexer.addDocument(id, doc);
        return true;
    }

    bool remove_internal(Id id) {
        auto it = db.find(id);
        if (it == db.end()) return false;

        expiry_manager.removeTTL(id);

        indexer.removeDocument(id, it->second);
        db.erase(it);
        return true;
    }

    // --- PERSISTENCE HELPERS ---

    void logOperation(uint8_t opCode, Id id, const std::vector<uint8_t>& data = {}) {
        if (!wal_file.is_open()) return;

        wal_file.put(static_cast<char>(opCode));
        wal_file.write(reinterpret_cast<const char*>(&id), sizeof(id));

        if (opCode == 0x01) {
            uint32_t size = static_cast<uint32_t>(data.size());
            wal_file.write(reinterpret_cast<const char*>(&size), sizeof(size));
            wal_file.write(reinterpret_cast<const char*>(data.data()), size);
        }

        wal_file.flush();
    }

    void save_internal(const std::string& filename) {
        fluxdb::Serializer writer;
        std::ofstream file(filename, std::ios::binary | std::ios::out);
        if (!file.is_open()) throw std::runtime_error("Cannot open file: " + filename);

        file.write(reinterpret_cast<const char*>(&next_id), sizeof(next_id));
        uint64_t count = db.size();
        file.write(reinterpret_cast<const char*>(&count), sizeof(count));

        for (const auto& [id, doc] : db) {
            std::vector<uint8_t> bytes = writer.serialize(doc);
            file.write(reinterpret_cast<const char*>(&id), sizeof(id));
            uint32_t docSize = static_cast<uint32_t>(bytes.size());
            file.write(reinterpret_cast<const char*>(&docSize), sizeof(docSize));
            file.write(reinterpret_cast<const char*>(bytes.data()), docSize);
        }
        std::cout << "[Snapshot] Saved " << count << " documents to " << filename << "\n";
    }

    void load_internal(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary | std::ios::in);
        if (!file.is_open()) return;

        db.clear();
        indexer.clear();

        file.read(reinterpret_cast<char*>(&next_id), sizeof(next_id));
        uint64_t count = 0;
        file.read(reinterpret_cast<char*>(&count), sizeof(count));

        for (uint64_t i = 0; i < count; ++i) {
            Id id;
            file.read(reinterpret_cast<char*>(&id), sizeof(id));
            uint32_t size;
            file.read(reinterpret_cast<char*>(&size), sizeof(size));

            std::vector<uint8_t> buffer(size);
            file.read(reinterpret_cast<char*>(buffer.data()), size);

            fluxdb::Deserializer reader(buffer);
            fluxdb::Document doc = reader.deserialize();

            indexer.addDocument(id, doc);
            db.emplace(id, std::move(doc));
        }
        std::cout << "[Snapshot] Loaded " << count << " documents.\n";
    }

    void recover() {

        std::cout << "[Recovery] Checking snapshot: " << full_snapshot_path << "\n";
        load_internal(full_snapshot_path);

        std::ifstream file(full_wal_path, std::ios::binary);
        if (!file.is_open()) return;

        if (file.peek() == EOF) {
            std::cout << "[Recovery] WAL is empty. Ready.\n";
            return;
        }

        std::cout << "[Recovery] Replaying WAL...\n";
        int count = 0;

        while (file.peek() != EOF) {
            char opCode;
            file.get(opCode);
            if (file.eof()) break;

            Id id;
            file.read(reinterpret_cast<char*>(&id), sizeof(id));

            if (id >= next_id) next_id = id + 1;

            if (opCode == 0x01) { // INSERT/UPDATE
                uint32_t size;
                file.read(reinterpret_cast<char*>(&size), sizeof(size));
                std::vector<uint8_t> buffer(size);
                file.read(reinterpret_cast<char*>(buffer.data()), size);

                fluxdb::Deserializer reader(buffer);
                fluxdb::Document doc = reader.deserialize();

                if (db.count(id)) update_internal(id, doc);
                else insert_internal(id, std::move(doc));

            } else if (opCode == 0x02) { // DELETE
                remove_internal(id);
            }
            count++;
        }
        std::cout << "[Recovery] Replayed " << count << " operations.\n";
    }

    // --- BACKGROUND TASKS ---

    void janitorTask() {
        while (running) {
            std::unique_lock<std::mutex> lk(cv_m);
            if (cv.wait_for(lk, std::chrono::seconds(5), [this]{ return !running; })) break;

            long size = 0;
            {
                std::shared_lock lock(rw_lock);
                size = wal_file.tellp();
            }

            if (size > MAX_WAL_SIZE) {
                std::cout << "[Janitor] WAL too large (" << size << " bytes). Checkpointing...\n";
                checkpoint();
            }
        }
    }

    void ttlTask() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 10Hz Tick

            std::vector<Id> deadIds = expiry_manager.getExpiredIds();
            if (!deadIds.empty()) {
                std::unique_lock lock(rw_lock);
                for (Id id : deadIds) {
                    if (remove_internal(id)) {
                        logOperation(0x02, id); // Log deletion
                        std::cout << "[TTL] Expired Document ID: " << id << "\n";
                    }
                }
            }
        }
    }

    int getDynamicThreshold() {
        size_t count = db.size();
        if (count < 100) return 2;
        return static_cast<int>(std::log10(count)) + 2;
    }

public:
    Collection(std::string name, std::string storageDir) : db_name(name) {
        full_wal_path = storageDir + "/" + db_name + ".wal";
        full_snapshot_path = storageDir + "/" + db_name + ".flux";

        recover();
        
        wal_file.open(full_wal_path, std::ios::binary | std::ios::app);

        janitor_thread = std::thread(&Collection::janitorTask, this);
        ttl_thread     = std::thread(&Collection::ttlTask, this);
    }

    ~Collection() {
        running = false;
        cv.notify_all();
        if (janitor_thread.joinable()) janitor_thread.join();
        if (ttl_thread.joinable())     ttl_thread.join();
    }

    void checkpoint() {
        std::unique_lock lock(rw_lock);
        std::cout << "[Checkpoint] Saving snapshot to " << full_snapshot_path << "...\n";

        save_internal(full_snapshot_path);

        wal_file.close();
        wal_file.open(full_wal_path, std::ios::binary | std::ios::out | std::ios::trunc);
        wal_file.close();
        wal_file.open(full_wal_path, std::ios::binary | std::ios::app);

        std::cout << "[Checkpoint] Complete. WAL truncated.\n";
    }

    // --- PUBLIC API ---

    void insert(Id id, const fluxdb::Document& doc) {
        std::unique_lock lock(rw_lock);
        insert_internal(id, doc);
    }

    Id insert(const fluxdb::Document& doc) {
        std::unique_lock lock(rw_lock);
        Id id = next_id++;
        insert_internal(id, doc);
        return id;
    }

    Id insert(fluxdb::Document&& doc) {
        std::unique_lock lock(rw_lock);
        Id id = next_id++;
        insert_internal(id, std::move(doc));
        return id;
    }

    std::optional<std::reference_wrapper<const fluxdb::Document>> getById(Id id) const {
        std::shared_lock lock(rw_lock);
        auto it = db.find(id);
        if (it != db.end()) return it->second;
        return std::nullopt;
    }

    bool update(Id id, const fluxdb::Document& doc) {
        std::unique_lock lock(rw_lock);
        if (db.find(id) == db.end()) return false;
        std::vector<uint8_t> serializedDoc = serializer.serialize(doc);
        logOperation(0x01, id, serializedDoc);
        update_internal(id, doc);
        return true;
    }

    bool removeById(Id id) {
        std::unique_lock lock(rw_lock);
        if (db.find(id) == db.end()) return false;
        logOperation(0x02, id);
        remove_internal(id);
        return true;
    }

    void expire(Id id, int seconds) {
        expiry_manager.setTTL(id, seconds);
    }

    // --- SEARCH & INDEXING ---

    void createIndex(const std::string& field, int type = 0) {
        std::unique_lock lock(rw_lock);
        indexer.createIndex(field, type);
        
        // Backfill
        for (const auto& [id, doc] : db) {
            auto it = doc.find(field);
            if (it != doc.end()) {
                indexer.addToIndex(field, id, *it->second);
            }
        }
    }

    std::vector<Id> find(const std::string& field, const fluxdb::Value& value) {
        std::shared_lock lock(rw_lock);
        return indexer.searchHash(field, value);
    }

    std::vector<Id> findRange(const std::string& field, const fluxdb::Value& min, const fluxdb::Value& max) {
        std::shared_lock lock(rw_lock);
        return indexer.searchSorted(field, min, max);
    }

    // Universal Scan
    std::vector<Id> findAll(std::function<bool(const fluxdb::Document&)> predicate) {
        std::shared_lock lock(rw_lock);
        std::vector<Id> results;
        results.reserve(db.size() / 4); 

        for (const auto& [id, doc] : db) {
            if (predicate(doc)) {
                results.push_back(id);
            }
        }
        return results;
    }

    // Adaptive Reporting
    void reportQueryMiss(const std::string& field, bool isRangeQuery = false) {
        if (!adaptive_mode) return;
        std::unique_lock lock(rw_lock); 

        if (indexer.hasIndex(field)) return;

        miss_counter[field]++;
        if (isRangeQuery) needs_sorted_index[field] = true;
        
        int current_threshold = getDynamicThreshold();
        
        if (miss_counter[field] >= current_threshold) {
            int type = 0; 
            if (needs_sorted_index[field]) type = 1;

            std::cout << "[Adaptive] Hot field '" << field << "' hit threshold. Indexing...\n";
            indexer.createIndex(field, type);

            for (const auto& [id, doc] : db) {
                auto it = doc.find(field);
                if (it != doc.end()) indexer.addToIndex(field, id, *it->second);
            }
            
            miss_counter[field] = 0;
            needs_sorted_index[field] = false;
            std::cout << "[Adaptive] Index built.\n";
        }
    }

    void setAdaptive(bool enabled) {
        adaptive_mode = enabled;
        std::cout << "[Config] Adaptive Indexing set to " << (enabled ? "ON" : "OFF") << "\n";
    }

    // --- MAINTENANCE ---

    void clear() {
        std::unique_lock lock(rw_lock);
        db.clear();
        indexer.clear();
        next_id = 1; 
        
        std::cout << "[Maintenance] DB Flushed.\n";
        save_internal(full_snapshot_path);
        
        // Truncate WAL
        wal_file.close();
        wal_file.open(full_wal_path, std::ios::binary | std::ios::out | std::ios::trunc);
        wal_file.close();
        wal_file.open(full_wal_path, std::ios::binary | std::ios::app);
    }

    std::string getStats() {
        std::shared_lock lock(rw_lock);
        std::string json = "{";
        json += "\"database\": \"" + db_name + "\", "; 
        json += "\"documents\": " + std::to_string(db.size()) + ", ";
        json += "\"next_id\": " + std::to_string(next_id) + ", ";
        json += "\"adaptive_mode\": " + std::string(adaptive_mode ? "true" : "false") + ", ";
        
        std::set<std::string> unique_fields;
        int limit = 50; 
        int count = 0;
        for (const auto& [id, doc] : db) {
            for (const auto& [key, val] : doc) unique_fields.insert(key);
            count++;
            if (count >= limit) break;
        }
        
        json += "\"fields\": [";
        auto it = unique_fields.begin();
        while (it != unique_fields.end()) {
            json += "\"" + *it + "\"";
            if (++it != unique_fields.end()) json += ", ";
        }
        json += "]}";
        return json;
    }

    void close() {
        std::unique_lock lock(rw_lock);
        running = false; // Stop threads
        
        if (janitor_thread.joinable()) janitor_thread.join();
        if (ttl_thread.joinable())     ttl_thread.join();
        
        wal_file.close(); // release locks
        std::cout << "[Collection] Closed '" << db_name << "' resources.\n";
    }
};

#endif