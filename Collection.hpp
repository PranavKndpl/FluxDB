#include <iostream>
#include <unordered_map>
#include <optional>
#include <cstdint>
#include "document.hpp"
#include "index_manager.hpp"
#include "serializer.hpp"
#include <fstream>

#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>

using Id = std::uint64_t;

class Collection {
private:
    std::unordered_map<Id, fluxdb::Document> db;
    fluxdb::IndexManager indexer;
    std::ofstream wal_file;

    fluxdb::Serializer serializer;
    Id next_id = 1;

    mutable std::shared_mutex rw_lock;
    std::thread janitor_thread; 
    std::condition_variable cv;
    std::mutex cv_m;      
    std::atomic<bool> running{true};  
    const size_t MAX_WAL_SIZE = 10 * 1024 * 1024; // 10 MB limit

    // --- INTERNAL HELPERS (No Locks) ---

    void insert_internal(Id id, const fluxdb::Document& doc) {
        // Log to WAL
        std::vector<uint8_t> serializedDoc = serializer.serialize(doc);
        logOperation(0x01, id, serializedDoc);

        // Update Index
        indexer.addDocument(id, doc);

        // Store Data
        db.emplace(id, doc);
    }

    // Move overload for internal
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

    // Added this helper to fix duplication in recover() and removeById()
    bool remove_internal(Id id) {
        auto it = db.find(id);
        if (it == db.end()) return false;

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

        // 1. Write Next ID
        file.write(reinterpret_cast<const char*>(&next_id), sizeof(next_id));

        // 2. Write Count
        uint64_t count = db.size();
        file.write(reinterpret_cast<const char*>(&count), sizeof(count));

        // 3. Write Documents
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

        // 1. Read Next ID (FIXED: Order matched save_internal)
        file.read(reinterpret_cast<char*>(&next_id), sizeof(next_id));

        // 2. Read Count
        uint64_t count = 0;
        file.read(reinterpret_cast<char*>(&count), sizeof(count));
        
        // 3. Load Loop
        for (uint64_t i = 0; i < count; ++i) {
            Id id;
            file.read(reinterpret_cast<char*>(&id), sizeof(id));

            uint32_t size; 
            file.read(reinterpret_cast<char*>(&size), sizeof(size));

            std::vector<uint8_t> buffer(size);
            file.read(reinterpret_cast<char*>(buffer.data()), size);

            fluxdb::Deserializer reader(buffer);
            fluxdb::Document doc = reader.deserialize();

            // Direct insertion (No Lock, No Log)
            indexer.addDocument(id, doc); 
            db.emplace(id, std::move(doc));
        }
        
        std::cout << "[Snapshot] Loaded " << count << " documents.\n";
    }

    void recover() {
        // NOTE: This runs in constructor, so locks aren't strictly needed 
        // unless you allow multi-threaded startup. Keeping it simple.

        // PHASE 1: Load Snapshot
        std::cout << "[Recovery] Checking for snapshot...\n";
        load_internal("snapshot.flux"); 

        // PHASE 2: Replay WAL
        std::ifstream file("wal.log", std::ios::binary);
        if (!file.is_open()) return; 

        if (file.peek() == EOF) {
            std::cout << "[Recovery] WAL is empty. Ready.\n";
            return;
        }

        std::cout << "[Recovery] Replaying WAL...\n";
        int count = 0;

        while (file.peek() != EOF) {
            char opCode;
            file.get(opCode); // Read 1 byte

            if (file.eof()) break; // Safety check

            Id id;
            file.read(reinterpret_cast<char*>(&id), sizeof(id));

            if (id >= next_id) {
                next_id = id + 1;
            }

            if (opCode == 0x01) { // INSERT / UPDATE
                uint32_t size;
                file.read(reinterpret_cast<char*>(&size), sizeof(size));

                std::vector<uint8_t> buffer(size);
                file.read(reinterpret_cast<char*>(buffer.data()), size);

                fluxdb::Deserializer reader(buffer);
                fluxdb::Document doc = reader.deserialize();

                // Logic: If exists -> Update, Else -> Insert
                if (db.count(id)) {
                    update_internal(id, doc);
                } else {
                    insert_internal(id, std::move(doc));
                }

            } else if (opCode == 0x02) { // DELETE
                remove_internal(id);
            }
            count++;
        }

        std::cout << "[Recovery] Replayed " << count << " operations.\n";
    }

    void janitorTask() {
        while (running) {
            std::unique_lock<std::mutex> lk(cv_m);
            if (cv.wait_for(lk, std::chrono::seconds(5), [this]{ return !running; })) {
                break; 
            }

            long size = 0;
            {   
                std::shared_lock lock(rw_lock); // Shared lock is enough to read position
                size = wal_file.tellp();
            } 

            if (size > MAX_WAL_SIZE) {
                std::cout << "[Janitor] WAL is " << size << " bytes. Compacting...\n";
                checkpoint(); 
            }
        }
    }

public:
    Collection() {
        recover();
        wal_file.open("wal.log", std::ios::binary | std::ios::app);
        janitor_thread = std::thread(&Collection::janitorTask, this);
    }

    explicit Collection(std::size_t capacity_hint) {
        recover();
        db.reserve(capacity_hint);
        wal_file.open("wal.log", std::ios::binary | std::ios::app);
        janitor_thread = std::thread(&Collection::janitorTask, this);
    }

    ~Collection() {
        running = false;
        cv.notify_all(); 
        if (janitor_thread.joinable()) janitor_thread.join();
    }

    void checkpoint() {
        std::unique_lock lock(rw_lock);
        std::cout << "[Checkpoint] Starting...\n";

        save_internal("snapshot.flux");

        wal_file.close();
        wal_file.open("wal.log", std::ios::binary | std::ios::out | std::ios::trunc);
        wal_file.close();
        wal_file.open("wal.log", std::ios::binary | std::ios::app);

        std::cout << "[Checkpoint] Complete. WAL truncated.\n";
    }

    // --- PUBLIC API ---

    void insert(Id id, const fluxdb::Document& doc) {
        std::unique_lock lock(rw_lock); 
        insert_internal(id, doc);       
    }

    void insert(Id id, fluxdb::Document&& doc) {
        std::unique_lock lock(rw_lock);
        insert_internal(id, std::move(doc));
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

        // 1. Check existence first
        if (db.find(id) == db.end()) return false;

        // 2. Log (WAL First)
        std::vector<uint8_t> serializedDoc = serializer.serialize(doc);
        logOperation(0x01, id, serializedDoc);

        // 3. Update RAM
        update_internal(id, doc);
        return true;
    }

    bool removeById(Id id) {
        std::unique_lock lock(rw_lock);
        
        // 1. Check existence
        if (db.find(id) == db.end()) return false; 

        // 2. Log (WAL First)
        logOperation(0x02, id);
        
        // 3. Update RAM
        remove_internal(id);
        return true;
    }

    void createIndex(const std::string& field, int type = 0) {
        std::unique_lock lock(rw_lock); // Writes to index structure
        indexer.createIndex(field, type);

        //backfill,Scan existing data
        std::cout << "[Indexer] Backfilling index for '" << field << "'...\n";
        int count = 0;
        
        for (const auto& [id, doc] : db) {

            auto it = doc.find(field);
            if (it != doc.end()) {
                indexer.addToIndex(field, id, *it->second);
                count++;
            }
        }
        std::cout << "[Indexer] Backfill complete. Added " << count << " entries.\n";
    }

    std::vector<Id> find(const std::string& field, const fluxdb::Value& value) {
        std::shared_lock lock(rw_lock);
        return indexer.searchHash(field, value);
    }

    std::vector<Id> findRange(const std::string& field, const fluxdb::Value& min, const fluxdb::Value& max) {
        std::shared_lock lock(rw_lock);
        return indexer.searchSorted(field, min, max);
    }

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

    void save(const std::string& filename) {
        std::shared_lock lock(rw_lock);
        save_internal(filename);
    }

    void load(const std::string& filename) {
        std::unique_lock lock(rw_lock);
        load_internal(filename);       
    }

    void clear() {
        std::unique_lock lock(rw_lock);
        
        db.clear();
        indexer.clear();
        
        std::cout << "[Maintenance] DB Flushed.\n";
        
        save_internal("snapshot.flux"); 
        wal_file.close();
        wal_file.open("wal.log", std::ios::binary | std::ios::out | std::ios::trunc);
        wal_file.close();
        wal_file.open("wal.log", std::ios::binary | std::ios::app);
    }
    
};