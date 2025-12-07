#include <iostream>
#include <unordered_map>
#include <optional>
#include <cstdint>
#include "document.hpp"
#include "index_manager.hpp"
#include "serializer.hpp"
#include <fstream>

#include<mutex>
#include<shared_mutex>
#include<condition_variable>
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
    std::atomic<bool> running{true};  // Flag 
    const size_t MAX_WAL_SIZE = 100 ; // 10 MB limit

    void logOperation(uint8_t opCode, Id id, const std::vector<uint8_t>& data = {}) {
        if (!wal_file.is_open()) return;

        wal_file.put(static_cast<char>(opCode));

        wal_file.write(reinterpret_cast<const char*>(&id), sizeof(id));

        if (opCode == 0x01) {
            uint32_t size = static_cast<uint32_t>(data.size());
            wal_file.write(reinterpret_cast<const char*>(&size), sizeof(size));
            wal_file.write(reinterpret_cast<const char*>(data.data()), size);
        }

        wal_file.flush(); // immediate write from buffer
    }

    void recover() {
        std::unique_lock lock(rw_lock);

        // PHASE 1: Load Snapshot (The Base State)
        std::cout << "[Recovery] Checking for snapshot...\n";
        
        std::ifstream snapshot("snapshot.flux", std::ios::binary);
        if (snapshot.is_open()) {
            std::cout << "[Recovery] Found snapshot. Loading...\n";
            snapshot.close(); // Close so load() can open it
            load_internal("snapshot.flux"); 
        } else {
            std::cout << "[Recovery] No snapshot found. Starting fresh.\n";
        }

        // PHASE 2: Replay WAL (The Recent History)
        std::ifstream file("wal.log", std::ios::binary);
        if (!file.is_open()) return; 

        // Check if file is empty
        if (file.peek() == EOF) {
            std::cout << "[Recovery] WAL is empty. Ready.\n";
            return;
        }

        std::cout << "[Recovery] Replaying WAL...\n";
        int count = 0;

        while (file.peek() != EOF) {
            char opCode;
            file.get(opCode);

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

                auto it = db.find(id);
                if (it != db.end()) {
                    // update
                    indexer.removeDocument(id, it->second);
                    it->second = doc; // Overwrite data
                } else {
                    // new insert
                    db.emplace(id, doc);
                }
                // Update index 
                indexer.addDocument(id, doc);

            } else if (opCode == 0x02) { // DELETE
                // SILENT DELETE
                auto it = db.find(id);
                if (it != db.end()) {
                    indexer.removeDocument(id, it->second);
                    db.erase(it);
                }
            }
            count++;
        }

        std::cout << "[Recovery] Replayed " << count << " operations.\n";
    }

    void janitorTask() {
        while (running) {
            // Smart Sleep: Waits 5s OR until notified
            std::unique_lock<std::mutex> lk(cv_m);
            if (cv.wait_for(lk, std::chrono::seconds(5), [this]{ return !running; })) {
                break; // Woke up because running == false
            }

            long size = 0;
            {   // Use unique_lock because tellp() on a writing stream isn't thread-safe
                std::unique_lock lock(rw_lock); 
                size = wal_file.tellp();
            } // Lock releases here


            if (size > MAX_WAL_SIZE) {
                std::cout << "[Janitor] WAL is " << size << " bytes. Compacting...\n";
                checkpoint(); // checkpoint() handles its own locking, so we call it directly
            }
        }
    }

    // No Locks Here
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

    void save_internal(const std::string& filename) {
        fluxdb::Serializer writer;
        
        std::ofstream file(filename, std::ios::binary | std::ios::out);
        if (!file.is_open()) throw std::runtime_error("Cannot open file: " + filename);

        file.write(reinterpret_cast<const char*>(&next_id), sizeof(next_id));

        //Write Count of documents
        uint64_t count = db.size();
        file.write(reinterpret_cast<const char*>(&count), sizeof(count));

        //Write each Document
        for (const auto& [id, doc] : db) {
            // Serialize the doc
            std::vector<uint8_t> bytes = writer.serialize(doc);
            
            //Write ID (So we keep the same ID)
            file.write(reinterpret_cast<const char*>(&id), sizeof(id));
            
            // Write Size of this doc's binary blob
            uint32_t docSize = static_cast<uint32_t>(bytes.size());
            file.write(reinterpret_cast<const char*>(&docSize), sizeof(docSize));
            
            // Write the blob
            file.write(reinterpret_cast<const char*>(bytes.data()), docSize);
        }
        
        std::cout << "[Snapshot] Saved " << count << " documents to " << filename << "\n";
    }

    void load_internal(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary | std::ios::in);
        if (!file.is_open()) return;

        db.clear();
        indexer.clear();

        uint64_t count = 0;
        file.read(reinterpret_cast<char*>(&count), sizeof(count));
        
        // Read Next ID 
        file.read(reinterpret_cast<char*>(&next_id), sizeof(next_id));

        // Load Loop
        for (uint64_t i = 0; i < count; ++i) {
            Id id;
            file.read(reinterpret_cast<char*>(&id), sizeof(id));

            uint32_t size; //docSize
            file.read(reinterpret_cast<char*>(&size), sizeof(size));

            //read blob
            std::vector<uint8_t> buffer(size);
            file.read(reinterpret_cast<char*>(buffer.data()), size);

            fluxdb::Deserializer reader(buffer);
            fluxdb::Document doc = reader.deserialize();

            // DIRECT INSERTION (Avoids Locks & Logging)
            indexer.addDocument(id, doc); //using insert method so indexes get rebuilt (addDocument method)
            db.emplace(id, std::move(doc));
        }
        
        std::cout << "[Snapshot] Loaded " << count << " documents.\n";
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
        cv.notify_all(); // WAKE UP!
        if (janitor_thread.joinable()) janitor_thread.join(); // join to let it finish the task first
    }

    //consolidate Memory to Disk and wipe the Log
    void checkpoint() {
        std::unique_lock lock(rw_lock);
        std::cout << "[Checkpoint] Starting...\n";

        save_internal("snapshot.flux");

        wal_file.close();

        wal_file.open("wal.log", std::ios::binary | std::ios::out | std::ios::trunc); // trunc mode to wipe the logs

        wal_file.close();
        wal_file.open("wal.log", std::ios::binary | std::ios::app);

        std::cout << "[Checkpoint] Complete. WAL truncated.\n";
    }

   // Manual ID (Copy)
   void insert(Id id, const fluxdb::Document& doc) {
        std::unique_lock lock(rw_lock); 
        insert_internal(id, doc);       
    }

    // Manual ID (Move)
    void insert(Id id, fluxdb::Document&& doc) {
        std::unique_lock lock(rw_lock);
        insert_internal(id, std::move(doc));
    }

    // Auto-ID (Copy)
    Id insert(const fluxdb::Document& doc) {
        std::unique_lock lock(rw_lock); 
        Id id = next_id++;              
        insert_internal(id, doc);       
        return id;
    }

    // Auto-ID (Move)
    Id insert(fluxdb::Document&& doc) {
        std::unique_lock lock(rw_lock);
        Id id = next_id++;
        insert_internal(id, std::move(doc));
        return id;
    }

    std::optional<std::reference_wrapper<const fluxdb::Document>> getById(Id id) const {
        std::shared_lock lock(rw_lock); // Allows multiple readers

        auto it = db.find(id);
        if (it != db.end())
            return it->second;  // returns reference_wrapper<const Document>
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

        auto it = db.find(id);
        if (it == db.end()) return false; 

        logOperation(0x02, id);
        indexer.removeDocument(id, it->second);

        db.erase(it);
        return true;
    }

    void createIndex(const std::string& field, int type = 0) {
        std::unique_lock lock(rw_lock);
        indexer.createIndex(field, type);
    }

    std::vector<Id> find(const std::string& field, const fluxdb::Value& value) {
        std::shared_lock lock(rw_lock);
        return indexer.searchHash(field, value);
    }

    std::vector<Id> findRange(const std::string& field, const fluxdb::Value& min, const fluxdb::Value& max) {
        std::shared_lock lock(rw_lock);
        return indexer.searchSorted(field, min, max);
    }

// ---------------------------------------------------------
// PERSISTENCE (Snapshots)

    void save(const std::string& filename) {
        std::shared_lock lock(rw_lock);
        save_internal(filename);
    }

    // Load state from disk 
    void load(const std::string& filename) {
        std::unique_lock lock(rw_lock); 
        load_internal(filename);       
    }
    
    void printDoc(Id id) const {
        std::shared_lock lock(rw_lock);

        auto it = db.find(id);
        if (it == db.end()) {
            std::cout << "Doc " << id << " not found.\n";
            return;
        }

        std::cout << "Doc " << id << ": { ";
        for (const auto& [key, valPtr] : it->second) {
            // valPtr is a shared_ptr, so we dereference it (*valPtr)
            std::cout << key << ": " << valPtr->ToString() << ", ";
        }
        std::cout << "}\n";
    }
};
