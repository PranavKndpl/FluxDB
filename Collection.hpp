#include <iostream>
#include <unordered_map>
#include <optional>
#include <cstdint>
#include "document.hpp"
#include "index_manager.hpp"
#include "serializer.hpp"
#include <fstream>


using Id = std::uint64_t;

class Collection {
private:
    std::unordered_map<Id, fluxdb::Document> db;
    fluxdb::IndexManager indexer;
    std::ofstream wal_file;

    fluxdb::Serializer serializer;
    Id next_id = 1;

    //consolidate Memory to Disk and wipe the Log
    void checkpoint() {
        std::cout << "[Checkpoint] Starting...\n";

        save("snapshot.flux");

        wal_file.close();

        wal_file.open("wal.log", std::ios::binary | std::ios::out | std::ios::trunc); // trunc mode to wipe the logs

        wal_file.close();
        wal_file.open("wal.log", std::ios::binary | std::ios::app);

        std::cout << "[Checkpoint] Complete. WAL truncated.\n";
    }

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

        // PHASE 1: Load Snapshot (The Base State)
        std::ifstream snapshot("snapshot.flux", std::ios::binary);
        if (snapshot.is_open()) {
            std::cout << "[Recovery] Found snapshot. Loading...\n";
            snapshot.close(); // Close so load() can open it
            load("snapshot.flux"); 
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

public:
    Collection() {
        recover();
        wal_file.open("wal.log", std::ios::binary | std::ios::app);
    }

    explicit Collection(std::size_t capacity_hint) {
        recover();
        db.reserve(capacity_hint);
        wal_file.open("wal.log", std::ios::binary | std::ios::app);
    }

    void insert(Id id, const fluxdb::Document& doc) {
        indexer.addDocument(id,doc);

        db.emplace(id, doc); // copying

        std::vector<uint8_t> serializedDoc = serializer.serialize(doc);
        logOperation(0x01, id, serializedDoc);

    }

    void insert(Id id, fluxdb::Document&& doc) { // for moving
        indexer.addDocument(id, doc);

        std::vector<uint8_t> serializedDoc = serializer.serialize(doc);
        logOperation(0x01, id, serializedDoc); // serilizing before, doc becomes empty after move

        db.emplace(id, std::move(doc));

    }

    //auto-increment insert
    Id insert(const fluxdb::Document& doc) {
        Id assignedId = next_id++;
        insert(assignedId, doc); 
        return assignedId;
    }
    
    Id insert(fluxdb::Document&& doc) {
        Id assignedId = next_id++;
        insert(assignedId, std::move(doc));
        return assignedId;
    }

    std::optional<std::reference_wrapper<const fluxdb::Document>> getById(Id id) const {
        auto it = db.find(id);
        if (it != db.end())
            return it->second;  // returns reference_wrapper<const Document>
        return std::nullopt;
    }

    bool update(Id id, const fluxdb::Document& doc) {
        auto it = db.find(id);
        if (it != db.end()) {
            // Remove OLD values from Index (Critical!)
            // If you don't do this, the index will point to ID 101 for both age 25 AND 26.
            indexer.removeDocument(id, it->second);

            it->second = doc;

            indexer.addDocument(id, doc);

            std::vector<uint8_t> serializedDoc = serializer.serialize(doc);
            logOperation(0x01, id, serializedDoc);

            return true;
        }
        return false;
    }

    bool removeById(Id id) {
        auto it = db.find(id);
        if (it == db.end()) return false; 

        logOperation(0x02, id);
        indexer.removeDocument(id, it->second);

        db.erase(it);
        return true;
    }

    void createIndex(const std::string& field, int type = 0) {
        indexer.createIndex(field, type);
    }

    std::vector<Id> find(const std::string& field, const fluxdb::Value& value) {
        return indexer.searchHash(field, value);
    }

    std::vector<Id> findRange(const std::string& field, const fluxdb::Value& min, const fluxdb::Value& max) {
        return indexer.searchSorted(field, min, max);
    }

// ---------------------------------------------------------
// PERSISTENCE (Snapshots)

    void save(const std::string& filename) {
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

    // Load state from disk 
    void load(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary | std::ios::in);
        if (!file.is_open()) throw std::runtime_error("Cannot open file: " + filename);

        //Clear current state
        db.clear();
        indexer.clear();

        file.read(reinterpret_cast<char*>(&next_id), sizeof(next_id));

        //Read Count
        uint64_t count = 0;
        file.read(reinterpret_cast<char*>(&count), sizeof(count));

        //Read Loop
        for (uint64_t i = 0; i < count; ++i) {
            Id id;
            file.read(reinterpret_cast<char*>(&id), sizeof(id));

            uint32_t docSize;
            file.read(reinterpret_cast<char*>(&docSize), sizeof(docSize));

            //Read the blob
            std::vector<uint8_t> buffer(docSize);
            file.read(reinterpret_cast<char*>(buffer.data()), docSize);

            // Deserialize
            fluxdb::Deserializer reader(buffer);
            fluxdb::Document doc = reader.deserialize();

            // ** using insert method so indexes get rebuilt (addDocument method)
            this->insert(id, std::move(doc));
        }

        std::cout << "[Snapshot] Loaded " << count << " documents.\n";
    }
    
    void printDoc(Id id) const {
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
