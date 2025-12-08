#ifndef PERSISTENCE_MANAGER_HPP
#define PERSISTENCE_MANAGER_HPP

#include "storage_engine.hpp"
#include "serializer.hpp"
#include <string>
#include <fstream>
#include <vector>

namespace fluxdb {

class PersistenceManager {
private:
    std::string wal_path;
    std::string snapshot_path;
    std::ofstream wal_file;
    Serializer serializer;

public:
    PersistenceManager(const std::string& wal, const std::string& snap) 
        : wal_path(wal), snapshot_path(snap) 
    {
        wal_file.open(wal_path, std::ios::binary | std::ios::app);
    }
    
    void appendLog(uint8_t opCode, Id id, const Document& doc = {}) {
        if (!wal_file.is_open()) return;

        wal_file.put(static_cast<char>(opCode));
        wal_file.write(reinterpret_cast<const char*>(&id), sizeof(id));

        if (opCode == 0x01) { // INSERT/UPDATE 
            std::vector<uint8_t> data = serializer.serialize(doc);
            uint32_t size = static_cast<uint32_t>(data.size());
            wal_file.write(reinterpret_cast<const char*>(&size), sizeof(size));
            wal_file.write(reinterpret_cast<const char*>(data.data()), size);
        }
        wal_file.flush();
    }
    
    long getWalSize() {
        return wal_file.tellp();
    }

    void saveSnapshot(const StorageEngine& engine) {
        Serializer writer;
        std::ofstream file(snapshot_path, std::ios::binary | std::ios::out);
        if (!file.is_open()) return;

        // Write Header
        Id nextId = engine.getNextId();
        uint64_t count = engine.size();
        file.write(reinterpret_cast<const char*>(&nextId), sizeof(nextId));
        file.write(reinterpret_cast<const char*>(&count), sizeof(count));

        // Write All Docs
        for (auto it = engine.begin(); it != engine.end(); ++it) {
            Id id = it->first;
            const Document& doc = it->second;
            
            std::vector<uint8_t> bytes = writer.serialize(doc);
            uint32_t size = static_cast<uint32_t>(bytes.size());
            
            file.write(reinterpret_cast<const char*>(&id), sizeof(id));
            file.write(reinterpret_cast<const char*>(&size), sizeof(size));
            file.write(reinterpret_cast<const char*>(bytes.data()), size);
        }
        std::cout << "[Snapshot] Saved to " << snapshot_path << "\n";
    }
    
    void truncateWal() {
        wal_file.close();
        wal_file.open(wal_path, std::ios::binary | std::ios::out | std::ios::trunc); // Clear file
        wal_file.close();
        wal_file.open(wal_path, std::ios::binary | std::ios::app); // Reopen append
    }

    // Loads data FROM disk INTO the StorageEngine
    void recover(StorageEngine& engine) {
        // 1. Load Snapshot
        std::ifstream snap(snapshot_path, std::ios::binary);
        if (snap.is_open()) {
            engine.clear(); // Reset engine before load
            
            Id nextId;
            uint64_t count;
            snap.read(reinterpret_cast<char*>(&nextId), sizeof(nextId));
            snap.read(reinterpret_cast<char*>(&count), sizeof(count));
            
            engine.setNextId(nextId);

            for (uint64_t i = 0; i < count; ++i) {
                Id id;
                uint32_t size;
                snap.read(reinterpret_cast<char*>(&id), sizeof(id));
                snap.read(reinterpret_cast<char*>(&size), sizeof(size));
                
                std::vector<uint8_t> buf(size);
                snap.read(reinterpret_cast<char*>(buf.data()), size);
                
                Deserializer reader(buf);
                engine.insert(id, reader.deserialize());
            }
            std::cout << "[Recovery] Snapshot loaded (" << count << " docs).\n";
        }

        // 2. Replay WAL
        std::ifstream wal(wal_path, std::ios::binary);
        if (!wal.is_open()) return;

        int ops = 0;
        while (wal.peek() != EOF) {
            char opCode;
            wal.get(opCode);
            if (wal.eof()) break;

            Id id;
            wal.read(reinterpret_cast<char*>(&id), sizeof(id));
            
            // Adjust ID counter if needed
            if (id >= engine.getNextId()) engine.setNextId(id + 1);

            if (opCode == 0x01) { // Insert/Update
                uint32_t size;
                wal.read(reinterpret_cast<char*>(&size), sizeof(size));
                std::vector<uint8_t> buf(size);
                wal.read(reinterpret_cast<char*>(buf.data()), size);
                
                Deserializer reader(buf);
                Document doc = reader.deserialize();
                
                // If exists, update; else insert
                if (engine.get(id)) engine.update(id, doc);
                else engine.insert(id, doc);
                
            } else if (opCode == 0x02) { // Delete
                engine.remove(id);
            }
            ops++;
        }
        std::cout << "[Recovery] Replayed " << ops << " WAL ops.\n";
    }
};

}

#endif