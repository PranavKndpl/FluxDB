#ifndef COLLECTION_HPP
#define COLLECTION_HPP

#include <string>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <optional>   
#include <functional>  
#include <vector>      

#include "storage_engine.hpp"
#include "persistence_manager.hpp"
#include "expiry_manager.hpp"

namespace fluxdb {

class Collection {
private:
    std::string db_name;
    
    // workers
    StorageEngine storage;
    PersistenceManager persistence;
    ExpiryManager expiry_manager;

    // concurrency control
    mutable std::shared_mutex rw_lock;
    std::atomic<bool> running{true};
    
    // Threads & Sync
    std::thread janitor_thread;
    std::thread ttl_thread;
    std::condition_variable cv;
    std::mutex cv_m;

    const long MAX_WAL_SIZE = 10 * 1024 * 1024; // 10MB

    // --- BACKGROUND TASKS ---
    
    void janitorTask() {
        while (running) {
            std::unique_lock<std::mutex> lk(cv_m);
            if (cv.wait_for(lk, std::chrono::seconds(5), [this]{ return !running; })) break;

            // --- Thread-Safe Size Check ---
            bool needsCheckpoint = false;
            {
                // Lock access to shared resources before checking persistence
                std::shared_lock lock(rw_lock); 
                if (persistence.getWalSize() > MAX_WAL_SIZE) {
                    needsCheckpoint = true;
                }
            } 

            if (needsCheckpoint) {
                lk.unlock(); // Release CV lock
                std::cout << "[Janitor] Compacting WAL...\n";
                checkpoint();
            }
        }
    }

    void ttlTask() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            auto deadIds = expiry_manager.getExpiredIds();
            if (!deadIds.empty()) {
                std::unique_lock lock(rw_lock);
                for (Id id : deadIds) {
                    if (storage.get(id)) {
                        persistence.appendLog(0x02, id);
                        storage.remove(id);
                        std::cout << "[TTL] Removed ID " << id << "\n";
                    }
                }
            }
        }
    }

public:
    Collection(std::string name, std::string storageDir) 
        : db_name(name),
          persistence(storageDir + "/" + name + ".wal", storageDir + "/" + name + ".flux") 
    {
        persistence.recover(storage); // recover
        
        
        janitor_thread = std::thread(&Collection::janitorTask, this); // thn start threads
        ttl_thread     = std::thread(&Collection::ttlTask, this);
    }

    ~Collection() {
        close();
    }

    void close() {
        {
            std::unique_lock<std::mutex> lk(cv_m);
            if (!running) return;
            running = false;
            cv.notify_all(); // Wake up janitor so it can exit
        }
        
        if (janitor_thread.joinable()) janitor_thread.join();
        if (ttl_thread.joinable())     ttl_thread.join();
    }

    // --- CRUD OPERATIONS ---

    Id insert(Document&& doc) {
        std::unique_lock lock(rw_lock);
        Id id = storage.getNextId();
        
        persistence.appendLog(0x01, id, doc);
        
        storage.insert(id, std::move(doc));
        storage.setNextId(id + 1);
        
        return id;
    }

    void insert(Id id, const Document& doc) {
        std::unique_lock lock(rw_lock);
        persistence.appendLog(0x01, id, doc);
        storage.insert(id, doc);
    }

    bool update(Id id, const Document& doc) {
        std::unique_lock lock(rw_lock);
        if (!storage.get(id)) return false;
        
        persistence.appendLog(0x01, id, doc);
        storage.update(id, doc);
        return true;
    }

    bool removeById(Id id) {
        std::unique_lock lock(rw_lock);
        if (!storage.get(id)) return false;

        persistence.appendLog(0x02, id); 
        storage.remove(id);
        expiry_manager.removeTTL(id); 
        return true;
    }

    std::optional<std::reference_wrapper<const Document>> getById(Id id) const {
        std::shared_lock lock(rw_lock);
        const Document* doc = storage.get(id);
        if (doc) return *doc;
        return std::nullopt;
    }

    std::vector<Id> find(const std::string& field, const Value& val) {
        std::shared_lock lock(rw_lock);
        
        return storage.find(field, val);
    }

    std::vector<Id> findRange(const std::string& field, const Value& min, const Value& max) {
        std::shared_lock lock(rw_lock);
        return storage.findRange(field, min, max);
    }

    std::vector<Id> findAll(std::function<bool(const Document&)> predicate) {
        std::shared_lock lock(rw_lock);
        std::vector<Id> results;
        for (auto it = storage.begin(); it != storage.end(); ++it) {
            if (predicate(it->second)) {
                results.push_back(it->first);
            }
        }
        return results;
    }

    // --- UTILITIES ---

    void createIndex(const std::string& field, int type = 0) {
        std::unique_lock lock(rw_lock);
        storage.createIndex(field, type);
    }

    void expire(Id id, int seconds) {
        expiry_manager.setTTL(id, seconds);
    }

    void checkpoint() {
        std::unique_lock lock(rw_lock);
        std::cout << "[Checkpoint] Saving snapshot...\n";
        persistence.saveSnapshot(storage);
        persistence.truncateWal();
    }

    void setAdaptive(bool enabled) {
        std::unique_lock lock(rw_lock);
        storage.setAdaptive(enabled);
    }

    void reportQueryMiss(const std::string& field, bool isRange = false) { // for QueryProcessor
        std::unique_lock lock(rw_lock); 
        storage.reportQueryMiss(field, isRange);
    }

    std::string getStats() {
        std::shared_lock lock(rw_lock);
        std::string json = "{";
        json += "\"database\": \"" + db_name + "\", ";
        json += "\"documents\": " + std::to_string(storage.size()) + ", ";
        json += "\"adaptive_mode\": " + std::string(storage.isAdaptive() ? "true" : "false") + ", ";
        
        auto fields = storage.getSampleFields();
        json += "\"fields\": [";
        for (size_t i = 0; i < fields.size(); ++i) {
            json += "\"" + fields[i] + "\"";
            if (i < fields.size() - 1) json += ", ";
        }
        json += "]}";
        return json;
    }

    void clear() {
        std::unique_lock lock(rw_lock);
        
        storage.clear(); 
        
        std::cout << "[Maintenance] DB Flushed.\n";
        
        persistence.saveSnapshot(storage);
        persistence.truncateWal();
    }
};

}

#endif