#ifndef DATABASE_MANAGER_HPP
#define DATABASE_MANAGER_HPP

#include "collection.hpp"
#include <unordered_map>
#include <string>
#include <memory>
#include <mutex>
#include <filesystem> 
#include <iostream>

namespace fs = std::filesystem;

namespace fluxdb {

class DatabaseManager {
private:
    // Name -> Collection
    std::unordered_map<std::string, std::unique_ptr<Collection>> databases;
    
    std::mutex lock;
    const std::string DATA_FOLDER;

public:
    DatabaseManager(std::string path = "data") : DATA_FOLDER(path) {
        if (!fs::exists(DATA_FOLDER)) {
            fs::create_directory(DATA_FOLDER);
            std::cout << "[DB Manager] Created '" << DATA_FOLDER << "' directory.\n";
        }
    }

    Collection* getDatabase(const std::string& name, bool* was_created = nullptr) {
        std::lock_guard<std::mutex> lk(lock);
        
        if (databases.find(name) == databases.end()) {
            std::cout << "[DB Manager] Creating new DB: '" << name << "'\n";
            databases[name] = std::make_unique<Collection>(name, DATA_FOLDER);
            
            if (was_created) *was_created = true; 
        } else {
            if (was_created) *was_created = false;
        }
        
        return databases[name].get();
    }

    bool dropDatabase(const std::string& name) {
        std::lock_guard<std::mutex> lk(lock);

        auto it = databases.find(name);
        if (it == databases.end()) return false;

        if (name == "default") {
            std::cout << "[Error] Cannot drop default database.\n";
            return false;
        }

        it->second->close();

        databases.erase(it);

        std::string wal = DATA_FOLDER + "/" + name + ".wal";
        std::string snap = DATA_FOLDER + "/" + name + ".flux";
        
        try {
            if (fs::exists(wal)) fs::remove(wal);
            if (fs::exists(snap)) fs::remove(snap);
            std::cout << "[DB Manager] Dropped database '" << name << "'\n";
            return true;
        } catch (const std::exception& e) {
            std::cerr << "[Error] File deletion failed: " << e.what() << "\n";
            return false;
        }
    }

    std::vector<std::string> listDatabases() {
        std::lock_guard<std::mutex> lk(lock);
        std::vector<std::string> names;
        for (const auto& [name, ptr] : databases) {
            names.push_back(name);
        }
        return names;
    }
};

}

#endif