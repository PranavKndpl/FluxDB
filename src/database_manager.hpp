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
        
        if (databases.find(name) != databases.end()) {
            if (was_created) *was_created = false; 
            return databases[name].get();
        }

        std::string walPath = DATA_FOLDER + "/" + name + ".wal";
        std::string snapPath = DATA_FOLDER + "/" + name + ".flux";
        
        bool files_exist = fs::exists(walPath) || fs::exists(snapPath);

        std::cout << "[DB Manager] Loading DB: '" << name << "'...\n";
        databases[name] = std::make_unique<Collection>(name, DATA_FOLDER);
        
        if (was_created) {
            *was_created = !files_exist; 
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
        std::set<std::string> unique_names; // Use set to avoid duplicates (.wal + .flux)

        for (const auto& [name, ptr] : databases) {
            unique_names.insert(name);
        }
        try {
            if (fs::exists(DATA_FOLDER) && fs::is_directory(DATA_FOLDER)) {
                for (const auto& entry : fs::directory_iterator(DATA_FOLDER)) {
                    if (entry.is_regular_file()) {
                        std::string path = entry.path().string();
                        std::string filename = entry.path().stem().string(); // "crm_db" from "crm_db.wal"
                        std::string ext = entry.path().extension().string();

                        if (ext == ".wal" || ext == ".flux") {
                            unique_names.insert(filename);
                        }
                    }
                }
            }
        } catch (...) {
            std::cerr << "[Error] Could not scan data directory.\n";
        }

        names.assign(unique_names.begin(), unique_names.end());
        return names;
    }
};

}

#endif