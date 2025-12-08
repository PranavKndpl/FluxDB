#ifndef STORAGE_ENGINE_HPP
#define STORAGE_ENGINE_HPP

#include "document.hpp"
#include "index_manager.hpp"
#include <unordered_map>
#include <vector>
#include <string>
#include <cmath>
#include <iostream>
#include <set>

namespace fluxdb {

using Id = std::uint64_t;

class StorageEngine {
private:
    std::unordered_map<Id, Document> db;
    IndexManager indexer;
    Id next_id = 1;

    // Adaptive State
    bool adaptive_mode = false;
    std::unordered_map<std::string, int> miss_counter;
    std::unordered_map<std::string, bool> needs_sorted_index;

    int getDynamicThreshold() {
        size_t count = db.size();
        if (count < 100) return 2;
        return static_cast<int>(std::log10(count)) + 2;
    }

public:
    // --- CRUD --- no locks
    
    const Document* get(Id id) const {
        auto it = db.find(id);
        if (it != db.end()) return &it->second;
        return nullptr;
    }

    void insert(Id id, const Document& doc) {
        indexer.addDocument(id, doc);
        db.emplace(id, doc);
        if (id >= next_id) next_id = id + 1;
    }
    
    // For auto-increment
    Id insert(const Document& doc) {
        Id id = next_id++;
        insert(id, doc);
        return id;
    }

    bool update(Id id, const Document& doc) {
        auto it = db.find(id);
        if (it == db.end()) return false;
        
        indexer.removeDocument(id, it->second);
        it->second = doc;
        indexer.addDocument(id, doc);
        return true;
    }

    bool remove(Id id) {
        auto it = db.find(id);
        if (it == db.end()) return false;
        
        indexer.removeDocument(id, it->second);
        db.erase(it);
        return true;
    }

    void clear() {
        db.clear();
        indexer.clear();
        next_id = 1;
    }
    
    size_t size() const { return db.size(); }
    Id getNextId() const { return next_id; }
    void setNextId(Id id) { next_id = id; }
    
    // Iterators for Persistence Manager to read data
    auto begin() const { return db.begin(); }
    auto end() const { return db.end(); }

    // --- SEARCH & INDEXING ---

    void createIndex(const std::string& field, int type) {
        indexer.createIndex(field, type);
        // backfill
        for (const auto& [id, doc] : db) {
            auto it = doc.find(field);
            if (it != doc.end()) indexer.addToIndex(field, id, *it->second);
        }
    }
    
    std::vector<Id> find(const std::string& field, const Value& val) {
        return indexer.searchHash(field, val);
    }

    std::vector<Id> findRange(const std::string& field, const Value& min, const Value& max) {
        return indexer.searchSorted(field, min, max);
    }
    
    bool hasIndex(const std::string& field) const {
        return indexer.hasIndex(field);
    }

    // --- ADAPTIVE LOGIC ---
    
    void setAdaptive(bool enabled) { adaptive_mode = enabled; }
    bool isAdaptive() const { return adaptive_mode; }

    void reportQueryMiss(const std::string& field, bool isRangeQuery) {
        if (!adaptive_mode) return;
        if (indexer.hasIndex(field)) return;

        miss_counter[field]++;
        if (isRangeQuery) needs_sorted_index[field] = true;

        int threshold = getDynamicThreshold();
        if (miss_counter[field] >= threshold) {
            int type = needs_sorted_index[field] ? 1 : 0;
            std::cout << "[Adaptive] Indexing '" << field << "' (Type " << type << ")...\n";
            createIndex(field, type);
            miss_counter[field] = 0;
            needs_sorted_index[field] = false;
        }
    }
    
    // --- SCHEMA SAMPLING ---
    std::vector<std::string> getSampleFields() const {
        std::set<std::string> fields;
        int limit = 50;
        int count = 0;
        for (const auto& [id, doc] : db) {
            for (const auto& [key, val] : doc) fields.insert(key);
            if (++count >= limit) break;
        }
        return std::vector<std::string>(fields.begin(), fields.end());
    }
};

}

#endif