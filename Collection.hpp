#include <iostream>
#include <unordered_map>
#include <optional>
#include <cstdint>
#include "document.hpp"
#include "index_manager.hpp"

using Id = std::uint64_t;

class Collection {
private:
    std::unordered_map<Id, fluxdb::Document> db;
    fluxdb::IndexManager indexer;

public:
    Collection() = default;

    explicit Collection(std::size_t capacity_hint) {
        db.reserve(capacity_hint);
    }

    void insert(Id id, const fluxdb::Document& doc) {
        indexer.addDocument(id,doc);

        db.emplace(id, doc); // copying
    }

    void insert(Id id, fluxdb::Document&& doc) { // for moving
        indexer.addDocument(id, doc);
        db.emplace(id, std::move(doc));
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
            it->second = doc;
            return true;
        }
        return false;
    }

    bool removeById(Id id) {
        auto it = db.find(id);
        if (it == db.end()) return false; 

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
