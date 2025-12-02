#include <iostream>
#include <unordered_map>
#include <optional>
#include <cstdint>
#include "document.hpp"

using namespace fluxdb;

using Id = std::uint64_t;

class Collection {
private:
    std::unordered_map<Id, Document> db;

public:
    Collection() = default;

    explicit Collection(std::size_t capacity_hint) {
        db.reserve(capacity_hint);
    }

    void insert(Id id, const Document& doc) {
        db.emplace(id, doc);
    }

    void insert(Id id, Document&& doc) { // for moving
        db.emplace(id, std::move(doc));
    }

    std::optional<std::reference_wrapper<const Document>> getById(Id id) const {
        auto it = db.find(id);
        if (it != db.end())
            return it->second;  // returns reference_wrapper<const Document>
        return std::nullopt;
    }

    bool update(Id id, const Document& doc) {
        auto it = db.find(id);
        if (it != db.end()) {
            it->second = doc;
            return true;
        }
        return false;
    }

    bool removeById(Id id) {
        return db.erase(id) > 0;
    }
};
