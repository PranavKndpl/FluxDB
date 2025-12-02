#ifndef INDEX_MANAGER_HPP
#define INDEX_MANAGER_HPP

#include "document.hpp"
#include <map>              //multimap (Sorted Index)
#include <unordered_map>    // unordered multimap (Hash Index)
#include <vector>
#include <string>
#include <iostream>

namespace fluxdb {

// B-Tree 
using SortedIndex = std::multimap<Value, uint64_t, ValueLess>;

// Hash Table 
using HashIndex   = std::unordered_multimap<Value, uint64_t, ValueHasher>;

class IndexManager {
private:
    std::unordered_map<std::string, SortedIndex> sorted_indexes;
    std::unordered_map<std::string, HashIndex>   hash_indexes;

public:
    // Type: 0 = Hash (Default), 1 = Sorted
    void createIndex(const std::string& field, int type = 0) {
        if (type == 1) {
            if (sorted_indexes.find(field) == sorted_indexes.end()) {
                sorted_indexes[field] = SortedIndex();
                std::cout << "[Index] Created SORTED index on '" << field << "'\n";
            }
        } else {
            if (hash_indexes.find(field) == hash_indexes.end()) {
                hash_indexes[field] = HashIndex();
                std::cout << "[Index] Created HASH index on '" << field << "'\n";
            }
        }
    }

    // Data Hooks 
    void addDocument(uint64_t docId, const Document& doc) {
        for (const auto& [key, valPtr] : doc) {
            if (!valPtr) continue; // Skip null pointers

            // for Sorted Index
            if (auto it = sorted_indexes.find(key); it != sorted_indexes.end()) {
                it->second.insert({ *valPtr, docId });
            }

            // for Hash Index
            if (auto it = hash_indexes.find(key); it != hash_indexes.end()) {
                it->second.insert({ *valPtr, docId });
            }
        }
    }

    void removeDocument(uint64_t docId, const Document& doc) {
        for (const auto& [key, valPtr] : doc) {
            if (!valPtr) continue;

            // Remove from Sorted Index
            if (auto it = sorted_indexes.find(key); it != sorted_indexes.end()) {
                removeFromMultimap(it->second, *valPtr, docId);
            }

            // Remove from Hash Index
            if (auto it = hash_indexes.find(key); it != hash_indexes.end()) {
                removeFromMultimap(it->second, *valPtr, docId);
            }
        }
    }

    // --- Query Engine ---
    std::vector<uint64_t> searchHash(const std::string& field, const Value& val) {

        std::vector<uint64_t> results;
        
        auto it = hash_indexes.find(field);
        if (it == hash_indexes.end()) return results; 

        const auto& index = it->second;

        auto range = index.equal_range(val);
        for (auto rit = range.first; rit != range.second; ++rit) {
            results.push_back(rit->second);
        }
        return results;
    }

private:
    template <typename MapType>
    void removeFromMultimap(MapType& index, const Value& val, uint64_t docId) {
    
        auto range = index.equal_range(val);
        
        for (auto it = range.first; it != range.second; ) {
            if (it->second == docId) {
                it = index.erase(it); 
                return; 
                ++it;
            }
        }
    }
};

}
#endif