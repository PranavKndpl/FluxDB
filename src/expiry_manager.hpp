#ifndef EXPIRY_MANAGER_HPP
#define EXPIRY_MANAGER_HPP

#include <iostream>
#include <queue>
#include <vector>
#include <chrono>
#include <unordered_map>
#include <mutex>
#include <functional>

namespace fluxdb {

using Clock = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Clock>;

struct ExpiryEntry {
    TimePoint expiresAt;
    uint64_t docId;

    // Operator > for Min-Heap, smallest time is greater priority
    bool operator>(const ExpiryEntry& other) const {
        return expiresAt > other.expiresAt;
    }
};

class ExpiryManager {
private:
    //Min-Heap
    std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> queue;
    
    // Fast lookup to check if an ID has a TTL (for updates/removals)
    std::unordered_map<uint64_t, TimePoint> active_ttls;
    
    std::mutex lock;

public:
    void setTTL(uint64_t docId, int seconds) {
        std::lock_guard<std::mutex> lk(lock);
        
        TimePoint expiry = Clock::now() + std::chrono::seconds(seconds);
        
        // Add to tracking map
        active_ttls[docId] = expiry;
        
        queue.push({expiry, docId});
    }

    std::vector<uint64_t> getExpiredIds() {
        std::lock_guard<std::mutex> lk(lock);
        std::vector<uint64_t> expired;
        
        TimePoint now = Clock::now();

        while (!queue.empty()) {
            const ExpiryEntry& top = queue.top();
            
            if (top.expiresAt > now) {
                break;
            }

            auto it = active_ttls.find(top.docId);
            if (it != active_ttls.end() && it->second == top.expiresAt) {
                expired.push_back(top.docId);
                active_ttls.erase(it); // Remove from active map
            }
            
            queue.pop(); // Remove from heap
        }

        return expired;
    }

    // Called if user manually deletes/updates a doc, preventing ghost expirations
    void removeTTL(uint64_t docId) {
        std::lock_guard<std::mutex> lk(lock);
        active_ttls.erase(docId);
        // Note: We don't remove from Heap (too slow O(N)). 
        // We let the "Valid check" in getExpiredIds handle the stale entry later.
    }
};

}

#endif