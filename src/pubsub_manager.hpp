#ifndef PUBSUB_MANAGER_HPP
#define PUBSUB_MANAGER_HPP

#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <algorithm>
#include <winsock2.h>

namespace fluxdb {

class PubSubManager {
private:
    // Channel Name -> List of Sockets listening
    std::unordered_map<std::string, std::vector<SOCKET>> channels;
    
    // Socket -> List of Channels (For fast cleanup on disconnect)
    std::unordered_map<SOCKET, std::vector<std::string>> client_subscriptions;
    
    std::mutex lock;
    bool active = true; 

public:
    void setEnabled(bool enabled) {
        std::lock_guard<std::mutex> lk(lock);
        active = enabled;
        if (!active) {
            // Kick everyone out or just stop processing
            channels.clear();
            client_subscriptions.clear();
        }
        std::cout << "[PubSub] Module set to " << (active ? "ON" : "OFF") << "\n";
    }

    bool isEnabled() const { return active; }

    void subscribe(const std::string& channel, SOCKET client) {
        std::lock_guard<std::mutex> lk(lock);
        if (!active) return;

        // Add to Channel List
        auto& subs = channels[channel];
        // Avoid duplicates
        if (std::find(subs.begin(), subs.end(), client) == subs.end()) {
            subs.push_back(client);
            
            // Add to Client Lookup (for cleanup)
            client_subscriptions[client].push_back(channel);
            
            std::cout << "[PubSub] Client " << client << " subscribed to '" << channel << "'\n";
        }
    }

    // Returns number of clients who received the message
    int publish(const std::string& channel, const std::string& message) {
        std::lock_guard<std::mutex> lk(lock);
        if (!active) return 0;

        if (channels.find(channel) == channels.end()) return 0;

        const auto& subs = channels[channel];
        std::string formattedMsg = "MESSAGE " + channel + " " + message + "\n";
        
        int count = 0;
        for (SOCKET s : subs) {
            // send() is thread-safe on Winsock for different threads
            int res = send(s, formattedMsg.c_str(), formattedMsg.size(), 0);
            if (res != SOCKET_ERROR) {
                count++;
            }
        }
        return count;
    }

    void unsubscribeAll(SOCKET client) {
        std::lock_guard<std::mutex> lk(lock);
        
        // Find what channels this client is in
        if (client_subscriptions.find(client) == client_subscriptions.end()) return;

        const auto& my_channels = client_subscriptions[client];

        // Remove client from each channel
        for (const std::string& ch : my_channels) {
            auto& subs = channels[ch];
            subs.erase(std::remove(subs.begin(), subs.end(), client), subs.end());
        }

        // Clean up the client map
        client_subscriptions.erase(client);
        std::cout << "[PubSub] Cleaned up client " << client << "\n";
    }
};

}

#endif