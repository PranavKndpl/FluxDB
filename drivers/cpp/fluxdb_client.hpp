#ifndef FLUXDB_CLIENT_HPP
#define FLUXDB_CLIENT_HPP

#include <iostream>
#include <string>
#include <vector>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <sstream>

#include "document.hpp"
#include "query_parser.hpp" 

#pragma comment(lib, "Ws2_32.lib")

namespace fluxdb {

using Id = std::uint64_t;

class FluxDBClient {
private:
    SOCKET sock = INVALID_SOCKET;
    std::string host;
    int port;

    // Helper: Send raw string, get raw string
    std::string sendCommand(const std::string& cmd) {
        if (sock == INVALID_SOCKET) throw std::runtime_error("Not connected");

        std::string payload = cmd + "\n";
        if (send(sock, payload.c_str(), payload.length(), 0) == SOCKET_ERROR) {
            throw std::runtime_error("Send failed");
        }

        // Receive Buffer
        char buffer[4096];
        std::string response;
        
        int bytes = recv(sock, buffer, 4096, 0);
        if (bytes > 0) {
            buffer[bytes] = '\0';
            response = std::string(buffer);

        }
        
        if (!response.empty() && response.back() == '\n') response.pop_back();
        
        return response;
    }

public:
    FluxDBClient(const std::string& h, int p) : host(h), port(p) {
        WSADATA wsaData;
        WSAStartup(MAKEWORD(2, 2), &wsaData);
        connectToServer();
    }

    ~FluxDBClient() {
        if (sock != INVALID_SOCKET) closesocket(sock);
        WSACleanup();
    }

    void connectToServer() {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in serverAddr{};
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(port);
        inet_pton(AF_INET, host.c_str(), &serverAddr.sin_addr);

        if (connect(sock, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
            std::cerr << "[Client] Connection failed.\n";
            sock = INVALID_SOCKET;
        } else {
            std::cout << "[Client] Connected to " << host << ":" << port << "\n";
        }
    }

    // --- API METHODS ---

    bool auth(const std::string& password) {
        std::string resp = sendCommand("AUTH " + password);
        return resp == "OK AUTHENTICATED";
    }

    bool use(const std::string& dbName) {
        std::string resp = sendCommand("USE " + dbName);
        return resp.find("OK SWITCHED_TO") == 0;
    }

    Id insert(const Document& doc) {
        
        Value v(doc); 
        std::string json = v.ToJson();
        
        std::string resp = sendCommand("INSERT " + json);

        if (resp.find("OK ID=") == 0) {
            return std::stoull(resp.substr(6));
        }
        return 0; 
    }

    std::vector<Document> find(const Document& query) {
        Value v(query);
        std::string resp = sendCommand("FIND " + v.ToJson());
        
        std::vector<Document> results;
        
        std::stringstream ss(resp);
        std::string line;
        
        if (!std::getline(ss, line) || line.find("OK") != 0) return results;

        while (std::getline(ss, line)) {
            if (line.find("ID ") == 0) {
                size_t jsonStart = line.find('{');
                if (jsonStart != std::string::npos) {
                    std::string jsonStr = line.substr(jsonStart);
                    
                    QueryParser parser(jsonStr);
                    results.push_back(parser.parseJSON());
                }
            }
        }
        return results;
    }
    
    std::string rawCommand(const std::string& cmd) {
        return sendCommand(cmd);
    }
};

}

#endif