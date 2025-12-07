#include <iostream>
#include <winsock2.h> 
#include <ws2tcpip.h>
#include <thread>
#include <vector>
#include "collection.hpp"
#include "query_parser.hpp"

// Link with Ws2_32.lib
#pragma comment(lib, "Ws2_32.lib")

using namespace fluxdb;

Collection db; // global instance is fine, its now thread safe

void handle_client(SOCKET clientSocket) {
    char buffer[4096];
    
    while (true) {
        int bytesReceived = recv(clientSocket, buffer, 4096, 0);
        if (bytesReceived <= 0) break;

        std::string request(buffer, bytesReceived);

        std::string response = "ERROR\n";
        
        try {
            if (request.rfind("INSERT ", 0) == 0) {

                std::string json = request.substr(7);
                QueryParser parser(json);
                Document doc = parser.parseJSON();
                
                Id id = db.insert(std::move(doc));
                response = "OK ID=" + std::to_string(id) + "\n";
                
            } else if (request.rfind("INDEX ", 0) == 0) {        
                std::stringstream ss(request.substr(6));
                std::string field;
                int type = 0;
                
                ss >> field;
                if (ss >> type) {
                    db.createIndex(field, type);
                    response = "OK INDEX_CREATED " + field + "\n";
                } else {
                    db.createIndex(field, 0);
                    response = "OK INDEX_CREATED " + field + " (Default Hash)\n";
                }
            } else if (request.rfind("FIND ", 0) == 0) {
                std::string json = request.substr(5);
                QueryParser parser(json);
                Document query = parser.parseJSON();
                
                auto it = query.begin();
                if (it != query.end()) {
                    // searchHash requires a Value, we dereference the shared_ptr
                    std::vector<Id> ids = db.find(it->first, *it->second);
                    response = "OK COUNT=" + std::to_string(ids.size()) + "\n";
                    for(Id id : ids) {
                        response += "ID " + std::to_string(id) + "\n";
                    }
                }
            } else {
                response = "UNKNOWN_COMMAND\n";
            }
        } catch (const std::exception& e) {
            response = std::string("ERROR ") + e.what() + "\n";
        }
        send(clientSocket, response.c_str(), response.size(), 0);
    }

    closesocket(clientSocket);
}

int main() {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed.\n";
        return 1;
    }

    SOCKET serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == INVALID_SOCKET) {
        std::cerr << "Socket creation failed.\n";
        return 1;
    }

    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(8080); // Port 8080

    if (bind(serverSocket, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::cerr << "Bind failed.\n";
        return 1;
    }

    if (listen(serverSocket, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "Listen failed.\n";
        return 1;
    }

    std::cout << "=== FluxDB Server Running on Port 8080 ===\n";

    while (true) {
        SOCKET clientSocket = accept(serverSocket, nullptr, nullptr);
        if (clientSocket == INVALID_SOCKET) {
            std::cerr << "Accept failed.\n";
            continue;
        }

        // Spawn a thread for each client
        std::thread clientThread(handle_client, clientSocket);
        clientThread.detach(); // Let it run independently
    }

    // Cleanup
    closesocket(serverSocket);
    WSACleanup();
    return 0;
}