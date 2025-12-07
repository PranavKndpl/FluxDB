#include <iostream>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include "query_processor.hpp"

#pragma comment(lib, "Ws2_32.lib")

using namespace fluxdb;

Collection db; // global instance is fine, its now thread safe

void handle_client(SOCKET clientSocket) {
    QueryProcessor processor(db); 
    char buffer[4096];
    
    while (true) {
        int bytes = recv(clientSocket, buffer, 4096, 0);
        if (bytes <= 0) break; 
        std::string request(buffer, bytes);
        std::string response = processor.process(request);

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
        WSACleanup();
        return 1;
    }

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(8080);

    if (bind(serverSocket, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::cerr << "Bind failed.\n";
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    if (listen(serverSocket, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "Listen failed.\n";
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    std::cout << "=== FluxDB Server Running on Port 8080 ===\n";

    while (true) {
        SOCKET clientSocket = accept(serverSocket, nullptr, nullptr);
        if (clientSocket == INVALID_SOCKET) {
            std::cerr << "Accept failed, retrying...\n";
            continue;
        }

        // Spawn detached thread for concurrency
        std::thread(handle_client, clientSocket).detach();
    }

    closesocket(serverSocket);
    WSACleanup();
    return 0;
}