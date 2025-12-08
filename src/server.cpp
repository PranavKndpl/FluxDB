#include <iostream>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <atomic>
#include <vector>
#include <filesystem>

#include "query_processor.hpp"
#include "pubsub_manager.hpp"
#include "database_manager.hpp"

#pragma comment(lib, "Ws2_32.lib")

namespace fs = std::filesystem; 
using namespace fluxdb;

DatabaseManager* manager_ptr = nullptr;
PubSubManager* pubsub_ptr = nullptr;
SOCKET serverSocket = INVALID_SOCKET;
std::string SERVER_PASSWORD = "flux_admin"; 
std::atomic<bool> is_running{true};

BOOL WINAPI ConsoleHandler(DWORD signal) {
    if (signal == CTRL_C_EVENT || signal == CTRL_CLOSE_EVENT) {
        std::cout << "\n[Server] Shutdown signal received. Cleaning up...\n";
        is_running = false;
        if (serverSocket != INVALID_SOCKET) {
            closesocket(serverSocket);
            serverSocket = INVALID_SOCKET;
        }
        return TRUE;
    }
    return FALSE;
}

void handle_client(SOCKET clientSocket) {
    if (!manager_ptr || !pubsub_ptr) { closesocket(clientSocket); return; }

    QueryProcessor processor(*manager_ptr, *pubsub_ptr, clientSocket, SERVER_PASSWORD); 
    
    DWORD timeout = 5000;
    setsockopt(clientSocket, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));

    std::string accumulator = ""; 
    char buffer[4096];
    
    while (is_running) {
        int bytes = recv(clientSocket, buffer, sizeof(buffer), 0);
        
        if (bytes == SOCKET_ERROR) {
            if (WSAGetLastError() == WSAETIMEDOUT) continue; 
            break;
        }
        if (bytes == 0) break; 

        accumulator.append(buffer, bytes);
        size_t pos = 0;
        while ((pos = accumulator.find('\n')) != std::string::npos) {
            std::string command = accumulator.substr(0, pos);
            accumulator.erase(0, pos + 1);

            if (!command.empty() && command.back() == '\r') command.pop_back();
            if (command.empty()) continue;

            std::string response;
            try {
                response = processor.process(command);
            } catch (const std::exception& e) {
                response = "ERROR INTERNAL\n";
            }
            send(clientSocket, response.c_str(), response.size(), 0);
        }
    }
    pubsub_ptr->unsubscribeAll(clientSocket);
    closesocket(clientSocket);
}

int main(int argc, char* argv[]) {
    if (!SetConsoleCtrlHandler(ConsoleHandler, TRUE)) return 1;

    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) return 1;


    int port = 8080;
    std::string dataPath = "data"; 

    if (argc > 1) port = std::stoi(argv[1]);

    if (argc > 2) {
        dataPath = argv[2];
    } 

    else if (fs::exists("../data") && fs::is_directory("../data")) {
        dataPath = "../data";
    }
    
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == INVALID_SOCKET) { WSACleanup(); return 1; }

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(port); 

    if (bind(serverSocket, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::cerr << "Bind failed (Port " << port << " in use?).\n";
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    if (listen(serverSocket, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "Listen failed.\n";
        return 1;
    }

    DatabaseManager dbManager(dataPath); 
    PubSubManager pubsub;

    manager_ptr = &dbManager; 
    pubsub_ptr = &pubsub;

    std::cout << "=== FluxDB Server Running on Port " << port << " ===\n";
    std::cout << "=== Storage Path: " << fs::absolute(dataPath) << " ===\n";
    std::cout << "=== Security: Enabled (Default pass: '" << SERVER_PASSWORD << "') ===\n";

    while (is_running) {
        SOCKET client = accept(serverSocket, nullptr, nullptr);
        if (client == INVALID_SOCKET) {
            if (!is_running) break; 
            continue;
        }
        std::thread(handle_client, client).detach();
    }

    std::cout << "[Server] Main loop finished.\n";
    manager_ptr = nullptr; 
    pubsub_ptr = nullptr;
    WSACleanup();
    std::cout << "[Server] Bye.\n";
    return 0;
}