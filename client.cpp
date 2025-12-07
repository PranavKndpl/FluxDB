#include <iostream>
#include <string>
#include <winsock2.h>
#include <ws2tcpip.h>

#pragma comment(lib, "Ws2_32.lib")

#define PORT 8080
#define SERVER_IP "127.0.0.1"

void start_shell(SOCKET sock) {
    char buffer[4096];
    std::string input;

    std::cout << "FluxDB v1.0.0\n";
    std::cout << "Connected to " << SERVER_IP << ":" << PORT << "\n";
    std::cout << "Type 'EXIT' to quit.\n\n";

    while (true) {
        std::cout << "flux> ";
        std::getline(std::cin, input);

        if (input == "EXIT" || input == "exit") break;
        if (input.empty()) continue;

        send(sock, input.c_str(), input.size(), 0);

        int bytes = recv(sock, buffer, 4096, 0);
        if (bytes <= 0) {
            std::cout << "Server disconnected.\n";
            break;
        }

        buffer[bytes] = '\0';
        std::cout << buffer; 
    }
}

int main() {
    // Init Winsock
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "Startup Failed.\n";
        return 1;
    }

    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        std::cerr << "Socket creation failed.\n";
        return 1;
    }

    // Connect
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(PORT);
    inet_pton(AF_INET, SERVER_IP, &serverAddr.sin_addr);

    if (connect(sock, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::cerr << "Connection failed. Is the server running?\n";
        return 1;
    }

    start_shell(sock);

    closesocket(sock);
    WSACleanup();
    return 0;
}