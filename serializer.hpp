#ifndef SERIALIZER_HPP
#define SERIALIZER_HPP

#include "document.hpp"
#include <iostream>
#include <vector>
#include <cstring> 
#include <fstream>

namespace fluxdb {

class Serializer {
private:
    std::vector<uint8_t> buffer;

public:
    void writeByte(uint8_t b) {
        buffer.push_back(b);
    }

    void writeBytes(const void* data, size_t size) {
        const uint8_t* ptr = static_cast<const uint8_t*>(data);
        buffer.insert(buffer.end(), ptr, ptr + size);
    }

    void writeInt64(int64_t v) {
        writeBytes(&v, sizeof(v));
    }

    void writeDouble(double v) {
        writeBytes(&v, sizeof(v));
    }

    void writeString(const std::string& s) {
        uint16_t len = static_cast<uint16_t>(s.size()); // length of the string, max = 65k
        writeBytes(&len, sizeof(len));

        writeBytes(s.data(), len); // string data
    }

    void writeDocumentMap(const Document& doc) {
        uint32_t count = static_cast<uint32_t>(doc.size());
        writeBytes(&count, sizeof(count));

        for (const auto& [key, valPtr] : doc) {
            writeString(key);          
            
            const Value& v = *valPtr;
            writeByte(static_cast<uint8_t>(v.type)); 

            switch (v.type) {
                case Type::Int:    writeInt64(v.asInt()); break;
                case Type::Double: writeDouble(v.asDouble()); break;
                case Type::Bool:   writeByte(v.asBool() ? 1 : 0); break;
                case Type::String: writeString(v.asString()); break;
                case Type::Object: 
                    writeDocumentMap(v.asObject()); // recursive call
                    break; 
            }
        }
    }

    std::vector<uint8_t> serialize(const Document& doc) {
        buffer.clear();
        writeDocumentMap(doc);
        return buffer;
    }

    void dumpToFile(const std::string& filename) {
        std::ofstream file(filename, std::ios::binary | std::ios::out);
        
        if (!file.is_open()) {
            throw std::runtime_error("Could not open file for writing: " + filename);
        }

        // vector.data() gives us the raw array pointer
        file.write(reinterpret_cast<const char*>(buffer.data()), buffer.size()); // Convert uint8_t pointer to const char* bcz write() expects char* 
        file.close();
        
        std::cout << "[Serializer] Saved " << buffer.size() << " bytes to " << filename << "\n";
    }
};

class Deserializer {
private:
    const std::vector<uint8_t>& buffer;
    size_t pos = 0; 

public:
    Deserializer(const std::vector<uint8_t>& buf) : buffer(buf) {}

    uint8_t readByte() {
        if (pos >= buffer.size()) throw std::runtime_error("Unexpected EOF");
        return buffer[pos++];
    }

    // helper to read raw bytes into a variable
    template <typename T>
    T readRaw() {
        if (pos + sizeof(T) > buffer.size()) throw std::runtime_error("Unexpected EOF");
        T val;
        std::memcpy(&val, &buffer[pos], sizeof(T));
        pos += sizeof(T);
        return val;
    }

    int64_t readInt64() { return readRaw<int64_t>(); }
    double readDouble() { return readRaw<double>(); }

    std::string readString() {
        // read Length (uint16_t)
        uint16_t len = readRaw<uint16_t>();
        
        // read Chars
        if (pos + len > buffer.size()) throw std::runtime_error("Unexpected EOF inside string");
        std::string s(reinterpret_cast<const char*>(&buffer[pos]), len);
        pos += len;
        return s;
    }

    Document readDocumentMap() {
        Document doc;

        uint32_t count = readRaw<uint32_t>();

        for (uint32_t i = 0; i < count; ++i) {
            std::string key = readString();
            Type type = static_cast<Type>(readByte());

            switch (type) {
                case Type::Int:    
                    doc[key] = std::make_shared<Value>(readInt64()); break;
                case Type::Double: 
                    doc[key] = std::make_shared<Value>(readDouble()); break;
                case Type::Bool:   
                    doc[key] = std::make_shared<Value>(readByte() != 0); break;
                case Type::String: 
                    doc[key] = std::make_shared<Value>(readString()); break;
                
                case Type::Object:
                    // read the inner map, wrap it in a Value, and store it
                    doc[key] = std::make_shared<Value>(readDocumentMap());
                    break; 
            }
        }
        return doc;
    }

    Document deserialize() {
        return readDocumentMap();
    }
    
    static Document loadFromFile(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary | std::ios::ate); // open at end to get size
        if (!file.is_open()) throw std::runtime_error("File not found: " + filename);

        std::streamsize size = file.tellg();
        file.seekg(0, std::ios::beg);

        std::vector<uint8_t> buf(size);
        if (!file.read(reinterpret_cast<char*>(buf.data()), size)) {
             throw std::runtime_error("Read error");
        }

        Deserializer reader(buf);
        return reader.deserialize();
    }
};

} 

#endif