#ifndef DOCUMENT_HPP
#define DOCUMENT_HPP     //include guard

#include<unordered_map>
#include<cstdint>
#include<variant>
#include<string>
#include<iostream>

enum class Type {
    Int,      // usually becomes 0
    Double,   // usually 1
    Bool,     // usually 2
    String,   // usually 3
    Object    // usually 4
};

struct Value;

using Object = std::unordered_map<std::string, Value>;

struct Value{
    Type type;
    std::variant<int, double, bool, std::string, Object> data; 

    Value(Type t, std::variant<int, double, bool, std::string, Object> dt) : type(t), data(dt) {}

    bool IsType(Type t) const {
        return type == t;
    }

    std::string TypeName() const{
        switch(type){
            case Type::Int: return "INT";
            case Type::Double: return "DB";
            case Type::Bool: return "BOOL";
            case Type::String: return "STR";
            case Type::Object: return "OBJ";
            default: return "UNKNOWN";
        }
    }

    template<typename T>
    T asDataType() const {   // asX function to maintain type safety
        return std::get<T>(data);
    }

    std::string ToString() const {
    switch(type) {
        case Type::Int:    return std::to_string(std::get<int>(data));
        case Type::Double: return std::to_string(std::get<double>(data));
        case Type::Bool:   return std::get<bool>(data) ? "true" : "false";
        case Type::String: return std::get<std::string>(data);
        case Type::Object: return "{...}";
        default: return "UNKNOWN";
    }
}

};

#endif



