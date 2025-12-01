#ifndef DOCUMENT_HPP
#define DOCUMENT_HPP     //include guard

#include<unordered_map>
#include<cstdint>
#include<variant>
#include<string>
#include<memory>
#include<stdexcept>

enum class Type {
    Int,    
    Double,  
    Bool,     
    String,   
    Object    
};

struct Value;

//but recursive Value objects copy the whole structure
using Object = std::unordered_map<std::string, std::shared_ptr<Value>>; //Using shared_ptr inside the variant makes it lighter and avoids deep copies

struct Value{
    Type type;
    std::variant<int, double, bool, std::string, Object> data; 

    // per type constructor for type value sync
    Value(int v)               : type(Type::Int), data(v) {}
    Value(double v)            : type(Type::Double), data(v) {}
    Value(bool v)              : type(Type::Bool), data(v) {}
    Value(const std::string& v): type(Type::String), data(v) {}
    Value(const Object& v)     : type(Type::Object), data(v) {}


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


    // per type asX for type safety
    int asInt() const {
    if (type != Type::Int) throw std::runtime_error("Value is not an int");
    return std::get<int>(data);
    }

    double asDouble() const {
        if (type != Type::Double) throw std::runtime_error("Value is not a double");
        return std::get<double>(data);
    }

    bool asBool() const {
        if (type != Type::Bool) throw std::runtime_error("Value is not a bool");
        return std::get<bool>(data);
    }

    std::string asString() const {
        if (type != Type::String) throw std::runtime_error("Value is not a string");
        return std::get<std::string>(data);
    }

    const Object asObject() const {
        if (type != Type::Object) throw std::runtime_error("Value is not an Object");
        return std::get<Object>(data);
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



