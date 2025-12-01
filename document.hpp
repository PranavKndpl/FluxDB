#ifndef DOCUMENT_HPP
#define DOCUMENT_HPP     //include guard

#include<unordered_map>
#include<cstdint>
#include<variant>
#include<string>
#include<memory>
#include<stdexcept>
#include<iomanip> // for double precision
#include <sstream> 
#include <utility>

namespace fluxdb{

enum class Type {
    Int,    
    Double,  
    Bool,     
    String,   
    Object    
};

struct Value;

// document alias for better understanding (not feeling like a recursive object)
using Document = std::unordered_map<std::string, std::shared_ptr<Value>>;

struct Value{
    Type type;
    std::variant<int64_t, double, bool, std::string, Document> data; 

    // per type constructor for type value sync
    Value(int64_t v)           : type(Type::Int), data(v) {}
    Value(double v)            : type(Type::Double), data(v) {}
    Value(bool v)              : type(Type::Bool), data(v) {}
    Value(const std::string& v): type(Type::String), data(v) {}

    Value(const Document& v)     : type(Type::Object), data(v) {}

    // accept Document&& and move it into the variant (no copy)
    Value(Document&& v)         : type(Type::Object), data(std::move(v)) {}

    // We delete copy ctor/assign to avoid accidental deep copies of Value.
    // The Document alias uses shared_ptr for child Values, so Documents are cheap to copy,
    // but Value may hold large std::string or Document; deleting copy forces you to be explicit.
    Value(const Value&) = delete;
    Value& operator=(const Value&) = delete;

    // Enable default move ctor/assign so Values can be moved (efficient).
    Value(Value&&) = default;
    Value& operator=(Value&&) = default;


    // Factory helper to build a Document in-place.
    //
    // Usage:
    // auto doc = make_document([](Document& d){
    //     d["a"] = std::make_shared<Value>(42);
    //     d["b"] = std::make_shared<Value>("hello");
    // });
    //
    // This returns a Document by value; RVO/move avoids copies.
    // ---------------------------
    template<typename Fn, typename = std::enable_if_t<std::is_invocable_v<Fn, Document&>>>
    static Document make_document(Fn f) {
        Document d;
        f(d);
        return d; // moved out (NRVO / move)
    }



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
    int64_t asInt() const {
    if (type != Type::Int) throw std::runtime_error("Value is not an int");
    return std::get<int64_t>(data);
    }

    double asDouble() const {
        if (type != Type::Double) throw std::runtime_error("Value is not a double");
        return std::get<double>(data);
    }

    bool asBool() const {
        if (type != Type::Bool) throw std::runtime_error("Value is not a bool");
        return std::get<bool>(data);
    }

    //refrences to avoid copies
    const std::string& asString() const {
        if (type != Type::String) throw std::runtime_error("Value is not a string");
        return std::get<std::string>(data);
    }

    const Document& asObject() const {
        if (type != Type::Object) throw std::runtime_error("Value is not an Object");
        return std::get<Document>(data);
    }


    std::string ToString() const {
    switch(type) {
        case Type::Int:    return std::to_string(std::get<int64_t>(data));
        case Type::Double: {
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(6);
            oss << std::get<double>(data);
            return oss.str();
        }
        case Type::Bool:   return std::get<bool>(data) ? "true" : "false";
        case Type::String: return std::get<std::string>(data);
        case Type::Object: return "{...}";
        default: return "UNKNOWN";
    }
}

};
}

#endif



