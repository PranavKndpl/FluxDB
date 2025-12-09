#ifndef DOCUMENT_HPP
#define DOCUMENT_HPP     

#include<unordered_map>
#include<cstdint>
#include<variant>
#include<string>
#include<memory>
#include<stdexcept>
#include<iomanip>
#include<sstream> 
#include<utility>
#include<functional>

namespace fluxdb{

enum class Type {
    Int,    
    Double,  
    Bool,     
    String,   
    Object,
    Array    
};

struct Value;

// document alias for better understanding (not feeling like a recursive object)
using Document = std::unordered_map<std::string, std::shared_ptr<Value>>;
using Array    = std::vector<std::shared_ptr<Value>>;

struct Value{
    Type type;
    std::variant<int64_t, double, bool, std::string, Document, Array> data; 

    // per type constructor for type value sync
    Value(int64_t v)           : type(Type::Int), data(v) {}
    Value(int v) : type(Type::Int), data(static_cast<int64_t>(v)) {} //if plain int, treat it as an int64_t
    Value(double v)            : type(Type::Double), data(v) {}
    Value(bool v)              : type(Type::Bool), data(v) {}
    Value(const std::string& v): type(Type::String), data(v) {}
    Value(const char* v)       : type(Type::String), data(std::string(v)) {} // helper for "literals"

    Value(const Document& v)   : type(Type::Object), data(v) {}

    // accept Document&& and move it into the variant (no copy)
    Value(Document&& v)        : type(Type::Object), data(std::move(v)) {}

    Value(const Value&) = default;
    Value& operator=(const Value&) = default;

    Value(Value&&) = default;
    Value& operator=(Value&&) = default;

    //array consts
    Value(const Array& v) : type(Type::Array), data(v) {}
    Value(Array&& v)      : type(Type::Array), data(std::move(v)) {}

    template<typename Fn, typename = std::enable_if_t<std::is_invocable_v<Fn, Document&>>> //SFINAE
    static Document make_document(Fn f) {
        Document d;
        f(d);
        return d;
    }

    bool IsType(Type t) const {
        return type == t;
    }

    bool isNumber() const { 
        return type == Type::Int || type == Type::Double; 
    }

    double getNumeric() const {
        if (type == Type::Int) return static_cast<double>(std::get<int64_t>(data));
        if (type == Type::Double) return std::get<double>(data);
        return 0.0;
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

    const Array& asArray() const {
        if (type != Type::Array) throw std::runtime_error("Value is not an Array");
        return std::get<Array>(data);
    }


    // Recursive JSON Serializer
    std::string ToJson() const {
        switch (type) {
            case Type::Int:    return std::to_string(std::get<int64_t>(data));
            case Type::Double: {
                std::ostringstream oss;
                oss << std::fixed << std::setprecision(6) << std::get<double>(data);
                // Remove trailing zeros for cleaner JSON
                std::string s = oss.str();
                s.erase(s.find_last_not_of('0') + 1, std::string::npos); 
                if (s.back() == '.') s.pop_back();
                return s;
            }
            case Type::Bool:   return std::get<bool>(data) ? "true" : "false";
            case Type::String: return "\"" + std::get<std::string>(data) + "\"";
            
            case Type::Array: {
                const auto& arr = std::get<Array>(data);
                std::string json = "[";
                for (size_t i = 0; i < arr.size(); ++i) {
                    json += arr[i]->ToJson(); // Recursion!
                    if (i < arr.size() - 1) json += ", ";
                }
                json += "]";
                return json;
            }

            case Type::Object: {
                const auto& doc = std::get<Document>(data);
                std::string json = "{";
                size_t i = 0;
                for (const auto& [key, valPtr] : doc) {
                    json += "\"" + key + "\": " + valPtr->ToJson(); // Recursion!
                    if (i < doc.size() - 1) json += ", ";
                    i++;
                }
                json += "}";
                return json;
            }
            
            default: return "null";
        }
    }

    friend bool operator==(const Value& lhs, const Value& rhs) {
        if (lhs.isNumber() && rhs.isNumber()) {
            return lhs.getNumeric() == rhs.getNumeric();
        }

        if (lhs.type != rhs.type) return false;

        switch(lhs.type) {
            case Type::Int: return lhs.asInt() == rhs.asInt();
            case Type::Bool: return lhs.asBool() == rhs.asBool();
            case Type::String: return lhs.asString() == rhs.asString();
            default: return false;
        }
    }

    bool operator<(const Value& other) const;
    bool operator>(const Value& other) const;
    bool operator<=(const Value& other) const;
    bool operator>=(const Value& other) const;
    bool operator!=(const Value& other) const;

};

//comparator (sort and ranging)
struct ValueLess {              // Number < Bool < String < Object
    static int getRank(const Value& v) {
        if (v.isNumber()) return 0;
        if (v.type == Type::Bool) return 1;
        if (v.type == Type::String) return 2;
        return 3; 
    }

    bool operator()(const Value& a, const Value& b) const {
        int rA = getRank(a);
        int rB = getRank(b);

        if (rA != rB) return rA < rB;

        if (rA == 0) return a.getNumeric() < b.getNumeric(); 
        if (rA == 1) return a.asBool() < b.asBool();         
        if (rA == 2) return a.asString() < b.asString();     

        return false;
    }
};


struct ValueHasher {
    std::size_t operator()(const Value& v) const {

        std::size_t h = std::hash<int>{}(static_cast<int>(v.type)); 

        std::size_t d_hash = 0;

        if (v.isNumber()) {
            d_hash = std::hash<double>{}(v.getNumeric());
        } else if (v.type == Type::Bool) {
            d_hash = std::hash<bool>{}(v.asBool());
        } else if (v.type == Type::String) {
            d_hash = std::hash<std::string>{}(v.asString());
        }

        return h ^ (d_hash + 0x9e3779b9 + (h << 6) + (h >> 2)); 
    }
};

inline bool Value::operator<(const Value& other) const {
    return ValueLess()(*this, other);
}

inline bool Value::operator>(const Value& other) const {
    return ValueLess()(other, *this);
}

inline bool Value::operator<=(const Value& other) const {
    return !(*this > other);
}

inline bool Value::operator>=(const Value& other) const {
    return !(*this < other);
}

inline bool Value::operator!=(const Value& other) const {
    return !(*this == other);
}


}

#endif