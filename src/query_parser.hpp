#ifndef QUERY_PARSER_HPP
#define QUERY_PARSER_HPP

#include "document.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>

namespace fluxdb {

class QueryParser {
private:
    std::string input;
    size_t pos = 0;

    void skipWhitespace() {
        while (pos < input.size() && std::isspace(input[pos])) pos++;
    }

    bool match(char expected) { //match a specific character
        skipWhitespace();
        if (pos < input.size() && input[pos] == expected) {
            pos++;
            return true;
        }
        return false;
    }

    std::string parseString() {
        if (!match('"')) throw std::runtime_error("Expected string start '\"'");
        
        std::string res;
        while (pos < input.size() && input[pos] != '"') {
            res += input[pos++];
        }
        
        if (!match('"')) throw std::runtime_error("Unterminated string");
        return res;
    }

    std::shared_ptr<Value> parseNumber() {
        skipWhitespace();
        size_t start = pos;
        bool isDouble = false;
        
        while (pos < input.size() && (std::isdigit(input[pos]) || input[pos] == '.' || input[pos] == '-')) {
            if (input[pos] == '.') isDouble = true;
            pos++;
        }
        
        std::string numStr = input.substr(start, pos - start);
        if (isDouble) return std::make_shared<Value>(std::stod(numStr)); // string to double
        return std::make_shared<Value>(static_cast<int64_t>(std::stoll(numStr))); // int case, string to long logn 
    }

    std::shared_ptr<Value> parseBool() {
        skipWhitespace();
        if (input.compare(pos, 4, "true") == 0) {
            pos += 4;
            return std::make_shared<Value>(true);
        }
        if (input.compare(pos, 5, "false") == 0) {
            pos += 5;
            return std::make_shared<Value>(false);
        }
        throw std::runtime_error("Expected boolean");
    }

    std::shared_ptr<Value> parseArray() {
        skipWhitespace();
        if (!match('[')) throw std::runtime_error("Array must start with '['");

        Array arr;

        while (pos < input.size()) {
            skipWhitespace();

            if (match(']')) break; 
            
            arr.push_back(parseValue());

            skipWhitespace();

            if (!match(',')) {
                if (pos < input.size() && input[pos] == ']') continue; 
                throw std::runtime_error("Expected ',' or ']'");
            }
        }

        return std::make_shared<Value>(arr); 
    }


public:
    QueryParser(const std::string& raw) : input(raw) {}

    std::shared_ptr<Value> parseValue() {
        skipWhitespace();
        char c = input[pos];

        if (c == '"') return std::make_shared<Value>(parseString());
        if (std::isdigit(c) || c == '-') return parseNumber();
        if (c == 't' || c == 'f') return parseBool();
        if (c == '{') return std::make_shared<Value>(parseJSON()); // Recursion
        if (c == '[') return parseArray();
        
        throw std::runtime_error("Unknown value type");
    }

    // Main Parser Entry: Expects a JSON Object "{ ... }"
    Document parseJSON() {
        skipWhitespace();
        if (!match('{')) throw std::runtime_error("Document must start with '{'");

        Document doc;
        
        while (pos < input.size()) {
            skipWhitespace();
            if (match('}')) break; 

            std::string key = parseString();
            
            if (!match(':')) throw std::runtime_error("Expected ':' after key");

            doc[key] = parseValue();

            if (!match(',')) {
                if (input[pos] == '}') continue; 
                throw std::runtime_error("Expected ',' or '}'");
            }
        }
        return doc;
    }

};

} 

#endif