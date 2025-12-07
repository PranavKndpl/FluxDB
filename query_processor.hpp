#ifndef QUERY_PROCESSOR_HPP
#define QUERY_PROCESSOR_HPP

#include "collection.hpp"
#include "query_parser.hpp"
#include <string>
#include <sstream>

namespace fluxdb {

class QueryProcessor {
private:
    Collection& db; 

public:
    QueryProcessor(Collection& database) : db(database) {}

    std::string process(const std::string& request) {
        try {
            if (request.rfind("INSERT ", 0) == 0) {
                return handleInsert(request.substr(7));
            } 
            else if (request.rfind("FIND ", 0) == 0) {
                return handleFind(request.substr(5));
            }
            else if (request.rfind("DELETE ", 0) == 0) {
                return handleDelete(request.substr(7));
            }
            else if (request.rfind("UPDATE ", 0) == 0) {
                return handleUpdate(request.substr(7));
            }
            else if (request.rfind("INDEX ", 0) == 0) {
                return handleIndex(request.substr(6));
            }
            else if (request.rfind("CHECKPOINT", 0) == 0) {
                db.checkpoint();
                return "OK CHECKPOINT_COMPLETE\n";
            }
            else if (request.rfind("FLUSHDB", 0) == 0) {
                db.clear(); 
                return "OK FLUSHED\n";
            }
            
            return "UNKNOWN_COMMAND\n";

        } catch (const std::exception& e) {
            return std::string("ERROR ") + e.what() + "\n";
        }
    }

private:

    std::string handleInsert(const std::string& json) {
        QueryParser parser(json);
        Document doc = parser.parseJSON();
        Id id = db.insert(std::move(doc));
        return "OK ID=" + std::to_string(id) + "\n";
    }

    std::string handleFind(const std::string& json) {
        QueryParser parser(json);
        Document query = parser.parseJSON();
        
        if (query.empty()) return "ERROR EMPTY_QUERY\n";

        auto it = query.begin();
        std::string field = it->first;
        std::shared_ptr<Value> val = it->second;

        std::vector<Id> ids;

        if (val->type == Type::Object) {
            const Document& ops = val->asObject();
            
            Value minVal(static_cast<int64_t>(-9999999)); 
            Value maxVal(static_cast<int64_t>(9999999));  
            
            bool isRange = false;

            if (ops.count("$gt")) {
                minVal = *ops.at("$gt");
                isRange = true;
            }
            if (ops.count("$lt")) {
                maxVal = *ops.at("$lt");
                isRange = true;
            }

            if (isRange) {
                ids = db.findRange(field, minVal, maxVal);
            } else {
                ids = db.find(field, *val);
            }
        } 
        else {
            ids = db.find(field, *val);
        }
        
        std::string response = "OK COUNT=" + std::to_string(ids.size()) + "\n";
        for(Id id : ids) response += "ID " + std::to_string(id) + "\n";
        return response;
    }

    std::string handleDelete(const std::string& args) {
        try {
            Id id = std::stoull(args);
            if (db.removeById(id)) return "OK DELETED\n";
            return "ERROR NOT_FOUND\n";
        } catch (...) {
            return "ERROR INVALID_ID\n";
        }
    }

    std::string handleUpdate(const std::string& args) {
        size_t jsonStart = args.find('{');
        if (jsonStart == std::string::npos) return "ERROR MISSING_JSON\n";

        try {
            std::string idStr = args.substr(0, jsonStart);
            Id id = std::stoull(idStr);
            
            QueryParser parser(args.substr(jsonStart));
            Document doc = parser.parseJSON();

            if (db.update(id, doc)) return "OK UPDATED\n";
            return "ERROR NOT_FOUND\n";
        } catch (...) {
            return "ERROR INVALID_FORMAT\n";
        }
    }

    std::string handleIndex(const std::string& args) {
        std::stringstream ss(args);
        std::string field;
        int type = 0;
        
        ss >> field;
        if (ss >> type) {
            db.createIndex(field, type);
        } else {
            db.createIndex(field, 0);
        }
        return "OK INDEX_CREATED\n";
    }
};

} 

#endif