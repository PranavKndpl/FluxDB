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

    bool checkCondition(const Value& val, const Value& constraint) {
        if (constraint.type != Type::Object) {
            return val == constraint;
        }

        const Document& ops = constraint.asObject();
        bool match = true;

        for (const auto& [op, criterion] : ops) {
            if (!criterion) continue;
            const Value& crit = *criterion;

            if (op == "$gt") {
                if (!(val > crit)) match = false;
            } 
            else if (op == "$lt") {
                if (!(val < crit)) match = false;
            } 
            else if (op == "$gte") {
                if (!(val >= crit)) match = false;
            } 
            else if (op == "$lte") {
                if (!(val <= crit)) match = false;
            } 
            else if (op == "$ne") {
                if (val == crit) match = false;
            }
        }
        return match;
    }

    bool matches(const Document& doc, const Document& query) {
        for (const auto& [key, constraint] : query) {
            if (!constraint) continue;

            auto it = doc.find(key);

            if (it == doc.end()) return false; 

            if (!checkCondition(*it->second, *constraint)) {
                return false;
            }
        }
        return true;
    }

public:
    QueryProcessor(Collection& database) : db(database) {}

    std::string process(const std::string& request) {
        try {
            if (request.rfind("INSERT ", 0) == 0)      return handleInsert(request.substr(7));
            else if (request.rfind("FIND ", 0) == 0)   return handleFind(request.substr(5));
            else if (request.rfind("DELETE ", 0) == 0) return handleDelete(request.substr(7));
            else if (request.rfind("UPDATE ", 0) == 0) return handleUpdate(request.substr(7));
            else if (request.rfind("INDEX ", 0) == 0)  return handleIndex(request.substr(6));
            
            else if (request.rfind("CHECKPOINT", 0) == 0) {
                db.checkpoint();
                return "OK CHECKPOINT_COMPLETE\n";
            }
            else if (request.rfind("FLUSHDB", 0) == 0) {
                db.clear(); 
                return "OK FLUSHED\n";
            }
            else if (request == "GET") {
                return handleGet("");
            }
            else if (request.rfind("GET ", 0) == 0) {
                return handleGet(request.substr(4));
            }
            else if (request.rfind("CONFIG ", 0) == 0) {
                return handleConfig(request.substr(7));
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

        std::vector<Id> ids;
        bool usedIndex = false;
        bool isRange = false;

        if (query.begin()->second->type == Type::Object) {
            isRange = true; 
        }

        if (query.size() == 1) {
            auto it = query.begin();
            std::string field = it->first;
            
            if (it->second->type != Type::Object) {
                 ids = db.find(field, *it->second);
                 
                 if (!ids.empty()) {
                     usedIndex = true;
                 } 
            }
            
            if (!usedIndex) {
                db.reportQueryMiss(field, isRange);
            }
        }

        if (!usedIndex) {
            ids = db.findAll([this, &query](const Document& doc) {
                return this->matches(doc, query);
            });
        }

        std::string response = "OK COUNT=" + std::to_string(ids.size()) + "\n";
        
        for(Id id : ids) {
            auto res = db.getById(id);
            if (res) {
                Value temp(res->get());
                response += "ID " + std::to_string(id) + " " + temp.ToJson() + "\n"; 
            }
        }
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

    std::string handleGet(const std::string& args) {
        if (args.empty()) {
             std::vector<Id> allIds = db.findAll([](const Document&){ return true; });
             
             std::string response = "OK COUNT=" + std::to_string(allIds.size()) + "\n";
             for(Id id : allIds) {
                 auto res = db.getById(id);
                 if (res) {
                     Value temp(res->get());
                     response += "ID " + std::to_string(id) + " " + temp.ToJson() + "\n";
                 }
             }
             return response;
        }

        size_t dashPos = args.find('-');
        if (dashPos != std::string::npos) {
            try {
                Id start = std::stoull(args.substr(0, dashPos));
                Id end   = std::stoull(args.substr(dashPos + 1));
                
                std::string response;
                int count = 0;
                
                for (Id i = start; i <= end; ++i) {
                     auto res = db.getById(i);
                     if (res) {
                         Value temp(res->get());
                         response += "ID " + std::to_string(i) + " " + temp.ToJson() + "\n";
                         count++;
                     }
                }
                return "OK COUNT=" + std::to_string(count) + "\n" + response;
            } catch (...) {
                return "ERROR INVALID_RANGE\n";
            }
        }

        try {
            Id id = std::stoull(args);
            auto result = db.getById(id); 
            if (result) {
                Value tempVal(result->get());
                return "OK " + tempVal.ToJson() + "\n";
            } else {
                return "ERROR NOT_FOUND\n";
            }
        } catch (...) {
            return "ERROR INVALID_ID\n";
        }
    }

    std::string handleConfig(const std::string& args) {
        // Usage: CONFIG ADAPTIVE 1
        std::stringstream ss(args);
        std::string param;
        int value;
        
        ss >> param >> value;
        
        if (param == "ADAPTIVE") {
            db.setAdaptive(value == 1);
            return "OK CONFIG_UPDATED\n";
        }
        
        return "ERROR UNKNOWN_CONFIG\n";
    }
};

} 

#endif