#include<iostream>
#include<unordered_map>

class Database{
    private:
        std::unordered_map<long, std::string> db;

    public:
        Database();

        void insert(const long id, const std::string document){
            db.emplace(id, document); // better than insert({})
        }

        const std::string* get_id(long id) {   // use option<string> after update
            auto it = db.find(id);
            if (it != db.end()) 
                return &it->second; // better than db.contains -> db.at(id) [O(logn) vs 2 x O(logn)]
            return nullptr;
        }

        void update(const long id, std::string document){
            auto it = db.find(id);
            if (it != db.end())
                it->second = document;
        }

        bool remove(const long id){
            return db.erase(id) > 0; // .erase() takes care of not found case
        }
};