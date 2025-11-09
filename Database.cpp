#include<iostream>
#include<unordered_map>
#include<optional>
#include <cstdint> // for uint64 -> fixed on all platforms unlike long

using Id = std::uint64_t;

class Database{
    private:
        std::unordered_map<Id, std::string> db;

    public:
        Database() = default;

        explicit Database(std::size_t capacity_hint) {
            db.reserve(capacity_hint); // allocates memory ahead of time, restricts copying db1 = db2
        }


        void insert(const Id id, const std::string& document){
            db.emplace(id, document); // better than insert({}) 
        }

        std::optional<std::string> getById(Id id) {
            auto it = db.find(id);
            if (it != db.end()) return it->second; // better than db.contains -> db.at(id) [O(logn) vs 2 x O(logn)]
            return std::nullopt;
        }

        void update(const Id id, std::string document){
            auto it = db.find(id);
            if (it != db.end())
                it->second = document;
        }

        bool removeById(const Id id){
            return db.erase(id) > 0; // .erase() takes care of not found case
        }
};


int main(){
    return 0;
}