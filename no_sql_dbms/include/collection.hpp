#pragma once
#include <string>
#include <unordered_map>
#include "hash_map.hpp"
#include "btree_index.hpp"
#include "query_evaluator.hpp"

class Collection {
public:
    Collection(const std::string &db_path, const std::string &name);
    ~Collection();

    std::string insert(json doc);
    std::vector<json> find(const json &query);
    int remove(const json &query);
    void create_index(const std::string &field);
    void save();
    void load();

private:
    std::string dbpath, collname, collfile, indexdir;
    HashMap<json> store;

    std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string>>> indexes;
    std::unordered_map<std::string, BTreeIndex> btree_indexes;

    static std::string index_key_for_value(const json &v);
    void save_index(const std::string &field);
};
