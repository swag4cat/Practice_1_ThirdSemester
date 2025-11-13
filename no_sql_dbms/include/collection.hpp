#pragma once
#include <string>
#include "hash_map.hpp"
#include "btree_index.hpp"
#include "query_evaluator.hpp"

class Collection {
public:
    Collection(const std::string &db_path, const std::string &name);
    ~Collection();

    std::string insert(json doc);
    Vector<json> find(const json &query);
    int remove(const json &query);
    void create_index(const std::string &field);
    void save();
    void load();

private:
    std::string dbpath, collname, collfile, indexdir;
    HashMap<json> store;

    HashMap<HashMap<Vector<std::string>>> indexes;
    HashMap<BTreeIndex> btree_indexes;

    static std::string index_key_for_value(const json &v);
    void save_index(const std::string &field);
};
