#include "../include/collection.hpp"
#include "../include/utils.hpp"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <algorithm>

Collection::Collection(const std::string &db_path, const std::string &name)
: dbpath(db_path), collname(name) {
    collfile = dbpath + "/" + collname + ".json";
    indexdir = dbpath + "/indexes";
    std::filesystem::create_directories(dbpath);
    std::filesystem::create_directories(indexdir);
    load();
}

Collection::~Collection() { save(); }

std::string Collection::insert(json doc) {
    if (!doc.is_object()) throw std::runtime_error("Document must be an object");
    std::string id = gen_id();
    doc["_id"] = id;
    store.put(id, doc);

    for (auto &p : indexes) {
        const std::string &field = p.first;
        if (doc.contains(field)) {
            std::string key = index_key_for_value(doc[field]);
            p.second[key].push_back(id);
        }
    }

    for (auto &p : btree_indexes) {
        const std::string &field = p.first;
        if (doc.contains(field) && doc[field].is_number()) {
            p.second.insert(doc[field].get<double>(), id);
        }
    }

    return id;
}

std::vector<json> Collection::find(const json &query) {
    std::vector<json> res;

    if (query.is_object() && query.size() == 1 && !query.contains("$or")) {
        auto it = query.begin();
        std::string field = it.key();
        const json &cond = it.value();

        if (btree_indexes.count(field) && cond.is_object()) {
            auto &bt = btree_indexes[field];
            std::vector<std::string> ids;

            if (cond.contains("$eq"))
                ids = bt.search(cond["$eq"].get<double>());
            else if (cond.contains("$gt") && cond.contains("$lt"))
                ids = bt.rangeSearch(cond["$gt"].get<double>(), cond["$lt"].get<double>());
            else if (cond.contains("$gt"))
                ids = bt.rangeSearch(cond["$gt"].get<double>(), 1e18);
            else if (cond.contains("$lt"))
                ids = bt.rangeSearch(-1e18, cond["$lt"].get<double>());

            if (!ids.empty()) {
                for (auto &id : ids) {
                    json d;
                    if (store.get(id, d)) res.push_back(d);
                }
                return res;
            }
        }
    }

    bool usedIndex = false;
    if (query.is_object() && query.size() == 1 && !query.contains("$or")) {
        auto it = query.begin();
        std::string field = it.key();
        const json &cond = it.value();

        if (indexes.count(field)) {
            auto &mapidx = indexes[field];
            if (!cond.is_object()) {
                std::string key = index_key_for_value(cond);
                if (mapidx.count(key)) {
                    for (auto &id : mapidx[key]) {
                        json d; if (store.get(id, d)) res.push_back(d);
                    }
                    usedIndex = true;
                }
            } else if (cond.contains("$eq")) {
                std::string key = index_key_for_value(cond["$eq"]);
                if (mapidx.count(key)) {
                    for (auto &id : mapidx[key]) {
                        json d; if (store.get(id, d)) res.push_back(d);
                    }
                    usedIndex = true;
                }
            } else if (cond.contains("$in")) {
                for (const auto &v : cond["$in"]) {
                    std::string key = index_key_for_value(v);
                    if (mapidx.count(key)) {
                        for (auto &id : mapidx[key]) {
                            json d; if (store.get(id, d)) res.push_back(d);
                        }
                    }
                }
                usedIndex = true;
            }
        }
    }

    if (!usedIndex) {
        for (auto &p : store.items()) {
            if (evaluate_query(p.second, query))
                res.push_back(p.second);
        }
    }

    return res;
}

int Collection::remove(const json &query) {
    auto found = find(query);
    int cnt = 0;
    for (auto &d : found) {
        std::string id = d["_id"].get<std::string>();
        if (store.remove(id)) {
            ++cnt;
            for (auto &p : indexes) {
                const std::string &field = p.first;
                if (d.contains(field)) {
                    std::string key = index_key_for_value(d[field]);
                    auto &vec = p.second[key];
                    vec.erase(std::remove(vec.begin(), vec.end(), id), vec.end());
                }
            }
        }
    }
    return cnt;
}

void Collection::create_index(const std::string &field) {
    bool numericField = false;
    for (auto &p : store.items()) {
        const json &doc = p.second;
        if (doc.contains(field) && doc[field].is_number()) {
            numericField = true;
            break;
        }
    }

    if (numericField) {
        BTreeIndex btree;
        for (auto &p : store.items()) {
            const json &doc = p.second;
            if (doc.contains(field) && doc[field].is_number())
                btree.insert(doc[field].get<double>(), p.first);
        }
        btree_indexes[field] = btree;

        std::string fname = indexdir + "/" + collname + "." + field + ".btree.json";
        std::ofstream ofs(fname);
        ofs << std::setw(2) << btree.to_json() << std::endl;
        std::cout << "B-Tree index created on numeric field '" << field << "'.\n";
    } else {
        std::unordered_map<std::string, std::vector<std::string>> mapidx;
        for (auto &p : store.items()) {
            const json &doc = p.second;
            if (doc.contains(field)) {
                std::string key = index_key_for_value(doc[field]);
                mapidx[key].push_back(p.first);
            }
        }
        indexes[field] = mapidx;
        save_index(field);
        std::cout << "Simple index created on field '" << field << "'.\n";
    }
}

void Collection::save() {
    json j = store.to_json();
    std::ofstream ofs(collfile);
    ofs << std::setw(2) << j << std::endl;
    for (auto &p : indexes) save_index(p.first);
}

void Collection::load() {
    if (!std::filesystem::exists(collfile)) return;
    std::ifstream ifs(collfile);
    json j; ifs >> j;
    store.from_json(j);

    if (!std::filesystem::exists(indexdir)) return;
    for (auto &p : std::filesystem::directory_iterator(indexdir)) {
        std::string fname = p.path().filename().string();
        std::string prefix = collname + ".";
        if (fname.rfind(prefix, 0) != 0) continue;

        if (fname.find(".index.json") != std::string::npos) {
            std::string field = fname.substr(prefix.size(), fname.find(".index.json") - prefix.size());
            std::ifstream fi(p.path());
            json ji; fi >> ji;
            std::unordered_map<std::string, std::vector<std::string>> mapidx;
            for (auto it = ji.begin(); it != ji.end(); ++it)
                mapidx[it.key()] = it.value().get<std::vector<std::string>>();
            indexes[field] = mapidx;
        } else if (fname.find(".btree.json") != std::string::npos) {
            std::string field = fname.substr(prefix.size(), fname.find(".btree.json") - prefix.size());
            std::ifstream fi(p.path());
            json jb; fi >> jb;
            BTreeIndex bt; bt.from_json(jb);
            btree_indexes[field] = bt;
        }
    }
}

std::string Collection::index_key_for_value(const json &v) {
    if (v.is_string()) return "s:" + v.get<std::string>();
    if (v.is_number()) {
        std::ostringstream oss; oss << v.get<double>();
        return "n:" + oss.str();
    }
    if (v.is_boolean()) return std::string("b:") + (v.get<bool>() ? "1" : "0");
    return "j:" + v.dump();
}

void Collection::save_index(const std::string &field) {
    std::string fname = indexdir + "/" + collname + "." + field + ".index.json";
    json ji;
    for (auto &p : indexes[field]) ji[p.first] = p.second;
    std::ofstream ofs(fname);
    ofs << std::setw(2) << ji << std::endl;
}
