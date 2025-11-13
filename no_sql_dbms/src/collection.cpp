#include "../include/collection.hpp"
#include "../include/utils.hpp"
#include "../include/algorithms.hpp"
#include <fstream>
#include <iostream>
#include <filesystem>

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

    auto index_items = indexes.items();
    for (const auto& item : index_items) {
        const std::string& field = item.first;
        if (doc.contains(field)) {
            std::string key = index_key_for_value(doc[field]);
            HashMap<Vector<std::string>> field_index = item.second;
            Vector<std::string> ids;
            field_index.get(key, ids);
            ids.push_back(id);
            field_index.put(key, ids);
            indexes.put(field, field_index);
        }
    }

    auto btree_items = btree_indexes.items();
    for (const auto& item : btree_items) {
        const std::string& field = item.first;
        if (doc.contains(field) && doc[field].is_number()) {
            BTreeIndex bt = item.second;
            bt.insert(doc[field].get<double>(), id);
            btree_indexes.put(field, bt);
        }
    }

    return id;
}

Vector<json> Collection::find(const json &query) {
    Vector<json> res;

    if (query.is_object() && query.size() == 1 && !query.contains("$or")) {
        auto it = query.begin();
        std::string field = it.key();
        const json &cond = it.value();

        BTreeIndex bt;
        if (btree_indexes.get(field, bt) && cond.is_object()) {
            Vector<std::string> ids;

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

        HashMap<Vector<std::string>> field_index;
        if (indexes.get(field, field_index)) {
            if (!cond.is_object()) {
                std::string key = index_key_for_value(cond);
                Vector<std::string> ids;
                if (field_index.get(key, ids)) {
                    for (auto &id : ids) {
                        json d; if (store.get(id, d)) res.push_back(d);
                    }
                    usedIndex = true;
                }
            } else if (cond.contains("$eq")) {
                std::string key = index_key_for_value(cond["$eq"]);
                Vector<std::string> ids;
                if (field_index.get(key, ids)) {
                    for (auto &id : ids) {
                        json d; if (store.get(id, d)) res.push_back(d);
                    }
                    usedIndex = true;
                }
            } else if (cond.contains("$in")) {
                for (const auto &v : cond["$in"]) {
                    std::string key = index_key_for_value(v);
                    Vector<std::string> ids;
                    if (field_index.get(key, ids)) {
                        for (auto &id : ids) {
                            json d; if (store.get(id, d)) res.push_back(d);
                        }
                    }
                }
                usedIndex = true;
            }
        }
    }

    if (!usedIndex) {
        auto all_items = store.items();
        for (auto &p : all_items) {
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
            auto index_items = indexes.items();
            for (const auto& item : index_items) {
                const std::string& field = item.first;
                if (d.contains(field)) {
                    std::string key = index_key_for_value(d[field]);
                    HashMap<Vector<std::string>> field_index = item.second;
                    Vector<std::string> ids;
                    if (field_index.get(key, ids)) {
                        size_t removed = custom_remove_if(ids.begin(), ids.end(),
                                                          [&](const std::string& current_id) { return current_id == id; });
                        if (removed > 0) {
                            ids.resize(ids.size() - removed);
                            if (ids.empty()) {
                                field_index.remove(key);
                            } else {
                                field_index.put(key, ids);
                            }
                            indexes.put(field, field_index);
                        }
                    }
                }
            }
        }
    }

    if (cnt > 0) {
        save();
    }

    return cnt;
}

void Collection::create_index(const std::string &field) {
    bool numericField = false;
    auto all_items = store.items();
    for (auto &p : all_items) {
        const json &doc = p.second;
        if (doc.contains(field) && doc[field].is_number()) {
            numericField = true;
            break;
        }
    }

    if (numericField) {
        BTreeIndex btree;
        for (auto &p : all_items) {
            const json &doc = p.second;
            if (doc.contains(field) && doc[field].is_number())
                btree.insert(doc[field].get<double>(), p.first);
        }
        btree_indexes.put(field, btree);

        std::string fname = indexdir + "/" + collname + "." + field + ".btree.json";
        std::ofstream ofs(fname);
        ofs << std::setw(2) << btree.to_json() << std::endl;
        std::cout << "B-Tree index created on numeric field '" << field << "'.\n";
    } else {
        HashMap<Vector<std::string>> mapidx;
        for (auto &p : all_items) {
            const json &doc = p.second;
            if (doc.contains(field)) {
                std::string key = index_key_for_value(doc[field]);
                Vector<std::string> ids;
                mapidx.get(key, ids);
                ids.push_back(p.first);
                mapidx.put(key, ids);
            }
        }
        indexes.put(field, mapidx);
        save_index(field);
        std::cout << "Simple index created on field '" << field << "'.\n";
    }
}

void Collection::save() {
    json j = store.to_json();
    std::ofstream ofs(collfile);
    ofs << std::setw(2) << j << std::endl;

    auto index_items = indexes.items();
    for (const auto& item : index_items) {
        save_index(item.first);
    }
}

Vector<std::string> json_to_string_vector(const json& j) {
    Vector<std::string> result;
    for (const auto& item : j) {
        result.push_back(item.get<std::string>());
    }
    return result;
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
            HashMap<Vector<std::string>> mapidx;
            for (auto it = ji.begin(); it != ji.end(); ++it) {
                mapidx.put(it.key(), json_to_string_vector(it.value()));
            }
            indexes.put(field, mapidx);
        } else if (fname.find(".btree.json") != std::string::npos) {
            std::string field = fname.substr(prefix.size(), fname.find(".btree.json") - prefix.size());
            std::ifstream fi(p.path());
            json jb; fi >> jb;
            BTreeIndex bt; bt.from_json(jb);
            btree_indexes.put(field, bt);
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
    HashMap<Vector<std::string>> field_index;
    if (!indexes.get(field, field_index)) return;

    std::string fname = indexdir + "/" + collname + "." + field + ".index.json";
    json ji;
    auto items = field_index.items();
    for (auto &p : items) {
        ji[p.first] = p.second;
    }
    std::ofstream ofs(fname);
    ofs << std::setw(2) << ji << std::endl;
}
