#pragma once
#include <string>
#include "vector.hpp"
#include <utility>
#include "../parcer/json.hpp"
#include "algorithms.hpp"

using json = nlohmann::json;

template<typename V>
class HashMap {
public:
    using Pair = std::pair<std::string, V>;

    HashMap(size_t init_buckets = 16, double max_load = 0.75);
    void put(const std::string &key, const V &value);
    bool get(const std::string &key, V &out) const;
    bool remove(const std::string &key);
    Vector<Pair> items() const;
    size_t size() const;
    json to_json() const;
    void from_json(const json &j);

private:
    Vector<Vector<Pair>> buckets;
    size_t size_;
    double max_load_factor;

    static uint64_t str_hash(const std::string &s);
    size_t bucket_index(const std::string &key) const;
    void rehash(size_t new_buckets);
};

template<typename V>
HashMap<V>::HashMap(size_t init_buckets, double max_load)
: size_(0), max_load_factor(max_load) {
    buckets = Vector<Vector<Pair>>(init_buckets);
}

template<typename V>
void HashMap<V>::put(const std::string &key, const V &value) {
    if (buckets.size() == 0 || (double)(size_ + 1) / buckets.size() > max_load_factor) {
        rehash(buckets.size() == 0 ? 16 : buckets.size() * 2);
    }
    size_t idx = bucket_index(key);
    for (auto &p : buckets[idx]) {
        if (p.first == key) { p.second = value; return; }
    }
    buckets[idx].emplace_back(key, value);
    ++size_;
}

template<typename V>
bool HashMap<V>::get(const std::string &key, V &out) const {
    if (buckets.size() == 0) return false;
    size_t idx = bucket_index(key);
    for (const auto &p : buckets[idx]) {
        if (p.first == key) { out = p.second; return true; }
    }
    return false;
}

template<typename V>
bool HashMap<V>::remove(const std::string &key) {
    if (buckets.size() == 0) return false;
    size_t idx = bucket_index(key);
    auto &chain = buckets[idx];

    bool found = false;
    for (size_t i = 0; i < chain.size(); ++i) {
        if (chain[i].first == key) {
            for (size_t j = i; j < chain.size() - 1; ++j) {
                chain[j] = chain[j + 1];
            }
            chain.pop_back();
            --size_;
            found = true;
            break;
        }
    }

    return found;
}

template<typename V>
Vector<typename HashMap<V>::Pair> HashMap<V>::items() const {
    Vector<Pair> res;
    for (const auto &chain : buckets) {
        for (const auto &p : chain) res.push_back(p);
    }
    return res;
}

template<typename V>
size_t HashMap<V>::size() const { return size_; }

template<typename V>
json HashMap<V>::to_json() const {
    json j = json::object();
    for (const auto &chain : buckets) {
        for (const auto &p : chain) {
            j[p.first] = p.second;
        }
    }
    return j;
}

template<typename V>
void HashMap<V>::from_json(const json &j) {
    buckets.clear();
    buckets = Vector<Vector<Pair>>(16);
    size_ = 0;
    for (auto it = j.begin(); it != j.end(); ++it) {
        put(it.key(), it.value());
    }
}

template<typename V>
uint64_t HashMap<V>::str_hash(const std::string &s) {
    uint64_t h = 1469598103934665603ULL;
    for (unsigned char c : s) {
        h ^= (uint64_t)c;
        h *= 1099511628211ULL;
        h ^= (h >> 33);
        h *= 0xff51afd7ed558ccdULL;
        h ^= (h >> 33);
    }
    return h;
}

template<typename V>
size_t HashMap<V>::bucket_index(const std::string &key) const {
    if (buckets.size() == 0) return 0;
    return (size_t)(str_hash(key) % buckets.size());
}

template<typename V>
void HashMap<V>::rehash(size_t new_buckets) {
    Vector<Vector<Pair>> new_table(new_buckets);
    for (const auto &chain : buckets) {
        for (const auto &p : chain) {
            uint64_t h = str_hash(p.first);
            size_t idx = (size_t)(h % new_buckets);
            new_table[idx].push_back(p);
        }
    }
    buckets = new_table;
}
