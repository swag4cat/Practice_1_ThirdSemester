#pragma once
#include <string>
#include <vector>
#include <utility>
#include "../parcer/json.hpp"

using json = nlohmann::json;

template<typename V>
class HashMap {
public:
    using Pair = std::pair<std::string, V>;

    HashMap(size_t init_buckets = 16, double max_load = 0.75);
    void put(const std::string &key, const V &value);
    bool get(const std::string &key, V &out) const;
    bool remove(const std::string &key);
    std::vector<Pair> items() const;
    size_t size() const;
    json to_json() const;
    void from_json(const json &j);

private:
    std::vector<std::vector<Pair>> buckets;
    size_t size_;
    double max_load_factor;

    static uint64_t str_hash(const std::string &s);
    size_t bucket_index(const std::string &key) const;
    void rehash(size_t new_buckets);
};

template<typename V>
HashMap<V>::HashMap(size_t init_buckets, double max_load)
: buckets(init_buckets), size_(0), max_load_factor(max_load) {}

template<typename V>
void HashMap<V>::put(const std::string &key, const V &value) {
    if ((double)(size_ + 1) / buckets.size() > max_load_factor) {
        rehash(buckets.size() * 2);
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
    size_t idx = bucket_index(key);
    for (const auto &p : buckets[idx]) {
        if (p.first == key) { out = p.second; return true; }
    }
    return false;
}

template<typename V>
bool HashMap<V>::remove(const std::string &key) {
    size_t idx = bucket_index(key);
    auto &chain = buckets[idx];
    for (auto it = chain.begin(); it != chain.end(); ++it) {
        if (it->first == key) {
            chain.erase(it);
            --size_;
            return true;
        }
    }
    return false;
}

template<typename V>
std::vector<typename HashMap<V>::Pair> HashMap<V>::items() const {
    std::vector<Pair> res; res.reserve(size_);
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
    buckets.resize(16);
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
    return (size_t)(str_hash(key) % buckets.size());
}

template<typename V>
void HashMap<V>::rehash(size_t new_buckets) {
    std::vector<std::vector<Pair>> new_table(new_buckets);
    for (const auto &chain : buckets) {
        for (const auto &p : chain) {
            uint64_t h = str_hash(p.first);
            size_t idx = (size_t)(h % new_buckets);
            new_table[idx].push_back(p);
        }
    }
    buckets.swap(new_table);
}
