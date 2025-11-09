#include "../include/query_evaluator.hpp"
#include <regex>
#include <cstring>

bool match_like(const std::string &value, const std::string &pattern) {
    std::string re = "^";
    for (size_t i=0;i<pattern.size();++i) {
        char c = pattern[i];
        if (c == '%') re += ".*";
        else if (c == '_') re += ".";
        else if (strchr("^.[]{}()\\+*?|$", c)) {
            re += '\\'; re += c;
        } else re += c;
    }
    re += "$";
    try {
        std::regex r(re, std::regex::ECMAScript | std::regex::icase);
        return std::regex_match(value, r);
    } catch(...) { return false; }
}

bool value_eq(const json &a, const json &b) {
    if (a.is_number() && b.is_number()) return a.get<double>() == b.get<double>();
    return a == b;
}

bool evaluate_condition_on_field(const json &doc, const std::string &field, const json &cond) {
    if (!doc.contains(field)) return false;
    const json &val = doc[field];

    if (!cond.is_object()) {
        return value_eq(val, cond);
    }

    for (auto it = cond.begin(); it != cond.end(); ++it) {
        std::string op = it.key();
        const json &arg = it.value();
        if (op == "$eq") {
            if (!value_eq(val, arg)) return false;
        } else if (op == "$gt") {
            if (!(val.is_number() && arg.is_number() && val.get<double>() > arg.get<double>())) return false;
        } else if (op == "$lt") {
            if (!(val.is_number() && arg.is_number() && val.get<double>() < arg.get<double>())) return false;
        } else if (op == "$like") {
            if (!val.is_string()) return false;
            if (!arg.is_string()) return false;
            if (!match_like(val.get<std::string>(), arg.get<std::string>())) return false;
        } else if (op == "$in") {
            if (!arg.is_array()) return false;
            bool any=false;
            for (const auto &x : arg) {
                if (value_eq(val, x)) { any=true; break; }
            }
            if (!any) return false;
        } else {
            return false;
        }
    }
    return true;
}

bool evaluate_query(const json &doc, const json &query) {
    if (!query.is_object()) return false;

    if (query.contains("$or")) {
        const json &arr = query["$or"];
        if (!arr.is_array()) return false;
        for (const auto &sub : arr) {
            if (evaluate_query(doc, sub)) return true;
        }
        return false;
    }

    if (query.contains("$and")) {
        const json &arr = query["$and"];
        if (!arr.is_array()) return false;
        for (const auto &sub : arr) {
            if (!evaluate_query(doc, sub)) return false;
        }
        return true;
    }

    for (auto it = query.begin(); it != query.end(); ++it) {
        std::string field = it.key();
        const json &cond = it.value();
        if (!evaluate_condition_on_field(doc, field, cond)) return false;
    }
    return true;
}
