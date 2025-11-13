#pragma once
#include <string>
#include "../parcer/json.hpp"
#include "vector.hpp"

using json = nlohmann::json;

bool match_like(const std::string &value, const std::string &pattern);
bool value_eq(const json &a, const json &b);
bool evaluate_condition_on_field(const json &doc, const std::string &field, const json &cond);
bool evaluate_query(const json &doc, const json &query);
