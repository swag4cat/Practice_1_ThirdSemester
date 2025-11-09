#include "../include/utils.hpp"
#include <random>
#include <sstream>
#include <chrono>

std::string gen_id() {
    static std::mt19937_64 rng(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    uint64_t a = rng();
    std::ostringstream oss;
    oss << std::hex << a;
    return oss.str();
}
