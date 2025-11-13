#pragma once
#include <memory>
#include "vector.hpp"
#include <string>
#include "../parcer/json.hpp"

using json = nlohmann::json;

struct BTreeNode {
    bool leaf;
    Vector<double> keys;
    Vector<Vector<std::string>> ids;
    Vector<std::shared_ptr<BTreeNode>> children;
    BTreeNode(bool isLeaf = true);
};

class BTreeIndex {
public:
    explicit BTreeIndex(int t = 3);
    void insert(double key, const std::string &id);
    Vector<std::string> search(double key) const;
    Vector<std::string> rangeSearch(double low, double high, bool includeLow = false, bool includeHigh = false) const;
    json to_json(std::shared_ptr<BTreeNode> node = nullptr) const;
    void from_json(const json &j);

private:
    int t;
    std::shared_ptr<BTreeNode> root;

    void splitChild(std::shared_ptr<BTreeNode> x, int i, std::shared_ptr<BTreeNode> y);
    void insertNonFull(std::shared_ptr<BTreeNode> x, double k, const std::string &id);
    Vector<std::string> searchNode(std::shared_ptr<BTreeNode> x, double k) const;
    void rangeSearchNode(std::shared_ptr<BTreeNode> x, double low, double high, bool includeLow, bool includeHigh, Vector<std::string> &result) const;
    std::shared_ptr<BTreeNode> load_node(const json &j) const;
};
