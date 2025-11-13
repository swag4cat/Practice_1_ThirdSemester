#include "../include/btree_index.hpp"
#include <iostream>

BTreeNode::BTreeNode(bool isLeaf) : leaf(isLeaf) {}

BTreeIndex::BTreeIndex(int t) : t(t), root(std::make_shared<BTreeNode>(true)) {}

void BTreeIndex::splitChild(std::shared_ptr<BTreeNode> x, int i, std::shared_ptr<BTreeNode> y) {
    auto z = std::make_shared<BTreeNode>(y->leaf);
    for (int j = 0; j < t - 1; j++) {
        z->keys.push_back(y->keys[j + t]);
        z->ids.push_back(y->ids[j + t]);
    }
    if (!y->leaf) {
        for (int j = 0; j < t; j++) z->children.push_back(y->children[j + t]);
    }
    y->keys.resize(t - 1);
    y->ids.resize(t - 1);
    if (!y->leaf) y->children.resize(t);

    x->children.insert(i + 1, z);
    x->keys.insert(i, y->keys[t - 1]);
    x->ids.insert(i, y->ids[t - 1]);
}

void BTreeIndex::insertNonFull(std::shared_ptr<BTreeNode> x, double k, const std::string &id) {
    int i = (int)x->keys.size() - 1;
    if (x->leaf) {
        while (i >= 0 && k < x->keys[i]) i--;
        if (i >= 0 && x->keys[i] == k) {
            x->ids[i].push_back(id);
            return;
        }

        x->keys.insert(i + 1, k);
        Vector<std::string> new_id_vec;
        new_id_vec.push_back(id);
        x->ids.insert(i + 1, new_id_vec);
    } else {
        while (i >= 0 && k < x->keys[i]) i--;
        i++;
        if ((int)x->children[i]->keys.size() == 2*t - 1) {
            splitChild(x, i, x->children[i]);
            if (k > x->keys[i]) i++;
        }
        insertNonFull(x->children[i], k, id);
    }
}

void BTreeIndex::insert(double key, const std::string &id) {
    if ((int)root->keys.size() == 2*t - 1) {
        auto s = std::make_shared<BTreeNode>(false);
        s->children.push_back(root);
        splitChild(s, 0, root);
        root = s;
    }
    insertNonFull(root, key, id);
}

Vector<std::string> BTreeIndex::searchNode(std::shared_ptr<BTreeNode> x, double k) const {
    int i = 0;
    while (i < (int)x->keys.size() && k > x->keys[i]) i++;
    if (i < (int)x->keys.size() && k == x->keys[i]) return x->ids[i];
    if (x->leaf) return Vector<std::string>();
    return searchNode(x->children[i], k);
}

Vector<std::string> BTreeIndex::search(double key) const {
    return searchNode(root, key);
}

void BTreeIndex::rangeSearchNode(std::shared_ptr<BTreeNode> x, double low, double high, bool includeLow, bool includeHigh, Vector<std::string> &result) const {
    int i;
    for (i = 0; i < (int)x->keys.size(); i++) {
        if (!x->leaf) rangeSearchNode(x->children[i], low, high, includeLow, includeHigh, result);
        double k = x->keys[i];
        bool inRange = (k > low || (includeLow && k == low)) && (k < high || (includeHigh && k == high));
        if (inRange) {
            for (const auto& id : x->ids[i]) {
                result.push_back(id);
            }
        }
    }
    if (!x->leaf) rangeSearchNode(x->children[i], low, high, includeLow, includeHigh, result);
}

Vector<std::string> BTreeIndex::rangeSearch(double low, double high, bool includeLow, bool includeHigh) const {
    Vector<std::string> result;
    rangeSearchNode(root, low, high, includeLow, includeHigh, result);
    return result;
}

json BTreeIndex::to_json(std::shared_ptr<BTreeNode> node) const {
    if (!node) node = root;
    json j;
    j["leaf"] = node->leaf;
    j["keys"] = node->keys;
    j["ids"] = node->ids;
    if (!node->leaf) {
        j["children"] = json::array();
        for (auto &ch : node->children) j["children"].push_back(to_json(ch));
    }
    return j;
}

template<typename T>
Vector<T> json_to_vector(const json& j) {
    Vector<T> result;
    for (const auto& item : j) {
        result.push_back(item.get<T>());
    }
    return result;
}

std::shared_ptr<BTreeNode> BTreeIndex::load_node(const json &j) const {
    auto node = std::make_shared<BTreeNode>(j["leaf"]);

    node->keys = json_to_vector<double>(j["keys"]);

    Vector<Vector<std::string>> ids_vec;
    for (const auto& id_array : j["ids"]) {
        ids_vec.push_back(json_to_vector<std::string>(id_array));
    }
    node->ids = ids_vec;

    if (!node->leaf) {
        for (auto &child : j["children"]) {
            node->children.push_back(load_node(child));
        }
    }
    return node;
}

void BTreeIndex::from_json(const json &j) {
    root = load_node(j);
}
