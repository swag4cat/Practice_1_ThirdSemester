#include "../include/collection.hpp"
#include <iostream>
#include <iomanip>

void print_json_array(const Vector<json>& v) {
    json out = json::array();
    for (auto &d : v) out.push_back(d);
    std::cout << std::setw(2) << out << std::endl;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: ./no_sql_dbms <database_dir> <command> <args...>\n";
        std::cerr << "Commands:\n  insert '<json_doc>'\n  find '<json_query>'\n  delete '<json_query>'\n  create_index <field>\n";
        return 1;
    }
    std::string dbdir = argv[1];
    std::string cmd = argv[2];

    std::string collname = "collection";
    Collection coll(dbdir, collname);

    try {
        if (cmd == "insert") {
            if (argc < 4) { std::cerr<<"insert needs a JSON document\n"; return 1; }
            std::string docstr = argv[3];
            json doc = json::parse(docstr);
            std::string id = coll.insert(doc);
            std::cout << "Document inserted successfully. _id=" << id << "\n";
        } else if (cmd == "find") {
            std::string qstr = argv[3];
            json q = json::parse(qstr);
            auto res = coll.find(q);
            print_json_array(res);
        } else if (cmd == "delete") {
            std::string qstr = argv[3];
            json q = json::parse(qstr);
            int cnt = coll.remove(q);
            std::cout << "Deleted " << cnt << " documents.\n";
        } else if (cmd == "create_index") {
            if (argc < 4) { std::cerr<<"create_index needs a field name\n"; return 1; }
            std::string field = argv[3];
            coll.create_index(field);
            std::cout << "Index on '" << field << "' created.\n";
        } else {
            std::cerr << "Unknown command\n"; return 1;
        }
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << "\n"; return 2;
    }

    return 0;
}
