#include "duckdb.hpp"
#include <iostream>

using namespace duckdb;

int main() {
    try {
        DuckDB db("my_duck_database.db");
        Connection con(db);

        // auto result = con.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';");

        // if (result->HasError()) {
        //     std::cerr << "Query failed: " << result->GetError() << std::endl;
        //     return 1;
        // }

        // std::cout << "Tables in database:" << std::endl;
        // while (result->Next()) {
        //     std::cout << "- " << result->GetValue(0, 0).ToString() << std::endl;
        // }

    } catch (std::exception &ex) {
        std::cerr << "Exception: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
