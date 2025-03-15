#include <arrow/acero/hash_join.h>
#include <arrow/acero/util.h>
#include <arrow/result.h>
#include <gtest/gtest.h>

#include <maximus/database.hpp>
#include <maximus/database_catalogue.hpp>

using namespace maximus;

namespace test {

TEST(sql, SelectFrom) {
    std::string path = PROJECT_SOURCE_DIR;
    path += "/tests/tpch/csv";
    std::cout << "Path: " << path << std::endl;

    auto catalogue = make_catalogue(path);
    auto db        = make_database(catalogue);

    auto table = db->query("SELECT * FROM customer;");

    EXPECT_TRUE(table);

    std::cout << table->to_string() << std::endl;
}

TEST(sql, HashJoin) {
    std::string path = PROJECT_SOURCE_DIR;
    path += "/tests/tpch/csv";
    std::cout << "Path: " << path << std::endl;

    auto catalogue = make_catalogue(path);
    auto db        = make_database(catalogue);

    auto table = db->query(
        "SELECT * FROM customer INNER JOIN orders ON customer.c_custkey = orders.o_custkey;");

    EXPECT_TRUE(table);

    if (table) std::cout << table->to_string() << std::endl;
}

TEST(sql, Multicolumn2HashJoin) {
    std::string path = PROJECT_SOURCE_DIR;
    path += "/tests/tpch/csv";
    std::cout << "Path: " << path << std::endl;

    auto catalogue = make_catalogue(path);
    auto db        = make_database(catalogue);

    auto table = db->query("SELECT * FROM customer INNER JOIN orders ON customer.c_custkey = "
                           "orders.o_custkey AND customer.c_name = orders.o_comment;");

    EXPECT_FALSE(table);

    // std::cout << table->to_string() << std::endl;
}
}  // namespace test

