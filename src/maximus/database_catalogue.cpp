#include <filesystem>
#include <maximus/database_catalogue.hpp>

namespace fs = std::filesystem;

namespace maximus {
DatabaseCatalogue::DatabaseCatalogue(std::string base_path): base_path_(base_path) {
}

std::string DatabaseCatalogue::table_path(std::string table_name) const {
    // Check if a CSV file exists
    std::string full_table_name = table_name;
    std::string csv_file        = base_path_ + "/" + full_table_name + ".csv";
    if (fs::exists(csv_file)) {
        return csv_file;
    }

    // Check if a Parquet file exists
    std::string parquet_file = base_path_ + "/" + full_table_name + ".parquet";
    if (fs::exists(parquet_file)) {
        return parquet_file;
    }

    throw std::runtime_error("Table not found: " + full_table_name);
}

std::shared_ptr<DatabaseCatalogue> make_catalogue(std::string base_path) {
    return std::make_shared<DatabaseCatalogue>(base_path);
}
}  // namespace maximus
