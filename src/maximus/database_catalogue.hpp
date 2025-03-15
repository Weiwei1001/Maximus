#pragma once

#include <string>

namespace maximus {

class DatabaseCatalogue {
public:
    DatabaseCatalogue() = default;

    explicit DatabaseCatalogue(std::string base_path);

    [[nodiscard]] std::string table_path(std::string table_name) const;

private:
    std::string base_path_ = "./";
};

std::shared_ptr<DatabaseCatalogue> make_catalogue(std::string base_path);
}  // namespace maximus
