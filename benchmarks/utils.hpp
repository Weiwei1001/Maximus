#pragma once

#include <iostream>
#include <maximus/context.hpp>
#include <maximus/database.hpp>
#include <maximus/tpch/tpch_queries.hpp>
#include <maximus/utils/utils.hpp>
#include <maximus/clickbench/clickbench_queries.hpp>
#include <maximus/h2o/h2o_queries.hpp>

std::string csv_path() {
    std::string path = PROJECT_SOURCE_DIR;
    path += "/tests/tpch/csv-0.01";
    return path;
}

std::string parquet_path() {
    std::string path = PROJECT_SOURCE_DIR;
    path += "/tests/tpch/parquet";
    return path;
}

std::string to_string(const std::vector<std::string>& vec) {
    std::string str = "";
    for (unsigned i = 0u; i < vec.size(); ++i) {
        if (i == vec.size() - 1) {
            str += vec[i];
            continue;
        }
        str += vec[i] + ", ";
    }
    return str;
}

void print_output_table(const std::shared_ptr<maximus::MaximusContext>& ctx,
                        const std::string& engines,
                        const std::string& engine,
                        const maximus::TablePtr& table) {
    if (maximus::contains(engines, engine)) {
        std::cout << engine + " RESULTS (top 10 rows):"
                  << "\n";
        if (table) {
            table->slice(0, 10)->print();
            std::cout << "\n\n";
        } else {
            std::cout << "---> The query result is empty."
                      << "\n\n";
        }
    }
}

void write_result_to_file(const std::shared_ptr<maximus::MaximusContext>& ctx,
                          const std::string& engines,
                          const std::string& engine,
                          const std::string& device_string,
                          const int query_id,
                          const maximus::TablePtr& table) {
    if (maximus::contains(engines, engine)) {
        std::ostringstream oss;
        oss << engine << "_" << query_id << "." << device_string << ".csv";
        std::string target_name = oss.str();
        std::ofstream file;
        file.open(target_name);
        file << table->to_string();
        file.close();
        std::cout << "Query results saved to " << target_name << std::endl;
    }
}

struct timing_stats {
    timing_stats() = default;
    timing_stats(std::vector<std::vector<int64_t>>& timings,
                 std::vector<std::string> queries,
                 std::string engine,
                 maximus::DeviceType device) {
        std::string device_string = device == maximus::DeviceType::CPU ? "cpu" : "gpu";
        std::stringstream csv_results_stream;
        for (int i = 0; i < timings.size(); ++i) {
            csv_results_stream << device_string << "," << engine << "," << queries[i] << ",";
            min.push_back(*std::min_element(timings[i].begin(), timings[i].end()));
            max.push_back(*std::max_element(timings[i].begin(), timings[i].end()));
            avg.push_back(std::accumulate(timings[i].begin(), timings[i].end(), 0) /
                          timings[i].size());

            std::string timings_flattened = "\t";
            for (int j = 0; j < timings[i].size(); ++j) {
                timings_flattened += std::to_string(timings[i][j]);
                if (j != timings[i].size() - 1) {
                    timings_flattened += ", \t";
                }
                csv_results_stream << timings[i][j] << ",";
            }
            csv_results_stream << "\n";

            flattened.push_back(timings_flattened);
        }
        csv_results = csv_results_stream.str();
    }

    // maps queries to their min, max, and avg timings as well as a flattened string containing all the timings
    std::vector<int64_t> min;
    std::vector<int64_t> max;
    std::vector<int64_t> avg;
    std::vector<std::string> flattened;
    std::string csv_results;
};

void load_tables(const std::shared_ptr<maximus::Database>& db,
                 const std::vector<std::string>& tables,
                 const std::vector<std::shared_ptr<maximus::Schema>>& schemas = {},
                 const maximus::DeviceType& storage_device = maximus::DeviceType::CPU) {
    assert(schemas.empty() || schemas.size() == tables.size());
    for (unsigned i = 0u; i < tables.size(); ++i) {
        db->load_table(tables[i], schemas.empty() ? nullptr : schemas[i], {}, storage_device);
    }
}

void print_timings(const std::string& csv_results, const std::string& filename) {
    std::ofstream file;
    file.open(filename);
    file << csv_results;
    file.close();
}

std::vector<std::string> get_table_names(const std::string& benchmark) {
    if (benchmark == "tpch") {
        return maximus::tpch::table_names();
    }
    if (benchmark == "clickbench") {
        return maximus::clickbench::table_names();
    }
    if (benchmark == "h2o") {
        return maximus::h2o::table_names();
    }
    throw std::runtime_error(
        "The benchmark argument not recognized. It can only take the values {tpch, clickbench, h2o}");
}

std::vector<std::shared_ptr<maximus::Schema>> get_table_schemas(const std::string& benchmark) {
    if (benchmark == "tpch") {
        return maximus::tpch::schemas();
    }
    if (benchmark == "clickbench") {
        return maximus::clickbench::schemas();
    }
    if (benchmark == "h2o") {
        return maximus::h2o::schemas();
    }
    throw std::runtime_error(
        "The benchmark argument not recognized. It can only take the values {tpch, clickbench, h2o}");
}

std::shared_ptr<maximus::QueryPlan> get_query(const std::string& query,
                                              std::shared_ptr<maximus::Database>& db,
                                              const std::string& benchmark) {
    if (benchmark == "tpch") {
        return maximus::tpch::query_plan(query, db);
    }
    if (benchmark == "clickbench") {
        return maximus::clickbench::query_plan(query, db);
    }
    if (benchmark == "h2o") {
        return maximus::h2o::query_plan(query, db);
    }
    throw std::runtime_error(
        "The benchmark argument not recognized. It can only take the values {tpch, clickbench, h2o}");
}

std::shared_ptr<maximus::QueryPlan> get_query(const std::string& query,
                                              std::shared_ptr<maximus::Database>& db,
                                              maximus::DeviceType device,
                                              const std::string& benchmark) {
    if (benchmark == "tpch") {
        return maximus::tpch::query_plan(query, db, device);
    }
    if (benchmark == "clickbench") {
        return maximus::clickbench::query_plan(query, db, device);
    }
    if (benchmark == "h2o") {
        return maximus::h2o::query_plan(query, db, device);
    }
    throw std::runtime_error(
        "The benchmark argument not recognized. It can only take the values {tpch, clickbench, h2o}");
}

std::string uppercase(const std::string& s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}
