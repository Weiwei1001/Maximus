#pragma once

#include <cmath>
#include <sstream>
#include <string>
#include <thread>

namespace maximus {
namespace env_vars_names {
// the number of threads that the executer is using for executing multiple independent pipelines
const std::string MAXIMUS_NUM_OUTER_THREADS = "MAXIMUS_NUM_OUTER_THREADS";
// the number of threads that each kernel (operator) within a pipeline is using
const std::string MAXIMUS_NUM_INNER_THREADS = "MAXIMUS_NUM_INNER_THREADS";
// if enabled, the operators within a pipeline that belong to the same engine will be fused together
const std::string MAXIMUS_OPERATORS_FUSION = "MAXIMUS_OPERATORS_FUSION";
// the batch size when reading csv files
const std::string MAXIMUS_CSV_BATCH_SIZE = "MAXIMUS_CSV_BATCH_SIZE";
// the max size of the pinned memory to be allocated in bytes
const std::string MAXIMUS_MAX_PINNED_POOL_SIZE = "MAXIMUS_MAX_PINNED_POOL_SIZE";
}  // namespace env_vars_names

namespace env_vars_defaults {
const int MAXIMUS_NUM_OUTER_THREADS            = 1;
const int MAXIMUS_NUM_INNER_THREADS            = std::thread::hardware_concurrency();
const bool MAXIMUS_OPERATORS_FUSION            = true;
const int32_t MAXIMUS_CSV_BATCH_SIZE           = 1 << 30;
const std::size_t MAXIMUS_MAX_PINNED_POOL_SIZE = std::size_t{4} * 1024 * 1024 * 1024;
}  // namespace env_vars_defaults

// checks if the specified environment variable is defined
bool env_var_defined(const char* var_name);

// Helper function to check if a number is a power of 2
bool is_power_of_two(std::size_t n);

// Helper function to find the exponent if the number is a power of 2
int find_exponent(std::size_t n);

template<typename T>
std::string pretty_print(T value) {
    if constexpr (std::is_integral_v<T>) {
        if (is_power_of_two(static_cast<std::size_t>(value))) {
            int exponent = find_exponent(static_cast<std::size_t>(value));
            return "2^" + std::to_string(exponent);
        }
    }
    return std::to_string(value);
}

template<typename T>
T get_value(const std::string& value, T default_value) {
    std::string str_value(value);

    // Check if the string contains '^'
    size_t pos = str_value.find('^');
    if (pos != std::string::npos) {
        // Parse the base and the exponent
        try {
            int base     = std::stoi(str_value.substr(0, pos));
            int exponent = std::stoi(str_value.substr(pos + 1));
            if (base == 2) {
                T result = static_cast<T>(std::pow(base, exponent));
                return result;
            }
        } catch (const std::exception& e) {
            // If parsing fails, fall back to default_value
            return default_value;
        }
    }

    // If no '^' found, try to convert the string to T directly
    std::istringstream iss(str_value);
    T result;
    if (!(iss >> result)) {
        return default_value;
    }
    return result;
}

template<typename T>
T get_env_var(const std::string& name, T default_value) {
    const char* value = std::getenv(name.c_str());
    if (!value) {
        return default_value;
    }
    return get_value<T>(value, default_value);
}

int get_num_outer_threads();

int get_num_inner_threads();

bool get_operators_fusion();

int32_t get_csv_batch_size();

std::size_t get_max_pinned_pool_size();

}  // namespace maximus
