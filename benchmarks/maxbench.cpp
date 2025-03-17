#include <cxxopts.hpp>
#include <maximus/operators/acero/interop.hpp>
#include <maximus/tpch/tpch_queries.hpp>

#include "utils.hpp"

int main(int argc, char** argv) {
    cxxopts::Options options("MAXIMUS BENCHMARKS (MAXBENCH)",
                             "Running benchmarks with Maximus and Apache Acero query engines.");
    options.add_options()(
        "path", "Path to the CSV files", cxxopts::value<std::string>()->default_value(csv_path()))(
        "engines",
        "Engines to run the queries with. Options: maximus, acero and maximus,acero (to run both).",
        cxxopts::value<std::string>()->default_value({"maximus"}))(
        "r,n_reps", "Number of repetitions", cxxopts::value<int>()->default_value("1"))(
        "benchmark",
        "which benchmark to run? (tpch, h2o, clickbench)",
        cxxopts::value<std::string>()->default_value("tpch"))(
        "q,queries",
        "name of the query in the benchmark",
        cxxopts::value<std::vector<std::string>>()->default_value({"q1"}))(
        "d,device",
        "Device to run the queries on. Options: cpu, gpu",
        cxxopts::value<std::string>()->default_value("cpu"))(
        "b,csv_batch_size",
        "Batch size (num. of rows) for reading CSV files. Options: e.g. 2^20, 2^30, max. If max "
        "chosen, all tables will be repackaged to a single chunk.",
        cxxopts::value<std::string>()->default_value("2^30"))(
        "s,storage_device",
        "Device where the tables are initially residing. Options: cpu, cpu-pinned, gpu",
        cxxopts::value<std::string>()->default_value("cpu"))(
        "n_reps_storage",
        "How many repetitions to run for loading the tables. This is useful for benchmarking I/O.",
        cxxopts::value<int>()->default_value("1"))(
        "persist_results",
        "Whether to write the resulting table to a csv file. No = Do not write, Any other value = "
        "Do write.",
        cxxopts::value<std::string>()->default_value("no"))(
        "p,profile",
        "Profiling options: see Caliper -P command-line arguments.",
        // cxxopts::value<std::string>()->default_value("runtime-report,calc.inclusive,mem.highwatermark,sample-report,cuda-activity-report"))(
        // cxxopts::value<std::string>()->default_value("runtime-report,calc.inclusive"))(
        cxxopts::value<std::string>()->default_value(
            "runtime-report(calc.inclusive=true,output=stdout),event-trace"))("h,help",
                                                                              "Print help");

    auto result = options.parse(argc, argv);
    if (result.count("help")) {
        std::cout << options.help() << std::endl;
        return 0;
    }

    // choose the base path where all the tables are stored
    auto path = result["path"].as<std::string>();
    // which engines to run the queries with
    auto engines = result["engines"].as<std::string>();
    // how many repetitions to run
    auto n_reps = result["n_reps"].as<int>();
    // which benchmark to run
    auto benchmark = result["benchmark"].as<std::string>();
    // which queries to run
    auto queries = result["queries"].as<std::vector<std::string>>();
    // which device to run the queries on
    auto device_string = result["device"].as<std::string>();
    // the device where the tables are stored
    auto storage_device_string = result["storage_device"].as<std::string>();
    // how many times to load tables
    auto n_reps_storage = result["n_reps_storage"].as<int>();
    // whether to enable profiling
    auto profile = result["profile"].as<std::string>();
    // the batch size for reading CSV files
    auto csv_batch_size_string = result["csv_batch_size"].as<std::string>();
    // Whether to persist the data
    auto persist_results = result["persist_results"].as<std::string>();

    // initialize the profiler if compiled with profiling enabled
    PROFILER_INIT(mgr, profile);
    PROFILER_START(mgr);

    // create a database catalogue and a database connection
    auto context = maximus::make_context();

    auto device         = maximus::DeviceType::CPU;
    auto storage_device = maximus::DeviceType::CPU;
    if (device_string == "gpu") {
        device = maximus::DeviceType::GPU;
    }
    if (storage_device_string == "gpu") {
        storage_device = maximus::DeviceType::GPU;
    }
    if (storage_device_string == "cpu-pinned") {
        context->tables_initially_pinned = true;
    }

    if (csv_batch_size_string == "max") {
        context->csv_batch_size                   = 1 << 30;
        context->tables_initially_as_single_chunk = true;
    } else {
        context->csv_batch_size = maximus::get_value<int32_t>(csv_batch_size_string, 1 << 30);
    }

    auto db_catalogue = maximus::make_catalogue(path);
    auto db           = maximus::make_database(db_catalogue, context);

    auto acero_engine                                           = maximus::AceroExecutor();
    std::vector<std::string> tables                             = get_table_names(benchmark);
    std::vector<std::shared_ptr<maximus::Schema>> table_schemas = get_table_schemas(benchmark);

    // preload all the tables, if you don't want to include I/O in the benchmarks
    std::vector<int64_t> timings_io(n_reps_storage, 0);
    for (int i = 0; i < n_reps_storage; ++i) {
        context->barrier();
        auto start = std::chrono::high_resolution_clock::now();
        load_tables(db, tables, table_schemas, storage_device);
        context->barrier();
        auto end      = std::chrono::high_resolution_clock::now();
        timings_io[i] = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    }

    std::cout << "===================================" << std::endl;
    std::cout << "          LOADING TABLES           " << std::endl;
    std::cout << "===================================" << std::endl;
    std::cout << "Loading tables to:                   " << storage_device_string << "\n";
    std::cout << "Loading times over repetitions [ms]: ";
    for (int i = 0; i < n_reps_storage; ++i) {
        std::cout << timings_io[i] << ",\t";
    }
    std::cout << "\n";

    std::cout << "===================================" << std::endl;
    std::cout << "    MAXBENCH " << uppercase(benchmark) << " BENCHMARK:    " << std::endl;
    std::cout << "===================================" << std::endl;
    std::cout << "---> benchmark:                " << uppercase(benchmark) << "\n";
    std::cout << "---> queries:                  " << to_string(queries) << "\n";
    std::cout << "---> Tables path:              " << path << "\n";
    std::cout << "---> Engines:                  " << engines << "\n";
    std::cout << "---> Number of reps:           " << n_reps << "\n";
    std::cout << "---> Device:                   " << device_string << "\n";
    std::cout << "---> Storage Device:           " << storage_device_string << "\n";
    std::cout << "---> Num. outer threads:       " << context->n_outer_threads << "\n";
    std::cout << "---> Num. inner threads:       " << context->n_inner_threads << "\n";
    std::cout << "---> Operators Fusion:         " << (context->fusing_enabled ? "ON" : "OFF")
              << "\n";
    std::cout << "---> CSV Batch Size (string):  " << csv_batch_size_string << "\n";
    std::cout << "---> CSV Batch (number):       " << context->csv_batch_size << "\n";
    std::cout << "---> Tables initially pinned:  "
              << (context->tables_initially_pinned ? "YES" : "NO") << "\n";
    std::cout << "---> Tables as single chunk:   "
              << (context->tables_initially_as_single_chunk ? "YES" : "NO") << "\n";

    std::vector<std::vector<int64_t>> timings_maximus(queries.size(),
                                                      std::vector<int64_t>(n_reps, 0));
    std::vector<std::vector<int64_t>> timings_acero(queries.size(),
                                                    std::vector<int64_t>(n_reps, 0));

    std::vector<maximus::TablePtr> maximus_result_tables(queries.size());
    std::vector<maximus::TablePtr> acero_result_tables(queries.size());

    int query_idx = 0;

    for (const auto& query : queries) {
        // PE(query);
        std::shared_ptr<maximus::QueryPlan> query_plan_acero = get_query(query, db, benchmark);
        std::shared_ptr<maximus::QueryPlan> query_plan_maximus =
            get_query(query, db, device, benchmark);

        std::cout << "===================================" << std::endl;
        std::cout << "            QUERY " << query << std::endl;
        std::cout << "===================================" << std::endl;
        std::cout << "---> query: " << query << "\n";
        std::cout << "---> Query Plan: \n";
        std::cout << query_plan_maximus->to_string() << "\n";

        PE(query);
        for (int i = 0; i < n_reps; ++i) {
            // recreate the query plans as table sources might contain dangling pointers
            // since the tables have been exported out of the source operators
            query_plan_acero = get_query(query, db, benchmark);
            query_plan_maximus = get_query(query, db, device, benchmark);

            PE("Repetition_" + std::to_string(i));
            // define the variables
            auto start_time = std::chrono::high_resolution_clock::now();
            auto end_time   = std::chrono::high_resolution_clock::now();

            std::vector<maximus::TablePtr> result;

            // std::cout << "Running acero" << std::endl;
            if (maximus::contains(engines, "acero")) {
                if (benchmark == "tpch" && (query == "q2" || query == "q20")) {
                    // if (i > 0) {
                    timings_acero[query_idx][i] = -1;
                    // }
                    if (i == n_reps - 1) {
                        acero_result_tables[query_idx] = nullptr;
                    }
                } else {
                    acero_engine.schedule(query_plan_acero);

                    context->barrier();
                    start_time = std::chrono::high_resolution_clock::now();
                    acero_engine.execute();
                    context->barrier();
                    result   = acero_engine.results();
                    end_time = std::chrono::high_resolution_clock::now();

                    // discard the first run as it is a warm-up
                    // if (i > 0) {
                    timings_acero[query_idx][i] =
                        std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time)
                            .count();
                    // }

                    // if this is the last repetition, store the result
                    if (i == n_reps - 1) {
                        acero_result_tables[query_idx] = result[0];
                    }
                }
            }

            // std::cout << "Running maximus" << std::endl;
            if (maximus::contains(engines, "maximus")) {
                db->schedule(query_plan_maximus);
                if (i == 0) {
                    std::cout << "Query Plan after scheduling: \n"
                              << query_plan_maximus->to_string() << "\n";
                }
                context->barrier();
                start_time = std::chrono::high_resolution_clock::now();
                result     = db->execute();
                context->barrier();
                end_time = std::chrono::high_resolution_clock::now();
                // discard the first run as it is a warm-up
                // if (i > 0) {
                timings_maximus[query_idx][i] =
                    std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time)
                        .count();
                // }
                // if this is the last repetition, store the result
                if (i == n_reps - 1) {
                    maximus_result_tables[query_idx] = result[0];
                }
            }
            PL("Repetition_" + std::to_string(i));
        }
        PL(query);

        std::cout << "===================================" << std::endl;
        std::cout << "              RESULTS              " << std::endl;
        std::cout << "===================================" << std::endl;

        print_output_table(context, engines, "maximus", maximus_result_tables[query_idx]);
        print_output_table(context, engines, "acero", acero_result_tables[query_idx]);

        // Persist result of last iteration if requested
        if (persist_results != "no") {
            write_result_to_file(context,
                                 engines,
                                 "maximus",
                                 device_string,
                                 query_idx,
                                 maximus_result_tables[query_idx]);
            write_result_to_file(context,
                                 engines,
                                 "acero",
                                 device_string,
                                 query_idx,
                                 maximus_result_tables[query_idx]);
        }

        context->barrier();

        std::cout << "===================================" << std::endl;
        std::cout << "              TIMINGS              " << std::endl;
        std::cout << "===================================" << std::endl;
        std::cout << "Execution times [ms]: \n";
        timing_stats maximus_stats;
        timing_stats acero_stats;

        if (maximus::contains(engines, "maximus")) {
            maximus_stats = timing_stats(timings_maximus, queries, "maximus", device);
            std::cout << "- MAXIMUS TIMINGS [ms]: " << maximus_stats.flattened[query_idx]
                      << std::endl;
        }
        if (maximus::contains(engines, "acero")) {
            acero_stats = timing_stats(timings_acero, queries, "acero", maximus::DeviceType::CPU);
            std::cout << "- ACERO TIMINGS   [ms]: " << acero_stats.flattened[query_idx] << std::endl
                      << std::endl;
        }
        std::cout << "Execution stats (min, max, avg): \n";

        if (maximus::contains(engines, "maximus")) {
            std::cout << "- MAXIMUS STATS: MIN = " << maximus_stats.min[query_idx]
                      << " ms; \tMAX = " << maximus_stats.max[query_idx]
                      << " ms; \tAVG = " << maximus_stats.avg[query_idx] << " ms"
                      << "\n";
        }

        if (maximus::contains(engines, "acero")) {
            std::cout << "- ACERO STATS  : MIN = " << acero_stats.min[query_idx]
                      << " ms; \tMAX = " << acero_stats.max[query_idx]
                      << " ms; \tAVG = " << acero_stats.avg[query_idx] << " ms"
                      << "\n\n";
        }

        // PL(query);
        ++query_idx;
        context->barrier();
    }

    std::cout << "===================================" << std::endl;
    std::cout << "        SUMMARIZED TIMINGS         " << std::endl;
    std::cout << "===================================" << std::endl;
    std::string filename = "./results.csv";
    std::stringstream csv_results_stream;

    if (maximus::contains(engines, "maximus")) {
        auto maximus_stats = timing_stats(timings_maximus, queries, "maximus", device);
        csv_results_stream << maximus_stats.csv_results;
    }

    if (maximus::contains(engines, "acero")) {
        auto acero_stats = timing_stats(timings_acero, queries, "acero", maximus::DeviceType::CPU);
        csv_results_stream << acero_stats.csv_results;
    }

    print_timings(csv_results_stream.str(), "results.csv");
    std::cout << "--->Results saved to " << filename << std::endl;
    std::cout << csv_results_stream.str() << std::endl;

    PROFILER_FLUSH(mgr);

    return 0;
}
