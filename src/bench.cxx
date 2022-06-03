#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <benchmark/benchmark.h>

#include "src/core/types.hpp"
#include "src/core/settings.hpp"
#include "src/core/profiler.hpp"
#include "src/core/db.hpp"
#include "src/core/workload.hpp"
#include "src/core/worker.hpp"
#include "src/core/db_brand.hpp"
#include "src/core/distribution.hpp"
#include "src/core/operation.hpp"
#include "src/core/exception.hpp"
#include "src/core/printable.hpp"
#include "src/core/results.hpp"
#include "src/core/threads_fence.hpp"

namespace bm = benchmark;

using settings_t = ucsb::settings_t;
using workload_t = ucsb::workload_t;
using workloads_t = ucsb::workloads_t;
using db_t = ucsb::db_t;
using data_accessor_t = ucsb::data_accessor_t;
using db_brand_t = ucsb::db_brand_t;
using timer_ref_t = ucsb::timer_ref_t;
using worker_t = ucsb::worker_t;
using distribution_kind_t = ucsb::distribution_kind_t;
using operation_kind_t = ucsb::operation_kind_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using operation_chooser_t = std::unique_ptr<ucsb::operation_chooser_t>;
using cpu_profiler_t = ucsb::cpu_profiler_t;
using mem_profiler_t = ucsb::mem_profiler_t;
using printable_bytes_t = ucsb::printable_bytes_t;
using threads_fence_t = ucsb::threads_fence_t;

inline void usage_message(const char* command) {
    fmt::print("Usage: {} [options]\n", command);
    fmt::print("Options:\n");
    fmt::print("-db: Database name\n");
    fmt::print("-t: transactional\n");
    fmt::print("-c: Database configuration file path\n");
    fmt::print("-w: Workloads file path\n");
    fmt::print("-wd: Working dir path\n");
    fmt::print("-r: Results dir path\n");
    fmt::print("-filter: Workload filter (Optional)\n");
    fmt::print("-threads: Threads count (Optional, default: 1)\n");
}

void parse_and_validate_args(int argc, char* argv[], settings_t& settings) {
    int arg_idx = 1;
    while (arg_idx < argc && ucsb::start_with(argv[arg_idx], "-")) {
        if (strcmp(argv[arg_idx], "-db") == 0) {
            ++arg_idx;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -db\n");
                exit(1);
            }
            settings.db_name = std::string(argv[arg_idx]);
            ++arg_idx;
        }
        else if (strcmp(argv[arg_idx], "-t") == 0) {
            settings.transactional = true;
            ++arg_idx;
        }
        else if (strcmp(argv[arg_idx], "-c") == 0) {
            ++arg_idx;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -c\n");
                exit(1);
            }
            settings.db_config_path = std::string(argv[arg_idx]);
            ++arg_idx;
        }
        else if (strcmp(argv[arg_idx], "-w") == 0) {
            ++arg_idx;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -w\n");
                exit(1);
            }
            settings.workloads_path = std::string(argv[arg_idx]);
            ++arg_idx;
        }
        else if (strcmp(argv[arg_idx], "-r") == 0) {
            ++arg_idx;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -r\n");
                exit(1);
            }
            std::string path(argv[arg_idx]);
            if (path.back() != '/')
                path.push_back('/');
            settings.results_path = path;
            ++arg_idx;
        }
        else if (strcmp(argv[arg_idx], "-wd") == 0) {
            ++arg_idx;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -wd\n");
                exit(1);
            }
            std::string path(argv[arg_idx]);
            if (path.back() != '/')
                path.push_back('/');
            settings.working_dir_path = path;
            ++arg_idx;
        }
        else if (strcmp(argv[arg_idx], "-threads") == 0) {
            ++arg_idx;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -threads\n");
                exit(1);
            }
            settings.threads_count = std::stoi(argv[arg_idx]);
            ++arg_idx;
        }
        else if (strcmp(argv[arg_idx], "-filter") == 0) {
            ++arg_idx;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -filter\n");
                exit(1);
            }
            settings.workload_filter = std::string(argv[arg_idx]);
            ++arg_idx;
        }
        else {
            usage_message(argv[0]);
            fmt::print("Unknown option '{}'\n", argv[arg_idx]);
            exit(1);
        }
    }

    if (arg_idx == 1 || arg_idx != argc) {
        usage_message(argv[0]);
        exit(1);
    }

    if (settings.db_name.empty()) {
        fmt::print("-db: DB name not specified\n");
        exit(1);
    }
    if (settings.db_config_path.empty()) {
        fmt::print("-c: DB configuration file path not specified\n");
        exit(1);
    }
    if (settings.workloads_path.empty()) {
        fmt::print("-w: workloads file path not specified\n");
        exit(1);
    }
    if (settings.working_dir_path.empty()) {
        fmt::print("-wd: working dir path not specified\n");
        exit(1);
    }
    if (settings.results_path.empty()) {
        fmt::print("-r: results dir path not specified\n");
        exit(1);
    }
}

inline void register_section(std::string const& name) {
    bm::RegisterBenchmark(name.c_str(), [=](bm::State& s) {
        for (auto _ : s)
            ;
    });
}

inline std::string section_name(settings_t const& settings, workloads_t const& workloads) {

    std::vector<std::string> infos;
    if (settings.transactional)
        infos.push_back("transactional");
    infos.push_back(fmt::format("threads: {}", settings.threads_count));
    if (!workloads.empty()) {
        size_t db_size = workloads[0].db_records_count * workloads[0].value_length;
        infos.push_back(fmt::format("size: {}", printable_bytes_t {db_size}));
    }

    std::string info = fmt::format("{}", fmt::join(infos, ", "));
    return fmt::format("{} [{}]", settings.db_name, info);
}

template <typename func_at>
inline void register_benchmark(std::string const& name, size_t threads_count, func_at func) {
    bm::RegisterBenchmark(name.c_str(), func)
        ->Threads(threads_count)
        ->Unit(bm::kMicrosecond)
        ->UseRealTime()
        ->Repetitions(1)
        ->Iterations(1);
}

void run_benchmarks(int argc, char* argv[], settings_t const& settings) {
    (void)argc;

    int bm_argc = 4;
    char* bm_argv[4];
    std::string arg0(argv[0]);
    bm_argv[0] = const_cast<char*>(arg0.c_str());

    std::string arg1("--benchmark_format=console");
    bm_argv[1] = const_cast<char*>(arg1.c_str());

    std::string arg2(fmt::format("--benchmark_out={}", settings.results_path.c_str()));
    bm_argv[2] = const_cast<char*>(arg2.c_str());

    std::string arg3("--benchmark_out_format=json");
    bm_argv[3] = const_cast<char*>(arg3.c_str());

    bm::Initialize(&bm_argc, bm_argv);
    if (bm::ReportUnrecognizedArguments(bm_argc, bm_argv)) {
        fmt::print("GoogleBM: Invalid Input Arguments\n");
        return;
    }

    benchmark::RunSpecifiedBenchmarks();
}

workloads_t filter_workloads(workloads_t const& workloads, std::string const& filter) {
    if (filter.empty())
        return workloads;

    // Note: It keeps order as mentioned in filter
    workloads_t filtered_workloads;
    std::vector<std::string> tokens = ucsb::split(filter, ',');
    for (auto const& token : tokens) {
        for (auto const& workload : workloads) {
            if (workload.name == token)
                filtered_workloads.push_back(workload);
        }
    }

    return filtered_workloads;
}

std::vector<workload_t> split_workload_into_threads(workload_t const& workload, size_t threads_count) {
    std::vector<workload_t> workloads;
    workloads.reserve(threads_count);

    auto records_count_per_thread = workload.db_records_count / threads_count;
    auto operations_count_per_thread = workload.db_operations_count / threads_count;
    auto leftover_records_count = workload.db_records_count % threads_count;
    auto leftover_operations_count = workload.db_operations_count % threads_count;

    size_t start_key = workload.start_key;
    for (size_t idx = 0; idx < threads_count; ++idx) {
        workload_t thread_workload = workload;
        thread_workload.records_count = records_count_per_thread + bool(leftover_records_count);
        thread_workload.operations_count = operations_count_per_thread + bool(leftover_operations_count);
        thread_workload.operations_count = std::max(size_t(1), thread_workload.operations_count);
        auto prev_thread_records_count = idx == 0 ? 0 : workloads[idx - 1].records_count;
        thread_workload.start_key = start_key;
        workloads.push_back(thread_workload);

        leftover_records_count -= bool(leftover_records_count);
        leftover_operations_count -= bool(leftover_operations_count);

        if (workload.upsert_proportion == 1.0 || workload.batch_upsert_proportion == 1.0 ||
            workload.bulk_load_proportion == 1.0)
        {
            size_t new_records_count =
                bool(workload.upsert_proportion) * thread_workload.operations_count +
                bool(workload.bulk_load_proportion) * thread_workload.operations_count * workload.bulk_load_max_length +
                bool(workload.batch_upsert_proportion) * thread_workload.operations_count *
                    workload.batch_upsert_max_length;
            start_key += new_records_count;
        }
        else
            start_key += workloads.back().records_count;
    }

    return workloads;
}

void validate_workload(workload_t const& workload, size_t threads_count) {

    assert(threads_count > 0);
    assert(!workload.name.empty());
    assert(workload.db_records_count > 0);
    assert(workload.db_operations_count > 0);

    float proportion = 0;
    proportion += workload.upsert_proportion;
    proportion += workload.update_proportion;
    proportion += workload.remove_proportion;
    proportion += workload.read_proportion;
    proportion += workload.read_modify_write_proportion;
    proportion += workload.batch_upsert_proportion;
    proportion += workload.batch_read_proportion;
    proportion += workload.bulk_load_proportion;
    proportion += workload.range_select_proportion;
    proportion += workload.scan_proportion;
    assert(proportion > 0.0 && proportion <= 1.0);

    assert(workload.value_length > 0);

    assert(workload.key_dist != distribution_kind_t::unknown_k);

    assert(workload.batch_upsert_proportion == 0.0 ||
           workload.batch_upsert_proportion > 0.0 && workload.batch_upsert_min_length > 0);
    assert(workload.batch_upsert_min_length <= workload.batch_upsert_max_length);
    assert(workload.batch_upsert_max_length <= workload.db_records_count / threads_count);

    assert(workload.batch_read_proportion == 0.0 ||
           workload.batch_read_proportion > 0.0 && workload.batch_read_min_length > 0);
    assert(workload.batch_read_min_length <= workload.batch_read_max_length);
    assert(workload.batch_read_max_length <= workload.db_records_count / threads_count);

    assert(workload.bulk_load_proportion == 0.0 ||
           workload.bulk_load_proportion > 0.0 && workload.bulk_load_min_length > 0);
    assert(workload.bulk_load_min_length <= workload.bulk_load_max_length);
    assert(workload.bulk_load_max_length <= workload.db_records_count / threads_count);

    assert(workload.range_select_proportion == 0.0 ||
           workload.range_select_proportion > 0.0 && workload.range_select_min_length > 0);
    assert(workload.range_select_min_length <= workload.range_select_max_length);
    assert(workload.range_select_max_length <= workload.db_records_count / threads_count);
}

inline operation_chooser_t create_operation_chooser(workload_t const& workload) {
    operation_chooser_t chooser = std::make_unique<ucsb::operation_chooser_t>();
    chooser->add(operation_kind_t::upsert_k, workload.upsert_proportion);
    chooser->add(operation_kind_t::update_k, workload.update_proportion);
    chooser->add(operation_kind_t::remove_k, workload.remove_proportion);
    chooser->add(operation_kind_t::read_k, workload.read_proportion);
    chooser->add(operation_kind_t::read_modify_write_k, workload.read_modify_write_proportion);
    chooser->add(operation_kind_t::batch_upsert_k, workload.batch_upsert_proportion);
    chooser->add(operation_kind_t::batch_read_k, workload.batch_read_proportion);
    chooser->add(operation_kind_t::bulk_load_k, workload.bulk_load_proportion);
    chooser->add(operation_kind_t::range_select_k, workload.range_select_proportion);
    chooser->add(operation_kind_t::scan_k, workload.scan_proportion);
    return chooser;
}

void bench(bm::State& state, workload_t const& workload, db_t& db, data_accessor_t& data_accessor) {

    auto chooser = create_operation_chooser(workload);
    timer_ref_t timer(state);
    worker_t worker(workload, data_accessor, timer);

    // Stats
    static size_t fails_count = 0;
    static size_t entries_touched = 0;
    static size_t bytes_processed_count = 0;
    cpu_profiler_t cpu_stat;
    mem_profiler_t mem_stat;

    // Progress
    static size_t done_iterations_count = 0;
    size_t last_printed_iterations_count = 0;
    size_t const iterations_total_count = workload.db_operations_count;
    size_t const printable_iterations_distance = iterations_total_count / 10;
    std::atomic_bool do_flash = true;

    if (state.thread_index() == 0) {
        fails_count = 0;
        entries_touched = 0;
        bytes_processed_count = 0;
        done_iterations_count = 0;
        last_printed_iterations_count = 0;
        cpu_stat.start();
        mem_stat.start();

        // Print initial progress
        fmt::print("{}: {:>6.2f}%\r", workload.name, 0.0);
        fflush(stdout);
    }

    while (state.KeepRunningBatch(workload.operations_count)) {
        size_t iterations_per_thread = workload.operations_count;
        while (iterations_per_thread) {
            operation_result_t result;
            auto operation = chooser->choose();
            switch (operation) {
            case operation_kind_t::upsert_k: result = worker.do_upsert(); break;
            case operation_kind_t::update_k: result = worker.do_update(); break;
            case operation_kind_t::remove_k: result = worker.do_remove(); break;
            case operation_kind_t::read_k: result = worker.do_read(); break;
            case operation_kind_t::read_modify_write_k: result = worker.do_read_modify_write(); break;
            case operation_kind_t::batch_upsert_k: result = worker.do_batch_upsert(); break;
            case operation_kind_t::batch_read_k: result = worker.do_batch_read(); break;
            case operation_kind_t::bulk_load_k: result = worker.do_bulk_load(); break;
            case operation_kind_t::range_select_k: result = worker.do_range_select(); break;
            case operation_kind_t::scan_k: result = worker.do_scan(); break;
            default: throw ucsb::exception_t("Unknown operation"); break;
            }

            bool success = result.status == operation_status_t::ok_k;
            ucsb::add_atomic(entries_touched, result.entries_touched);
            ucsb::add_atomic(fails_count, size_t(!success) * result.entries_touched);
            ucsb::add_atomic(bytes_processed_count, size_t(success) * workload.value_length * result.entries_touched);

            // Print progress
            ucsb::add_atomic(done_iterations_count, size_t(1));
            if (done_iterations_count - last_printed_iterations_count > printable_iterations_distance ||
                done_iterations_count <= state.threads() || done_iterations_count == iterations_total_count) {

                last_printed_iterations_count = done_iterations_count;
                float percent = 100.f * done_iterations_count / iterations_total_count;
                fmt::print("{}: {:>6.2f}%\r", workload.name, percent);
                fflush(stdout);
            }

            // Last thread will flush the DB
            bool only_once = true;
            if (done_iterations_count == iterations_total_count && do_flash.compare_exchange_weak(only_once, false))
                db.flush();
            --iterations_per_thread;
        }
    }

    if (state.thread_index() == 0) {
        cpu_stat.stop();
        mem_stat.stop();

        state.SetBytesProcessed(bytes_processed_count);
        state.counters["fails,%"] = bm::Counter(entries_touched ? fails_count * 100.0 / entries_touched : 100.0);
        state.counters["operations/s"] = bm::Counter(entries_touched - fails_count, bm::Counter::kIsRate);
        state.counters["cpu_max,%"] = bm::Counter(cpu_stat.percent().max);
        state.counters["cpu_avg,%"] = bm::Counter(cpu_stat.percent().avg);
        state.counters["mem_max,bytes"] = bm::Counter(mem_stat.rss().max, bm::Counter::kDefaults, bm::Counter::kIs1024);
        state.counters["mem_avg,bytes"] = bm::Counter(mem_stat.rss().avg, bm::Counter::kDefaults, bm::Counter::kIs1024);
        state.counters["processed,bytes"] =
            bm::Counter(bytes_processed_count, bm::Counter::kDefaults, bm::Counter::kIs1024);
        state.counters["disk,bytes"] = bm::Counter(db.size_on_disk(), bm::Counter::kDefaults, bm::Counter::kIs1024);
    }
}

void bench(bm::State& state, workload_t const& workload, db_t& db, bool transactional, threads_fence_t& fence) {

    if (state.thread_index() == 0) {
        if (!db.open())
            throw ucsb::exception_t("Failed to open DB");
    }
    fence.sync();

    if (transactional) {
        auto transaction = db.create_transaction();
        if (!transaction)
            throw ucsb::exception_t("Failed to create DB transaction");
        bench(state, workload, db, *transaction);
    }
    else
        bench(state, workload, db, db);

    fence.sync();
    if (state.thread_index() == 0) {
        if (!db.close())
            throw ucsb::exception_t("Failed to close DB");
    }
}

int main(int argc, char** argv) {

    try {
        // Setup settings
        settings_t settings;
        parse_and_validate_args(argc, argv, settings);
        std::string results_dir_path(
            fmt::format("{}/cores_{}/{}/", settings.results_path.string(), settings.threads_count, settings.db_name));
        std::string results_file_path =
            fmt::format("{}{}.json", results_dir_path, settings.workloads_path.stem().c_str());
        std::string partial_results_file_path =
            fmt::format("{}{}_partial.json", results_dir_path, settings.workloads_path.stem().c_str());
        bool partial_benchmark = !settings.workload_filter.empty();
        if (partial_benchmark)
            settings.results_path = partial_results_file_path;
        else
            settings.results_path = results_file_path;

        // Prepare worklods
        workloads_t workloads;
        if (!ucsb::load(settings.workloads_path, workloads)) {
            fmt::print("Failed to load workloads. path: {}\n", settings.workloads_path.c_str());
            return 1;
        }
        if (workloads.empty()) {
            fmt::print("Workloads file is empty. path: {}\n", settings.workloads_path.c_str());
            return 1;
        }
        workloads = filter_workloads(workloads, settings.workload_filter);
        if (workloads.empty()) {
            fmt::print("Filter dones't match any workload. filter: {}\n", settings.workload_filter);
            return 1;
        }
        std::vector<workloads_t> threads_workloads;
        for (auto const& workload : workloads) {
            validate_workload(workload, settings.threads_count);
            std::vector<workload_t> splited_workloads = split_workload_into_threads(workload, settings.threads_count);
            threads_workloads.push_back(splited_workloads);
        }

        ucsb::fs::create_directories(settings.working_dir_path.string());
        ucsb::fs::create_directories(settings.results_path.parent_path());

        // Setup DB
        db_brand_t db_brand = ucsb::parse_db_brand(settings.db_name);
        std::shared_ptr<db_t> db = ucsb::make_db(db_brand, settings.transactional);
        if (!db) {
            if (settings.transactional)
                fmt::print("Failed to create transactional DB: {}\n", settings.db_name);
            else
                fmt::print("Failed to create DB: {}\n", settings.db_name);
            return 1;
        }
        db->set_config(settings.db_config_path, settings.working_dir_path.string());

        threads_fence_t fence(settings.threads_count);

        // Register benchmarks
        register_section(section_name(settings, workloads));
        for (auto const& splited_workloads : threads_workloads) {
            std::string bench_name = splited_workloads.front().name;
            register_benchmark(bench_name, settings.threads_count, [&](bm::State& state) {
                auto const& workload = splited_workloads[state.thread_index()];
                bench(state, workload, *db, settings.transactional, fence);
            });
        }

        run_benchmarks(argc, argv, settings);

        if (partial_benchmark) {
            ucsb::marge_results(partial_results_file_path, results_file_path);
            ucsb::fs::remove(partial_results_file_path);
        }
    }
    catch (ucsb::exception_t const& ex) {
        fmt::print("exception: {}\n", ex.what());
    }
    catch (...) {
        fmt::print("Unknown exception was thrown\n");
    }

    return 0;
}
