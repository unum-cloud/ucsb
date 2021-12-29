#include <memory>
#include <string>
#include <fmt/format.h>
#include <benchmark/benchmark.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/settings.hpp"
#include "ucsb/core/stat.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/workload.hpp"
#include "ucsb/core/transaction.hpp"
#include "ucsb/core/factory.hpp"
#include "ucsb/core/operation.hpp"
#include "ucsb/core/exception.hpp"
#include "ucsb/core/format.hpp"

namespace bm = benchmark;

using settings_t = ucsb::settings_t;
using workload_t = ucsb::workload_t;
using workloads_t = ucsb::workloads_t;
using db_t = ucsb::db_t;
using db_kind_t = ucsb::db_kind_t;
using factory_t = ucsb::factory_t;
using transaction_t = ucsb::transaction_t;
using operation_kind_t = ucsb::operation_kind_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using operation_chooser_t = std::unique_ptr<ucsb::operation_chooser_t>;
using cpu_stat_t = ucsb::cpu_stat_t;
using mem_stat_t = ucsb::mem_stat_t;
using exception_t = ucsb::exception_t;
using printable_bytes_t = ucsb::printable_bytes_t;

inline bool start_with(const char* str, const char* prefix) {
    return strncmp(str, prefix, strlen(prefix)) == 0;
}

inline void usage_message(const char* command) {
    fmt::print("Usage: {} [options]\n", command);
    fmt::print("Options:\n");
    fmt::print("-db: Database name\n");
    fmt::print("-c: Database configuration file path\n");
    fmt::print("-w: Workload file path\n");
}

void parse_args(int argc, char* argv[], settings_t& settings) {
    int arg_idx = 1;
    while (arg_idx < argc && start_with(argv[arg_idx], "-")) {
        if (strcmp(argv[arg_idx], "-db") == 0) {
            arg_idx++;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -db\n");
                exit(1);
            }
            settings.db_name = std::string(argv[arg_idx]);
            arg_idx++;
        }
        else if (strcmp(argv[arg_idx], "-c") == 0) {
            arg_idx++;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -c\n");
                exit(1);
            }
            settings.db_config_path = std::string(argv[arg_idx]);
            arg_idx++;
        }
        else if (strcmp(argv[arg_idx], "-w") == 0) {
            arg_idx++;
            if (arg_idx >= argc) {
                usage_message(argv[0]);
                fmt::print("Missing argument value for -w\n");
                exit(1);
            }
            settings.workload_path = std::string(argv[arg_idx]);
            arg_idx++;
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
}

inline void register_section(std::string const& name) {
    bm::RegisterBenchmark(name.c_str(),
                          [=](bm::State& s) {
                              for (auto _ : s)
                                  ;
                          })
        ->Iterations(1)
        ->Unit(bm::kMicrosecond)
        ->UseRealTime();
}

inline void drop_system_caches() {
    auto res = system("sudo sh -c '/usr/bin/echo 3 > /proc/sys/vm/drop_caches'");
    if (res == 0)
        sleep(5);
}

inline std::string section_name(settings_t const& settings, workloads_t const& workloads) {
    return workloads.empty()
               ? settings.db_name
               : fmt::format("{} ({})",
                             settings.db_name,
                             printable_bytes_t {workloads[0].db_records_count * workloads[0].value_length});
}

template <typename func_at>
inline void register_benchmark(std::string const& name, size_t iterations_count, func_at func) {
    bm::RegisterBenchmark(name.c_str(), func)->Iterations(iterations_count)->Unit(bm::kMicrosecond)->UseRealTime();
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

inline operation_chooser_t create_operation_chooser(workload_t const& workload) {
    operation_chooser_t chooer(new ucsb::operation_chooser_t);
    chooer->add(operation_kind_t::insert_k, workload.insert_proportion);
    chooer->add(operation_kind_t::update_k, workload.update_proportion);
    chooer->add(operation_kind_t::read_k, workload.read_proportion);
    chooer->add(operation_kind_t::remove_k, workload.remove_proportion);
    chooer->add(operation_kind_t::batch_read_k, workload.batch_read_proportion);
    chooer->add(operation_kind_t::range_select_k, workload.range_select_proportion);
    chooer->add(operation_kind_t::scan_k, workload.scan_proportion);
    chooer->add(operation_kind_t::read_modify_write_k, workload.read_modify_write_proportion);
    return chooer;
}

void transaction(bm::State& state, workload_t const& workload, db_t& db) {
    // drop_system_caches();

    bool ok = db.open();
    assert(ok);
    auto chooser = create_operation_chooser(workload);
    transaction_t transaction(workload, db);

    size_t fails = 0;
    size_t operations_done = 0;
    size_t bytes_processed_count = 0;
    cpu_stat_t cpu_stat;
    mem_stat_t mem_stat;
    cpu_stat.start();
    mem_stat.start();

    size_t current_iteration = 0;
    size_t last_printed_iteration = 0;
    size_t const printable_iterations_distance = workload.operations_count / 10;
    for (auto _ : state) {
        operation_result_t result;
        auto operation = chooser->choose();
        switch (operation) {
        case operation_kind_t::insert_k: result = transaction.do_insert(); break;
        case operation_kind_t::update_k: result = transaction.do_update(); break;
        case operation_kind_t::read_k: result = transaction.do_read(); break;
        case operation_kind_t::remove_k: result = transaction.do_remove(); break;
        case operation_kind_t::batch_read_k: result = transaction.do_batch_read(); break;
        case operation_kind_t::range_select_k: result = transaction.do_range_select(); break;
        case operation_kind_t::scan_k: result = transaction.do_scan(); break;
        case operation_kind_t::read_modify_write_k: result = transaction.do_read_modify_write(); break;
        default: throw exception_t("Unknown operation"); break;
        }

        operations_done += result.entries_touched;
        bool success = result.status == operation_status_t::ok_k;
        fails += size_t(!success) * result.entries_touched;
        bytes_processed_count += size_t(success) * workload.value_length * result.entries_touched;

        // Print progress
        ++current_iteration;
        if (current_iteration - last_printed_iteration > printable_iterations_distance || current_iteration == 1 ||
            current_iteration == workload.operations_count) {
            float percent = 100.f * current_iteration / workload.operations_count;
            last_printed_iteration = current_iteration;
            fmt::print("{}: {:.2f}%\r", workload.name, percent);
            fflush(stdout);
        }
    }

    cpu_stat.stop();
    mem_stat.stop();
    state.SetBytesProcessed(bytes_processed_count);
    state.counters["fails,%"] = bm::Counter(fails * 100.0 / operations_done);
    state.counters["operations/s"] = bm::Counter(operations_done - fails, bm::Counter::kIsRate);
    state.counters["cpu_max,%"] = bm::Counter(cpu_stat.percent().max);
    state.counters["cpu_avg,%"] = bm::Counter(cpu_stat.percent().avg);
    state.counters["mem_max,bytes"] = bm::Counter(mem_stat.rss().max, bm::Counter::kDefaults, bm::Counter::kIs1024);
    state.counters["mem_avg,bytes"] = bm::Counter(mem_stat.rss().avg, bm::Counter::kDefaults, bm::Counter::kIs1024);
    state.counters["disk,bytes"] = bm::Counter(db.size_on_disk(), bm::Counter::kDefaults, bm::Counter::kIs1024);
    ok = db.close();
    assert(ok);
}

int main(int argc, char** argv) {

    settings_t settings;
    parse_args(argc, argv, settings);
    settings.db_dir_path = fmt::format("./tmp/{}/", settings.db_name);
    settings.results_path =
        fmt::format("./bench/results/{}/{}.json", settings.db_name, settings.workload_path.stem().c_str());
    settings.delete_db_at_the_end = false;

    workloads_t workloads;
    if (!ucsb::load(settings.workload_path, workloads)) {
        fmt::print("Failed to load workloads. path: {}\n", settings.workload_path.c_str());
        return 1;
    }

    ucsb::fs::create_directories(settings.db_dir_path);
    ucsb::fs::create_directories(settings.results_path.parent_path());

    db_kind_t kind = ucsb::parse_db(settings.db_name);
    std::unique_ptr<db_t> db(factory_t {}.create(kind));
    if (!db) {
        fmt::print("Failed to create DB: {}\n", settings.db_name);
        return 1;
    }
    db->set_config(settings.db_config_path, settings.db_dir_path);

    register_section(section_name(settings, workloads));
    for (auto const& workload : workloads) {
        register_benchmark(workload.name, workload.operations_count, [&](bm::State& state) {
            transaction(state, workload, *db.get());
        });
    }

    run_benchmarks(argc, argv, settings);

    if (settings.delete_db_at_the_end) {
        db->destroy();
        ucsb::fs::remove_all(settings.db_dir_path);
    }

    return 0;
}