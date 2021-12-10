#include <string>
#include <memory>
#include <fmt/format.h>
#include <benchmark/benchmark.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/settings.hpp"
#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/workload.hpp"
#include "ucsb/core/transaction.hpp"
#include "ucsb/core/factory.hpp"
#include "ucsb/core/operation.hpp"
#include "ucsb/core/exception.hpp"

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
using exception_t = ucsb::exception_t;

int drop_system_caches() {
    return system("sudo sh -c '/usr/bin/echo 3 > /proc/sys/vm/drop_caches'");
}

std::string parse_settings_path(int argc, char* argv[]) {

    std::string path;
    if (argc > 1)
        path = std::string(argv[1]);
    return path;
}

template <typename func_at>
inline void register_benchmark(std::string const& name, size_t iterations_count, func_at func) {
    bm::RegisterBenchmark(name.c_str(), func)->Iterations(iterations_count)->Unit(bm::kMicrosecond)->UseRealTime();
}

void run_benchmarks(int argc, char* argv[], settings_t const& settings) {
    int bm_argc = 4;
    char* bm_argv[4];
    std::string arg0(argv[0]);
    bm_argv[0] = const_cast<char*>(arg0.c_str());

    std::string arg1("--benchmark_format=console");
    bm_argv[1] = const_cast<char*>(arg1.c_str());

    std::string arg2(fmt::format("--benchmark_out={}{}/{}.json",
                                 settings.result_dir_path.c_str(),
                                 settings.db_name.c_str(),
                                 settings.workload_path.stem().c_str()));
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

operation_chooser_t create_operation_chooser(workload_t const& workload) {
    operation_chooser_t chooer(new ucsb::operation_chooser_t);
    chooer->add(operation_kind_t::insert_k, workload.insert_proportion);
    chooer->add(operation_kind_t::update_k, workload.update_proportion);
    chooer->add(operation_kind_t::read_k, workload.read_proportion);
    chooer->add(operation_kind_t::remove_k, workload.remove_proportion);
    chooer->add(operation_kind_t::batch_read_k, workload.batch_read_proportion);
    chooer->add(operation_kind_t::range_select_k, workload.range_select_proportion);
    chooer->add(operation_kind_t::scan_k, workload.scan_proportion);
    return chooer;
}

void transaction(bm::State& state, workload_t const& workload, db_t& db) {

    auto chooser = create_operation_chooser(workload);
    transaction_t transaction(workload, db);
    size_t operations_done = 0;
    size_t failes = 0;

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
        default: throw exception_t("Unknown operation"); break;
        }

        operations_done += result.depth;
        bool ok = result.status == operation_status_t::ok_k || result.status == operation_status_t::not_found_k;
        failes += size_t(!ok) * result.depth;
    }

    state.counters["operations/s"] = bm::Counter(operations_done - failes, bm::Counter::kIsRate);
    state.counters["failes"] = bm::Counter(failes);
}

int main(int argc, char** argv) {

    std::string settings_path = parse_settings_path(argc, argv);
    if (settings_path.empty()) {
        fmt::print("Settings path is not specified\n");
        return 1;
    }
    settings_t settings;
    if (!ucsb::load(settings_path, settings)) {
        fmt::print("Failed to load settings: {}\n", settings_path);
        return 1;
    }

    workloads_t workloads;
    if (!ucsb::load(settings.workload_path, workloads)) {
        fmt::print("Failed to load workloads. path: {}\n", settings.workload_path.c_str());
        return 1;
    }

    ucsb::fs::create_directories(settings.db_dir_path);
    ucsb::fs::create_directories(fmt::format("{}{}", settings.result_dir_path.c_str(), settings.db_name.c_str()));

    db_kind_t kind = ucsb::parse_db(settings.db_name);
    std::unique_ptr<db_t> db(factory_t {}.create(kind));
    if (db == nullptr) {
        fmt::print("Failed to create DB: {}\n", settings.db_name);
        return 1;
    }
    if (!db->init(settings.db_config_path, settings.db_dir_path)) {
        fmt::print("Failed to init DB: {}\n", settings.db_name);
        return 1;
    }

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