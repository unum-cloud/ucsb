#include <fmt/format.h>
#include <benchmark/benchmark.h>
#include <string>

#include "ucsb/core/workload.hpp"
#include "ucsb/core/operation.hpp"
#include "ucsb/core/exception.hpp"

namespace bm = benchmark;
using namespace fs = ucsb::fs;

using workload_t = ucsb::workload_t;
using db_t = ucsb::db_t;
using transaction_t = ucsb::transaction_t;
using operation_kind_t = ucsb::operation_kind_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using operation_chooser_t = std::unique_ptr<ucsb::operation_chooser_t>;
using exception_t = ucsb::exception_t;

constexpr std::string bench_folder_k = "./tmp/";

operation_chooser_t create_operation_chooser(workload_t const& workload) {
    operation_chooser_t chooer(new ucsb::operation_chooser_t);
    chooer->add(operation_kind_t::insert_k, workload.insert_proportion);
    chooer->add(operation_kind_t::update_k, workload.update_proportion);
    chooer->add(operation_kind_t::read_k, workload.read_proportion);
    chooer->add(operation_kind_t::delete_k, workload.delete_proportion);
    chooer->add(operation_kind_t::batch_read_k, workload.batch_read_proportion);
    chooer->add(operation_kind_t::range_select_k, workload.range_select_proportion);
    chooer->add(operation_kind_t::scan_k, workload.scan_proportion);
    return chooer;
}

template <typename func_at>
inline void register_benchmark(string const& name, func_at func, size_t iterations_count, size_t min_time = 0) {
    bm::RegisterBenchmark(name.c_str(), func)
        ->Iterations(iterations_count)
        ->MinTime(min_time)
        ->Unit(bm::kMicrosecond)
        ->UseRealTime();
}

inline void register_section(string const& name) {
    bm::RegisterBenchmark(name.c_str(), [=](bm::State& s) {
        for (auto _ : s)
            ;
    });
}

int drop_system_caches() {
    return system("sudo sh -c '/usr/bin/echo 3 > /proc/sys/vm/drop_caches'");
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
        case operation_kind_t::delete_k: result = transaction.do_delete(); break;
        case operation_kind_t::batch_read_k: result = transaction.do_batch_read(); break;
        case operation_kind_t::range_scan_k: result = transaction.do_range_select(); break;
        case operation_kind_t::scan_k: result = transaction.do_scan(); break;
        default: throw exception_t("Unknown operation"); break;
        }

        operations_done = result.depth;
        failes = size_t(result.status != operation_status_t::success_k) * result.depth;
    }

    state.counters["operations/s"] = bm::Counter(operations_done - failes, bm::Counter::kIsRate);
    state.counters["failes"] = bm::Counter(failes);
}

int main(int argc, char** argv) {
    fs::create_directorie(bench_folder_k.c_str());

    bm::Initialize(&argc, argv);
    bm::RunSpecifiedBenchmarks();

    return 0;
}