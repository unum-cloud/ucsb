#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/color.h>
#include <benchmark/benchmark.h>

#include <nlohmann/json.hpp>
#include <argparse/argparse.hpp>

#include "src/core/timer.hpp"
#include "src/core/types.hpp"
#include "src/core/settings.hpp"
#include "src/core/profiler.hpp"
#include "src/core/db.hpp"
#include "src/core/workload.hpp"
#include "src/core/worker.hpp"
#include "src/core/db_brand.hpp"
#include "src/core/db_hint.hpp"
#include "src/core/distribution.hpp"
#include "src/core/operation.hpp"
#include "src/core/exception.hpp"
#include "src/core/printable.hpp"
#include "src/core/reporter.hpp"
#include "src/core/threads_fence.hpp"

namespace bm = benchmark;
using namespace ucsb;

using operation_chooser_ptr_t = std::unique_ptr<operation_chooser_t>;

void parse_and_validate_args(int argc, char* argv[], settings_t& settings) {

    argparse::ArgumentParser program(argv[0]);
    program.add_argument("-db", "--db-name").required().help("Database name");
    program.add_argument("-t", "--transaction").default_value(false).implicit_value(true).help("Transactional");
    program.add_argument("-cfg", "--config-path").required().help("Database configuration file path");
    program.add_argument("-wl", "--workload-path").required().help("Workloads file path");
    program.add_argument("-res", "--results-path").required().help("Results file path");
    program.add_argument("-md", "--main-dir").required().help("Database main directory path");
    program.add_argument("-sd", "--storage-dirs")
        .default_value(std::string(""))
        .help("Database storage directory paths");
    program.add_argument("-th", "--threads").default_value(std::string("1")).help("Threads count");
    program.add_argument("-fl", "--filter").default_value(std::string("")).help("Workloads filter");
    program.add_argument("-ri", "--run-index").default_value(std::string("0")).help("Run index in sequence");
    program.add_argument("-rc", "--runs-count").default_value(std::string("1")).help("Total runs count");

    program.parse_known_args(argc, argv);

    settings.db_name = program.get("db-name");
    settings.transactional = program.get<bool>("transaction");
    settings.db_config_file_path = program.get("config-path");
    settings.workloads_file_path = program.get("workload-path");
    settings.results_file_path = program.get("results-path");
    settings.threads_count = std::stoi(program.get("threads"));
    settings.workload_filter = program.get("filter");
    settings.run_idx = std::stoi(program.get("run-index"));
    settings.runs_count = std::stoi(program.get("runs-count"));

    // Resolve paths
    auto path = program.get("main-dir");
    if (!path.empty() && path.back() != '/')
        path.push_back('/');
    settings.db_main_dir_path = path;
    //
    settings.db_storage_dir_paths.clear();
    std::string str_dir_paths = program.get("storage-dirs");
    auto dir_paths = split(str_dir_paths, ',');
    for (auto& dir_path : dir_paths) {
        if (!dir_path.empty() && dir_path.back() != '/')
            dir_path.push_back('/');
        settings.db_storage_dir_paths.push_back(dir_path);
    }

    // Check arguments
    if (settings.threads_count == 0) {
        fmt::print("Zero threads count specified\n");
        exit(1);
    }
    if (settings.runs_count == 0) {
        fmt::print("Zero total runs count specified\n");
        exit(1);
    }
    if (settings.run_idx >= settings.runs_count) {
        fmt::print("Invalid run index specified\n");
        exit(1);
    }
}

std::string build_title(settings_t const& settings, workloads_t const& workloads, std::string const& db_info) {

    std::vector<std::string> infos;
    std::string db_details;
    if (settings.transactional)
        db_details.append("transactional");
    if (!db_info.empty())
        db_details.append(db_details.empty() ? db_info : fmt::format(", {}", db_info));
    if (!db_details.empty())
        infos.push_back(fmt::format("Database: {} ({})", settings.db_name, db_details));
    else
        infos.push_back(fmt::format("Database: {}", settings.db_name));

    if (!workloads.empty()) {
        size_t db_size = workloads.front().db_records_count * workloads.front().value_length;
        infos.push_back(fmt::format("Workload size: {}", printable_bytes_t {db_size}));
    }

    infos.push_back(fmt::format("Threads: {}", settings.threads_count));
    infos.push_back(fmt::format("Disks: {}", std::max(size_t(1), settings.db_storage_dir_paths.size())));

    return fmt::format("{}", fmt::join(infos, " | "));
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

void run(int argc, char* argv[], std::string const& title, size_t idx, size_t count, std::string const& results_path) {
    (void)argc;

    int bm_argc = 4;
    char* bm_argv[4];
    std::string arg0(argv[0]);
    bm_argv[0] = const_cast<char*>(arg0.c_str());

    std::string arg1("--benchmark_format=console");
    bm_argv[1] = const_cast<char*>(arg1.c_str());

    std::string arg2(fmt::format("--benchmark_out={}", results_path.c_str()));
    bm_argv[2] = const_cast<char*>(arg2.c_str());

    std::string arg3("--benchmark_out_format=json");
    bm_argv[3] = const_cast<char*>(arg3.c_str());

    bm::Initialize(&bm_argc, bm_argv);
    if (bm::ReportUnrecognizedArguments(bm_argc, bm_argv)) {
        fmt::print("GoogleBM: Invalid Input Arguments\n");
        return;
    }

    // Prepare reporter and run benchmarks
    console_reporter_t::sections_t sections = console_reporter_t::all_k;
    if (count > 1) {
        if (idx == 0)
            sections = console_reporter_t::sections_t(console_reporter_t::header_k | console_reporter_t::result_k);
        else if (idx == count - 1)
            sections = console_reporter_t::sections_t(console_reporter_t::logo_k | console_reporter_t::result_k);
        else if (idx < count)
            sections = console_reporter_t::sections_t(console_reporter_t::result_k);
    }
    console_reporter_t console(title, sections);
    bm::RunSpecifiedBenchmarks(&console);
}

void validate_workload(workload_t const& workload, [[maybe_unused]] size_t threads_count) {

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
           (workload.batch_upsert_proportion > 0.0 && workload.batch_upsert_min_length > 0));
    assert(workload.batch_upsert_min_length <= workload.batch_upsert_max_length);
    assert(workload.batch_upsert_max_length <= workload.db_records_count / threads_count);

    assert(workload.batch_read_proportion == 0.0 ||
           (workload.batch_read_proportion > 0.0 && workload.batch_read_min_length > 0));
    assert(workload.batch_read_min_length <= workload.batch_read_max_length);
    assert(workload.batch_read_max_length <= workload.db_records_count / threads_count);

    assert(workload.bulk_load_proportion == 0.0 ||
           (workload.bulk_load_proportion > 0.0 && workload.bulk_load_min_length > 0));
    assert(workload.bulk_load_min_length <= workload.bulk_load_max_length);
    assert(workload.bulk_load_max_length <= workload.db_records_count / threads_count);

    assert(workload.range_select_proportion == 0.0 ||
           (workload.range_select_proportion > 0.0 && workload.range_select_min_length > 0));
    assert(workload.range_select_min_length <= workload.range_select_max_length);
    assert(workload.range_select_max_length <= workload.db_records_count / threads_count);
}

workloads_t filter_workloads(workloads_t const& workloads, std::string const& filter) {
    if (filter.empty())
        return workloads;

    // Note: It keeps order as mentioned in filter
    workloads_t filtered_workloads;
    std::vector<std::string> tokens = split(filter, ',');
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

    auto start_key = workload.start_key;
    for (size_t idx = 0; idx < threads_count; ++idx) {
        workload_t thread_workload = workload;
        thread_workload.records_count = records_count_per_thread + bool(leftover_records_count);
        thread_workload.operations_count = operations_count_per_thread + bool(leftover_operations_count);
        thread_workload.operations_count = std::max(size_t(1), thread_workload.operations_count);
        thread_workload.start_key = start_key;
        workloads.push_back(thread_workload);

        leftover_records_count -= bool(leftover_records_count);
        leftover_operations_count -= bool(leftover_operations_count);

        if (workload.upsert_proportion == 1.0 || workload.batch_upsert_proportion == 1.0 ||
            workload.bulk_load_proportion == 1.0) {
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

db_hints_t make_hints(settings_t const& settings, workloads_t const& workloads) {
    db_hints_t hints {settings.threads_count, 0, 0};
    if (!workloads.empty()) {
        hints.records_count = workloads.front().db_records_count;
        hints.value_length = workloads.front().value_length;
    }
    return hints;
}

operation_chooser_ptr_t create_operation_chooser(workload_t const& workload) {
    operation_chooser_ptr_t chooser = std::make_unique<operation_chooser_t>();
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

struct progress_t {
    size_t entries_touched = 0;
    size_t bytes_processed = 0;

    size_t done_iterations = 0;
    size_t failed_iterations = 0;
    size_t last_printed_iterations = 0;
    size_t total_iterations = 0;

    int64_t prev_ops_per_second = 0.0;

    static void print_db_open() {
        fmt::print("\33[2K\r");
        fmt::print(" [✱] Opening DB...\r");
        fflush(stdout);
    }

    static void print_db_close() {
        fmt::print("\33[2K\r");
        fmt::print(" [✱] Closing DB...\r");
        fflush(stdout);
    }

    static void print_db_flush() {
        fmt::print("\33[2K\r");
        fmt::print(" [✱] Flushing DB...\r");
        fflush(stdout);
    }

    static void clear_last_print() {
        fmt::print("\33[2K\r");
        fflush(stdout);
    }

    void print_start(std::string const& workload_name) {
        fmt::print("\33[2K\r");
        auto name = fmt::format(fmt::fg(fmt::color::light_green), "{}", workload_name);
        fmt::print(" [✱] {}: 0.00%\r", name, 0.0);
        fflush(stdout);
    }

    void print_end() {
        fmt::print("\33[2K\r");
        fmt::print(" [✱] Completed\r");
        fflush(stdout);
    }

    bool is_time_to_print() {
        auto print_iterations_step = std::max(size_t(0.05 * total_iterations), size_t(1));
        auto done_its = atomic_load(done_iterations);
        return done_its - atomic_load(last_printed_iterations) >= print_iterations_step || done_its == total_iterations;
    }

    void print(std::string const& workload_name, elapsed_time_t operations_elapsed_time, elapsed_time_t elapsed_time) {

        auto done_percent = 100.f * done_iterations / total_iterations;
        auto fails_percent = failed_iterations * 100.0 / done_iterations;
        auto ops_per_second = entries_touched / std::chrono::duration<double>(operations_elapsed_time).count();
        auto opps_delta = int64_t(ops_per_second) - prev_ops_per_second;
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed_time).count();
        auto remaining = std::chrono::milliseconds(size_t((elapsed / done_percent) * (100.f - done_percent))).count();

        fmt::print("\33[2K\r");
        auto name = fmt::format(fmt::fg(fmt::color::light_green), "{}", workload_name);
        std::string delta;
        if (opps_delta < 0 && std::abs(opps_delta) > prev_ops_per_second * 0.0001)
            delta = fmt::format(fmt::fg(fmt::color::red), "▼");
        else if (opps_delta > 0 && std::abs(opps_delta) > prev_ops_per_second * 0.0001)
            delta = fmt::format(fmt::fg(fmt::color::green), "▲");
        auto fails = fails_percent == 0.0 ? fmt::format("{}%", fails_percent)
                                          : fmt::format(fmt::fg(fmt::color::red), "{}%", fails_percent);
        fmt::print(" [✱] {}: {:.2f}% [{}/s {}| fails: {} | elapsed: {} | left: {}]\r",
                   name,
                   done_percent,
                   printable_float_t {ops_per_second},
                   delta,
                   fails,
                   printable_duration_t {size_t(elapsed)},
                   printable_duration_t {size_t(remaining)});
        fflush(stdout);

        atomic_store(last_printed_iterations, done_iterations);
        atomic_store(prev_ops_per_second, int64_t(ops_per_second));
    }

    void clear() {
        failed_iterations = 0;
        entries_touched = 0;
        bytes_processed = 0;
        done_iterations = 0;
        last_printed_iterations = 0;
        total_iterations = 0;
        prev_ops_per_second = 0;
    }
};

void bench(bm::State& state, workload_t const& workload, db_t& db, data_accessor_t& data_accessor) {

    // Bench components
    auto chooser = create_operation_chooser(workload);
    ucsb::timer_t timer(state);
    worker_t worker(workload, data_accessor, timer);
    std::atomic_bool do_flash = true;

    // Monitoring
    cpu_profiler_t cpu_prof;    // Only one thread profiles
    mem_profiler_t mem_prof;    // Only one thread profiles
    static progress_t progress; // Shared between threads

    // Bench initialization
    atomic_add_fetch(progress.total_iterations, workload.operations_count);
    if (state.thread_index() == 0) {
        cpu_prof.start();
        mem_prof.start();
        progress.print_start(workload.name);
    }

    // Bench
    timer.start();
    while (state.KeepRunningBatch(workload.operations_count)) {
        size_t thread_iterations = workload.operations_count;
        while (thread_iterations) {
            // Do operation
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
            default: throw exception_t("Unknown operation"); break;
            }

            // Update progress
            bool success = result.status == operation_status_t::ok_k;
            auto bytes_processed = size_t(success) * workload.value_length * result.entries_touched;
            atomic_add_fetch(progress.entries_touched, size_t(success) * result.entries_touched);
            atomic_add_fetch(progress.failed_iterations, size_t(!success));
            atomic_add_fetch(progress.bytes_processed, bytes_processed);
            auto done_iterations = atomic_add_fetch(progress.done_iterations, size_t(1));

            if (progress.is_time_to_print())
                progress.print(workload.name, timer.operations_elapsed_time(), timer.elapsed_time());

            // Last thread flushes the DB
            bool only_once = true;
            bool is_last_iteration = done_iterations == progress.total_iterations;
            if (is_last_iteration && do_flash.compare_exchange_weak(only_once, false)) {
                progress_t::print_db_flush();
                db.flush();
            }

            --thread_iterations;
        }
    }
    timer.stop();

    // clang-format off

    // Conclusion
    if (state.thread_index() == 0) {
        progress.print_end();
        cpu_prof.stop();
        mem_prof.stop();

        // Note: This counters are hardcoded and also used in the reporter, so if you do any change here you should also change in the reporter
        state.SetBytesProcessed(progress.bytes_processed);
        state.counters["fails,%"] = bm::Counter(progress.failed_iterations * 100.0 / progress.done_iterations);
        state.counters["operations/s"] = bm::Counter(progress.entries_touched, bm::Counter::kIsRate);
        state.counters["cpu_max,%"] = bm::Counter(cpu_prof.percent().max);
        state.counters["cpu_avg,%"] = bm::Counter(cpu_prof.percent().avg);
        state.counters["mem_max(rss),bytes"] = bm::Counter(mem_prof.rss().max, bm::Counter::kDefaults, bm::Counter::kIs1024);
        state.counters["mem_avg(rss),bytes"] = bm::Counter(mem_prof.rss().avg, bm::Counter::kDefaults, bm::Counter::kIs1024);
        state.counters["mem_max(vm),bytes"] = bm::Counter(mem_prof.vm().max, bm::Counter::kDefaults, bm::Counter::kIs1024);
        state.counters["mem_avg(vm),bytes"] = bm::Counter(mem_prof.vm().avg, bm::Counter::kDefaults, bm::Counter::kIs1024);
        state.counters["processed,bytes"] = bm::Counter(progress.bytes_processed, bm::Counter::kDefaults, bm::Counter::kIs1024);
        state.counters["disk,bytes"] = bm::Counter(db.size_on_disk(), bm::Counter::kDefaults, bm::Counter::kIs1024);

        progress.clear();
    }

    // clang-format on
}

void bench(bm::State& state, workload_t const& workload, db_t& db, bool transactional, threads_fence_t& fence) {

    if (state.thread_index() == 0) {
        progress_t::print_db_open();
        std::string error;
        if (!db.open(error))
            throw exception_t(error);
    }
    fence.sync();

    if (transactional) {
        auto transaction = db.create_transaction();
        if (!transaction)
            throw exception_t("Failed to create DB transaction");
        bench(state, workload, db, *transaction);
    }
    else
        bench(state, workload, db, db);

    fence.sync();
    if (state.thread_index() == 0) {
        progress_t::print_db_close();
        db.close();
        progress_t::clear_last_print();
    }
}

int main(int argc, char** argv) {

    try {
        // Setup settings
        settings_t settings;
        parse_and_validate_args(argc, argv, settings);

        // Resolve results paths
        fs::path final_results_file_path = settings.results_file_path;
        if (final_results_file_path.string().back() == '/')
            final_results_file_path = fmt::format("{}/{}.json",
                                                  final_results_file_path.parent_path().string(),
                                                  settings.workloads_file_path.filename().stem().string());
        fs::path in_progress_results_file_path = fmt::format("{}/{}_in_progress.json",
                                                             final_results_file_path.parent_path().string(),
                                                             final_results_file_path.filename().stem().string());
        // Remove if exists
        if (fs::exists(in_progress_results_file_path))
            fs::remove(in_progress_results_file_path);

        // Create directories
        std::error_code ec;
        if (!fs::exists(settings.db_main_dir_path)) {
            fs::create_directories(settings.db_main_dir_path, ec);
            if (ec) {
                fmt::print("Failed to create DB main directory. path: {}\n", settings.db_main_dir_path.string());
                return 1;
            }
        }
        if (!fs::exists(in_progress_results_file_path.parent_path())) {
            fs::create_directories(in_progress_results_file_path.parent_path(), ec);
            if (ec) {
                fmt::print("Failed to create results directory. path: {}\n",
                           in_progress_results_file_path.parent_path().string());
                return 1;
            }
        }
        for (auto const& dir_path : settings.db_storage_dir_paths) {
            if (!fs::exists(dir_path)) {
                fs::create_directories(dir_path, ec);
                if (ec) {
                    fmt::print("Failed to create DB storage directory. path: {}\n", dir_path.string());
                    return 1;
                }
            }
        }

        // Prepare workloads
        workloads_t workloads;
        if (!load(settings.workloads_file_path, workloads)) {
            fmt::print("Failed to load workloads. path: {}\n", settings.workloads_file_path.string());
            return 1;
        }
        if (workloads.empty()) {
            fmt::print("Workloads file is empty. path: {}\n", settings.workloads_file_path.string());
            return 1;
        }
        workloads = filter_workloads(workloads, settings.workload_filter);
        if (workloads.empty()) {
            fmt::print("Filter doesn't match any workload. filter: {}\n", settings.workload_filter);
            return 1;
        }
        std::vector<workloads_t> threads_workloads;
        for (auto const& workload : workloads) {
            validate_workload(workload, settings.threads_count);
            std::vector<workload_t> splitted_workloads = split_workload_into_threads(workload, settings.threads_count);
            threads_workloads.push_back(splitted_workloads);
        }

        // Setup DB
        db_brand_t db_brand = parse_db_brand(settings.db_name);
        std::shared_ptr<db_t> db = make_db(db_brand, settings.transactional);
        if (!db) {
            fmt::print("Failed to create DB: {} (probably it's disabled in CMaleLists.txt)\n", settings.db_name);
            return 1;
        }
        auto hints = make_hints(settings, workloads);
        db->set_config(settings.db_config_file_path, settings.db_main_dir_path, settings.db_storage_dir_paths, hints);

        threads_fence_t fence(settings.threads_count);

        // Register benchmarks
        for (auto const& splitted_workloads : threads_workloads) {
            std::string workload_name = splitted_workloads.front().name;
            register_benchmark(workload_name, settings.threads_count, [&](bm::State& state) {
                auto const& workload = splitted_workloads[state.thread_index()];
                bench(state, workload, *db, settings.transactional, fence);
            });
        }

        std::string title = build_title(settings, workloads, db->info());
        run(argc, argv, title, settings.run_idx, settings.runs_count, in_progress_results_file_path);

        file_reporter_t::merge_results(in_progress_results_file_path, final_results_file_path);
        fs::remove(in_progress_results_file_path);
    }
    catch (exception_t const& ex) {
        fmt::print("UCSB exception: {}\n", ex.what());
    }
    catch (std::exception const& ex) {
        fmt::print("std exception: {}\n", ex.what());
    }
    catch (...) {
        fmt::print("Unknown exception was thrown\n");
    }

    return 0;
}