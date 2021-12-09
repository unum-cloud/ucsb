#include <fmt/format.h>

#include "ucsb/benchmark.hpp"

int main(int argc, char** argv) {

    std::string settings_path = parse_settings_path(argc, argv);
    if (settings_path.emtpy())
        fmt::print("Settings path not specified\n");
    settings_t settings;
    if (load(settings_path, settings))
        fmt::print("Failed to load settings: {}\n", settings_path);

    fs::create_directories(settings.db_dir_path);
    fs::create_directories(settings.result_dir_path);

    workloads_t workloads;
    if (load(settings.workload_path, workloads))
        fmt::print("Failed to load workloads. path: {}\n", settings.workload_path);

    db_kind_t kind = parse_db(settings.db_name);
    db_t* db = factory_t {}.create(kind);
    if (db == nullptr)
        fmt::print("Failed to create db: {}\n", settings.db_name);

    for (auto workload : workloads) {
        register_benchmark(name, workload.operations_count, [&](bm::State& state) {
            transaction(state, workload, db);
        });
    }

    bm::Initialize(&argc, argv);
    bm::RunSpecifiedBenchmarks();

    return 0;
}