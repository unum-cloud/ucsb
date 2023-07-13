#pragma once

#include <fstream>

#include "src/core/types.hpp"

namespace ucsb {

struct settings_t {
    std::string db_name;
    bool transactional = false;
    fs::path db_config_file_path;
    fs::path db_main_dir_path;
    std::vector<fs::path> db_storage_dir_paths;

    fs::path workloads_file_path;
    std::string workload_filter;
    size_t threads_count = 0;

    fs::path results_file_path;
    size_t run_idx = 0;
    size_t runs_count = 0;
};

} // namespace ucsb