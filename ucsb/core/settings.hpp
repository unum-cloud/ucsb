#pragma once

#include <fstream>

#include "ucsb/core/types.hpp"

namespace ucsb {

struct settings_t {
    std::string db_name;
    fs::path db_config_path;
    fs::path db_dir_path;
    fs::path workloads_path;
    std::string workload_filter;
    fs::path results_path;
    size_t threads_count = 1;
};

} // namespace ucsb