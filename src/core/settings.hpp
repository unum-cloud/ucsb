#pragma once

#include <fstream>

#include "core/types.hpp"

namespace ucsb
{

    struct settings_t
    {
        std::string db_name;
        std::string workload_filter;

        fs::path db_config_path;
        fs::path working_dir_path;
        fs::path workloads_path;
        fs::path results_path;

        size_t threads_count = 1;
        bool transactional = false;
    };

} // namespace ucsb