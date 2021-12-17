#pragma once

#include <fstream>

#include "ucsb/core/types.hpp"

namespace ucsb {

struct settings_t {
    std::string db_name;
    fs::path db_config_path;
    fs::path db_dir_path;
    fs::path workload_path;
    fs::path results_path;
    bool delete_db_at_the_end = false;
};

} // namespace ucsb