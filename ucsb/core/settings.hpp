#pragma once

#include <fstream>
#include <nlohmann/json.hpp>

#include "ucsb/core/types.hpp"

using json = nlohmann::json;

namespace ucsb {

struct settings_t {
    std::string db_name;
    fs::path db_config_path;
    fs::path db_dir_path;
    fs::path workload_path;
    fs::path result_dir_path;
};

bool load(fs::path const& path, settings_t& settings) {

    settings = settings_t();
    if (!fs::exists(path))
        return false;

    std::ifstream ifstream(path);
    json j_settings;
    ifstream >> j_settings;

    settings.db_name = j_settings["db_name"].get<std::string>();
    settings.db_config_path = fs::path(j_settings["db_config_path"].get<std::string>());
    settings.db_dir_path = fs::path(j_settings["db_dir_path"].get<std::string>());
    settings.workload_path = fs::path(j_settings["workload_path"].get<std::string>());
    settings.result_dir_path = fs::path(j_settings["result_dir_path"].get<std::string>());

    return true;
}

} // namespace ucsb