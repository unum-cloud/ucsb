#pragma once

#include <cstddef>
#include <string>
#include <vector>
#include <fstream>
#include <nlohmann/json.hpp>

#include "types.hpp"
#include "ucsb/core/distribution.hpp"

using json = nlohmann::json;

namespace ucsb {

struct workload_t {
    std::string name;

    size_t records_count = 0;
    size_t operations_count = 0;

    double insert_proportion = 0;
    double update_proportion = 0;
    double read_proportion = 0;
    double remove_proportion = 0;
    double batch_read_proportion = 0;
    double range_select_proportion = 0;
    double scan_proportion = 0;

    distribution_kind_t key_dist = distribution_kind_t::uniform_k;

    value_length_t value_length = 0;
    distribution_kind_t value_length_dist = distribution_kind_t::const_k;

    size_t batch_min_length = 0;
    size_t batch_max_length = 0;
    distribution_kind_t batch_length_dist = distribution_kind_t::uniform_k;

    size_t range_select_min_length = 0;
    size_t range_select_max_length = 0;
    distribution_kind_t range_select_length_dist = distribution_kind_t::uniform_k;
};

using workloads_t = std::vector<workload_t>;

distribution_kind_t parse_distribution(std::string const& name) {
    distribution_kind_t dist = distribution_kind_t::unknown_k;
    if (name == "const")
        dist = distribution_kind_t::const_k;
    else if (name == "counter")
        dist = distribution_kind_t::counter_k;
    else if (name == "uniform")
        dist = distribution_kind_t::uniform_k;
    else if (name == "zipfian")
        dist = distribution_kind_t::zipfian_k;
    else if (name == "scrambled")
        dist = distribution_kind_t::scrambled_zipfian_k;
    else if (name == "skewed")
        dist = distribution_kind_t::scrambled_zipfian_k;
    else if (name == "acknowledged")
        dist = distribution_kind_t::acknowledged_counter_k;
    return dist;
}

bool load(fs::path const& path, workloads_t& workloads) {

    workloads.clear();
    if (!fs::exists(path))
        return false;

    std::ifstream ifstream(path);
    json j_workloads;
    ifstream >> j_workloads;

    for (auto j_workload = j_workloads.begin(); j_workload != j_workloads.end(); ++j_workload) {
        workload_t workload;

        workload.name = (*j_workload)["name"].get<std::string>();

        workload.records_count = (*j_workload)["records_count"].get<size_t>();
        workload.operations_count = (*j_workload)["operations_count"].get<size_t>();

        workload.insert_proportion = (*j_workload)["insert_proportion"].get<double>();
        workload.update_proportion = (*j_workload)["update_proportion"].get<double>();
        workload.read_proportion = (*j_workload)["read_proportion"].get<double>();
        workload.remove_proportion = (*j_workload)["remove_proportion"].get<double>();
        workload.batch_read_proportion = (*j_workload)["batch_read_proportion"].get<double>();
        workload.range_select_proportion = (*j_workload)["range_select_proportion"].get<double>();
        workload.scan_proportion = (*j_workload)["scan_proportion"].get<double>();

        workload.key_dist = parse_distribution((*j_workload)["key_dist"].get<std::string>());

        workload.value_length = (*j_workload)["value_length"].get<size_t>();
        workload.value_length_dist = parse_distribution((*j_workload)["value_length_dist"].get<std::string>());

        workload.batch_length_dist = parse_distribution((*j_workload)["batch_length_dist"].get<std::string>());
        workload.batch_min_length = (*j_workload)["batch_min_length"].get<size_t>();
        workload.batch_max_length = (*j_workload)["batch_max_length"].get<size_t>();

        workload.range_select_min_length = (*j_workload)["range_select_min_length"].get<size_t>();
        workload.range_select_max_length = (*j_workload)["range_select_max_length"].get<size_t>();
        workload.range_select_length_dist =
            parse_distribution((*j_workload)["range_select_length_dist"].get<std::string>());
    }

    return true;
}

} // namespace ucsb