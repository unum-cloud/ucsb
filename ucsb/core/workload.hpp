#pragma once
#include <vector>
#include <string>
#include <cstddef>
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

        workload.insert_proportion = (*j_workload).value("insert_proportion", 0.0);
        workload.update_proportion = (*j_workload).value("update_proportion", 0.0);
        workload.read_proportion = (*j_workload).value("read_proportion", 0.0);
        workload.remove_proportion = (*j_workload).value("remove_proportion", 0.0);
        workload.batch_read_proportion = (*j_workload).value("batch_read_proportion", 0.0);
        workload.range_select_proportion = (*j_workload).value("range_select_proportion", 0.0);
        workload.scan_proportion = (*j_workload).value("scan_proportion", 0.0);

        workload.key_dist = parse_distribution((*j_workload).value("key_dist", "uniform"));
        if (workload.key_dist == distribution_kind_t::unknown_k) {
            workloads.clear();
            return false;
        }

        workload.value_length = (*j_workload).value("value_length", 1024);
        workload.value_length_dist = parse_distribution((*j_workload).value("value_length_dist", "const"));
        if (workload.value_length_dist == distribution_kind_t::unknown_k) {
            workloads.clear();
            return false;
        }

        workload.batch_min_length = (*j_workload).value("batch_min_length", 256);
        workload.batch_max_length = (*j_workload).value("batch_max_length", 256);
        workload.batch_length_dist = parse_distribution((*j_workload).value("batch_length_dist", "uniform"));
        if (workload.batch_length_dist == distribution_kind_t::unknown_k) {
            workloads.clear();
            return false;
        }

        workload.range_select_min_length = (*j_workload).value("range_select_min_length", 100);
        workload.range_select_max_length = (*j_workload).value("range_select_max_length", 100);
        workload.range_select_length_dist =
            parse_distribution((*j_workload).value("range_select_length_dist", "uniform"));
        if (workload.key_dist == distribution_kind_t::unknown_k) {
            workloads.clear();
            return false;
        }

        workloads.push_back(workload);
    }

    return true;
}

} // namespace ucsb