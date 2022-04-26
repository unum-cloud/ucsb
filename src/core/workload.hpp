#pragma once
#include <vector>
#include <string>
#include <cstddef>
#include <fstream>
#include <nlohmann/json.hpp>

#include "src/core/types.hpp"
#include "src/core/distribution.hpp"

using json = nlohmann::json;

namespace ucsb {

/**
 * @brief A description of a single benchmark.
 * It's post-processed version will devide the task
 * by the number of threads.
 */
struct workload_t {
    std::string name;

    /**
     * @brief Qualitative reference number of entries in the DB.
     * Doesn't change during the benchmark.
     * Defines the number of entries after initialization,
     * but it's outdated after insertions and deletions.
     */
    size_t db_records_count = 0;
    /**
     * @brief Number of entries to be changed/inserted/deleted
     * for this specific workload, divided by the number of threads.
     */
    size_t records_count = 0;
    /**
     * @brief Number of operations which will be done by all threads
     * Loads from workload file, doesn't change during the benchmark.
     */
    size_t db_operations_count = 0;
    /**
     * @brief Number of operations for this specific workload,
     * which will be done by a single thread, divided by the number of threads.
     */
    size_t operations_count = 0;

    float upsert_proportion = 0;
    float update_proportion = 0;
    float remove_proportion = 0;
    float read_proportion = 0;
    float read_modify_write_proportion = 0;
    float batch_upsert_proportion = 0;
    float batch_read_proportion = 0;
    float bulk_load_proportion = 0;
    float range_select_proportion = 0;
    float scan_proportion = 0;

    key_t start_key = 0;
    distribution_kind_t key_dist = distribution_kind_t::uniform_k;

    value_length_t value_length = 0;
    distribution_kind_t value_length_dist = distribution_kind_t::const_k;

    size_t batch_upsert_min_length = 0;
    size_t batch_upsert_max_length = 0;
    distribution_kind_t batch_upsert_length_dist = distribution_kind_t::uniform_k;

    size_t batch_read_min_length = 0;
    size_t batch_read_max_length = 0;
    distribution_kind_t batch_read_length_dist = distribution_kind_t::uniform_k;

    size_t bulk_load_min_length = 0;
    size_t bulk_load_max_length = 0;
    distribution_kind_t bulk_load_length_dist = distribution_kind_t::uniform_k;

    size_t range_select_min_length = 0;
    size_t range_select_max_length = 0;
    distribution_kind_t range_select_length_dist = distribution_kind_t::uniform_k;
};

using workloads_t = std::vector<workload_t>;

inline distribution_kind_t parse_distribution(std::string const& name) {
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
    else if (name == "latest")
        dist = distribution_kind_t::skewed_latest_k;
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

        workload.db_records_count = (*j_workload)["records_count"].get<size_t>();
        workload.db_operations_count = (*j_workload)["operations_count"].get<size_t>();

        workload.upsert_proportion = (*j_workload).value("upsert_proportion", 0.0);
        workload.update_proportion = (*j_workload).value("update_proportion", 0.0);
        workload.remove_proportion = (*j_workload).value("remove_proportion", 0.0);
        workload.read_proportion = (*j_workload).value("read_proportion", 0.0);
        workload.read_modify_write_proportion = (*j_workload).value("read_modify_write_proportion", 0.0);
        workload.batch_upsert_proportion = (*j_workload).value("batch_upsert_proportion", 0.0);
        workload.batch_read_proportion = (*j_workload).value("batch_read_proportion", 0.0);
        workload.bulk_load_proportion = (*j_workload).value("bulk_load_proportion", 0.0);
        workload.range_select_proportion = (*j_workload).value("range_select_proportion", 0.0);
        workload.scan_proportion = (*j_workload).value("scan_proportion", 0.0);

        workload.start_key = (*j_workload).value("start_key", 0);
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

        workload.batch_upsert_min_length = (*j_workload).value("batch_upsert_min_length", 256);
        workload.batch_upsert_max_length = (*j_workload).value("batch_upsert_max_length", 256);
        workload.batch_upsert_length_dist =
            parse_distribution((*j_workload).value("batch_upsert_length_dist", "uniform"));
        if (workload.batch_upsert_length_dist == distribution_kind_t::unknown_k) {
            workloads.clear();
            return false;
        }

        workload.batch_read_min_length = (*j_workload).value("batch_read_min_length", 256);
        workload.batch_read_max_length = (*j_workload).value("batch_read_max_length", 256);
        workload.batch_read_length_dist = parse_distribution((*j_workload).value("batch_read_length_dist", "uniform"));
        if (workload.batch_read_length_dist == distribution_kind_t::unknown_k) {
            workloads.clear();
            return false;
        }

        workload.bulk_load_min_length = (*j_workload).value("bulk_load_min_length", 256);
        workload.bulk_load_max_length = (*j_workload).value("bulk_load_max_length", 256);
        workload.bulk_load_length_dist = parse_distribution((*j_workload).value("bulk_load_length_dist", "uniform"));
        if (workload.bulk_load_length_dist == distribution_kind_t::unknown_k) {
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