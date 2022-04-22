#pragma once
#include <vector>
#include <random>
#include <cassert>
#include <cstddef>

#include "src/core/generators/random_generator.hpp"

namespace ucsb {

enum class operation_kind_t {
    insert_k,
    update_k,
    remove_k,
    read_k,
    read_modify_write_k,
    batch_insert_k,
    batch_read_k,
    bulk_load_k,
    range_select_k,
    scan_k,
};

enum class operation_status_t : int {
    ok_k = 1,
    error_k = -1,
    not_found_k = -2,
    not_implemented_k = -3,
};

struct operation_result_t {
    /**
     * @brief The number of entries touched during the operation.
     *
     * For basic single read/writes will be 1.
     * For batch operations is equal to the size of batch.
     * For global scans is equal to the total number of entries in DB.
     */
    size_t entries_touched = 0;

    operation_status_t status = operation_status_t::ok_k;
};

struct operation_chooser_t {
    inline operation_chooser_t() : generator_(0.0, 1.0), sum_(0) {}

    inline void add(operation_kind_t op, float weight);
    inline operation_kind_t choose();

  private:
    std::vector<std::pair<operation_kind_t, float>> ops_;
    random_double_generator_t generator_;
    float sum_;
};

inline void operation_chooser_t::add(operation_kind_t op, float weight) {
    ops_.push_back(std::make_pair(op, weight));
    sum_ += weight;
}

inline operation_kind_t operation_chooser_t::choose() {
    float chooser = generator_.generate();
    for (auto op = ops_.cbegin(); op != ops_.cend(); ++op) {
        float part = op->second / sum_;
        if (chooser < part)
            return op->first;
        chooser -= part;
    }

    assert(false);
    return operation_kind_t::read_k;
}

} // namespace ucsb