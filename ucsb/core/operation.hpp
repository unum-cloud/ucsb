#pragma once

#include <cassert>
#include <cstddef>
#include <vector>
#include <random>

#include "ucsb/core/generators/random_generator.hpp"

namespace ucsb {

enum class operation_kind_t {
    insert_k,
    update_k,
    read_k,
    delete_k,
    batch_read_k,
    range_select_k,
    scan_k,
};

struct operation_status_t {
    size_t depth = 0;
    bool success = false;
};

struct operation_chooser_t {
    inline operation_chooser_t() : sum_(0), generator_(0.0, 1.0) {}

    inline void add(operation_kind_t op, double weight);
    inline operation_kind_t generate();

  private:
    randome_double_generator_t generator_;
    std::vector<std::pair<operation_kind_t, double>> ops_;
    double sum_;
};

inline void operation_chooser_t::add(operation_kind_t op, double weight) {
    ops_.push_back(std::make_pair(op, weight));
    sum_ += weight;
}

inline operation_kind_t operation_chooser_t::generate() {
    double chooser = generator_.generate();
    for (auto op = ops_.cbegin(); op != ops_.cend(); ++op) {
        double part = op->second / sum_;
        if (chooser < part)
            return op->first;
        chooser -= part;
    }

    assert(false);
    return operation_kind_t::read_k;
}

} // namespace ucsb