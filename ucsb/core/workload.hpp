#pragma once

#include <cstddef>

#include "types.hpp"
#include "ucsb/core/distribution.hpp"

namespace ucsb {

struct workload_t {
    size_t records_count = 0;
    size_t operations_count = 0;

    double insert_proportion = 0;
    double update_proportion = 0;
    double read_proportion = 0;
    double delete_proportion = 0;
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

} // namespace ucsb