#pragma once

#include <cstddef>

#include "types.hpp"
#include "ucsb/core/distribution.hpp"

namespace ucsb {

struct workload_t {
    size_t records_count = 0;
    size_t operations_count = 0;

    value_length_t value_length = 0;
    distribution_kind_t value_length = distribution_kind_t::const_k;

    size_t batch_size = 0;
};

} // namespace ucsb