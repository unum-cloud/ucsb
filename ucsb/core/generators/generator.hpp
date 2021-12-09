#pragma once

#include "ucsb/core/types.hpp"

namespace ucsb {

template <typename value_at>
struct generator_gt {
    using value_span_t = value_at;

    virtual ~generator_gt() {}

    virtual value_span_t generate() = 0;
    virtual value_span_t last() = 0;
};

} // namespace ucsb