#pragma once

#include "ucsb/core/types.hpp"

namespace ucsb {

template <typename value_at>
struct generator_gt {
    using value_t = value_at;

    virtual ~generator_gt() {}

    virtual value_t generate() = 0;
    virtual value_t last() = 0;
};

} // namespace ucsb