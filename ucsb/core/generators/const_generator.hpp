#pragma once
#include <random>

#include "ucsb/core/generators/generator.hpp"

namespace ucsb {

template <typename value_at>
struct const_generator_gt : generator_gt<value_at> {
    using value_t = value_at;

    inline const_generator_gt(value_t constant) : constant_(constant) {}

    inline value_t generate() override { return constant_; }
    inline value_t last() override { return constant_; }

  private:
    value_t constant_;
};

} // namespace ucsb