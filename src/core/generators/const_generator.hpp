#pragma once

#include <random>

#include "src/core/generators/generator.hpp"

namespace ucsb::core {

template <typename value_at>
class const_generator_gt : public generator_gt<value_at> {
  public:
    using value_t = value_at;

    inline const_generator_gt(value_t constant) : constant_(constant) {}

    inline value_t generate() override { return constant_; }
    inline value_t last() override { return constant_; }

  private:
    value_t constant_;
};

} // namespace ucsb::core