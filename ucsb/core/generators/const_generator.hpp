#pragma once
#include <random>

#include "ucsb/core/generators/generator.hpp"

namespace ucsb {

struct const_generator_t : generator_gt<size_t> {
    inline const_generator_t(size_t constant) : constant_(constant) {}

    inline size_t generate() override { return constant_; }
    inline size_t last() override { return constant_; }

  private:
    size_t constant_;
};

} // namespace ucsb