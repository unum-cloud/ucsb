#pragma once
#include <random>

#include "ucsb/core/generators/generator.hpp"

namespace ucsb {

struct counter_generator_t : public generator_gt<size_t> {
    inline counter_generator_t(size_t start) : counter_(start) {}

    size_t generate() override { return counter_++; }
    size_t last() override { return counter_ - 1; }

  protected:
    size_t counter_;
};

} // namespace ucsb