#pragma once

#include <cassert>

#include "ucsb/core/hash.hpp"
#include "ucsb/core/generators/zipfian_generator.hpp"

namespace ucsb {

struct scrambled_zipfian_generator_t : public generator_gt<size_t> {
  public:
    scrambled_zipfian_generator_t(size_t min, size_t max, double zipfian_const)
        : base_(min), num_items_(max - min + 1), generator_(0, 10000000000LL, zipfian_const) {}
    scrambled_zipfian_generator_t(size_t min, size_t max)
        : base_(min), num_items_(max - min + 1),
          generator_(0, 10000000000LL, zipfian_generator_t::zipfian_const_k, kZetan) {}
    scrambled_zipfian_generator_t(size_t num_items) : scrambled_zipfian_generator_t(0, num_items - 1) {}

    size_t generate() override { return Scramble(generator_.generate()); }
    size_t last() override { return Scramble(generator_.last()); }

  private:
    size_t Scramble(size_t value) const { return base_ + fnv_hash64(value) % num_items_; }

    static constexpr double kZetan = 26.46902820178302;

    const size_t base_;
    const size_t num_items_;
    zipfian_generator_t generator_;
};

} // namespace ucsb