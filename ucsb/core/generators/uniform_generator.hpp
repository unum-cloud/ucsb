#pragma once
#include <random>

#include "ucsb/core/generators/generator.hpp"

namespace ucsb {

struct uniform_generator_t : public generator_gt<size_t> {
    inline uniform_generator_t(size_t min, size_t max) : dist_(min, max), last_(0) { generate(); }

    inline size_t generate() override { return last_ = dist_(generator_); }
    inline size_t last() override { return last_; }

  private:
    std::mt19937_64 generator_;
    std::uniform_int_distribution<uint64_t> dist_;
    size_t last_;
};

} // namespace ucsb