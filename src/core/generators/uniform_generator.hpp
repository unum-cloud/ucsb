#pragma once
#include <random>

#include "src/core/generators/generator.hpp"

namespace ucsb::core::generators {

template <typename value_at>
class uniform_generator_gt : public generator_gt<value_at> {
  public:
    using value_t = value_at;
    static_assert(std::is_integral<value_t>());

    inline uniform_generator_gt(value_t min, value_t max) : dist_(min, max), last_(0) { generate(); }
    inline value_t generate() override { return last_ = dist_(generator_); }
    inline value_t last() override { return last_; }

  private:
    std::mt19937_64 generator_;
    std::uniform_int_distribution<value_t> dist_;
    value_t last_;
};

} // namespace ucsb