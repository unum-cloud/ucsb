#pragma once
#include <random>

#include "core/generators/generator.hpp"

namespace ucsb
{

  class counter_generator_t : public generator_gt<size_t>
  {
  public:
    inline counter_generator_t(size_t start) : counter_(start) {}

    inline size_t generate() override { return counter_++; }
    inline size_t last() override { return counter_ - 1; }

  protected:
    size_t counter_;
  };

} // namespace ucsb