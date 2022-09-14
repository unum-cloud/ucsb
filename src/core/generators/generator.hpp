#pragma once

#include "core/types.hpp"

namespace ucsb
{

  template <typename value_at>
  class generator_gt
  {
  public:
    using value_t = value_at;

    virtual ~generator_gt() {}

    virtual value_t generate() = 0;
    virtual value_t last() = 0;
  };

} // namespace ucsb