#pragma once
#include <cassert>

#include "ucsb/core/hash.hpp"
#include "ucsb/core/generators/counter_generator.hpp"
#include "ucsb/core/generators/zipfian_generator.hpp"

namespace ucsb {

struct skewed_latest_generator_t : public generator_gt<size_t> {
  public:
    skewed_latest_generator_t(counter_generator_t& counter) : basis_(&counter), zipfian_(basis_->last()) { generate(); }

    size_t generate() override;
    size_t last() override { return last_; }

  private:
    counter_generator_t* basis_;
    zipfian_generator_t zipfian_;
    size_t last_;
};

inline size_t skewed_latest_generator_t::generate() {

    size_t max = basis_->generate();
    return last_ = max - zipfian_.generate(max);
}

} // namespace ucsb