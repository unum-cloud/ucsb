#pragma once

namespace ucsb {

enum class distribution_kind_t {
    const_k,
    counter_k,
    uniform_k,
    zipfian_k,
    scrambled_zipfian_k,
    skewed_zipfian_k,
    acknowledged_counter_k,
};

} // namespace ucsb