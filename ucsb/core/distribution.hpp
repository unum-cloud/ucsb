#pragma once

namespace ucsb {

enum class distribution_kind_t {
    unknown_k,

    const_k,
    counter_k,
    uniform_k,
    zipfian_k,
    scrambled_zipfian_k,
    skewed_latest_k,
    acknowledged_counter_k,
};

} // namespace ucsb