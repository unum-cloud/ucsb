#pragma once
#include <cstddef>

const size_t fnv_offset_basis64 = 0xCBF29CE484222325ull;
const size_t fnv_prime64 = 1099511628211ull;

namespace ucsb {

inline size_t fnv_hash64(size_t val) {
    size_t hash = fnv_offset_basis64;
    for (int i = 0; i < 8; i++) {
        size_t octet = val & 0x00ff;
        val = val >> 8;

        hash = hash ^ octet;
        hash = hash * fnv_prime64;
    }
    return hash;
}

} // namespace ucsb