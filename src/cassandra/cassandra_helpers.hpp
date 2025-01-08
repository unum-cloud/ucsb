#pragma once

#include <string>
#include <cstdio>
#include <cstdint>
#include <iostream>

#include "src/core/types.hpp"

using value_spanc_t = ucsb::value_spanc_t;


/*
* Do You Really Need __builtin_bswap64 in key_to_string?

Whether you need it depends on why you were swapping the bytes in the first place. Often, code calls __builtin_bswap64 for one of the following reasons:
    1.	Big-Endian Sorting
Some databases or benchmarks want a lexical (string) ordering that matches the numerical ordering of keys. If the platform is little-endian, storing raw bytes will not match numeric order in a straightforward lexicographic sense. By reversing the bytes, you effectively store them in big-endian format, so sorting by string will match sorting by numeric value.
    •	If you truly need that property (key strings sorted the same way as numeric keys), keep the bswap.
    •	If you don’t need that property, you can remove it.
 * */
//inline std::string key_to_string(key_t key) {
//    std::cout << "sizeof(key_t) = " << sizeof(key_t) << std::endl;
//    static_assert(sizeof(key_t) == 8, "key_t must be 64 bits for byte-swapping!");
//
//    // Convert key from little-endian to big-endian to preserve lexical order as numeric order
//    key = __builtin_bswap64(key);
//    // Store as a hex string (16 hex chars)
//    char buffer[17];
//    snprintf(buffer, sizeof(buffer), "%016lx", static_cast<unsigned long>(key));
//    return std::string(buffer);
//}


// Currently using a direct conversion from key to str
inline std::string key_to_string(key_t key) {
//    std::cout << "sizeof(key_t) = " << sizeof(key_t) << std::endl;
    char buffer[17];
    snprintf(buffer, sizeof(buffer), "%016llx", (unsigned long long)key);
    return std::string(buffer);
}

inline std::string value_to_string(value_spanc_t value) {
    // Treat value as text. If values are binary, consider using base64.
    return std::string(reinterpret_cast<const char*>(value.data()), value.size());
}

//inline std::string value_to_string(std::span<const uint8_t> value) {
//    return std::string(reinterpret_cast<const char*>(value.data()), value.size());
//}