#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>
#include <filesystem>

namespace ucsb {

namespace fs = std::filesystem;

using key_t = size_t;
using keys_t = std::vector<key_t>;
using keys_span_t = std::span<key_t>;
using value_length_t = size_t;
using value_t = std::vector<std::byte>;
using value_span_t = std::span<std::byte>;

} // namespace ucsb