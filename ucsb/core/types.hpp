#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <filesystem>

namespace ucsb {

namespace fs = std::filesystem;

using key_t = size_t;
using value_t = std::span<std::byte>;
using value_length_t = uint32_t;
using keys_t = std::span<key_t>;

} // namespace ucsb