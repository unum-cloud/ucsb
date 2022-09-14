#pragma once
#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>

#include "core/aligned_buffer.hpp"

namespace ucsb
{

    namespace fs = std::filesystem;

    using key_t = size_t;
    using keys_t = std::vector<key_t>;
    using keys_span_t = std::span<key_t>;
    using keys_spanc_t = std::span<key_t const>;
    using value_length_t = uint32_t;
    using value_t = std::vector<std::byte>;
    using values_t = std::vector<value_t>;
    using value_span_t = std::span<std::byte>;
    using value_spanc_t = std::span<std::byte const>;
    using values_buffer_t = aligned_buffer_t;
    using values_span_t = std::span<std::byte>;
    using values_spanc_t = std::span<std::byte const>;
    using value_lengths_t = std::vector<value_length_t>;
    using value_lengths_span_t = std::span<value_length_t>;
    using value_lengths_spanc_t = std::span<value_length_t const>;

} // namespace ucsb