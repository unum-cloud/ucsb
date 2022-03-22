#pragma once
#include <fmt/format.h>

namespace ucsb {

/**
 * @brief Formatting sugar for "fmt" library.
 */
struct printable_bytes_t {
    size_t bytes = 0;
};

} // namespace ucsb

template <>
struct fmt::formatter<ucsb::printable_bytes_t> {

    size_t suffix_idx = 0;

    template <typename ctx_at>
    inline constexpr auto parse(ctx_at& ctx) {
        auto it = ctx.begin();
        if (it != ctx.end()) {
            switch (*it) {
            case 'B': suffix_idx = 1; break;
            case 'K': suffix_idx = 2; break;
            case 'M': suffix_idx = 3; break;
            case 'G': suffix_idx = 4; break;
            case 'T': suffix_idx = 5; break;
            case 'P': suffix_idx = 6; break;
            case 'E': suffix_idx = 7; break;
            default: throw format_error("invalid unit");
            }
            ++it;
        }
        if (it != ctx.end() && *it == 'B')
            ++it;
        if (it != ctx.end() && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template <typename ctx_at>
    inline auto format(ucsb::printable_bytes_t const& v, ctx_at& ctx) {

        char const* suffix_k[] = {"", "B", "KB", "MB", "GB", "TB", "PB", "EB"};

        size_t bytes = v.bytes;
        float float_bytes = bytes;
        char const length = sizeof(suffix_k) / sizeof(suffix_k[0]);
        if (suffix_idx == 0) {
            ++suffix_idx;
            for (; (bytes / 1024) > 0 && suffix_idx < length - 1; suffix_idx++, bytes /= 1024)
                float_bytes = bytes / 1024.0;
        }
        else
            float_bytes /= std::pow(1024, suffix_idx - 1);

        if (suffix_idx == 1)
            return fmt::format_to(ctx.out(), "{}B", bytes);
        else
            return fmt::format_to(ctx.out(), "{:02.2f}{}", float_bytes, suffix_k[suffix_idx]);
    }
};
