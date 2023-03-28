#pragma once

#include <fmt/format.h>

namespace ucsb {

/**
 * @brief Formatting sugar for "fmt" library.
 */
struct printable_byte_t {
    size_t bytes = 0;
};

struct printable_float_t {
    double value = 0;
};

struct printable_duration_t {
    size_t duration = 0; // In milliseconds
};

} // namespace ucsb

template <>
class fmt::formatter<ucsb::printable_byte_t> {
  public:
    size_t suffix_idx = 0;
    unsigned char precision = 2;

    template <typename ctx_at>
    inline constexpr auto parse(ctx_at& ctx) {
        auto it = ctx.begin();
        if (it == ctx.end())
            return it;

        if (*it == '.') {
            ++it;
            if (it != ctx.end() && *it >= '0' && *it <= '9')
                precision = (int)(*it) - (int)48;
            else
                throw format_error("Invalid precision");
            ++it;
        }

        if (it == ctx.end())
            return it;

        switch (*it) {
        case 'B': suffix_idx = 1; break;
        case 'K': suffix_idx = 2; break;
        case 'M': suffix_idx = 3; break;
        case 'G': suffix_idx = 4; break;
        case 'T': suffix_idx = 5; break;
        default: throw format_error("Invalid unit");
        }
        ++it;

        if (it != ctx.end() && *it == 'B')
            ++it;
        if (it != ctx.end() && *it != '}')
            throw format_error("Invalid format");

        return it;
    }

    template <typename ctx_at>
    inline auto format(ucsb::printable_byte_t const& v, ctx_at& ctx) {

        char const* suffix_k[] = {"", "B", "KB", "MB", "GB", "TB"};

        size_t bytes = v.bytes;
        float float_bytes = bytes;
        char const length = sizeof(suffix_k) / sizeof(suffix_k[0]);
        if (suffix_idx == 0) {
            ++suffix_idx;
            while (bytes > 1024 && suffix_idx < length - 1) {
                ++suffix_idx;
                float_bytes = bytes / 1024.0;
                bytes /= 1024;
            }
        }
        else
            float_bytes /= std::pow(1024, suffix_idx - 1);

        if (suffix_idx == 1)
            return fmt::format_to(ctx.out(), "{}B", bytes);
        else {
            std::string format = fmt::format("{{:.{}f}}{{}}", precision);
            return fmt::format_to(ctx.out(), fmt::runtime(format), float_bytes, suffix_k[suffix_idx]);
        }
    }
};

template <>
class fmt::formatter<ucsb::printable_float_t> {
  public:
    size_t suffix_idx = 0;
    unsigned char precision = 2;

    template <typename ctx_at>
    constexpr auto parse(ctx_at& ctx) {
        auto it = ctx.begin();
        if (it == ctx.end())
            return it;

        if (*it == '.') {
            ++it;
            if (it != ctx.end() && *it >= '0' && *it <= '9')
                precision = (int)(*it) - (int)48;
            else
                throw format_error("Invalid precision");
            ++it;
        }

        if (it == ctx.end())
            return it;

        switch (*it) {
        case 'k': suffix_idx = 1; break;
        case 'M': suffix_idx = 2; break;
        case 'B': suffix_idx = 3; break;
        case 'T': suffix_idx = 4; break;
        default: throw format_error("Invalid unit");
        }
        it++;

        if (it != ctx.end() && *it != '}')
            throw format_error("Invalid format");

        return it;
    }

    template <typename ctx_at>
    auto format(ucsb::printable_float_t const& v, ctx_at& ctx) {

        char const* suffix_k[] = {"", "k", "M", "B", "T"};

        double value = v.value;
        char const length = sizeof(suffix_k) / sizeof(suffix_k[0]);
        if (suffix_idx == 0) {
            while (value > 1'000.0 && suffix_idx < length - 1) {
                ++suffix_idx;
                value /= 1'000.0;
            }
        }
        else
            value /= std::pow(1'000, suffix_idx);

        std::string format;
        if (suffix_idx == 0)
            format = fmt::format("{{:.{}f}}", precision);
        else
            format = fmt::format("{{:.{}f}}{{}}", precision);

        return fmt::format_to(ctx.out(), fmt::runtime(format), value, suffix_k[suffix_idx]);
    }
};

template <>
class fmt::formatter<ucsb::printable_duration_t> {
  public:
    template <typename ctx_at>
    constexpr auto parse(ctx_at& ctx) {
        return ctx.begin();
    }

    template <typename ctx_at>
    auto format(ucsb::printable_duration_t const& v, ctx_at& ctx) {

        // Resolve human readable duration
        double duration = v.duration;
        size_t major = 0;
        size_t minor = 0;
        std::string major_unit;
        std::string minor_unit;
        auto resolve = [&](double& duration, size_t period, std::string const& maj_unit, std::string const& min_unit) {
            if (duration > period) {
                duration /= period;
                return false;
            }
            major = duration;
            minor = (duration - major) * period;
            major_unit = maj_unit;
            minor_unit = min_unit;
            return true;
        };
        //
        do {
            if (resolve(duration, 1'000, "ms", ""))
                break;
            if (resolve(duration, 60, "s", "ms"))
                break;
            if (resolve(duration, 60, "m", "s"))
                break;
            if (resolve(duration, 24, "h", "m"))
                break;
            resolve(duration, std::numeric_limits<size_t>::max(), "d", "h");
        } while (false);

        // Format
        std::string str_duration = fmt::format("{}{}", major, major_unit);
        if (!minor_unit.empty())
            str_duration = fmt::format("{} {}{}", str_duration, minor, minor_unit);

        return fmt::format_to(ctx.out(), "{}", str_duration);
    }
};