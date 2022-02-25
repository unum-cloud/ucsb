#pragma once

#include <string>
#include <vector>

#include "src/core/types.hpp"

namespace ucsb
{

    template <typename at>
    inline void add_atomic(at &value, at delta) noexcept
    {
        __atomic_add_fetch(&value, delta, __ATOMIC_RELAXED);
    }

    inline bool start_with(const char *str, const char *prefix)
    {
        return strncmp(str, prefix, strlen(prefix)) == 0;
    }

    std::vector<std::string> split(std::string const &str, char delimiter)
    {
        size_t start = 0;
        size_t end = 0;
        std::vector<std::string> tokens;

        while ((start = str.find_first_not_of(delimiter, end)) != std::string::npos)
        {
            end = str.find(delimiter, start);
            tokens.push_back(str.substr(start, end - start));
        }

        return tokens;
    }

    size_t size_on_disk(fs::path const &path)
    {
        size_t total_size = 0;
        for (auto const &entry : fs::directory_iterator(path))
        {
            if (entry.is_directory())
                total_size += size_on_disk(entry.path());
            else
                total_size += fs::file_size(entry.path());
        }
        return total_size;
    }

    void clear_directory(fs::path const &dir_path)
    {
        for (auto const &entry : fs::directory_iterator(dir_path))
            fs::remove_all(entry.path());
    }

} // namespace ucsb