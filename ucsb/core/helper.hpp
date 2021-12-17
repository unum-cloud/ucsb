#pragma once

#include "ucsb/core/types.hpp"

namespace ucsb {

size_t size(fs::path const& path) {
    size_t total_size = 0;
    for (const auto& entry : fs::directory_iterator(path)) {
        if (entry.is_directory())
            total_size += size(entry.path());
        else
            total_size += fs::file_size(entry.path());
    }
    return total_size;
}

} // namespace ucsb