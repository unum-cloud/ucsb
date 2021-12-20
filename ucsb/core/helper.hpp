#pragma once

#include "ucsb/core/types.hpp"

namespace ucsb {

size_t size_on_disk(fs::path const& path) {
    size_t total_size = 0;
    for (auto const& entry : fs::directory_iterator(path)) {
        if (entry.is_directory())
            total_size += size_on_disk(entry.path());
        else
            total_size += fs::file_size(entry.path());
    }
    return total_size;
}

} // namespace ucsb