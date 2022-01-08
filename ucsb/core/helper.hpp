#pragma once

#include "ucsb/core/types.hpp"

namespace ucsb {

template <typename at>
inline void add_atomic(at& value, at const& delta) noexcept {
    __atomic_add_fetch(&value, delta, __ATOMIC_RELAXED);
}

inline bool start_with(const char* str, const char* prefix) {
    return strncmp(str, prefix, strlen(prefix)) == 0;
}

inline void drop_system_caches() {
    auto res = system("sudo sh -c '/usr/bin/echo 3 > /proc/sys/vm/drop_caches'");
    if (res == 0)
        sleep(5);
}

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

void remove_dir_contents(fs::path const& dir_path) {
    for (auto const& entry : fs::directory_iterator(dir_path))
        fs::remove_all(entry.path());
}

} // namespace ucsb