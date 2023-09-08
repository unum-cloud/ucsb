#pragma once

#include <stddef.h>

namespace ucsb {

/**
 * @brief hints for a DB
 */
struct db_hints_t {
    size_t threads_count = 0;
    size_t records_count = 0;
    size_t value_length = 0;
};

} // namespace ucsb