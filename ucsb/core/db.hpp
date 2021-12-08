#pragma once

#include "ucsb/core/types.hpp"
#include "ucsb/core/operation.hpp"

namespace ucsb {

struct db_t {
    virtual ~db_t() = 0;

    virtual bool init(fs::path const& dir_path, fs::path const& config_file_path) = 0;
    virtual void destroy() = 0;

    virtual operation_status_t insert(key_t key, value_t value) = 0;
    virtual operation_status_t update(key_t key, value_t value) = 0;
    virtual operation_status_t read(key_t key) = 0;
    virtual operation_status_t delete (key_t key) = 0;
    virtual operation_status_t batch_read(keys_t keys) = 0;
    virtual operation_status_t range_select(key_t key, size_t depth) = 0;
    virtual operation_status_t scan() = 0;
};

} // namespace ucsb