#pragma once

#include "ucsb/core/types.hpp"
#include "ucsb/core/operation.hpp"

namespace ucsb {

struct db_t {
    virtual ~db_t() = 0;

    virtual bool init(fs::path const& config_path, fs::path const& dir_path) = 0;
    virtual void destroy() = 0;

    virtual operation_result_t insert(key_t key, value_span_t value) = 0;
    virtual operation_result_t update(key_t key, value_span_t value) = 0;
    virtual operation_result_t read(key_t key, value_span_t value) const = 0;
    virtual operation_result_t delete (key_t key) = 0;
    virtual operation_result_t batch_read(keys_span_t keys, value_span_t single_value) const = 0;
    virtual operation_result_t range_select(key_t key, size_t length, value_span_t single_value) const = 0;
    virtual operation_result_t scan(value_span_t single_value) const = 0;
};

} // namespace ucsb