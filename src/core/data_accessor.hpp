#pragma once
#include <set>

#include "src/core/types.hpp"
#include "src/core/operation.hpp"

namespace ucsb {

/**
 * @brief A base class for data accessing: on DBs and Transactions state.
 *
 * @section Keys.
 * The key type is set to a 64-bit unsigned integer.
 * Todays engines often support string keys of variable length,
 * but generally work faster if keys are of identical length.
 * To avoid extra heap allocations and expensive integer formatting,
 * under the hood, we pass a view to raw bytes forming the integer key.
 * For order consistency it's also recommended to provide a custom
 * comparator.
 */
struct data_accessor_t {
    virtual ~data_accessor_t() {}

    virtual operation_result_t insert(key_t key, value_spanc_t value) = 0;
    virtual operation_result_t update(key_t key, value_spanc_t value) = 0;
    virtual operation_result_t remove(key_t key) = 0;
    virtual operation_result_t read(key_t key, value_span_t value) const = 0;

    /**
     * @brief Performs many insert at once in a batch-asynchronous fashion.
     *
     * @param keys Keys are in strict ascending order.
     * @param values Values are written in continuous form.
     * @param sizes Value sizes.
     */
    virtual operation_result_t batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) = 0;

    /**
     * @brief Performs many reads at once in a batch-asynchronous fashion.
     * This means, that the order of lookups within the batch is irrelevant
     * and the engine can reorganize them for faster execution.
     *
     * Every DB engine implements the interface for this operation in a different
     * way, making designing generic interfaces cumbersome and costly (performance-
     * wise). For this benchmark we don't return the retrieved the values and only
     * check them under the hood.
     *
     * @param keys Keys are random.
     * @param values A single buffer for all concatenated values.
     */
    virtual operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const = 0;

    /**
     * @brief Performs many insert at once.
     * In contrast to `batch_insert` this interface used to initialize DB,
     * asume that after every `batch_insert` DB flushes, but in thiscase
     * DB shuld do many `bulk_load`s and periodically flushs
     *
     * @param keys Keys are in strict ascending order.
     * @param values Values are written in continuous form.
     * @param sizes Value sizes.
     */
    virtual operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) = 0;

    /**
     * @brief Performs many reads at once in an ordered fashion,
     * starting from a specified `key` location.
     *
     * @param key The first entry to find and read.
     * @param length The number of consecutive entries to read.
     * @param values A temporary buffer big enough for a all values.
     */
    virtual operation_result_t range_select(key_t key, size_t length, values_span_t values) const = 0;

    /**
     * @brief Performs many reads in an ordered fashion,
     * starting from a specified `key` location.
     *
     * @param key The first entry to find and read.
     * @param length The number of consecutive entries to read.
     * @param values A temporary buffer big enough for a all values.
     */
    virtual operation_result_t scan(key_t key, size_t length, value_span_t single_value) const = 0;
};

} // namespace ucsb