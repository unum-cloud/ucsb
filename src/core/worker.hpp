#pragma once

#include <vector>
#include <memory>
#include <utility>
#include <set>
#include <fmt/format.h>

#include "src/core/types.hpp"
#include "src/core/data_accessor.hpp"
#include "src/core/workload.hpp"
#include "src/core/timer.hpp"
#include "src/core/helper.hpp"
#include "src/core/generators/generator.hpp"
#include "src/core/generators/const_generator.hpp"
#include "src/core/generators/counter_generator.hpp"
#include "src/core/generators/uniform_generator.hpp"
#include "src/core/generators/zipfian_generator.hpp"
#include "src/core/generators/scrambled_zipfian_generator.hpp"
#include "src/core/generators/skewed_zipfian_generator.hpp"
#include "src/core/generators/acknowledged_counter_generator.hpp"

namespace ucsb {

/**
 * @brief Performs a single workload on a single DB.
 * Responsible for generating the synthetic dataset and
 * managing most of memory allocations outside of the DB.
 */
class worker_t {
  public:
    using key_generator_t = std::unique_ptr<core::generator_gt<key_t>>;
    using acknowledged_key_generator_t = std::unique_ptr<core::acknowledged_counter_generator_t>;
    using value_length_generator_t = std::unique_ptr<core::generator_gt<value_length_t>>;
    using value_generator_t = core::random_byte_generator_t;
    using length_generator_t = std::unique_ptr<core::generator_gt<size_t>>;
    using values_and_sizes_spanc_t = std::pair<values_spanc_t, value_lengths_spanc_t>;

    worker_t(workload_t const& workload, data_accessor_t& data_accessor, timer_t& timer);

    inline operation_result_t do_upsert();
    inline operation_result_t do_update();
    inline operation_result_t do_remove();
    inline operation_result_t do_read();
    inline operation_result_t do_read_modify_write();
    inline operation_result_t do_batch_upsert();
    inline operation_result_t do_batch_read();
    inline operation_result_t do_bulk_load();
    inline operation_result_t do_range_select();
    inline operation_result_t do_scan();

  private:
    inline key_generator_t create_key_generator(workload_t const& workload,
                                                core::counter_generator_t& counter_generator);
    inline value_length_generator_t create_value_length_generator(workload_t const& workload);
    inline length_generator_t create_batch_upsert_length_generator(workload_t const& workload);
    inline length_generator_t create_batch_read_length_generator(workload_t const& workload);
    inline length_generator_t create_bulk_load_length_generator(workload_t const& workload);
    inline length_generator_t create_range_select_length_generator(workload_t const& workload);

    inline key_t generate_key();
    inline keys_spanc_t generate_batch_upsert_keys();
    inline keys_spanc_t generate_batch_read_keys();
    inline keys_spanc_t generate_bulk_load_keys();
    inline value_spanc_t generate_value();
    inline values_and_sizes_spanc_t generate_values(size_t count);
    inline value_span_t value_buffer();
    inline values_span_t values_buffer(size_t count);

    workload_t workload_;
    data_accessor_t* data_accessor_;
    timer_t* timer_;

    key_generator_t upsert_key_sequence_generator;
    acknowledged_key_generator_t acknowledged_key_generator;
    key_generator_t key_generator_;
    keys_t keys_buffer_;

    value_length_generator_t value_length_generator_;
    value_generator_t value_generator_;
    values_buffer_t values_buffer_;
    value_lengths_t value_sizes_buffer_;

    length_generator_t batch_upsert_length_generator_;
    length_generator_t batch_read_length_generator_;
    length_generator_t bulk_load_length_generator_;
    length_generator_t range_select_length_generator_;
};

worker_t::worker_t(workload_t const& workload, data_accessor_t& data_accessor, timer_t& timer)
    : workload_(workload), data_accessor_(&data_accessor), timer_(&timer) {

    if (workload.upsert_proportion == 1.0 || workload.batch_upsert_proportion == 1.0 ||
        workload.bulk_load_proportion == 1.0)
        upsert_key_sequence_generator = std::make_unique<core::counter_generator_t>(workload.start_key);
    else {
        acknowledged_key_generator =
            std::make_unique<core::acknowledged_counter_generator_t>(workload.db_records_count);
        key_generator_ = create_key_generator(workload, *acknowledged_key_generator);
        upsert_key_sequence_generator = std::move(acknowledged_key_generator);
    }
    size_t elements_max_count = std::max({workload.batch_upsert_max_length,
                                          workload.batch_read_max_length,
                                          workload.bulk_load_max_length,
                                          workload.range_select_max_length,
                                          size_t(1)});
    keys_buffer_ = keys_t(elements_max_count);

    value_length_generator_ = create_value_length_generator(workload);
    size_t value_aligned_length = roundup_to_multiple<values_buffer_t::alignment_k>(workload_.value_length);
    values_buffer_ = values_buffer_t(elements_max_count * value_aligned_length);
    value_sizes_buffer_ = value_lengths_t(elements_max_count, 0);

    batch_upsert_length_generator_ = create_batch_upsert_length_generator(workload);
    batch_read_length_generator_ = create_batch_read_length_generator(workload);
    bulk_load_length_generator_ = create_bulk_load_length_generator(workload);
    range_select_length_generator_ = create_range_select_length_generator(workload);
}

inline operation_result_t worker_t::do_upsert() {
    key_t key = upsert_key_sequence_generator->generate();
    value_spanc_t value = generate_value();
    auto status = data_accessor_->upsert(key, value);
    if (acknowledged_key_generator)
        acknowledged_key_generator->acknowledge(key);
    return status;
}

inline operation_result_t worker_t::do_update() {
    key_t key = generate_key();
    value_spanc_t value = generate_value();
    return data_accessor_->update(key, value);
}

inline operation_result_t worker_t::do_remove() {
    key_t key = generate_key();
    return data_accessor_->remove(key);
}

inline operation_result_t worker_t::do_read() {
    key_t key = generate_key();
    value_span_t value = value_buffer();
    return data_accessor_->read(key, value);
}

inline operation_result_t worker_t::do_read_modify_write() {
    key_t key = generate_key();
    value_span_t read_value = value_buffer();
    data_accessor_->read(key, read_value);

    value_spanc_t value = generate_value();
    return data_accessor_->update(key, value);
}

inline operation_result_t worker_t::do_batch_upsert() {
    // Note: Pause benchmark timer to do data preparation, to measure batch upsert time only
    timer_->pause();
    keys_spanc_t keys = generate_batch_upsert_keys();
    values_and_sizes_spanc_t values_and_sizes = generate_values(keys.size());
    timer_->resume();

    return data_accessor_->batch_upsert(keys, values_and_sizes.first, values_and_sizes.second);
}

inline operation_result_t worker_t::do_batch_read() {
    // Note: Pause benchmark timer to do data preparation, to measure batch read time only
    timer_->pause();
    keys_spanc_t keys = generate_batch_read_keys();
    values_span_t values = values_buffer(keys.size());
    timer_->resume();
    return data_accessor_->batch_read(keys, values);
}

inline operation_result_t worker_t::do_bulk_load() {
    // Note: Pause benchmark timer to do data preparation, to measure bulk load time only
    timer_->pause();
    keys_spanc_t keys = generate_bulk_load_keys();
    values_and_sizes_spanc_t values_and_sizes = generate_values(keys.size());
    timer_->resume();

    return data_accessor_->bulk_load(keys, values_and_sizes.first, values_and_sizes.second);
}

inline operation_result_t worker_t::do_range_select() {
    key_t key = generate_key();
    size_t length = range_select_length_generator_->generate();
    values_span_t values = values_buffer(length);
    return data_accessor_->range_select(key, length, values);
}

inline operation_result_t worker_t::do_scan() {
    value_span_t single_value = value_buffer();
    return data_accessor_->scan(workload_.start_key, workload_.records_count, single_value);
}

inline worker_t::key_generator_t worker_t::create_key_generator(workload_t const& workload,
                                                                core::counter_generator_t& counter_generator) {
    key_generator_t generator;
    switch (workload.key_dist) {
    case distribution_kind_t::uniform_k:
        generator =
            std::make_unique<core::uniform_generator_gt<key_t>>(workload.start_key,
                                                                workload.start_key + workload.records_count - 1);
        break;
    case distribution_kind_t::zipfian_k: {
        size_t new_keys = (size_t)(workload.operations_count * workload.upsert_proportion * 2);
        generator = std::make_unique<core::scrambled_zipfian_generator_t>(workload.start_key,
                                                                          workload.start_key + workload.records_count +
                                                                              new_keys - 1);
        break;
    }
    case distribution_kind_t::skewed_latest_k:
        generator = std::make_unique<core::skewed_latest_generator_t>(counter_generator);
        break;
    default: throw exception_t(fmt::format("Unknown key distribution: {}", int(workload.key_dist)));
    }
    return generator;
}

inline worker_t::value_length_generator_t worker_t::create_value_length_generator(workload_t const& workload) {

    value_length_generator_t generator;
    switch (workload.value_length_dist) {
    case distribution_kind_t::const_k:
        generator = std::make_unique<core::const_generator_gt<value_length_t>>(workload.value_length);
        break;
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<core::uniform_generator_gt<value_length_t>>(1, workload.value_length);
        break;
    default: throw exception_t(fmt::format("Unknown value length distribution: {}", int(workload.value_length_dist)));
    }
    return generator;
}

inline worker_t::length_generator_t worker_t::create_batch_upsert_length_generator(workload_t const& workload) {

    length_generator_t generator;
    switch (workload.batch_upsert_length_dist) {
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<core::uniform_generator_gt<size_t>>(workload.batch_upsert_min_length,
                                                                         workload.batch_upsert_max_length);
        break;
    case distribution_kind_t::zipfian_k:
        generator = std::make_unique<core::zipfian_generator_t>(workload.batch_upsert_min_length,
                                                                workload.batch_upsert_max_length);
        break;
    default:
        throw exception_t(
            fmt::format("Unknown range select length distribution: {}", int(workload.batch_upsert_length_dist)));
    }
    return generator;
}

inline worker_t::length_generator_t worker_t::create_batch_read_length_generator(workload_t const& workload) {
    length_generator_t generator;
    switch (workload.batch_read_length_dist) {
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<core::uniform_generator_gt<size_t>>(workload.batch_read_min_length,
                                                                         workload.batch_read_max_length);
        break;
    case distribution_kind_t::zipfian_k:
        generator =
            std::make_unique<core::zipfian_generator_t>(workload.batch_read_min_length, workload.batch_read_max_length);
        break;
    default:
        throw exception_t(
            fmt::format("Unknown batch read length distribution: {}", int(workload.batch_read_length_dist)));
    }
    return generator;
}

inline worker_t::length_generator_t worker_t::create_bulk_load_length_generator(workload_t const& workload) {

    length_generator_t generator;
    switch (workload.bulk_load_length_dist) {
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<core::uniform_generator_gt<size_t>>(workload.bulk_load_min_length,
                                                                         workload.bulk_load_max_length);
        break;
    case distribution_kind_t::zipfian_k:
        generator =
            std::make_unique<core::zipfian_generator_t>(workload.bulk_load_min_length, workload.bulk_load_max_length);
        break;
    default:
        throw exception_t(
            fmt::format("Unknown range select length distribution: {}", int(workload.bulk_load_length_dist)));
    }
    return generator;
}

inline worker_t::length_generator_t worker_t::create_range_select_length_generator(workload_t const& workload) {

    length_generator_t generator;
    switch (workload.range_select_length_dist) {
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<core::uniform_generator_gt<size_t>>(workload.range_select_min_length,
                                                                         workload.range_select_max_length);
        break;
    case distribution_kind_t::zipfian_k:
        generator = std::make_unique<core::zipfian_generator_t>(workload.range_select_min_length,
                                                                workload.range_select_max_length);
        break;
    default:
        throw exception_t(
            fmt::format("Unknown range select length distribution: {}", int(workload.range_select_length_dist)));
    }
    return generator;
}

inline key_t worker_t::generate_key() {
    key_t key = 0;
    do {
        key = key_generator_->generate();
    } while (key > upsert_key_sequence_generator->last());
    return key;
}

inline keys_spanc_t worker_t::generate_batch_upsert_keys() {
    size_t batch_length = batch_upsert_length_generator_->generate();
    keys_span_t keys(keys_buffer_.data(), batch_length);
    for (size_t i = 0; i < batch_length; ++i) {
        key_t key = upsert_key_sequence_generator->generate();
        keys[i] = key;
        if (acknowledged_key_generator)
            acknowledged_key_generator->acknowledge(key);
    }

    return keys;
}

inline keys_spanc_t worker_t::generate_batch_read_keys() {
    size_t batch_length = batch_read_length_generator_->generate();
    keys_span_t keys(keys_buffer_.data(), batch_length);
    size_t unique_keys_count = 0;
    std::set<key_t> unique_keys;
    while (unique_keys_count != batch_length) {
        auto key = generate_key();
        if (!unique_keys.contains(key)) {
            keys[unique_keys_count] = key;
            unique_keys_count++;
            unique_keys.insert(key);
        }
    }
    return keys;
}

inline keys_spanc_t worker_t::generate_bulk_load_keys() {
    size_t bulk_length = bulk_load_length_generator_->generate();
    keys_span_t keys(keys_buffer_.data(), bulk_length);
    for (size_t i = 0; i < bulk_length; ++i) {
        key_t key = upsert_key_sequence_generator->generate();
        keys[i] = key;
        if (acknowledged_key_generator)
            acknowledged_key_generator->acknowledge(key);
    }

    return keys;
}

inline value_spanc_t worker_t::generate_value() {
    values_and_sizes_spanc_t value_and_size = generate_values(1);
    return value_spanc_t {value_and_size.first.data(), value_and_size.second.front()};
}

inline worker_t::values_and_sizes_spanc_t worker_t::generate_values(size_t count) {
    for (size_t i = 0; i < count * workload_.value_length; ++i)
        values_buffer_[i] = std::byte(value_generator_.generate());

    size_t total_length = 0;
    for (size_t i = 0; i < count; ++i) {
        value_length_t length = value_length_generator_->generate();
        value_sizes_buffer_[i] = length;
        total_length += length;
    }
    return std::make_pair(values_spanc_t(values_buffer_.data(), total_length),
                          value_lengths_spanc_t(value_sizes_buffer_.data(), count));
}

inline value_span_t worker_t::value_buffer() { return values_buffer(1); }

inline values_span_t worker_t::values_buffer(size_t count) {
    size_t value_aligned_length = roundup_to_multiple<values_buffer_t::alignment_k>(workload_.value_length);
    size_t total_length = count * value_aligned_length;
    return values_span_t(values_buffer_.data(), total_length);
}

} // namespace ucsb