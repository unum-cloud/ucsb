#pragma once
#include <utility>
#include <memory>
#include <vector>
#include <fmt/format.h>

#include "types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/workload.hpp"
#include "ucsb/core/generators/generator.hpp"
#include "ucsb/core/generators/const_generator.hpp"
#include "ucsb/core/generators/counter_generator.hpp"
#include "ucsb/core/generators/uniform_generator.hpp"
#include "ucsb/core/generators/zipfian_generator.hpp"
#include "ucsb/core/generators/scrambled_zipfian_generator.hpp"
#include "ucsb/core/generators/skewed_zipfian_generator.hpp"
#include "ucsb/core/generators/acknowledged_counter_generator.hpp"

namespace ucsb {

struct transaction_t {
    using key_generator_t = std::unique_ptr<generator_gt<key_t>>;
    using acknowledged_key_generator_t = std::unique_ptr<acknowledged_counter_generator_t>;
    using value_length_generator_t = std::unique_ptr<generator_gt<value_length_t>>;
    using length_generator_t = std::unique_ptr<generator_gt<size_t>>;
    using values_and_sizes_spans_t = std::pair<values_span_t, value_sizes_span_t>;

    inline transaction_t(workload_t const& workload, db_t& db);

    inline operation_result_t do_insert();
    inline operation_result_t do_update();
    inline operation_result_t do_remove();
    inline operation_result_t do_read();
    inline operation_result_t do_batch_insert();
    inline operation_result_t do_batch_read();
    inline operation_result_t do_range_select();
    inline operation_result_t do_scan();
    inline operation_result_t do_read_modify_write();

  private:
    inline key_generator_t create_key_generator(workload_t const& workload, counter_generator_t& counter_generator);
    inline value_length_generator_t create_value_length_generator(workload_t const& workload);
    inline length_generator_t create_batch_insert_length_generator(workload_t const& workload);
    inline length_generator_t create_batch_read_length_generator(workload_t const& workload);
    inline length_generator_t create_range_select_length_generator(workload_t const& workload);

    inline key_t generate_key();
    inline keys_spanc_t generate_batch_insert_keys();
    inline keys_spanc_t generate_batch_read_keys();
    inline value_spanc_t generate_value();
    inline values_and_sizes_spans_t generate_values();
    inline value_span_t value_buffer();

    workload_t workload_;
    db_t* db_;

    key_generator_t insert_key_sequence_generator;
    acknowledged_key_generator_t acknowledged_key_generator;
    key_generator_t key_generator_;
    keys_t keys_buffer_;

    value_length_generator_t value_length_generator_;
    random_byte_generator_t value_generator_;
    values_buffer_t values_buffer_;
    value_sizes_t value_sizes_buffer_;

    length_generator_t batch_insert_length_generator_;
    length_generator_t batch_read_length_generator_;
    length_generator_t range_select_length_generator_;
};

inline transaction_t::transaction_t(workload_t const& workload, db_t& db) : workload_(workload), db_(&db) {

    if (workload.insert_proportion == 1.0 || workload.batch_insert_proportion == 1.0) // Initialization case
        insert_key_sequence_generator = std::make_unique<counter_generator_t>(workload.start_key);
    else {
        acknowledged_key_generator =
            std::make_unique<acknowledged_counter_generator_t>(workload.start_key + workload.db_records_count);
        key_generator_ = create_key_generator(workload, *acknowledged_key_generator);
        insert_key_sequence_generator = std::move(acknowledged_key_generator);
    }
    keys_buffer_ = keys_t(std::max(workload.batch_insert_max_length, workload.batch_read_max_length));

    value_length_generator_ = create_value_length_generator(workload);
    size_t values_count = std::max(workload.batch_insert_max_length, size_t(1));
    values_buffer_ = values_buffer_t(values_count * workload.value_length);
    value_sizes_buffer_ = value_sizes_t(values_count, 0);

    batch_insert_length_generator_ = create_batch_insert_length_generator(workload);
    batch_read_length_generator_ = create_batch_read_length_generator(workload);
    range_select_length_generator_ = create_range_select_length_generator(workload);
}

inline operation_result_t transaction_t::do_insert() {
    key_t key = insert_key_sequence_generator->generate();
    value_spanc_t value = generate_value();
    auto status = db_->insert(key, value);
    if (acknowledged_key_generator)
        acknowledged_key_generator->acknowledge(key);
    return status;
}

inline operation_result_t transaction_t::do_update() {
    key_t key = generate_key();
    value_spanc_t value = generate_value();
    return db_->update(key, value);
}

inline operation_result_t transaction_t::do_remove() {
    key_t key = generate_key();
    return db_->remove(key);
}

inline operation_result_t transaction_t::do_read() {
    key_t key = generate_key();
    value_span_t value = value_buffer();
    return db_->read(key, value);
}

inline operation_result_t transaction_t::do_batch_insert() {
    keys_spanc_t keys = generate_batch_insert_keys();
    values_and_sizes_spans_t values_and_sizes = generate_values();
    return db_->batch_insert(keys, values_and_sizes.first, values_and_sizes.second);
}

inline operation_result_t transaction_t::do_batch_read() {
    keys_spanc_t keys = generate_batch_read_keys();
    return db_->batch_read(keys);
}

inline operation_result_t transaction_t::do_range_select() {
    key_t key = generate_key();
    size_t length = range_select_length_generator_->generate();
    value_span_t single_value = value_buffer();
    return db_->range_select(key, length, single_value);
}

inline operation_result_t transaction_t::do_scan() {
    value_span_t single_value = value_buffer();
    return db_->scan(single_value);
}

inline operation_result_t transaction_t::do_read_modify_write() {
    key_t key = generate_key();
    value_span_t read_value = value_buffer();
    db_->read(key, read_value);

    value_spanc_t value = generate_value();
    return db_->update(key, value);
}

inline transaction_t::key_generator_t transaction_t::create_key_generator(workload_t const& workload,
                                                                          counter_generator_t& counter_generator) {
    key_generator_t generator;
    switch (workload.key_dist) {
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<uniform_generator_gt<key_t>>(workload.start_key,
                                                                  workload.start_key + workload.records_count - 1);
        break;
    case distribution_kind_t::zipfian_k: {
        size_t new_keys = (size_t)(workload.operations_count * workload.insert_proportion * 2);
        generator = std::make_unique<scrambled_zipfian_generator_t>(workload.start_key,
                                                                    workload.start_key + workload.db_records_count +
                                                                        new_keys - 1);
        break;
    }
    case distribution_kind_t::skewed_latest_k:
        generator = std::make_unique<skewed_latest_generator_t>(counter_generator);
        break;
    default: throw exception_t(fmt::format("Unknown key distribution: {}", int(workload.key_dist)));
    }
    return generator;
}

inline transaction_t::value_length_generator_t transaction_t::create_value_length_generator(
    workload_t const& workload) {

    value_length_generator_t generator;
    switch (workload.value_length_dist) {
    case distribution_kind_t::const_k:
        generator = std::make_unique<const_generator_gt<value_length_t>>(workload.value_length);
        break;
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<uniform_generator_gt<value_length_t>>(1, workload.value_length);
        break;
    default: throw exception_t(fmt::format("Unknown value length distribution: {}", int(workload.value_length_dist)));
    }
    return generator;
}

inline transaction_t::length_generator_t transaction_t::create_batch_insert_length_generator(
    workload_t const& workload) {

    length_generator_t generator;
    switch (workload.batch_insert_length_dist) {
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<uniform_generator_gt<size_t>>(workload.batch_insert_min_length,
                                                                   workload.batch_insert_max_length);
        break;
    case distribution_kind_t::zipfian_k:
        generator =
            std::make_unique<zipfian_generator_t>(workload.batch_insert_min_length, workload.batch_insert_max_length);
        break;
    default:
        throw exception_t(
            fmt::format("Unknown range select length distribution: {}", int(workload.batch_insert_length_dist)));
    }
    return generator;
}

inline transaction_t::length_generator_t transaction_t::create_batch_read_length_generator(workload_t const& workload) {
    length_generator_t generator;
    switch (workload.batch_read_length_dist) {
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<uniform_generator_gt<size_t>>(workload.batch_read_min_length,
                                                                   workload.batch_read_max_length);
        break;
    case distribution_kind_t::zipfian_k:
        generator =
            std::make_unique<zipfian_generator_t>(workload.batch_read_min_length, workload.batch_read_max_length);
        break;
    default:
        throw exception_t(
            fmt::format("Unknown batch read length distribution: {}", int(workload.batch_read_length_dist)));
    }
    return generator;
}

inline transaction_t::length_generator_t transaction_t::create_range_select_length_generator(
    workload_t const& workload) {

    length_generator_t generator;
    switch (workload.range_select_length_dist) {
    case distribution_kind_t::uniform_k:
        generator = std::make_unique<uniform_generator_gt<size_t>>(workload.range_select_min_length,
                                                                   workload.range_select_max_length);
        break;
    case distribution_kind_t::zipfian_k:
        generator =
            std::make_unique<zipfian_generator_t>(workload.range_select_min_length, workload.range_select_max_length);
        break;
    default:
        throw exception_t(
            fmt::format("Unknown range select length distribution: {}", int(workload.range_select_length_dist)));
    }
    return generator;
}

inline key_t transaction_t::generate_key() {
    key_t key = 0;
    do {
        key = key_generator_->generate();
    } while (key > insert_key_sequence_generator->last());
    return key;
}

inline keys_spanc_t transaction_t::generate_batch_insert_keys() {
    size_t batch_length = batch_insert_length_generator_->generate();
    keys_span_t keys(keys_buffer_.data(), batch_length);
    for (size_t i = 0; i < batch_length; ++i) {
        key_t key = insert_key_sequence_generator->generate();
        keys[i] = key;
        if (acknowledged_key_generator)
            acknowledged_key_generator->acknowledge(key);
    }

    return keys;
}

inline keys_spanc_t transaction_t::generate_batch_read_keys() {
    size_t batch_length = batch_read_length_generator_->generate();
    keys_span_t keys(keys_buffer_.data(), batch_length);
    for (size_t i = 0; i < batch_length; ++i)
        keys[i] = generate_key();
    return keys;
}

inline value_spanc_t transaction_t::generate_value() {
    value_length_t length = value_length_generator_->generate();
    value_sizes_buffer_[0] = length;
    value_span_t value(values_buffer_.data(), length);
    for (size_t i = 0; i < value.size(); ++i)
        value[i] = std::byte(value_generator_.generate());
    return value;
}

inline transaction_t::values_and_sizes_spans_t transaction_t::generate_values() {
    auto aaa = values_buffer_.size();
    for (size_t i = 0; i < values_buffer_.size(); ++i)
        values_buffer_[i] = std::byte(value_generator_.generate());

    size_t total_length = 0;
    size_t values_count = std::max(workload_.batch_insert_max_length, size_t(1));
    for (size_t i = 0; i < values_count; ++i) {
        value_length_t length = value_length_generator_->generate();
        value_sizes_buffer_[i] = length;
        total_length += length;
    }
    return std::make_pair(values_span_t(values_buffer_.data(), total_length),
                          value_sizes_span_t(value_sizes_buffer_.data(), values_count));
}

inline value_span_t transaction_t::value_buffer() {
    return value_span_t(values_buffer_.data(), workload_.value_length);
}

} // namespace ucsb