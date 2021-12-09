#pragma once

#include <memory>
#include <vector>
#include <fmt/format.h>

#include "types.hpp"
#include "ucsb/core/workload.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helpers.hpp"
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
    using generator_t = std::unique_ptr<generator_gt<size_t>>;
    using key_generator_t = std::unique_ptr<generator_gt<key_t>>;
    using value_length_generator_t = std::unique_ptr<generator_gt<value_length_t>>;

    inline transaction_t(workload_t const& workload, db_t& db);

    operation_result_t do_insert();
    operation_result_t do_update();
    operation_result_t do_read();
    operation_result_t do_remove();
    operation_result_t do_batch_read();
    operation_result_t do_range_select();
    operation_result_t do_scan();

  private:
    inline key_generator_t create_key_generator(workload_t const& workload, counter_generator_t& counter_generator);
    inline value_length_generator_t create_value_length_generator(workload_t const& workload);
    inline generator_t create_batch_length_generator(workload_t const& workload);
    inline generator_t create_range_select_length_generator(workload_t const& workload);

    inline key_t generate_key();
    inline keys_span_t generate_batch_keys();
    inline value_span_t generate_value();

    workload_t workload_;
    db_t* db_;

    key_generator_t insert_key_sequence_counter_generator;
    key_generator_t insert_key_sequence_generator;
    key_generator_t key_generator_;
    keys_t keys_;

    generator_t batch_length_generator_;
    generator_t range_select_length_generator_;

    value_length_generator_t value_length_generator_;
    random_byte_generator_t value_generator_;
    std::vector<std::byte> value_buffer_;
};

inline transaction_t::transaction_t(workload_t const& workload, db_t& db)
    : workload_(workload), db_(&db), insert_key_sequence_generator(nullptr) {

    if (workload.insert_proportion == 1.0) // Initialization case
        insert_key_sequence_generator.reset(new counter_generator_t(0));
    else {
        auto acknowledged_counter_generator = new acknowledged_counter_generator_t(workload.records_count);
        insert_key_sequence_generator.reset(acknowledged_counter_generator);
        key_generator_ = create_key_generator(workload, *acknowledged_counter_generator);
    }
    keys_ = keys_t(workload.batch_max_length);

    batch_length_generator_ = create_batch_length_generator(workload);
    range_select_length_generator_ = create_range_select_length_generator(workload);

    value_length_generator_ = create_value_length_generator(workload);
    value_buffer_ = value_t(workload.value_length);
}

operation_result_t transaction_t::do_insert() {
    key_t key = insert_key_sequence_generator->generate();
    value_span_t value = generate_value();
    return db_->insert(key, value);
}

operation_result_t transaction_t::do_update() {
    key_t key = generate_key();
    value_span_t value = generate_value();
    return db_->update(key, value);
}

operation_result_t transaction_t::do_remove() {
    key_t key = generate_key();
    return db_->remove(key);
}

operation_result_t transaction_t::do_read() {
    key_t key = generate_key();
    value_span_t value(value_buffer_.data(), value_buffer_.size());
    return db_->read(key, value);
}

operation_result_t transaction_t::do_batch_read() {
    keys_span_t keys = generate_batch_keys();
    value_span_t single_value(value_buffer_.data(), value_buffer_.size());
    return db_->batch_read(keys, single_value);
}

operation_result_t transaction_t::do_range_select() {
    key_t key = generate_key();
    size_t length = range_select_length_generator_->generate();
    value_span_t single_value(value_buffer_.data(), value_buffer_.size());
    return db_->range_select(key, length, single_value);
}

operation_result_t transaction_t::do_scan() {
    value_span_t single_value(value_buffer_.data(), value_buffer_.size());
    return db_->scan(single_value);
}

inline transaction_t::key_generator_t transaction_t::create_key_generator(workload_t const& workload,
                                                                          counter_generator_t& counter_generator) {
    key_generator_t generator;
    switch (workload.key_dist) {
    case distribution_kind_t::uniform_k: generator.reset(new uniform_generator_t(0, workload.records_count - 1)); break;
    case distribution_kind_t::scrambled_zipfian_k: {
        size_t new_keys = (size_t)(workload.operations_count * workload.insert_proportion * 2);
        generator.reset(new scrambled_zipfian_generator_t(workload.records_count + new_keys));
        break;
    }
    case distribution_kind_t::skewed_latest_k: generator.reset(new skewed_latest_generator_t(counter_generator)); break;
    default: throw exception_t(format("Unknown key distribution: {}", int(workload.key_dist)));
    }
    return generator;
}

inline transaction_t::value_length_generator_t transaction_t::create_value_length_generator(
    workload_t const& workload) {

    value_length_generator_t generator;
    switch (workload.value_length_dist) {
    case distribution_kind_t::const_k: generator.reset(new const_generator_t(workload.value_length)); break;
    case distribution_kind_t::uniform_k: generator.reset(new uniform_generator_t(1, workload.value_length)); break;
    case distribution_kind_t::zipfian_k: generator.reset(new zipfian_generator_t(1, workload.value_length)); break;
    default: throw exception_t(format("Unknown value length distribution: {}", int(workload.value_length_dist)));
    }
    return generator;
}

inline transaction_t::generator_t transaction_t::create_batch_length_generator(workload_t const& workload) {
    generator_t generator;
    switch (workload.batch_length_dist) {
    case distribution_kind_t::uniform_k:
        generator.reset(new uniform_generator_t(workload.batch_min_length, workload.batch_max_length));
        break;
    case distribution_kind_t::zipfian_k:
        generator.reset(new zipfian_generator_t(workload.batch_min_length, workload.batch_max_length));
        break;
    default: throw exception_t(format("Unknown range select length distribution: {}", int(workload.batch_length_dist)));
    }
    return generator;
}

inline transaction_t::generator_t transaction_t::create_range_select_length_generator(workload_t const& workload) {
    generator_t generator;
    switch (workload.range_select_length_dist) {
    case distribution_kind_t::uniform_k:
        generator.reset(new uniform_generator_t(workload.range_select_min_length, workload.range_select_max_length));
        break;
    case distribution_kind_t::zipfian_k:
        generator.reset(new zipfian_generator_t(workload.range_select_min_length, workload.range_select_max_length));
        break;
    default:
        throw exception_t(
            format("Unknown range select length distribution: {}", int(workload.range_select_length_dist)));
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

inline keys_span_t transaction_t::generate_batch_keys() {
    size_t batch_length = batch_length_generator_->generate();
    keys_span_t keys(keys_.data(), batch_length);
    for (size_t i = 0; i < batch_length; ++i)
        keys[i] = generate_key();
    return keys;
}

inline value_span_t transaction_t::generate_value() {
    value_span_t value(value_buffer_.data(), value_buffer_.size());
    for (size_t i = 0; i < value.size(); ++i)
        value[i] = std::byte(value_generator_.generate());
    return value;
}

} // namespace ucsb