#pragma once
#include <utility>
#include <memory>
#include <vector>
#include <fmt/format.h>

#include "src/core/types.hpp"
#include "src/core/data_accessor.hpp"
#include "src/core/workload.hpp"
#include "src/core/timer.hpp"
#include "src/core/generators/generator.hpp"
#include "src/core/generators/const_generator.hpp"
#include "src/core/generators/counter_generator.hpp"
#include "src/core/generators/uniform_generator.hpp"
#include "src/core/generators/zipfian_generator.hpp"
#include "src/core/generators/scrambled_zipfian_generator.hpp"
#include "src/core/generators/skewed_zipfian_generator.hpp"
#include "src/core/generators/acknowledged_counter_generator.hpp"

namespace ucsb
{

    struct worker_t
    {
        using key_generator_t = std::unique_ptr<generator_gt<key_t>>;
        using acknowledged_key_generator_t = std::unique_ptr<acknowledged_counter_generator_t>;
        using value_length_generator_t = std::unique_ptr<generator_gt<value_length_t>>;
        using length_generator_t = std::unique_ptr<generator_gt<size_t>>;
        using values_and_sizes_spansc_t = std::pair<values_spanc_t, value_lengths_spanc_t>;

        inline worker_t(workload_t const &workload, data_accessor_t &data_accessor, timer_ref_t timer);

        inline operation_result_t do_insert();
        inline operation_result_t do_update();
        inline operation_result_t do_remove();
        inline operation_result_t do_read();
        inline operation_result_t do_read_modify_write();
        inline operation_result_t do_batch_insert();
        inline operation_result_t do_batch_read();
        inline operation_result_t do_bulk_import();
        inline operation_result_t do_range_select();
        inline operation_result_t do_scan();

    private:
        inline key_generator_t create_key_generator(workload_t const &workload, counter_generator_t &counter_generator);
        inline value_length_generator_t create_value_length_generator(workload_t const &workload);
        inline length_generator_t create_batch_insert_length_generator(workload_t const &workload);
        inline length_generator_t create_batch_read_length_generator(workload_t const &workload);
        inline length_generator_t create_bulk_import_length_generator(workload_t const &workload);
        inline length_generator_t create_range_select_length_generator(workload_t const &workload);

        inline key_t generate_key();
        inline keys_spanc_t generate_batch_insert_keys();
        inline keys_spanc_t generate_batch_read_keys();
        inline keys_spanc_t generate_bulk_import_keys();
        inline value_spanc_t generate_value();
        inline values_and_sizes_spansc_t generate_values(size_t count);
        inline value_span_t value_buffer();
        inline values_span_t values_buffer(size_t count);

        workload_t workload_;
        data_accessor_t *data_accessor_;
        timer_ref_t timer_;

        key_generator_t insert_key_sequence_generator;
        acknowledged_key_generator_t acknowledged_key_generator;
        key_generator_t key_generator_;
        keys_t keys_buffer_;

        value_length_generator_t value_length_generator_;
        random_byte_generator_t value_generator_;
        values_buffer_t values_buffer_;
        value_lengths_t value_sizes_buffer_;

        length_generator_t batch_insert_length_generator_;
        length_generator_t batch_read_length_generator_;
        length_generator_t bulk_import_length_generator_;
        length_generator_t range_select_length_generator_;
    };

    inline worker_t::worker_t(workload_t const &workload, data_accessor_t &data_accessor, timer_ref_t timer)
        : workload_(workload), data_accessor_(&data_accessor), timer_(timer)
    {

        if (workload.insert_proportion == 1.0 || workload.batch_insert_proportion == 1.0 ||
            workload.bulk_import_proportion == 1.0) // Initialization case
            insert_key_sequence_generator = std::make_unique<counter_generator_t>(workload.start_key);
        else
        {
            acknowledged_key_generator =
                std::make_unique<acknowledged_counter_generator_t>(workload.start_key + workload.db_records_count);
            key_generator_ = create_key_generator(workload, *acknowledged_key_generator);
            insert_key_sequence_generator = std::move(acknowledged_key_generator);
        }
        size_t elements_max_count = std::max(
            {workload.batch_insert_max_length, workload.batch_read_max_length, workload.bulk_import_max_length, size_t(1)});
        keys_buffer_ = keys_t(elements_max_count);

        value_length_generator_ = create_value_length_generator(workload);
        values_buffer_ = values_buffer_t(elements_max_count * workload.value_length);
        value_sizes_buffer_ = value_lengths_t(elements_max_count, 0);

        batch_insert_length_generator_ = create_batch_insert_length_generator(workload);
        batch_read_length_generator_ = create_batch_read_length_generator(workload);
        bulk_import_length_generator_ = create_bulk_import_length_generator(workload);
        range_select_length_generator_ = create_range_select_length_generator(workload);
    }

    inline operation_result_t worker_t::do_insert()
    {
        key_t key = insert_key_sequence_generator->generate();
        value_spanc_t value = generate_value();
        auto status = data_accessor_->insert(key, value);
        if (acknowledged_key_generator)
            acknowledged_key_generator->acknowledge(key);
        return status;
    }

    inline operation_result_t worker_t::do_update()
    {
        key_t key = generate_key();
        value_spanc_t value = generate_value();
        return data_accessor_->update(key, value);
    }

    inline operation_result_t worker_t::do_remove()
    {
        key_t key = generate_key();
        return data_accessor_->remove(key);
    }

    inline operation_result_t worker_t::do_read()
    {
        key_t key = generate_key();
        value_span_t value = value_buffer();
        return data_accessor_->read(key, value);
    }

    inline operation_result_t worker_t::do_read_modify_write()
    {
        key_t key = generate_key();
        value_span_t read_value = value_buffer();
        data_accessor_->read(key, read_value);

        value_spanc_t value = generate_value();
        return data_accessor_->update(key, value);
    }

    inline operation_result_t worker_t::do_batch_insert()
    {
        // Note: Pause benchmark timer to do data preparation, to measure batch insert time only
        timer_.pause();
        keys_spanc_t keys = generate_batch_insert_keys();
        values_and_sizes_spansc_t values_and_sizes = generate_values(keys.size());
        timer_.resume();

        return data_accessor_->batch_insert(keys, values_and_sizes.first, values_and_sizes.second);
    }

    inline operation_result_t worker_t::do_batch_read()
    {
        keys_spanc_t keys = generate_batch_read_keys();
        values_span_t values = values_buffer(keys.size());
        return data_accessor_->batch_read(keys, values);
    }

    inline operation_result_t worker_t::do_bulk_import()
    {
        // Note: Pause benchmark timer to do data preparation, to measure bulk import time only
        timer_.pause();
        keys_spanc_t keys = generate_bulk_import_keys();
        values_and_sizes_spansc_t values_and_sizes = generate_values(keys.size());
        auto metadata = data_accessor_->prepare_bulk_import_data(keys, values_and_sizes.first, values_and_sizes.second);
        timer_.resume();

        return data_accessor_->bulk_import(metadata);
    }

    inline operation_result_t worker_t::do_range_select()
    {
        key_t key = generate_key();
        size_t length = range_select_length_generator_->generate();
        values_span_t values = values_buffer(length);
        return data_accessor_->range_select(key, length, values);
    }

    inline operation_result_t worker_t::do_scan()
    {
        value_span_t single_value = value_buffer();
        return data_accessor_->scan(workload_.start_key, workload_.records_count, single_value);
    }

    inline worker_t::key_generator_t worker_t::create_key_generator(workload_t const &workload,
                                                                    counter_generator_t &counter_generator)
    {
        key_generator_t generator;
        switch (workload.key_dist)
        {
        case distribution_kind_t::uniform_k:
            generator = std::make_unique<uniform_generator_gt<key_t>>(workload.start_key,
                                                                      workload.start_key + workload.records_count - 1);
            break;
        case distribution_kind_t::zipfian_k:
        {
            size_t new_keys = (size_t)(workload.operations_count * workload.insert_proportion * 2);
            generator =
                std::make_unique<scrambled_zipfian_generator_t>(workload.start_key,
                                                                workload.start_key + workload.records_count + new_keys - 1);
            break;
        }
        case distribution_kind_t::skewed_latest_k:
            generator = std::make_unique<skewed_latest_generator_t>(counter_generator);
            break;
        default:
            throw exception_t(fmt::format("Unknown key distribution: {}", int(workload.key_dist)));
        }
        return generator;
    }

    inline worker_t::value_length_generator_t worker_t::create_value_length_generator(workload_t const &workload)
    {

        value_length_generator_t generator;
        switch (workload.value_length_dist)
        {
        case distribution_kind_t::const_k:
            generator = std::make_unique<const_generator_gt<value_length_t>>(workload.value_length);
            break;
        case distribution_kind_t::uniform_k:
            generator = std::make_unique<uniform_generator_gt<value_length_t>>(1, workload.value_length);
            break;
        default:
            throw exception_t(fmt::format("Unknown value length distribution: {}", int(workload.value_length_dist)));
        }
        return generator;
    }

    inline worker_t::length_generator_t worker_t::create_batch_insert_length_generator(workload_t const &workload)
    {

        length_generator_t generator;
        switch (workload.batch_insert_length_dist)
        {
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

    inline worker_t::length_generator_t worker_t::create_batch_read_length_generator(workload_t const &workload)
    {
        length_generator_t generator;
        switch (workload.batch_read_length_dist)
        {
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

    inline worker_t::length_generator_t worker_t::create_bulk_import_length_generator(workload_t const &workload)
    {

        length_generator_t generator;
        switch (workload.bulk_import_length_dist)
        {
        case distribution_kind_t::uniform_k:
            generator = std::make_unique<uniform_generator_gt<size_t>>(workload.bulk_import_min_length,
                                                                       workload.bulk_import_max_length);
            break;
        case distribution_kind_t::zipfian_k:
            generator =
                std::make_unique<zipfian_generator_t>(workload.bulk_import_min_length, workload.bulk_import_max_length);
            break;
        default:
            throw exception_t(
                fmt::format("Unknown range select length distribution: {}", int(workload.bulk_import_length_dist)));
        }
        return generator;
    }

    inline worker_t::length_generator_t worker_t::create_range_select_length_generator(workload_t const &workload)
    {

        length_generator_t generator;
        switch (workload.range_select_length_dist)
        {
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

    inline key_t worker_t::generate_key()
    {
        key_t key = 0;
        do
        {
            key = key_generator_->generate();
        } while (key > insert_key_sequence_generator->last());
        return key;
    }

    inline keys_spanc_t worker_t::generate_batch_insert_keys()
    {
        size_t batch_length = batch_insert_length_generator_->generate();
        keys_span_t keys(keys_buffer_.data(), batch_length);
        for (size_t i = 0; i < batch_length; ++i)
        {
            key_t key = insert_key_sequence_generator->generate();
            keys[i] = key;
            if (acknowledged_key_generator)
                acknowledged_key_generator->acknowledge(key);
        }

        return keys;
    }

    inline keys_spanc_t worker_t::generate_batch_read_keys()
    {
        size_t batch_length = batch_read_length_generator_->generate();
        keys_span_t keys(keys_buffer_.data(), batch_length);
        for (size_t i = 0; i < batch_length; ++i)
            keys[i] = generate_key();
        return keys;
    }

    inline keys_spanc_t worker_t::generate_bulk_import_keys()
    {
        size_t bulk_length = bulk_import_length_generator_->generate();
        keys_span_t keys(keys_buffer_.data(), bulk_length);
        for (size_t i = 0; i < bulk_length; ++i)
        {
            key_t key = insert_key_sequence_generator->generate();
            keys[i] = key;
            if (acknowledged_key_generator)
                acknowledged_key_generator->acknowledge(key);
        }

        return keys;
    }

    inline value_spanc_t worker_t::generate_value()
    {
        values_and_sizes_spansc_t value_and_size = generate_values(1);
        return value_spanc_t{value_and_size.first.data(), value_and_size.second[0]};
    }

    inline worker_t::values_and_sizes_spansc_t worker_t::generate_values(size_t count)
    {
        for (size_t i = 0; i < count * workload_.value_length; ++i)
            values_buffer_[i] = std::byte(value_generator_.generate());

        size_t total_length = 0;
        for (size_t i = 0; i < count; ++i)
        {
            value_length_t length = value_length_generator_->generate();
            value_sizes_buffer_[i] = length;
            total_length += length;
        }
        return std::make_pair(values_spanc_t(values_buffer_.data(), total_length),
                              value_lengths_spanc_t(value_sizes_buffer_.data(), count));
    }

    inline value_span_t worker_t::value_buffer()
    {
        return values_buffer(1);
    }

    inline values_span_t worker_t::values_buffer(size_t count)
    {
        size_t total_length = count * workload_.value_length;
        return values_span_t(values_buffer_.data(), total_length);
    }

} // namespace ucsb