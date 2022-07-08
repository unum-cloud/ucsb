#pragma once

#include <memory>
#include <vector>
#include <fmt/format.h>

#include "diskkv/lsm/region.hpp"

#include "ucsb/core/types.hpp"
#include "ucsb/core/data_accessor.hpp"
#include "ucsb/core/helper.hpp"

namespace unum {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

using fingerprint_t = key_t;
using region_t = region_gt<key_t, data_source_t::unfixed_size_k>;
using region_transaction_t = region_t::transaction_t;
using find_purpose_t = region_transaction_t::find_purpose_t;

/**
 * @brief UnumDB transactional wrapper for the UCSB benchmark.
 */
struct unumdb_transaction_t : public ucsb::transaction_t {
  public:
    inline unumdb_transaction_t(std::unique_ptr<region_transaction_t>&& transaction,
                                citizen_size_t citizen_max_size,
                                size_t uring_queue_depth,
                                resources_ptr_t resources)
        : transaction_(std::forward<std::unique_ptr<region_transaction_t>&&>(transaction)), resources_(resources),
          uring_queue_depth_(uring_queue_depth), citizen_max_size_(citizen_max_size) {}
    inline ~unumdb_transaction_t();

    operation_result_t upsert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const override;

    operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;

    operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;
    operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

  private:
    std::unique_ptr<region_transaction_t> transaction_;
    resources_ptr_t resources_;
    size_t uring_queue_depth_;
    citizen_size_t citizen_max_size_;
};

inline unumdb_transaction_t::~unumdb_transaction_t() {
    [[maybe_unused]] auto status = transaction_->commit();
    assert(status == status_t::ok_k);
}

operation_result_t unumdb_transaction_t::upsert(key_t key, value_spanc_t value) {
    citizen_view_t citizen {reinterpret_cast<byte_t const*>(value.data()), value.size()};
    auto status = transaction_->insert(key, citizen);
    if (status != status_t::ok_k) {
        assert(status == status_t::not_enough_ram_k);
        status = transaction_->commit();
        assert(status == status_t::ok_k);
        status = transaction_->insert(key, citizen);
        assert(status == status_t::ok_k);
    }
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_transaction_t::update(key_t key, value_spanc_t value) {

    citizen_location_t location;
    transaction_->find<find_purpose_t::read_only_k>(key, location);
    if (!location)
        return {1, operation_status_t::not_found_k};

    citizen_view_t citizen {reinterpret_cast<byte_t const*>(value.data()), value.size()};
    auto status = transaction_->insert(key, citizen);
    if (status != status_t::ok_k) {
        assert(status == status_t::not_enough_ram_k);
        status = transaction_->commit();
        assert(status == status_t::ok_k);
        status = transaction_->insert(key, citizen);
        assert(status == status_t::ok_k);
    }
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_transaction_t::remove(key_t key) {
    auto status = transaction_->remove(key);
    if (status != status_t::ok_k) {
        assert(status == status_t::not_enough_ram_k);
        status = transaction_->commit();
        assert(status == status_t::ok_k);
        status = transaction_->remove(key);
        assert(status == status_t::ok_k);
    }
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_transaction_t::read(key_t key, value_span_t value) const {
    citizen_location_t location;
    transaction_->find<find_purpose_t::read_only_k>(key, location);
    if (!location)
        return {1, operation_status_t::not_found_k};

    countdown_t countdown;
    notifier_t read_notifier(countdown);
    citizen_span_t citizen {reinterpret_cast<byte_t*>(value.data()), value.size()};
    transaction_->select(location, citizen, read_notifier);
    if (!countdown.wait(*resources_->fibers))
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_transaction_t::batch_upsert(keys_spanc_t keys,
                                                      values_spanc_t values,
                                                      value_lengths_spanc_t sizes) {
    size_t offset = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        citizen_view_t citizen {reinterpret_cast<byte_t const*>(values.data() + offset), sizes[idx]};
        auto status = transaction_->insert(keys[idx], citizen);
        if (status != status_t::ok_k) {
            assert(status == status_t::not_enough_ram_k);
            status = transaction_->commit();
            assert(status == status_t::ok_k);
            status = transaction_->insert(keys[idx], citizen);
            assert(status == status_t::ok_k);
        }
        offset += sizes[idx];
    }
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t unumdb_transaction_t::batch_read(keys_spanc_t keys, values_span_t values) const {

    darray_gt<fingerprint_t> fingerprints;
    fingerprints.reserve(keys.size());
    for (const auto& key : keys)
        fingerprints.push_back(key);

    size_t found_cnt = 0;
    size_t current_idx = 0;
    size_t buffer_offset = 0;
    while (current_idx < fingerprints.size()) {
        size_t batch_size = std::min(uring_queue_depth_, fingerprints.size() - current_idx);
        size_t found_buffer_size = 0;
        darray_gt<citizen_location_t> locations(batch_size);
        transaction_->find(fingerprints.view().subspan(current_idx, batch_size), locations.span(), found_buffer_size);

        if (found_buffer_size) {
            countdown_t countdown(batch_size);
            span_gt<byte_t> buffer_span(reinterpret_cast<byte_t*>(values.data()) + buffer_offset, found_buffer_size);
            transaction_->select(locations.view(), buffer_span, countdown);
            if (!countdown.wait(*resources_->fibers))
                return {0, operation_status_t::error_k};
            found_cnt += batch_size;
        }
        current_idx += batch_size;
        buffer_offset += found_buffer_size;
    };

    if (!found_cnt)
        return {current_idx, operation_status_t::not_found_k};
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t unumdb_transaction_t::bulk_load(keys_spanc_t keys,
                                                   values_spanc_t values,
                                                   value_lengths_spanc_t sizes) {
    return batch_upsert(keys, values, sizes);
}

operation_result_t unumdb_transaction_t::range_select(key_t key, size_t length, values_span_t values) const {

    size_t task_cnt = 0;
    size_t selected_records_count = 0;
    size_t batch_size = std::min(length, uring_queue_depth_);
    citizen_size_t citizen_max_size = ucsb::roundup_to_multiple<ucsb::values_buffer_t::alignment_k>(citizen_max_size_);

    countdown_t countdown(0);
    transaction_->lock_commit_shared();
    auto it = transaction_->find(key);
    for (size_t i = 0; it != transaction_->end() && i < length; ++it, ++i) {
        citizen_span_t citizen {reinterpret_cast<byte_t*>(values.data()) + i * citizen_max_size, citizen_max_size};
        countdown.increment(1);
        it.get(citizen, countdown);
        ++task_cnt;

        if ((task_cnt == batch_size) | countdown.has_failed()) {
            selected_records_count += size_t(countdown.wait(*resources_->fibers)) * batch_size;
            batch_size = std::min(length - i + 1, uring_queue_depth_);
            task_cnt = 0;
            countdown.reset(0);
        }
    }
    transaction_->unlock_commit_shared();

    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t unumdb_transaction_t::scan(key_t key, size_t length, value_span_t single_value) const {

    using read_ahead_t = read_ahead_gt<typename region_transaction_t::iterator_t>;

    citizen_size_t citizen_aligned_max_size =
        ucsb::roundup_to_multiple<ucsb::values_buffer_t::alignment_k>(citizen_max_size_);

    size_t const it_batch_size = std::bit_ceil(std::min(length, 1024ul));
    size_t const it_buffer_size = it_batch_size * citizen_aligned_max_size;

    transaction_->lock_commit_shared();
    auto read_ahead_it = read_ahead_t {transaction_->find(key),
                                       it_batch_size,
                                       it_buffer_size,
                                       resources_->fibers,
                                       resources_->disk_router};
    size_t scanned_records_count = 0;
    while (read_ahead_it != end_sentinel_t {} && scanned_records_count != length) {
        auto current = *read_ahead_it;
        memcpy(single_value.data(), current.second.data(), current.second.size());
        ++read_ahead_it;
        ++scanned_records_count;
    }

    transaction_->unlock_commit_shared();
    return {scanned_records_count, operation_status_t::ok_k};
}

} // namespace unum