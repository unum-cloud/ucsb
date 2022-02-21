#pragma once

#include <memory>
#include <vector>
#include <fmt/format.h>

#include "diskkv/region.hpp"
#include "diskkv/validator.hpp"

#include "ucsb/core/types.hpp"
#include "ucsb/core/data_accessor.hpp"
#include "ucsb/core/helper.hpp"

namespace unum {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using bulk_metadata_t = ucsb::bulk_metadata_t;
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
    inline unumdb_transaction_t(std::unique_ptr<region_transaction_t>&& transaction, size_t uring_queue_depth)
        : transaction_(std::forward<std::unique_ptr<region_transaction_t>&&>(transaction)),
          uring_queue_depth_(uring_queue_depth) {}
    inline ~unumdb_transaction_t();

    operation_result_t insert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t batch_read(keys_spanc_t keys) const override;

    bulk_metadata_t prepare_bulk_import_data(keys_spanc_t keys,
                                             values_spanc_t values,
                                             value_lengths_spanc_t sizes) const override;
    operation_result_t bulk_import(bulk_metadata_t const& metadata) override;

    operation_result_t range_select(key_t key, size_t length, value_span_t single_value) const override;
    operation_result_t scan(value_span_t single_value) const override;

  private:
    size_t uring_queue_depth_;
    std::unique_ptr<region_transaction_t> transaction_;
    mutable dbuffer_t batch_buffer_;
};

inline unumdb_transaction_t::~unumdb_transaction_t() {
    auto status = transaction_->commit();
    assert(status == status_t::ok_k);
}

operation_result_t unumdb_transaction_t::insert(key_t key, value_spanc_t value) {
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
    transaction_->select<caching_t::io_k>(location, citizen, read_notifier);
    if (!countdown.wait())
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_transaction_t::batch_insert(keys_spanc_t keys,
                                                      values_spanc_t values,
                                                      value_lengths_spanc_t sizes) {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t unumdb_transaction_t::batch_read(keys_spanc_t keys) const {
    size_t batch_size = keys.size();
    darray_gt<fingerprint_t> fingerprints;
    fingerprints.reserve(batch_size);
    for (const auto& key : keys)
        fingerprints.push_back(key);
    size_t batch_buffer_size = 0;
    darray_gt<citizen_location_t> locations(batch_size);
    transaction_->find<find_purpose_t::read_only_k>(fingerprints.view(), locations.span(), batch_buffer_size);

    if (!batch_buffer_size)
        return {batch_size, operation_status_t::not_found_k};

    if (batch_buffer_size > batch_buffer_.size())
        batch_buffer_ = dbuffer_t(batch_buffer_size);
    countdown_t countdown(locations.size());
    notifier_t notifier(countdown);
    transaction_->select(locations.view(), {batch_buffer_.span()}, notifier);
    if (!countdown.wait())
        return {0, operation_status_t::error_k};

    return {batch_size, operation_status_t::ok_k};
}

bulk_metadata_t unumdb_transaction_t::prepare_bulk_import_data(keys_spanc_t keys,
                                                               values_spanc_t values,
                                                               value_lengths_spanc_t sizes) const {
    // UnumDB doesn't support bulk import by transaction
    (void)keys;
    (void)values;
    (void)sizes;
    return {};
}

operation_result_t unumdb_transaction_t::bulk_import(bulk_metadata_t const& metadata) {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t unumdb_transaction_t::range_select(key_t key, size_t length, value_span_t single_value) const {
    countdown_t countdown(0);
    notifier_t read_notifier(countdown);
    if (length * single_value.size() > batch_buffer_.size())
        batch_buffer_ = dbuffer_t(length * single_value.size());

    size_t selected_records_count = 0;
    size_t tasks_cnt = std::min(length, uring_queue_depth_);
    size_t task_idx = 0;
    transaction_->lock_commit_shared();
    auto it = transaction_->find(key);
    for (size_t i = 0; it != transaction_->end() && i < length; ++task_idx, ++i, ++it) {
        if ((task_idx == tasks_cnt) | (read_notifier.has_failed())) {
            selected_records_count += size_t(countdown.wait()) * tasks_cnt;
            tasks_cnt = std::min(length - i, uring_queue_depth_);
            task_idx = 0;
        }
        if (!it.is_removed()) {
            citizen_span_t citizen {batch_buffer_.data() + task_idx * single_value.size(), single_value.size()};
            read_notifier.add_one();
            it.get(citizen, countdown);
        }
    }
    selected_records_count += size_t(countdown.wait()) * tasks_cnt;
    transaction_->unlock_commit_shared();
    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t unumdb_transaction_t::scan(value_span_t single_value) const {
    countdown_t countdown;
    citizen_span_t citizen {reinterpret_cast<byte_t*>(single_value.data()), single_value.size()};
    size_t scanned_records_count = 0;

    transaction_->lock_commit_shared();
    auto it = transaction_->begin<caching_t::ram_k>();
    for (; it != transaction_->end<caching_t::ram_k>(); ++it) {
        if (!it.is_removed()) {
            countdown.reset(1);
            it.get(citizen, countdown);
            countdown.wait();
            ++scanned_records_count;
        }
    }
    transaction_->unlock_commit_shared();

    return {scanned_records_count, operation_status_t::ok_k};
}

} // namespace unum