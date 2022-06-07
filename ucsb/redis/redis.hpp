#pragma once

#include <string>
#include <vector>
#include <fmt/format.h>
#include <iostream>

#include <sw/redis++/redis++.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

#include <sys/wait.h>

namespace redis {

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
using transaction_t = ucsb::transaction_t;

/**
 * @brief Redis wrapper for the UCSB benchmark.
 * Using redis-plus-plus client, based on hiredis.
 * https://github.com/sewenew/redis-plus-plus
 */

struct redis_t : public ucsb::db_t {
  public:
    inline redis_t() : redis_(sw::redis::Redis("tcp://127.0.0.1:6379")) {};
    inline ~redis_t() { close(); }
    void set_config(fs::path const& config_path, fs::path const& dir_path) override;
    bool open() override;
    bool close() override;
    void destroy() override;

    operation_result_t upsert(key_t key, value_spanc_t value) override;

    operation_result_t update(key_t key, value_spanc_t value) override;

    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;

    operation_result_t batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;

    operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const override;

    operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;

    operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;

    operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

    void flush() override;
    size_t size_on_disk() const override;

    std::unique_ptr<transaction_t> create_transaction() override;

  private:
    mutable sw::redis::Redis redis_;
    fs::path config_path_;
    fs::path dir_path_;
};

inline sw::redis::StringView to_string_view(std::byte const* p, size_t size_bytes) noexcept {
    return {reinterpret_cast<const char*>(p), size_bytes};
}

inline sw::redis::StringView to_string_view(key_t const& k) noexcept {
    return {reinterpret_cast<const char*>(&k), sizeof(key_t)};
}

void redis_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool redis_t::open() {
    return true;
}

bool redis_t::close() {
    return true;
}

void redis_t::destroy() {
    bool ok = close();
    assert(ok);
}

operation_result_t redis_t::upsert(key_t key, value_spanc_t value) {
    auto status = redis_.set(to_string_view(key),
                             to_string_view(value.data(), value.size()),
                             std::chrono::milliseconds(0),
                             sw::redis::UpdateType::ALWAYS);
    return {status, status ? operation_status_t::ok_k : operation_status_t::not_implemented_k};
}

operation_result_t redis_t::update(key_t key, value_spanc_t value) {
    auto status = redis_.set(to_string_view(key),
                             to_string_view(value.data(), value.size()),
                             std::chrono::milliseconds(0),
                             sw::redis::UpdateType::EXIST);
    return {status, status ? operation_status_t::ok_k : operation_status_t::not_implemented_k};
}

operation_result_t redis_t::remove(key_t key) {
    auto cnt = redis_.del(to_string_view(key));
    return {cnt, cnt ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t redis_t::read(key_t key, value_span_t value) const {
    auto val = redis_.get(to_string_view(key));
    if (val) {
        memcpy(value.data(), &val, sizeof(val));
        return {1, operation_status_t::ok_k};
    }
    else
        return {0, operation_status_t::error_k};
}

operation_result_t redis_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    // TODO: Same as bulk_load but UpateType = ALWAYS.
    size_t offset = 0;
    auto pipe = redis_.pipeline(false);
    for (size_t i = 0; i < keys.size(); i++)
        pipe.set(to_string_view(keys[i]),
                 to_string_view(&values[i], sizes[i]),
                 std::chrono::milliseconds(0),
                 sw::redis::UpdateType::ALWAYS);

    auto pipe_replies = pipe.exec();

    size_t cnt = 0;
    for (size_t i = 0; i < keys.size(); i++)
        cnt += pipe_replies.get<bool>(i);

    return {cnt, cnt == keys.size() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t redis_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    auto pipe = redis_.pipeline(false);
    for (size_t i = 0; i < keys.size(); i++)
        pipe.get(to_string_view(keys[i]));

    auto pipe_replies = pipe.exec();

    size_t cnt = 0;
    size_t offset = 0;
    for (size_t i = 0; i < keys.size(); i++) {
        auto val = pipe_replies.get<sw::redis::OptionalString>(i);
        if (val) {
            memcpy(values.data() + offset, &val, sizeof(val));
            cnt += 1;
            offset += sizeof(val);
        }
        else
            break;
    }
    return {cnt, cnt == keys.size() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t redis_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    size_t offset = 0;
    auto data_offset = 0;
    auto pipe = redis_.pipeline(false);
    for (size_t i = 0; i != keys.size(); i++) {
        pipe.set(to_string_view(keys[i]),
                 to_string_view(values.data() + data_offset, sizes[i]),
                 std::chrono::milliseconds(0),
                 sw::redis::UpdateType::ALWAYS);
        data_offset += sizes[i];
    }

    auto pipe_replies = pipe.exec();

    size_t cnt = 0;
    for (size_t i = 0; i != keys.size(); i++)
        cnt += pipe_replies.get<bool>(i);

    return {cnt, operation_status_t::ok_k};
}

operation_result_t redis_t::range_select(key_t key, size_t length, values_span_t values) const {
    // TODO: Assumption that keys are increasing doesn't work.
    //     auto pipe = (*redis_).pipeline(false);
    //     for (size_t i = key; i < key + length; i++)
    //         pipe.get(to_string_view(&i, sizeof(key_t)));

    //     auto pipe_replies = pipe.exec();

    //     size_t cnt = 0;
    //     size_t offset = 0;
    //     for (size_t i = 0; i < length; i++)
    //     {
    //         auto val = pipe_replies.get<sw::redis::OptionalString>(i);
    //         if (val)
    //         {
    //             cnt += 1;
    //             memcpy(values.data() + offset, &val, sizeof(val));
    //         }
    //         offset += sizeof(val);
    //     }

    //     return {cnt, cnt == length ? operation_status_t::ok_k : operation_status_t::error_k};
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t redis_t::scan(key_t key, size_t length, value_span_t single_value) const {
    // TODO: scan, but it returns only keys.
    return {0, operation_status_t::not_implemented_k};
}

void redis_t::flush() {
    // (*redis_).flushall();
}

size_t redis_t::size_on_disk() const {
    return ucsb::size_on_disk(dir_path_);
}

std::unique_ptr<transaction_t> redis_t::create_transaction() {
    return {};
}

} // namespace redis
