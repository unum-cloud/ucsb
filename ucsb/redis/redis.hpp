#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <sw/redis++/redis++.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

namespace redis {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using value_length_t = ucsb::value_length_t;
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

    void get_options(fs::path const& path);

  private:
    std::unique_ptr<sw::redis::Redis> redis_;
    sw::redis::ConnectionOptions connection_options_;
    sw::redis::ConnectionPoolOptions connection_pool_options_;
    fs::path config_path_;
    fs::path dir_path_;
};

inline sw::redis::StringView to_string_view(std::byte const* p, size_t size_bytes) noexcept {
    return {reinterpret_cast<const char*>(p), size_bytes};
}

inline sw::redis::StringView to_string_view(key_t const& k) noexcept {
    return {reinterpret_cast<const char*>(&k), sizeof(key_t)};
}

std::string exec_cmd(const char* cmd) {
    using namespace std::chrono_literals;
    std::array<char, 4096> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
    if (!pipe)
        throw std::runtime_error("popen() failed!");

    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
        result += buffer.data();

    std::this_thread::sleep_for(2s);
    return result;
}

void redis_t::get_options(fs::path const& path) {
    std::ifstream cfg_file(path);
    nlohmann::json j_config;
    cfg_file >> j_config;
    auto connection_type = j_config["connection_type"].get<std::string>();
    if (connection_type == "TCP") {
        connection_options_.host = j_config["host"].get<std::string>();
        connection_options_.port = j_config["port"].get<int>();
    }
    else if (connection_type == "UNIX") {
        connection_options_.type = sw::redis::ConnectionType::UNIX;
        connection_options_.path = j_config["path"].get<std::string>();
    }
    connection_pool_options_.wait_timeout = std::chrono::milliseconds(j_config["wait_timeout"].get<int>());
    connection_pool_options_.size = j_config["pool_size"].get<int>();
}

void redis_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool redis_t::open() {
    std::string start_cmd("redis-server ");
    start_cmd += config_path_;
    start_cmd += ".redis";
    exec_cmd(start_cmd.c_str());

    get_options(config_path_);
    redis_ = std::make_unique<sw::redis::Redis>(sw::redis::Redis(connection_options_));
    return true;
}

bool redis_t::close() {
    std::string stop_cmd = "redis-cli -s ";
    stop_cmd += connection_options_.path;
    stop_cmd += " shutdown";
    exec_cmd(stop_cmd.c_str());
    return true;
}

void redis_t::destroy() {
}

operation_result_t redis_t::upsert(key_t key, value_spanc_t value) {
    auto status = (*redis_).set(to_string_view(key),
                                to_string_view(value.data(), value.size()),
                                std::chrono::milliseconds(0),
                                sw::redis::UpdateType::ALWAYS);
    return {status, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t redis_t::update(key_t key, value_spanc_t value) {
    auto status = (*redis_).set(to_string_view(key),
                                to_string_view(value.data(), value.size()),
                                std::chrono::milliseconds(0),
                                sw::redis::UpdateType::EXIST);
    return {status, status ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

operation_result_t redis_t::remove(key_t key) {
    auto count = (*redis_).del(to_string_view(key));
    return {count, count ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

operation_result_t redis_t::read(key_t key, value_span_t value) const {
    auto val = (*redis_).get(to_string_view(key));
    if (!val)
        return {0, operation_status_t::not_found_k};

    memcpy(value.data(), &val, sizeof(val));
    return {1, operation_status_t::ok_k};
}

operation_result_t redis_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    struct kv_iterator_t {
        using pair_t = std::pair<sw::redis::StringView, sw::redis::StringView>;
        using val_t = typename values_spanc_t::element_type;
        key_t* key_ptr_;
        val_t* val_ptr_;
        value_length_t* size_ptr_;
        pair_t pair_;

        kv_iterator_t(key_t* key_ptr, val_t* val_ptr, value_length_t* size_ptr) noexcept
            : key_ptr_(key_ptr), val_ptr_(val_ptr), size_ptr_(size_ptr),
              pair_(std::make_pair(to_string_view(*key_ptr_), to_string_view(val_ptr_, *size_ptr_))) {}

        pair_t const& operator*() const noexcept { return pair_; }
        pair_t const* operator->() const noexcept { return &pair_; }
        bool operator==(kv_iterator_t const& other) const noexcept { return key_ptr_ == other.key_ptr_; }

        kv_iterator_t& operator++() noexcept {
            key_ptr_++;
            val_ptr_ += *size_ptr_;
            size_ptr_++;
            pair_.first = to_string_view(*key_ptr_);
            pair_.second = to_string_view(val_ptr_, *size_ptr_);
            return *this;
        }
    };

    (*redis_).mset(
        kv_iterator_t(keys.data(), values.data(), sizes.data()),
        kv_iterator_t(keys.data() + keys.size(), values.data() + values.size(), sizes.data() + sizes.size()));
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t redis_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    struct key_iterator_t {
        key_t* key_ptr_;

        key_iterator_t(key_t* key_ptr) noexcept : key_ptr_(key_ptr) {}
        sw::redis::StringView operator*() const noexcept { return to_string_view(*key_ptr_); }
        bool operator==(key_iterator_t const& other) const noexcept { return key_ptr_ == other.key_ptr_; }

        key_iterator_t& operator++() noexcept {
            key_ptr_++;
            return *this;
        }
    };

    struct value_getter_t {
        using value_type = sw::redis::Optional<std::string>;
        using iterator = values_span_t::pointer;

        values_span_t values;
        size_t count = 0;
        size_t offset = 0;

        iterator push_back(value_type value) noexcept {
            if (!value)
                return values.data() + offset;
            memcpy(values.data() + offset, &value, sizeof(value));
            offset += sizeof(value);
            count++;
            return values.data() + offset;
        }
    };

    value_getter_t getter(values);
    (*redis_).mget(key_iterator_t(keys.data()), key_iterator_t(keys.data() + keys.size()), std::back_inserter(getter));
    return {getter.count, getter.count ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t redis_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    auto data_offset = 0;
    auto pipe = (*redis_).pipeline(false);
    for (size_t i = 0; i != keys.size(); ++i) {
        pipe.set(to_string_view(keys[i]),
                 to_string_view(values.data() + data_offset, sizes[i]),
                 std::chrono::milliseconds(0),
                 sw::redis::UpdateType::ALWAYS);
        data_offset += sizes[i];
    }

    auto pipe_replies = pipe.exec();

    size_t count = 0;
    for (size_t i = 0; i != keys.size(); ++i)
        count += pipe_replies.get<bool>(i);

    return {count, operation_status_t::ok_k};
}

operation_result_t redis_t::range_select(key_t key, size_t length, values_span_t values) const {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t redis_t::scan(key_t key, size_t length, value_span_t single_value) const {
    // TODO: We cant scan from key because redis is not sorted
    // struct key_getter_t {
    //     using value_type = std::string;
    //     using iterator = value_span_t::pointer;

    //     value_span_t single_value;
    //     size_t count = 0;

    //     iterator begin() { return nullptr; }
    //     iterator insert(iterator, std::string key) {
    //         single_value = value_span_t(reinterpret_cast<iterator>(key.data()), key.size());
    //         count++;
    //         return nullptr;
    //     }
    // };

    // auto cursor = 0LL;
    // key_getter_t getter(single_value);
    // cursor = (*redis_).scan(cursor, "*", length, std::inserter(getter, getter.begin()));
    // return {getter.count, getter.count ? operation_status_t::ok_k : operation_status_t::error_k};
    return {0, operation_status_t::not_implemented_k};
}

void redis_t::flush() {
}

size_t redis_t::size_on_disk() const {
    return 0;
}

std::unique_ptr<transaction_t> redis_t::create_transaction() {
    return {};
}

} // namespace redis
