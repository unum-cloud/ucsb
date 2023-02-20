#pragma once
#include <cassert>

#include <ukv/cpp/db.hpp>
#include <ukv/cpp/types.hpp>
#include <ukv/cpp/blobs_collection.hpp>

#include "src/core/db.hpp"
#include "src/core/helper.hpp"
#include "src/core/types.hpp"

namespace ucsb::ukv {

namespace fs = ucsb::fs;
using namespace unum::ukv;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using db_hints_t = ucsb::db_hints_t;
using transaction_t = ucsb::transaction_t;

class ukv_t : public ucsb::db_t {
  public:
    inline ukv_t() : db_(nullptr) {}
    ~ukv_t() { close(); }
    void set_config(fs::path const& config_path,
                    fs::path const& main_dir_path,
                    std::vector<fs::path> const& storage_dir_paths,
                    db_hints_t const& hints) override;
    bool open() override;
    bool close() override;
    void destroy() override;

    operation_result_t upsert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) override;
    operation_result_t batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t batch_read(keys_spanc_t keys, values_span_t values) override;

    operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t range_select(key_t key, size_t length, values_span_t values) override;
    operation_result_t scan(key_t key, size_t length, value_span_t single_value) override;

    void flush() override;
    size_t size_on_disk() override;
    std::unique_ptr<transaction_t> create_transaction() override;

  private:
    inline value_view_t make_value(std::byte const* ptr, size_t length) {
        return {reinterpret_cast<char const*>(ptr), length};
    }
    fs::path config_path_;
    fs::path main_dir_path_;
    std::vector<fs::path> storage_dir_paths_;

    std::unique_ptr<database_t> db_;
    blobs_collection_t collection_;
};

void ukv_t::set_config(fs::path const& config_path,
                       fs::path const& main_dir_path,
                       [[maybe_unused]] std::vector<fs::path> const& storage_dir_paths,
                       [[maybe_unused]] db_hints_t const& hints) {
    config_path_ = config_path;
    main_dir_path_ = main_dir_path;
}

bool ukv_t::open() {
    if (db_)
        return true;
    db_ = std::make_unique<database_t>();
    auto status = db_->open(config_path_.c_str());
    if (!status)
        return status;

    collection_ = db_->main();
    return true;
}

bool ukv_t::close() {
    db_.reset(nullptr);
    return true;
}

void ukv_t::destroy() {
    auto status = db_->clear();
    assert(!status);
}

operation_result_t ukv_t::upsert(key_t key, value_spanc_t value) {
    auto status = collection_[key].assign(make_value(value.data(), value.size()));
    return {size_t(status), status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::update(key_t key, value_spanc_t value) {
    auto status = collection_[key].value();
    if (status)
        return {0, operation_status_t::not_found_k};

    status = collection_[key].assign(make_value(value.data(), value.size()));
    return {size_t(status), status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::remove(key_t key) {
    auto status = collection_[key].erase();
    return {bool(status), status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::read(key_t key, value_span_t value) {
    auto status = collection_[key].value();
    if (!status)
        return {0, operation_status_t::not_found_k};

    auto data = *status;
    memcpy(value.data(), data.data(), data.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t ukv_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

    size_t offset = 0;
    for (size_t idx = 0; idx != keys.size(); ++idx) {
        key_t key = keys[idx];
        auto span = values.subspan(offset, sizes[idx]);
        auto status = collection_[key].assign(make_value(span.data(), span.size()));
        if (!status)
            return {0, operation_status_t::error_k};
        offset += sizes[idx];
    }
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t ukv_t::batch_read(keys_spanc_t keys, values_span_t values) {

    size_t offset = 0;
    size_t found_cnt = 0;
    for (auto key : keys) {
        std::string data;
        auto status = collection_[key].value();
        if (status) {
            data = *status;
            memcpy(values.data() + offset, data.data(), data.size());
            offset += data.size();
            ++found_cnt;
        }
    }
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t ukv_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    return batch_upsert(keys, values, sizes);
}

operation_result_t ukv_t::range_select(key_t key, size_t length, values_span_t values) {

    size_t i = 0;
    size_t offset = 0;
    auto it = collection_.keys(key).begin();
    for (; i != length; i++, ++it) {
        auto data = *collection_[it.key()].value();
        memcpy(values.data() + offset, data.data(), data.size());
        offset += data.size();
    }
    return {i, operation_status_t::ok_k};
}

operation_result_t ukv_t::scan(key_t key, size_t length, value_span_t single_value) {

    size_t i = 0;
    auto it = collection_.keys(key).begin();
    for (; i != length; i++, ++it) {
        auto data = *collection_[it.key()].value();
        memcpy(single_value.data(), data.data(), data.size());
    }
    return {i, operation_status_t::ok_k};
}

void ukv_t::flush() {
    close();
    open();
}

size_t ukv_t::size_on_disk() {
    return ucsb::size_on_disk(main_dir_path_);
}

std::unique_ptr<transaction_t> ukv_t::create_transaction() {
    return {};
}

} // namespace ucsb::ukv