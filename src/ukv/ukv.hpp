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
    inline value_view_t make_value(std::byte const* ptr, size_t length) {
        return {reinterpret_cast<ukv_bytes_cptr_t>(ptr), length};
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
    status_t status;
    arena_t arena(db_.get());
    ukv_key_t key_ = key;
    ukv_length_t length = value.size();
    ukv_length_t offsets = 0;
    auto value_ = make_value(value.data(),value.size());
    ukv_collection_t coll = collection_;

    ukv_write_t write{
        .db = db_.get()->to_ukv(),
        .error = status.member_ptr(),
        .transaction = nullptr,
        .arena = arena.member_ptr(),
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = 1,
        .collections = &coll,
        .collections_stride = 0,
        .keys = &key_,
        .keys_stride = sizeof(ukv_key_t),
        .presences = nullptr,
        .offsets = &offsets,
        .offsets_stride = sizeof(ukv_length_t),
        .lengths = reinterpret_cast<ukv_length_t const*>(&length),
        .lengths_stride = sizeof(ukv_length_t),
        .values = value_.member_ptr(),
        .values_stride = sizeof(ukv_bytes_cptr_t),
    };
    ukv_write(&write);
    return {size_t(status), status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::update(key_t key, value_spanc_t value) {
    status_t status;
    arena_t arena(db_.get());
    ukv_key_t key_ = key;
    ukv_byte_t* value_ = nullptr;
    ukv_collection_t coll = collection_;

    ukv_read_t read{
        .db = db_.get()->to_ukv(),
        .error = status.member_ptr(),
        .transaction = nullptr,
        .snapshot = 0,
        .arena = arena.member_ptr(),
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = 1,
        .collections = &coll,
        .collections_stride = 0,
        .keys = &key_,
        .keys_stride = sizeof(ukv_key_t),
        .presences = nullptr,
        .offsets = nullptr,
        .lengths = nullptr,
        .values = &value_,
    };
    ukv_read(&read);
    if (status)
        return {0, operation_status_t::not_found_k};

    return upsert(key,value);
}

operation_result_t ukv_t::remove(key_t key) {
    status_t status;
    arena_t arena(db_.get());
    ukv_key_t key_ = key;
    ukv_collection_t coll = collection_;

    ukv_write_t write{
        .db = db_.get()->to_ukv(),
        .error = status.member_ptr(),
        .transaction = nullptr,
        .arena = arena.member_ptr(),
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = 1,
        .collections = &coll,
        .collections_stride = 0,
        .keys = &key_,
        .keys_stride = sizeof(ukv_key_t),
        .presences = nullptr,
        .offsets = nullptr,
        .offsets_stride = 0,
        .lengths = nullptr,
        .lengths_stride = 0,
        .values = nullptr,
        .values_stride = 0,
    };
    ukv_write(&write);
    return {status ? size_t(1) : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::read(key_t key, value_span_t value) const {
    status_t status;
    arena_t arena(db_.get());
    ukv_key_t key_ = key;
    ukv_byte_t* value_ = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_collection_t coll = collection_;

    ukv_read_t read{
        .db = db_.get()->to_ukv(),
        .error = status.member_ptr(),
        .transaction = nullptr,
        .snapshot = 0,
        .arena = arena.member_ptr(),
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = 1,
        .collections = &coll,
        .collections_stride = 0,
        .keys = &key_,
        .keys_stride = sizeof(ukv_key_t),
        .presences = nullptr,
        .offsets = nullptr,
        .lengths = &lengths,
        .values = &value_,
    };
    ukv_read(&read);
    if (!status)
        return {0, operation_status_t::not_found_k};

    memcpy(value.data(), value_, lengths[0]);
    return {1, operation_status_t::ok_k};
}

operation_result_t ukv_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    status_t status;
    arena_t arena(db_.get());
    size_t size = 0;
    std::vector<ukv_length_t> offsets;
    ukv_collection_t coll = collection_;
    offsets.reserve(sizes.size() + 1);
    offsets.push_back(0);
    for(auto sz : sizes){
        size += sz;
        offsets.push_back(sz);
    }

    auto val = make_value(values.data(), size);
    ukv_write_t write{
        .db = db_.get()->to_ukv(),
        .error = status.member_ptr(),
        .transaction = nullptr,
        .arena = arena.member_ptr(),
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = keys.size(),
        .collections = &coll,
        .collections_stride = 0,
        .keys = reinterpret_cast<ukv_key_t const*>(keys.data()),
        .keys_stride = sizeof(ukv_key_t),
        .presences = nullptr,
        .offsets = offsets.data(),
        .offsets_stride = sizeof(ukv_length_t),
        .lengths = reinterpret_cast<ukv_length_t const*>(sizes.data()),
        .lengths_stride = sizeof(ukv_length_t),
        .values = val.member_ptr(),
        .values_stride = 0,
    };
    ukv_write(&write);
    return {status ? keys.size() : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    status_t status;
    arena_t arena(db_.get());
    ukv_byte_t* value = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_length_t* offsets = nullptr;
    size_t found_cnt = 0;
    ukv_collection_t coll = collection_;

    ukv_read_t read{
        .db = db_.get()->to_ukv(),
        .error = status.member_ptr(),
        .transaction = nullptr,
        .snapshot = 0,
        .arena = arena.member_ptr(),
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = keys.size(),
        .collections = &coll,
        .collections_stride = 0,
        .keys = reinterpret_cast<ukv_key_t const*>(keys.data()),
        .keys_stride = sizeof(ukv_key_t),
        .presences = nullptr,
        .offsets = &offsets,
        .lengths = &lengths,
        .values = &value,
    };
    ukv_read(&read);
    while(found_cnt != keys.size() && lengths[found_cnt] != ukv_length_missing_k)
        ++found_cnt;
    memcpy(values.data(), value, offsets[found_cnt - 1] + lengths[found_cnt - 1]);
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t ukv_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    return batch_upsert(keys, values, sizes);
}

operation_result_t ukv_t::range_select(key_t key, size_t length, values_span_t values) const {
    status_t status;
    arena_t arena(db_.get());
    keys_stream_t stream(db_.get()->to_ukv(), collection_, length);
    ukv_byte_t* value = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_length_t* offsets = nullptr;
    size_t found_cnt = 0;
    status = stream.seek(key);
    if(status)
        return {0, operation_status_t::error_k};
    auto batch = stream.keys_batch();
    ukv_collection_t coll = collection_;
    
    ukv_read_t read{
        .db = db_.get()->to_ukv(),
        .error = status.member_ptr(),
        .transaction = nullptr,
        .snapshot = 0,
        .arena = arena.member_ptr(),
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = batch.size(),
        .collections = &coll,
        .collections_stride = 0,
        .keys = reinterpret_cast<ukv_key_t const*>(batch.begin()),
        .keys_stride = sizeof(ukv_key_t),
        .presences = nullptr,
        .offsets = &offsets,
        .lengths = &lengths,
        .values = &value,
    };
    ukv_read(&read);
    while(lengths[found_cnt - 1] != ukv_length_missing_k)
        ++found_cnt;
    memcpy(values.data(),value, offsets[found_cnt - 1] + lengths[found_cnt - 1]);
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t ukv_t::scan(key_t key, size_t length, value_span_t single_value) const {
    status_t status;
    arena_t arena(db_.get());
    keys_stream_t stream(db_.get()->to_ukv(), collection_, length);
    ukv_byte_t* value = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_length_t* offsets = nullptr;
    size_t found_cnt = 0;
    status = stream.seek(key);
    if(status)
        return {0, operation_status_t::error_k};
    auto batch = stream.keys_batch();
    ukv_collection_t coll = collection_;
    
    ukv_read_t read{
        .db = db_.get()->to_ukv(),
        .error = status.member_ptr(),
        .transaction = nullptr,
        .snapshot = 0,
        .arena = arena.member_ptr(),
        .options = ukv_option_dont_discard_memory_k,
        .tasks_count = batch.size(),
        .collections = &coll,
        .collections_stride = 0,
        .keys = reinterpret_cast<ukv_key_t const*>(batch.begin()),
        .keys_stride = sizeof(ukv_key_t),
        .presences = nullptr,
        .offsets = &offsets,
        .lengths = &lengths,
        .values = &value,
    };
    ukv_read(&read);
    while(lengths[found_cnt - 1] != ukv_length_missing_k)
        ++found_cnt;
    memcpy(single_value.data(),value + offsets[found_cnt - 1], lengths[found_cnt - 1]);
    return {found_cnt, operation_status_t::ok_k};
}

void ukv_t::flush() {
    close();
    open();
}

size_t ukv_t::size_on_disk() const {
    return ucsb::size_on_disk(main_dir_path_);
}

std::unique_ptr<transaction_t> ukv_t::create_transaction() {
    return {};
}

} // namespace ucsb::ukv