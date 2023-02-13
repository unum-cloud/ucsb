#include <cassert>

#include <ukv/cpp/ukv.hpp>
#include <ukv/cpp/blobs_collection.hpp>

#include "src/core/db.hpp"
#include "src/core/helper.hpp"
#include "src/core/types.hpp"

namespace ukv {

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
    ukv_t(ukv_str_view_t config = nullptr, ) : db_(nullptr), config_(config), arena_(nullptr) {}
    ~ukv_t();
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
    std::unique_ptr<database_t> db_;
    blobs_collection_t collection_;
    ukv_str_view_t config_;

    status_t status_;
    arena_t arena_;
};

bool ukv_t::open() {
    if (db_)
        return true;
    status_ = db_->open(config_);
    if (status_)
        return !status_;

    collection_ = db.main();
    return true;
}

bool ukv_t::close() {
    db_.reset(nullptr);
    return true;
}

void ukv_t::destroy() {
    status_ = db_->clear();
    assert(status_);
}

operation_result_t ukv_t::upsert(key_t key, value_spanc_t value) {
    status_ = collection_[key].assign(value);
    return {size_t(status_), status_ ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::update(key_t key, value_spanc_t value) {
    status_ = collection_[key];
    if (status_)
        return {0, operation_status_t::not_found_k};

    status_ = collection_[key].assign(value);
    return {size_t(status_), status_ ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::remove(key_t key) {
    status_ = status_ = collection_[key].erase();
    return {size_t(status), status ? operation_status_t::ok_k : operation_status_t::error_k};
}

} // namespace ukv