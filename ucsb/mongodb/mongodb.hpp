#pragma once

#include <vector>

#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

namespace mongo {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using bulk_metadata_t = ucsb::bulk_metadata_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using transaction_t = ucsb::transaction_t;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static mongocxx::instance instance {};

/**
 * @brief MongoDB wrapper for the UCSB benchmark.
 * https://github.com/mongodb/mongo-cxx-driver
 */
struct mongodb_t : public ucsb::db_t {
  public:
    inline mongodb_t() : pool_(mongocxx::uri {}) {}
    inline ~mongodb_t() { close(); }

    void set_config(fs::path const& config_path, fs::path const& dir_path) override;
    bool open() override;
    bool close() override;
    void destroy() override;

    operation_result_t insert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const override;

    operation_result_t bulk_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;
    operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

    void flush() override;
    size_t size_on_disk() const override;

    std::unique_ptr<transaction_t> create_transaction() override;

  private:
    mongocxx::pool pool_;
    fs::path dir_path_;
    std::string coll_name;
};

void mongodb_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    dir_path_ = dir_path;
    coll_name = dir_path.parent_path().filename();
};

bool mongodb_t::open() {
    return true;
}

bool mongodb_t::close() {
    return true;
}

void mongodb_t::destroy() {
    auto client = pool_.acquire();
    mongocxx::collection coll = mongocxx::collection((*client)["mongodb"][coll_name]);
    coll.drop();
};

operation_result_t mongodb_t::insert(key_t key, value_spanc_t value) {
    auto client = pool_.acquire();
    mongocxx::collection coll = mongocxx::collection((*client)["mongodb"][coll_name]);
    bsoncxx::stdx::string_view val(reinterpret_cast<const char*>(value.data()), value.size());
    coll.insert_one(make_document(kvp("_id", int(key)), kvp("key", val)));
    return {1, operation_status_t::ok_k};
}

operation_result_t mongodb_t::update(key_t key, value_spanc_t value) {
    auto client = pool_.acquire();
    mongocxx::collection coll = mongocxx::collection((*client)["mongodb"][coll_name]);
    bsoncxx::stdx::string_view val(reinterpret_cast<const char*>(value.data()), value.size());
    if (coll.replace_one(make_document(kvp("_id", int(key))), make_document(kvp("_id", int(key)), kvp("key", val)))
            ->modified_count())
        return {1, operation_status_t::ok_k};
    return {0, operation_status_t::error_k};
};

operation_result_t mongodb_t::remove(key_t key) {
    auto client = pool_.acquire();
    mongocxx::collection coll = mongocxx::collection((*client)["mongodb"][coll_name]);
    if (coll.delete_one(make_document(kvp("_id", int(key))))->deleted_count())
        return {1, operation_status_t::ok_k};
    return {1, operation_status_t::not_found_k};
};

operation_result_t mongodb_t::read(key_t key, value_span_t value) const {
    auto client = pool_.acquire();
    mongocxx::collection coll = mongocxx::collection((*client)["mongodb"][coll_name]);
    bsoncxx::stdx::optional<bsoncxx::document::value> doc = coll.find_one(make_document(kvp("_id", int(key))));
    if (!doc)
        return {1, operation_status_t::not_found_k};
    bsoncxx::stdx::string_view data = (*doc).view()["key"].get_utf8();
    memcpy(value.data(), data.data(), data.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t mongodb_t::batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    auto client = pool_.acquire();
    mongocxx::collection coll = mongocxx::collection((*client)["mongodb"][coll_name]);
    std::vector<bsoncxx::document::value> cont;
    size_t data_offset = 0;
    for (size_t index = 0; index < keys.size(); ++index) {
        bsoncxx::stdx::string_view val(reinterpret_cast<char const*>(values.data() + data_offset), sizes[index]);
        data_offset += sizes[index];
        bsoncxx::document::value doc =
            make_document(kvp("_id", int(keys[index])), kvp(std::to_string(keys[index]), val));
        cont.push_back(doc);
    }

    size_t inserted_count = coll.insert_many(cont)->inserted_count();
    if (inserted_count == keys.size())
        return {keys.size(), operation_status_t::ok_k};

    return {inserted_count, operation_status_t::error_k};
}

operation_result_t mongodb_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    return {0, operation_status_t::not_implemented_k};
}

bulk_metadata_t mongodb_t::prepare_bulk_insert_data(keys_spanc_t keys,
                                                    values_spanc_t values,
                                                    value_lengths_spanc_t sizes) const {
    bulk_metadata_t metadata;
    auto client = pool_.acquire();
    mongocxx::collection coll = mongocxx::collection((*client)["mongodb"][coll_name]);
    mongocxx::bulk_write* bulk = new mongocxx::bulk_write(coll.create_bulk_write());
    metadata.records_count = keys.size();
    size_t data_offset = 0;
    for (size_t index = 0; index < keys.size(); ++index) {
        bsoncxx::stdx::string_view val(reinterpret_cast<char const*>(values.data() + data_offset), sizes[index]);
        data_offset += sizes[index];
        bsoncxx::document::value doc = make_document(kvp("_id", int(keys[index])), kvp("key", val));
        mongocxx::model::insert_one insert_op {doc.view()};
        bulk->append(insert_op);
    }
    metadata.data = bulk;
    return metadata;
}

operation_result_t mongodb_t::bulk_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    if (metadata.data == nullptr)
        return {0, operation_status_t::error_k};
    mongocxx::bulk_write* bulk = reinterpret_cast<mongocxx::bulk_write*>(metadata.data);
    size_t inserted_count = bulk->execute()->inserted_count();
    delete bulk;
    if (inserted_count == metadata.records_count)
        return {metadata.records_count, operation_status_t::ok_k};
    return {inserted_count, operation_status_t::error_k};
}

operation_result_t mongodb_t::range_select(key_t key, size_t length, values_span_t values) const {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t mongodb_t::scan(key_t key, size_t length, value_span_t single_value) const {
    // TODO: scan method interface changed so need to reimplement
    return {0, operation_status_t::not_implemented_k};

    // auto client = pool_.acquire();
    // mongocxx::collection coll = mongocxx::collection((*client)["mongodb"][coll_name]);
    // size_t scanned_records_count = 0;
    // for (auto&& doc : coll.find({})) {
    //     std::string_view data = bsoncxx::to_json(doc);
    //     memcpy(single_value.data(), data.data(), data.size());
    //     ++scanned_records_count;
    // }
    // return {scanned_records_count, operation_status_t::ok_k};
}

void mongodb_t::flush() {
    // Nothing to do
}

size_t mongodb_t::size_on_disk() const {
    return ucsb::size_on_disk(dir_path_);
}

std::unique_ptr<transaction_t> mongodb_t::create_transaction() {
    return {};
}

} // namespace mongo