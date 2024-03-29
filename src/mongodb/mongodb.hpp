#pragma once

#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>

#include "src/core/types.hpp"
#include "src/core/db.hpp"
#include "src/core/helper.hpp"

namespace ucsb::mongo {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_t = ucsb::value_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_length_t = ucsb::value_length_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using db_hints_t = ucsb::db_hints_t;
using transaction_t = ucsb::transaction_t;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

/**
 * @brief MongoDB wrapper for the UCSB benchmark.
 * https://github.com/mongodb/mongo-cxx-driver
 */

/*
 * @brief Preallocated buffers used for batch operations.
 * Globals and especially `thread_local`s are a bad practice.
 */

struct oid_hash_t {
    size_t operator()(const bsoncxx::oid& oid) const {
        return std::hash<std::string_view> {}({oid.bytes(), oid.size()});
    }
};
thread_local std::unordered_map<bsoncxx::oid, size_t, oid_hash_t> batch_keys_map;
thread_local bsoncxx::builder::basic::array batch_keys_array;

class mongodb_t : public ucsb::db_t {
  public:
    inline mongodb_t() : inst_(mongocxx::instance {}) {}

    void set_config(fs::path const& config_path,
                    fs::path const& main_dir_path,
                    std::vector<fs::path> const& storage_dir_paths,
                    db_hints_t const& hints) override;
    bool open(std::string& error) override;
    void close() override;

    std::string info() override;

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
    fs::path config_path_;
    fs::path main_dir_path_;
    std::vector<fs::path> storage_dir_paths_;

    mongocxx::instance inst_;
    std::unique_ptr<mongocxx::pool> pool_;
    std::string coll_name;
};

static bsoncxx::oid make_oid(key_t key) {
    size_t len_k = bsoncxx::oid::size();
    char padded_key[len_k] = {0};
    memcpy(padded_key + len_k - sizeof(key_t), &key, sizeof(key_t));
    return bsoncxx::oid {&padded_key[0], len_k};
}

static bsoncxx::types::b_binary make_binary(auto value, size_t size) {
    bsoncxx::types::b_binary bin_val;
    bin_val.sub_type = bsoncxx::binary_sub_type::k_binary;
    bin_val.bytes = reinterpret_cast<uint8_t const*>(value);
    bin_val.size = size;
    return bin_val;
}

static void exec_cmd(const char* cmd) {
    using namespace std::chrono_literals;
    FILE* pipe = popen(cmd, "r");
    if (!pipe)
        throw std::runtime_error("popen() failed!");
    std::this_thread::sleep_for(2s);
}

void mongodb_t::set_config(fs::path const& config_path,
                           fs::path const& main_dir_path,
                           std::vector<fs::path> const& storage_dir_paths,
                           [[maybe_unused]] db_hints_t const& hints) {
    config_path_ = config_path;
    main_dir_path_ = main_dir_path;
    storage_dir_paths_ = storage_dir_paths;
    coll_name = main_dir_path.parent_path().filename();
};

bool mongodb_t::open(std::string& error) {

    if (!storage_dir_paths_.empty()) {
        error = "Doesn't support multiple disks";
        return false;
    }

    std::string start_cmd = "mongod --config ";
    start_cmd += config_path_;
    exec_cmd(start_cmd.c_str());
    pool_ = std::make_unique<mongocxx::pool>(mongocxx::uri {"mongodb://127.0.0.1:27017/?minPoolSize=1&maxPoolSize=64"});
    return true;
}

void mongodb_t::close() {
    batch_keys_array.clear();
    batch_keys_map.clear();
    std::string stop_cmd = "sudo mongod -f ";
    stop_cmd += config_path_;
    stop_cmd += " --shutdown";
    exec_cmd(stop_cmd.c_str());
}

operation_result_t mongodb_t::upsert(key_t key, value_spanc_t value) {
    auto client = (*pool_).acquire();
    auto coll = (*client)["mongodb"][coll_name];
    auto bin_val = make_binary(value.data(), value.size());
    mongocxx::options::update opts;
    opts.upsert(true);
    if (coll.update_one(make_document(kvp("_id", make_oid(key))),
                        make_document(kvp("$set", make_document(kvp("data", bin_val)))),
                        opts)
            ->modified_count())
        return {1, operation_status_t::ok_k};
    return {0, operation_status_t::error_k};
}

operation_result_t mongodb_t::update(key_t key, value_spanc_t value) {
    auto client = (*pool_).acquire();
    auto coll = (*client)["mongodb"][coll_name];
    // TODO: Do we need upsert here?
    mongocxx::options::update opts;
    opts.upsert(true);
    auto bin_val = make_binary(value.data(), value.size());
    if (coll.update_one(make_document(kvp("_id", make_oid(key))),
                        make_document(kvp("$set", make_document(kvp("data", bin_val)))),
                        opts)
            ->modified_count())
        return {1, operation_status_t::ok_k};
    return {0, operation_status_t::error_k};
};

operation_result_t mongodb_t::remove(key_t key) {
    auto client = (*pool_).acquire();
    auto coll = (*client)["mongodb"][coll_name];
    if (coll.delete_one(make_document(kvp("_id", make_oid(key))))->deleted_count())
        return {1, operation_status_t::ok_k};
    return {0, operation_status_t::not_found_k};
};

operation_result_t mongodb_t::read(key_t key, value_span_t value) const {
    auto client = (*pool_).acquire();
    auto coll = (*client)["mongodb"][coll_name];
    bsoncxx::stdx::optional<bsoncxx::document::value> doc = coll.find_one(make_document(kvp("_id", make_oid(key))));
    if (!doc)
        return {0, operation_status_t::not_found_k};
    auto data = (*doc).view()["data"].get_binary();
    memcpy(value.data(), data.bytes, data.size);
    return {1, operation_status_t::ok_k};
}

operation_result_t mongodb_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    auto client = (*pool_).acquire();
    auto coll = (*client)["mongodb"][coll_name];
    auto bulk = mongocxx::bulk_write(coll.create_bulk_write());
    size_t data_offset = 0;
    for (size_t index = 0; index < keys.size(); index++) {
        auto bin_val = make_binary(values.data() + data_offset, sizes[index]);
        bsoncxx::document::value doc1 = make_document(kvp("_id", make_oid(keys[index])));
        bsoncxx::document::value doc2 = make_document(kvp("$set", make_document(kvp("data", bin_val))));
        mongocxx::model::update_one upsert_op {doc1.view(), doc2.view()};
        upsert_op.upsert(true);
        bulk.append(upsert_op);
        data_offset += sizes[index];
    }
    size_t modified_count = bulk.execute()->modified_count();
    if (modified_count == keys.size())
        return {keys.size(), operation_status_t::ok_k};
    return {0, operation_status_t::error_k};
}

operation_result_t mongodb_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    batch_keys_map.reserve(keys.size());

    for (size_t index = 0; index < keys.size(); index++) {
        auto oid = make_oid(keys[index]);
        batch_keys_map.emplace(oid, index);
        batch_keys_array.append(oid);
    }

    size_t found_cnt = 0;

    auto client = (*pool_).acquire();
    auto coll = (*client)["mongodb"][coll_name];
    auto cursor = coll.find(make_document(kvp("_id", make_document(kvp("$in", batch_keys_array)))));

    for (auto&& doc : cursor) {
        found_cnt++;
        auto key = doc["_id"].get_oid().value;
        auto data = doc["data"].get_binary();
        auto idx = batch_keys_map[key];
        memcpy(&values[idx], data.bytes, data.size);
    }

    batch_keys_array.clear();
    batch_keys_map.clear();

    if (found_cnt == keys.size())
        return {keys.size(), operation_status_t::ok_k};

    return {0, operation_status_t::error_k};
}

operation_result_t mongodb_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    auto client = (*pool_).acquire();
    auto coll = (*client)["mongodb"][coll_name];
    auto bulk = mongocxx::bulk_write(coll.create_bulk_write());
    size_t data_offset = 0;
    for (size_t index = 0; index < keys.size(); index++) {
        auto bin_val = make_binary(values.data() + data_offset, sizes[index]);
        bsoncxx::document::value doc = make_document(kvp("_id", make_oid(keys[index])), kvp("data", bin_val));
        mongocxx::model::insert_one insert_op {doc.view()};
        bulk.append(insert_op);
        data_offset += sizes[index];
    }

    size_t inserted_count = bulk.execute()->inserted_count();
    if (inserted_count == keys.size())
        return {keys.size(), operation_status_t::ok_k};
    return {0, operation_status_t::error_k};
}

operation_result_t mongodb_t::range_select(key_t key, size_t length, [[maybe_unused]] values_span_t values) const {
    size_t i = 0;
    auto client = (*pool_).acquire();
    auto coll = (*client)["mongodb"][coll_name];
    mongocxx::options::find opts;
    opts.limit(length);
    auto cursor = coll.find(make_document(kvp("_id", make_document(kvp("$gt", make_oid(key))))), opts);

    if (cursor.begin() == cursor.end())
        return {0, operation_status_t::error_k};

    for (auto&& doc : cursor) {
        (void)doc;
        i++;
    }

    return {i, operation_status_t::ok_k};
}

operation_result_t mongodb_t::scan([[maybe_unused]] key_t key, size_t length, value_span_t single_value) const {
    auto client = (*pool_).acquire();
    auto coll = (*client)["mongodb"][coll_name];
    auto cursor = coll.find({});
    size_t i = 0;
    for (auto doc = cursor.begin(); doc != cursor.end() && i++ < length; doc++) {
        auto data = (*doc)["data"].get_binary();
        memcpy(single_value.data(), data.bytes, data.size);
    }
    return {i, operation_status_t::ok_k};
}

std::string mongodb_t::info() { return {}; }

void mongodb_t::flush() {}

size_t mongodb_t::size_on_disk() const { return ucsb::size_on_disk(main_dir_path_); }

std::unique_ptr<transaction_t> mongodb_t::create_transaction() { return {}; }

} // namespace ucsb::mongo