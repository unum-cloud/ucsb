#pragma once

#include <vector>

#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>

#include "src/core/types.hpp"
#include "src/core/db.hpp"
#include "src/core/helper.hpp"

#include <iostream>

#include <algorithm>

namespace mongo
{
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
    using transaction_t = ucsb::transaction_t;

    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    static mongocxx::instance instance{};
    constexpr char kDataBaseName[] = "mongodb";

    /**
     * @brief MongoDB wrapper for the UCSB benchmark.
     * https://github.com/mongodb/mongo-cxx-driver
     */
    struct mongodb_t : public ucsb::db_t
    {
    public:
        inline mongodb_t() : pool_(mongocxx::uri{}) {}
        inline ~mongodb_t() { close(); }

        void set_config(fs::path const &config_path, fs::path const &dir_path) override;
        bool open() override;
        bool close() override;
        void destroy() override;

        operation_result_t insert(key_t key, value_spanc_t value) override;
        operation_result_t update(key_t key, value_spanc_t value) override;
        operation_result_t remove(key_t key) override;

        operation_result_t read(key_t key, value_span_t value) const override;
        operation_result_t batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
        operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const override;

        operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
        operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;
        operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

        void flush() override;
        size_t size_on_disk() const override;
        std::unique_ptr<transaction_t> create_transaction() override;

    private:
        mongocxx::collection get_collection() const;
        mutable mongocxx::pool pool_;
        fs::path dir_path_;
        std::string coll_name;
    };

    static bsoncxx::oid make_oid(key_t key)
    {
        size_t len_k = bsoncxx::oid::size();
        char padded_key[len_k] = {0};
        memcpy(padded_key + len_k - sizeof(key_t), &key, sizeof(key_t));
        return bsoncxx::oid{&padded_key[0], len_k};
    }

    // static int64_t make_oid(key_t key)
    // {
    //     return key;
    // }

    static bsoncxx::types::b_binary make_binary(auto value, size_t size)
    {
        bsoncxx::types::b_binary bin_val;
        bin_val.sub_type = bsoncxx::binary_sub_type::k_binary;
        bin_val.bytes = reinterpret_cast<uint8_t const *>(value);
        bin_val.size = size;
        return bin_val;
    }

    mongocxx::collection mongodb_t::get_collection() const
    {
        auto client = pool_.acquire();
        return mongocxx::collection((*client)[kDataBaseName][coll_name]);
    }

    void mongodb_t::set_config(fs::path const &config_path, fs::path const &dir_path)
    {
        dir_path_ = dir_path;
        coll_name = dir_path.parent_path().filename();
    };

    bool mongodb_t::open()
    {
        return true;
    }

    bool mongodb_t::close()
    {
        return true;
    }

    void mongodb_t::destroy()
    {
        mongocxx::collection coll = this->get_collection();
        coll.drop();
    };

    operation_result_t mongodb_t::insert(key_t key, value_spanc_t value)
    {
        mongocxx::collection coll = this->get_collection();
        auto bin_val = make_binary(value.data(), value.size());
        coll.insert_one(make_document(kvp("_id", make_oid(key)), kvp("data", bin_val)));
        return {1, operation_status_t::ok_k};
    }

    operation_result_t mongodb_t::update(key_t key, value_spanc_t value)
    {
        mongocxx::collection coll = this->get_collection();
        auto bin_val = make_binary(value.data(), value.size());
        if (coll.replace_one(make_document(kvp("_id", make_oid(key))), make_document(kvp("_id", make_oid(key)), kvp("data", bin_val)))
                ->modified_count())
            return {1, operation_status_t::ok_k};
        return {0, operation_status_t::error_k};
    };

    operation_result_t mongodb_t::remove(key_t key)
    {
        mongocxx::collection coll = this->get_collection();
        if (coll.delete_one(make_document(kvp("_id", make_oid(key))))->deleted_count())
            return {1, operation_status_t::ok_k};
        return {1, operation_status_t::not_found_k};
    };

    operation_result_t mongodb_t::read(key_t key, value_span_t value) const
    {
        mongocxx::collection coll = this->get_collection();
        bsoncxx::stdx::optional<bsoncxx::document::value> doc = coll.find_one(make_document(kvp("_id", make_oid(key))));
        if (!doc)
            return {1, operation_status_t::not_found_k};
        auto data = (*doc).view()["data"].get_binary();
        memcpy(value.data(), data.bytes, data.size);
        return {1, operation_status_t::ok_k};
    }

    operation_result_t mongodb_t::batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes)
    {
        mongocxx::collection coll = this->get_collection();
        std::vector<bsoncxx::document::value> cont;
        cont.reserve(keys.size());
        size_t data_offset = 0;
        for (size_t index = 0; index < keys.size(); ++index)
        {
            auto bin_val = make_binary(values.data() + data_offset, sizes[index]);
            data_offset += sizes[index];
            bsoncxx::document::value doc =
                make_document(kvp("_id", make_oid(keys[index])), kvp("data", bin_val));
            cont.push_back(doc);
        }

        size_t inserted_count = coll.insert_many(cont)->inserted_count();
        if (inserted_count == keys.size())
            return {keys.size(), operation_status_t::ok_k};

        return {inserted_count, operation_status_t::error_k};
    }

    // TODO: Not all keys are found. Fix it.
    operation_result_t mongodb_t::batch_read(keys_spanc_t keys, values_span_t values) const
    {
        auto keys_arr = bsoncxx::builder::basic::array{};
        std::set<bsoncxx::oid> input_keys_set;
        size_t found_cnt = 0;

        // ! Keys are const;
        // std::sort(keys.begin(), keys.end());
        // size_t unique_keys_cnt = std::unique(keys.begin(), keys.end) - keys.begin();
        for (const auto &key : keys)
        {
            keys_arr.append(make_oid(key));
            input_keys_set.insert(make_oid(key));
        }
        size_t unique_keys_cnt = input_keys_set.size();

        mongocxx::collection coll = this->get_collection();
        auto cursor = coll.find(
            make_document(
                kvp("_id", make_document(kvp("$in", keys_arr)))));

        // TODO: memcpy values.
        for (auto &&doc : cursor)
            found_cnt++;

        if (found_cnt == unique_keys_cnt)
            return {unique_keys_cnt, operation_status_t::ok_k};

        return {found_cnt, operation_status_t::error_k};
    }

    operation_result_t mongodb_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes)
    {
        mongocxx::collection coll = this->get_collection();
        auto bulk = mongocxx::bulk_write(coll.create_bulk_write());
        size_t data_offset = 0;
        for (size_t index = 0; index < keys.size(); ++index)
        {
            auto bin_val = make_binary(values.data() + data_offset, sizes[index]);
            bsoncxx::document::value doc = make_document(kvp("_id", make_oid(keys[index])), kvp("data", bin_val));
            mongocxx::model::insert_one insert_op{doc.view()};
            bulk.append(insert_op);
            data_offset += sizes[index];
        }

        size_t inserted_count = bulk.execute()->inserted_count();
        if (inserted_count == keys.size())
            return {keys.size(), operation_status_t::ok_k};
        return {inserted_count, operation_status_t::error_k};
    }

    operation_result_t mongodb_t::range_select(key_t key, size_t length, values_span_t values) const
    {
        size_t i = 0;
        mongocxx::collection coll = this->get_collection();
        mongocxx::options::find opts;
        opts.limit(length);
        auto cursor = coll.find(
            make_document(
                kvp("_id", make_document(kvp("$gt", make_oid(key))))),
            opts);

        if (cursor.begin() == cursor.end())
            return {0, operation_status_t::error_k};

        for (auto &&doc : cursor)
            i++;

        return {i, operation_status_t::ok_k};
    }

    operation_result_t mongodb_t::scan(key_t key, size_t length, value_span_t single_value) const
    {
        mongocxx::collection coll = this->get_collection();
        auto cursor = coll.find({});
        size_t i = 0;
        for (auto doc = cursor.begin(); doc != cursor.end() && i++ < length; doc++)
        {
            // bsoncxx::stdx::string_view data = (*doc)["data"].get_utf8();
            auto data = (*doc)["data"].get_binary();
            memcpy(single_value.data(), data.bytes, data.size);
        }
        return {i, operation_status_t::ok_k};
    }

    void mongodb_t::flush()
    {
        // Nothing to do
    }

    size_t mongodb_t::size_on_disk() const
    {
        return ucsb::size_on_disk(dir_path_);
    }

    std::unique_ptr<transaction_t> mongodb_t::create_transaction()
    {
        return {};
    }

} // namespace mongo
