#pragma once

#include "core/types.hpp"
#include "core/db.hpp"

#include "rocksdb/rocksdb.hpp"
#include "leveldb/leveldb.hpp"
#include "wiredtiger/wiredtiger.hpp"
// #include "lmdb/lmdb.hpp"

namespace ucsb
{

    enum class db_brand_t
    {
        unknown_k,

        rocksdb_k,
        leveldb_k,
        wiredtiger_k,
        // lmdb_k,
    };

    std::shared_ptr<db_t> make_db(db_brand_t db_brand, bool transactional)
    {
        if (transactional)
        {
            switch (db_brand)
            {
            case db_brand_t::rocksdb_k:
                return std::make_shared<facebook::rocksdb_t>(facebook::db_mode_t::transactional_k);
            default:
                break;
            }
        }
        else
        {
            switch (db_brand)
            {
            case db_brand_t::rocksdb_k:
                return std::make_shared<facebook::rocksdb_t>(facebook::db_mode_t::regular_k);
            case db_brand_t::leveldb_k:
                return std::make_shared<google::leveldb_t>();
            case db_brand_t::wiredtiger_k:
                return std::make_shared<mongodb::wiredtiger_t>();
            // case db_brand_t::lmdb_k:
            //     return std::make_shared<symas::lmdb_t>();
            default:
                break;
            }
        }
        return {};
    }

    inline db_brand_t parse_db_brand(std::string const &name)
    {
        if (name == "rocksdb")
            return db_brand_t::rocksdb_k;
        if (name == "leveldb")
            return db_brand_t::leveldb_k;
        if (name == "wiredtiger")
            return db_brand_t::wiredtiger_k;
        // if (name == "lmdb")
        //     return db_brand_t::lmdb_k;
        return db_brand_t::unknown_k;
    }

} // namespace ucsb