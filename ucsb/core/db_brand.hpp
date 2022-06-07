#pragma once

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

#include "ucsb/unumdb/unumdb.hpp"
#include "ucsb/rocksdb/rocksdb.hpp"
#include "ucsb/leveldb/leveldb.hpp"
#include "ucsb/wiredtiger/wiredtiger.hpp"
#include "ucsb/lmdb/lmdb.hpp"
#include "ucsb/mongodb/mongodb.hpp"
#include "ucsb/redis/redis.hpp"

namespace ucsb {

enum class db_brand_t {
    unknown_k,

    unumdb_k,
    rocksdb_k,
    leveldb_k,
    wiredtiger_k,
    lmdb_k,
    mongodb_k,
    redis_k,
};

inline std::shared_ptr<db_t> make_db(db_brand_t db_brand, bool transactional) {
    if (transactional) {
        switch (db_brand) {
        case db_brand_t::unumdb_k: {
            init_file_io_by_posix(darray_gt<string_t> {"./"}); // Note: Temporary solution
            return std::make_shared<unum::unumdb_t>();
        }
        case db_brand_t::rocksdb_k: return std::make_shared<facebook::rocksdb_t>(facebook::db_mode_t::transactional_k);
        default: break;
        }
    }
    else {
        switch (db_brand) {
        case db_brand_t::unumdb_k: {
            init_file_io_by_posix(darray_gt<string_t> {"./"}); // Note: Temporary solution
            return std::make_shared<unum::unumdb_t>();
        }
        case db_brand_t::rocksdb_k: return std::make_shared<facebook::rocksdb_t>(facebook::db_mode_t::regular_k);
        case db_brand_t::leveldb_k: return std::make_shared<google::leveldb_t>();
        case db_brand_t::wiredtiger_k: return std::make_shared<mongodb::wiredtiger_t>();
        case db_brand_t::lmdb_k: return std::make_shared<symas::lmdb_t>();
        case db_brand_t::mongodb_k: return std::make_shared<mongo::mongodb_t>();
        case db_brand_t::redis_k: return std::make_shared<redis::redis_t>();
        default: break;
        }
    }
    return {};
}

inline db_brand_t parse_db_brand(std::string const& name) {
    if (name == "unumdb")
        return db_brand_t::unumdb_k;
    if (name == "rocksdb")
        return db_brand_t::rocksdb_k;
    if (name == "leveldb")
        return db_brand_t::leveldb_k;
    if (name == "wiredtiger")
        return db_brand_t::wiredtiger_k;
    if (name == "lmdb")
        return db_brand_t::lmdb_k;
    if (name == "mongodb")
        return db_brand_t::mongodb_k;
    if (name == "redis")
        return db_brand_t::redis_k;
    return db_brand_t::unknown_k;
}

} // namespace ucsb