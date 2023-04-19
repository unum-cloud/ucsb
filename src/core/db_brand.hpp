#pragma once

#include "src/core/types.hpp"
#include "src/core/db.hpp"

#if defined(UCSB_HAS_USTORE)
#include "src/ustore/ustore.hpp"
#endif
#if defined(UCSB_HAS_ROCKSDB)
#include "src/rocksdb/rocksdb.hpp"
#endif
#if defined(UCSB_HAS_LEVELDB)
#include "src/leveldb/leveldb.hpp"
#endif
#if defined(UCSB_HAS_WIREDTIGER)
#include "src/wiredtiger/wiredtiger.hpp"
#endif
#if defined(UCSB_HAS_MONGODB)
#include "src/mongodb/mongodb.hpp"
#endif
#if defined(UCSB_HAS_REDIS)
#include "src/redis/redis.hpp"
#endif
#if defined(UCSB_HAS_LMDB)
#include "src/lmdb/lmdb.hpp"
#endif

namespace ucsb {

enum class db_brand_t {
    unknown_k,

    ustore_k,
    rocksdb_k,
    leveldb_k,
    wiredtiger_k,
    mongodb_k,
    redis_k,
    lmdb_k,
};

std::shared_ptr<db_t> make_db(db_brand_t db_brand, bool transactional) {
    if (transactional) {
        switch (db_brand) {
#if defined(UCSB_HAS_USTORE)
        case db_brand_t::ustore_k: return std::make_shared<ustore::ustore_t>();
#endif
#if defined(UCSB_HAS_ROCKSDB)
        case db_brand_t::rocksdb_k: return std::make_shared<facebook::rocksdb_t>(facebook::db_mode_t::transactional_k);
#endif
        default: break;
        }
    }
    else {
        switch (db_brand) {
#if defined(UCSB_HAS_USTORE)
        case db_brand_t::ustore_k: return std::make_shared<ustore::ustore_t>();
#endif
#if defined(UCSB_HAS_ROCKSDB)
        case db_brand_t::rocksdb_k: return std::make_shared<facebook::rocksdb_t>(facebook::db_mode_t::regular_k);
#endif
#if defined(UCSB_HAS_LEVELDB)
        case db_brand_t::leveldb_k: return std::make_shared<google::leveldb_t>();
#endif
#if defined(UCSB_HAS_WIREDTIGER)
        case db_brand_t::wiredtiger_k: return std::make_shared<mongo::wiredtiger_t>();
#endif
#if defined(UCSB_HAS_MONGODB)
        case db_brand_t::mongodb_k: return std::make_shared<mongo::mongodb_t>();
#endif
#if defined(UCSB_HAS_REDIS)
        case db_brand_t::redis_k: return std::make_shared<redis::redis_t>();
#endif
#if defined(UCSB_HAS_LMDB)
        case db_brand_t::lmdb_k: return std::make_shared<symas::lmdb_t>();
#endif
        default: break;
        }
    }
    return {};
}

inline db_brand_t parse_db_brand(std::string const& name) {
    if (name == "ustore")
        return db_brand_t::ustore_k;
    if (name == "rocksdb")
        return db_brand_t::rocksdb_k;
    if (name == "leveldb")
        return db_brand_t::leveldb_k;
    if (name == "wiredtiger")
        return db_brand_t::wiredtiger_k;
    if (name == "mongodb")
        return db_brand_t::mongodb_k;
    if (name == "redis")
        return db_brand_t::redis_k;
    if (name == "lmdb")
        return db_brand_t::lmdb_k;
    return db_brand_t::unknown_k;
}

} // namespace ucsb