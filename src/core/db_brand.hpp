#pragma once

#include "core/types.hpp"
#include "core/db.hpp"

#if UCSB_USE_ROCKSDB
#include "rocksdb/rocksdb.hpp"
#endif

#if UCSB_USE_LEVELDB
#include "leveldb/leveldb.hpp"
#endif

#if UCSB_USE_WIREDTIGER
#include "wiredtiger/wiredtiger.hpp"
#endif

#if UCSB_USE_LMDB
#include "lmdb/lmdb.hpp"
#endif

namespace ucsb {

enum class db_brand_t {
    unknown_k,

    rocksdb_k,
    leveldb_k,
    wiredtiger_k,
    lmdb_k,
};

std::shared_ptr<db_t> make_db(db_brand_t db_brand, bool transactional) {
    if (transactional) {
        switch (db_brand) {
#if UCSB_USE_ROCKSDB
        case db_brand_t::rocksdb_k: return std::make_shared<facebook::rocksdb_t>(facebook::db_mode_t::transactional_k);
#endif
        default: break;
        }
    }
    else {
        switch (db_brand) {
#if UCSB_USE_ROCKSDB
        case db_brand_t::rocksdb_k: return std::make_shared<facebook::rocksdb_t>(facebook::db_mode_t::regular_k);
#endif
#if UCSB_USE_LEVELDB
        case db_brand_t::leveldb_k: return std::make_shared<google::leveldb_t>();
#endif
#if UCSB_USE_WIREDTIGER
        case db_brand_t::wiredtiger_k: return std::make_shared<mongodb::wiredtiger_t>();
#endif
#if UCSB_USE_LMDB
        case db_brand_t::lmdb_k: return std::make_shared<symas::lmdb_t>();
#endif
        default: break;
        }
    }
    return {};
}

inline db_brand_t parse_db_brand(std::string const& name) {
    if (name == "rocksdb")
        return db_brand_t::rocksdb_k;
    if (name == "leveldb")
        return db_brand_t::leveldb_k;
    if (name == "wiredtiger")
        return db_brand_t::wiredtiger_k;
    if (name == "lmdb")
        return db_brand_t::lmdb_k;
    return db_brand_t::unknown_k;
}

} // namespace ucsb