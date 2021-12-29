#pragma once

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

#include "ucsb/unumdb/unumdb.hpp"
#include "ucsb/rocksdb/rocksdb.hpp"
#include "ucsb/leveldb/leveldb.hpp"
#include "ucsb/wiredtiger/wiredtiger.hpp"
#include "ucsb/lmdb/lmdb.hpp"

namespace ucsb {

enum class db_kind_t {
    unknown_k,

    unumdb_k,
    rocksdb_k,
    leveldb_k,
    wiredtiger_k,
    lmdb_k,
};

struct factory_t {
    inline std::shared_ptr<db_t> create(db_kind_t kind);
};

inline std::shared_ptr<db_t> factory_t::create(db_kind_t kind) {
    switch (kind) {
    case db_kind_t::unumdb_k: return std::make_shared<unum::unumdb_t>();
    case db_kind_t::rocksdb_k: return std::make_shared<facebook::rocksdb_t>();
    case db_kind_t::leveldb_k: return std::make_shared<google::leveldb_t>();
    case db_kind_t::wiredtiger_k: return std::make_shared<mongodb::wiredtiger_t>();
    case db_kind_t::lmdb_k: return std::make_shared<symas::lmdb_t>();
    default: break;
    }
    return {};
}

inline db_kind_t parse_db(std::string const& name) {
    db_kind_t dist = db_kind_t::unknown_k;
    if (name == "unumdb")
        return db_kind_t::unumdb_k;
    if (name == "rocksdb")
        return db_kind_t::rocksdb_k;
    if (name == "leveldb")
        return db_kind_t::leveldb_k;
    if (name == "wiredtiger")
        return db_kind_t::wiredtiger_k;
    if (name == "lmdb")
        return db_kind_t::lmdb_k;
    return dist;
}

} // namespace ucsb