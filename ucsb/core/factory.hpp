#pragma once

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

#include "ucsb/unumdb/unumdb.hpp"
#include "ucsb/rocksdb/rocksdb.hpp"
#include "ucsb/leveldb/leveldb.hpp"
#include "ucsb/lmdb/lmdb.hpp"

namespace ucsb {

enum class db_kind_t {
    unknown_k,

    unumdb_k,
    rocksdb_k,
    leveldb_k,
    lmdb_k,
};

struct factory_t {
    db_t* create(db_kind_t kind);
};

db_t* factory_t::create(db_kind_t kind) {
    switch (kind) {
    case db_kind_t::unumdb_k: return new unum::unumdb_t();
    case db_kind_t::rocksdb_k: return new facebook::rocksdb_t();
    case db_kind_t::leveldb_k: return new google::leveldb_t();
    case db_kind_t::lmdb_k: return new symas::lmdb_t();
    default: break;
    }
    return nullptr;
}

db_kind_t parse_db(std::string const& name) {
    db_kind_t dist = db_kind_t::unknown_k;
    if (name == "unumdb")
        return db_kind_t::unumdb_k;
    if (name == "rocksdb")
        return db_kind_t::rocksdb_k;
    if (name == "leveldb")
        return db_kind_t::leveldb_k;
    if (name == "lmdb")
        return db_kind_t::lmdb_k;
    return dist;
}

} // namespace ucsb