#pragma once

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

#include "ucsb/unumdb/unumdb.h"
#include "ucsb/rocksdb/rocksdb.h"

namespace ucsb {

enum class db_kind_t {
    unknown_k,

    unumdb_k,
    rocksdb_k,
};

struct factory_t {
    db_t* create(db_kind_t kind);
};

db_t* factory_t::create(db_kind_t kind) {
    switch (kind) {
    case db_kind_t::unumdb_k: return new unum::unumdb_t();
    case db_kind_t::rocksdb_k: return new facebook::rocksdb_t();
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
    return dist;
}

} // namespace ucsb