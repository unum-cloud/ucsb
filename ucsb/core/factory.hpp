#pragma once

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/unumdb/unumdb.h"

enum class db_kind_t {
    unknown_k,

    unumdb_k,
};

namespace ucsb {

struct factory_t {
    db_t* create(db_kind_t kind, fs::path const& dir_path, fs::path const& config_path);
};

db_t* factory_t::create(db_kind_t kind, fs::path const& dir_path, fs::path const& config_path) {
    if (kind == db_kind_t::unumdb_k)
        return new UnumDB(dir_path, config_path);
    return nullptr;
}

db_kind_t parse_db(std::string const& name) {
    db_kind_t dist = db_kind_t::unknown_k;
    if (name == "unumdb")
        dist = db_kind_t::unumdb_k;
    return dist;
}

} // namespace ucsb