#pragma once

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/unumdb/unumdb.h"

enum class db_kind_t {
    unumdb_k,
};

namespace ucsb {

struct factory_t {
    db_t* create(db_kind_t kind, fs::path const& dir_path, fs::path const& config_file_path);
};

db_t* factory_t::create(db_kind_t kind, fs::path const& dir_path, fs::path const& config_file_path) {
    if (kind == db_kind_t::unumdb_k)
        return new UnumDB(dir_path, config_file_path);
    return nullptr;
}

} // namespace ucsb