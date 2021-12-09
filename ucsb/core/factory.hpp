#pragma once

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

namespace ucsb {

enum class db_kind_t {
    unknown_k,

    unumdb_k,
};

struct factory_t {
    db_t* create(db_kind_t kind);
};

db_t* factory_t::create(db_kind_t kind) {
    return nullptr;
}

db_kind_t parse_db(std::string const& name) {
    db_kind_t dist = db_kind_t::unknown_k;
    return dist;
}

} // namespace ucsb