#pragma once

#pragma once

#include <cassert>
#include <unordered_map>

#include "ucsb/core/factory.hpp"

namespace ucsb {

struct storage_t {
    void add(db_kind_t kind, db_t* db);
    db_t* get(db_kind_t kind);
    void delete_all();

  private:
    std::unordered_map<db_kind_t, db_t*> map_;
};

void storage_t::add(db_kind_t kind, db_t* db) {
    assert(db != nullptr);
    auto it = map_.find(kind);
    assert(it == map_.end());
    map_.insert({kind, db});
}

db_t* storage_t::get(db_kind_t kind) {
    auto it = map_.find(kind);
    if (it != map_.end())
        return it.operator->second;
    return nullptr;
}

void storage_t::delete_all() {
    for (auto db : map_)
        delete db;
    map_.clear();
}

} // namespace ucsb