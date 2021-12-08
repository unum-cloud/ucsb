#pragma once

#include <memory>
#include <format>

#include "types.hpp"
#include "ucsb/core/workload.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/generators/generator.hpp"
#include "ucsb/core/generators/const_generator.hpp"
#include "ucsb/core/generators/counter_generator.hpp"
#include "ucsb/core/generators/uniform_generator.hpp"
#include "ucsb/core/generators/zipfian_generator.hpp"
#include "ucsb/core/generators/scrambled_zipfian_generator.hpp"
#include "ucsb/core/generators/skewed_zipfian_generator.hpp"
#include "ucsb/core/generators/acknowledged_counter_generator.hpp"

namespace ucsb {

struct transaction_t {
    using key_generator_t = std::unique_ptr<generator_gt<key_t>>;
    using value_length_generator_t = std::unique_ptr<generator_gt<value_length_t>>;

    inline transaction_t(workload_t const& workload, db_t& db);

    operation_status_t do_insert();
    operation_status_t do_update();
    operation_status_t do_read();
    operation_status_t do_delete();
    operation_status_t do_batch_read();
    operation_status_t do_range_select();
    operation_status_t do_scan();

  private:
    inline generator_t create_value_length_generator(generator_kind_t kind, value_length_t length);

    workload_t workload_;
    db_t* db_;

    key_generator_t insert_key_sequence_;
    value_length_generator_t value_length_generator_;
};

inline transaction_t::transaction_t(workload_t const& workload, db_t& db)
    : workload_(workload), db_(&db), insert_key_sequence_(nullptr) {

    insert_key_sequence_.reset(new acknowledged_counter_generator_t(workload.records_count));
    value_length_generator_ = create_value_length_generator(workload.value_length_dist, workload.value_length);
}

operation_status_t transaction_t::do_insert() {

    uint64_t key = insert_key_sequence_->generate();
    value_t value = generate_value();
    return db_->insert(key, value);
}

inline transaction_t::value_length_generator_t transaction_t::create_value_length_generator(generator_kind_t kind,
                                                                                            value_length_t length) {
    generator_t generator;
    switch (kind) {
    case generator_kind_t::const_k: generator.reset(new const_generator_t(length)); break;
    case generator_kind_t::uniform_k: generator.reset(new uniform_generator_t(1, length)); break;
    case generator_kind_t::zipfian_k: generator.reset(new zipfian_generator_t(1, length)); break;
    default: throw exception_t(std::format("Unknown field length distribution: {}", int(kind)));
    }
    return generator;
}

} // namespace ucsb