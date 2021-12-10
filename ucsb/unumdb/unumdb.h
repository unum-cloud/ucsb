#pragma once

#include <vector>
#include <fmt/format.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

#include "diskkv/region.hpp"

namespace unum {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_span_t = ucsb::keys_span_t;
using value_span_t = ucsb::value_span_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

using fingerprint_t = key_t;
using region_t = region_gt<key_t, data_source_t::unfixed_size_k>;
using building_schema_t = building_schema_gt<fingerprint_t>;
using street_schema_t = street_schema_gt<fingerprint_t>;
using city_schema_t = city_schema_gt<fingerprint_t>;
using region_schema_t = region_schema_gt<fingerprint_t>;

struct unumdb_t : public ucsb::db_t {
  public:
    inline unumdb_t() : name_("Kovkas"), region_(region_config_t()) {}
    ~unumdb_t() override;

    bool init(fs::path const& config_path, fs::path const& dir_path) override;
    void destroy() override;

    operation_result_t insert(key_t key, value_span_t value) override;
    operation_result_t update(key_t key, value_span_t value) override;
    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t remove(key_t key) override;
    operation_result_t batch_read(keys_span_t keys) const override;
    operation_result_t range_select(key_t key, size_t length, value_span_t single_value) const override;
    operation_result_t scan(value_span_t single_value) const override;

  private:
    struct db_config_t {
        region_config_t region_config;
        size_t uring_queue_depth = 0;
    };

    void save(region_config_t const& config, region_schema_t const& schema, string_t const& name);
    bool load(region_config_t& config, region_schema_t& schema, string_t const& name);
    void unumdb_t::dump_fingerprint(fingerprint_t const& fingerprint, std::vector<uint8_t>& data);
    void unumdb_t::parse_fingerprint(std::vector<uint8_t> const& data, fingerprint_t& fingerprint);
    bool load_config(db_config_t& db_config);

    string_t name_;
    fs::path config_path_;
    fs::path dir_path_;
    region_t region_;
    dbuffer_t batch_buffer_;
};

unumdb_t::~unumdb_t() {
    if (region_.population_count()) {
        region_config_t config = region_.get_config();
        region_schema_t schema = region_.get_schema();
        save(config, schema, name_);
    }
}

bool unumdb_t::init(fs::path const& config_path, fs::path const& dir_path) {

    config_path_ = config_path;
    dir_path_ = dir_path;

    db_config_t db_config;
    if (!load_config(db_config))
        return false;

    init_file_io_by_pulling(dir_path.c_str(), db_config.uring_queue_depth);

    region_config_t config;
    region_schema_t schema;
    if (load(config, schema, name_))
        region_ = std::move(region_t(config, schema));
    else
        region_ = std::move(region_t(db_config.region_config));

    return true;
}

void unumdb_t::destroy() {
    region_.destroy();
    std::string config_path = fmt::format("{}{}.cfg", dir_path_.c_str(), name_.c_str());
    std::string schema_path = fmt::format("{}{}.sch", dir_path_.c_str(), name_.c_str());
    fs::remove(config_path);
    fs::remove(schema_path);
}

operation_result_t unumdb_t::insert(key_t key, value_span_t value) {
    citizenc_t citizen {reinterpret_cast<byte_t*>(value.data()), value.size()};
    region_.insert(key, citizen);
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::update(key_t key, value_span_t value) {
    return insert(key, value);
}

operation_result_t unumdb_t::read(key_t key, value_span_t value) const {
    citizen_location_t location;
    region_.find(key, location);
    if (!location)
        return {1, operation_status_t::not_found_k};

    countdown_t countdown;
    notifier_t read_notifier(countdown);
    citizen_t citizen {reinterpret_cast<byte_t*>(value.data()), value.size()};
    region_.select<caching_t::io_k>(location, citizen, read_notifier);
    if (!countdown.wait())
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::remove(key_t key) {
    region_.remove(key);
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::batch_read(keys_span_t keys) const {
    size_t batch_size = keys.size();
    darray_gt<fingerprint_t> fingerprints;
    fingerprints.reserve(batch_size);
    for (const auto& key : keys)
        fingerprints.push_back(key);
    size_t batch_buffer_size = 0;
    darray_gt<citizen_location_t> locations(batch_size);
    region_.find(fingerprints.view(), locations.span(), batch_buffer_size);

    if (!batch_buffer_size)
        return {batch_size, operation_status_t::not_found_k};

    if (batch_buffer_size > batch_buffer_.size())
        batch_buffer_ = dbuffer_t(batch_buffer_size);
    countdown_t countdown(locations.size());
    notifier_t notifier(countdown);
    region_.select(locations.view(), {batch_buffer_.span()}, notifier);
    if (!countdown.wait())
        return {0, operation_status_t::error_k};

    return {batch_size, operation_status_t::ok_k};
}

operation_result_t unumdb_t::range_select(key_t key, size_t length, value_span_t single_value) const {
    countdown_t countdown;
    citizen_t citizen {reinterpret_cast<byte_t*>(single_value.data()), single_value.size()};
    size_t selected_records_count = 0;
    auto it = region_.find(key);
    for (size_t i = 0; it != region_.end() && i < length; ++i, ++it) {
        if (!it.is_removed()) {
            countdown.reset(1);
            it.get(citizen, countdown);
            countdown.wait();
        }
        ++selected_records_count;
    }
    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t unumdb_t::scan(value_span_t single_value) const {
    countdown_t countdown;
    citizen_t citizen {reinterpret_cast<byte_t*>(single_value.data()), single_value.size()};
    size_t scaned_records_count = 0;
    auto it = region_.begin();
    for (; it != region_.end(); ++it) {
        if (!it.is_removed()) {
            countdown.reset(1);
            it.get(citizen, countdown);
            countdown.wait();
        }
        ++scaned_records_count;
    }
    return {scaned_records_count, operation_status_t::ok_k};
}

void unumdb_t::save(region_config_t const& config, region_schema_t const& schema, string_t const& name) {
    // Save config
    auto const& countriy = config;
    auto const& city = countriy.city;
    nlohmann::json j_city = {{"name", city.name.c_str()},
                             {"fixed_citizen_size", city.fixed_citizen_size},
                             {"files_size_enlarge_factor", city.files_size_enlarge_factor},
                             {"street",
                              {{"fixed_citizen_size", city.street.fixed_citizen_size},
                               {"max_files_cnt", city.street.max_files_cnt},
                               {"files_count_enlarge_factor", city.street.files_count_enlarge_factor},
                               {"building", {{"fixed_citizen_size", city.street.building.fixed_citizen_size}}}}}};

    dbuffer_t uuid_buffer(uuid_t::printed_length_k + 1);
    uuid_buffer.refill_with_zeros();
    auto span = uuid_buffer.span().template cast<char, uuid_t::printed_length_k>();

    countriy.uuid.print(span);
    string_t str_uuid(span.data());
    nlohmann::json j_config = {
        {"uuid", str_uuid.c_str()},
        {"fixed_citizen_size", countriy.fixed_citizen_size},
        {"migration_capacity", countriy.migration_capacity},
        {"migration_max_cnt", countriy.migration_max_cnt},
        {"city", j_city},
    };

    std::string std_dir_path(dir_path_.c_str());
    string_t config_path = string_t::format("{}{}.cfg", std_dir_path, name);
    std::ofstream o_cfg(config_path.c_str());
    o_cfg << std::setw(4) << j_config << std::endl;

    // Save schema
    nlohmann::json j_schema;
    {
        auto const& countriy = schema;
        auto const& city = countriy.city;
        nlohmann::json j_region, j_city, j_streets;

        for (size_t i = 0; i < city.streets.size(); ++i) {
            auto const& street = city.streets[i];
            std::vector<uint8_t> lower_data;
            std::vector<uint8_t> upper_data;
            dump_fingerprint(street.lower_fingerprint, lower_data);
            dump_fingerprint(street.upper_fingerprint, upper_data);
            nlohmann::json j_street = {{"lower_fingerprint", lower_data}, {"upper_fingerprint", upper_data}};
            nlohmann::json j_buildings;
            for (size_t j = 0; j < street.buildings.size(); ++j) {
                auto const& building = street.buildings[j];
                lower_data.clear();
                upper_data.clear();
                dump_fingerprint(building.lower_fingerprint, lower_data);
                dump_fingerprint(building.upper_fingerprint, upper_data);
                auto j_building = nlohmann::json::object({{"file_name", building.file_name.c_str()},
                                                          {"lower_fingerprint", lower_data},
                                                          {"upper_fingerprint", upper_data}});
                j_buildings.push_back(j_building);
            }
            j_street["buildings"] = j_buildings;
            j_streets.push_back(j_street);
        }

        j_city["streets"] = j_streets;
        j_schema["city"] = j_city;
    }

    string_t schema_path = string_t::format("{}{}.sch", std_dir_path, name);
    std::ofstream o_sch(schema_path.c_str());
    o_sch << std::setw(4) << j_schema << std::endl;
}

bool unumdb_t::load(region_config_t& config, region_schema_t& schema, string_t const& name) {

    config = region_config_t();
    schema = region_schema_t();

    string_t config_path = string_t::format("{}{}.cfg", dir_path_.c_str(), name);
    string_t schema_path = string_t::format("{}{}.sch", dir_path_.c_str(), name);
    if (!jbod->exists(config_path) || !jbod->exists(schema_path))
        return false;

    // Load config
    std::ifstream i_cfg(config_path.c_str());
    nlohmann::json j_config;
    i_cfg >> j_config;

    region_config_t country;
    string_t str_uuid = string_t(j_config["uuid"].get<std::string>().c_str());
    auto span = str_uuid.view().template cast<const byte_t, uuid_t::printed_length_k>();
    country.uuid.print_parse(span);
    country.fixed_citizen_size = j_config["fixed_citizen_size"].get<citizen_size_t>();
    country.migration_capacity = j_config["migration_capacity"].get<size_t>();
    country.migration_max_cnt = j_config["migration_max_cnt"].get<size_t>();

    country.city.name = string_t(j_config["city"]["name"].get<std::string>().c_str());
    country.city.fixed_citizen_size = j_config["city"]["fixed_citizen_size"].get<citizen_size_t>();
    country.city.files_size_enlarge_factor = j_config["city"]["files_size_enlarge_factor"].get<size_t>();
    country.city.street.fixed_citizen_size = j_config["city"]["street"]["fixed_citizen_size"].get<citizen_size_t>();
    country.city.street.max_files_cnt = j_config["city"]["street"]["max_files_cnt"].get<size_t>();
    country.city.street.files_count_enlarge_factor =
        j_config["city"]["street"]["files_count_enlarge_factor"].get<size_t>();
    country.city.street.building.fixed_citizen_size =
        j_config["city"]["street"]["building"]["fixed_citizen_size"].get<citizen_size_t>();
    config = country;

    // Load schema
    std::ifstream i_sch(schema_path.c_str());
    nlohmann::json j_schema;
    i_sch >> j_schema;

    {
        nlohmann::json j_city = j_schema["city"];
        region_schema_t country;
        city_schema_t city;

        nlohmann::json j_streets = j_city["streets"];
        for (auto j_street = j_streets.begin(); j_street != j_streets.end(); ++j_street) {
            street_schema_t street;
            std::vector<uint8_t> lower_data = (*j_street)["lower_fingerprint"].get<std::vector<uint8_t>>();
            std::vector<uint8_t> upper_data = (*j_street)["upper_fingerprint"].get<std::vector<uint8_t>>();
            parse_fingerprint(lower_data, street.lower_fingerprint);
            parse_fingerprint(upper_data, street.upper_fingerprint);
            nlohmann::json j_buildings = (*j_street)["buildings"];
            for (auto j_building = j_buildings.begin(); j_building != j_buildings.end(); ++j_building) {
                building_schema_t building;
                building.file_name = string_t((*j_building)["file_name"].get<std::string>().c_str());
                lower_data = (*j_building)["lower_fingerprint"].get<std::vector<uint8_t>>();
                upper_data = (*j_building)["upper_fingerprint"].get<std::vector<uint8_t>>();
                parse_fingerprint(lower_data, building.lower_fingerprint);
                parse_fingerprint(upper_data, building.upper_fingerprint);
                street.buildings.push_back(building);
            }
            city.streets.push_back(street);
        }
        country.city = city;
        schema = country;
    }

    return true;
}

bool unumdb_t::load_config(db_config_t& db_config) {
    if (!fs::exists(config_path_.c_str()))
        return false;

    std::ifstream i_config(config_path_);
    nlohmann::json j_config;
    i_config >> j_config;

    db_config.region_config.uuid = unum::algo::rand::uuid4_t {}();
    db_config.region_config.fixed_citizen_size = 0;
    db_config.region_config.migration_capacity = j_config["migration_capacity"].get<size_t>();
    db_config.region_config.migration_max_cnt = j_config["migration_max_cnt"].get<size_t>();

    db_config.region_config.city.name = name_;
    db_config.region_config.city.fixed_citizen_size = 0;
    db_config.region_config.city.files_size_enlarge_factor = j_config["files_size_enlarge_factor"].get<size_t>();

    db_config.region_config.city.street.fixed_citizen_size = 0;
    db_config.region_config.city.street.max_files_cnt = j_config["max_files_cnt"].get<size_t>();
    db_config.region_config.city.street.files_count_enlarge_factor =
        j_config["files_count_enlarge_factor"].get<size_t>();
    db_config.region_config.city.street.building.fixed_citizen_size = 0;

    db_config.uring_queue_depth = j_config["uring_queue_depth"].get<size_t>();

    return true;
}

void unumdb_t::dump_fingerprint(fingerprint_t const& fingerprint, std::vector<uint8_t>& data) {

    serialize_gt<fingerprint_t> serializer;
    data.resize(serializer.dump_length(fingerprint), 0);
    serializer.dump(fingerprint, {reinterpret_cast<byte_t*>(data.data()), data.size()});
}

void unumdb_t::parse_fingerprint(std::vector<uint8_t> const& data, fingerprint_t& fingerprint) {

    serialize_gt<fingerprint_t> serializer;
    serializer.dump_parse({reinterpret_cast<byte_t const*>(data.data()), data.size()}, fingerprint);
}

} // namespace unum