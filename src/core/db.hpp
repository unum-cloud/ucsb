#pragma once

#include <set>
#include <string>
#include <memory>

#include "src/core/types.hpp"
#include "src/core/db_hint.hpp"
#include "src/core/data_accessor.hpp"

namespace ucsb {

using transaction_t = data_accessor_t;

/**
 * @brief A base class for benchmarking key-value stores.
 * This doesn't apply to transactional benchmarks.
 *
 * General usage procedure is the following:
 * 1. Configure `.set_config` and a custom file,
 * 2. Recover the state from disk via `.open`,
 * 3. Perform all the needed benchmarks,
 * 4. Flush the state via `.close`,
 * 5. Remove all the data, if needed, via `.destroy`.
 */
class db_t : public data_accessor_t {
  public:
    virtual ~db_t() {}

    virtual bool open(std::string& error) = 0;
    virtual void close() = 0;

    /**
     * @brief Returns high level description about the DB
     */
    virtual std::string info() = 0;

    /**
     * @brief Initializes the DB before usage.
     *
     * This function can't be used more than once.
     * Every DB has it's own format for configuration files.
     * LMDB and LevelDB use `.json`, RocksDB uses `.ini`.
     * The internal contents and available settings also differ.
     *
     * @param config_path The path of configuration files.
     * @param main_dir_path The target directory, where DB should store metadata.
     * @param storage_dir_paths The target directories, where DB should store data.
     * @param hints hints for the DB to prepare to work better.
     */
    virtual void set_config(fs::path const& config_path,
                            fs::path const& main_dir_path,
                            std::vector<fs::path> const& storage_dir_paths,
                            db_hints_t const& hints) = 0;

    virtual void flush() = 0;

    /**
     * @brief Accumulates the size (in bytes) of all the files the engine persisted on disk.
     */
    virtual size_t size_on_disk() const = 0;

    virtual std::unique_ptr<transaction_t> create_transaction() = 0;
};

} // namespace ucsb