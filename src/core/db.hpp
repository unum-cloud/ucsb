#pragma once
#include <memory>
#include <set>

#include "src/core/types.hpp"
#include "src/core/data_accessor.hpp"

namespace ucsb
{

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
    struct db_t : public data_accessor_t
    {
        virtual ~db_t() {}

        virtual bool open() = 0;
        virtual bool close() = 0;

        /**
         * @brief Initializes the DB before usage.
         *
         * This function can't be used more than once.
         * Every DB has it's own format for configuration files.
         * LMDB and LevelDB use `.json`, RocksDB uses `.ini`.
         * The internal contents and available settings also differ.
         *
         * @param config_path The path of configuration files.
         * @param dir_path The target directory, where DB should be stored.
         */
        virtual void set_config(fs::path const &config_path, fs::path const &dir_path) = 0;

        /**
         * @brief Removes all the information stored in the DB and deletes the files on disk.
         */
        virtual void destroy() = 0;

        virtual void flush() = 0;

        /**
         * @brief Accumulates the size (in bytes) of all the files the engine persisted on disk.
         */
        virtual size_t size_on_disk() const = 0;

        virtual std::unique_ptr<transaction_t> create_transaction() = 0;
    };

} // namespace ucsb