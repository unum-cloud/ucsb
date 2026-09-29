#pragma once

#include <iostream>
#include <atomic>
#include <thread>

namespace ucsb {

/**
 * @brief Synchronization primitive to isolate workers across
 * threads from operation on uninitialized or closing DB.
 */
class threads_fence_t {
  public:
    inline threads_fence_t(size_t threads_count)
        : threads_count_(threads_count), waiting_threads_count_(0), released_threads_count_(0) {}

    inline void sync() {
        while (released_threads_count_.load())
            ;

        ++waiting_threads_count_;
        while (waiting_threads_count_.load() != threads_count_)
            ;

        if (++released_threads_count_ == threads_count_) {
            waiting_threads_count_.store(0);
            released_threads_count_.store(0);
        }
    }

  private:
    size_t const threads_count_;
    std::atomic_size_t waiting_threads_count_;
    std::atomic_size_t released_threads_count_;
};

} // namespace ucsb