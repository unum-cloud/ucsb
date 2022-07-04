#pragma once

#include <chrono>
#include <benchmark/benchmark.h>

namespace bm = benchmark;

namespace ucsb {

using high_resolution_clock_t = std::chrono::high_resolution_clock;
using time_point_t = std::chrono::time_point<high_resolution_clock_t>;
using elapsed_time_t = std::chrono::duration<double>;

/**
 * @brief Trivial Google Benchmark wrapper.
 * No added value here :)
 */
struct timer_t {
    inline timer_t(bm::State& state) : state_(&state), operations_paused_(true) {}

    // Google benchmark timer methods
    inline void pause() {
        state_->PauseTiming();

        assert(!operations_paused_);
        recalculate_elapsed_time();
        operations_paused_ = true;
    }
    inline void resume() {
        assert(operations_paused_);
        operations_start_time_ = high_resolution_clock_t::now();
        operations_paused_ = false;

        state_->ResumeTiming();
    }

    // Helper methods to calculate real time statistics
    inline void start() {
        assert(operations_paused_);
        elapsed_time_ = elapsed_time_t(0);
        operations_elapsed_time_ = elapsed_time_t(0);
        auto now = high_resolution_clock_t::now();
        start_time_ = now;
        operations_start_time_ = now;
        operations_paused_ = false;
    }
    inline auto operations_elapsed_time() {
        if (!operations_paused_)
            recalculate_elapsed_time();
        return operations_elapsed_time_;
    }
    inline auto elapsed_time() {
        auto now = high_resolution_clock_t::now();
        return elapsed_time_t(now - start_time_);
    }

  private:
    void recalculate_elapsed_time() {
        auto now = high_resolution_clock_t::now();
        operations_elapsed_time_ += elapsed_time_t(now - operations_start_time_);
        operations_start_time_ = now;
    }

    // Bench state
    bm::State* state_;

    // Note: These are not needed if we were able to get elapsed times from bm::State
    time_point_t start_time_;
    elapsed_time_t elapsed_time_;
    //
    time_point_t operations_start_time_;
    elapsed_time_t operations_elapsed_time_;
    bool operations_paused_;
};

} // namespace ucsb