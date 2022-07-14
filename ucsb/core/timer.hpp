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
    enum class state_t {
        stopped_k,
        running_k,
        paused_k,
    };

    inline timer_t(bm::State& bench) : bench_(&bench), state_(state_t::stopped_k) {}

    // Google benchmark timer methods
    inline void pause() {
        bench_->PauseTiming();

        assert(state_ == state_t::running_k);
        recalculate_operations_elapsed_time();
        state_ = state_t::paused_k;
    }
    inline void resume() {
        assert(state_ == state_t::paused_k);
        operations_start_time_ = high_resolution_clock_t::now();
        state_ = state_t::running_k;

        bench_->ResumeTiming();
    }

    // Helper methods to calculate real time statistics
    inline void start() {
        assert(state_ == state_t::stopped_k);
        elapsed_time_ = elapsed_time_t(0);
        operations_elapsed_time_ = elapsed_time_t(0);
        auto now = high_resolution_clock_t::now();
        start_time_ = now;
        operations_start_time_ = now;
        state_ = state_t::running_k;
    }
    inline void stop() {
        assert(state_ == state_t::running_k);
        recalculate_operations_elapsed_time();
        state_ = state_t::stopped_k;
    }
    inline auto operations_elapsed_time() {
        if (state_ == state_t::running_k)
            recalculate_operations_elapsed_time();
        return operations_elapsed_time_;
    }
    inline auto elapsed_time() {
        if (state_ != state_t::stopped_k)
            recalculate_elapsed_time();
        return elapsed_time_;
    }

  private:
    void recalculate_operations_elapsed_time() {
        auto now = high_resolution_clock_t::now();
        operations_elapsed_time_ += elapsed_time_t(now - operations_start_time_);
        operations_start_time_ = now;
    }
    void recalculate_elapsed_time() {
        auto now = high_resolution_clock_t::now();
        elapsed_time_ += elapsed_time_t(now - start_time_);
        start_time_ = now;
    }

    // Bench state
    bm::State* bench_;

    // Note: These are not needed if we were able to get elapsed times from bm::State
    state_t state_;
    //
    time_point_t start_time_;
    elapsed_time_t elapsed_time_;
    //
    time_point_t operations_start_time_;
    elapsed_time_t operations_elapsed_time_;
};

} // namespace ucsb