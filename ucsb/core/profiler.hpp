#pragma once

#include <sys/times.h>
#include <unistd.h>
#include <string>
#include <fstream>
#include <limits>
#include <chrono>
#include <thread>
#include <atomic>

namespace ucsb {

/**
 * @brief Manages a sibling thread, that samples CPU time and real time from OS.
 * Uses similar methodology to Python package `psutil`, to estimate CPU load
 * from the aforementioned timers.
 *
 * @see psutil: https://pypi.org/project/psutil/
 */
struct cpu_profiler_t {

    inline cpu_profiler_t(size_t request_delay = 100)
        : time_to_die_(true), request_delay_(request_delay), requests_count_(0) {}
    inline ~cpu_profiler_t() { stop(); }

    struct stats_t {
        float min = std::numeric_limits<float>::max();
        float max = 0;
        float avg = 0;
    };

    inline void start() {
        if (!time_to_die_.load())
            return;

        stats_.min = std::numeric_limits<float>::max();
        stats_.max = 0;
        stats_.avg = 0;

        requests_count_ = 0;
        time_to_die_.store(false);
        thread_ = std::thread(&cpu_profiler_t::request_cpu_usage, this);
    }
    inline void stop() {
        if (time_to_die_.load())
            return;

        // Wait to calculate one more times for  to get more accuracy
        std::this_thread::sleep_for(std::chrono::milliseconds(request_delay_ + 1));

        time_to_die_.store(true);
        thread_.join();
    }

    inline stats_t percent() const { return stats_; }

  private:
    inline void recalculate(float percent) {
        stats_.min = std::min(percent, stats_.min);
        stats_.max = std::max(percent, stats_.max);
        stats_.avg = (stats_.avg * (requests_count_ - 1) + percent) / requests_count_;
    }

    void request_cpu_usage() {
        bool first_time = true;
        clock_t last_cpu = 0;
        clock_t last_proc_user = 0;
        clock_t last_proc_sys = 0;

        while (!time_to_die_.load(std::memory_order_relaxed)) {
            tms time_sample;
            clock_t cpu = times(&time_sample);
            clock_t proc_user = time_sample.tms_utime;
            clock_t proc_sys = time_sample.tms_stime;
            clock_t delta_proc = (proc_user - last_proc_user) + (proc_sys - last_proc_sys);
            clock_t delta_cpu = cpu - last_cpu;

            if (!first_time && delta_cpu > 0) {
                float percent = 100.0 * delta_proc / delta_cpu;
                ++requests_count_;
                recalculate(percent);
            }
            else
                first_time = false;

            last_cpu = cpu;
            last_proc_user = proc_user;
            last_proc_sys = proc_sys;

            std::this_thread::sleep_for(std::chrono::milliseconds(request_delay_));
        }
    }

    std::thread thread_;
    std::atomic_bool time_to_die_;

    stats_t stats_;
    size_t request_delay_;
    size_t requests_count_;
};

/**
 * @brief Manages a sibling thread, that sample the virtual "/proc/self/stat" file
 * to estimate memory usage stats of the current process, similar to Valgrind.
 * Collects both Resident Set Size and Virtual Memory Size.
 * ! To avoid reimplementing STL for a purposes of benchmark, this uses `std::ifstream`
 * ! and does numerous heap allocations. Not a recommended practice :)
 *
 * @see valgrind: https://valgrind.org/
 */
struct mem_profiler_t {

    inline mem_profiler_t(size_t request_delay = 100)
        : time_to_die_(true), request_delay_(request_delay), requests_count_(0), page_size_(sysconf(_SC_PAGE_SIZE)) {}
    inline ~mem_profiler_t() { stop(); }

    struct stats_t {
        size_t min = std::numeric_limits<size_t>::max();
        size_t max = 0;
        size_t avg = 0;
    };

    inline void start() {
        if (!time_to_die_.load())
            return;

        stats_vms_.min = std::numeric_limits<size_t>::max();
        stats_vms_.max = 0;
        stats_vms_.avg = 0;

        stats_rss_.min = std::numeric_limits<size_t>::max();
        stats_rss_.max = 0;
        stats_rss_.avg = 0;

        requests_count_ = 0;
        time_to_die_.store(false);
        thread_ = std::thread(&mem_profiler_t::request_mem_usage, this);
    }
    inline void stop() {
        if (time_to_die_.load())
            return;

        time_to_die_.store(true);
        thread_.join();
    }

    inline stats_t vm() const { return stats_vms_; }
    inline stats_t rss() const { return stats_rss_; }

  private:
    inline void recalculate(size_t vm, size_t rss) {
        stats_vms_.min = std::min(vm, stats_vms_.min);
        stats_vms_.max = std::max(vm, stats_vms_.max);
        stats_vms_.avg = (stats_vms_.avg * (requests_count_ - 1) + vm) / requests_count_;

        stats_rss_.min = std::min(rss, stats_rss_.min);
        stats_rss_.max = std::max(rss, stats_rss_.max);
        stats_rss_.avg = (stats_rss_.avg * (requests_count_ - 1) + rss) / requests_count_;
    }

    inline void request_mem_usage() {
        while (!time_to_die_.load(std::memory_order_relaxed)) {
            size_t vm = 0, rss = 0;
            mem_usage(vm, rss);
            ++requests_count_;
            recalculate(vm, rss);
            std::this_thread::sleep_for(std::chrono::milliseconds(request_delay_));
        }
    }

    inline void mem_usage(size_t& vm, size_t& rss) {
        vm = 0;
        rss = 0;

        std::ifstream stat("/proc/self/stat", std::ios_base::in);
        std::string pid, comm, state, ppid, pgrp, session, tty_nr;
        std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
        std::string utime, stime, cutime, cstime, priority, nice;
        std::string O, itrealvalue, starttime;
        stat >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid >> flags >> minflt >> cminflt >>
            majflt >> cmajflt >> utime >> stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >>
            starttime >> vm >> rss;
        stat.close();

        rss = rss * page_size_;
    }

    std::thread thread_;
    std::atomic_bool time_to_die_;

    stats_t stats_vms_;
    stats_t stats_rss_;

    size_t request_delay_;
    size_t requests_count_;
    size_t page_size_;
};

} // namespace ucsb