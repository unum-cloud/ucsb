#pragma once

#include <sys/times.h>
#include <unistd.h>
#include <string>
#include <fstream>
#include <limits>
#include <chrono>
#include <thread>

namespace ucsb {

struct cpu_stat_t {

    inline cpu_stat_t(size_t request_delay = 100)
        : request_delay_(request_delay), requests_count_(0), time_to_die_(true) {}
    inline ~cpu_stat_t() { stop(); }

    struct stat_t {
        float min = std::numeric_limits<float>::max();
        float max = 0;
        float avg = 0;
    };

    inline void start() {
        if (!time_to_die_)
            return;

        percent_.min = std::numeric_limits<float>::max();
        percent_.max = 0;
        percent_.avg = 0;

        requests_count_ = 0;
        time_to_die_ = false;
        thread_ = std::thread(&cpu_stat_t::request_cpu_usage, this);
    }
    inline void stop() {
        if (time_to_die_)
            return;

        // Wait to calculate one more times for  to get more accuracy
        std::this_thread::sleep_for(std::chrono::milliseconds(request_delay_ + 1));

        time_to_die_ = true;
        thread_.join();
    }

    inline stat_t percent() const { return percent_; }

  private:
    void recalculate(float percent) {
        percent_.min = std::min(percent, percent_.min);
        percent_.max = std::max(percent, percent_.max);
        percent_.avg = (percent_.avg * (requests_count_ - 1) + percent) / requests_count_;
    }

    void request_cpu_usage() {
        bool first_time = true;
        clock_t last_cpu = 0;
        clock_t last_proc_user = 0;
        clock_t last_proc_sys = 0;

        while (!time_to_die_) {
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

    stat_t percent_;

    size_t request_delay_;
    size_t requests_count_;
    std::thread thread_;
    bool time_to_die_;
};

struct mem_stat_t {

    inline mem_stat_t(size_t request_delay = 100)
        : request_delay_(request_delay), requests_count_(0), time_to_die_(true) {}
    inline ~mem_stat_t() { stop(); }

    struct stat_t {
        size_t min = std::numeric_limits<size_t>::max();
        size_t max = 0;
        size_t avg = 0;
    };

    inline void start() {
        if (!time_to_die_)
            return;

        vm_.min = std::numeric_limits<size_t>::max();
        vm_.max = 0;
        vm_.avg = 0;

        rss_.min = std::numeric_limits<size_t>::max();
        rss_.max = 0;
        rss_.avg = 0;

        requests_count_ = 0;
        time_to_die_ = false;
        thread_ = std::thread(&mem_stat_t::request_mem_usage, this);
    }
    inline void stop() {
        if (time_to_die_)
            return;

        time_to_die_ = true;
        thread_.join();
    }

    inline stat_t vm() const { return vm_; }
    inline stat_t rss() const { return rss_; }

  private:
    void recalculate(size_t vm, size_t rss) {
        vm_.min = std::min(vm, vm_.min);
        vm_.max = std::max(vm, vm_.max);
        vm_.avg = (vm_.avg * (requests_count_ - 1) + vm) / requests_count_;

        rss_.min = std::min(rss, rss_.min);
        rss_.max = std::max(rss, rss_.max);
        rss_.avg = (rss_.avg * (requests_count_ - 1) + rss) / requests_count_;
    }

    void request_mem_usage() {
        while (!time_to_die_) {
            size_t vm = 0, rss = 0;
            mem_usage(vm, rss);
            ++requests_count_;
            recalculate(vm, rss);
            std::this_thread::sleep_for(std::chrono::milliseconds(request_delay_));
        }
    }

    void mem_usage(size_t& vm, size_t& rss) {
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

        size_t page_size = sysconf(_SC_PAGE_SIZE);
        rss = rss * page_size;
    }

    stat_t vm_;
    stat_t rss_;

    size_t request_delay_;
    size_t requests_count_;
    std::thread thread_;
    bool time_to_die_;
};

} // namespace ucsb