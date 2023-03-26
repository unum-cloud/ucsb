#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <fstream>

#include <benchmark/benchmark.h>
#include <tabulate/table.hpp>

#include "src/core/types.hpp"
#include "src/core/printable.hpp"

using ordered_json = nlohmann::ordered_json;

namespace ucsb {

namespace bm = benchmark;
namespace fs = ucsb::fs;

class console_reporter_t : public bm::BenchmarkReporter {

    using base_t = bm::BenchmarkReporter;

  public:
    inline console_reporter_t(std::string const& title);

  public:
    // Prints environment information
    bool ReportContext(Context const&) override;
    // Prints results
    void ReportRuns(std::vector<Run> const& reports) override;
    // Prints logo
    void Finalize() override;

  private:
    double convert_duration(double duration, bm::TimeUnit from, bm::TimeUnit to);

  private:
    std::string title_;
    bool print_header_;

    tabulate::Table::Row_t columns_;
    size_t column_width_;
    size_t workload_column_width_;
    size_t columns_total_width_;
};

inline console_reporter_t::console_reporter_t(std::string const& title)
    : base_t(), title_(title), print_header_(true), column_width_(0), workload_column_width_(0),
      columns_total_width_(0) {

    columns_ = {
        "Workload",
        "Throughput",
        "Data Rate",
        "Data Processed",
        "Disk Usage",
        "CPU (avg,%)",
        "CPU (max,%)",
        "Memory (avg)",
        "Memory (max)",
        "Fails (%)",
        "Duration",
    };

    column_width_ = 12;
    workload_column_width_ = 18;
    columns_total_width_ = workload_column_width_ + (columns_.size() - 1) * column_width_ + columns_.size() - 1;
}

bool console_reporter_t::ReportContext(Context const&) {
    tabulate::Table table;
    table.add_row({title_});
    table.column(0)
        .format()
        .width(columns_total_width_)
        .font_align(tabulate::FontAlign::center)
        .font_color(tabulate::Color::blue)
        .locale("C");
    std::cout << table << std::endl;
    return true;
}

void console_reporter_t::ReportRuns(std::vector<Run> const& reports) {

    if (reports.size() != 1) {
        fmt::print("Each benchmark should be in separate group");
        return;
    }
    auto const& report = reports.front();

    // Print header
    if (print_header_) {
        print_header_ = false;
        tabulate::Table table;
        table.add_row({columns_});
        table.row(0)
            .format()
            .width(column_width_)
            .font_align(tabulate::FontAlign::center)
            .font_color(tabulate::Color::blue)
            .hide_border_top()
            .locale("C");
        table.column(0).format().width(workload_column_width_);
        std::cout << table << std::endl;
    }

    // Counters
    double throughput = report.counters.at("operations/s").value;
    size_t data_rate = report.counters.at("bytes_per_second").value;
    //
    size_t data_processed = report.counters.at("processed,bytes").value;
    size_t disk_usage = report.counters.at("disk,bytes").value;
    //
    double cpu_avg = report.counters.at("cpu_avg,%").value;
    double cpu_max = report.counters.at("cpu_max,%").value;
    size_t mem_avg = report.counters.at("mem_avg(rss),bytes").value;
    size_t mem_max = report.counters.at("mem_max(rss),bytes").value;
    //
    double fails = report.counters.at("fails,%").value;
    double duration = convert_duration(report.real_accumulated_time, bm::TimeUnit::kSecond, bm::TimeUnit::kMillisecond);

    // Build table
    tabulate::Table table;
    table.add_row({report.run_name.function_name,
                   fmt::format("{}/s", printable_float_t {throughput}),
                   fmt::format("{}/s", printable_byte_t {data_rate}),
                   fmt::format("{}", printable_byte_t {data_processed}),
                   fmt::format("{}", printable_byte_t {disk_usage}),
                   fmt::format("{}", printable_float_t {cpu_avg}),
                   fmt::format("{}", printable_float_t {cpu_max}),
                   fmt::format("{}", printable_byte_t {mem_avg}),
                   fmt::format("{}", printable_byte_t {mem_max}),
                   fmt::format("{}", printable_float_t {fails}),
                   fmt::format("{}", printable_duration_t {size_t(duration)})});
    table.row(0).format().width(column_width_).font_align(tabulate::FontAlign::right).hide_border_top().locale("C");
    table.column(0)
        .format()
        .width(workload_column_width_)
        .font_align(tabulate::FontAlign::left)
        .font_color(tabulate::Color::green);

    // Highlight cells
    if (fails > 0)
        table[0][2].format().font_color(tabulate::Color::red);

    // Print
    std::cout << table << std::endl;
}

void console_reporter_t::Finalize() {
    tabulate::Table table;
    table.add_row({"C 2015-2023 UCSB, Unum Cloud"});
    table.row(0)
        .format()
        .width(columns_total_width_)
        .font_align(tabulate::FontAlign::center)
        .font_color(tabulate::Color::blue)
        .hide_border_top()
        .locale("C");
    std::cout << table << std::endl;
}

double console_reporter_t::convert_duration(double duration, bm::TimeUnit from, bm::TimeUnit to) {
    // First convert to nanoseconds
    switch (from) {
    case bm::TimeUnit::kSecond: duration *= 1'000; [[fallthrough]];
    case bm::TimeUnit::kMillisecond: duration *= 1'000; [[fallthrough]];
    case bm::TimeUnit::kMicrosecond: duration *= 1'000; [[fallthrough]];
    case bm::TimeUnit::kNanosecond: [[fallthrough]];
    default: break;
    }

    // Convert to specified
    switch (to) {
    case bm::TimeUnit::kSecond: duration /= 1'000; [[fallthrough]];
    case bm::TimeUnit::kMillisecond: duration /= 1'000; [[fallthrough]];
    case bm::TimeUnit::kMicrosecond: duration /= 1'000; [[fallthrough]];
    case bm::TimeUnit::kNanosecond: [[fallthrough]];
    default: break;
    }

    return duration;
}

class file_reporter_t {
  public:
    static void merge_results(fs::path const& source_file_path, fs::path const& destination_file_path);

  private:
    static std::string parse_workload_name(std::string const& benchmark_name);
};

std::string file_reporter_t::parse_workload_name(std::string const& benchmark_name) {
    std::string name;
    size_t pos = benchmark_name.find('/');
    if (pos != std::string::npos)
        name = benchmark_name.substr(0, pos);
    else
        name = benchmark_name;
    return name;
}

void file_reporter_t::merge_results(fs::path const& source_file_path, fs::path const& destination_file_path) {

    if (!fs::exists(source_file_path))
        return;

    std::ifstream ifstream(source_file_path);
    ordered_json j_source;
    ifstream >> j_source;

    ordered_json j_destination;
    if (fs::exists(destination_file_path)) {
        ifstream = std::ifstream(destination_file_path);
        ifstream >> j_destination;
    }

    if (!j_destination.empty()) {
        auto j_source_benchmarks = j_source["benchmarks"];
        auto j_destination_benchmarks = j_destination["benchmarks"];
        std::vector<ordered_json> results;
        // Take olds
        for (auto it = j_destination_benchmarks.begin(); it != j_destination_benchmarks.end(); ++it)
            results.push_back(*it);
        // Update with new
        for (auto it = j_source_benchmarks.begin(); it != j_source_benchmarks.end(); ++it) {
            auto src_name = (*it)["name"].get<std::string>();
            src_name = parse_workload_name(src_name);

            size_t idx = 0;
            for (; idx < results.size(); ++idx) {
                auto dest_name = results[idx]["name"].get<std::string>();
                dest_name = parse_workload_name(dest_name);
                if (src_name == dest_name) {
                    results[idx] = *it;
                    break;
                }
            }
            if (idx == results.size())
                results.push_back(*it);
        }
        j_destination["benchmarks"] = results;
    }
    else
        j_destination = j_source;

    std::ofstream ofstream(destination_file_path);
    ofstream << std::setw(2) << j_destination << std::endl;
}

} // namespace ucsb