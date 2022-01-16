#pragma once

#include <fstream>
#include <vector>
#include <unordered_map>

#include "ucsb/core/types.hpp"

using ordered_json = nlohmann::ordered_json;

namespace ucsb {

void marge_results(ucsb::fs::path const& source_file_path, ucsb::fs::path const& destination_file_path) {

    if (!ucsb::fs::exists(source_file_path))
        return false;

    std::ifstream ifstream(source_file_path);
    ordered_json j_sourace;
    ifstream >> j_sourace;

    ordered_json j_destinatino;
    if (ucsb::fs::exists(destination_file_path)) {
        ifstream = std::ifstream(destination_file_path);
        ifstream >> j_destinatino;
    }

    if (!j_destinatino.empty()) {
        auto j_source_benchmarks = j_sourace["benchmarks"];
        auto j_destination_benchmarks = j_destinatino["benchmarks"];
        std::vector<ordered_json> results;
        // Take olds
        for (auto it = j_destination_benchmarks.begin(); it != j_destination_benchmarks.end(); ++it)
            results.push_back(*it);
        // Update with new
        for (auto it = j_source_benchmarks.begin(); it != j_source_benchmarks.end(); ++it) {
            auto src_name = (*it)["name"].get<std::string>();
            size_t idx = 0;
            for (; idx < results.size(); ++idx) {
                auto dest_name = results[idx]["name"].get<std::string>();
                if (src_name == dest_name) {
                    results[idx] = *it;
                    break;
                }
            }
            if (idx == results.size())
                results.push_back(*it);
        }
        j_destinatino["benchmarks"] = results;
    }
    else
        j_destinatino = j_sourace;

    std::ofstream ofstream(destination_file_path);
    ofstream << std::setw(4) << j_destinatino << std::endl;
}

} // namespace ucsb