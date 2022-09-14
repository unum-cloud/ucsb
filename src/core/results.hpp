#pragma once

#include <fstream>
#include <vector>
#include <unordered_map>

#include "core/types.hpp"

using ordered_json = nlohmann::ordered_json;

namespace ucsb
{

    std::string parse_workload_name(std::string const &benchmark_name)
    {
        std::string name;
        size_t pos = benchmark_name.find('/');
        if (pos != std::string::npos)
            name = benchmark_name.substr(0, pos);
        else
            name = benchmark_name;
        return name;
    }

    void marge_results(ucsb::fs::path const &source_file_path,
                       ucsb::fs::path const &destination_file_path,
                       std::string const &db_name)
    {

        if (!ucsb::fs::exists(source_file_path))
            return;

        std::ifstream ifstream(source_file_path);
        ordered_json j_source;
        ifstream >> j_source;

        ordered_json j_destination;
        if (ucsb::fs::exists(destination_file_path))
        {
            ifstream = std::ifstream(destination_file_path);
            ifstream >> j_destination;
        }

        if (!j_destination.empty())
        {
            auto j_source_benchmarks = j_source["benchmarks"];
            auto j_destination_benchmarks = j_destination["benchmarks"];
            std::vector<ordered_json> results;
            // Take olds
            ordered_json header;
            for (auto it = j_destination_benchmarks.begin(); it != j_destination_benchmarks.end(); ++it)
            {
                auto dest_name = (*it)["name"].get<std::string>();
                if (dest_name.rfind(db_name, 0) == 0)
                    header = *it;
                else
                    results.push_back(*it);
            }
            // Update with new
            for (auto it = j_source_benchmarks.begin(); it != j_source_benchmarks.end(); ++it)
            {
                auto src_name = (*it)["name"].get<std::string>();
                src_name = parse_workload_name(src_name);
                if (src_name.rfind(db_name, 0) == 0)
                {
                    header = *it;
                    continue;
                }

                size_t idx = 0;
                for (; idx < results.size(); ++idx)
                {
                    auto dest_name = results[idx]["name"].get<std::string>();
                    dest_name = parse_workload_name(dest_name);
                    if (src_name == dest_name)
                    {
                        results[idx] = *it;
                        break;
                    }
                }
                if (idx == results.size())
                    results.push_back(*it);
            }
            if (!header.is_null())
                results.insert(results.begin(), header);
            j_destination["benchmarks"] = results;
        }
        else
            j_destination = j_source;

        std::ofstream ofstream(destination_file_path);
        ofstream << std::setw(2) << j_destination << std::endl;
    }

} // namespace ucsb