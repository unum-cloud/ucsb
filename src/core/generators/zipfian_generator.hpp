#pragma once
#include <random>
#include <cassert>

#include "src/core/generators/generator.hpp"
#include "src/core/generators/random_generator.hpp"

namespace ucsb {

struct zipfian_generator_t : public generator_gt<size_t> {
    static constexpr float zipfian_const_k = 0.99;
    static constexpr size_t items_max_count = (UINT64_MAX >> 24);

    inline zipfian_generator_t(size_t items_count) : zipfian_generator_t(0, items_count - 1) {}
    inline zipfian_generator_t(size_t min, size_t max, float zipfian_const = zipfian_const_k)
        : zipfian_generator_t(min, max, zipfian_const, zeta(max - min + 1, zipfian_const)) {}
    inline zipfian_generator_t(size_t min, size_t max, float zipfian_const, float zeta_n);

    inline size_t generate() override { return generate(items_count_); }
    inline size_t last() override { return last_; }

    inline size_t generate(size_t items_count);

  private:
    inline float eta() { return (1 - std::pow(2.f / items_count_, 1 - theta_)) / (1 - zeta_2_ / zeta_n_); }
    inline float zeta(size_t num, float theta) { return zeta(0, num, theta, 0.f); }
    inline float zeta(size_t last_num, size_t cur_num, float theta, float last_zeta);

    random_double_generator_t generator_;
    size_t items_count_;
    size_t base_;
    size_t count_for_zeta_;
    size_t last_;
    float theta_;
    float zeta_n_;
    float eta_;
    float alpha_;
    float zeta_2_;
    bool allow_count_decrease_;
};

inline zipfian_generator_t::zipfian_generator_t(size_t min, size_t max, float zipfian_const, float zeta_n)
    : generator_(0.0, 1.0), items_count_(max - min + 1), base_(min), theta_(zipfian_const),
      allow_count_decrease_(false) {
    assert(items_count_ >= 2 && items_count_ < items_max_count);

    zeta_2_ = zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    zeta_n_ = zeta_n;
    count_for_zeta_ = items_count_;
    eta_ = eta();

    generate();
}

inline size_t zipfian_generator_t::generate(size_t num) {
    assert(num >= 2 && num < items_max_count);
    if (num != count_for_zeta_) {
        if (num > count_for_zeta_) {
            zeta_n_ = zeta(count_for_zeta_, num, theta_, zeta_n_);
            count_for_zeta_ = num;
            eta_ = eta();
        }
        else if (num < count_for_zeta_ && allow_count_decrease_) {
            // TODO
        }
    }

    float u = generator_.generate();
    float uz = u * zeta_n_;

    if (uz < 1.0)
        return last_ = base_;
    if (uz < 1.0 + std::pow(0.5, theta_))
        return last_ = base_ + 1;
    return last_ = base_ + num * std::pow(eta_ * u - eta_ + 1, alpha_);
}

inline float zipfian_generator_t::zeta(size_t last_num, size_t cur_num, float theta, float last_zeta) {
    float zeta = last_zeta;
    for (size_t i = last_num + 1; i <= cur_num; ++i)
        zeta += 1 / std::pow(i, theta);
    return zeta;
}

} // namespace ucsb