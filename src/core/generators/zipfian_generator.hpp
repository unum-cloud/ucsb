#pragma once

#include <random>
#include <cassert>

#include "src/core/generators/generator.hpp"
#include "src/core/generators/random_generator.hpp"

namespace ucsb::core {

class zipfian_generator_t : public generator_gt<size_t> {
  public:
    static constexpr double zipfian_const_k = 0.99;
    static constexpr size_t items_max_count = (UINT64_MAX >> 24);

    zipfian_generator_t(size_t items_count) : zipfian_generator_t(0, items_count - 1) {}
    zipfian_generator_t(size_t min, size_t max, double zipfian_const = zipfian_const_k)
        : zipfian_generator_t(min, max, zipfian_const, zeta(max - min + 1, zipfian_const)) {}
    zipfian_generator_t(size_t min, size_t max, double zipfian_const, double zeta_n);

    inline size_t generate() override { return generate(items_count_); }
    inline size_t last() override { return last_; }

    size_t generate(size_t items_count);

  private:
    inline double eta() { return (1 - std::pow(2.0 / items_count_, 1 - theta_)) / (1 - zeta_2_ / zeta_n_); }
    inline double zeta(size_t num, double theta) { return zeta(0, num, theta, 0.0); }
    inline double zeta(size_t last_num, size_t cur_num, double theta, double last_zeta);

    random_double_generator_t generator_;
    size_t items_count_;
    size_t base_;
    size_t count_for_zeta_;
    size_t last_;
    double theta_;
    double zeta_n_;
    double eta_;
    double alpha_;
    double zeta_2_;
    bool allow_count_decrease_;
};

zipfian_generator_t::zipfian_generator_t(size_t min, size_t max, double zipfian_const, double zeta_n)
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

size_t zipfian_generator_t::generate(size_t num) {
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

    double u = generator_.generate();
    double uz = u * zeta_n_;

    if (uz < 1.0)
        return last_ = base_;
    if (uz < 1.0 + std::pow(0.5, theta_))
        return last_ = base_ + 1;
    return last_ = base_ + num * std::pow(eta_ * u - eta_ + 1, alpha_);
}

inline double zipfian_generator_t::zeta(size_t last_num, size_t cur_num, double theta, double last_zeta) {
    double zeta = last_zeta;
    for (size_t i = last_num + 1; i <= cur_num; ++i)
        zeta += 1 / std::pow(i, theta);
    return zeta;
}

} // namespace ucsb::core