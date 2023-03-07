#pragma once
#include <random>

#include "src/core/generators/generator.hpp"

namespace ucsb::core {

class random_int_generator_t final : public generator_gt<uint32_t> {
  public:
    inline random_int_generator_t() : device_(), rand_(device_()), last_(0) { generate(); }

    inline uint32_t generate() override { return last_ = rand_(); }
    inline uint32_t last() override { return last_; }

  private:
    std::random_device device_;
    std::minstd_rand rand_;
    float last_;
};

class random_double_generator_t final : public generator_gt<float> {
  public:
    inline random_double_generator_t(float min, float max)
        : device_(), rand_(device_()), uniform_(min, max), last_(0.0) {
        generate();
    }
    ~random_double_generator_t() override = default;

    inline float generate() override { return last_ = uniform_(rand_); }
    inline float last() override { return last_; }

  private:
    std::random_device device_;
    std::minstd_rand rand_;
    std::uniform_real_distribution<float> uniform_;
    float last_;
};

class random_byte_generator_t final : public generator_gt<char> {
  public:
    inline random_byte_generator_t() : off_(6) {}
    ~random_byte_generator_t() override = default;

    inline char generate() override;
    inline char last() override { return buf_[(off_ - 1 + 6) % 6]; }

  private:
    random_int_generator_t generator_;
    char buf_[6];
    int off_;
};

inline char random_byte_generator_t::generate() {
    if (off_ == 6) {
        uint32_t bytes = generator_.generate();
        buf_[0] = static_cast<char>((bytes & 31) + ' ');
        buf_[1] = static_cast<char>(((bytes >> 5) & 63) + ' ');
        buf_[2] = static_cast<char>(((bytes >> 10) & 95) + ' ');
        buf_[3] = static_cast<char>(((bytes >> 15) & 31) + ' ');
        buf_[4] = static_cast<char>(((bytes >> 20) & 63) + ' ');
        buf_[5] = static_cast<char>(((bytes >> 25) & 95) + ' ');
        off_ = 0;
    }

    return buf_[off_++];
}

} // namespace ucsb::core