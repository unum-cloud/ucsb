#pragma once

#include <cstdlib>

namespace ucsb {

/**
 * @brief Aligned buffer for direct file I/O
 */
struct aligned_buffer_t {
  public:
    constexpr static size_t alignment_k = 4096;

    inline aligned_buffer_t() noexcept : buffer_(nullptr), size_(0) {}
    inline aligned_buffer_t(size_t size) : buffer_(nullptr), size_(size) {
        assert(size % alignment_k == 0);
        buffer_ = std::aligned_alloc(alignment_k, size);
    }

    inline aligned_buffer_t(aligned_buffer_t const& other) : aligned_buffer_t(other.size_) {
        memcpy(buffer_, other.buffer_, size_);
    }
    inline aligned_buffer_t(aligned_buffer_t&& other) noexcept
        : buffer_(std::move(other.buffer_)), size_(std::move(other.size_)) {
        other.buffer_ = nullptr;
        other.size_ = 0;
    }

    inline ~aligned_buffer_t() { std::free(buffer_); }

    inline aligned_buffer_t operator=(aligned_buffer_t const& other) {
        buffer_ = std::aligned_alloc(alignment_k, other.size_);
        size_ = other.size_;
        memcpy(buffer_, other.buffer_, size_);
        return *this;
    }
    inline aligned_buffer_t operator=(aligned_buffer_t&& other) {
        std::swap(buffer_, other.buffer_);
        std::swap(size_, other.size_);
        return *this;
    }

    inline size_t size() const noexcept { return size_; }

    inline std::byte& operator[](size_t idx) noexcept { return buffer_[idx]; }
    inline std::byte const& operator[](size_t idx) const noexcept { return buffer_[idx]; }

    inline std::byte* data() noexcept { return buffer_; }
    inline std::byte const* data() const noexcept { return buffer_; }

  private:
    std::byte* buffer_;
    size_t size_;
};

} // namespace ucsb