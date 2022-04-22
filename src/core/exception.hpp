#pragma once

#include "src/core/types.hpp"

namespace ucsb {

struct exception_t : public std::exception {
  public:
    inline exception_t(std::string const& message) : message_(message) {}
    inline const char* what() const noexcept override { return message_.c_str(); }

  private:
    std::string message_;
};

} // namespace ucsb