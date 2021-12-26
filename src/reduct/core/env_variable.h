// Copyright 2021 Alexey Timin
#ifndef REDUCT_CORE_ENV_VARIABLE_H_
#define REDUCT_CORE_ENV_VARIABLE_H_

#include <cstdlib>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

namespace reduct::core {

/**
 * @class EnvVariable
 *
 * Helper class to get env variable and replace its value with default one if it
 * is not found
 *
 * @author Aleksey Timin
 * @tparam T
 */

class EnvVariable {
 public:
  /**
   * Make environment variable
   * @tparam T
   * @param name the name of env variable
   * @param defaultValue the default value if it doesn't exists
   */
  template <typename T>
  T Get(const std::string &name, const T &defaultValue) {
    T value;
    std::string additional_part;
    auto envVar = std::getenv(name.c_str());
    if (envVar == nullptr) {
      value = defaultValue;
      additional_part = "(default)";
    } else {
      std::stringstream ss(envVar);
      ss >> value;
    }

    stream_ << "\t" << name << " = ";
    stream_ << value << " " << additional_part << "\n";

    return value;
  }

  /**
   * Get log with variables names and values
   */
  std::string Print();

 private:
  std::ostringstream stream_;
};

}  // namespace reduct::core
#endif  //  REDUCT_CORE_ENV_VARIABLE_H_
