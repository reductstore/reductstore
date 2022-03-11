// Copyright 2021-2022 Alexey Timin
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
   * @param default_value the default value if it doesn't exists
   * @param masked if true print **** instead of value
   */
  template <typename T>
  T Get(const std::string &name, const T &default_value, bool masked = false) {
    T value;
    std::string additional_part;
    auto env_var = std::getenv(name.c_str());
    if (env_var == nullptr) {
      value = default_value;
      additional_part = "(default)";
    } else {
      std::stringstream ss(env_var);
      ss >> value;
    }

    if (value != T{}) {
      stream_ << "\t" << name << " = ";
      if (masked) {
        stream_ << "********** (masked)\n";
      } else {
        stream_ << value << " " << additional_part << "\n";
      }
    }

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
