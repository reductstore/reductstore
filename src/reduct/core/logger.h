// Copyright 2021 Alexey Timin
#ifndef REDUCT_CORE_LOGGER_H_
#define REDUCT_CORE_LOGGER_H_

#include <fmt/core.h>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <thread>

namespace reduct::core {
/**
 * Levels for logging
 */
enum class LogLevels { kNone = 0, kError, kWarning, kInfo, kDebug, kTrace };

class Logger {
 public:
  template <typename... T>
  static void Log(LogLevels level, std::string_view message, int line, std::string_view file, T &&...args) {
    using Clock = std::chrono::system_clock;

    static const std::map<LogLevels, std::string> kLoglevelNames = {
        std::make_pair(LogLevels::kTrace, "[TRACE]"), std::make_pair(LogLevels::kDebug, "[DEBUG]"),
        std::make_pair(LogLevels::kInfo, "[INFO]"),   std::make_pair(LogLevels::kWarning, "[WARNING]"),
        std::make_pair(LogLevels::kError, "[ERROR]"),
    };

    if (log_level_ != LogLevels::kNone && level <= log_level_) {
      auto timestamp = Clock::now();
      std::time_t unixTime = Clock::to_time_t(timestamp);
      std::tm *gtime = std::gmtime(&unixTime);
      auto milliseconds =
          std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch()).count() % 1000;

      std::stringstream ss;
      ss << std::put_time(gtime, "%F %T");
      auto thid = std::this_thread::get_id();
      auto msg = fmt::vformat(message, fmt::make_format_args(args...));
      std::cout << fmt::format("{}.{:03d} ({:>5}) {}:{}\t {} {}", ss.str(), milliseconds,
                               reinterpret_cast<uint16_t &>(thid), file, line, kLoglevelNames.at(level), msg)
                << std::endl;
    }
  }

  static void SetLogLevel(LogLevels print_level);
  static void SetLogLevel(const std::string &print_level);

 private:
  static LogLevels log_level_;
};

}  // namespace reduct::core

constexpr const char *str_end(const char *str) { return *str ? str_end(str + 1) : str; }

constexpr bool str_slant(const char *str) { return *str == '/' ? true : (*str ? str_slant(str + 1) : false); }

constexpr const char *r_slant(const char *str) { return *str == '/' ? (str + 1) : r_slant(str - 1); }

constexpr const char *file_name(const char *str) { return str_slant(str) ? r_slant(str_end(str)) : str; }

#define LOG_ERROR(msg, ...)                                                 \
  reduct::core::Logger::Log(reduct::core::LogLevels::kError, msg, __LINE__, \
                            file_name(__FILE__) __VA_OPT__(, ) __VA_ARGS__)  // NOLINT

#define LOG_WARNING(msg, ...)                                                 \
  reduct::core::Logger::Log(reduct::core::LogLevels::kWarning, msg, __LINE__, \
                            file_name(__FILE__) __VA_OPT__(, ) __VA_ARGS__)  // NOLINT

#define LOG_INFO(msg, ...)                                                 \
  reduct::core::Logger::Log(reduct::core::LogLevels::kInfo, msg, __LINE__, \
                            file_name(__FILE__) __VA_OPT__(, ) __VA_ARGS__)  // NOLINT

#define LOG_DEBUG(msg, ...)                                                 \
  reduct::core::Logger::Log(reduct::core::LogLevels::kDebug, msg, __LINE__, \
                            file_name(__FILE__) __VA_OPT__(, ) __VA_ARGS__)  // NOLINT

#define LOG_TRACE(msg, ...)                                                 \
  reduct::core::Logger::Log(reduct::core::LogLevels::kTrace, msg, __LINE__, \
                            file_name(__FILE__) __VA_OPT__(, ) __VA_ARGS__)  // NOLINT

#endif  //  REDUCT_CORE_LOGGER_H_
