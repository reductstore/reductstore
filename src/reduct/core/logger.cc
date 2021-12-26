// Copyright 2021 Alexey Timin
#include "reduct/core/logger.h"

#include <map>
#include <utility>

namespace reduct::core {


void Logger::SetLogLevel(LogLevels print_level) { log_level_ = print_level; }

void Logger::SetLogLevel(const std::string &print_level) {
  static const std::map<std::string, LogLevels> kIdsLoglevel = {
      std::make_pair("TRACE", LogLevels::kTrace), std::make_pair("DEBUG", LogLevels::kDebug),
      std::make_pair("INFO", LogLevels::kInfo),   std::make_pair("WARNING", LogLevels::kWarning),
      std::make_pair("ERROR", LogLevels::kError),
  };

  log_level_ = kIdsLoglevel.at(print_level);
}

}  // namespace reduct::core
