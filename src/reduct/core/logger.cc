// Copyright 2022 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#include "reduct/core/logger.h"

#include <map>
#include <utility>

namespace reduct::core {

LogLevels Logger::log_level_ = LogLevels::kInfo;

void Logger::set_level(const std::string &print_level) {
  static const std::map<std::string, LogLevels> kIdsLoglevel = {
      std::make_pair("TRACE", LogLevels::kTrace), std::make_pair("DEBUG", LogLevels::kDebug),
      std::make_pair("INFO", LogLevels::kInfo),   std::make_pair("WARNING", LogLevels::kWarning),
      std::make_pair("ERROR", LogLevels::kError),
  };

  log_level_ = kIdsLoglevel.at(print_level);
}

}  // namespace reduct::core
