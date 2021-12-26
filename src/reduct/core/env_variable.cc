// Copyright 2021 Alexey Timin

#include "reduct/core/env_variable.h"

namespace reduct::core {

std::string EnvVariable::Print() { return stream_.str(); }

}  // namespace reduct::core
