#include "reduct/config.h"
#include "reduct/core/logger.h"

int main() {
  LOG_INFO("Reduct Storage {}", reduct::kVersion);
  return 0;
}
