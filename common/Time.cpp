#include "common/Time.h"

namespace lefr {
std::chrono::steady_clock::time_point Time::startTime = std::chrono::steady_clock::now();
}