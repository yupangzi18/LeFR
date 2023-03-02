#pragma once

#include "benchmark/ycsb/Schema.h"

namespace lefr {

namespace ycsb {
struct Storage {
    ycsb::key ycsb_keys[YCSB_FIELD_SIZE];
    ycsb::value ycsb_values[YCSB_FIELD_SIZE];
};

}  // namespace ycsb
}  // namespace lefr