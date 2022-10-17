cmake_minimum_required(VERSION 3.22)
project(cozorocks)

set(CMAKE_CXX_STANDARD 17)

include_directories("bridge")
include_directories("./rocksdb/include")
include_directories("../target/cxxbridge")

add_library(cozorocks "bridge/bridge.h" "bridge/common.h" "bridge/db.cpp" "bridge/db.h" "bridge/iter.h" "bridge/opts.h"
        "bridge/slice.h" "bridge/status.cpp" "bridge/status.h" "bridge/tx.cpp" "bridge/tx.h")