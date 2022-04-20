cmake_minimum_required(VERSION 3.22)
project(cozorocks)

set(CMAKE_CXX_STANDARD 17)

include_directories("bridge")
include_directories("../deps/include")
include_directories("../target/cxxbridge")

add_library(cozorocks "bridge/cozorocks.cc" "bridge/cozorocks.h")