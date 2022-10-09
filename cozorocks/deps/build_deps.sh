#!/usr/bin/env bash

set -e

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  export CC=/usr/bin/clang-12
  export CPP=/usr/bin/clang-cpp-12
  export CXX=/usr/bin/clang++-12
  export LD=/usr/bin/ld.lld-12
  if [ ! -f ${CC} ]; then echo "${CC} not found"; exit; fi
  if [ ! -f ${CPP} ]; then echo "${CPP} not found"; exit; fi
  if [ ! -f ${CXX} ]; then echo "${CXX} not found"; exit; fi
  if [ ! -f ${LD} ]; then echo "${LD} not found"; exit; fi
fi

N_CORES=$(getconf _NPROCESSORS_ONLN)

export CFLAGS=-fPIE
export CXXFLAGS=-fPIE

# gflags

cd gflags
rm -fr cmake_build
mkdir cmake_build && cd cmake_build
cmake ..
make -j $N_CORES
cd ../..

# lz4

cd lz4
make clean
make -j $N_CORES
cd ..

# zstd

cd zstd
make clean
make -j $N_CORES
cd ..

# jemalloc
cd jemalloc
./autogen.sh --disable-debug --with-jemalloc-prefix=''
make -j $N_CORES
cd ..

# rocksdb

cd rocksdb
rm -fr cmake_build
mkdir cmake_build && cd cmake_build
# cp ../../thirdparty.inc ../thirdparty.inc
cmake -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_MODULE_PATH="${PWD}/../../lz4;${PWD}/../../zstd" \
  -DROCKSDB_BUILD_SHARED=0 \
  -DGFLAGS_INCLUDE_DIR=${PWD}/../../gflags/cmake_build/include \
  -DGFLAGS_LIBRARIES=${PWD}/../../gflags/cmake_build/lib \
  -Dlz4_INCLUDE_DIRS=${PWD}/../../lz4/lib \
  -Dlz4_LIBRARIES=${PWD}/../../lz4/lib \
  -Dzstd_INCLUDE_DIRS=${PWD}/../../zstd/lib \
  -Dzstd_LIBRARIES=${PWD}/../../zstd/lib \
  -DJeMalloc_INCLUDE_DIRS=${PWD}/../../jemalloc/include \
  -DJeMalloc_LIBRARIES=${PWD}/../../jemalloc/lib \
  -DCMAKE_CXX_STANDARD=20 -DWITH_GFLAGS=1 -DWITH_LZ4=1 -DWITH_ZSTD=1 -DUSE_RTTI=1 -DWITH_TESTS=0 \
  -DWITH_JEMALLOC=1 -DWITH_BENCHMARK_TOOLS=0 -DWITH_CORE_TOOLS=0 -DWITH_TOOLS=0 -DWITH_TRACE_TOOLS=0 ..
make -j $N_CORES
cd ..