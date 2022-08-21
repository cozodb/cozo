#!/usr/bin/env bash

set -e

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  export CC=/usr/bin/clang-12
  export CPP=/usr/bin/clang-cpp-12
  export CXX=/usr/bin/clang++-12
  export LD=/usr/bin/ld.lld-12
fi

# gflags

cd gflags
rm -fr cmake_build
mkdir cmake_build && cd cmake_build
cmake ..
make -j 8
cd ../..

# lz4

cd lz4
make clean
make -j 8
cd ..

# zstd

cd zstd
make clean
make -j 8
cd ..

# jemalloc
cd jemalloc
make clean
./autogen.sh --with-jemalloc-prefix=''
make -j 8

cd ..

# rocksdb

cd rocksdb
rm -fr cmake_build
mkdir cmake_build && cd cmake_build
cp ../../thirdparty.inc ../thirdparty.inc
cmake -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_MODULE_PATH="${PWD}/../../lz4;${PWD}/../../zstd" \
  -DROCKSDB_BUILD_SHARED=0 \
  -Dlz4_INCLUDE_DIRS=${PWD}/../../lz4/lib \
  -Dlz4_LIBRARIES=${PWD}/../../lz4/lib \
  -Dzstd_INCLUDE_DIRS=${PWD}/../../zstd/lib \
  -Dzstd_LIBRARIES=${PWD}/../../zstd/lib \
  -DJeMalloc_INCLUDE_DIRS=${PWD}/../../jemalloc/include \
  -DJeMalloc_LIBRARIES=${PWD}/../../jemalloc/lib \
  -DCMAKE_CXX_STANDARD=20 -DWITH_GFLAGS=1 -DWITH_LZ4=1 -DWITH_ZSTD=1 -DUSE_RTTI=1 -DWITH_TESTS=0 \
  -DWITH_JEMALLOC=1 -DWITH_BENCHMARK_TOOLS=0 -DWITH_CORE_TOOLS=0 -DWITH_TOOLS=0 ..
make -j 8
cd ..