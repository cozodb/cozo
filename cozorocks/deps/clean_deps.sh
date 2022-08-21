#!/usr/bin/env bash

set -e

cd gflags
rm -fr cmake_build
cd ..

cd lz4
make clean
cd ..

cd zstd
make clean
cd ..

cd jemalloc
make clean
rm -f 'configure~'
cd ..

cd rocksdb
rm -fr cmake_build
cd ..