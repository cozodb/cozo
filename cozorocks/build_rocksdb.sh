#!/usr/bin/env bash

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  export CC=/usr/bin/clang-14
  export CPP=/usr/bin/clang-cpp-14
  export CXX=/usr/bin/clang++-14
  export LD=/usr/bin/ld.lld-14
fi

mkdir -p deps
INSTALL_DIR=$(readlink -f deps)
echo "$INSTALL_DIR"

cd jemalloc || exit

./autogen.sh --disable-debug --prefix="$INSTALL_DIR" --with-jemalloc-prefix=""
make
make install

cd ..

cd rocksdb || exit
make clean

export JEMALLOC_BASE=$INSTALL_DIR

export EXTRA_CFLAGS='-fPIC'
export EXTRA_CXXFLAGS='-fPIC'

DEBUG_LEVEL=0 \
  JEMALLOC_INCLUDE=" -I $JEMALLOC_BASE/include/" \
  JEMALLOC_LIB=" $JEMALLOC_BASE/lib/libjemalloc.a" \
  USE_RTTI=1 \
  USE_CLANG=1 \
  JEMALLOC=1 \
  PREFIX=$INSTALL_DIR \
  make install-static || exit

DEBUG_LEVEL=0 make libz.a libsnappy.a liblz4.a libzstd.a
mv ./*.a ../deps/lib || exit
make clean
