#!/usr/bin/env bash

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  export CC=/usr/bin/clang-14
  export CPP=/usr/bin/clang-cpp-14
  export CXX=/usr/bin/clang++-14
  export LD=/usr/bin/ld.lld-14
fi

mkdir -p deps
mkdir -p deps/lib
INSTALL_DIR=$(readlink -f deps)
echo "$INSTALL_DIR"

#cd jemalloc || exit
#
#./autogen.sh --disable_initial_exec_tls --disable-debug --prefix="$INSTALL_DIR" --with-jemalloc-prefix=""
#make
#make install
#
#cd ..

cd rocksdb || exit
make clean

export JEMALLOC_BASE=$INSTALL_DIR

export EXTRA_CFLAGS='-fPIC'
export EXTRA_CXXFLAGS='-fPIC'

#  JEMALLOC_INCLUDE=" -I $JEMALLOC_BASE/include/" \
#  JEMALLOC_LIB=" $JEMALLOC_BASE/lib/libjemalloc.a" \
#  JEMALLOC=1 \

DEBUG_LEVEL=0 make libz.a libsnappy.a liblz4.a libzstd.a
mv ./*.a ../deps/lib || exit

export EXTRA_CFLAGS="-fPIC -I${PWD}/lz4-1.9.3/lib"
export EXTRA_CXXFLAGS="-fPIC -I${PWD}/lz4-1.9.3/lib"

DEBUG_LEVEL=0 \
  USE_RTTI=1 \
  USE_CLANG=1 \
  PREFIX=$INSTALL_DIR \
  make install-static || exit

mv ./*.a ../deps/lib || exit

make clean
