cd rocksdb || exit
make clean
INSTALL_DIR=$(readlink -f ../deps)
echo "$INSTALL_DIR"
rm -fr "$INSTALL_DIR"
mkdir "$INSTALL_DIR"

export JEMALLOC_BASE=/opt/homebrew

JEMALLOC_INCLUDE=" -I $JEMALLOC_BASE/include/" \
JEMALLOC_LIB=" $JEMALLOC_BASE/lib/libjemalloc.a" \
USE_RTTI=1 \
USE_CLANG=1 \
JEMALLOC=1 \
PREFIX=$INSTALL_DIR \
make install-static

DEBUG_LEVEL=0 make libz.a libsnappy.a liblz4.a libzstd.a
mv ./*.a ../deps/lib