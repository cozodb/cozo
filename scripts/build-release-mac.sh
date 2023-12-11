#!/usr/bin/env bash

set -e

VERSION=$(cat ./VERSION)
export MACOSX_DEPLOYMENT_TARGET=10.14

#rm -fr release
mkdir -p release

for TARGET in aarch64-apple-darwin x86_64-apple-darwin; do
  # standalone, c, java, nodejs
  CARGO_PROFILE_RELEASE_LTO=fat cargo build --release -p cozo-bin -p cozo_c -p cozo_java -p cozo-node -F compact -F storage-rocksdb --target $TARGET
  cp target/$TARGET/release/cozo-bin release/cozo-$VERSION-$TARGET # standalone
  cp target/$TARGET/release/libcozo_c.a release/libcozo_c-$VERSION-$TARGET.a # c static
  cp target/$TARGET/release/libcozo_c.dylib release/libcozo_c-$VERSION-$TARGET.dylib # c dynamic
  cp target/$TARGET/release/libcozo_java.dylib release/libcozo_java-$VERSION-$TARGET.dylib # java
  cp target/$TARGET/release/libcozo_node.dylib release/libcozo_node-$VERSION-$TARGET.dylib # nodejs

  # python
  cd cozo-lib-python
  CARGO_PROFILE_RELEASE_LTO=fat PYO3_NO_PYTHON=1 maturin build -F compact -F storage-rocksdb --release --strip --target $TARGET
  cd ..
done

# copy python
cp target/wheels/*.whl release/

# swift
cd cozo-lib-swift
CARGO_PROFILE_RELEASE_LTO=fat ./build-rust.sh
cd ..

# with TiKV
#for TARGET in aarch64-apple-darwin x86_64-apple-darwin; do
#  CARGO_PROFILE_RELEASE_LTO=fat cargo build --release -p cozo-bin \
#    -F compact -F storage-rocksdb -F storage-tikv -F storage-sled --target $TARGET
#  cp target/$TARGET/release/cozo-bin release/cozo_all-$VERSION-$TARGET # standalone
#done

# WASM
cd cozo-lib-wasm
CARGO_PROFILE_RELEASE_LTO=fat ./build.sh
cd ..

cp cozo-lib-c/cozo_c.h release/

zip release/cozo_wasm-$VERSION-wasm32-unknown-unknown.zip $(find cozo-lib-wasm/pkg)