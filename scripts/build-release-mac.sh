#!/usr/bin/env bash

set -e

VERSION=$(cat ./VERSION)

#rm -fr release
mkdir -p release

for TARGET in x86_64-apple-darwin aarch64-apple-darwin; do
  # standalone, c, java, nodejs
  CARGO_PROFILE_RELEASE_LTO=fat cargo build --release -p cozoserver -p cozo_c -p cozo_java -p cozo-node -F compact -F storage-rocksdb --target $TARGET
  cp target/$TARGET/release/cozoserver release/cozoserver-$VERSION-$TARGET # standalone
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
cargo clean
cd cozo-lib-swift
CARGO_PROFILE_RELEASE_LTO=fat ./build-rust.sh
cd ..

# WASM
cd cozo-lib-wasm
CARGO_PROFILE_RELEASE_LTO=fat ./build.sh
cd ..
