#!/usr/bin/env bash

set -e

VERSION=$(cat ./VERSION)

for TARGET in aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu; do
  # standalone, c, java, nodejs
  CARGO_PROFILE_RELEASE_LTO=fat cross build --release -p cozoserver -p cozo_c -p cozo_java -p cozo-node -F compact -F storage-rocksdb --target $TARGET
  cp target/$TARGET/release/cozoserver release/cozoserver-$VERSION-$TARGET # standalone
  cp target/$TARGET/release/libcozo_c.a release/libcozo_c-$VERSION-$TARGET.a # c static
  cp target/$TARGET/release/libcozo_c.so release/libcozo_c-$VERSION-$TARGET.so # c dynamic
  cp target/$TARGET/release/libcozo_java.so release/libcozo_java-$VERSION-$TARGET.so # java
  cp target/$TARGET/release/libcozo_node.so release/libcozo_node-$VERSION-$TARGET.so # nodejs

  # python
#  cd cozo-lib-python
#  CARGO_PROFILE_RELEASE_LTO=fat PYO3_NO_PYTHON=1 maturin build -F compact -F storage-rocksdb --release --strip --target $TARGET
#  cd ..
done

##rm -fr release
#mkdir -p release
#
#cross build --target=aarch64-unknown-linux-gnu --release
#cross build --target=aarch64-unknown-linux-gnu --release --manifest-path=cozo-lib-c/Cargo.toml
#cross build --target=x86_64-unknown-linux-gnu --release
#cross build --target=x86_64-unknown-linux-gnu --release --manifest-path=cozo-lib-c/Cargo.toml
#cross build --target=x86_64-unknown-linux-gnu --release --manifest-path=cozo-lib-java/Cargo.toml
#
#cp target/aarch64-unknown-linux-gnu/release/cozoserver release/cozoserver-${VERSION}-linux-arm64
#cp target/aarch64-unknown-linux-gnu/release/libcozo_c.a release/libcozo_c-${VERSION}-linux-arm64.a
#cp target/aarch64-unknown-linux-gnu/release/libcozo_c.so release/libcozo_c-${VERSION}-linux-arm64.so
#cp target/x86_64-unknown-linux-gnu/release/cozoserver release/cozoserver-${VERSION}-linux-x86_64
#cp target/x86_64-unknown-linux-gnu/release/libcozo_c.a release/libcozo_c-${VERSION}-linux-x86_64.a
#cp target/x86_64-unknown-linux-gnu/release/libcozo_c.so release/libcozo_c-${VERSION}-linux-x86_64.so
#cp target/x86_64-unknown-linux-gnu/release/libcozo_java.so release/libcozo_java-${VERSION}-linux-x86_64.so
#aarch64-linux-gnu-strip release/cozoserver-${VERSION}-linux-arm64
#strip release/cozoserver-${VERSION}-linux-x86_64
#gzip release/*
