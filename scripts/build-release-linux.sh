#!/usr/bin/env bash

set -e

VERSION=$(cat ./VERSION)

#rm -fr release
mkdir -p release

# python
CARGO_NET_GIT_FETCH_WITH_CLI=true podman run --rm -v $(pwd):/io -w /io/cozo-lib-python ghcr.io/pyo3/maturin:latest build --release --strip -F compact -F storage-rocksdb
# copy python
cp target/wheels/*.whl release/

for TARGET in aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu; do
  # standalone, c, java, nodejs
  CROSS_CONTAINER_ENGINE=podman CARGO_PROFILE_RELEASE_LTO=fat cross build --release -p cozo-bin -p cozo_c -p cozo_java -p cozo-node -F compact -F storage-rocksdb --target $TARGET
  cp target/$TARGET/release/cozo-bin release/cozo-$VERSION-$TARGET # standalone
  cp target/$TARGET/release/libcozo_c.a release/libcozo_c-$VERSION-$TARGET.a # c static
  cp target/$TARGET/release/libcozo_c.so release/libcozo_c-$VERSION-$TARGET.so # c dynamic
  cp target/$TARGET/release/libcozo_java.so release/libcozo_java-$VERSION-$TARGET.so # java
  cp target/$TARGET/release/libcozo_node.so release/libcozo_node-$VERSION-$TARGET.so # nodejs

done

#for TARGET in x86_64-unknown-linux-gnu; do
#  CROSS_CONTAINER_ENGINE=podman PROTOC=$PWD/tools/protoc CARGO_PROFILE_RELEASE_LTO=fat cross build --release -p cozo-bin \
#    -F compact -F storage-rocksdb -F storage-tikv -F storage-sled --target $TARGET
#  cp target/$TARGET/release/cozo-bin release/cozo_all-$VERSION-$TARGET # standalone
#done

#for TARGET in aarch64-unknown-linux-musl x86_64-unknown-linux-musl; do
#  CROSS_CONTAINER_ENGINE=podman CARGO_PROFILE_RELEASE_LTO=fat cross build --release -p cozo-bin -p cozo_c -F compact -F storage-rocksdb --target $TARGET
#  cp target/$TARGET/release/cozo-bin release/cozo-$VERSION-$TARGET # standalone
#  cp target/$TARGET/release/libcozo_c.a release/libcozo_c-$VERSION-$TARGET.a # c static
#done

for TARGET in aarch64-linux-android armv7-linux-androideabi i686-linux-android x86_64-linux-android; do
  CROSS_CONTAINER_ENGINE=podman CARGO_PROFILE_RELEASE_LTO=fat cross build -p cozo_java --release --target=$TARGET
  cp target/$TARGET/release/libcozo_java.so release/libcozo_java-$VERSION-$TARGET.so # java
done
