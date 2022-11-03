#!/usr/bin/env bash

set -e

VERSION=$(cat ./Cargo.toml | grep -E "^version" | grep -Eo '[0-9.]+')
ARCH=$(uname -m)

rm -fr release
mkdir release

cargo build --release
cargo build --release --manifest-path=cozo-lib-c/Cargo.toml
cargo build --release --manifest-path=cozo-lib-java/Cargo.toml

cp target/release/cozoserver release/cozoserver-${VERSION}-mac-${ARCH}
cp target/release/libcozo_c.a release/libcozo_c-${VERSION}-mac-${ARCH}.a
cp target/release/libcozo_c.dylib release/libcozo_c-${VERSION}-mac-${ARCH}.dylib
cp target/release/libcozo_java.dylib release/libcozo_java-${VERSION}-mac-${ARCH}.dylib
strip release/cozoserver-${VERSION}-mac-${ARCH}

gzip release/*