#!/usr/bin/env bash
set -e

VERSION=$(cat ./Cargo.toml | grep -E "^version" | grep -Eo '[0-9.]+')

rm -fr release
mkdir release

cross build --target=aarch64-unknown-linux-gnu --release
cross build --target=aarch64-unknown-linux-gnu --release --manifest-path=cozo-lib-c/Cargo.toml
cross build --target=aarch64-unknown-linux-gnu --release --manifest-path=cozo-lib-java/Cargo.toml
cross build --target=x86_64-unknown-linux-gnu --release
cross build --target=x86_64-unknown-linux-gnu --release --manifest-path=cozo-lib-c/Cargo.toml
cross build --target=x86_64-unknown-linux-gnu --release --manifest-path=cozo-lib-java/Cargo.toml

cp target/aarch64-unknown-linux-gnu/release/cozoserver release/cozoserver-${VERSION}-linux-arm64
cp target/aarch64-unknown-linux-gnu/release/libcozo_c.a release/libcozo_c-${VERSION}-linux-arm64.a
cp target/aarch64-unknown-linux-gnu/release/libcozo_c.so release/libcozo_c-${VERSION}-linux-arm64.so
cp target/aarch64-unknown-linux-gnu/release/libcozo_java.so release/libcozo_java-${VERSION}-linux-arm64.so
cp target/x86_64-unknown-linux-gnu/release/cozoserver release/cozoserver-${VERSION}-linux-x86_64
cp target/x86_64-unknown-linux-gnu/release/libcozo_c.a release/libcozo_c-${VERSION}-linux-x86_64.a
cp target/x86_64-unknown-linux-gnu/release/libcozo_c.so release/libcozo_c-${VERSION}-linux-x86_64.so
cp target/x86_64-unknown-linux-gnu/release/libcozo_java.so release/libcozo_java-${VERSION}-linux-x86_64.so
aarch64-linux-gnu-strip release/cozoserver-${VERSION}-linux-arm64
strip release/cozoserver-${VERSION}-linux-x86_64
gzip release/*
