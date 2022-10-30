#!/usr/bin/env bash
set -e

VERSION=$(cat ./Cargo.toml | grep -E "^version" | grep -Eo '[0-9.]+')

rm -fr release
mkdir release

cross build --target=aarch64-unknown-linux-gnu --release
cross build --target=x86_64-unknown-linux-gnu --release

cp target/aarch64-unknown-linux-gnu/release/cozoserver release/cozoserver-${VERSION}-linux-arm64
cp target/x86_64-unknown-linux-gnu/release/cozoserver release/cozoserver-${VERSION}-linux-x86_64
podman run --rm -v $PWD:/work ghcr.io/cross-rs/aarch64-unknown-linux-gnu:main strip /work/release/cozoserver-${VERSION}-linux-arm64
podman run --rm -v $PWD:/work ghcr.io/cross-rs/x86_64-unknown-linux-gnu:main strip /work/release/cozoserver-${VERSION}-linux-x86_64
gzip release/cozoserver-${VERSION}-linux-arm64
gzip release/cozoserver-${VERSION}-linux-x86_64