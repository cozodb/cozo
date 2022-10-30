#!/usr/bin/env bash
set -e

VERSION=$(cat ./Cargo.toml | grep -E "^version" | grep -Eo '[0-9.]+')

mkdir release

cross build --target=aarch64-unknown-linux-gnu --release
cross build --target=x86_64-unknown-linux-gnu --release

cp target/aarch64-unknown-linux-gnu/release/cozoserver release/cozoserver-${VERSION}-linux-arm64
cp target/x86_64-unknown-linux-gnu/release/cozoserver release/cozoserver-${VERSION}-linux-x86_64
strip release/cozoserver-${VERSION}-linux-arm64
strip release/cozoserver-${VERSION}-linux-x86_64
gzip release/cozoserver-${VERSION}-linux-arm64
gzip release/cozoserver-${VERSION}-linux-x86_64