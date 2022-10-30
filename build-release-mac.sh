#!/usr/bin/env bash

set -e

VERSION=$(cat ./Cargo.toml | grep -E "^version" | grep -Eo '[0-9.]+')
ARCH=$(arch)

rm -fr release
mkdir release

cargo build --release

cp target/release/cozoserver release/cozoserver-${VERSION}-mac-${ARCH}
strip release/cozoserver-${VERSION}-mac-${ARCH}
gzip release/*