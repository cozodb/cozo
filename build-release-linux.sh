#!/usr/bin/env bash
set -e

VERSION=$(cat ./Cargo.toml | grep -E "^version" | grep -Eo '[0-9.]+')

mkdir release

cross build --target=aarch64-unknown-linux-gnu --release
cross build --target=x86_64-unknown-linux-gnu --release

mv target/aarch64-unknown-linux-gnu/release/cozoserver release/cozoserver-linux-arm64
mv target/x86_64-unknown-linux-gnu/release/cozoserver release/cozoserver-linux-x86_64