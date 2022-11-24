#!/usr/bin/env bash
set -e

THISDIR=$(dirname $0)
cd $THISDIR

export SWIFT_BRIDGE_OUT_DIR="$(pwd)/generated"
# Build the project for the desired platforms:
cargo build -p cozo-swift --target x86_64-apple-darwin --release
cargo build -p cozo-swift --target aarch64-apple-darwin --release
mkdir -p ../target/universal-macos/release

lipo \
    ../target/aarch64-apple-darwin/release/libcozo_swift.a \
    ../target/x86_64-apple-darwin/release/libcozo_swift.a -create -output \
    ../target/universal-macos/release/libcozo_swift.a

cargo build -p cozo-swift --target aarch64-apple-ios --release
cargo build -p cozo-swift --target x86_64-apple-ios --release
cargo build -p cozo-swift --target aarch64-apple-ios-sim --release

mkdir -p ../target/universal-ios/release

lipo \
    ../target/aarch64-apple-ios-sim/release/libcozo_swift.a \
    ../target/x86_64-apple-ios/release/libcozo_swift.a -create -output \
    ../target/universal-ios/release/libcozo_swift.a

swift-bridge-cli create-package \
    --bridges-dir ./generated \
    --out-dir CozoSwiftBridge \
    --ios ../target/aarch64-apple-ios/release/libcozo_swift.a \
    --simulator ../target/universal-ios/release/libcozo_swift.a \
    --macos ../target/universal-macos/release/libcozo_swift.a \
    --name CozoSwiftBridge