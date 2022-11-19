#!/usr/bin/env bash

export ANDROID_HOME=/Users/$USER/Library/Android/sdk
export NDK_HOME=/Users/$USER/Library/Android/sdk/ndk/23.1.7779620/
export PATH=$PATH:$HOME/Library/Android/sdk/ndk/23.1.7779620/toolchains/llvm/prebuilt/darwin-x86_64/bin/
export CC=aarch64-linux-android23-clang
export CXX=aarch64-linux-android23-clang++
export LD=$HOME/Library/Android/sdk/ndk/23.1.7779620/toolchains/llvm/prebuilt/darwin-x86_64/bin/ld
export LD_LIBRARY_PATH=/Users/zh217/Library/Android/sdk/ndk/23.1.7779620/toolchains/llvm/prebuilt/darwin-x86_64/sysroot/usr/lib/aarch64-linux-android/26
cargo build --release -p cozo_c --target=aarch64-linux-android