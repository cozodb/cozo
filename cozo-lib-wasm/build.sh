#!/usr/bin/env bash

CARGO_PROFILE_RELEASE_LTO=fat wasm-pack build --target web --release
echo "Copying js-bindings to pkg..."
cp src/js/* pkg/
