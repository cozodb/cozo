#!/usr/bin/env bash

cross build -p cozo_java --release --target=aarch64-linux-android
cross build -p cozo_java --release --target=armv7-linux-androideabi
cross build -p cozo_java --release --target=i686-linux-android
cross build -p cozo_java --release --target=x86_64-linux-android
cross build -p cozo_java --release -F storage-rocksdb --target=aarch64-unknown-linux-gnu
cross build -p cozo_java --release -F storage-rocksdb --target=x86_64-unknown-linux-gnu