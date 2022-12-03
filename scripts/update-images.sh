#!/usr/bin/env bash
#
# Copyright 2022, The Cozo Project Authors.
#
# This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
# If a copy of the MPL was not distributed with this file,
# You can obtain one at https://mozilla.org/MPL/2.0/.
#

podman pull ghcr.io/pyo3/maturin:latest
podman pull ghcr.io/cross-rs/x86_64-unknown-linux-musl:main
podman pull ghcr.io/cross-rs/aarch64-unknown-linux-musl:main
podman pull ghcr.io/cross-rs/x86_64-linux-android:main
podman pull ghcr.io/cross-rs/i686-linux-android:main
podman pull ghcr.io/cross-rs/aarch64-linux-android:main
podman pull ghcr.io/cross-rs/armv7-linux-androideabi:main
podman pull ghcr.io/cross-rs/x86_64-unknown-linux-gnu:main
podman pull ghcr.io/cross-rs/aarch64-unknown-linux-gnu:main