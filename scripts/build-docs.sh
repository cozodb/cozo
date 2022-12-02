#!/usr/bin/env bash

set -e

VERSION=$(cat ./VERSION)

cargo doc -p cozo --no-default-features

mv target/doc ~/cozodb_site/$VERSION/docs.rs