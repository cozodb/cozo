#!/usr/bin/env bash

set -e

cd cozoscript-kernel
yarn clean
yarn build:prod
pip install -e .
jupyter labextension develop . --overwrite
cd ..
rm -fr _output
jupyter lite build --no-unused-shared-packages --no-sourcemaps --force --settings-overrides="overrides.json"