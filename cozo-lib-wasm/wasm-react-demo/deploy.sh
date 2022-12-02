#!/usr/bin/env bash

rm -fr node_modules
rm -fr build
yarn
yarn build
rm -fr ~/cozodb_site/wasm-demo/
mv build ~/cozodb_site/wasm-demo