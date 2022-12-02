#!/usr/bin/env bash

VERSION=$(cat ./VERSION)

gzip release/*java*.dll
gzip release/libcozo_node*

cd release

for f in *.exe *.dll *.lib; do
  zip $f.zip $f
  rm $f
done

cd ..

gzip release/*.a release/*.so release/*.dylib release/*-darwin release/*-gnu release/*-musl

mkdir -p cozo-lib-nodejs/build/stage/$VERSION/

cp release/libcozo_node-$VERSION-aarch64-apple-darwin.dylib.gz cozo-lib-nodejs/build/stage/$VERSION/6-darwin-arm64.tar.gz
cp release/libcozo_node-$VERSION-x86_64-apple-darwin.dylib.gz cozo-lib-nodejs/build/stage/$VERSION/6-darwin-x64.tar.gz
cp release/libcozo_node-$VERSION-x86_64-unknown-linux-gnu.so.gz cozo-lib-nodejs/build/stage/$VERSION/6-linux-x64.tar.gz
cp release/libcozo_node-$VERSION-aarch64-unknown-linux-gnu.so.gz cozo-lib-nodejs/build/stage/$VERSION/6-linux-arm64.tar.gz
cp release/libcozo_node-$VERSION-x86_64-pc-windows-msvc.dll.gz cozo-lib-nodejs/build/stage/$VERSION/6-win32-x64.tar.gz

for f in release/*; do
  gpg --armor --detach-sign $f
done