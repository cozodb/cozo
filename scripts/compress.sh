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

gzip release/*.a release/*.so release/*.dylib release/*-darwin release/*-gnu

NODE_DIR=cozo-lib-nodejs/build/stage/$VERSION/
NODE_DIR_INNER=cozo-lib-nodejs/build/stage/$VERSION/6

rm -fr $NODE_DIR_INNER
mkdir -p $NODE_DIR_INNER
cp release/libcozo_node-$VERSION-aarch64-apple-darwin.dylib.gz $NODE_DIR_INNER/cozo_node_prebuilt.node.gz
pushd $NODE_DIR
gunzip 6/cozo_node_prebuilt.node.gz
tar cvzf 6-darwin-arm64.tar.gz 6/
popd

rm -fr $NODE_DIR_INNER
mkdir -p $NODE_DIR_INNER
cp release/libcozo_node-$VERSION-x86_64-apple-darwin.dylib.gz $NODE_DIR_INNER/cozo_node_prebuilt.node.gz
pushd $NODE_DIR
gunzip 6/cozo_node_prebuilt.node.gz
tar cvzf 6-darwin-x64.tar.gz 6/
popd

rm -fr $NODE_DIR_INNER
mkdir -p $NODE_DIR_INNER
cp release/libcozo_node-$VERSION-x86_64-unknown-linux-gnu.so.gz $NODE_DIR_INNER/cozo_node_prebuilt.node.gz
pushd $NODE_DIR
gunzip 6/cozo_node_prebuilt.node.gz
tar cvzf 6-linux-x64.tar.gz 6/
popd

rm -fr $NODE_DIR_INNER
mkdir -p $NODE_DIR_INNER
cp release/libcozo_node-$VERSION-aarch64-unknown-linux-gnu.so.gz $NODE_DIR_INNER/cozo_node_prebuilt.node.gz
pushd $NODE_DIR
gunzip 6/cozo_node_prebuilt.node.gz
tar cvzf 6-linux-arm64.tar.gz 6/
popd

rm -fr $NODE_DIR_INNER
mkdir -p $NODE_DIR_INNER
cp release/libcozo_node-$VERSION-x86_64-pc-windows-msvc.dll.gz $NODE_DIR_INNER/cozo_node_prebuilt.node.gz
pushd $NODE_DIR
gunzip 6/cozo_node_prebuilt.node.gz
tar cvzf 6-win32-x64.tar.gz 6/
popd

for f in release/*; do
  gpg --armor --detach-sign $f
done