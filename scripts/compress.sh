#!/usr/bin/env bash

gzip release/*java*.dll

for f in release/*.exe release/*.dll release/*.lib; do
  zip $f.zip $f
  rm $f
done

gzip release/*.a release/*.so release/*.dylib release/*-darwin release/*-gnu release/*-musl