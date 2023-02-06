#!/usr/bin/env bash

PYO3_NO_PYTHON=1 maturin build -F compact -F storage-rocksdb --release --strip