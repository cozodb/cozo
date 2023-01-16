#
# Copyright 2022, The Cozo Project Authors.
#
# This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
# If a copy of the MPL was not distributed with this file,
# You can obtain one at https://mozilla.org/MPL/2.0/.
#

cargo build -p cozo-node -F compact -F storage-rocksdb
mv ../target/debug/libcozo_node.dylib native/6/cozo_node_prebuilt.node
node example.js