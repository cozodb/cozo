// Copyright 2022, The Cozo Project Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

#include "tx.h"
#include "cozorocks/src/bridge/mod.rs.h"

void TxBridge::start() {
    if (odb != nullptr) {
        Transaction *txn = odb->BeginTransaction(*w_opts, *o_tx_opts);
        tx.reset(txn);
    } else if (tdb != nullptr) {
        Transaction *txn = tdb->BeginTransaction(*w_opts, *p_tx_opts);
        tx.reset(txn);
    }
    assert(tx);
}