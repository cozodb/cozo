/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

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