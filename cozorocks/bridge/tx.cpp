//
// Created by Ziyang Hu on 2022/7/3.
//

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
}