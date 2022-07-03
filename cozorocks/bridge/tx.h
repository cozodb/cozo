//
// Created by Ziyang Hu on 2022/7/3.
//

#ifndef COZOROCKS_TX_H
#define COZOROCKS_TX_H

#include "common.h"
#include "slice.h"
#include "status.h"

struct TxBridge {
    OptimisticTransactionDB *odb;
    TransactionDB *tdb;
    unique_ptr<Transaction> tx;
    unique_ptr<WriteOptions> w_opts;
    unique_ptr<ReadOptions> r_opts;
    unique_ptr<OptimisticTransactionOptions> o_tx_opts;
    unique_ptr<TransactionOptions> p_tx_opts;

    TxBridge(OptimisticTransactionDB *odb_) : odb(odb_), tdb(nullptr), w_opts(new WriteOptions),
                                              r_opts(new ReadOptions),
                                              o_tx_opts(new OptimisticTransactionOptions), p_tx_opts(nullptr), tx() {}

    TxBridge(TransactionDB *tdb_) : odb(nullptr), tdb(tdb_), w_opts(new WriteOptions), o_tx_opts(nullptr),
                                    r_opts(new ReadOptions),
                                    p_tx_opts(new TransactionOptions), tx() {}

    WriteOptions &get_w_opts() {
        return *w_opts;
    }

    inline void set_snapshot() {
        if (tx != nullptr) {
            tx->SetSnapshot();
        } else if (o_tx_opts != nullptr) {
            o_tx_opts->set_snapshot = true;
        } else if (p_tx_opts != nullptr) {
            p_tx_opts->set_snapshot = true;
        }
    }

    inline void clear_snapshot() {
        tx->ClearSnapshot();
    }

    inline DB *get_db() const {
        if (tdb != nullptr) {
            return tdb;
        } else {
            return odb;
        }
    }

    void start();

    inline unique_ptr<PinnableSlice> get(RustBytes key, bool for_update, RdbStatus &status) {
        Slice key_ = convert_slice(key);
        auto ret = make_unique<PinnableSlice>();
        if (for_update) {
            auto s = tx->GetForUpdate(*r_opts, get_db()->DefaultColumnFamily(), key_, &*ret);
            write_status(s, status);
        } else {
            auto s = tx->Get(*r_opts, key_, &*ret);
            write_status(s, status);
        }
        return ret;
    }

    inline void put(RustBytes key, RustBytes val, RdbStatus &status) {
        write_status(tx->Put(convert_slice(key), convert_slice(val)), status);
    }

    inline void del(RustBytes key, RdbStatus &status) {
        write_status(tx->Delete(convert_slice(key)), status);
    }

    inline void commit(RdbStatus &status) {
        write_status(tx->Commit(), status);
    }

    inline void rollback(RdbStatus &status) {
        write_status(tx->Rollback(), status);
    }

    inline void rollback_to_savepoint(RdbStatus &status) {
        write_status(tx->RollbackToSavePoint(), status);
    }

    inline void pop_savepoint(RdbStatus &status) {
        write_status(tx->PopSavePoint(), status);
    }

    inline void set_savepoint() {
        tx->SetSavePoint();
    }
};

#endif //COZOROCKS_TX_H
