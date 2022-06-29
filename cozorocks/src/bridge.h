//
// Created by Ziyang Hu on 2022/6/29.
//

#ifndef COZOROCKS_ADDITIONS_H
#define COZOROCKS_ADDITIONS_H

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"

namespace rocksdb_additions {

    using namespace rocksdb;

    // for write options

    void set_w_opts_sync(WriteOptions &opts, bool v) {
        Status s;
        opts.sync = v;
    }

    void set_w_opts_disable_wal(WriteOptions &opts, bool v) {
        opts.disableWAL = v;
    }

    void set_w_opts_low_pri(WriteOptions &opts, bool v) {
        opts.low_pri = v;
    }

    // for read options

    void set_iterate_lower_bound(ReadOptions &opts, const Slice &lower_bound) {
        opts.iterate_lower_bound = &lower_bound;
    }

    void set_iterate_upper_bound(ReadOptions &opts, const Slice &lower_bound) {
        opts.iterate_upper_bound = &lower_bound;
    }

    void set_snapshot(ReadOptions &opts, const Snapshot &snapshot) {
        opts.snapshot = &snapshot;
    }

    void set_r_opts_total_order_seek(ReadOptions &opts, bool total_order_seek) {
        opts.total_order_seek = total_order_seek;
    }

    void set_r_opts_auto_prefix_mode(ReadOptions &opts, bool auto_prefix_mode) {
        opts.auto_prefix_mode = auto_prefix_mode;
    }

    void set_r_opts_prefix_same_as_start(ReadOptions &opts, bool prefix_same_as_start) {
        opts.prefix_same_as_start = prefix_same_as_start;
    }

    void set_r_opts_tailing(ReadOptions &opts, bool tailing) {
        opts.tailing = tailing;
    }

    void set_r_opts_pin_data(ReadOptions &opts, bool pin_data) {
        opts.pin_data = pin_data;
    }

    void set_r_opts_verify_checksums(ReadOptions &opts, bool verify_checksums) {
        opts.verify_checksums = verify_checksums;
    }

    void set_r_opts_fill_cache(ReadOptions &opts, bool fill_cache) {
        opts.fill_cache = fill_cache;
    }
}

#endif //COZOROCKS_ADDITIONS_H
