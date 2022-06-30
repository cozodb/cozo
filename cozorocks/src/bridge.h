//
// Created by Ziyang Hu on 2022/6/29.
//

#ifndef COZOROCKS_ADDITIONS_H
#define COZOROCKS_ADDITIONS_H

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"

namespace rocksdb_additions {

    using namespace std;
    using namespace rocksdb;

    // for write options

    // force generation

    unique_ptr<Iterator> _u1() {
        return unique_ptr<Iterator>(nullptr);
    }

    unique_ptr<Transaction> _u2() {
        return unique_ptr<Transaction>(nullptr);
    }


    inline void set_w_opts_sync(WriteOptions &opts, bool v) {
        Status s;
        opts.sync = v;
    }

    inline void set_w_opts_disable_wal(WriteOptions &opts, bool v) {
        opts.disableWAL = v;
    }

    inline void set_w_opts_low_pri(WriteOptions &opts, bool v) {
        opts.low_pri = v;
    }

    // for read options

    inline void set_iterate_lower_bound(ReadOptions &opts, const Slice &lower_bound) {
        opts.iterate_lower_bound = &lower_bound;
    }

    inline void set_iterate_upper_bound(ReadOptions &opts, const Slice &lower_bound) {
        opts.iterate_upper_bound = &lower_bound;
    }

    inline void set_snapshot(ReadOptions &opts, const Snapshot &snapshot) {
        opts.snapshot = &snapshot;
    }

    inline void set_r_opts_total_order_seek(ReadOptions &opts, bool total_order_seek) {
        opts.total_order_seek = total_order_seek;
    }

    inline void set_r_opts_auto_prefix_mode(ReadOptions &opts, bool auto_prefix_mode) {
        opts.auto_prefix_mode = auto_prefix_mode;
    }

    inline void set_r_opts_prefix_same_as_start(ReadOptions &opts, bool prefix_same_as_start) {
        opts.prefix_same_as_start = prefix_same_as_start;
    }

    inline void set_r_opts_tailing(ReadOptions &opts, bool tailing) {
        opts.tailing = tailing;
    }

    inline void set_r_opts_pin_data(ReadOptions &opts, bool pin_data) {
        opts.pin_data = pin_data;
    }

    inline void set_r_opts_verify_checksums(ReadOptions &opts, bool verify_checksums) {
        opts.verify_checksums = verify_checksums;
    }

    inline void set_r_opts_fill_cache(ReadOptions &opts, bool fill_cache) {
        opts.fill_cache = fill_cache;
    }

    // for options

    inline void set_opts_create_if_mission(Options &opts, bool v) {
        opts.create_if_missing = v;
    }

    inline void set_opts_error_if_exists(Options &opts, bool v) {
        opts.error_if_exists = v;
    }

    inline void set_opts_create_missing_column_families(Options &opts, bool v) {
        opts.create_missing_column_families = v;
    }

    inline void set_opts_paranoid_checks(Options &opts, bool v) {
        opts.paranoid_checks = v;
    }

    inline void set_opts_flush_verify_memtable_count(Options &opts, bool v) {
        opts.flush_verify_memtable_count = v;
    }

    inline void set_opts_track_and_verify_wals_in_manifest(Options &opts, bool v) {
        opts.track_and_verify_wals_in_manifest = v;
    }

    inline void set_opts_verify_sst_unique_id_in_manifest(Options &opts, bool v) {
        opts.verify_sst_unique_id_in_manifest = v;
    }

    inline void set_opts_bloom_filter(Options &options, const double bits_per_key, const bool whole_key_filtering) {
        BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(NewBloomFilterPolicy(bits_per_key, false));
        table_options.whole_key_filtering = whole_key_filtering;
        options.table_factory.reset(
                NewBlockBasedTableFactory(
                        table_options));
    }

    inline void set_opts_capped_prefix_extractor(Options &options, const size_t cap_len) {
        options.prefix_extractor.reset(NewCappedPrefixTransform(cap_len));
    }


    inline void set_opts_comparator(Options &inner, const Comparator &cmp_obj) {
        inner.comparator = &cmp_obj;
    }

    inline void set_opts_enable_blob_files(Options &inner, bool v) {
        inner.enable_blob_files = v;
    }

    inline void set_opts_min_blob_size(Options &inner, uint64_t size) {
        inner.min_blob_size = size;
    }

    inline void set_opts_blob_file_size(Options &inner, uint64_t size) {
        inner.blob_file_size = size;
    }

    inline void set_opts_enable_blob_garbage_collection(Options &inner, bool v) {
        inner.enable_blob_garbage_collection = v;
    }

    // otopts

    inline void set_otopts_comparator(OptimisticTransactionOptions &opts, Comparator &cmp) {
        opts.cmp = &cmp;
    }

    // database

    enum DbKind {
        RAW = 0,
        PESSIMISTIC = 1,
        OPTIMISTIC = 2,
    };

    struct DbBridge {
        mutable unique_ptr<DB> db;
        mutable TransactionDB *tdb;
        mutable OptimisticTransactionDB *odb;
        bool is_odb;

        DbBridge(DB *db_) : db(db_) {}

        DbBridge(TransactionDB *db_) : db(db_), tdb(db_) {}

        DbBridge(OptimisticTransactionDB *db_) : db(db_), odb(db_) {}

        DbKind kind() const {
            if (tdb != nullptr) {
                return DbKind::PESSIMISTIC;
            } else if (odb != nullptr) {
                return DbKind::OPTIMISTIC;
            } else {
                return DbKind::RAW;
            }
        }

        DB *inner_db() const {
            return db.get();
        }

        TransactionDB *inner_tdb() const {
            return tdb;
        }

        OptimisticTransactionDB *inner_odb() const {
            return odb;
        }
    };

    inline shared_ptr<DbBridge>
    open_db_raw(const Options &options, const string &path, Status &status) {
        DB *db = nullptr;

        status = DB::Open(options, path, &db);
        return make_shared<DbBridge>(db);
    }

    inline shared_ptr<DbBridge>
    open_tdb_raw(const Options &options,
                 const TransactionDBOptions &txn_db_options,
                 const string &path,
                 Status &status) {
        TransactionDB *txn_db = nullptr;

        status = TransactionDB::Open(options, txn_db_options, path, &txn_db);

        return make_shared<DbBridge>(txn_db);
    }


    inline shared_ptr<DbBridge>
    open_odb_raw(const Options &options, const string &path, Status &status) {
        OptimisticTransactionDB *txn_db = nullptr;

        status = OptimisticTransactionDB::Open(options,
                                               path,
                                               &txn_db);

        return make_shared<DbBridge>(txn_db);
    }


    // comparator

    typedef int(*CmpFn)(const Slice &a, const Slice &b);

    class RustComparator : public Comparator {
    public:
        inline RustComparator(string name_, bool can_different_bytes_be_equal_, void const *const f) :
                name(name_),
                can_different_bytes_be_equal(can_different_bytes_be_equal_) {
            CmpFn f_ = CmpFn(f);
            ext_cmp = f_;
        }

        inline int Compare(const Slice &a, const Slice &b) const {
            return ext_cmp(a, b);
        }

        inline const char *Name() const {
            return name.c_str();
        }

        inline virtual bool CanKeysWithDifferentByteContentsBeEqual() const {
            return can_different_bytes_be_equal;
        }

        inline void FindShortestSeparator(string *, const Slice &) const {}

        inline void FindShortSuccessor(string *) const {}

        string name;
        CmpFn ext_cmp;
        bool can_different_bytes_be_equal;
    };

    inline unique_ptr<RustComparator>
    new_rust_comparator(
            string name_,
            bool can_different_bytes_be_equal_,
            void const *const f
    ) {
        return make_unique<RustComparator>(name_, can_different_bytes_be_equal_, f);
    }

}

#endif //COZOROCKS_ADDITIONS_H
