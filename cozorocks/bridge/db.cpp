//
// Created by Ziyang Hu on 2022/7/3.
//

#include <iostream>
#include <memory>
#include "db.h"
#include "cozorocks/src/bridge/mod.rs.h"

shared_ptr<RawRocksDbBridge>
open_raw_db(const DbOpts &opts, RocksDbStatus &status, bool use_cmp, RustComparatorFn cmp_impl, bool no_wal) {
    auto options = make_unique<Options>();
    if (opts.prepare_for_bulk_load) {
        options->PrepareForBulkLoad();
    }
    if (opts.increase_parallelism > 0) {
        options->IncreaseParallelism(opts.increase_parallelism);
    }
    if (opts.optimize_level_style_compaction) {
        options->OptimizeLevelStyleCompaction();
    }
    options->create_if_missing = opts.create_if_missing;
    options->paranoid_checks = opts.paranoid_checks;
    if (opts.enable_blob_files) {
        options->enable_blob_files = true;
        options->min_blob_size = opts.min_blob_size;
        options->blob_file_size = opts.blob_file_size;
        options->enable_blob_garbage_collection = opts.enable_blob_garbage_collection;
    }
    if (opts.use_bloom_filter) {
        BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(NewBloomFilterPolicy(opts.bloom_filter_bits_per_key, false));
        table_options.whole_key_filtering = opts.bloom_filter_whole_key_filtering;
        options->table_factory.reset(NewBlockBasedTableFactory(table_options));
    }
    if (opts.use_capped_prefix_extractor) {
        options->prefix_extractor.reset(NewCappedPrefixTransform(opts.capped_prefix_extractor_len));
    }
    if (opts.use_fixed_prefix_extractor) {
        options->prefix_extractor.reset(NewFixedPrefixTransform(opts.fixed_prefix_extractor_len));
    }
    RustComparator *cmp = nullptr;
    if (use_cmp) {
        cmp = new RustComparator(
                string(opts.comparator_name),
                opts.comparator_different_bytes_can_be_equal,
                cmp_impl);
        options->comparator = cmp;
    }

    shared_ptr<RawRocksDbBridge> db_wrapper = shared_ptr<RawRocksDbBridge>(nullptr);
    auto db = new RawRocksDbBridge();
    db->options = std::move(options);
    db->db_path = string(opts.db_path);
    db->comparator.reset(cmp);

    DB *db_ptr = nullptr;
    write_status(DB::Open(*db->options, db->db_path, &db_ptr), status);
    db->db.reset(db_ptr);
    db->destroy_on_exit = opts.destroy_on_exit;
    db->r_opts = std::make_unique<ReadOptions>();
    db->w_opts = std::make_unique<WriteOptions>();
    if (no_wal) {
        db->w_opts->disableWAL = true;
    }
    db_wrapper.reset(db);
    return db_wrapper;
}

shared_ptr<RocksDbBridge> open_db(const DbOpts &opts, RocksDbStatus &status, bool use_cmp, RustComparatorFn cmp_impl) {
    auto options = make_unique<Options>();
    if (opts.prepare_for_bulk_load) {
        options->PrepareForBulkLoad();
    }
    if (opts.increase_parallelism > 0) {
        options->IncreaseParallelism(opts.increase_parallelism);
    }
    if (opts.optimize_level_style_compaction) {
        options->OptimizeLevelStyleCompaction();
    }
    options->create_if_missing = opts.create_if_missing;
    options->paranoid_checks = opts.paranoid_checks;
    if (opts.enable_blob_files) {
        options->enable_blob_files = true;
        options->min_blob_size = opts.min_blob_size;
        options->blob_file_size = opts.blob_file_size;
        options->enable_blob_garbage_collection = opts.enable_blob_garbage_collection;
    }
    if (opts.use_bloom_filter) {
        BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(NewBloomFilterPolicy(opts.bloom_filter_bits_per_key, false));
        table_options.whole_key_filtering = opts.bloom_filter_whole_key_filtering;
        options->table_factory.reset(NewBlockBasedTableFactory(table_options));
    }
    if (opts.use_capped_prefix_extractor) {
        options->prefix_extractor.reset(NewCappedPrefixTransform(opts.capped_prefix_extractor_len));
    }
    if (opts.use_fixed_prefix_extractor) {
        options->prefix_extractor.reset(NewFixedPrefixTransform(opts.fixed_prefix_extractor_len));
    }
    RustComparator *cmp = nullptr;
    if (use_cmp) {
        cmp = new RustComparator(
                string(opts.comparator_name),
                opts.comparator_different_bytes_can_be_equal,
                cmp_impl);
        options->comparator = cmp;
    }

    shared_ptr<RocksDbBridge> db_wrapper = shared_ptr<RocksDbBridge>(nullptr);
    if (opts.optimistic) {
        auto db = new OptimisticRocksDb();
        db->options = std::move(options);
        db->db_path = string(opts.db_path);
        db->comparator.reset(cmp);

        OptimisticTransactionDB *txn_db = nullptr;
        write_status(OptimisticTransactionDB::Open(*db->options, db->db_path, &txn_db), status);
        db->db.reset(txn_db);
        db->destroy_on_exit = opts.destroy_on_exit;
        db_wrapper.reset(db);
    } else {
        auto db = new PessimisticRocksDb();
        db->options = std::move(options);
        db->db_path = string(opts.db_path);
        db->tdb_opts = make_unique<TransactionDBOptions>();
        db->comparator.reset(cmp);

        TransactionDB *txn_db = nullptr;
        write_status(TransactionDB::Open(*db->options, *db->tdb_opts, db->db_path, &txn_db), status);
        db->db.reset(txn_db);
        db->destroy_on_exit = opts.destroy_on_exit;
        db_wrapper.reset(db);
    }

    return db_wrapper;
}

PessimisticRocksDb::~PessimisticRocksDb() {
    if (destroy_on_exit && (db != nullptr)) {
        cerr << "destroying database on exit: " << db_path << endl;
        auto status = db->Close();
        if (!status.ok()) {
            cerr << status.ToString() << endl;
        }
        db.reset();
        auto status2 = DestroyDB(db_path, *options);
        if (!status2.ok()) {
            cerr << status2.ToString() << endl;
        }
    }
}

OptimisticRocksDb::~OptimisticRocksDb() {
    if (destroy_on_exit) {
        cerr << "destroying database on exit: " << db_path << endl;
        auto status = db->Close();
        if (!status.ok()) {
            cerr << status.ToString() << endl;
        }
        db.reset();
        auto status2 = DestroyDB(db_path, *options);
        if (!status2.ok()) {
            cerr << status.ToString() << endl;
        }
    }
}