//
// Created by Ziyang Hu on 2022/4/13.
//

#pragma once

#include <memory>
#include "rust/cxx.h"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

struct Status;

typedef ROCKSDB_NAMESPACE::Status::Code StatusCode;
typedef ROCKSDB_NAMESPACE::Status::SubCode StatusSubCode;
typedef ROCKSDB_NAMESPACE::Status::Severity StatusSeverity;

std::unique_ptr<ROCKSDB_NAMESPACE::DB> new_db();

struct Options {
    mutable ROCKSDB_NAMESPACE::Options inner;

public:
    inline void prepare_for_bulk_load() const {
        inner.PrepareForBulkLoad();
    }

    inline void increase_parallelism() const {
        inner.IncreaseParallelism();
    }

    inline void optimize_level_style_compaction() const {
        inner.OptimizeLevelStyleCompaction();
    };

    inline void set_create_if_missing(bool v) const {
        inner.create_if_missing = v;
    }
};

inline std::unique_ptr<Options> new_options() {
    return std::unique_ptr<Options>(new Options);
}


struct PinnableSlice {
    ROCKSDB_NAMESPACE::PinnableSlice inner;

    inline rust::Slice<const std::uint8_t> as_bytes() const {
        return rust::Slice(reinterpret_cast<const std::uint8_t *>(inner.data()), inner.size());
    }
};


struct DB {
    mutable ROCKSDB_NAMESPACE::DB *inner;

    inline ~DB() {
        if (inner != nullptr) {
            delete inner;
        }
    }

    void put(rust::Slice<const uint8_t> key, rust::Slice<const uint8_t> val, Status &status) const;

    inline std::unique_ptr<PinnableSlice> get(rust::Slice<const uint8_t> key) const {
        auto pinnable_val = std::make_unique<PinnableSlice>();
        inner->Get(ROCKSDB_NAMESPACE::ReadOptions(),
                   inner->DefaultColumnFamily(),
                   ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char *>(key.data()), key.size()),
                   &pinnable_val->inner);
        return pinnable_val;
    }
};

inline std::unique_ptr<DB> open_db(const Options &options, const rust::Str path) {
    ROCKSDB_NAMESPACE::DB *db_ptr;
    ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::DB::Open(options.inner, std::string(path), &db_ptr);
    auto db = std::unique_ptr<DB>(new DB);
    db->inner = db_ptr;
    return db;
}