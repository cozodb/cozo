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

namespace RDB = ROCKSDB_NAMESPACE;

typedef RDB::Status::Code StatusCode;
typedef RDB::Status::SubCode StatusSubCode;
typedef RDB::Status::Severity StatusSeverity;

std::unique_ptr <RDB::DB> new_db();

struct ReadOptionsBridge {
    mutable RDB::ReadOptions inner;
};

struct WriteOptionsBridge {
    mutable RDB::WriteOptions inner;

public:
    inline void set_disable_wal(bool v) const {
        inner.disableWAL = v;
    }
};

typedef rust::Fn<std::int8_t(rust::Slice<const std::uint8_t>, rust::Slice<const std::uint8_t>)> RustComparatorFn;

class RustComparator : public RDB::Comparator {
public:
    inline int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
        auto ra = rust::Slice(reinterpret_cast<const std::uint8_t *>(a.data()), a.size());
        auto rb = rust::Slice(reinterpret_cast<const std::uint8_t *>(b.data()), b.size());
        return int(rust_compare(ra, rb));
    }

    const char *Name() const {
        return "RustComparator";
    }

    void FindShortestSeparator(std::string *, const rocksdb::Slice &) const {}

    void FindShortSuccessor(std::string *) const {}

    void set_fn(RustComparatorFn f) const {
        rust_compare = f;
    }

    void set_name(rust::Str name_) const {
        name = std::string(name_);
    }

    mutable std::string name;
    mutable RustComparatorFn rust_compare;
};

struct OptionsBridge {
    mutable RDB::Options inner;
    mutable RustComparator cmp_obj;

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

    inline void set_comparator(rust::Str name, RustComparatorFn f) const {
        cmp_obj = RustComparator();
        cmp_obj.set_name(name);
        cmp_obj.set_fn(f);
        inner.comparator = &cmp_obj;
    }
};

inline std::unique_ptr <ReadOptionsBridge> new_read_options() {
    return std::unique_ptr<ReadOptionsBridge>(new ReadOptionsBridge);
}

inline std::unique_ptr <WriteOptionsBridge> new_write_options() {
    return std::unique_ptr<WriteOptionsBridge>(new WriteOptionsBridge);
}

inline std::unique_ptr <OptionsBridge> new_options() {
    return std::unique_ptr<OptionsBridge>(new OptionsBridge);
}


struct PinnableSliceBridge {
    RDB::PinnableSlice inner;

    inline rust::Slice<const std::uint8_t> as_bytes() const {
        return rust::Slice(reinterpret_cast<const std::uint8_t *>(inner.data()), inner.size());
    }
};

void write_status_impl(Status &status, StatusCode code, StatusSubCode subcode, StatusSeverity severity);

inline void write_status(RDB::Status &&rstatus, Status &status) {
    if (rstatus.code() != StatusCode::kOk || rstatus.subcode() != StatusSubCode::kNoSpace ||
        rstatus.severity() != StatusSeverity::kNoError) {
        write_status_impl(status, rstatus.code(), rstatus.subcode(), rstatus.severity());
    }
}

struct DBBridge {
    mutable std::unique_ptr <RDB::DB> inner;

    DBBridge(RDB::DB *inner_) : inner(inner_) {}

    inline void put(
            const WriteOptionsBridge &options,
            rust::Slice<const uint8_t> key,
            rust::Slice<const uint8_t> val,
            Status &status
    ) const {
        write_status(
                inner->Put(options.inner,
                           RDB::Slice(reinterpret_cast<const char *>(key.data()), key.size()),
                           RDB::Slice(reinterpret_cast<const char *>(val.data()), val.size())),
                status
        );
    }

    inline std::unique_ptr <PinnableSliceBridge> get(
            const ReadOptionsBridge &options,
            rust::Slice<const uint8_t> key,
            Status &status
    ) const {
        auto pinnable_val = std::make_unique<PinnableSliceBridge>();
        write_status(
                inner->Get(options.inner,
                           inner->DefaultColumnFamily(),
                           RDB::Slice(reinterpret_cast<const char *>(key.data()), key.size()),
                           &pinnable_val->inner),
                status
        );
        return pinnable_val;
    }
};

inline std::unique_ptr <std::vector<std::string>> list_column_families(const OptionsBridge &options,
                                                                       const rust::Slice<const uint8_t> path) {
    auto column_families = std::make_unique < std::vector < std::string >> ();
    RDB::DB::ListColumnFamilies(options.inner,
                                std::string(reinterpret_cast<const char *>(path.data()), path.size()),
                                &*column_families);
    return column_families;
}

inline std::unique_ptr <DBBridge> open_db(const OptionsBridge &options, const rust::Slice<const uint8_t> path, Status &status) {
    auto old_column_families = std::vector<std::string>();
    RDB::DB::ListColumnFamilies(options.inner,
                                std::string(reinterpret_cast<const char *>(path.data()), path.size()),
                                &old_column_families);
    if (old_column_families.empty()) {
        old_column_families.push_back(RDB::kDefaultColumnFamilyName);
    }

    std::vector <RDB::ColumnFamilyDescriptor> column_families;

    for (auto el: old_column_families) {
        column_families.push_back(RDB::ColumnFamilyDescriptor(
                el, options.inner));
    }

    std::vector < RDB::ColumnFamilyHandle * > handles;

    RDB::DB *db_ptr;
    RDB::Status s = RDB::DB::Open(options.inner,
                                  std::string(reinterpret_cast<const char *>(path.data()), path.size()),
                                  column_families,
                                  &handles,
                                  &db_ptr);
    write_status(std::move(s), status);
    return std::unique_ptr<DBBridge>(new DBBridge(db_ptr));
}