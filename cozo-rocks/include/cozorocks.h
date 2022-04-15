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

using namespace ROCKSDB_NAMESPACE;
using std::unique_ptr;
using std::shared_ptr;
using std::make_unique;
using std::make_shared;
using std::string;
using std::vector;
using std::unordered_map;

struct BridgeStatus;

typedef Status::Code StatusCode;
typedef Status::SubCode StatusSubCode;
typedef Status::Severity StatusSeverity;

inline Slice convert_slice(rust::Slice<const uint8_t> d) {
    return Slice(reinterpret_cast<const char *>(d.data()), d.size());
}

inline rust::Slice<const uint8_t> convert_slice_back(const Slice &s) {
    return rust::Slice(reinterpret_cast<const std::uint8_t *>(s.data()), s.size());
}

struct ReadOptionsBridge {
    mutable ReadOptions inner;

    inline void do_set_verify_checksums(bool v) const {
        inner.verify_checksums = v;
    }

    inline void do_set_total_order_seek(bool v) const {
        inner.total_order_seek = v;
    }
};

struct WriteOptionsBridge {
    mutable WriteOptions inner;

public:
    inline void do_set_disable_wal(bool v) const {
        inner.disableWAL = v;
    }
};

typedef rust::Fn<std::int8_t(rust::Slice<const std::uint8_t>, rust::Slice<const std::uint8_t>)> RustComparatorFn;

class RustComparator : public Comparator {
public:
    inline int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const {
        return int(rust_compare(convert_slice_back(a), convert_slice_back(b)));
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
    mutable Options inner;
    mutable RustComparator cmp_obj;

    inline void do_prepare_for_bulk_load() const {
        inner.PrepareForBulkLoad();
    }

    inline void do_increase_parallelism() const {
        inner.IncreaseParallelism();
    }

    inline void do_optimize_level_style_compaction() const {
        inner.OptimizeLevelStyleCompaction();
    };

    inline void do_set_create_if_missing(bool v) const {
        inner.create_if_missing = v;
    }

    inline void do_set_comparator(rust::Str name, RustComparatorFn f) const {
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
    PinnableSlice inner;

    inline rust::Slice<const std::uint8_t> as_bytes() const {
        return convert_slice_back(inner);
    }
};

struct SliceBridge {
    Slice inner;

    SliceBridge(Slice &&s) : inner(s) {}

    inline rust::Slice<const std::uint8_t> as_bytes() const {
        return convert_slice_back(inner);
    }
};

void write_status_impl(BridgeStatus &status, StatusCode code, StatusSubCode subcode, StatusSeverity severity,
                       int bridge_code);

inline void write_status(Status &&rstatus, BridgeStatus &status) {
    if (rstatus.code() != StatusCode::kOk || rstatus.subcode() != StatusSubCode::kNoSpace ||
        rstatus.severity() != StatusSeverity::kNoError) {
        write_status_impl(status, rstatus.code(), rstatus.subcode(), rstatus.severity(), 0);
    }
}

struct IteratorBridge {
    mutable std::unique_ptr <Iterator> inner;

    IteratorBridge(Iterator *it) : inner(it) {}

    inline void seek_to_first() const {
        inner->SeekToFirst();
    }

    inline void seek_to_last() const {
        inner->SeekToLast();
    }

    inline void next() const {
        inner->Next();
    }

    inline bool is_valid() const {
        return inner->Valid();
    }

    inline void do_seek(rust::Slice<const uint8_t> key) const {
        auto k = Slice(reinterpret_cast<const char *>(key.data()), key.size());
        inner->Seek(k);
    }

    inline void do_seek_for_prev(rust::Slice<const uint8_t> key) const {
        auto k = Slice(reinterpret_cast<const char *>(key.data()), key.size());
        inner->SeekForPrev(k);
    }

    inline std::unique_ptr <SliceBridge> key() const {
        return std::make_unique<SliceBridge>(inner->key());
    }

    inline std::unique_ptr <SliceBridge> value() const {
        return std::make_unique<SliceBridge>(inner->value());
    }

    BridgeStatus status() const;
};


struct WriteBatchBridge {
    mutable WriteBatch inner;

    inline void batch_put_raw(
            const ColumnFamilyHandle &cf,
            rust::Slice<const uint8_t> key,
            rust::Slice<const uint8_t> val,
            BridgeStatus &status
    ) const {
        write_status(
                inner.Put(const_cast<ColumnFamilyHandle *>(&cf),
                          convert_slice(key),
                          convert_slice(val)),
                status
        );
    }

    inline void batch_delete_raw(
            const ColumnFamilyHandle &cf,
            rust::Slice<const uint8_t> key,
            BridgeStatus &status
    ) const {
        write_status(
                inner.Delete(const_cast<ColumnFamilyHandle *>(&cf),
                             convert_slice(key)),
                status
        );
    }
};

inline unique_ptr <WriteBatchBridge> new_write_batch_raw() {
    return make_unique<WriteBatchBridge>();
}

struct DBBridge {
    mutable unique_ptr <DB> db;
    mutable unordered_map <string, shared_ptr<ColumnFamilyHandle>> handles;

    DBBridge(DB *db_,
             unordered_map <string, shared_ptr<ColumnFamilyHandle>> &&handles_) : db(db_), handles(handles_) {}


    inline shared_ptr <ColumnFamilyHandle> get_cf_handle_raw(const string &name) const {
        try {
            return handles.at(name);
        } catch (const std::out_of_range &) {
            return shared_ptr<ColumnFamilyHandle>(nullptr);
        }
    }

    inline void put_raw(
            const WriteOptionsBridge &options,
            const ColumnFamilyHandle &cf,
            rust::Slice<const uint8_t> key,
            rust::Slice<const uint8_t> val,
            BridgeStatus &status
    ) const {
        write_status(
                db->Put(options.inner,
                        const_cast<ColumnFamilyHandle *>(&cf),
                        convert_slice(key),
                        convert_slice(val)),
                status
        );
    }

    inline void delete_raw(
            const WriteOptionsBridge &options,
            const ColumnFamilyHandle &cf,
            rust::Slice<const uint8_t> key,
            BridgeStatus &status
    ) const {
        write_status(
                db->Delete(options.inner,
                           const_cast<ColumnFamilyHandle *>(&cf),
                           convert_slice(key)),
                status
        );
    }

    inline void write_raw(
            const WriteOptionsBridge &options,
            WriteBatchBridge &updates,
            BridgeStatus &status
    ) const {
        write_status(db->Write(options.inner, &updates.inner), status);
    }

    inline std::unique_ptr <PinnableSliceBridge> get_raw(
            const ReadOptionsBridge &options,
            const ColumnFamilyHandle &cf,
            rust::Slice<const uint8_t> key,
            BridgeStatus &status
    ) const {
        auto pinnable_val = std::make_unique<PinnableSliceBridge>();
        write_status(
                db->Get(options.inner,
                        const_cast<ColumnFamilyHandle *>(&cf),
                        convert_slice(key),
                        &pinnable_val->inner),
                status
        );
        return pinnable_val;
    }

    inline std::unique_ptr <IteratorBridge> iterator_raw(
            const ReadOptionsBridge &options,
            const ColumnFamilyHandle &cf) const {
        return std::make_unique<IteratorBridge>(db->NewIterator(options.inner, const_cast<ColumnFamilyHandle *>(&cf)));
    }

    inline void create_column_family_raw(const OptionsBridge &options, const string &name, BridgeStatus &status) const {
        if (handles.find(name) != handles.end()) {
            write_status_impl(status, StatusCode::kMaxCode, StatusSubCode::kMaxSubCode, StatusSeverity::kSoftError, 2);
            return;
        }
        ColumnFamilyHandle *handle;
        auto s = db->CreateColumnFamily(options.inner, name, &handle);
        write_status(std::move(s), status);
        handles[name] = shared_ptr<ColumnFamilyHandle>(handle);
    }

    inline void drop_column_family_raw(const string &name, BridgeStatus &status) const {
        auto cf_it = handles.find(name);
        if (cf_it != handles.end()) {
            auto s = db->DropColumnFamily(cf_it->second.get());
            handles.erase(cf_it);
            write_status(std::move(s), status);
        } else {
            write_status_impl(status, StatusCode::kMaxCode, StatusSubCode::kMaxSubCode, StatusSeverity::kSoftError, 3);
        }
        // When should we call DestroyColumnFamilyHandle?
    }

    inline unique_ptr <vector<string>> get_column_family_names_raw() const {
        auto ret = make_unique < vector < string >> ();
        for (auto entry: handles) {
            ret->push_back(entry.first);
        }
        return ret;
    }
};


inline std::unique_ptr <DBBridge>
open_db_raw(const OptionsBridge &options,
            const string &path,
            BridgeStatus &status) {
    auto cf_names = std::vector<std::string>();
    DB::ListColumnFamilies(options.inner, path, &cf_names);
    if (cf_names.empty()) {
        cf_names.push_back(kDefaultColumnFamilyName);
    }

    std::vector <ColumnFamilyDescriptor> column_families;

    for (auto el: cf_names) {
        column_families.push_back(ColumnFamilyDescriptor(
                el, options.inner));
    }

    std::vector < ColumnFamilyHandle * > handles;

    DB *db_ptr;
    Status s = DB::Open(options.inner, path, column_families, &handles, &db_ptr);

    auto ok = s.ok();
    write_status(std::move(s), status);
    unordered_map <string, shared_ptr<ColumnFamilyHandle>> handle_map;
    if (ok) {
        assert(handles.size() == cf_names.size());
        for (size_t i = 0; i < handles.size(); ++i) {
            handle_map[cf_names[i]] = shared_ptr<ColumnFamilyHandle>(handles[i]);
        }
    }
    return std::make_unique<DBBridge>(db_ptr, std::move(handle_map));
}
