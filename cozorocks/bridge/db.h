//
// Created by Ziyang Hu on 2022/7/3.
//

#ifndef COZOROCKS_DB_H
#define COZOROCKS_DB_H

#include <utility>

#include "iostream"
#include "common.h"
#include "tx.h"
#include "slice.h"

struct SnapshotBridge {
    const Snapshot *snapshot;
    DB *db;

    explicit SnapshotBridge(const Snapshot *snapshot_, DB *db_) : snapshot(snapshot_), db(db_) {}

    ~SnapshotBridge() {
        db->ReleaseSnapshot(snapshot);
//        printf("released snapshot\n");
    }
};

struct SstFileWriterBridge {
    SstFileWriter inner;

    SstFileWriterBridge(EnvOptions eopts, Options opts) : inner(eopts, opts) {
    }

    inline void finish(RocksDbStatus &status) {
        write_status(inner.Finish(), status);
    }

    inline void put(RustBytes key, RustBytes val, RocksDbStatus &status) {
        write_status(inner.Put(convert_slice(key), convert_slice(val)), status);
    }

};

struct RocksDbBridge {
    unique_ptr<Comparator> comparator;
    unique_ptr<Options> options;
    unique_ptr<TransactionDBOptions> tdb_opts;
    unique_ptr<TransactionDB> db;

    bool destroy_on_exit;
    string db_path;

    inline unique_ptr<SstFileWriterBridge> get_sst_writer(rust::Str path, RocksDbStatus &status) const {
        DB *db_ = get_base_db();
        Options options_ = db_->GetOptions();
        auto sst_file_writer = std::make_unique<SstFileWriterBridge>(EnvOptions(), options_);
        string path_(path);

        write_status(sst_file_writer->inner.Open(path_), status);
        return sst_file_writer;
    }

    inline void ingest_sst(rust::Str path, RocksDbStatus &status) const {
        IngestExternalFileOptions ifo;
        DB *db_ = get_base_db();
        string path_(path);
        write_status(db_->IngestExternalFile({std::move(path_)}, ifo), status);
    }

    [[nodiscard]] inline const string &get_db_path() const {
        return db_path;
    }


    [[nodiscard]] inline unique_ptr<TxBridge> transact() const {
        auto ret = make_unique<TxBridge>(&*this->db);
        return ret;
    }

    inline void del_range(RustBytes start, RustBytes end, RocksDbStatus &status) const {
        WriteBatch batch;
        auto s = batch.DeleteRange(db->DefaultColumnFamily(), convert_slice(start), convert_slice(end));
        if (!s.ok()) {
            write_status(s, status);
            return;
        }
        WriteOptions w_opts;
        TransactionDBWriteOptimizations optimizations;
        optimizations.skip_concurrency_control = true;
        optimizations.skip_duplicate_key_check = true;
        auto s2 = db->Write(w_opts, optimizations, &batch);
        write_status(s2, status);
    }

    void compact_range(RustBytes start, RustBytes end, RocksDbStatus &status) const {
        CompactRangeOptions options;
        auto start_s = convert_slice(start);
        auto end_s = convert_slice(end);
        auto s = db->CompactRange(options, &start_s, &end_s);
        write_status(s, status);
    }

    DB *get_base_db() const {
        return db->GetBaseDB();
    }

    ~RocksDbBridge();
};

//typedef int8_t (*CmpFn)(RustBytes a, RustBytes b);
typedef rust::Fn<std::int8_t(rust::Slice<const std::uint8_t>, rust::Slice<const std::uint8_t>)> RustComparatorFn;

class RustComparator : public Comparator {
public:
    inline RustComparator(string name_, bool can_different_bytes_be_equal_, RustComparatorFn f) :
            name(std::move(name_)),
            ext_cmp(f),
            can_different_bytes_be_equal(can_different_bytes_be_equal_) {
    }

    [[nodiscard]] inline int Compare(const Slice &a, const Slice &b) const override {
        return ext_cmp(convert_slice_back(a), convert_slice_back(b));
    }

    [[nodiscard]] inline const char *Name() const override {
        return name.c_str();
    }

    [[nodiscard]] inline bool CanKeysWithDifferentByteContentsBeEqual() const override {
        return can_different_bytes_be_equal;
    }

    inline void FindShortestSeparator(string *, const Slice &) const override {}

    inline void FindShortSuccessor(string *) const override {}

    string name;
    RustComparatorFn ext_cmp;
    bool can_different_bytes_be_equal;
};

shared_ptr<RocksDbBridge> open_db(const DbOpts &opts, RocksDbStatus &status, bool use_cmp, RustComparatorFn cmp_impl);

#endif //COZOROCKS_DB_H
