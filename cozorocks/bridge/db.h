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

struct RocksDbBridge {
    unique_ptr<Comparator> comparator;
    unique_ptr<Options> options;
    bool destroy_on_exit;
    string db_path;

    [[nodiscard]] virtual unique_ptr<TxBridge> transact() const = 0;

    [[nodiscard]] inline const string &get_db_path() const {
        return db_path;
    }
};

struct OptimisticRocksDb : public RocksDbBridge {
    unique_ptr<OptimisticTransactionDB> db;

    [[nodiscard]] inline unique_ptr<TxBridge> transact() const override {
        auto ret = make_unique<TxBridge>(&*this->db);
        ret->o_tx_opts->cmp = &*comparator;
        return ret;
    }

    virtual ~OptimisticRocksDb();
};

struct PessimisticRocksDb : public RocksDbBridge {
    unique_ptr<TransactionDBOptions> tdb_opts;
    unique_ptr<TransactionDB> db;

    [[nodiscard]] inline unique_ptr<TxBridge> transact() const override {
        auto ret = make_unique<TxBridge>(&*this->db);
        return ret;
    }

    virtual ~PessimisticRocksDb();
};

typedef int8_t (*CmpFn)(RustBytes a, RustBytes b);

class RustComparator : public Comparator {
public:
    inline RustComparator(string name_, bool can_different_bytes_be_equal_, uint8_t const *const f) :
            name(std::move(name_)),
            can_different_bytes_be_equal(can_different_bytes_be_equal_) {
        auto f_ = CmpFn(f);
        ext_cmp = f_;
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
    CmpFn ext_cmp;
    bool can_different_bytes_be_equal;
};

shared_ptr<RocksDbBridge> open_db(const DbOpts &opts, RocksDbStatus &status);

#endif //COZOROCKS_DB_H
