//
// Created by Ziyang Hu on 2022/7/3.
//

#ifndef COZOROCKS_DB_H
#define COZOROCKS_DB_H

#include "iostream"
#include "common.h"
#include "tx.h"

struct RocksDb {
    unique_ptr<Comparator> comparator;
    unique_ptr<Options> options;
    bool destroy_on_exit;
    string db_path;

    virtual unique_ptr<RdbTx> start_txn() = 0;

    inline const string &get_db_path() const {
        return db_path;
    }
};

struct OptimisticRocksDb : public RocksDb {
    unique_ptr<OptimisticTransactionDB> db;

    virtual unique_ptr<RdbTx> start_txn();

    virtual ~OptimisticRocksDb();
};

struct PessimisticRocksDb : public RocksDb {
    unique_ptr<TransactionDBOptions> tdb_opts;
    unique_ptr<TransactionDB> db;

    virtual unique_ptr<RdbTx> start_txn();

    virtual ~PessimisticRocksDb();
};

typedef int8_t (*CmpFn)(const Slice &a, const Slice &b);

class RustComparator : public Comparator {
public:
    inline RustComparator(string name_, bool can_different_bytes_be_equal_, uint8_t const *const f) :
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

shared_ptr<RocksDb> open_db(const DbOpts &opts, RdbStatus &status);

#endif //COZOROCKS_DB_H
