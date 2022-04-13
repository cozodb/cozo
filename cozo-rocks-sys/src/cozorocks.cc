//
// Created by Ziyang Hu on 2022/4/13.
//

#include <iostream>
#include "cozorocks.h"


#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "cozo-rocks-sys/src/lib.rs.h"

//using ROCKSDB_NAMESPACE::DB;
//using ROCKSDB_NAMESPACE::Options;
//using ROCKSDB_NAMESPACE::PinnableSlice;
//using ROCKSDB_NAMESPACE::ReadOptions;
//using ROCKSDB_NAMESPACE::Status;
//using ROCKSDB_NAMESPACE::WriteBatch;
//using ROCKSDB_NAMESPACE::WriteOptions;
//using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
//using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
//using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
//using ROCKSDB_NAMESPACE::Slice;
//using ROCKSDB_NAMESPACE::Snapshot;
//using ROCKSDB_NAMESPACE::Transaction;
//using ROCKSDB_NAMESPACE::TransactionDB;
//using ROCKSDB_NAMESPACE::TransactionDBOptions;
//using ROCKSDB_NAMESPACE::TransactionOptions;


#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

//std::unique_ptr<DB> new_db() {
//    DB *db_ptr;
//    Options options;
//    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
//    options.IncreaseParallelism();
//    options.OptimizeLevelStyleCompaction();
//    // create the DB if it's not already present
//    options.create_if_missing = true;
//
//    // open DB
//    Status s = DB::Open(options, kDBPath, &db_ptr);
//    std::unique_ptr<DB> db(db_ptr);
//    assert(s.ok());
//
//    // Put key-value
//    s = db->Put(WriteOptions(), "key1", "value");
//    assert(s.ok());
//    std::string value;
//    // get value
//    s = db->Get(ReadOptions(), "key1", &value);
//    assert(s.ok());
//    assert(value == "value");
//
//    // atomically apply a set of updates
//    {
//        WriteBatch batch;
//        batch.Delete("key1");
//        batch.Put("key2", value);
//        s = db->Write(WriteOptions(), &batch);
//    }
//
//    s = db->Get(ReadOptions(), "key1", &value);
//    assert(s.IsNotFound());
//
//    db->Get(ReadOptions(), "key2", &value);
//    assert(value == "value");
//    std::cout << value << " and fuck!" << std::endl;
//
//    {
//        PinnableSlice pinnable_val;
//        db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
//        assert(pinnable_val == "value");
//    }
//
//    {
//        std::string string_val;
//        // If it cannot pin the value, it copies the value to its internal buffer.
//        // The intenral buffer could be set during construction.
//        PinnableSlice pinnable_val(&string_val);
//        db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
//        assert(pinnable_val == "value");
//        // If the value is not pinned, the internal buffer must have the value.
//        assert(pinnable_val.IsPinned() || string_val == "value");
//    }
//
//    PinnableSlice pinnable_val;
//    s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
//    assert(s.IsNotFound());
//    // Reset PinnableSlice after each use and before each reuse
//    pinnable_val.Reset();
//    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
//    assert(pinnable_val == "value");
//    pinnable_val.Reset();
//    // The Slice pointed by pinnable_val is not valid after this point
//
//    std::cout << "hello from C++" << std::endl;
////    return std::unique_ptr<BlobstoreClient>(new BlobstoreClient());
//    return db;
//}

//std::unique_ptr<CozoRocksDB> open_db(const Options& options, const std::string& path) {
//    DB *db_ptr;
//    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
////    options.IncreaseParallelism();
////    options.OptimizeLevelStyleCompaction();
//    // create the DB if it's not already present
////    options.create_if_missing = true;
//
//    // open DB
//    Status s = DB::Open(options, path, &db_ptr);
//    std::unique_ptr<DB> db(db_ptr);
//    std::unique_ptr<CozoRocksDB> cdb(new CozoRocksDB{});
//    cdb->db = std::move(db);
//    cdb->db_status = std::move(s);
//    return cdb;
//}

void DB::put(rust::Slice<const uint8_t> key, rust::Slice<const uint8_t> val, Status &status) const {
    auto s = inner->Put(ROCKSDB_NAMESPACE::WriteOptions(),
                        ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char *>(key.data()), key.size()),
                        ROCKSDB_NAMESPACE::Slice(reinterpret_cast<const char *>(val.data()), val.size()));
        status.code = s.code();
        status.subcode = s.subcode();
        status.severity = s.severity();
}