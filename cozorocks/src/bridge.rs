#[cxx::bridge]
mod ffi {
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub enum StatusBridgeCode {
        OK = 0,
        LOCK_ERROR = 1,
        EXISTING_ERROR = 2,
        NOT_FOUND_ERROR = 3,
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub struct BridgeStatus {
        pub code: StatusCode,
        pub subcode: StatusSubCode,
        pub severity: StatusSeverity,
        pub bridge_code: StatusBridgeCode,
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub enum StatusCode {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5,
        kMergeInProgress = 6,
        kIncomplete = 7,
        kShutdownInProgress = 8,
        kTimedOut = 9,
        kAborted = 10,
        kBusy = 11,
        kExpired = 12,
        kTryAgain = 13,
        kCompactionTooLarge = 14,
        kColumnFamilyDropped = 15,
        kMaxCode,
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub enum StatusSubCode {
        kNone = 0,
        kMutexTimeout = 1,
        kLockTimeout = 2,
        kLockLimit = 3,
        kNoSpace = 4,
        kDeadlock = 5,
        kStaleFile = 6,
        kMemoryLimit = 7,
        kSpaceLimit = 8,
        kPathNotFound = 9,
        KMergeOperandsInsufficientCapacity = 10,
        kManualCompactionPaused = 11,
        kOverwritten = 12,
        kTxnNotPrepared = 13,
        kIOFenced = 14,
        kMaxSubCode,
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub enum StatusSeverity {
        kNoError = 0,
        kSoftError = 1,
        kHardError = 2,
        kFatalError = 3,
        kUnrecoverableError = 4,
        kMaxSeverity,
    }

    unsafe extern "C++" {
        include!("cozorocks.h");

        type StatusCode;
        type StatusSubCode;
        type StatusSeverity;

        type Slice;
        type PinnableSlice;
        fn convert_slice_back(s: &Slice) -> &[u8];
        fn convert_pinnable_slice_back(s: &PinnableSlice) -> &[u8];

        type Options;
        fn new_options() -> UniquePtr<Options>;
        fn prepare_for_bulk_load(o: Pin<&mut Options>);
        fn increase_parallelism(o: Pin<&mut Options>);
        fn optimize_level_style_compaction(o: Pin<&mut Options>);
        fn set_create_if_missing(o: Pin<&mut Options>, v: bool);
        fn set_comparator(o: Pin<&mut Options>, cmp: &RustComparator);

        type ReadOptions;
        fn new_read_options() -> UniquePtr<ReadOptions>;
        fn set_verify_checksums(o: Pin<&mut ReadOptions>, v: bool);
        fn set_total_order_seek(o: Pin<&mut ReadOptions>, v: bool);
        type WriteOptions;
        fn new_write_options() -> UniquePtr<WriteOptions>;
        fn set_disable_wal(o: Pin<&mut WriteOptions>, v: bool);
        type TransactionOptions;
        fn new_transaction_options() -> UniquePtr<TransactionOptions>;
        fn set_deadlock_detect(o: Pin<&mut TransactionOptions>, v: bool);
        type OptimisticTransactionOptions;
        fn new_optimistic_transaction_options(cmp: &RustComparator) -> UniquePtr<OptimisticTransactionOptions>;
        type TransactionDBOptions;
        fn new_tdb_options() -> UniquePtr<TransactionDBOptions>;
        type OptimisticTransactionDBOptions;
        fn new_odb_options() -> UniquePtr<OptimisticTransactionDBOptions>;

        type RustComparator;
        fn new_rust_comparator(name: &str, cmp: fn(&[u8], &[u8]) -> i8) -> UniquePtr<RustComparator>;

        pub type IteratorBridge;
        fn seek_to_first(self: &IteratorBridge);
        fn seek_to_last(self: &IteratorBridge);
        fn next(self: &IteratorBridge);
        fn is_valid(self: &IteratorBridge) -> bool;
        fn do_seek(self: &IteratorBridge, key: &[u8]);
        fn do_seek_for_prev(self: &IteratorBridge, key: &[u8]);
        fn key_raw(self: &IteratorBridge) -> UniquePtr<Slice>;
        fn value_raw(self: &IteratorBridge) -> UniquePtr<Slice>;
        fn status(self: &IteratorBridge) -> BridgeStatus;

        type TransactionBridge;
        fn set_snapshot(self: &TransactionBridge);
        fn commit(self: &TransactionBridge, status: &mut BridgeStatus);
        fn rollback(self: &TransactionBridge, status: &mut BridgeStatus);
        fn set_savepoint(self: &TransactionBridge);
        fn rollback_to_savepoint(self: &TransactionBridge, status: &mut BridgeStatus);
        fn pop_savepoint(self: &TransactionBridge, status: &mut BridgeStatus);
        fn get_txn(self: &TransactionBridge, cf: &ColumnFamilyHandle, key: &[u8],
                   status: &mut BridgeStatus) -> UniquePtr<PinnableSlice>;
        fn get_for_update_txn(self: &TransactionBridge, cf: &ColumnFamilyHandle, key: &[u8],
                              status: &mut BridgeStatus) -> UniquePtr<PinnableSlice>;
        fn get_raw(self: &TransactionBridge, cf: &ColumnFamilyHandle, key: &[u8],
                   status: &mut BridgeStatus) -> UniquePtr<PinnableSlice>;
        fn put_txn(self: &TransactionBridge, cf: &ColumnFamilyHandle, key: &[u8], val: &[u8],
                   status: &mut BridgeStatus);
        fn put_raw(self: &TransactionBridge, cf: &ColumnFamilyHandle, key: &[u8], val: &[u8],
                   status: &mut BridgeStatus);
        fn del_txn(self: &TransactionBridge, cf: &ColumnFamilyHandle, key: &[u8],
                   status: &mut BridgeStatus);
        fn del_raw(self: &TransactionBridge, cf: &ColumnFamilyHandle, key: &[u8],
                   status: &mut BridgeStatus);
        fn iterator_txn(self: &TransactionBridge, cf: &ColumnFamilyHandle) -> UniquePtr<IteratorBridge>;
        fn iterator_raw(self: &TransactionBridge, cf: &ColumnFamilyHandle) -> UniquePtr<IteratorBridge>;

        pub type ColumnFamilyHandle;

        type TDBBridge;
        fn begin_t_transaction(self: &TDBBridge,
                               w_ops: UniquePtr<WriteOptions>,
                               raw_w_ops: UniquePtr<WriteOptions>,
                               r_ops: UniquePtr<ReadOptions>,
                               raw_r_ops: UniquePtr<ReadOptions>,
                               txn_options: UniquePtr<TransactionOptions>) -> UniquePtr<TransactionBridge>;
        fn begin_o_transaction(self: &TDBBridge,
                               w_ops: UniquePtr<WriteOptions>,
                               raw_w_ops: UniquePtr<WriteOptions>,
                               r_ops: UniquePtr<ReadOptions>,
                               raw_r_ops: UniquePtr<ReadOptions>,
                               txn_options: UniquePtr<OptimisticTransactionOptions>) -> UniquePtr<TransactionBridge>;
        fn get_cf_handle_raw(self: &TDBBridge, name: &CxxString) -> SharedPtr<ColumnFamilyHandle>;
        fn get_default_cf_handle_raw(self: &TDBBridge) -> SharedPtr<ColumnFamilyHandle>;
        fn create_column_family_raw(self: &TDBBridge, options: &Options, name: &CxxString, status: &mut BridgeStatus) -> SharedPtr<ColumnFamilyHandle>;
        fn drop_column_family_raw(self: &TDBBridge, name: &CxxString, status: &mut BridgeStatus);
        fn get_column_family_names_raw(self: &TDBBridge) -> UniquePtr<CxxVector<CxxString>>;
        fn open_tdb_raw(options: &Options,
                        txn_options: &TransactionDBOptions,
                        path: &CxxString,
                        status: &mut BridgeStatus) -> UniquePtr<TDBBridge>;
        fn open_odb_raw(options: &Options,
                        txn_options: &OptimisticTransactionDBOptions,
                        path: &CxxString,
                        status: &mut BridgeStatus) -> UniquePtr<TDBBridge>;
    }
}

pub use ffi::*;
