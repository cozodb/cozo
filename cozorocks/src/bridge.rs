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
        fn reset_pinnable_slice(o: Pin<&mut PinnableSlice>);
        fn new_pinnable_slice() -> UniquePtr<PinnableSlice>;
        fn convert_slice_back(s: &Slice) -> &[u8];
        fn convert_pinnable_slice_back(s: &PinnableSlice) -> &[u8];

        type Options;
        fn new_options() -> UniquePtr<Options>;
        fn prepare_for_bulk_load(o: Pin<&mut Options>);
        fn increase_parallelism(o: Pin<&mut Options>);
        fn optimize_level_style_compaction(o: Pin<&mut Options>);
        fn set_create_if_missing(o: Pin<&mut Options>, v: bool);
        fn set_comparator(o: Pin<&mut Options>, cmp: &RustComparator);
        fn set_paranoid_checks(o: Pin<&mut Options>, v: bool);
        fn set_bloom_filter(o: Pin<&mut Options>, bits_per_key: f64, whole_key_filtering: bool);
        fn set_capped_prefix_extractor(o: Pin<&mut Options>, cap_len: usize);
        fn set_fixed_prefix_extractor(o: Pin<&mut Options>, prefix_len: usize);
        fn set_enable_blob_files(o: Pin<&mut Options>, v: bool);
        fn set_enable_blob_garbage_collection(o: Pin<&mut Options>, v: bool);
        fn set_min_blob_size(o: Pin<&mut Options>, size: u64);
        fn set_blob_file_size(o: Pin<&mut Options>, size: u64);

        type ReadOptions;
        fn new_read_options() -> UniquePtr<ReadOptions>;
        fn set_verify_checksums(o: Pin<&mut ReadOptions>, v: bool);
        fn set_total_order_seek(o: Pin<&mut ReadOptions>, v: bool);
        fn set_prefix_same_as_start(o: Pin<&mut ReadOptions>, v: bool);
        fn set_auto_prefix_mode(o: Pin<&mut ReadOptions>, v: bool);
        type WriteOptions;
        fn new_write_options() -> UniquePtr<WriteOptions>;
        fn set_disable_wal(o: Pin<&mut WriteOptions>, v: bool);
        type TransactionOptions;
        fn new_transaction_options() -> UniquePtr<TransactionOptions>;
        fn set_deadlock_detect(o: Pin<&mut TransactionOptions>, v: bool);
        type OptimisticTransactionOptions;
        fn new_optimistic_transaction_options(
            cmp: &RustComparator,
        ) -> UniquePtr<OptimisticTransactionOptions>;
        type TransactionDBOptions;
        fn new_tdb_options() -> UniquePtr<TransactionDBOptions>;
        type OptimisticTransactionDBOptions;
        fn new_odb_options() -> UniquePtr<OptimisticTransactionDBOptions>;

        type FlushOptions;
        fn new_flush_options() -> UniquePtr<FlushOptions>;
        fn set_flush_wait(o: Pin<&mut FlushOptions>, v: bool);
        fn set_allow_write_stall(o: Pin<&mut FlushOptions>, v: bool);

        type RustComparator;
        fn new_rust_comparator(
            name: &str,
            cmp: fn(&[u8], &[u8]) -> i8,
            diff_bytes_can_equal: bool,
        ) -> UniquePtr<RustComparator>;

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
        fn refresh(self: &IteratorBridge, status: &mut BridgeStatus);

        type TransactionBridge;
        fn set_snapshot(self: &TransactionBridge);
        fn set_readoption_snapshot_to_current(self: &TransactionBridge, opts: Pin<&mut ReadOptions>) -> bool;
        fn commit(self: &TransactionBridge, status: &mut BridgeStatus);
        fn rollback(self: &TransactionBridge, status: &mut BridgeStatus);
        fn set_savepoint(self: &TransactionBridge);
        fn rollback_to_savepoint(self: &TransactionBridge, status: &mut BridgeStatus);
        fn pop_savepoint(self: &TransactionBridge, status: &mut BridgeStatus);
        fn get_txn(
            self: &TransactionBridge,
            options: &ReadOptions,
            key: &[u8],
            slice: Pin<&mut PinnableSlice>,
            status: &mut BridgeStatus,
        );
        fn get_for_update_txn(
            self: &TransactionBridge,
            options: &ReadOptions,
            key: &[u8],
            slice: Pin<&mut PinnableSlice>,
            status: &mut BridgeStatus,
        );
        fn get_raw(
            self: &TransactionBridge,
            options: &ReadOptions,
            key: &[u8],
            slice: Pin<&mut PinnableSlice>,
            status: &mut BridgeStatus,
        );
        fn put_txn(
            self: &TransactionBridge,
            key: &[u8],
            val: &[u8],
            status: &mut BridgeStatus,
        );
        fn put_raw(
            self: &TransactionBridge,
            options: &WriteOptions,
            key: &[u8],
            val: &[u8],
            status: &mut BridgeStatus,
        );
        fn del_txn(
            self: &TransactionBridge,
            key: &[u8],
            status: &mut BridgeStatus,
        );
        fn del_raw(
            self: &TransactionBridge,
            options: &WriteOptions,
            key: &[u8],
            status: &mut BridgeStatus,
        );
        fn del_range_raw(
            self: &TransactionBridge,
            options: &WriteOptions,
            start_key: &[u8],
            end_key: &[u8],
            status: &mut BridgeStatus,
        );
        fn flush_raw(
            self: &TransactionBridge,
            options: &FlushOptions,
            status: &mut BridgeStatus,
        );
        fn compact_all_raw(self: &TransactionBridge, status: &mut BridgeStatus);
        fn iterator_txn(self: &TransactionBridge, r_opts: &ReadOptions) -> UniquePtr<IteratorBridge>;
        fn iterator_raw(self: &TransactionBridge, r_opts: &ReadOptions) -> UniquePtr<IteratorBridge>;

        type TDBBridge;
        fn begin_t_transaction(
            self: &TDBBridge,
            w_ops: UniquePtr<WriteOptions>,
            txn_options: UniquePtr<TransactionOptions>,
        ) -> UniquePtr<TransactionBridge>;
        fn begin_o_transaction(
            self: &TDBBridge,
            w_ops: UniquePtr<WriteOptions>,
            txn_options: UniquePtr<OptimisticTransactionOptions>,
        ) -> UniquePtr<TransactionBridge>;
        fn open_tdb_raw(
            options: &Options,
            txn_options: &TransactionDBOptions,
            path: &CxxString,
            status: &mut BridgeStatus,
        ) -> SharedPtr<TDBBridge>;
        fn open_odb_raw(
            options: &Options,
            // txn_options: &OptimisticTransactionDBOptions,
            path: &CxxString,
            status: &mut BridgeStatus,
        ) -> SharedPtr<TDBBridge>;
    }
}

pub use ffi::*;
use std::fmt::Formatter;

impl std::fmt::Display for StatusBridgeCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                StatusBridgeCode::OK => "Ok",
                StatusBridgeCode::LOCK_ERROR => "LockError",
                StatusBridgeCode::EXISTING_ERROR => "ExistingError",
                StatusBridgeCode::NOT_FOUND_ERROR => "NotFoundError",
                _ => "Unknown",
            }
        )
    }
}

impl std::fmt::Display for StatusCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                StatusCode::kOk => "Ok",
                StatusCode::kNotFound => "NotFound",
                StatusCode::kCorruption => "Corruption",
                StatusCode::kNotSupported => "NotSupported",
                StatusCode::kInvalidArgument => "InvalidArgument",
                StatusCode::kIOError => "IoError",
                StatusCode::kMergeInProgress => "MergeInProgress",
                StatusCode::kIncomplete => "Incomplete",
                StatusCode::kShutdownInProgress => "ShutdownInProgress",
                StatusCode::kTimedOut => "TimedOut",
                StatusCode::kAborted => "Aborted",
                StatusCode::kBusy => "Busy",
                StatusCode::kExpired => "Expired",
                StatusCode::kTryAgain => "TryAgain",
                StatusCode::kCompactionTooLarge => "CompactionTooLarge",
                StatusCode::kColumnFamilyDropped => "ColumnFamilyDropped",
                StatusCode::kMaxCode => "MaxCode",
                _ => "Unknown",
            }
        )
    }
}

impl std::fmt::Display for StatusSubCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                StatusSubCode::kNone => "None",
                StatusSubCode::kMutexTimeout => "MutexTimeout",
                StatusSubCode::kLockTimeout => "LockTimeout",
                StatusSubCode::kLockLimit => "LockLimit",
                StatusSubCode::kNoSpace => "NoSpace",
                StatusSubCode::kDeadlock => "DeadLock",
                StatusSubCode::kStaleFile => "StaleFile",
                StatusSubCode::kMemoryLimit => "MemoryLimit",
                StatusSubCode::kSpaceLimit => "SpaceLimit",
                StatusSubCode::kPathNotFound => "PathNotFound",
                StatusSubCode::KMergeOperandsInsufficientCapacity =>
                    "MergeOperandsInsufficientCapacity",
                StatusSubCode::kManualCompactionPaused => "ManualCompactionPaused",
                StatusSubCode::kOverwritten => "Overwritten",
                StatusSubCode::kTxnNotPrepared => "TxnNotPrepared",
                StatusSubCode::kIOFenced => "IoFenced",
                StatusSubCode::kMaxSubCode => "MaxSubCode",
                _ => "Unknown",
            }
        )
    }
}

impl std::fmt::Display for StatusSeverity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                StatusSeverity::kNoError => "NoError",
                StatusSeverity::kSoftError => "SoftError",
                StatusSeverity::kHardError => "HardError",
                StatusSeverity::kFatalError => "FatalError",
                StatusSeverity::kUnrecoverableError => "UnrecoverableError",
                StatusSeverity::kMaxSeverity => "MaxSeverity",
                _ => "Unknown",
            }
        )
    }
}
