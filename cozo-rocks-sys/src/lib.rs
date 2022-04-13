#[cxx::bridge]
mod ffi {
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    struct Status {
        code: StatusCode,
        subcode: StatusSubCode,
        severity: StatusSeverity
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    enum StatusCode {
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
    enum StatusSubCode {
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
    enum StatusSeverity {
        kNoError = 0,
        kSoftError = 1,
        kHardError = 2,
        kFatalError = 3,
        kUnrecoverableError = 4,
        kMaxSeverity,
    }

    unsafe extern "C++" {
        include!("cozo-rocks-sys/include/cozorocks.h");

        type DB;
        type Options;
        type PinnableSlice;

        type StatusCode;
        type StatusSubCode;
        type StatusSeverity;

        fn as_bytes(self: &PinnableSlice) -> &[u8];

        fn new_options() -> UniquePtr<Options>;
        fn prepare_for_bulk_load(self: &Options);
        fn increase_parallelism(self: &Options);
        fn optimize_level_style_compaction(self: &Options);
        fn set_create_if_missing(self: &Options, v: bool);

        fn open_db(options: &Options, path: &str) -> UniquePtr<DB>;
        fn put(self: &DB, key: &[u8], val: &[u8], status: &mut Status);
        fn get(self: &DB, key: &[u8]) -> UniquePtr<PinnableSlice>;
    }
}
pub use ffi::*;

impl Status {
    pub fn new() -> Self {
        Self {
            code: StatusCode::kOk,
            subcode: StatusSubCode::kNone,
            severity: StatusSeverity::kNoError
        }
    }
}