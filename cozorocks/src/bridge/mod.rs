/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt::{Display, Formatter};

use miette::{Diagnostic, Severity};

use crate::StatusSeverity;

pub(crate) mod db;
pub(crate) mod iter;
pub(crate) mod tx;

#[cxx::bridge]
pub(crate) mod ffi {
    #[derive(Debug, Clone)]
    struct DbOpts {
        pub db_path: Vec<u8>,
        pub options_path: Vec<u8>,
        pub prepare_for_bulk_load: bool,
        pub increase_parallelism: usize,
        pub optimize_level_style_compaction: bool,
        pub create_if_missing: bool,
        pub paranoid_checks: bool,
        pub enable_blob_files: bool,
        pub min_blob_size: usize,
        pub blob_file_size: usize,
        pub enable_blob_garbage_collection: bool,
        pub use_bloom_filter: bool,
        pub bloom_filter_bits_per_key: f64,
        pub bloom_filter_whole_key_filtering: bool,
        pub use_capped_prefix_extractor: bool,
        pub capped_prefix_extractor_len: usize,
        pub use_fixed_prefix_extractor: bool,
        pub fixed_prefix_extractor_len: usize,
        pub destroy_on_exit: bool,
        pub block_cache_size: usize,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RocksDbStatus {
        pub code: StatusCode,
        pub subcode: StatusSubCode,
        pub severity: StatusSeverity,
        pub message: String,
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
        kMergeOperatorFailed = 15,
        kMergeOperandThresholdExceeded = 16,
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
        include!("bridge.h");

        type StatusCode;
        type StatusSubCode;
        type StatusSeverity;
        type WriteOptions;
        type PinnableSlice;
        fn convert_pinnable_slice_back(s: &PinnableSlice) -> &[u8];

        fn set_w_opts_sync(o: Pin<&mut WriteOptions>, val: bool);
        fn set_w_opts_disable_wal(o: Pin<&mut WriteOptions>, val: bool);
        fn set_w_opts_no_slowdown(o: Pin<&mut WriteOptions>, val: bool);

        // type ReadOptions;

        pub type SnapshotBridge;

        type RocksDbBridge;
        fn get_db_path(self: &RocksDbBridge) -> &CxxString;
        fn open_db(builder: &DbOpts, status: &mut RocksDbStatus) -> SharedPtr<RocksDbBridge>;
        fn transact(self: &RocksDbBridge) -> UniquePtr<TxBridge>;
        fn del_range(self: &RocksDbBridge, lower: &[u8], upper: &[u8], status: &mut RocksDbStatus);
        fn put(self: &RocksDbBridge, key: &[u8], val: &[u8], status: &mut RocksDbStatus);
        fn compact_range(
            self: &RocksDbBridge,
            lower: &[u8],
            upper: &[u8],
            status: &mut RocksDbStatus,
        );
        fn get_sst_writer(
            self: &RocksDbBridge,
            path: &str,
            status: &mut RocksDbStatus,
        ) -> UniquePtr<SstFileWriterBridge>;
        fn ingest_sst(self: &RocksDbBridge, path: &str, status: &mut RocksDbStatus);

        type SstFileWriterBridge;
        fn put(
            self: Pin<&mut SstFileWriterBridge>,
            key: &[u8],
            val: &[u8],
            status: &mut RocksDbStatus,
        );
        fn finish(self: Pin<&mut SstFileWriterBridge>, status: &mut RocksDbStatus);

        type TxBridge;
        // fn get_r_opts(self: Pin<&mut TxBridge>) -> Pin<&mut ReadOptions>;
        fn verify_checksums(self: Pin<&mut TxBridge>, val: bool);
        fn fill_cache(self: Pin<&mut TxBridge>, val: bool);
        fn get_w_opts(self: Pin<&mut TxBridge>) -> Pin<&mut WriteOptions>;
        fn start(self: Pin<&mut TxBridge>);
        fn set_snapshot(self: Pin<&mut TxBridge>, val: bool);
        fn clear_snapshot(self: Pin<&mut TxBridge>);
        fn get(
            self: &TxBridge,
            key: &[u8],
            for_update: bool,
            status: &mut RocksDbStatus,
        ) -> UniquePtr<PinnableSlice>;
        fn exists(self: &TxBridge, key: &[u8], for_update: bool, status: &mut RocksDbStatus);
        fn put(self: &TxBridge, key: &[u8], val: &[u8], status: &mut RocksDbStatus);
        fn del(self: &TxBridge, key: &[u8], status: &mut RocksDbStatus);
        fn commit(self: Pin<&mut TxBridge>, status: &mut RocksDbStatus);
        fn rollback(self: Pin<&mut TxBridge>, status: &mut RocksDbStatus);
        fn rollback_to_savepoint(self: Pin<&mut TxBridge>, status: &mut RocksDbStatus);
        fn pop_savepoint(self: Pin<&mut TxBridge>, status: &mut RocksDbStatus);
        fn set_savepoint(self: Pin<&mut TxBridge>);
        fn iterator(self: &TxBridge) -> UniquePtr<IterBridge>;

        type IterBridge;
        fn start(self: Pin<&mut IterBridge>);
        fn reset(self: Pin<&mut IterBridge>);
        // fn get_r_opts(self: Pin<&mut IterBridge>) -> Pin<&mut ReadOptions>;
        fn clear_bounds(self: Pin<&mut IterBridge>);
        fn set_lower_bound(self: Pin<&mut IterBridge>, bound: &[u8]);
        fn set_upper_bound(self: Pin<&mut IterBridge>, bound: &[u8]);
        fn verify_checksums(self: Pin<&mut IterBridge>, val: bool);
        fn fill_cache(self: Pin<&mut IterBridge>, val: bool);
        fn tailing(self: Pin<&mut IterBridge>, val: bool);
        fn total_order_seek(self: Pin<&mut IterBridge>, val: bool);
        fn auto_prefix_mode(self: Pin<&mut IterBridge>, val: bool);
        fn prefix_same_as_start(self: Pin<&mut IterBridge>, val: bool);
        fn pin_data(self: Pin<&mut IterBridge>, val: bool);

        fn to_start(self: Pin<&mut IterBridge>);
        fn to_end(self: Pin<&mut IterBridge>);
        fn seek(self: Pin<&mut IterBridge>, key: &[u8]);
        fn seek_backward(self: Pin<&mut IterBridge>, key: &[u8]);
        fn is_valid(self: &IterBridge) -> bool;
        fn next(self: Pin<&mut IterBridge>);
        fn prev(self: Pin<&mut IterBridge>);
        fn status(self: &IterBridge, status: &mut RocksDbStatus);
        fn key(self: &IterBridge) -> &[u8];
        fn val(self: &IterBridge) -> &[u8];
    }
}

impl Default for ffi::RocksDbStatus {
    #[inline]
    fn default() -> Self {
        ffi::RocksDbStatus {
            code: ffi::StatusCode::kOk,
            subcode: ffi::StatusSubCode::kNone,
            severity: ffi::StatusSeverity::kNoError,
            message: "".to_string(),
        }
    }
}

impl Error for ffi::RocksDbStatus {}

impl Display for ffi::RocksDbStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() {
            write!(f, "RocksDB error: {self:?}")
        } else {
            write!(f, "RocksDB error: {}", self.message)
        }
    }
}

impl Diagnostic for ffi::RocksDbStatus {
    fn code<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        if self.is_ok() {
            None
        } else {
            Some(Box::new(format!(
                "rocksdb::{:?}::{:?}",
                self.code, self.subcode
            )))
        }
    }
    fn severity(&self) -> Option<Severity> {
        match self.severity {
            StatusSeverity::kNoError => None,
            _ => Some(Severity::Error),
        }
    }
    fn help<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        Some(Box::new("This error is usually outside Cozo's control"))
    }
}

impl ffi::RocksDbStatus {
    #[inline(always)]
    pub fn is_ok(&self) -> bool {
        self.code == ffi::StatusCode::kOk
    }
    #[inline(always)]
    pub fn is_not_found(&self) -> bool {
        self.code == ffi::StatusCode::kNotFound
    }
    #[inline(always)]
    pub fn is_ok_or_not_found(&self) -> bool {
        self.is_ok() || self.is_not_found()
    }
}
