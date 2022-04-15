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

        type PinnableSliceBridge;
        fn as_bytes(self: &PinnableSliceBridge) -> &[u8];

        type SliceBridge;
        fn as_bytes(self: &SliceBridge) -> &[u8];

        type ReadOptionsBridge;
        fn new_read_options() -> UniquePtr<ReadOptionsBridge>;
        fn do_set_verify_checksums(self: &ReadOptionsBridge, v: bool);
        fn do_set_total_order_seek(self: &ReadOptionsBridge, v: bool);

        type WriteOptionsBridge;
        fn new_write_options() -> UniquePtr<WriteOptionsBridge>;
        fn do_set_disable_wal(self: &WriteOptionsBridge, v: bool);

        type OptionsBridge;
        fn new_options() -> UniquePtr<OptionsBridge>;
        fn do_prepare_for_bulk_load(self: &OptionsBridge);
        fn do_increase_parallelism(self: &OptionsBridge);
        fn do_optimize_level_style_compaction(self: &OptionsBridge);
        fn do_set_create_if_missing(self: &OptionsBridge, v: bool);
        fn do_set_comparator(self: &OptionsBridge, name: &str, compare: fn(&[u8], &[u8]) -> i8);

        pub type ColumnFamilyHandle;
        type DBBridge;
        fn open_db_raw(options: &OptionsBridge, path: &CxxString, status: &mut BridgeStatus) -> UniquePtr<DBBridge>;
        fn get_cf_handle_raw(self: &DBBridge, name: &CxxString) -> SharedPtr<ColumnFamilyHandle>;
        fn put_raw(self: &DBBridge, options: &WriteOptionsBridge, cf: &ColumnFamilyHandle, key: &[u8], val: &[u8], status: &mut BridgeStatus);
        fn get_raw(self: &DBBridge, options: &ReadOptionsBridge, cf: &ColumnFamilyHandle, key: &[u8], status: &mut BridgeStatus) -> UniquePtr<PinnableSliceBridge>;
        fn iterator_raw(self: &DBBridge, options: &ReadOptionsBridge, cf: &ColumnFamilyHandle) -> UniquePtr<IteratorBridge>;
        fn create_column_family_raw(self: &DBBridge, options: &OptionsBridge, name: &CxxString, status: &mut BridgeStatus);
        fn drop_column_family_raw(self: &DBBridge, name: &CxxString, status: &mut BridgeStatus);
        fn get_column_family_names_raw(self: &DBBridge) -> UniquePtr<CxxVector<CxxString>>;

        //         fn write_batch(self: &DBBridge) -> UniquePtr<WriteBatchBridge>;
//
//         type WriteBatchBridge;
//
        type IteratorBridge;
        fn seek_to_first(self: &IteratorBridge);
        fn seek_to_last(self: &IteratorBridge);
        fn next(self: &IteratorBridge);
        fn is_valid(self: &IteratorBridge) -> bool;
        fn do_seek(self: &IteratorBridge, key: &[u8]);
        fn do_seek_for_prev(self: &IteratorBridge, key: &[u8]);
        fn key(self: &IteratorBridge) -> UniquePtr<SliceBridge>;
        fn value(self: &IteratorBridge) -> UniquePtr<SliceBridge>;
        fn status(self: &IteratorBridge) -> BridgeStatus;
    }
}

use std::path::Path;
use cxx::{UniquePtr, SharedPtr, let_cxx_string};
use ffi::*;

type Result<T> = std::result::Result<T, BridgeStatus>;

pub type Options = UniquePtr<OptionsBridge>;

type ColumnFamilyHandle = SharedPtr<ffi::ColumnFamilyHandle>;

pub trait OptionsTrait {
    fn prepare_for_bulk_load(self) -> Self;
    fn increase_parallelism(self) -> Self;
    fn optimize_level_style_compaction(self) -> Self;
    fn set_create_if_missing(self, v: bool) -> Self;
    fn set_comparator(self, name: &str, compare: fn(&[u8], &[u8]) -> i8) -> Self;
    fn default() -> Self;
}

impl OptionsTrait for Options {
    #[inline]
    fn prepare_for_bulk_load(self) -> Self {
        self.do_prepare_for_bulk_load();
        self
    }

    #[inline]
    fn increase_parallelism(self) -> Self {
        self.do_increase_parallelism();
        self
    }

    #[inline]
    fn optimize_level_style_compaction(self) -> Self {
        self.do_optimize_level_style_compaction();
        self
    }

    #[inline]
    fn set_create_if_missing(self, v: bool) -> Self {
        self.do_set_create_if_missing(v);
        self
    }

    #[inline]
    fn set_comparator(self, name: &str, compare: fn(&[u8], &[u8]) -> i8) -> Self {
        self.do_set_comparator(name, compare);
        self
    }

    #[inline]
    fn default() -> Self {
        new_options()
    }
}

pub type ReadOptions = UniquePtr<ReadOptionsBridge>;

pub trait ReadOptionsTrait {
    fn set_total_order_seek(self, v: bool) -> Self;
    fn set_verify_checksums(self, v: bool) -> Self;
    fn default() -> Self;
}

impl ReadOptionsTrait for ReadOptions {
    fn set_total_order_seek(self, v: bool) -> Self {
        self.do_set_total_order_seek(v);
        self
    }
    fn set_verify_checksums(self, v: bool) -> Self {
        self.do_set_verify_checksums(v);
        self
    }

    fn default() -> Self {
        new_read_options()
    }
}

pub type WriteOptions = UniquePtr<WriteOptionsBridge>;

pub trait WriteOptionsTrait {
    fn set_disable_wal(self, v: bool) -> Self;
    fn default() -> Self;
}

impl WriteOptionsTrait for WriteOptions {
    #[inline]
    fn set_disable_wal(self, v: bool) -> Self {
        self.do_set_disable_wal(v);
        self
    }
    fn default() -> Self {
        new_write_options()
    }
}

pub struct PinnableSlice(UniquePtr<PinnableSliceBridge>);

impl AsRef<[u8]> for PinnableSlice {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

pub struct Slice(UniquePtr<SliceBridge>);

impl AsRef<[u8]> for Slice {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}


pub type Iterator = UniquePtr<IteratorBridge>;

pub trait IteratorImpl {
    fn seek(&self, key: impl AsRef<[u8]>);
    fn seek_for_prev(&self, key: impl AsRef<[u8]>);
}

impl IteratorImpl for IteratorBridge {
    fn seek(&self, key: impl AsRef<[u8]>) {
        self.do_seek(key.as_ref());
    }
    fn seek_for_prev(&self, key: impl AsRef<[u8]>) {
        self.do_seek_for_prev(key.as_ref())
    }
}

fn get_path_bytes(path: &std::path::Path) -> &[u8] {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes()
    }

    #[cfg(not(unix))]
    { path.to_string_lossy().to_string().as_bytes() }
}

//
//     #[inline]
//     pub fn write_batch(&self) -> UniquePtr<WriteBatchBridge> {
//         self.bridge.write_batch()
//     }
//
//     // #[inline]
//     // pub fn get_column_family_id(&self, name: impl AsRef<str>) -> Result<Option<usize>, Status> {
//     //     let handles = self.cf_map.read()
//     //         .map_err(|_| Status::bridge(StatusBridgeCode::LOCK_ERROR))?;
//     //     Ok(handles.get(name.as_ref()).copied())
//     // }
//
//     // #[inline]
//     // pub fn create_column_family(&self, name: impl AsRef<str>) -> Result<(), Status> {
//     //     let mut s = Status::default();
//     //     let mut cf_map = self.cf_map.write()
//     //         .map_err(|_| Status::bridge(StatusBridgeCode::LOCK_ERROR))?;
//     //     let mut cfs = self.cfs.write()
//     //         .map_err(|_| Status::bridge(StatusBridgeCode::LOCK_ERROR))?;
//     //     let v = self.bridge.create_column_family(&self.options, name.as_ref(), &mut s);
//     //     if v > 0 {
//     //         assert_eq!(v as usize, cfs.len());
//     //         cf_map.insert(name.as_ref().to_string(), v as usize);
//     //         cfs.push(name.as_ref().to_string());
//     //         Ok(())
//     //     } else {
//     //         Err(s)
//     //     }
//     // }
//
//     // #[inline]
//     // pub fn drop_column_family(&self, _name: impl AsRef<str>) -> Result<(), Status> {
//     //     unimplemented!()
//     // }
//
//     pub fn destroy_data(self) -> Result<(), Status> {
//         unimplemented!()
//     }
// }
//
impl Default for BridgeStatus {
    #[inline]
    fn default() -> Self {
        Self {
            code: StatusCode::kOk,
            subcode: StatusSubCode::kNone,
            severity: StatusSeverity::kNoError,
            bridge_code: StatusBridgeCode::OK,
        }
    }
}

impl BridgeStatus {
    #[inline]
    fn bridge(c: StatusBridgeCode) -> Self {
        Self {
            code: StatusCode::kMaxCode,
            subcode: StatusSubCode::kMaxSubCode,
            severity: StatusSeverity::kMaxSeverity,
            bridge_code: c,
        }
    }
}

pub trait DBRead {
    fn get(&self, key: impl AsRef<[u8]>, cf: &ColumnFamilyHandle, options: Option<&ReadOptions>)
           -> Result<Option<PinnableSlice>>;
}

pub trait DBWrite {
    fn put(&self, key: impl AsRef<[u8]>, val: impl AsRef<[u8]>, cf: &ColumnFamilyHandle, options: Option<&WriteOptions>)
           -> Result<BridgeStatus>;
}

pub struct DB {
    inner: UniquePtr<DBBridge>,
    pub options: Options,
    pub default_read_options: ReadOptions,
    pub default_write_options: WriteOptions,
}

impl DBRead for DB {
    #[inline]
    fn get(&self, key: impl AsRef<[u8]>, cf: &ColumnFamilyHandle, options: Option<&ReadOptions>) -> Result<Option<PinnableSlice>> {
        let mut status = BridgeStatus::default();
        let slice = self.inner.get_raw(options.unwrap_or(&self.default_read_options), cf, key.as_ref(), &mut status);
        match status.code {
            StatusCode::kOk => Ok(Some(PinnableSlice(slice))),
            StatusCode::kNotFound => Ok(None),
            _ => Err(status)
        }
    }
}

impl DBWrite for DB {
    #[inline]
    fn put(&self, key: impl AsRef<[u8]>, val: impl AsRef<[u8]>, cf: &ColumnFamilyHandle, options: Option<&WriteOptions>) -> Result<BridgeStatus> {
        let mut status = BridgeStatus::default();
        self.inner.put_raw(options.unwrap_or(&self.default_write_options), cf,
                           key.as_ref(), val.as_ref(),
                           &mut status);
        if status.code == StatusCode::kOk {
            Ok(status)
        } else {
            Err(status)
        }
    }
}


pub trait DBImpl {
    fn open(options: Options, path: &Path) -> Result<DB>;
    fn get_cf_handle(&self, name: impl AsRef<str>) -> Result<ColumnFamilyHandle>;
    fn iterator(&self, cf: &ColumnFamilyHandle, options: Option<&ReadOptions>) -> Iterator;
    fn create_column_family(&self, name: impl AsRef<str>) -> Result<()>;
    fn drop_column_family(&self, name: impl AsRef<str>) -> Result<()>;
    fn get_column_family_names(&self) -> Vec<String>;
}

impl DBImpl for DB {
    fn open(options: Options, path: &Path) -> Result<DB> {
        let_cxx_string!(path = get_path_bytes(path));
        let mut status = BridgeStatus::default();
        let bridge = open_db_raw(
            &options,
            &path,
            &mut status,
        );

        if status.code == StatusCode::kOk {
            Ok(DB {
                inner: bridge,
                options,
                default_read_options: ReadOptions::default(),
                default_write_options: WriteOptions::default(),
            })
        } else {
            Err(status)
        }
    }

    fn get_cf_handle(&self, name: impl AsRef<str>) -> Result<ColumnFamilyHandle> {
        let_cxx_string!(name = name.as_ref());
        let ret = self.inner.get_cf_handle_raw(&name);
        if ret.is_null() {
            Err(BridgeStatus {
                code: StatusCode::kMaxCode,
                subcode: StatusSubCode::kMaxSubCode,
                severity: StatusSeverity::kSoftError,
                bridge_code: StatusBridgeCode::NOT_FOUND_ERROR
            })
        } else {
            Ok(ret)
        }
    }

    #[inline]
    fn iterator(&self, cf: &ColumnFamilyHandle, options: Option<&ReadOptions>) -> Iterator {
        self.inner.iterator_raw(options.unwrap_or(&self.default_read_options), cf)
    }

    fn create_column_family(&self, name: impl AsRef<str>) -> Result<()> {
        let_cxx_string!(name = name.as_ref());
        let mut status = BridgeStatus::default();
        self.inner.create_column_family_raw(&self.options, &name, &mut status);
        if status.code == StatusCode::kOk {
            Ok(())
        } else {
            Err(status)
        }
    }

    fn drop_column_family(&self, name: impl AsRef<str>) -> Result<()> {
        let_cxx_string!(name = name.as_ref());
        let mut status = BridgeStatus::default();
        self.inner.drop_column_family_raw(&name, &mut status);
        if status.code == StatusCode::kOk {
            Ok(())
        } else {
            Err(status)
        }
    }

    fn get_column_family_names(&self) -> Vec<String> {
        self.inner.get_column_family_names_raw().iter().map(|v| v.to_string_lossy().to_string()).collect()
    }
}