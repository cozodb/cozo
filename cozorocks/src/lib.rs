mod bridge;

use bridge::*;

use std::fmt::{Display, Formatter};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use cxx::{let_cxx_string};
pub use cxx::{UniquePtr, SharedPtr};
pub use bridge::BridgeStatus;
pub use bridge::StatusBridgeCode;
pub use bridge::StatusCode;
pub use bridge::StatusSubCode;
pub use bridge::StatusSeverity;
pub use bridge::Slice;
pub use bridge::PinnableSlice;
pub use bridge::ColumnFamilyHandle;


impl std::fmt::Display for BridgeStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

#[derive(Debug)]
pub struct BridgeError {
    pub status: BridgeStatus,
}

impl Display for BridgeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl std::error::Error for BridgeError {}

impl Default for BridgeStatus {
    fn default() -> Self {
        BridgeStatus {
            code: StatusCode::kOk,
            subcode: StatusSubCode::kNone,
            severity: StatusSeverity::kNoError,
            bridge_code: StatusBridgeCode::OK,
        }
    }
}

impl BridgeStatus {
    fn check_err<T>(self, data: T) -> Result<T> {
        let err: Option<BridgeError> = self.into();
        match err {
            Some(e) => Err(e),
            None => Ok(data)
        }
    }
}

impl From<BridgeStatus> for Option<BridgeError> {
    fn from(s: BridgeStatus) -> Self {
        if s.severity == StatusSeverity::kNoError && s.bridge_code == StatusBridgeCode::OK {
            None
        } else {
            Some(BridgeError { status: s })
        }
    }
}

pub type Result<T> = std::result::Result<T, BridgeError>;

pub trait SlicePtr {
    fn as_bytes(&self) -> &[u8];
}

impl SlicePtr for UniquePtr<Slice> {
    fn as_bytes(&self) -> &[u8] {
        convert_slice_back(self)
    }
}

impl SlicePtr for UniquePtr<PinnableSlice> {
    fn as_bytes(&self) -> &[u8] {
        convert_pinnable_slice_back(self)
    }
}

pub struct RustComparatorPtr(UniquePtr<RustComparator>);

impl RustComparatorPtr {
    pub fn new(name: &str, cmp: fn(&[u8], &[u8]) -> i8) -> Self {
        Self(new_rust_comparator(name, cmp))
    }
}

impl Deref for RustComparatorPtr {
    type Target = UniquePtr<RustComparator>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct OptionsPtr(UniquePtr<Options>);

impl Deref for OptionsPtr {
    type Target = UniquePtr<Options>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OptionsPtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl OptionsPtr {
    pub fn default() -> Self {
        Self(new_options())
    }
    pub fn prepare_for_bulk_load(&mut self) -> &mut Self {
        prepare_for_bulk_load(self.pin_mut());
        self
    }
    pub fn increase_parallelism(&mut self) -> &mut Self {
        increase_parallelism(self.pin_mut());
        self
    }
    pub fn optimize_level_style_compaction(&mut self) -> &mut Self {
        optimize_level_style_compaction(self.pin_mut());
        self
    }
    pub fn set_create_if_missing(&mut self, v: bool) -> &mut Self {
        set_create_if_missing(self.pin_mut(), v);
        self
    }
    pub fn set_comparator(&mut self, cmp: &RustComparatorPtr) -> &mut Self {
        set_comparator(self.pin_mut(), cmp);
        self
    }
}


pub struct ReadOptionsPtr(UniquePtr<ReadOptions>);

impl Deref for ReadOptionsPtr {
    type Target = UniquePtr<ReadOptions>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReadOptionsPtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


impl ReadOptionsPtr {
    pub fn default() -> Self {
        Self(new_read_options())
    }
    pub fn set_verify_checksums(&mut self, v: bool) -> &mut Self {
        set_verify_checksums(self.pin_mut(), v);
        self
    }
    pub fn set_total_order_seek(&mut self, v: bool) -> &mut Self {
        set_total_order_seek(self.pin_mut(), v);
        self
    }
}

pub struct WriteOptionsPtr(UniquePtr<WriteOptions>);

impl Deref for WriteOptionsPtr {
    type Target = UniquePtr<WriteOptions>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WriteOptionsPtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl WriteOptionsPtr {
    pub fn default() -> Self {
        Self(new_write_options())
    }
    pub fn set_disable_wal(&mut self, v: bool) -> &mut Self {
        set_disable_wal(self.pin_mut(), v);
        self
    }
}

pub struct TransactionOptionsPtr(UniquePtr<TransactionOptions>);

impl Deref for TransactionOptionsPtr {
    type Target = UniquePtr<TransactionOptions>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TransactionOptionsPtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TransactionOptionsPtr {
    pub fn default() -> Self {
        Self(new_transaction_options())
    }
    pub fn set_deadlock_detect(&mut self, v: bool) -> &mut Self {
        set_deadlock_detect(self.pin_mut(), v);
        self
    }
}

pub struct OptimisticTransactionOptionsPtr(UniquePtr<OptimisticTransactionOptions>);

impl Deref for OptimisticTransactionOptionsPtr {
    type Target = UniquePtr<OptimisticTransactionOptions>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OptimisticTransactionOptionsPtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl OptimisticTransactionOptionsPtr {
    pub fn new(cmp: &RustComparatorPtr) -> Self {
        Self(new_optimistic_transaction_options(cmp))
    }
}

pub struct TransactionDBOptionsPtr(UniquePtr<TransactionDBOptions>);

impl Deref for TransactionDBOptionsPtr {
    type Target = UniquePtr<TransactionDBOptions>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TransactionDBOptionsPtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TransactionDBOptionsPtr {
    pub fn default() -> Self {
        Self(new_tdb_options())
    }
}

pub struct OptimisticTransactionDBOptionsPtr(UniquePtr<OptimisticTransactionDBOptions>);

impl Deref for OptimisticTransactionDBOptionsPtr {
    type Target = UniquePtr<OptimisticTransactionDBOptions>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OptimisticTransactionDBOptionsPtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl OptimisticTransactionDBOptionsPtr {
    pub fn default() -> Self {
        Self(new_odb_options())
    }
}

pub struct IteratorPtr(UniquePtr<IteratorBridge>);

impl Deref for IteratorPtr {
    type Target = UniquePtr<IteratorBridge>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IteratorPtr {
    pub fn to_first(&self) {
        IteratorBridge::seek_to_first(self)
    }
    pub fn to_last(&self) {
        IteratorBridge::seek_to_last(self)
    }
    pub fn next(&self) {
        IteratorBridge::next(self)
    }
    pub fn is_valid(&self) -> bool {
        IteratorBridge::is_valid(self)
    }
    pub fn seek(&self, key: impl AsRef<[u8]>) {
        IteratorBridge::do_seek(self, key.as_ref())
    }
    pub fn seek_for_prev(&self, key: impl AsRef<[u8]>) {
        IteratorBridge::do_seek_for_prev(self, key.as_ref())
    }
    pub fn key(&self) -> UniquePtr<Slice> {
        IteratorBridge::key_raw(self)
    }
    pub fn val(&self) -> UniquePtr<Slice> {
        IteratorBridge::value_raw(self)
    }
    pub fn status(&self) -> BridgeStatus {
        IteratorBridge::status(self)
    }
    pub fn iter(&self) -> KVIterator {
        KVIterator { it: self }
    }
    pub fn keys(&self) -> KeyIterator {
        KeyIterator { it: self }
    }
}

pub struct KVIterator<'a> {
    it: &'a IteratorPtr,
}

impl Iterator for KVIterator<'_> {
    type Item = (UniquePtr<Slice>, UniquePtr<Slice>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.it.is_valid() {
            let ret = (self.it.key(), self.it.val());
            self.next();
            Some(ret)
        } else {
            None
        }
    }
}


pub struct KeyIterator<'a> {
    it: &'a IteratorPtr,
}

impl Iterator for KeyIterator<'_> {
    type Item = UniquePtr<Slice>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.it.is_valid() {
            let ret = self.it.key();
            self.next();
            Some(ret)
        } else {
            None
        }
    }
}

pub struct TransactionPtr(UniquePtr<TransactionBridge>);

impl Deref for TransactionPtr {
    type Target = UniquePtr<TransactionBridge>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl TransactionPtr {
    pub fn set_snapshot(&self) {
        TransactionBridge::set_snapshot(self)
    }
    pub fn commit(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        TransactionBridge::commit(self, &mut status);
        status.check_err(())
    }
    pub fn rollback(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        TransactionBridge::rollback(self, &mut status);
        status.check_err(())
    }
    pub fn set_savepoint(&self) {
        TransactionBridge::set_savepoint(self);
    }
    pub fn rollback_to_savepoint(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        TransactionBridge::rollback_to_savepoint(self, &mut status);
        status.check_err(())
    }
    pub fn pop_savepoint(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        TransactionBridge::pop_savepoint(self, &mut status);
        status.check_err(())
    }
    pub fn get(&self, transact: bool, cf: &ColumnFamilyHandle, key: impl AsRef<[u8]>) -> Result<UniquePtr<PinnableSlice>> {
        let mut status = BridgeStatus::default();
        if transact {
            let ret = self.get_txn(cf, key.as_ref(), &mut status);
            status.check_err(ret)
        } else {
            let ret = self.get_raw(cf, key.as_ref(), &mut status);
            status.check_err(ret)
        }
    }
    pub fn get_for_update(&self, cf: &ColumnFamilyHandle, key: impl AsRef<[u8]>) -> Result<UniquePtr<PinnableSlice>> {
        let mut status = BridgeStatus::default();
        let ret = self.get_for_update_txn(cf, key.as_ref(), &mut status);
        status.check_err(ret)
    }
    pub fn del(&self, transact: bool, cf: &ColumnFamilyHandle, key: impl AsRef<[u8]>) -> Result<()> {
        let mut status = BridgeStatus::default();
        if transact {
            let ret = self.del_txn(cf, key.as_ref(), &mut status);
            status.check_err(ret)
        } else {
            let ret = self.del_raw(cf, key.as_ref(), &mut status);
            status.check_err(ret)
        }
    }
    pub fn put(&self, transact: bool, cf: &ColumnFamilyHandle, key: impl AsRef<[u8]>, val: impl AsRef<[u8]>) -> Result<()> {
        let mut status = BridgeStatus::default();
        if transact {
            let ret = self.put_txn(cf, key.as_ref(), val.as_ref(), &mut status);
            status.check_err(ret)
        } else {
            let ret = self.put_raw(cf, key.as_ref(), val.as_ref(), &mut status);
            status.check_err(ret)
        }
    }
    pub fn iterator(&self, transact: bool, cf: &ColumnFamilyHandle) -> IteratorPtr {
        if transact {
            IteratorPtr(self.iterator_txn(cf))
        } else {
            IteratorPtr(self.iterator_raw(cf))
        }
    }
}

pub struct DBPtr(UniquePtr<TDBBridge>);

impl Deref for DBPtr {
    type Target = UniquePtr<TDBBridge>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl Send for DBPtr {}

unsafe impl Sync for DBPtr {}

pub enum TransactOption {
    Pessimistic(TransactionOptionsPtr),
    Optimistic(OptimisticTransactionOptionsPtr),
}

impl DBPtr {
    pub fn open_pessimistic(options: &OptionsPtr, t_options: &TransactionDBOptionsPtr, path: impl AsRef<str>) -> Result<Self> {
        let_cxx_string!(cname = path.as_ref());
        let mut status = BridgeStatus::default();
        let ret = open_tdb_raw(options, t_options, &cname, &mut status);
        status.check_err(Self(ret))
    }

    pub fn open_optimistic(options: &OptionsPtr, t_options: &OptimisticTransactionDBOptionsPtr, path: impl AsRef<str>) -> Result<Self> {
        let_cxx_string!(cname = path.as_ref());
        let mut status = BridgeStatus::default();
        let ret = open_odb_raw(options, t_options, &cname, &mut status);
        status.check_err(Self(ret))
    }

    pub fn make_transaction(&self,
                            options: TransactOption,
                            read_ops: ReadOptionsPtr,
                            raw_read_ops: ReadOptionsPtr,
                            write_ops: WriteOptionsPtr,
                            raw_write_ops: WriteOptionsPtr,
    ) -> TransactionPtr {
        TransactionPtr(match options {
            TransactOption::Optimistic(o) => {
                self.begin_o_transaction(
                    write_ops.0,
                    raw_write_ops.0,
                    read_ops.0,
                    raw_read_ops.0,
                    o.0,
                )
            }
            TransactOption::Pessimistic(o) => {
                self.begin_t_transaction(
                    write_ops.0,
                    raw_write_ops.0,
                    read_ops.0,
                    raw_read_ops.0,
                    o.0,
                )
            }
        })
    }

    pub fn get_cf(&self, name: impl AsRef<str>) -> Option<SharedPtr<ColumnFamilyHandle>> {
        let_cxx_string!(cname = name.as_ref());
        let spt = self.get_cf_handle_raw(&cname);
        if spt.is_null() {
            None
        } else {
            Some(spt)
        }
    }

    pub fn create_cf(&self, options: &OptionsPtr, name: impl AsRef<str>) -> Result<()> {
        let_cxx_string!(name = name.as_ref());
        let mut status = BridgeStatus::default();
        self.create_column_family_raw(options, &name, &mut status);
        status.check_err(())
    }

    pub fn drop_cf(&self, name: impl AsRef<str>) -> Result<()> {
        let_cxx_string!(name = name.as_ref());
        let mut status = BridgeStatus::default();
        self.drop_column_family_raw(&name, &mut status);
        status.check_err(())
    }

    pub fn cf_names(&self) -> Vec<String> {
        self.get_column_family_names_raw().iter().map(|v| v.to_string_lossy().to_string()).collect()
    }
}