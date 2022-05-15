mod bridge;
mod status;
mod options;

use bridge::*;
pub use options::*;

pub use bridge::BridgeStatus;
pub use bridge::PinnableSlice;
pub use bridge::Slice;
pub use bridge::StatusBridgeCode;
pub use bridge::StatusCode;
pub use bridge::StatusSeverity;
pub use bridge::StatusSubCode;
use cxx::let_cxx_string;
pub use cxx::{SharedPtr, UniquePtr};
use std::fmt::Debug;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use status::Result;


pub struct PinnableSlicePtr(UniquePtr<PinnableSlice>);

impl Default for PinnableSlicePtr {
    fn default() -> Self {
        PinnableSlicePtr(new_pinnable_slice())
    }
}

impl PinnableSlicePtr {
    pub fn reset(&mut self) {
        reset_pinnable_slice(self.0.pin_mut());
    }
}

impl AsRef<[u8]> for PinnableSlicePtr {
    fn as_ref(&self) -> &[u8] {
        convert_pinnable_slice_back(&self.0)
    }
}

impl Deref for PinnableSlicePtr {
    type Target = PinnableSlice;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct SlicePtr(UniquePtr<Slice>);

impl AsRef<[u8]> for SlicePtr {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        convert_slice_back(&self.0)
    }
}

pub struct IteratorPtr(UniquePtr<IteratorBridge>);

impl<'a> Deref for IteratorPtr {
    type Target = UniquePtr<IteratorBridge>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IteratorPtr {
    #[inline]
    pub fn to_first(&self) {
        IteratorBridge::seek_to_first(self)
    }
    #[inline]
    pub fn to_last(&self) {
        IteratorBridge::seek_to_last(self)
    }
    #[inline]
    pub fn next(&self) {
        IteratorBridge::next(self)
    }
    #[inline]
    pub fn is_valid(&self) -> bool {
        IteratorBridge::is_valid(self)
    }
    #[inline]
    pub fn seek(&self, key: impl AsRef<[u8]>) {
        IteratorBridge::do_seek(self, key.as_ref())
    }
    #[inline]
    pub fn seek_for_prev(&self, key: impl AsRef<[u8]>) {
        IteratorBridge::do_seek_for_prev(self, key.as_ref())
    }
    #[inline]
    pub fn refresh(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        IteratorBridge::refresh(self, &mut status);
        status.check_err(())
    }

    #[inline]
    pub fn key(&self) -> Option<SlicePtr> {
        if self.is_valid() {
            Some(SlicePtr(IteratorBridge::key_raw(self)))
        } else {
            None
        }
    }
    #[inline]
    pub fn val(&self) -> Option<SlicePtr> {
        if self.is_valid() {
            Some(SlicePtr(IteratorBridge::value_raw(self)))
        } else {
            None
        }
    }
    #[inline]
    pub fn pair(&self) -> Option<(SlicePtr, SlicePtr)> {
        if self.is_valid() {
            Some((
                SlicePtr(IteratorBridge::key_raw(self)),
                SlicePtr(IteratorBridge::value_raw(self)),
            ))
        } else {
            None
        }
    }
    #[inline]
    pub fn status(&self) -> BridgeStatus {
        IteratorBridge::status(self)
    }
}

pub struct TransactionPtr(UniquePtr<TransactionBridge>);

impl Deref for TransactionPtr {
    type Target = UniquePtr<TransactionBridge>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TransactionPtr {
    #[inline]
    pub fn null() -> Self {
        TransactionPtr(UniquePtr::null())
    }
    #[inline]
    pub fn set_snapshot(&self) {
        TransactionBridge::set_snapshot(self)
    }
    #[inline]
    pub fn commit(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        TransactionBridge::commit(self, &mut status);
        status.check_err(())
    }
    #[inline]
    pub fn rollback(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        TransactionBridge::rollback(self, &mut status);
        status.check_err(())
    }
    #[inline]
    pub fn set_savepoint(&self) {
        TransactionBridge::set_savepoint(self);
    }
    #[inline]
    pub fn rollback_to_savepoint(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        TransactionBridge::rollback_to_savepoint(self, &mut status);
        status.check_err(())
    }
    #[inline]
    pub fn pop_savepoint(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        TransactionBridge::pop_savepoint(self, &mut status);
        status.check_err(())
    }
    #[inline]
    pub fn get(
        &self,
        options: &ReadOptions,
        transact: bool,
        key: impl AsRef<[u8]>,
        slice: &mut PinnableSlicePtr,
    ) -> Result<bool> {
        let mut status = BridgeStatus::default();
        let res = if transact {
            let ret = self.get_txn(options, key.as_ref(), slice.0.pin_mut(), &mut status);
            status.check_err(())
        } else {
            let ret = self.get_raw(options, key.as_ref(), slice.0.pin_mut(), &mut status);
            status.check_err(())
        };
        match res {
            Ok(r) => Ok(true),
            Err(e) if e.status.code == StatusCode::kNotFound => Ok(false),
            Err(e) => Err(e)
        }
    }
    #[inline]
    pub fn get_for_update(
        &self,
        options: &ReadOptions,
        key: impl AsRef<[u8]>,
        slice: &mut PinnableSlicePtr,
    ) -> Result<bool> {
        let mut status = BridgeStatus::default();
        let ret = self.get_for_update_txn(options, key.as_ref(), slice.0.pin_mut(), &mut status);
        match status.check_err(()) {
            Ok(r) => Ok(true),
            Err(e) if e.status.code == StatusCode::kNotFound => Ok(false),
            Err(e) => Err(e)
        }
    }
    #[inline]
    pub fn del(
        &self,
        options: &WriteOptions,
        transact: bool,
        key: impl AsRef<[u8]>,
    ) -> Result<()> {
        let mut status = BridgeStatus::default();
        if transact {
            let ret = self.del_txn(key.as_ref(), &mut status);
            status.check_err(ret)
        } else {
            let ret = self.del_raw(options, key.as_ref(), &mut status);
            status.check_err(ret)
        }
    }
    #[inline]
    pub fn del_range(
        &self,
        options: &WriteOptions,
        start_key: impl AsRef<[u8]>,
        end_key: impl AsRef<[u8]>,
    ) -> Result<()> {
        let mut status = BridgeStatus::default();
        let ret = self.del_range_raw(options, start_key.as_ref(), end_key.as_ref(), &mut status);
        status.check_err(ret)
    }
    #[inline]
    pub fn flush(&self, options: FlushOptionsPtr) -> Result<()> {
        let mut status = BridgeStatus::default();
        self.flush_raw(&options, &mut status);
        status.check_err(())
    }
    #[inline]
    pub fn compact_all(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        self.compact_all_raw(&mut status);
        status.check_err(())
    }
    #[inline]
    pub fn put(
        &self,
        options: &WriteOptions,
        transact: bool,
        key: impl AsRef<[u8]>,
        val: impl AsRef<[u8]>,
    ) -> Result<()> {
        let mut status = BridgeStatus::default();
        if transact {
            let ret = self.put_txn(key.as_ref(), val.as_ref(), &mut status);
            status.check_err(ret)
        } else {
            let ret = self.put_raw(options, key.as_ref(), val.as_ref(), &mut status);
            status.check_err(ret)
        }
    }
    #[inline]
    pub fn iterator(&self,
                    options: &ReadOptions,
                    transact: bool,
    ) -> IteratorPtr {
        if transact {
            IteratorPtr(self.iterator_txn(options))
        } else {
            IteratorPtr(self.iterator_raw(options))
        }
    }
}

pub struct DBPtr(SharedPtr<TDBBridge>);

impl Deref for DBPtr {
    type Target = SharedPtr<TDBBridge>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl Send for DBPtr {}

unsafe impl Sync for DBPtr {}

impl DBPtr {
    pub fn open(
        options: &OptionsPtr,
        t_options: &TDBOptions,
        path: impl AsRef<str>,
    ) -> Result<Self> {
        let_cxx_string!(cname = path.as_ref());
        let mut status = BridgeStatus::default();
        let ret = match t_options {
            TDBOptions::Pessimistic(o) => open_tdb_raw(options, o, &cname, &mut status),
            TDBOptions::Optimistic(_o) => open_odb_raw(options, &cname, &mut status),
        };
        status.check_err(Self(ret))
    }

    #[inline]
    pub fn make_transaction(
        &self,
        options: TransactOptions,
        write_ops: WriteOptionsPtr,
    ) -> TransactionPtr {
        TransactionPtr(match options {
            TransactOptions::Optimistic(o) => self.begin_o_transaction(
                write_ops.0,
                o.0,
            ),
            TransactOptions::Pessimistic(o) => self.begin_t_transaction(
                write_ops.0,
                o.0,
            ),
        })
    }
}
