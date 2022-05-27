mod bridge;
mod options;
mod status;

pub use bridge::BridgeStatus;
pub use bridge::PinnableSlice;
pub use bridge::Slice;
pub use bridge::StatusBridgeCode;
pub use bridge::StatusCode;
pub use bridge::StatusSeverity;
pub use bridge::StatusSubCode;
use bridge::*;
use cxx::let_cxx_string;
pub use cxx::{SharedPtr, UniquePtr};
pub use options::*;
pub use status::BridgeError;
use status::Result;
use std::ops::Deref;
use std::pin::Pin;

pub struct PinnableSlicePtr(UniquePtr<PinnableSlice>);

impl PinnableSlicePtr {
    #[inline]
    pub fn pin_mut(&mut self) -> Pin<&mut PinnableSlice> {
        self.0.pin_mut()
    }

    #[inline]
    pub fn to_shared(self) -> PinnableSlicePtrShared {
        PinnableSlicePtrShared(make_shared_pinnable_slice(self.0))
    }
}

impl Default for PinnableSlicePtr {
    #[inline]
    fn default() -> Self {
        PinnableSlicePtr(new_pinnable_slice())
    }
}

impl PinnableSlicePtr {
    #[inline]
    pub fn reset(&mut self) {
        reset_pinnable_slice(self.pin_mut());
    }
}

impl AsRef<[u8]> for PinnableSlicePtr {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        convert_pinnable_slice_back(&self.0)
    }
}

impl Deref for PinnableSlicePtr {
    type Target = PinnableSlice;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct PinnableSlicePtrShared(SharedPtr<PinnableSlice>);

impl AsRef<[u8]> for PinnableSlicePtrShared {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        convert_pinnable_slice_back(&self.0)
    }
}

impl Deref for PinnableSlicePtrShared {
    type Target = PinnableSlice;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct SlicePtr(UniquePtr<Slice>);

impl SlicePtr {
    #[inline]
    pub fn pin_mut(&mut self) -> Pin<&mut Slice> {
        self.0.pin_mut()
    }

    #[inline]
    pub fn to_shared(self) -> SlicePtrShared {
        SlicePtrShared(make_shared_slice(self.0))
    }
}

impl AsRef<[u8]> for SlicePtr {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        convert_slice_back(&self.0)
    }
}

impl Deref for SlicePtr {
    type Target = Slice;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct SlicePtrShared(SharedPtr<Slice>);

impl AsRef<[u8]> for SlicePtrShared {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        convert_slice_back(&self.0)
    }
}

impl Deref for SlicePtrShared {
    type Target = Slice;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
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

pub struct PrefixIterator<P: AsRef<[u8]>> {
    iter: IteratorPtr,
    started: bool,
    prefix: P,
}

impl<P: AsRef<[u8]>> PrefixIterator<P> {
    #[inline]
    pub fn reset_prefix(&mut self, prefix: P) {
        self.prefix = prefix;
        self.iter.seek(self.prefix.as_ref());
        self.started = false;
    }
}

impl<P: AsRef<[u8]>> Iterator for PrefixIterator<P> {
    type Item = (SlicePtr, SlicePtr);
    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            self.iter.next()
        } else {
            self.started = true
        }
        match self.iter.pair() {
            None => None,
            Some((k, v)) => {
                if k.as_ref().starts_with(self.prefix.as_ref()) {
                    Some((k, v))
                } else {
                    None
                }
            }
        }
    }
}

pub struct RowIterator {
    iter: IteratorPtr,
    started: bool,
}

impl Iterator for RowIterator {
    type Item = (SlicePtr, SlicePtr);
    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            self.iter.next()
        } else {
            self.started = true
        }
        self.iter.pair()
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
    pub fn iter_rows<T: AsRef<[u8]>>(self, prefix: T) -> RowIterator {
        self.seek(prefix.as_ref());
        RowIterator {
            iter: self,
            started: false,
        }
    }
    #[inline]
    pub fn iter_prefix<T: AsRef<[u8]>>(self, prefix: T) -> PrefixIterator<T> {
        self.seek(prefix.as_ref());
        PrefixIterator {
            iter: self,
            started: false,
            prefix,
        }
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

#[derive(Clone)]
pub struct TransactionPtr(SharedPtr<TransactionBridge>);

impl Deref for TransactionPtr {
    type Target = TransactionBridge;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TransactionPtr {
    /// # Safety
    ///
    /// Only for testing use, as a placeholder
    #[inline]
    pub unsafe fn null() -> Self {
        TransactionPtr(SharedPtr::null())
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
        key: impl AsRef<[u8]>,
        slice: &mut PinnableSlicePtr,
    ) -> Result<bool> {
        let mut status = BridgeStatus::default();
        let res = {
            self.get_txn(options, key.as_ref(), slice.pin_mut(), &mut status);
            status.check_err(())
        };
        match res {
            Ok(_) => Ok(true),
            Err(e) if e.status.code == StatusCode::kNotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
    #[inline]
    pub fn get_owned(
        &self,
        options: &ReadOptions,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<PinnableSlicePtr>> {
        let mut slice = PinnableSlicePtr::default();
        if self.get(options, key, &mut slice)? {
            Ok(Some(slice))
        } else {
            Ok(None)
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
        self.get_for_update_txn(options, key.as_ref(), slice.pin_mut(), &mut status);
        match status.check_err(()) {
            Ok(_) => Ok(true),
            Err(e) if e.status.code == StatusCode::kNotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
    #[inline]
    pub fn get_for_update_owned(
        &self,
        options: &ReadOptions,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<PinnableSlicePtr>> {
        let mut slice = PinnableSlicePtr::default();
        if self.get_for_update(options, key, &mut slice)? {
            Ok(Some(slice))
        } else {
            Ok(None)
        }
    }
    #[inline]
    pub fn del(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let mut status = BridgeStatus::default();
        let ret = self.del_txn(key.as_ref(), &mut status);
        status.check_err(ret)
    }
    #[inline]
    pub fn put(&self, key: impl AsRef<[u8]>, val: impl AsRef<[u8]>) -> Result<()> {
        let mut status = BridgeStatus::default();
        let ret = self.put_txn(key.as_ref(), val.as_ref(), &mut status);
        status.check_err(ret)
    }
    #[inline]
    pub fn iterator(&self, options: &ReadOptions) -> IteratorPtr {
        IteratorPtr(self.iterator_txn(options))
    }
}

#[derive(Clone)]
pub struct DbPtr(SharedPtr<TDBBridge>);

impl Deref for DbPtr {
    type Target = SharedPtr<TDBBridge>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl Send for DbPtr {}

unsafe impl Sync for DbPtr {}

impl DbPtr {
    /// # Safety
    ///
    /// Only for testing use, as a placeholder
    pub unsafe fn null() -> Self {
        DbPtr(SharedPtr::null())
    }

    pub fn open_non_txn(options: &Options, path: impl AsRef<str>) -> Result<Self> {
        let_cxx_string!(cname = path.as_ref());
        let mut status = BridgeStatus::default();
        let ret = open_db_raw(options, &cname, &mut status);
        status.check_err(Self(ret))
    }

    pub fn open(options: &Options, t_options: &TDbOptions, path: impl AsRef<str>) -> Result<Self> {
        let_cxx_string!(cname = path.as_ref());
        let mut status = BridgeStatus::default();
        let ret = match t_options {
            TDbOptions::Pessimistic(o) => open_tdb_raw(options, o, &cname, &mut status),
            TDbOptions::Optimistic(_o) => open_odb_raw(options, &cname, &mut status),
        };
        status.check_err(Self(ret))
    }

    #[inline]
    pub fn get(
        &self,
        options: &ReadOptions,
        key: impl AsRef<[u8]>,
        slice: &mut PinnableSlicePtr,
    ) -> Result<bool> {
        let mut status = BridgeStatus::default();
        let res = {
            self.get_raw(options, key.as_ref(), slice.pin_mut(), &mut status);
            status.check_err(())
        };
        match res {
            Ok(_) => Ok(true),
            Err(e) if e.status.code == StatusCode::kNotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
    #[inline]
    pub fn get_owned(
        &self,
        options: &ReadOptions,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<PinnableSlicePtr>> {
        let mut slice = PinnableSlicePtr::default();
        if self.get(options, key, &mut slice)? {
            Ok(Some(slice))
        } else {
            Ok(None)
        }
    }
    #[inline]
    pub fn del(&self, options: &WriteOptions, key: impl AsRef<[u8]>) -> Result<()> {
        let mut status = BridgeStatus::default();
        let ret = self.del_raw(options, key.as_ref(), &mut status);
        status.check_err(ret)
    }
    #[inline]
    pub fn put(
        &self,
        options: &WriteOptions,
        key: impl AsRef<[u8]>,
        val: impl AsRef<[u8]>,
    ) -> Result<()> {
        let mut status = BridgeStatus::default();
        let ret = self.put_raw(options, key.as_ref(), val.as_ref(), &mut status);
        status.check_err(ret)
    }
    #[inline]
    pub fn iterator(&self, options: &ReadOptions) -> IteratorPtr {
        IteratorPtr(self.iterator_raw(options))
    }

    #[inline]
    pub fn txn(&self, options: TransactOptions, write_ops: WriteOptionsPtr) -> TransactionPtr {
        TransactionPtr(match options {
            TransactOptions::Optimistic(o) => self.begin_o_transaction(write_ops.0, o.0),
            TransactOptions::Pessimistic(o) => self.begin_t_transaction(write_ops.0, o.0),
        })
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
    pub fn get_approximate_sizes<T: AsRef<[u8]>>(&self, ranges: &[(T, T)]) -> Result<Vec<u64>> {
        let mut status = BridgeStatus::default();
        let n = ranges.len();
        let mut ret = vec![0u64; n];

        let mut bridge_range = Vec::with_capacity(2 * n);
        for (start, end) in ranges {
            let start = start.as_ref();
            let end = end.as_ref();
            bridge_range.push(start);
            bridge_range.push(end);
        }

        self.get_approximate_sizes_raw(&bridge_range, &mut ret, &mut status);

        status.check_err(ret)
    }

    #[inline]
    pub fn close(&self) -> Result<()> {
        let mut status = BridgeStatus::default();
        self.close_raw(&mut status);
        status.check_err(())
    }
}

pub fn destroy_db(options: &Options, path: impl AsRef<str>) -> Result<()> {
    let_cxx_string!(cname = path.as_ref());
    let mut status = BridgeStatus::default();
    destroy_db_raw(options, &cname, &mut status);
    status.check_err(())
}

pub fn repair_db(options: &Options, path: impl AsRef<str>) -> Result<()> {
    let_cxx_string!(cname = path.as_ref());
    let mut status = BridgeStatus::default();
    repair_db_raw(options, &cname, &mut status);
    status.check_err(())
}
