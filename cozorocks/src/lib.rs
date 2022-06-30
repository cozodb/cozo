use autocxx::prelude::*;
use std::os::raw::c_char;
use std::pin::Pin;

include_cpp! {
    #include "bridge.h"

    safety!(unsafe)
    generate!("rocksdb::ReadOptions")
    generate!("rocksdb::WriteOptions")
    generate!("rocksdb::Options")
    generate!("rocksdb::DBOptions")
    generate!("rocksdb::Status")
    generate_pod!("rocksdb::Slice")
    generate!("rocksdb::PinnableSlice")
    generate!("rocksdb::TransactionOptions")
    generate!("rocksdb::OptimisticTransactionOptions")
    generate!("rocksdb::TransactionDBOptions")
    generate!("rocksdb::OptimisticTransactionDBOptions")
    generate!("rocksdb::FlushOptions")
    generate!("rocksdb::Iterator")
    generate!("rocksdb::Transaction")
    generate!("rocksdb::TransactionDB")
    generate!("rocksdb::OptimisticTransactionDB")
    generate!("rocksdb::StackableDB")
    generate!("rocksdb::DB")
    generate!("rocksdb::Snapshot")
    generate_ns!("rocksdb_additions")
}

pub use autocxx::{c_int, c_void};
pub use cxx::{CxxString, CxxVector, SharedPtr, UniquePtr};
pub use ffi::rocksdb::Status_Code as StatusCode;
pub use ffi::rocksdb::Status_Severity as StatusSeverity;
pub use ffi::rocksdb::Status_SubCode as StatusSubCode;
pub use ffi::rocksdb::*;
pub use ffi::rocksdb_additions::*;

pub struct DbStatus {
    pub code: StatusCode,
    pub subcode: StatusSubCode,
    pub severity: StatusSeverity,
}

#[inline(always)]
fn convert_status(status: &ffi::rocksdb::Status) -> DbStatus {
    let code = status.code();
    let subcode = status.subcode();
    let severity = status.severity();
    DbStatus {
        code,
        subcode,
        severity,
    }
}

#[inline(always)]
pub fn convert_slice(src: &[u8]) -> Slice {
    Slice {
        data_: src.as_ptr() as *const c_char,
        size_: src.len(),
    }
}

#[inline(always)]
pub fn convert_slice_back(src: &Slice) -> &[u8] {
    unsafe { std::slice::from_raw_parts(src.data() as *const u8, src.size()) }
}

impl DbBridge {
    #[inline]
    fn get_raw_db(&self) -> Pin<&mut DB> {
        unsafe { Pin::new_unchecked(&mut *self.inner_db()) }
    }
    #[inline]
    fn get_tdb(&self) -> Pin<&mut TransactionDB> {
        debug_assert_eq!(self.kind(), DbKind::PESSIMISTIC);
        unsafe { Pin::new_unchecked(&mut *self.inner_tdb()) }
    }
    #[inline]
    fn get_odb(&self) -> Pin<&mut OptimisticTransactionDB> {
        debug_assert_eq!(self.kind(), DbKind::OPTIMISTIC);
        unsafe { Pin::new_unchecked(&mut *self.inner_odb()) }
    }

    #[inline]
    pub fn p_txn(
        &self,
        write_options: &WriteOptions,
        txn_options: &TransactionOptions,
    ) -> UniquePtr<Transaction> {
        let tdb = self.get_tdb();
        unsafe {
            UniquePtr::from_raw(tdb.BeginTransaction(
                write_options,
                txn_options,
                std::ptr::null_mut(),
            ))
        }
    }

    #[inline]
    pub fn o_txn(
        &self,
        write_options: &WriteOptions,
        txn_options: &OptimisticTransactionOptions,
    ) -> UniquePtr<Transaction> {
        let odb = self.get_odb();
        unsafe {
            UniquePtr::from_raw(odb.BeginTransaction(
                write_options,
                txn_options,
                std::ptr::null_mut(),
            ))
        }
    }

    #[inline]
    pub fn get(
        &self,
        opts: &ReadOptions,
        key: impl AsRef<[u8]>,
        val: Pin<&mut PinnableSlice>,
    ) -> DbStatus {
        let db = self.get_raw_db();
        let key = convert_slice(key.as_ref());
        let cf = db.DefaultColumnFamily();

        moveit! { let status = unsafe {
            let val = Pin::into_inner_unchecked(val) as *mut PinnableSlice;
            db.Get1(opts, cf, &key, val)
        }; }
        convert_status(&status)
    }

    #[inline]
    pub fn put(
        &self,
        opts: &WriteOptions,
        key: impl AsRef<[u8]>,
        val: impl AsRef<[u8]>,
    ) -> DbStatus {
        let db = self.get_raw_db();
        let key = convert_slice(key.as_ref());
        let val = convert_slice(val.as_ref());
        moveit! { let status = db.Put2(opts, &key, &val); }
        convert_status(&status)
    }

    #[inline]
    pub fn delete(&self, opts: &WriteOptions, key: impl AsRef<[u8]>) -> DbStatus {
        let db = self.get_raw_db();
        let key = convert_slice(key.as_ref());
        moveit! { let status = db.Delete2(opts, &key); }
        convert_status(&status)
    }
    #[inline]
    pub fn delete_range(
        &self,
        opts: &WriteOptions,
        start: impl AsRef<[u8]>,
        end: impl AsRef<[u8]>,
    ) -> DbStatus {
        let db = self.get_raw_db();
        let start = convert_slice(start.as_ref());
        let end = convert_slice(end.as_ref());
        let cf = db.DefaultColumnFamily();
        moveit! { let status = unsafe { db.DeleteRange(opts, cf, &start, &end) }; }
        convert_status(&status)
    }
    #[inline]
    pub fn iterator(&self, opts: &ReadOptions) -> UniquePtr<Iterator> {
        let db = self.get_raw_db();
        unsafe { UniquePtr::from_raw(db.NewIterator1(opts)) }
    }

    #[inline]
    pub fn get_snapshot(&self) -> *const Snapshot {
        let db = self.get_raw_db();
        db.GetSnapshot()
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    #[inline]
    pub fn release_snapshot(&self, snapshot: *const Snapshot) {
        let db = self.get_raw_db();
        unsafe {
            db.ReleaseSnapshot(snapshot)
        }
    }
}

#[macro_export]
macro_rules! let_pinnable_slice {
    ($i:ident) => {
        $crate::moveit! {
            let mut $i = $crate::PinnableSlice::new();
        }
    };
}

#[macro_export]
macro_rules! let_write_opts {
    ($i:ident = {$( $opt_name:ident => $opt_val:expr ),*}) => {
        $crate::moveit! {
            let mut $i = $crate::WriteOptions::new();
        }
        let_write_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, ) => {};
    ( @let_opts, $i:ident, sync, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_w_opts_sync($i.as_mut(), $val);
        let_write_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, disable_wal, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_w_opts_disable_wal($i.as_mut(), $val);
        let_write_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, low_pri, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_w_opts_low_pri($i.as_mut(), $val);
        let_write_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
}

#[macro_export]
macro_rules! let_read_opts {
    ($i:ident = {$( $opt_name:ident => $opt_val:expr ),*}) => {
        $crate::moveit! {
            let mut $i = $crate::ReadOptions::new();
        }
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, ) => {};
    ( @let_opts, $i:ident, lower_bound, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_iterate_lower_bound($i.as_mut(), &$crate::convert_slice($val.as_ref()));
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, upper_bound, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_iterate_upper_bound($i.as_mut(), &$crate::convert_slice($val.as_ref()));
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, snapshot, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_snapshot($i.as_mut(), $val);
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, total_order_seek, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_r_opts_total_order_seek($i.as_mut(), $val);
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, auto_prefix_mode, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_r_opts_auto_prefix_mode($i.as_mut(), $val);
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, prefix_same_as_start, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_r_opts_prefix_same_as_start($i.as_mut(), $val);
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, tailing, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_r_opts_tailing($i.as_mut(), $val);
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, pin_data, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_r_opts_pin_data($i.as_mut(), $val);
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, verify_checksums, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_r_opts_verify_checksums($i.as_mut(), $val);
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
    ( @let_opts, $i:ident, fill_cache, $val:expr, $( $opt_name:ident, $opt_val:expr, )* ) => {
        $crate::ffi::rocksdb_additions::set_r_opts_fill_cache($i.as_mut(), $val);
        let_read_opts! { @let_opts, $i, $( $opt_name, $opt_val, )* }
    };
}

unsafe impl Send for RustComparator {}
unsafe impl Sync for RustComparator {}

#[cfg(test)]
mod tests {
    use super::ffi::rocksdb::{Options, ReadOptions, WriteOptions};
    use super::*;
    use std::mem::{size_of, size_of_val};

    #[test]
    fn it_works() {
        dbg!(size_of::<ReadOptions>());
        for i in 0..100000 {
            let mut g_opts = Options::new().within_unique_ptr();
            g_opts.pin_mut().OptimizeForSmallDb();

            let lower_bound = "lower".as_bytes();
            let upper_bound = "upper".as_bytes();
            let should_tail = true;

            let_read_opts!(r_opts = { lower_bound => lower_bound, tailing => should_tail, upper_bound => upper_bound });
            let_write_opts!(w_opts = { disable_wal => true });
        }
        dbg!(size_of::<ReadOptions>());
        // let cmp = RustComparator::new().within_unique_ptr();

        #[no_mangle]
        extern "C" fn rusty_cmp(
            a: &ffi::rocksdb::Slice,
            b: &ffi::rocksdb::Slice,
        ) -> autocxx::c_int {
            dbg!(convert_slice_back(a));
            dbg!(convert_slice_back(b));
            autocxx::c_int(0)
        }

        let cmp = unsafe {
            let f_ptr = rusty_cmp as *const autocxx::c_void;
            new_rust_comparator("hello", false, f_ptr)
        };

        let a = convert_slice(&[1, 2, 3]);
        let b = convert_slice(&[4, 5, 6, 7]);
        cmp.Compare(&a, &b);
    }
}
