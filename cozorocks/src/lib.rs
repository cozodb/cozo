extern crate core;

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

pub use ffi::rocksdb::Options;
pub use ffi::rocksdb::ReadOptions;
pub use ffi::rocksdb::Slice;
pub use ffi::rocksdb::Snapshot;
pub use ffi::rocksdb::Status;
pub use ffi::rocksdb::Status_Code as StatusCode;
pub use ffi::rocksdb::Status_Severity as StatusSeverity;
pub use ffi::rocksdb::Status_SubCode as StatusSubCode;
pub use ffi::rocksdb::WriteOptions;
pub use ffi::rocksdb::DB;
pub use ffi::rocksdb_additions::set_iterate_lower_bound;
pub use ffi::rocksdb_additions::set_iterate_upper_bound;
pub use ffi::rocksdb_additions::set_snapshot;

pub struct DbStatus {
    pub code: StatusCode,
    pub subcode: StatusSubCode,
    pub severity: StatusSeverity,
}

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

fn convert_slice(src: impl AsRef<[u8]>) -> Slice {
    let src = src.as_ref();
    Slice {
        data_: src.as_ptr() as *const c_char,
        size_: src.len(),
    }
}

pub fn put(
    db: Pin<&mut DB>,
    opts: &WriteOptions,
    key: impl AsRef<[u8]>,
    val: impl AsRef<[u8]>,
) -> DbStatus {
    let key = convert_slice(key);
    let val = convert_slice(val);
    moveit! { let status = db.Put2(opts, &key, &val); }
    convert_status(&status)
}

#[macro_export]
macro_rules! let_write_opts {
    ($i:ident = [$( $opt:ident ),*]) => {
        $crate::moveit! {
            let mut $i = $crate::WriteOptions::new();
        }

        $(
            match stringify!($opt) {
                "sync" => {
                    $crate::ffi::rocksdb_additions::set_w_opts_sync($i.as_mut(), true);
                }
                "no_sync" => {
                    $crate::ffi::rocksdb_additions::set_w_opts_sync($i.as_mut(), false);
                }
                "disable_wal" => {
                    $crate::ffi::rocksdb_additions::set_w_opts_disable_wal($i.as_mut(), true);
                }
                "no_disable_wal" => {
                    $crate::ffi::rocksdb_additions::set_w_opts_disable_wal($i.as_mut(), false);
                }
                "low_pri" => {
                    $crate::ffi::rocksdb_additions::set_w_opts_low_pri($i.as_mut(), true);
                }
                "no_set_w_opts_low_pri" => {
                    $crate::ffi::rocksdb_additions::set_w_opts_low_pri($i.as_mut(), false);
                }
                _ => panic!("unknown option to let_write_opts: {}", stringify!($i))
            };
        )*
    }
}

#[macro_export]
macro_rules! let_read_opts {
    ($i:ident = [$( $opt:ident ),*]) => {
        $crate::moveit! {
            let mut $i = $crate::ReadOptions::new();
        }

        $(
            match stringify!($opt) {
                "total_order_seek" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_total_order_seek($i.as_mut(), true);
                }
                "no_total_order_seek" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_total_order_seek($i.as_mut(), false);
                }
                "auto_prefix_mode" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_auto_prefix_mode($i.as_mut(), true);
                }
                "no_auto_prefix_mode" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_auto_prefix_mode($i.as_mut(), false);
                }
                "prefix_same_as_start" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_prefix_same_as_start($i.as_mut(), true);
                }
                "no_prefix_same_as_start" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_prefix_same_as_start($i.as_mut(), false);
                }
                "tailing" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_tailing($i.as_mut(), true);
                }
                "no_tailing" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_tailing($i.as_mut(), false);
                }
                "pin_data" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_pin_data($i.as_mut(), true);
                }
                "no_pin_data" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_pin_data($i.as_mut(), false);
                }
                "verify_checksums" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_verify_checksums($i.as_mut(), true);
                }
                "no_verify_checksums" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_verify_checksums($i.as_mut(), false);
                }
                "fill_cache" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_fill_cache($i.as_mut(), true);
                }
                "fill_cache" => {
                    $crate::ffi::rocksdb_additions::set_r_opts_fill_cache($i.as_mut(), false);
                }
                _ => panic!("unknown option to let_read_opts: {}", stringify!($i))
            };
        )*
    };
}

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

            let_read_opts!(r_opts = []);
            let_write_opts!(w_opts = [disable_wal]);
        }
        dbg!(size_of::<ReadOptions>());
    }
}
