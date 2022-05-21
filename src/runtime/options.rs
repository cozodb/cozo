use crate::data::tuple::PREFIX_LEN;
use cozorocks::{
    OTxnDbOptionsPtr, OTxnOptionsPtr, OptionsPtr, PTxnDbOptionsPtr,
    PTxnOptionsPtr, ReadOptionsPtr, RustComparatorPtr, TDbOptions, TransactOptions,
    WriteOptionsPtr,
};
use lazy_static::lazy_static;

const COMPARATOR_NAME: &str = "cozo_cmp_v1";

lazy_static! {
    static ref DEFAULT_COMPARATOR: RustComparatorPtr =
        RustComparatorPtr::new(COMPARATOR_NAME, crate::data::key_order::compare, false);
}

pub fn default_options() -> OptionsPtr {
    let mut options = OptionsPtr::default();

    options
        .set_comparator(&DEFAULT_COMPARATOR)
        .set_create_if_missing(true)
        .set_bloom_filter(10., true)
        .set_fixed_prefix_extractor(PREFIX_LEN);
    options
}

pub fn default_read_options() -> ReadOptionsPtr {
    ReadOptionsPtr::default()
}

pub fn default_write_options() -> WriteOptionsPtr {
    WriteOptionsPtr::default()
}

// pub fn default_flush_options() -> FlushOptionsPtr {
//     FlushOptionsPtr::default()
// }

pub fn default_txn_db_options(optimistic: bool) -> TDbOptions {
    if optimistic {
        TDbOptions::Optimistic(OTxnDbOptionsPtr::default())
    } else {
        TDbOptions::Pessimistic(PTxnDbOptionsPtr::default())
    }
}

pub fn default_txn_options(optimistic: bool) -> TransactOptions {
    if optimistic {
        let o = OTxnOptionsPtr::new(&DEFAULT_COMPARATOR);
        TransactOptions::Optimistic(o)
    } else {
        let mut o = PTxnOptionsPtr::default();
        o.set_deadlock_detect(true);
        TransactOptions::Pessimistic(o)
    }
}
