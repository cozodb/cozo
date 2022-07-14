use std::cmp::Ordering;

use crate::data::encode::{
    decode_ae_key, decode_attr_key_by_id, decode_ea_key, decode_sentinel_attr_val, decode_vae_key,
    decode_value_from_key, StorageTag,
};

pub(crate) fn rusty_cmp(a: &[u8], b: &[u8]) -> i8 {
    match compare_key(a, b) {
        Ordering::Greater => 1,
        Ordering::Equal => 0,
        Ordering::Less => -1,
    }
}

pub(crate) const DB_KEY_PREFIX_LEN: usize = 8;

macro_rules! return_if_resolved {
    ($o:expr) => {
        match $o {
            std::cmp::Ordering::Equal => {}
            o => return o,
        }
    };
}

#[inline]
pub(crate) fn compare_key(a: &[u8], b: &[u8]) -> Ordering {
    use StorageTag::*;

    return_if_resolved!(a[0].cmp(&b[0]));

    let tag = match StorageTag::try_from(a[0]) {
        Ok(tag) => tag,
        Err(e) => {
            panic!("comparison failed with {:?} for {:x?}, {:x?}", e, a, b)
        }
    };

    match tag {
        TripleEntityAttrValue => compare_key_triple_eav(a, b),
        TripleAttrEntityValue => compare_key_triple_aev(a, b),
        TripleAttrValueEntity => compare_key_triple_ave(a, b),
        TripleValueAttrEntity => compare_key_triple_vae(a, b),
        AttrById => compare_key_attr_by_id(a, b),
        Tx => compare_key_tx(a, b),
        SentinelEntityAttr => compare_key_unique_entity_attr(a, b),
        SentinelAttrValue => compare_key_unique_attr_val(a, b),
        SentinelAttrById => compare_key_unique_attr_by_id(a, b),
        SentinelAttrByKeyword => compare_key_unique_attr_by_kw(a, b),
    }
}

#[inline]
fn compare_key_triple_eav(a: &[u8], b: &[u8]) -> Ordering {
    let (a_e, a_a, a_t) = decode_ea_key(a).unwrap();
    let (b_e, b_a, b_t) = decode_ea_key(b).unwrap();

    return_if_resolved!(a_e.cmp(&b_e));
    return_if_resolved!(a_a.cmp(&b_a));

    let a_v = decode_value_from_key(a).unwrap();
    let b_v = decode_value_from_key(b).unwrap();

    return_if_resolved!(a_v.cmp(&b_v));
    a_t.cmp(&b_t).reverse()
}

#[inline]
fn compare_key_triple_aev(a: &[u8], b: &[u8]) -> Ordering {
    let (a_a, a_e, a_t) = decode_ae_key(a).unwrap();
    let (b_a, b_e, b_t) = decode_ae_key(b).unwrap();

    return_if_resolved!(a_a.cmp(&b_a));
    return_if_resolved!(a_e.cmp(&b_e));

    let a_v = decode_value_from_key(a).unwrap();
    let b_v = decode_value_from_key(b).unwrap();

    return_if_resolved!(a_v.cmp(&b_v));
    a_t.cmp(&b_t).reverse()
}

#[inline]
fn compare_key_triple_ave(a: &[u8], b: &[u8]) -> Ordering {
    let (a_a, a_e, a_t) = decode_ae_key(a).unwrap();
    let (b_a, b_e, b_t) = decode_ae_key(b).unwrap();

    return_if_resolved!(a_a.cmp(&b_a));

    let a_v = decode_value_from_key(a).unwrap();
    let b_v = decode_value_from_key(b).unwrap();

    return_if_resolved!(a_v.cmp(&b_v));
    return_if_resolved!(a_e.cmp(&b_e));
    a_t.cmp(&b_t).reverse()
}

#[inline]
fn compare_key_triple_vae(a: &[u8], b: &[u8]) -> Ordering {
    let (a_v, a_a, a_e, a_t) = decode_vae_key(a).unwrap();
    let (b_v, b_a, b_e, b_t) = decode_vae_key(b).unwrap();

    return_if_resolved!(a_v.cmp(&b_v));
    return_if_resolved!(a_a.cmp(&b_a));
    return_if_resolved!(a_e.cmp(&b_e));
    a_t.cmp(&b_t).reverse()
}

#[inline]
fn compare_key_attr_by_id(a: &[u8], b: &[u8]) -> Ordering {
    debug_assert_eq!(a[0], StorageTag::AttrById as u8);
    debug_assert_eq!(b[0], StorageTag::AttrById as u8);
    let (a_a, a_t) = decode_attr_key_by_id(a).unwrap();
    let (b_a, b_t) = decode_attr_key_by_id(b).unwrap();

    return_if_resolved!(a_a.cmp(&b_a));
    a_t.cmp(&b_t).reverse()
}

#[inline]
fn compare_key_tx(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b).reverse()
}

#[inline]
fn compare_key_unique_entity_attr(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

#[inline]
fn compare_key_unique_attr_val(a: &[u8], b: &[u8]) -> Ordering {
    let (a_a, a_v) = decode_sentinel_attr_val(a).unwrap();
    let (b_a, b_v) = decode_sentinel_attr_val(b).unwrap();
    return_if_resolved!(a_a.cmp(&b_a));
    a_v.cmp(&b_v)
}

#[inline]
fn compare_key_unique_attr_by_id(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

#[inline]
fn compare_key_unique_attr_by_kw(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}
