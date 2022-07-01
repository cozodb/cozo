use crate::data::encode::StorageTag::Tx;
use crate::data::id::{AttrId, EntityId, TxId};
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::data::value::Value;
use anyhow::Result;
use rmp_serde::Serializer;
use serde::Serialize;
use smallvec::SmallVec;
use std::ops::Deref;

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
pub(crate) enum StorageTag {
    TripleEntityAttrValue = 1,
    TripleAttrEntityValue = 2,
    TripleAttrValueEntity = 3,
    TripleValueAttrEntity = 4,
    AttrById = 5,
    AttrByKeyword = 6,
    Tx = 7,
    UniqueEntity = 8,
    UniqueAttrValue = 9,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageTagError {
    #[error("unexpected value for StoreOp: {0}")]
    UnexpectedValue(u8),
}

impl TryFrom<u8> for StorageTag {
    type Error = StorageTagError;
    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        use StorageTag::*;
        Ok(match value {
            1 => TripleEntityAttrValue,
            2 => TripleAttrEntityValue,
            3 => TripleAttrValueEntity,
            4 => TripleValueAttrEntity,
            5 => AttrById,
            6 => AttrByKeyword,
            7 => Tx,
            8 => UniqueEntity,
            9 => UniqueAttrValue,
            n => return Err(StorageTagError::UnexpectedValue(n)),
        })
    }
}

#[inline]
pub(crate) fn encode_value(val: Value) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 60]>::new();
    val.serialize(&mut Serializer::new(&mut ret)).unwrap();
    ret
}

#[inline]
pub(crate) fn decode_value(src: &[u8]) -> Result<Value> {
    Ok(rmp_serde::from_slice(src)?)
}

#[inline]
pub(crate) fn decode_value_from_key(src: &[u8]) -> Result<Value> {
    Ok(rmp_serde::from_slice(&src[20..])?)
}

/// eid: 8 bytes (incl. tag)
/// aid: 4 bytes
/// val: variable
/// tx: 8 bytes (incl. op)
#[inline]
pub(crate) fn encode_eav_key(
    eid: EntityId,
    aid: AttrId,
    val: Value,
    tx: TxId,
    op: StoreOp,
) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 60]>::new();

    ret.extend(eid.0.to_be_bytes());
    ret[0] = StorageTag::TripleEntityAttrValue as u8;

    ret.extend(aid.0.to_be_bytes());

    ret.extend(tx.0.to_be_bytes());
    ret[12] = op as u8;
    debug_assert_eq!(ret.len(), 20);

    val.serialize(&mut Serializer::new(&mut ret)).unwrap();

    ret
}

#[inline]
pub(crate) fn decode_ea_key(src: &[u8]) -> Result<(EntityId, AttrId, TxId, StoreOp)> {
    let eid = EntityId::from_bytes(&src[0..8]);
    let aid = AttrId::from_bytes(&src[8..12]);
    let tx = TxId::from_bytes(&src[12..20]);
    let op = src[12].try_into()?;

    Ok((eid, aid, tx, op))
}

/// eid: 8 bytes (incl. tag)
/// aid: 4 bytes
/// val: variable
/// tx: 8 bytes (incl. op)
#[inline]
pub(crate) fn encode_aev_key(
    aid: AttrId,
    eid: EntityId,
    val: Value,
    tx: TxId,
    op: StoreOp,
) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 60]>::new();

    ret.extend(aid.0.to_be_bytes());
    ret[0] = StorageTag::TripleAttrEntityValue as u8;

    ret.extend(eid.0.to_be_bytes());
    ret.extend(tx.0.to_be_bytes());
    ret[12] = op as u8;
    debug_assert_eq!(ret.len(), 20);

    val.serialize(&mut Serializer::new(&mut ret)).unwrap();

    ret
}

#[inline]
pub(crate) fn decode_ae_key(src: &[u8]) -> Result<(AttrId, EntityId, TxId, StoreOp)> {
    let aid = AttrId::from_bytes(&src[0..4]);
    let eid = EntityId::from_bytes(&src[4..12]);
    let tx = TxId::from_bytes(&src[12..20]);
    let op = src[12].try_into()?;

    Ok((aid, eid, tx, op))
}

/// aid: 4 bytes (incl. tag)
/// val: variable
/// eid: 8 bytes
/// tx: 8 bytes (incl. op)
#[inline]
pub(crate) fn encode_ave_key(
    aid: AttrId,
    val: Value,
    eid: EntityId,
    tx: TxId,
    op: StoreOp,
) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 60]>::new();

    ret.extend(aid.0.to_be_bytes());
    ret[0] = StorageTag::TripleAttrValueEntity as u8;

    ret.extend(eid.0.to_be_bytes());
    ret.extend(tx.0.to_be_bytes());
    ret[12] = op as u8;
    debug_assert_eq!(ret.len(), 20);

    val.serialize(&mut Serializer::new(&mut ret)).unwrap();

    ret
}

/// val: 8 bytes (incl. tag)
/// eid: 8 bytes
/// aid: 4 bytes
/// tx: 8 bytes (incl. op)
#[inline]
pub(crate) fn encode_vae_key(
    val: EntityId,
    aid: AttrId,
    eid: EntityId,
    tx: TxId,
    op: StoreOp,
) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 60]>::new();

    ret.extend(val.0.to_be_bytes());
    ret[0] = StorageTag::TripleAttrValueEntity as u8;

    ret.extend(aid.0.to_be_bytes());
    ret.extend(tx.0.to_be_bytes());
    ret[12] = op as u8;
    debug_assert_eq!(ret.len(), 20);
    ret.extend(eid.0.to_be_bytes());
    debug_assert_eq!(ret.len(), 28);

    ret
}

#[inline]
pub(crate) fn decode_vae_key(src: &[u8]) -> Result<(EntityId, AttrId, EntityId, TxId, StoreOp)> {
    let vid = EntityId::from_bytes(&src[0..8]);
    let aid = AttrId::from_bytes(&src[8..12]);
    let tx = TxId::from_bytes(&src[12..20]);
    let eid = EntityId::from_bytes(&src[20..28]);
    let op = src[12].try_into()?;

    Ok((vid, aid, eid, tx, op))
}

/// aid: 4 bytes (incl. tag)
/// tx: 8 bytes (incl. op)
#[inline]
pub(crate) fn encode_attr_by_id(aid: AttrId, tx: TxId, op: StoreOp) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 12]>::new();
    ret.extend(aid.0.to_be_bytes());
    ret[0] = StorageTag::AttrById as u8;
    ret.extend(tx.0.to_be_bytes());
    ret[4] = op as u8;
    debug_assert_eq!(ret.len(), 12);
    ret
}

#[inline]
pub(crate) fn decode_attr_key_by_id(src: &[u8]) -> Result<(AttrId, TxId, StoreOp)> {
    let aid = AttrId::from_bytes(&src[0..4]);
    let tx = TxId::from_bytes(&src[4..12]);
    let op = src[4].try_into()?;
    Ok((aid, tx, op))
}

/// tag: 4 bytes (with prefix)
/// tx: 8 bytes (incl. op)
/// attr as kw: variable (segmented by \0)
#[inline]
pub(crate) fn encode_attr_by_kw(
    attr_name: Keyword,
    tx: TxId,
    op: StoreOp,
) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 12]>::new();
    ret.push(StorageTag::AttrByKeyword as u8);
    let ns_bytes = attr_name.ns.as_bytes();
    ret.push(ns_bytes.get(0).cloned().unwrap_or(0));
    ret.push(ns_bytes.get(1).cloned().unwrap_or(0));
    ret.push(ns_bytes.get(2).cloned().unwrap_or(0));
    ret.extend(tx.0.to_be_bytes());
    ret[4] = op as u8;
    ret.extend_from_slice(ns_bytes);
    ret.push(b'/');
    ret.extend_from_slice(attr_name.ident.as_bytes());
    ret
}

#[inline]
pub(crate) fn decode_attr_key_by_kw(src: &[u8]) -> Result<(Keyword, TxId, StoreOp)> {
    let tx = TxId::from_bytes(&src[4..12]);
    let op = src[4].try_into()?;
    let kw = Keyword::try_from(&src[12..])?;
    Ok((kw, tx, op))
}

/// tx: 8 bytes (incl. tag)
#[inline]
pub(crate) fn encode_tx(tx: TxId) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 8]>::new();
    ret.extend(tx.0.to_be_bytes());
    ret[0] = StorageTag::Tx as u8;
    ret
}

#[inline]
pub(crate) fn encode_unique_entity_placeholder(eid: EntityId) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 8]>::new();
    ret.extend(eid.0.to_be_bytes());
    ret[0] = StorageTag::UniqueEntity as u8;
    ret
}

#[inline]
pub(crate) fn encode_unique_attr_val(aid: AttrId, val: Value) -> impl Deref<Target = [u8]> {
    let mut ret = SmallVec::<[u8; 60]>::new();
    ret.extend(aid.0.to_be_bytes());
    ret[0] = StorageTag::UniqueAttrValue as u8;
    val.serialize(&mut Serializer::new(&mut ret)).unwrap();
    ret
}

#[inline]
pub(crate) fn decode_unique_attr_val(src: &[u8]) -> Result<(AttrId, Value)> {
    let a_id = AttrId::from_bytes(&src[..4]);
    let val = rmp_serde::from_slice(&src[4..])?;
    Ok((a_id, val))
}
