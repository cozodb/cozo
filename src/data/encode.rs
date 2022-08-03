use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};

use anyhow::{bail, Result};
use rmp_serde::Serializer;
use serde::Serialize;
use smallvec::SmallVec;

use crate::data::attr::Attribute;
use crate::data::id::{AttrId, EntityId, TxId, Validity};
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::data::value::DataValue;
use crate::runtime::transact::TxLog;

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
pub(crate) enum StorageTag {
    TripleEntityAttrValue = 1,
    TripleAttrEntityValue = 2,
    TripleAttrValueEntity = 3,
    TripleValueAttrEntity = 4,
    AttrById = 5,
    Tx = 6,
    SentinelEntityAttr = 7,
    SentinelAttrValue = 8,
    SentinelAttrById = 9,
    SentinelAttrByKeyword = 10,
}

#[derive(Clone)]
pub struct EncodedVec<const N: usize> {
    pub(crate) inner: SmallVec<[u8; N]>,
}

impl<const N: usize> EncodedVec<N> {
    pub(crate) fn copy_from_slice(&mut self, src: &[u8]) {
        self.inner.clear();
        self.inner.extend_from_slice(src)
    }
}

impl EncodedVec<LARGE_VEC_SIZE> {
    pub fn debug_value(&self, data: &[u8]) -> String {
        match StorageTag::try_from(self.inner[0]).unwrap() {
            StorageTag::TripleEntityAttrValue
            | StorageTag::TripleAttrEntityValue
            | StorageTag::TripleAttrValueEntity
            | StorageTag::TripleValueAttrEntity => {
                let op = StoreOp::try_from(data[0]).unwrap();
                let tx = TxId::from_bytes(&data[0..8]);
                if data.len() > 8 {
                    let v = decode_value(&data[8..]).unwrap();
                    format!("{:?}{} {:?}", tx, op, v)
                } else {
                    format!("{:?}{}", tx, op)
                }
            }
            StorageTag::AttrById
            | StorageTag::SentinelAttrById
            | StorageTag::SentinelAttrByKeyword => {
                let op = StoreOp::try_from(data[0]).unwrap();
                if data.len() <= 1 {
                    op.to_string()
                } else {
                    format!(
                        "{}{:?}",
                        op,
                        Attribute::decode(&data[VEC_SIZE_8..]).unwrap()
                    )
                }
            }
            StorageTag::Tx => format!("{:?}", TxLog::decode(data).unwrap()),
            StorageTag::SentinelEntityAttr | StorageTag::SentinelAttrValue => {
                format!("{:?}", TxId::from_bytes(data))
            }
        }
    }
}

impl<const N: usize> Debug for EncodedVec<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match StorageTag::try_from(self.inner[0]) {
            Err(_) => {
                write!(
                    f,
                    "?{:x?} {}",
                    self.inner,
                    String::from_utf8_lossy(&self.inner)
                )
            }
            Ok(tag) => {
                write!(f, "{:?}", tag)?;
                match tag {
                    StorageTag::TripleEntityAttrValue => {
                        let (e, a, t) = decode_ea_key(self).unwrap();
                        let v = decode_value_from_key(self).unwrap();
                        write!(f, " [{:?}, {:?}, {:?}] @{:?}", e, a, v, t)
                    }
                    StorageTag::TripleAttrEntityValue | StorageTag::TripleAttrValueEntity => {
                        let (a, e, t) = decode_ae_key(self).unwrap();
                        let v = decode_value_from_key(self).unwrap();
                        write!(f, " [{:?}, {:?}, {:?}] @{:?}", e, a, v, t)
                    }
                    StorageTag::TripleValueAttrEntity => {
                        let (v, a, e, t) = decode_vae_key(self).unwrap();
                        write!(f, " [{:?}, {:?}, {:?}] @{:?}", e, a, v, t)
                    }
                    StorageTag::AttrById => {
                        debug_assert_eq!(self[0], StorageTag::AttrById as u8);
                        let (a, t) = decode_attr_key_by_id(self).unwrap();
                        write!(f, " {:?} @{:?}", a, t)
                    }
                    StorageTag::Tx => {
                        write!(f, " {:?}", TxId::from_bytes(self))
                    }
                    StorageTag::SentinelEntityAttr => {
                        write!(
                            f,
                            " <{:?}: {:?}>",
                            EntityId::from_bytes(self),
                            AttrId::from_bytes(&self[VEC_SIZE_8..])
                        )
                    }
                    StorageTag::SentinelAttrValue => {
                        let (a, v) = decode_sentinel_attr_val(self).unwrap();
                        write!(f, " <{:?}: {:?}>", a, v)
                    }
                    StorageTag::SentinelAttrById => {
                        write!(f, " {:?}", AttrId::from_bytes(self))
                    }
                    StorageTag::SentinelAttrByKeyword => {
                        let kw = decode_sentinel_attr_by_kw(self).unwrap();
                        write!(f, " {:?}", kw)
                    }
                }
            }
        }
    }
}

impl<const N: usize> EncodedVec<N> {
    pub(crate) fn encoded_entity_amend_validity(&mut self, vld: Validity) {
        let tx_bytes = vld.bytes();
        #[allow(clippy::needless_range_loop)]
        for i in 1..8 {
            self.inner[VEC_SIZE_16 + i] = tx_bytes[i];
        }
    }
    pub(crate) fn encoded_entity_amend_validity_to_inf_future(&mut self) {
        self.encoded_entity_amend_validity(Validity::MAX)
    }
    pub(crate) fn encoded_entity_amend_validity_to_inf_past(&mut self) {
        self.encoded_entity_amend_validity(Validity::MIN)
    }
}

impl<const N: usize> Deref for EncodedVec<N> {
    type Target = SmallVec<[u8; N]>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<const N: usize> DerefMut for EncodedVec<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<const N: usize> From<SmallVec<[u8; N]>> for EncodedVec<N> {
    fn from(inner: SmallVec<[u8; N]>) -> Self {
        Self { inner }
    }
}

impl TryFrom<u8> for StorageTag {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        use StorageTag::*;
        Ok(match value {
            1 => TripleEntityAttrValue,
            2 => TripleAttrEntityValue,
            3 => TripleAttrValueEntity,
            4 => TripleValueAttrEntity,
            5 => AttrById,
            6 => Tx,
            7 => SentinelEntityAttr,
            8 => SentinelAttrValue,
            9 => SentinelAttrById,
            10 => SentinelAttrByKeyword,
            n => bail!("unexpected storage tag {}", n),
        })
    }
}

pub(crate) const LARGE_VEC_SIZE: usize = 60;
pub(crate) const VEC_SIZE_32: usize = 32;
pub(crate) const VEC_SIZE_24: usize = 24;
pub(crate) const VEC_SIZE_16: usize = 16;
pub(crate) const VEC_SIZE_8: usize = 8;

#[inline]
pub(crate) fn decode_value(src: &[u8]) -> Result<DataValue> {
    Ok(rmp_serde::from_slice(src)?)
}

#[inline]
pub(crate) fn decode_value_from_key(src: &[u8]) -> Result<DataValue> {
    Ok(rmp_serde::from_slice(&src[VEC_SIZE_24..])?)
}

#[inline]
pub(crate) fn decode_value_from_val(src: &[u8]) -> Result<DataValue> {
    Ok(rmp_serde::from_slice(&src[VEC_SIZE_8..])?)
}

/// eid: 8 bytes (incl. tag)
/// aid: 8 bytes
/// val: variable
/// vld: 8 bytes
#[inline]
pub(crate) fn encode_eav_key(
    eid: EntityId,
    aid: AttrId,
    val: &DataValue,
    vld: Validity,
) -> EncodedVec<LARGE_VEC_SIZE> {
    let mut ret = SmallVec::<[u8; LARGE_VEC_SIZE]>::new();

    ret.extend(eid.bytes());
    ret[0] = StorageTag::TripleEntityAttrValue as u8;

    ret.extend(aid.bytes());

    ret.extend(vld.bytes());
    debug_assert_eq!(ret.len(), VEC_SIZE_24);

    val.serialize(&mut Serializer::new(&mut ret)).unwrap();

    ret.into()
}

#[inline]
pub(crate) fn decode_ea_key(src: &[u8]) -> Result<(EntityId, AttrId, Validity)> {
    let eid = EntityId::from_bytes(&src[0..VEC_SIZE_8]);
    let aid = AttrId::from_bytes(&src[VEC_SIZE_8..VEC_SIZE_16]);
    let vld = Validity::from_bytes(&src[VEC_SIZE_16..VEC_SIZE_24]);

    Ok((eid, aid, vld))
}

/// eid: 8 bytes (incl. tag)
/// aid: 8 bytes
/// val: variable
/// vld: 8 bytes
#[inline]
pub(crate) fn encode_aev_key(
    aid: AttrId,
    eid: EntityId,
    val: &DataValue,
    vld: Validity,
) -> EncodedVec<LARGE_VEC_SIZE> {
    let mut ret = SmallVec::<[u8; LARGE_VEC_SIZE]>::new();

    ret.extend(aid.bytes());
    ret[0] = StorageTag::TripleAttrEntityValue as u8;

    ret.extend(eid.bytes());
    ret.extend(vld.bytes());
    debug_assert_eq!(ret.len(), VEC_SIZE_24);

    val.serialize(&mut Serializer::new(&mut ret)).unwrap();

    ret.into()
}

#[inline]
pub(crate) fn decode_ae_key(src: &[u8]) -> Result<(AttrId, EntityId, Validity)> {
    debug_assert!(
        src[0] == StorageTag::TripleAttrEntityValue as u8
            || src[0] == StorageTag::TripleAttrValueEntity as u8
    );
    debug_assert!(src.len() >= VEC_SIZE_24);
    let aid = AttrId::from_bytes(&src[0..VEC_SIZE_8]);
    let eid = EntityId::from_bytes(&src[VEC_SIZE_8..VEC_SIZE_16]);
    let vld = Validity::from_bytes(&src[VEC_SIZE_16..VEC_SIZE_24]);

    Ok((aid, eid, vld))
}

#[inline]
pub(crate) fn encode_ave_key_for_unique_v(
    aid: AttrId,
    val: &DataValue,
    vld: Validity,
) -> EncodedVec<LARGE_VEC_SIZE> {
    encode_ave_key(aid, val, EntityId(0), vld)
}

/// aid: 8 bytes (incl. tag)
/// val: variable
/// eid: 8 bytes
/// vld: 8 bytes
#[inline]
pub(crate) fn encode_ave_key(
    aid: AttrId,
    val: &DataValue,
    eid: EntityId,
    vld: Validity,
) -> EncodedVec<LARGE_VEC_SIZE> {
    let mut ret = SmallVec::<[u8; LARGE_VEC_SIZE]>::new();

    ret.extend(aid.bytes());
    ret[0] = StorageTag::TripleAttrValueEntity as u8;

    ret.extend(eid.bytes());
    ret.extend(vld.bytes());
    debug_assert_eq!(ret.len(), VEC_SIZE_24);

    val.serialize(&mut Serializer::new(&mut ret)).unwrap();

    ret.into()
}

/// val: 8 bytes (incl. tag)
/// eid: 8 bytes
/// aid: 8 bytes
/// vld: 8 bytes
#[inline]
pub(crate) fn encode_vae_key(
    val: EntityId,
    aid: AttrId,
    eid: EntityId,
    vld: Validity,
) -> EncodedVec<LARGE_VEC_SIZE> {
    let mut ret = SmallVec::<[u8; LARGE_VEC_SIZE]>::new();

    ret.extend(val.bytes());
    ret[0] = StorageTag::TripleValueAttrEntity as u8;

    ret.extend(aid.bytes());
    ret.extend(vld.bytes());
    debug_assert_eq!(ret.len(), VEC_SIZE_24);
    ret.extend(eid.bytes());
    debug_assert_eq!(ret.len(), VEC_SIZE_32);

    ret.into()
}

#[inline]
pub(crate) fn decode_vae_key(src: &[u8]) -> Result<(EntityId, AttrId, EntityId, Validity)> {
    let vid = EntityId::from_bytes(&src[0..VEC_SIZE_8]);
    let aid = AttrId::from_bytes(&src[VEC_SIZE_8..VEC_SIZE_16]);
    let vld = Validity::from_bytes(&src[VEC_SIZE_16..VEC_SIZE_24]);
    let eid = EntityId::from_bytes(&src[VEC_SIZE_24..VEC_SIZE_32]);

    Ok((vid, aid, eid, vld))
}

/// aid: 8 bytes (incl. tag)
/// tx: 8 bytes
#[inline]
pub(crate) fn encode_attr_by_id(aid: AttrId, tx: TxId) -> EncodedVec<VEC_SIZE_16> {
    let mut ret = SmallVec::<[u8; VEC_SIZE_16]>::new();
    ret.extend(aid.bytes());
    ret[0] = StorageTag::AttrById as u8;
    ret.extend(tx.bytes());
    debug_assert_eq!(ret.len(), VEC_SIZE_16);
    ret.into()
}

#[inline]
pub(crate) fn decode_attr_key_by_id(src: &[u8]) -> Result<(AttrId, TxId)> {
    debug_assert_eq!(src[0], StorageTag::AttrById as u8);
    let aid = AttrId::from_bytes(&src[0..VEC_SIZE_8]);
    let tx = TxId::from_bytes(&src[VEC_SIZE_8..VEC_SIZE_16]);
    Ok((aid, tx))
}

/// tx: 8 bytes (incl. tag)
#[inline]
pub(crate) fn encode_tx(tx: TxId) -> EncodedVec<VEC_SIZE_8> {
    let mut ret = SmallVec::<[u8; VEC_SIZE_8]>::new();
    ret.extend(tx.bytes());
    ret[0] = StorageTag::Tx as u8;
    ret.into()
}

#[inline]
pub(crate) fn encode_sentinel_entity_attr(eid: EntityId, aid: AttrId) -> EncodedVec<VEC_SIZE_16> {
    let mut ret = SmallVec::<[u8; VEC_SIZE_16]>::new();
    ret.extend(eid.bytes());
    ret[0] = StorageTag::SentinelEntityAttr as u8;
    ret.extend(aid.bytes());
    ret.into()
}

#[inline]
pub(crate) fn encode_sentinel_attr_val(aid: AttrId, val: &DataValue) -> EncodedVec<LARGE_VEC_SIZE> {
    let mut ret = SmallVec::<[u8; LARGE_VEC_SIZE]>::new();
    ret.extend(aid.bytes());
    ret[0] = StorageTag::SentinelAttrValue as u8;
    val.serialize(&mut Serializer::new(&mut ret)).unwrap();
    ret.into()
}

#[inline]
pub(crate) fn decode_sentinel_attr_val(src: &[u8]) -> Result<(AttrId, DataValue)> {
    let a_id = AttrId::from_bytes(&src[..VEC_SIZE_8]);
    let val = rmp_serde::from_slice(&src[VEC_SIZE_8..])?;
    Ok((a_id, val))
}

#[inline]
pub(crate) fn encode_sentinel_attr_by_id(aid: AttrId) -> EncodedVec<VEC_SIZE_8> {
    let mut ret = SmallVec::<[u8; VEC_SIZE_8]>::new();
    ret.extend(aid.bytes());
    ret[0] = StorageTag::SentinelAttrById as u8;
    debug_assert_eq!(ret.len(), VEC_SIZE_8);
    ret.into()
}

#[inline]
pub(crate) fn encode_sentinel_attr_by_kw(kw: &Keyword) -> EncodedVec<LARGE_VEC_SIZE> {
    let mut ret = SmallVec::<[u8; LARGE_VEC_SIZE]>::new();
    ret.push(StorageTag::SentinelAttrByKeyword as u8);
    ret.extend_from_slice(kw.0.as_bytes());
    ret.into()
}

#[inline]
pub(crate) fn decode_sentinel_attr_by_kw(src: &[u8]) -> Result<Keyword> {
    Ok(Keyword::try_from(&src[1..])?)
}
