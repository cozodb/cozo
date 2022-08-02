use std::cmp::Reverse;
use std::fmt::{Debug, Formatter};

use anyhow::{bail, Result};
use ordered_float::OrderedFloat;
use rmp_serde::Serializer;
use serde::Serialize;
use serde_derive::{Deserialize, Serialize};
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};
use uuid::Uuid;

use crate::data::encode::EncodedVec;
use crate::data::id::{EntityId, TxId};
use crate::data::triple::StoreOp;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub(crate) enum DataValue {
    #[serde(rename = "n")]
    Null,
    #[serde(rename = "b")]
    Bool(bool),
    #[serde(rename = "e")]
    EnId(EntityId),
    #[serde(rename = "i")]
    Int(i64),
    #[serde(rename = "f")]
    Float(OrderedFloat<f64>),
    #[serde(rename = "s")]
    String(SmartString<LazyCompact>),
    #[serde(rename = "u")]
    Uuid(Uuid),
    #[serde(rename = "m")]
    Timestamp(i64),
    #[serde(rename = "v")]
    Bytes(Box<[u8]>),

    #[serde(rename = "z")]
    List(Box<[DataValue]>),
    #[serde(rename = "o")]
    DescVal(Reverse<Box<DataValue>>),
    #[serde(rename = "r")]
    Bottom,
}

impl Debug for DataValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataValue::Null => {
                write!(f, "null")
            }
            DataValue::Bool(b) => {
                write!(f, "{}", b)
            }
            DataValue::EnId(id) => id.fmt(f),
            DataValue::Int(i) => {
                write!(f, "{}", i)
            }
            DataValue::Float(n) => {
                write!(f, "{}", n.0)
            }
            DataValue::String(s) => {
                write!(f, "{:?}", s)
            }
            DataValue::Uuid(u) => {
                write!(f, "{}", u)
            }
            DataValue::Timestamp(ts) => {
                write!(f, "ts@{}", ts)
            }
            DataValue::Bytes(b) => {
                write!(f, "bytes(len={})", b.len())
            }
            DataValue::List(t) => f.debug_list().entries(t.iter()).finish(),
            DataValue::DescVal(v) => {
                write!(f, "desc<{:?}>", v)
            }
            DataValue::Bottom => {
                write!(f, "bottom")
            }
        }
    }
}

pub(crate) const INLINE_VAL_SIZE_LIMIT: usize = 60;

impl DataValue {
    pub(crate) fn encode_with_op_and_tx(
        &self,
        op: StoreOp,
        txid: TxId,
    ) -> EncodedVec<INLINE_VAL_SIZE_LIMIT> {
        let mut ret = SmallVec::<[u8; INLINE_VAL_SIZE_LIMIT]>::new();
        ret.extend(txid.bytes());
        ret[0] = op as u8;
        self.serialize(&mut Serializer::new(&mut ret)).unwrap();
        ret.into()
    }

    pub(crate) fn get_entity_id(&self) -> Result<EntityId> {
        match self {
            DataValue::EnId(id) => Ok(*id),
            v => bail!("type mismatch: expect type {:?}, got value {:?}", self, v),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::mem::size_of;

    use crate::data::keyword::Keyword;
    use crate::data::value::DataValue;

    #[test]
    fn show_size() {
        dbg!(size_of::<DataValue>());
        dbg!(size_of::<Keyword>());
        dbg!(size_of::<String>());
        dbg!(size_of::<HashMap<String, String>>());
        dbg!(size_of::<BTreeMap<String, String>>());
    }
}
