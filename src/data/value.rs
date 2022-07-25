use std::cmp::Reverse;
use std::fmt::{Binary, Debug, Display, Formatter, Pointer};

use anyhow::Result;
use ordered_float::OrderedFloat;
use rmp_serde::Serializer;
use serde::Serialize;
use serde_derive::{Deserialize, Serialize};
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};
use uuid::Uuid;

use cozorocks::PinSlice;

use crate::data::encode::{decode_value, EncodedVec};
use crate::data::id::{EntityId, TxId};
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;

#[derive(Debug, thiserror::Error)]
pub enum ValueError {
    #[error("type mismatch: expected {0}, got {1}")]
    TypeMismatch(String, String),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum DataValue {
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
    #[serde(rename = "k")]
    Keyword(Keyword),
    #[serde(rename = "s")]
    String(SmartString<LazyCompact>),
    #[serde(rename = "u")]
    Uuid(Uuid),
    #[serde(rename = "m")]
    Timestamp(i64),
    #[serde(rename = "v")]
    Bytes(Box<[u8]>),

    #[serde(rename = "z")]
    Tuple(Box<[DataValue]>),
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
            DataValue::EnId(id) => {
                id.fmt(f)
            }
            DataValue::Int(i) => {
                write!(f, "{}", i)
            }
            DataValue::Float(n) => {
                write!(f, "{}", n.0)
            }
            DataValue::Keyword(k) => {
                write!(f, "{:?}", k)
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
            DataValue::Tuple(t) => {
                f.debug_list()
                    .entries(t.iter())
                    .finish()
            }
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

    pub(crate) fn get_entity_id(&self) -> Result<EntityId, ValueError> {
        match self {
            DataValue::EnId(id) => Ok(*id),
            v => Err(ValueError::TypeMismatch(
                "EntityId".to_string(),
                format!("{:?}", v),
            )),
        }
    }
}

pub(crate) struct PinSliceValue {
    inner: PinSlice,
}

impl PinSliceValue {
    pub(crate) fn as_value(&self) -> Result<DataValue> {
        decode_value(&self.inner.as_ref()[1..])
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
