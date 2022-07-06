use crate::data::id::EntityId;
use crate::data::keyword::Keyword;
use ordered_float::OrderedFloat;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::fmt::Debug;
use rmp_serde::Serializer;
use serde::Serialize;
use smallvec::SmallVec;
use uuid::Uuid;
use crate::data::encode::EncodedVec;
use crate::data::triple::StoreOp;

#[derive(Debug, thiserror::Error)]
pub enum ValueError {
    #[error("type mismatch: expected {0}, got {1}")]
    TypeMismatch(String, String)
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub enum Value<'a> {
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
    #[serde(borrow)]
    #[serde(rename = "s")]
    String(Cow<'a, str>),
    #[serde(rename = "u")]
    Uuid(Uuid),
    #[serde(rename = "m")]
    Timestamp(i64),
    #[serde(borrow)]
    #[serde(rename = "v")]
    Bytes(Cow<'a, [u8]>),

    #[serde(rename = "z")]
    Tuple(Box<[Value<'a>]>),
    #[serde(rename = "o")]
    DescVal(Reverse<Box<Value<'a>>>),
    #[serde(rename = "r")]
    Bottom,
}

impl<'a> Value<'a> {
    pub(crate) fn to_static(&self) -> StaticValue {
        todo!()
    }
}

pub(crate) type StaticValue = Value<'static>;

pub(crate) const INLINE_VAL_SIZE_LIMIT: usize = 60;

impl<'a> Value<'a> {
    pub(crate) fn encode_with_op(&self, op: StoreOp) -> EncodedVec<INLINE_VAL_SIZE_LIMIT> {
        let mut ret = SmallVec::<[u8; INLINE_VAL_SIZE_LIMIT]>::new();
        ret.push(op as u8);
        self.serialize(&mut Serializer::new(&mut ret)).unwrap();
        ret.into()
    }

    pub(crate) fn encode(&self) -> EncodedVec<INLINE_VAL_SIZE_LIMIT> {
        let mut ret = SmallVec::<[u8; INLINE_VAL_SIZE_LIMIT]>::new();
        self.serialize(&mut Serializer::new(&mut ret)).unwrap();
        ret.into()
    }

    pub(crate) fn get_entity_id(&self) -> Result<EntityId, ValueError> {
        match self {
            Value::EnId(id) => Ok(*id),
            v => Err(ValueError::TypeMismatch("EntityId".to_string(), format!("{:?}", v)))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::keyword::Keyword;
    use crate::data::value::Value;
    use std::collections::{BTreeMap, HashMap};
    use std::mem::size_of;

    #[test]
    fn show_size() {
        dbg!(size_of::<Value>());
        dbg!(size_of::<Keyword>());
        dbg!(size_of::<String>());
        dbg!(size_of::<HashMap<String, String>>());
        dbg!(size_of::<BTreeMap<String, String>>());
    }
}
